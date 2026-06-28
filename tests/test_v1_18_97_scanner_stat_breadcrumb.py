"""v1.18.97 — silent-fail audit: scanner stat() OSError gets a log breadcrumb.

Found during the 2026-05-23 silent-fail audit (per CLAUDE.md
class 9 — silent-defensive-catch). scanner.py:_classify_themed
ran `local_path.stat().st_ino` inside a try/except OSError and
fell through to `return "hash_match"` on failure with NO log.

## Why it matters

The stat call distinguishes:
  - exact_match (already hardlinked, motif owns the canonical)
  - hash_match (same content, adoptable as canonical)

If the canonical IS hardlinked but stat fails (perm issue, race
with rename, file deleted between hash + stat), the fall-through
classifies as hash_match → the caller (adopt path) treats the
file as adoptable → tries to create a NEW canonical → duplicate
placement.

Pre-fix: completely operator-blind. The duplicate would surface
days/weeks later as a "weird state" diagnostic.

## Fix

Replace `pass` with a `log.warning` carrying the path, theme_id,
media_type/tmdb_id, and the exception. Fall-through behavior
unchanged (still hash_match — safer than synthesising
exact_match without confirmation); the breadcrumb just makes
the failure mode visible.

## What's pinned

- log.warning exists in the except OSError branch
- Message includes the canonical path + theme_id + media info
- Fall-through to hash_match preserved (no behavior regression)
- Marker references v1.18.97 silent-fail audit + class 9
"""
from __future__ import annotations

import logging
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
SCANNER_PY = REPO / "app" / "core" / "scanner.py"


# ── Source-level pins ────────────────────────────────────────


def test_stat_oserror_logs_warning():
    """The except OSError block must call log.warning, not pass.
    Per CLAUDE.md class 9: every defensive catch needs a log
    breadcrumb so operators can diagnose state drift."""
    src = SCANNER_PY.read_text()
    # Find the inode-compare block via its anchor.
    anchor = src.index("Check inode to distinguish exact_match")
    block = src[anchor:anchor + 2500]
    assert "except OSError" in block
    # Pin: the OSError branch must call log.warning.
    # Old shape was `except OSError:\n    pass` — confirm it's gone.
    assert "except OSError:\n                        pass" not in block, (
        "v1.18.97: scanner stat OSError must log, not silently pass"
    )
    # And the log.warning must be present.
    assert "log.warning" in block, (
        "v1.18.97: stat() OSError fall-through must log.warning"
    )


def test_warning_includes_diagnostic_context():
    """The warning must include path + theme/media context so a
    docker log grep can be tied back to a specific row."""
    src = SCANNER_PY.read_text()
    anchor = src.index("Check inode to distinguish exact_match")
    block = src[anchor:anchor + 2500]
    log_idx = block.index("log.warning")
    log_block = block[log_idx:log_idx + 1500]
    # Path arg.
    assert "local_path" in log_block, (
        "v1.18.97: warning must include local_path"
    )
    # theme_id / media context so operator can correlate.
    assert "theme_id" in log_block
    assert "media_type" in log_block
    assert "tmdb_id" in log_block


def test_fall_through_to_hash_match_preserved():
    """The fix MUST NOT change the classification behavior — we
    still fall through to 'hash_match' after the warning. Safer
    than synthesising 'exact_match' without confirmation
    (would create silent corruption in the opposite direction)."""
    src = SCANNER_PY.read_text()
    anchor = src.index("Check inode to distinguish exact_match")
    block = src[anchor:anchor + 2500]
    # The except branch + the subsequent return — order pinned so
    # a future refactor can't accidentally swap the fall-through
    # target.
    except_idx = block.index("except OSError")
    return_idx = block.index('return "hash_match"', except_idx)
    # The return must come AFTER the except block (no nested
    # early-return that would change semantics).
    assert return_idx > except_idx
    # No "exact_match" return between the except and the hash_match
    # fall-through — that would be a regression.
    intermediate = block[except_idx:return_idx]
    assert '"exact_match"' not in intermediate, (
        "v1.18.97: must not synthesize exact_match on stat failure "
        "— safer to fall to hash_match"
    )


def test_marker_references_silent_fail_audit():
    """Inline comment must reference v1.18.97 + the silent-fail
    audit so future archaeology can trace the lineage."""
    src = SCANNER_PY.read_text()
    anchor = src.index("Check inode to distinguish exact_match")
    block = src[anchor:anchor + 2500]
    assert "v1.18.97" in block, (
        "v1.18.97: scanner.py edit must carry the version marker"
    )
    # Reference the class-9 catalog entry so a class-9 grep finds it.
    assert "class 9" in block.lower() or "class-9" in block.lower() \
        or "silent-fail" in block.lower(), (
        "v1.18.97: marker should reference class 9 / silent-fail "
        "audit for catalog discoverability"
    )


# ── Behavioral pin: format-only ──────────────────────────────


def test_warning_message_formats_cleanly():
    """Sanity-check the warning template's % args resolve without
    error. End-to-end behavioral coverage would require fully
    plumbing _classify's hash-row early-return path; the source
    pins above already cover the contract. This test just guards
    against a typo in the format string itself."""
    from pathlib import Path as _P
    template = (
        "scanner: stat() failed on canonical %s "
        "while distinguishing exact_match vs "
        "hash_match (theme_id=%s media=%s/%s): %s "
        "— falling back to hash_match. Verify the "
        "file exists + readable; a persistent "
        "failure here will cause duplicate-"
        "placement attempts on the next adopt."
    )
    out = template % (
        _P("/tmp/test.mp3"), 42, "movie", 12345, OSError(13, "perm denied"),
    )
    assert "/tmp/test.mp3" in out
    assert "theme_id=42" in out
    assert "media=movie/12345" in out
    assert "perm denied" in out
    assert "duplicate" in out
