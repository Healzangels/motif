"""v1.22.74 (audit round 2, Batch C #3) — git drop detection was
one-shot with no retry path.

_run_git_differential_upsert advanced the git baseline
(commit_sync_ok → MOTIF_LAST_SYNC) at its END, but
_detect_and_stamp_drops_git runs much later in run_sync. Any
detection failure — the broad `except Exception` around the sweep, a
`database is locked` in the stamping loop, or a crash/cancel in the
window — permanently lost the removals: the next sync diffs from the
NEW head, the removed paths never reappear in a changeset, and the
themes stay un-stamped (SRC=T, live-looking) forever. The full-walk
path naturally retries via stale last_seen_sync_at; the git path had
no second chance (the structural sibling of the v1.21.38
partial-fetch class — a partial OUTCOME treated as final).

Fix: run_sync advances the baseline right AFTER
_detect_and_stamp_drops_git returns (still inside the try — a raise
skips the advance and the next run re-diffs from the old head; the
differential upsert is idempotent). The detector's 5% safety-cap skip
returns normally and still advances — that's the deliberate
amplifier guard. The empty-changeset early return inside the upsert
keeps its own commit (nothing to lose).
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SYNC_PY = (REPO / "app" / "core" / "sync.py").read_text()


def _upsert_body() -> str:
    i = SYNC_PY.index("def _run_git_differential_upsert(")
    return SYNC_PY[i:SYNC_PY.index("\ndef ", i + 1)]


def test_upsert_no_longer_commits_at_its_end():
    body = _upsert_body()
    # Exactly ONE commit left in the helper: the empty-changeset early
    # return (no removals to lose). The end-of-function commit is gone.
    assert body.count("git_mirror.commit_sync_ok()") == 1
    empty_idx = body.index("if changes.is_empty:")
    commit_idx = body.index("git_mirror.commit_sync_ok()")
    assert commit_idx > empty_idx
    assert commit_idx - empty_idx < 400, (
        "the surviving commit must be the is_empty early-return one"
    )


def test_run_sync_commits_after_drop_detection():
    """The baseline advance runs only after git drop detection consumed the
    changeset AND succeeded. v1.24.0 split it out of the detection try into an
    `if ran_git_diff and detection_ok:` gate (per-phase error labels) — a
    detection raise leaves detection_ok False, so the next run re-diffs the
    same changeset (the v1.22.74 retry semantic, now structural)."""
    call = SYNC_PY.index(
        "n_dropped = _detect_and_stamp_drops_git(\n"
        "                        db_path, git_mirror, sync_ts=sync_ts)")
    # v0.51.294: this window was widened TWICE (3200→4000→7000) and rotted a
    # third time when the snapshot validator-commit landed before the gate —
    # the .261 bug class. Anchored now to the compaction line after the gate.
    after = SYNC_PY[call:SYNC_PY.index("compact the mirror if over", call)]
    assert "git_mirror.commit_sync_ok()" in after, (
        "v1.22.74: run_sync must advance MOTIF_LAST_SYNC after drop detection"
    )
    # The advance is gated on detection success — the detection try (with its
    # own "drop detection failed" except) closes BEFORE the commit gate.
    assert "detection_ok = True" in after
    # v1.24.14: the commit gate gained `and stats.errors == 0` (hold the
    # baseline when a changed-path read failed).
    gate_at = after.index("if ran_git_diff and detection_ok and stats.errors == 0:")
    # v0.51.14: search from the gate — the chronic escape has its own earlier
    # commit_sync_ok call; this assert pins the CLEAN-path one.
    commit_at = after.index("git_mirror.commit_sync_ok()", gate_at)
    except_at = after.index("log.warning(\"drop detection failed")
    assert except_at < gate_at < commit_at, (
        "v1.24.0: commit_sync_ok sits under the detection_ok gate (after the "
        "detection except) so a detection failure skips the baseline advance"
    )


def test_detection_failure_semantics_documented():
    """The moved-commit rationale is marked at both ends (searchable
    archaeology per CLAUDE.md)."""
    body = _upsert_body()
    assert "v1.22.74" in body
    i = SYNC_PY.index("advance the git baseline only AFTER drop")
    assert "v1.22.74" in SYNC_PY[i - 200:i]
