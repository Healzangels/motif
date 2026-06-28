"""v1.15.92 — scanner multi-candidate folder_path lookup.

Third site in the same path-domain mismatch class as
v1.15.90 / v1.15.91. The scanner iterates CONTAINER paths
(it walks /data/media/movies/...) but plex_items.folder_path
stores Plex's reported path which on Unraid is the HOST path
(/mnt/user/movies/...). The pre-v1.15.92 single-candidate
WHERE missed the row.

Unlike unplace + place worker (where natural-key lookup via
theme_id is the clean answer), the scanner discovers orphan
sidecars where theme_id may still be NULL. So v1.15.92 uses
a two-stage strategy:

1. If theme_id is non-NULL (classified row): natural-key
   WHERE (theme_id OR folder_path IN candidates).
2. If theme_id is NULL (orphan_unresolved): folder_path IN
   candidates only.

The "candidates" set is the container path the scanner saw
PLUS every REVERSE-translated host-path form. We invert
_PATH_PREFIX_TRANSLATIONS: if the container path starts with
the `dst` side of a translation pair, prepend the `src` side
to produce a candidate host path. Covers Plex setups where
host paths look like /mnt/user/* and the container has /data/*.

## Tests

Static-text guards mirror v1.15.91's pattern:
* The scanner's UPDATE block uses folder_path IN (?,...) not
  folder_path = ? — the multi-candidate form.
* The block references theme_id (natural-key first stage).
* The block imports / uses _PATH_PREFIX_TRANSLATIONS (the
  reverse-translation candidate generator).
* Counter-guard: no `WHERE folder_path = ?` (single-candidate)
  pattern survives in the affected block.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SCANNER_PY = REPO / "app" / "core" / "scanner.py"


def _scan_finding_block() -> str:
    """Slice the scanner block where plex_items.local_theme_file
    gets set after a finding is recorded. Anchored to the v1.15.92
    marker that introduces the multi-candidate logic."""
    src = SCANNER_PY.read_text()
    anchor = src.index("# v1.15.92: multi-candidate folder_path UPDATE")
    # The block ends at `return True` of the enclosing function.
    end_idx = src.index("return True", anchor)
    return src[anchor:end_idx]


def test_scanner_uses_multi_candidate_folder_path():
    """The UPDATE must use `folder_path IN (?,...)` not a single
    `folder_path = ?`. Multi-candidate covers the host-vs-container
    path divergence on Unraid setups."""
    block = _scan_finding_block()
    # Strip narrative comments before regex-checking the SQL.
    code = re.sub(r"#[^\n]*\n", "\n", block)
    assert "folder_path IN (" in code, (
        "v1.15.92: scanner must use `folder_path IN (?,...)` to "
        "match any of the path-translation candidates. "
        "Single-candidate `folder_path = ?` re-introduces the "
        "Unraid silent-miss bug."
    )


def test_scanner_no_longer_uses_single_candidate_folder_path():
    """Counter-guard: the pre-v1.15.92 `folder_path = ?` UPDATE
    must not survive in this block."""
    block = _scan_finding_block()
    code = re.sub(r"#[^\n]*\n", "\n", block)
    # Look for the anti-pattern: a `WHERE folder_path = ?` that's
    # NOT part of the IN clause we expect.
    # The actual UPDATE WHERE is `folder_path IN (?,?,?)` so a
    # bare `folder_path = ?` should be absent.
    bad = re.search(
        r"UPDATE\s+plex_items[\s\S]*?WHERE[\s\S]*?folder_path\s*=\s*\?",
        code,
        re.IGNORECASE,
    )
    assert bad is None, (
        "v1.15.92 regression: a single-candidate `folder_path = ?` "
        "WHERE clause returned in the scanner. Switch back to the "
        "multi-candidate `folder_path IN (?,...)` form."
    )


def test_scanner_natural_key_first_when_theme_id_available():
    """For classified rows (theme_id set by _classify), the UPDATE
    should try theme_id first. Pin that the block references
    theme_id in a WHERE clause."""
    block = _scan_finding_block()
    code = re.sub(r"#[^\n]*\n", "\n", block)
    assert re.search(r"theme_id\s*=\s*\?", code), (
        "v1.15.92: scanner must use `theme_id = ?` as the natural-key "
        "WHERE branch for classified rows (theme_id non-NULL post-"
        "_classify). Only orphan_unresolved findings should fall "
        "through to folder_path-only matching."
    )


def test_scanner_imports_path_prefix_translations():
    """The reverse-translation candidate generation requires
    `_PATH_PREFIX_TRANSLATIONS`. Pin that the import is present
    in the scanner block so a future refactor that removes it
    fails this test."""
    block = _scan_finding_block()
    code = re.sub(r"#[^\n]*\n", "\n", block)
    assert "_PATH_PREFIX_TRANSLATIONS" in code, (
        "v1.15.92: scanner must import _PATH_PREFIX_TRANSLATIONS "
        "from plex_enum so it can generate reverse-translated "
        "host-path candidates."
    )


def test_scanner_generates_reverse_translation_candidates():
    """The reverse-translation logic iterates the translation
    pairs and prepends src when the path starts with dst. Pin
    the shape (src + ...) so a refactor doesn't accidentally
    invert the direction back to host→container."""
    block = _scan_finding_block()
    code = re.sub(r"#[^\n]*\n", "\n", block)
    # The reverse direction: src + folder_str[len(dst):]
    assert re.search(
        r"src\s*\+\s*folder_str\[\s*len\(dst\)\s*:\s*\]",
        code,
    ), (
        "v1.15.92: scanner's reverse translation must produce "
        "`src + folder_str[len(dst):]` — adding the host-side "
        "prefix to a container-relative tail. A future refactor "
        "that inverts this back to `dst + folder_str[len(src):]` "
        "regresses to the container-path-only matching the bug "
        "is meant to fix."
    )
