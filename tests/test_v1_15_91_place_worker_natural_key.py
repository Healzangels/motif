"""v1.15.91 — place worker uses natural keys for plex_items updates.

Parallel fix to v1.15.90 (which fixed the unplace handler). The
place worker had two folder_path-keyed UPDATEs on plex_items that
had the SAME path-domain mismatch bug:

* worker.py:1720+ — `existing_theme:` branch
  Sets local_theme_file=1 when the place worker finds an
  unmanaged sidecar already at the target folder.

* worker.py:1729+ — `plex_has_theme` branch
  Sets has_theme=1, local_theme_file=0 when the place worker
  skips because Plex already has its own theme.

Both used:
  WHERE folder_path = str(outcome.target_folder) AND media_type = ?

`outcome.target_folder` is the FolderIndex-resolved CONTAINER
path (e.g. /data/media/movies/X). `plex_items.folder_path` is
Plex's REPORTED path which on Unraid is the HOST path
(/mnt/user/movies/X). The strings diverge → UPDATE matches 0
rows → motif's plex_items state stays inconsistent with the
place outcome.

The downstream impact is smaller than the v1.15.90 unplace bug:
plex_enum's next stat would catch up on reachable folders. But
on Unraid setups where `_candidate_local_paths`'s translation
table doesn't cover the user's mount layout, stat returns
indeterminate (None) and preserves the stale value — same
trap that made the user's LET PLEX SERVE row stick at M+P. So
the place worker bugs are latent silent bugs in the same class.

## Fix

Switched both UPDATEs to the v1.15.90 natural-key pattern:

  WHERE theme_id = ? OR (guid_tmdb = ? AND media_type = ?)
  AND section_id = ?

theme_id is the canonical FK; guid_tmdb + media_type covers
pre-resolve and orphan rows. section_id scopes per-edition.

## Tests

This file pins the code shape (static guards). A behavioral
test for the place worker would require mocking the place
outcome, the job dispatcher, and the worker loop — out of
scope. The static guards mirror v1.15.90's
test_unplace_no_longer_uses_folder_path_lookup_only check.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
WORKER_PY = REPO / "app" / "core" / "worker.py"


def _place_outcome_stamp_block() -> str:
    """Extract the place-outcome stamping block (worker.py:1720+).
    Anchored to the v1.11.24 marker comment that introduces the
    block; ends at the next `try:` (the next try-catch block) so
    we get the full conditional UPDATE structure."""
    src = WORKER_PY.read_text()
    anchor = src.index("# v1.11.24: stamp the place outcome")
    # Reach forward to the next top-level except — enough to
    # capture both the existing_theme and plex_has_theme branches.
    end_idx = src.index("except Exception as e:", anchor)
    return src[anchor:end_idx]


def test_place_worker_uses_natural_key_for_plex_items_update():
    """The existing_theme + plex_has_theme branches must use a
    natural-key WHERE clause (theme_id or guid_tmdb) on
    plex_items, not folder_path. Path-domain mismatches on
    multi-volume setups silently break folder_path matching."""
    block = _place_outcome_stamp_block()
    # Strip comments so narrative prose mentioning folder_path
    # doesn't create false positives.
    block_no_comments = re.sub(r"#[^\n]*\n", "\n", block)
    # Find every `UPDATE plex_items ...` SQL string in the block.
    # Each must include a theme_id or guid_tmdb reference.
    update_chunks = re.findall(
        r"UPDATE\s+plex_items[^\"\']*", block_no_comments,
        re.IGNORECASE,
    )
    assert update_chunks, (
        "v1.15.91 self-check: no UPDATE plex_items statements found "
        "in the place-outcome stamp block. The expected SET clauses "
        "(local_theme_file=1 / has_theme=1) may have moved — adjust "
        "the anchor in _place_outcome_stamp_block."
    )
    # The actual SQL build now uses string concatenation, so the
    # `theme_id` / `guid_tmdb` references live in the surrounding
    # Python (pi_where_parts setup) not inside the UPDATE literal.
    # Verify they exist in the block.
    assert "theme_id" in block_no_comments, (
        "v1.15.91: place-outcome stamp block must reference theme_id "
        "(natural-key lookup for plex_items). The pre-fix WHERE "
        "folder_path = ? matched 0 rows on multi-volume setups."
    )
    assert "guid_tmdb" in block_no_comments, (
        "v1.15.91: place-outcome stamp block must reference "
        "guid_tmdb as the fallback when theme_id is NULL "
        "(pre-resolve window / orphans)."
    )


def test_place_worker_no_longer_uses_folder_path_in_plex_items_update():
    """Counter-guard: a future refactor that re-introduces
    `WHERE folder_path = ?` on the plex_items UPDATE here
    re-introduces the v1.15.91 silent bug. Fire on that pattern."""
    block = _place_outcome_stamp_block()
    block_no_comments = re.sub(r"#[^\n]*\n", "\n", block)
    # Look for the anti-pattern: a `WHERE folder_path = ?` clause
    # on a plex_items UPDATE. The local_files UPDATE in this same
    # block uses (media_type, tmdb_id, section_id) — different
    # SET clause so it doesn't false-positive.
    bad = re.search(
        r"UPDATE\s+plex_items[\s\S]*?WHERE\s+folder_path\s*=\s*\?",
        block_no_comments,
        re.IGNORECASE,
    )
    assert bad is None, (
        "v1.15.91 regression: a `UPDATE plex_items ... WHERE "
        "folder_path = ?` pattern returned in the place-outcome "
        "stamp block. Switch back to the natural-key lookup "
        "(theme_id / guid_tmdb). See v1.15.90 + v1.15.91 journal "
        "entries for the multi-volume Unraid path-mismatch story."
    )


def test_place_worker_scopes_update_by_section_id():
    """The v1.15.91 fix scopes the UPDATE by section_id so a
    per-edition place outcome doesn't stamp sibling sections'
    plex_items rows. Counter-guard against a refactor that
    drops the section_id constraint."""
    block = _place_outcome_stamp_block()
    block_no_comments = re.sub(r"#[^\n]*\n", "\n", block)
    # Confirm the natural-key block references section_id.
    assert "section_id" in block_no_comments, (
        "v1.15.91: the place-outcome stamp must scope by "
        "section_id so per-edition places don't stamp siblings. "
        "Check that pi_args appends section_id to the WHERE."
    )
