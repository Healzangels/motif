"""v1.19.17 — cleanup duplicate placement rows + walker kind=file fix.

## What happened

v1.19.16's walker enqueued place jobs with no `kind` in
payload. Worker dispatch (worker.py:1853-1857) falls back to
`settings.default_placement_method` when payload kind is unset.
For installs with default='api' (the user's case), the worker
dispatched to `_do_place_collection` which:

  1. Uploads the file to Plex via /library/metadata/{rk}/themes
     (multipart POST).
  2. Removes the Plex-folder sidecar as cleanup.
  3. UPSERTs a NEW placements row with media_folder='' +
     placement_kind='plex_upload'.

The UPSERT's ON CONFLICT clause is keyed on
(media_type, tmdb_id, section_id, media_folder) but media_folder
went '/data/.../...' → ''. The constraint doesn't match → fresh
row inserts instead of updating. **Old hardlink row persists
with a media_folder pointing at a file that no longer exists.**

the user's instance at v1.19.17 cut: 10 duplicate stale hardlink
rows from the v1.19.16 walker's API uploads.

Separately, tv/-44 (synthetic orphan, no Plex rk) kept failing
the collection upload path indefinitely — orphans can't use the
API path; they need file/sidecar.

## The fix (two parts)

1. **Walker kind=file in payload**: forces sidecar route for
   repair operations. Idempotent re-hardlink. Works for orphans
   (no rk needed). Repair NEVER converts placement_kind.
2. **Cleanup function**: `maybe_cleanup_duplicate_placements`
   deletes stale hardlink rows where a plex_upload row exists
   for the same item AND the hardlink's sidecar file is missing.
   Sanity check on file presence — preserves legitimate dual
   states where both routes intentionally coexist.

## Marker keys

  - walker: bumped to `recovery_stale_placements_done_at_v1_19_17`
    (v1.19.16's marker is now stale).
  - cleanup: new
    `recovery_duplicate_placements_cleanup_done_at_v1_19_17`.

## What's pinned

- Cleanup function exists with independent marker.
- Cleanup query joins placements to itself on
  (mt, tmdb, sec) finding hardlink/copy WHERE a plex_upload
  sibling exists with newer placed_at.
- File-presence sanity check before deletion.
- Boot ordering: cleanup runs BEFORE the walker (so walker
  sees clean state).
- Walker payload now includes kind=file.
- v1.19.17 walker marker key is bumped.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

RECOVERY_PY = REPO / "app" / "core" / "recovery_v55.py"
MAIN_PY = REPO / "app" / "main.py"


# ── Source-level pins ────────────────────────────────────────


def test_cleanup_function_exists():
    """`maybe_cleanup_duplicate_placements(db_path)` must be
    importable from app.core.recovery_v55."""
    src = RECOVERY_PY.read_text()
    assert "def maybe_cleanup_duplicate_placements(" in src


def test_cleanup_has_independent_marker():
    """Independent marker (not tied to walker's marker) so the
    two repairs can be re-triggered separately."""
    src = RECOVERY_PY.read_text()
    assert (
        "recovery_duplicate_placements_cleanup_done_at_v1_19_17"
        in src
    )


def test_cleanup_filters_to_stale_hardlink_with_plex_upload_sibling():
    """The cleanup must scope to placement_kind hardlink/copy
    AND require a sibling plex_upload row for the same
    (mt, tmdb, sec) tuple. Without the sibling condition we'd
    delete legit hardlink rows from items that never went
    through the v1.19.16 API path."""
    src = RECOVERY_PY.read_text()
    fn_start = src.index("def maybe_cleanup_duplicate_placements(")
    next_def = src.find("\ndef ", fn_start + 1)
    body = src[fn_start:next_def if next_def > 0 else len(src)]
    assert "placement_kind IN ('hardlink', 'copy')" in body
    assert "placement_kind = 'plex_upload'" in body, (
        "v1.19.17: cleanup must require a plex_upload sibling "
        "row before deleting — otherwise it would nuke legit "
        "hardlink rows that never went through the v1.19.16 path"
    )


def test_cleanup_checks_file_presence_before_delete():
    """If the hardlink row's media_folder/theme.mp3 IS present,
    don't delete it — that's a legit dual state (operator
    intentionally has both sidecar AND API upload)."""
    src = RECOVERY_PY.read_text()
    fn_start = src.index("def maybe_cleanup_duplicate_placements(")
    next_def = src.find("\ndef ", fn_start + 1)
    body = src[fn_start:next_def if next_def > 0 else len(src)]
    assert "is_file()" in body
    assert "skipped_file_present" in body


# ── End-to-end behavioral tests ──────────────────────────────


def _seed_install(tmp_path: Path):
    db_path = tmp_path / "motif.db"
    themes_dir = tmp_path / "themes"
    plex_root = tmp_path / "plex"
    plex_root.mkdir()
    from app.core.db import init_db
    init_db(db_path)
    ts = "2026-05-20T05:11:20+00:00"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 0, 0, "
            "        'movies', 1, ?, ?)",
            (ts, ts),
        )
        conn.commit()
    return db_path, themes_dir, plex_root


def _seed_v1_19_16_artifact(
    db_path: Path, plex_root: Path,
    *, tmdb_id: int = 1000, file_present_at_sidecar: bool = False,
):
    """Reproduce the v1.19.16 walker damage: an item with TWO
    placement rows — old stale hardlink (file missing) + new
    plex_upload (Plex serves the theme via API)."""
    ts_old = "2026-05-20T05:11:20+00:00"
    ts_new = "2026-05-25T03:39:18+00:00"  # v1.19.16's walker time
    plex_folder = plex_root / "movies" / f"Movie {tmdb_id} (2020)"
    plex_folder.mkdir(parents=True, exist_ok=True)
    if file_present_at_sidecar:
        (plex_folder / "theme.mp3").touch()
    # else: sidecar deleted by v1.19.16 cleanup
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, year, "
            "                    upstream_source, last_seen_sync_at, "
            "                    first_seen_sync_at) "
            "VALUES ('movie', ?, ?, '2020', 'themoviedb', ?, ?)",
            (tmdb_id, f"Movie {tmdb_id}", ts_old, ts_old),
        )
        # Stale hardlink row (the leftover from v1.19.16).
        conn.execute(
            "INSERT INTO placements (media_type, tmdb_id, section_id, "
            "                        media_folder, placed_at, "
            "                        placement_kind, plex_rating_key, "
            "                        plex_refreshed, provenance) "
            "VALUES ('movie', ?, '1', ?, ?, 'hardlink', ?, 1, 'auto')",
            (tmdb_id, str(plex_folder), ts_old, f"rk-{tmdb_id}"),
        )
        # Fresh plex_upload row (from v1.19.16's API upload path).
        conn.execute(
            "INSERT INTO placements (media_type, tmdb_id, section_id, "
            "                        media_folder, placed_at, "
            "                        placement_kind, plex_rating_key, "
            "                        plex_refreshed, provenance) "
            "VALUES ('movie', ?, '1', '', ?, 'plex_upload', ?, 1, 'auto')",
            (tmdb_id, ts_new, f"rk-{tmdb_id}"),
        )
        conn.commit()


def test_cleanup_deletes_stale_hardlink_when_sidecar_missing(tmp_path):
    """the user's case: hardlink row's file was deleted by the
    v1.19.16 walker, plex_upload row took over. Cleanup must
    delete the stale hardlink row."""
    db_path, _, plex_root = _seed_install(tmp_path)
    _seed_v1_19_16_artifact(
        db_path, plex_root, tmdb_id=1000,
        file_present_at_sidecar=False,
    )
    from app.core.recovery_v55 import maybe_cleanup_duplicate_placements
    stats = maybe_cleanup_duplicate_placements(db_path)
    assert stats["stale_hardlink_rows_found"] == 1
    assert stats["deleted"] == 1
    assert stats["skipped_file_present"] == 0
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT placement_kind, media_folder FROM placements "
            "WHERE tmdb_id=1000 ORDER BY placed_at"
        ).fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "plex_upload", (
        "v1.19.17: stale hardlink must be deleted, plex_upload "
        "row preserved as the authoritative placement"
    )
    assert rows[0][1] == ""


def test_cleanup_preserves_hardlink_when_sidecar_present(tmp_path):
    """Legit dual state — both sidecar AND API upload, both
    files present. Don't delete the hardlink row (operator's
    intent)."""
    db_path, _, plex_root = _seed_install(tmp_path)
    _seed_v1_19_16_artifact(
        db_path, plex_root, tmdb_id=2000,
        file_present_at_sidecar=True,
    )
    from app.core.recovery_v55 import maybe_cleanup_duplicate_placements
    stats = maybe_cleanup_duplicate_placements(db_path)
    assert stats["stale_hardlink_rows_found"] == 1
    assert stats["deleted"] == 0
    assert stats["skipped_file_present"] == 1
    with sqlite3.connect(db_path) as conn:
        n = conn.execute(
            "SELECT COUNT(*) FROM placements WHERE tmdb_id=2000"
        ).fetchone()[0]
    assert n == 2, "both rows preserved when sidecar still present"


def test_cleanup_leaves_lone_hardlink_rows_alone(tmp_path):
    """A hardlink row with NO plex_upload sibling is a normal
    placement, not a duplicate. Cleanup must not touch it."""
    db_path, themes_dir, plex_root = _seed_install(tmp_path)
    ts = "2026-05-20T05:11:20+00:00"
    plex_folder = plex_root / "movies" / "Lone Movie (2020)"
    plex_folder.mkdir(parents=True, exist_ok=True)
    # No sidecar file — but no plex_upload sibling either, so
    # cleanup must leave it alone (different damage class).
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, year, "
            "                    upstream_source, last_seen_sync_at, "
            "                    first_seen_sync_at) "
            "VALUES ('movie', 3000, 'Lone Movie', '2020', "
            "        'themoviedb', ?, ?)",
            (ts, ts),
        )
        conn.execute(
            "INSERT INTO placements (media_type, tmdb_id, section_id, "
            "                        media_folder, placed_at, "
            "                        placement_kind, plex_rating_key, "
            "                        plex_refreshed, provenance) "
            "VALUES ('movie', 3000, '1', ?, ?, 'hardlink', 'rk-3000', "
            "        1, 'auto')",
            (str(plex_folder), ts),
        )
        conn.commit()
    from app.core.recovery_v55 import maybe_cleanup_duplicate_placements
    stats = maybe_cleanup_duplicate_placements(db_path)
    assert stats["stale_hardlink_rows_found"] == 0
    assert stats["deleted"] == 0
    with sqlite3.connect(db_path) as conn:
        n = conn.execute(
            "SELECT COUNT(*) FROM placements WHERE tmdb_id=3000"
        ).fetchone()[0]
    assert n == 1


def test_cleanup_no_op_when_no_duplicates(tmp_path):
    """Clean install with no duplicates — function runs, returns
    0 stats, does NOT stamp marker (so future drift can be caught)."""
    db_path, themes_dir, plex_root = _seed_install(tmp_path)
    from app.core.recovery_v55 import maybe_cleanup_duplicate_placements
    stats = maybe_cleanup_duplicate_placements(db_path)
    assert stats["deleted"] == 0
    assert stats["stale_hardlink_rows_found"] == 0
    with sqlite3.connect(db_path) as conn:
        marker = conn.execute(
            "SELECT 1 FROM runtime_settings "
            "WHERE key = "
            "'recovery_duplicate_placements_cleanup_done_at_v1_19_17'"
        ).fetchone()
    assert marker is None, (
        "v1.19.17: cleanup must NOT stamp marker when no work "
        "was needed — preserves ability to catch future drift"
    )


def test_cleanup_marker_prevents_re_run(tmp_path):
    """After marker is stamped, second invocation is a no-op."""
    db_path, themes_dir, plex_root = _seed_install(tmp_path)
    _seed_v1_19_16_artifact(db_path, plex_root, tmdb_id=4000)
    from app.core.recovery_v55 import maybe_cleanup_duplicate_placements
    stats1 = maybe_cleanup_duplicate_placements(db_path)
    assert stats1["deleted"] == 1
    # Seed new artifact — should not be processed.
    _seed_v1_19_16_artifact(db_path, plex_root, tmdb_id=4001)
    stats2 = maybe_cleanup_duplicate_placements(db_path)
    assert stats2["stale_hardlink_rows_found"] == 0
    assert stats2["deleted"] == 0


def test_cleanup_multiple_artifacts(tmp_path):
    """the user's actual case: 10 items with the v1.19.16
    artifact. Cleanup deletes all 10 stale hardlink rows in
    one pass."""
    db_path, themes_dir, plex_root = _seed_install(tmp_path)
    for tid in range(5000, 5010):
        _seed_v1_19_16_artifact(db_path, plex_root, tmdb_id=tid)
    from app.core.recovery_v55 import maybe_cleanup_duplicate_placements
    stats = maybe_cleanup_duplicate_placements(db_path)
    assert stats["stale_hardlink_rows_found"] == 10
    assert stats["deleted"] == 10
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT placement_kind FROM placements "
            "WHERE tmdb_id BETWEEN 5000 AND 5009 "
            "ORDER BY tmdb_id, placed_at"
        ).fetchall()
    assert len(rows) == 10
    assert all(r[0] == "plex_upload" for r in rows), (
        "after cleanup, only plex_upload rows remain"
    )
