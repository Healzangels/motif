"""v0.51.250 — reconcile_placement_paths sees theme_id-linked placements.

The v1.22.17 class, one more time: that tag widened six sync.py joins with the
`pi.guid_tmdb = X OR pi.theme_id = t.id` arm for AniDB/HAMA anime rows (whose
guid_tmdb is NULL), and this join was missed. A placement on such a row was
INVISIBLE to the folder-move reconcile: Sonarr renames the show folder, the
enum updates plex_items.folder_path, but placements.media_folder never follows
— verify_placement_health then stats the dead old path, theme_present goes 0,
and the row shows a permanent false "broken placement" red PL while Plex is
actually still serving the moved sidecar.

Measured on the operator's library before fixing: 32 of 2,387 placements were
reachable ONLY via theme_id (the corrected query — the first attempt asked a
section-level question and returned 2387/2387, which meant nothing).

Both halves must widen together: the divergence-detect JOIN and the
plex_paths_by_item guard set. Widening only the join re-arms the v1.18.49
churn loop for this cohort — every theme_id row would look "moved" on every
enum because the guard set never contained its key.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from app.core.db import init_db
from app.core.plex_enum import reconcile_placement_paths

REPO = Path(__file__).resolve().parent.parent
NOW = "2026-08-05T00:00:00"
OLD_FOLDER = "/data/anime/Excalibur Academy (2023)"
NEW_FOLDER = "/data/anime/The Demon Sword Master of Excalibur Academy (2023)"


def _seed_anime(db, *, plex_folder: str, placement_folder: str,
                extra_rk_folder: str | None = None):
    """A HAMA-matched show: guid_tmdb NULL, linked to its theme only via
    plex_items.theme_id — the cohort the old join could not see."""
    init_db(db)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('7','Anime','show',1,0,'anime',1,?,?)", (NOW, NOW))
        conn.execute(
            "INSERT INTO themes (id, media_type, tmdb_id, title,"
            " upstream_source, last_seen_sync_at, first_seen_sync_at)"
            " VALUES (9,'tv',136840,'Excalibur Academy','themoviedb',?,?)",
            (NOW, NOW))
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type,"
            " theme_id, guid_tmdb, title, has_theme, local_theme_file,"
            " folder_path, edition_key, first_seen_at, last_seen_at)"
            " VALUES ('rk-anime','7','show',9,NULL,'Excalibur Academy',1,1,"
            " ?,'',?,?)", (plex_folder, NOW, NOW))
        if extra_rk_folder is not None:
            conn.execute(
                "INSERT INTO plex_items (rating_key, section_id, media_type,"
                " theme_id, guid_tmdb, title, has_theme, local_theme_file,"
                " folder_path, edition_key, first_seen_at, last_seen_at)"
                " VALUES ('rk-anime-2','7','show',9,NULL,'Excalibur Academy',"
                " 1,1,?,'',?,?)", (extra_rk_folder, NOW, NOW))
        conn.execute(
            "INSERT INTO placements (media_type, tmdb_id, section_id,"
            " edition_key, media_folder, placed_at, placement_kind,"
            " plex_refreshed, provenance)"
            " VALUES ('tv',136840,'7','',?,?, 'hardlink',1,'auto')",
            (placement_folder, NOW))
        conn.commit()


def test_theme_id_linked_placement_reconciles_after_a_folder_move(tmp_path):
    """THE fix. Sonarr renamed the folder; the enum updated folder_path; the
    placement must follow. Pre-fix: enqueued == 0 forever."""
    db = tmp_path / "m.db"
    _seed_anime(db, plex_folder=NEW_FOLDER, placement_folder=OLD_FOLDER)

    enqueued = reconcile_placement_paths(db)

    assert enqueued == 1, "the guid_tmdb-NULL placement was invisible pre-fix"
    with sqlite3.connect(db) as conn:
        folder = conn.execute(
            "SELECT media_folder FROM placements WHERE tmdb_id=136840"
        ).fetchone()[0]
        job = conn.execute(
            "SELECT payload FROM jobs WHERE job_type='place' AND tmdb_id=136840"
            " AND status='pending'").fetchone()
    assert folder == NEW_FOLDER, "media_folder must follow the rename"
    assert job is not None and "folder_relocated" in job[0]


def test_guard_still_skips_when_plex_reports_the_old_folder_elsewhere(tmp_path):
    """The v1.18.49 churn protection, extended to the new cohort. Plex lists a
    SECOND ratingKey at the placement's folder (multi-rk case): the placement
    is still valid and must NOT be moved. Widening the join without the guard
    key would relocate it on every enum — the exact loop v1.18.49 fixed."""
    db = tmp_path / "m.db"
    _seed_anime(db, plex_folder=NEW_FOLDER, placement_folder=OLD_FOLDER,
                extra_rk_folder=OLD_FOLDER)

    enqueued = reconcile_placement_paths(db)

    assert enqueued == 0, (
        "old folder is still Plex-reported under rk-anime-2 — moving it is the "
        "v1.18.49 churn class, re-armed for the theme_id cohort")


def test_second_run_is_a_no_op(tmp_path):
    """Idempotence — the anti-churn property stated end-to-end. After the fix
    run, a second reconcile must find nothing."""
    db = tmp_path / "m.db"
    _seed_anime(db, plex_folder=NEW_FOLDER, placement_folder=OLD_FOLDER)
    assert reconcile_placement_paths(db) == 1
    assert reconcile_placement_paths(db) == 0, (
        "second pass re-moved the row — unbounded per-enum churn")


def test_a_matching_theme_id_placement_is_untouched(tmp_path):
    """No-move no-op: folder_path == media_folder must produce nothing, or
    every healthy anime row would churn."""
    db = tmp_path / "m.db"
    _seed_anime(db, plex_folder=NEW_FOLDER, placement_folder=NEW_FOLDER)
    assert reconcile_placement_paths(db) == 0


# ── structural: both queries carry the arm ───────────────────────────────

def _reconcile_src() -> str:
    s = (REPO / "app" / "core" / "plex_enum.py").read_text()
    i = s.index("def reconcile_placement_paths(")
    j = s.index("\ndef ", i + 1)
    return "\n".join(ln for ln in s[i:j].splitlines()
                     if not ln.lstrip().startswith(("#", "--")))


def test_divergence_join_carries_the_theme_id_arm():
    src = _reconcile_src()
    assert "pi.theme_id = t.id" in src, (
        "the theme_id OR-arm is gone — guid_tmdb-NULL placements are invisible "
        "to the folder-move reconcile again (the v1.22.17 class)")


def test_guard_set_resolves_the_effective_id():
    """The other half. The guard SELECT must key by COALESCE(guid_tmdb, theme's
    tmdb) — a bare guid_tmdb key re-arms the churn loop for the new cohort."""
    src = _reconcile_src()
    assert "COALESCE(pi.guid_tmdb, t.tmdb_id)" in src, (
        "guard set keyed by raw guid_tmdb — theme_id rows always look moved")
