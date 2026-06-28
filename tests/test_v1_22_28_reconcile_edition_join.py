"""v1.22.28 — behavioral coverage for reconcile_placement_paths' edition JOIN.

The v1.22.0 rollover comment noted this as a DEFERRED coverage follow-up:
reconcile_placement_paths' v1.21.94 edition JOIN (`pi.edition_key = p.edition_key`)
was only guarded by the v1.21.94 edition-blind-read lint + a v1.21.99 unit test —
no behavioral test proved a stale placement reconciles to ITS OWN edition's
folder rather than cross-producing onto a sibling edition's tagged folder.

Pre-fix (edition-blind join): a '' (standard) placement whose folder Plex no
longer reports cross-joined every edition's plex_items row, so it could be
"relocated" onto the {edition-X}-tagged Extended folder — a wrong-edition write.
This pins the fix end-to-end.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from app.core.db import init_db
from app.core.plex_enum import reconcile_placement_paths

REPO = Path(__file__).resolve().parent.parent
PLEX_ENUM_PY = (REPO / "app" / "core" / "plex_enum.py").read_text()
NOW = "2026-06-07T00:00:00"
STD_FOLDER = "/data/m/Two Towers"
EXT_FOLDER = "/data/m/Two Towers {edition-Extended}"
STALE_FOLDER = "/data/m/Two Towers OLD PATH"


def _seed(db):
    init_db(db)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        conn.execute(
            "INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source,"
            " last_seen_sync_at, first_seen_sync_at) VALUES (1,'movie',500,'TT',"
            " 'imdb',?,?)", (NOW, NOW))
        for rk, ed, folder in (("rk-std", "", STD_FOLDER),
                               ("rk-ext", "extended", EXT_FOLDER)):
            conn.execute(
                "INSERT INTO plex_items (rating_key, section_id, media_type,"
                " theme_id, guid_tmdb, title, year, has_theme, local_theme_file,"
                " folder_path, edition_key, first_seen_at, last_seen_at)"
                " VALUES (?, '1','movie',1,500,'TT',2002,1,1,?,?,?,?)",
                (rk, folder, ed, NOW, NOW))
        # The standard ('') placement's folder is STALE (Plex no longer reports
        # it) → it's a genuine relocate candidate, NOT masked by the
        # old_folder-in-current-paths guard.
        conn.execute(
            "INSERT INTO placements (media_type, tmdb_id, section_id,"
            " edition_key, media_folder, placed_at, placement_kind,"
            " plex_rating_key, plex_refreshed, provenance)"
            " VALUES ('movie',500,'1','',?,?, 'hardlink', NULL, 1, 'auto')",
            (STALE_FOLDER, NOW))
        conn.commit()


def test_stale_placement_reconciles_to_its_own_edition_folder(tmp_path):
    db = tmp_path / "m.db"
    _seed(db)

    enqueued = reconcile_placement_paths(db)

    assert enqueued == 1, "the stale '' placement relocates exactly once"
    with sqlite3.connect(db) as conn:
        folder = conn.execute(
            "SELECT media_folder FROM placements WHERE tmdb_id=500"
            " AND edition_key=''").fetchone()[0]
        # No placement landed on the Extended (sibling-edition) folder.
        on_ext = conn.execute(
            "SELECT COUNT(*) FROM placements WHERE media_folder=?",
            (EXT_FOLDER,)).fetchone()[0]
        # The relocate place job carries the right edition.
        job = conn.execute(
            "SELECT payload FROM jobs WHERE job_type='place' AND tmdb_id=500"
        ).fetchone()
    assert folder == STD_FOLDER, (
        "v1.22.28: the '' placement must reconcile to the STANDARD edition's "
        f"folder, not the Extended one — got {folder!r}")
    assert on_ext == 0, (
        "v1.22.28: the edition JOIN must keep the '' placement off the sibling "
        "Extended folder")
    assert job is not None and '"edition_key": ""' in job[0], (
        "the relocate force-place job carries the placement's edition")


def test_no_spurious_move_when_folder_already_correct(tmp_path):
    """Control: a placement already at its own edition's current folder is NOT
    treated as a move (the cross-edition mismatch must not manufacture one)."""
    db = tmp_path / "m.db"
    init_db(db)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        conn.execute(
            "INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source,"
            " last_seen_sync_at, first_seen_sync_at) VALUES (1,'movie',500,'TT',"
            " 'imdb',?,?)", (NOW, NOW))
        for rk, ed, folder in (("rk-std", "", STD_FOLDER),
                               ("rk-ext", "extended", EXT_FOLDER)):
            conn.execute(
                "INSERT INTO plex_items (rating_key, section_id, media_type,"
                " theme_id, guid_tmdb, title, year, has_theme, local_theme_file,"
                " folder_path, edition_key, first_seen_at, last_seen_at)"
                " VALUES (?, '1','movie',1,500,'TT',2002,1,1,?,?,?,?)",
                (rk, folder, ed, NOW, NOW))
        # '' placement already at the standard folder (correct).
        conn.execute(
            "INSERT INTO placements (media_type, tmdb_id, section_id,"
            " edition_key, media_folder, placed_at, placement_kind,"
            " plex_rating_key, plex_refreshed, provenance)"
            " VALUES ('movie',500,'1','',?,?, 'hardlink', NULL, 1, 'auto')",
            (STD_FOLDER, NOW))
        conn.commit()

    assert reconcile_placement_paths(db) == 0, (
        "v1.22.28: a correctly-placed '' edition must not be 'moved' toward the "
        "Extended folder (pre-edition-JOIN cross-product bug)")


def test_join_pins_edition_key():
    i = PLEX_ENUM_PY.index("def reconcile_placement_paths(")
    body = PLEX_ENUM_PY[i:i + 4500]
    assert "AND pi.edition_key = p.edition_key" in body, (
        "v1.21.94 edition JOIN must stay in reconcile_placement_paths")


def test_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
