"""v0.50.89 — holistic-audit Batch 1: edition-scope bleeds.

The full-codebase audit found the SAME recurring bug class (a write/read
scoped by (media_type, tmdb_id, section_id) but missing edition_key) in 5
places that predate or sit just outside the v1.21.52+ edition-scoping arc:

1. sections.py `migrate_v1_14_94_colon_folders` — the colon-folder rename
   migration's UPDATE had no edition_key filter, so migrating one edition's
   legacy folder silently overwrote a sibling edition's file_path too.
2. adopt.py `_do_adopt`'s trailing `plex_items.theme_id` UPDATE had no
   edition_key filter, re-linking every sibling edition's plex_items row.
3. adopt.py `_verify_adopt_state`'s diagnostic read-back had no edition_key
   filter, so it could log a sibling edition's state instead of the row
   just adopted.
4. api.py `_is_p_row_for_section` (the P-row/slot-theft guard) had no
   edition_key filter on either its plex_items match or its NOT
   EXISTS(placements) sub-check, so a sibling edition's placement masked a
   genuine P-row.
5. recovery_v55.py `maybe_recover_lost_adopts` (dead code today, but
   re-wireable) read/wrote local_files with no edition_key filter.
"""
from __future__ import annotations

import sqlite3

import pytest

from app.core import db as db_module
from app.core.db import init_db, get_conn
from app.core.events import now_iso

NOW = now_iso()


# ── 1. sections.py colon-folder migration ───────────────────────────────


def _setup_migration_db(tmp_path):
    db_path = tmp_path / "motif.db"
    db_module.init_db(db_path)
    with db_module.get_conn(db_path) as conn:
        conn.execute(
            "INSERT INTO plex_sections "
            "(section_id, title, type, included, "
            " discovered_at, last_seen_at, themes_subdir) "
            "VALUES ('1', 'Movies', 'movie', 1, "
            "        datetime('now'), datetime('now'), 'movies')"
        )
        conn.execute(
            "INSERT INTO themes "
            "(media_type, tmdb_id, title, year, upstream_source, "
            " youtube_url, last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('movie', 280, 'Terminator 2: Judgment Day', 1991, "
            "        'themoviedb', 'https://example.com/x', "
            "        datetime('now'), datetime('now'))"
        )
    return db_path


def test_colon_migration_does_not_bleed_across_editions(tmp_path):
    from app.core.sections import migrate_v1_14_94_colon_folders

    db_path = _setup_migration_db(tmp_path)
    themes_dir = tmp_path / "themes"
    # Standard edition: legacy (pre-v1.14.94) folder present on disk — this
    # is the one row that actually needs renaming.
    std_legacy = themes_dir / "movies" / "Terminator 2- Judgment Day (1991)"
    std_legacy.mkdir(parents=True)
    (std_legacy / "theme.mp3").write_bytes(b"std")
    # Extended edition: created later (edition support postdates the
    # v1.14.94 colon fix, so any real edition folder already uses the
    # correct shape) — its OWN folder, never touched by the rename.
    ext_dir = themes_dir / "movies" / "Terminator 2 - Judgment Day (1991) {edition-extended}"
    ext_dir.mkdir(parents=True)
    (ext_dir / "theme.mp3").write_bytes(b"ext")

    with get_conn(db_path) as conn:
        conn.execute(
            "INSERT INTO local_files "
            "(media_type, tmdb_id, section_id, edition_key, file_path, "
            " downloaded_at, source_video_id) "
            "VALUES ('movie', 280, '1', '', "
            "'movies/Terminator 2- Judgment Day (1991)/theme.mp3', "
            "datetime('now'), 'vidstd')"
        )
        conn.execute(
            "INSERT INTO local_files "
            "(media_type, tmdb_id, section_id, edition_key, file_path, "
            " downloaded_at, source_video_id) "
            "VALUES ('movie', 280, '1', 'extended', "
            "'movies/Terminator 2 - Judgment Day (1991) {edition-extended}/theme.mp3', "
            "datetime('now'), 'vidext')"
        )

    migrate_v1_14_94_colon_folders(db_path, themes_dir)

    with get_conn(db_path) as conn:
        rows = {
            r["edition_key"]: r["file_path"]
            for r in conn.execute(
                "SELECT edition_key, file_path FROM local_files "
                "WHERE media_type='movie' AND tmdb_id=280"
            ).fetchall()
        }
    assert rows[""] == "movies/Terminator 2 - Judgment Day (1991)/theme.mp3"
    assert rows["extended"] == (
        "movies/Terminator 2 - Judgment Day (1991) {edition-extended}/theme.mp3"
    ), (
        "v0.50.89: the extended edition's file_path (already correct, never "
        "renamed) must be untouched by the standard edition's migration — "
        "pre-fix the trailing UPDATE had no edition_key filter and "
        "clobbered it with the standard edition's new relative path"
    )


# ── 2 + 3. adopt.py _do_adopt + _verify_adopt_state ─────────────────────


@pytest.fixture
def adopt_settings(tmp_path):
    from app.config import Settings
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    (tmp_path / "themes").mkdir(parents=True, exist_ok=True)
    s._cfg.paths.themes_dir = str(tmp_path / "themes")
    init_db(s.db_path)
    return s


def test_do_adopt_plex_items_update_scoped_to_edition(adopt_settings, tmp_path):
    s = adopt_settings
    db = s.db_path
    tmdb_id = 120
    ext_folder_name = "LotR (2001) {edition-extended}"
    media_folder = tmp_path / "Movies" / ext_folder_name
    media_folder.mkdir(parents=True, exist_ok=True)
    src = media_folder / "theme.mp3"
    src.write_bytes(b"ID3audio!")

    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        cur = conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, year,"
            " upstream_source, last_seen_sync_at, first_seen_sync_at)"
            " VALUES ('movie',?,'LotR','2001','imdb',?,?)", (tmdb_id, NOW, NOW))
        tid = cur.lastrowid
        # Sibling STANDARD-edition plex_items row, same guid_tmdb/section.
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type,"
            " theme_id, guid_tmdb, title, year, has_theme, local_theme_file,"
            " folder_path, edition_key, plex_independent_theme,"
            " plex_theme_verified_ok, first_seen_at, last_seen_at)"
            " VALUES ('rk-std','1','movie',NULL,?,'LotR',2001,0,0,"
            " '/data/Movies/LotR (2001)','',0,1,?,?)",
            (tmdb_id, NOW, NOW))
        # Target EXTENDED-edition plex_items row.
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type,"
            " theme_id, guid_tmdb, title, year, has_theme, local_theme_file,"
            " folder_path, edition_key, plex_independent_theme,"
            " plex_theme_verified_ok, first_seen_at, last_seen_at)"
            " VALUES ('rk-ext','1','movie',NULL,?,'LotR',2001,0,0,?,"
            " 'extended',0,1,?,?)",
            (tmdb_id, str(media_folder), NOW, NOW))
        conn.commit()

    finding = {
        "section_id": "1",
        "section_type": "movie",
        "finding_kind": "content_mismatch",
        "theme_id": tid,
        "file_path": str(src),
        "file_sha256": "deadbeef" * 8,
        "file_size": src.stat().st_size,
        "media_folder": str(media_folder),
    }

    from app.core.adopt import _do_adopt
    outcome = _do_adopt(db, finding, s, decided_by="testadmin")

    with get_conn(db) as conn:
        std_pi = conn.execute(
            "SELECT theme_id FROM plex_items WHERE rating_key='rk-std'"
        ).fetchone()
        ext_pi = conn.execute(
            "SELECT theme_id FROM plex_items WHERE rating_key='rk-ext'"
        ).fetchone()

    assert ext_pi["theme_id"] == outcome["theme_id"], (
        "the adopting edition's plex_items row must be re-linked"
    )
    assert std_pi["theme_id"] is None, (
        "v0.50.89: a sibling STANDARD edition's plex_items row must NOT "
        "be re-linked by adopting the EXTENDED edition — pre-fix the "
        "trailing UPDATE had no edition_key filter and bled to every "
        "sibling edition in the section"
    )


def test_verify_adopt_state_reads_back_the_adopted_edition_only(
    adopt_settings, tmp_path,
):
    """_verify_adopt_state (called right after _do_adopt) must report the
    ADOPTED edition's row, not a sibling's, when both exist."""
    s = adopt_settings
    db = s.db_path
    tmdb_id = 121
    ext_folder_name = "Watchmen (2009) {edition-directors cut}"
    media_folder = tmp_path / "Movies" / ext_folder_name
    media_folder.mkdir(parents=True, exist_ok=True)
    src = media_folder / "theme.mp3"
    src.write_bytes(b"ID3audio2!")

    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        cur = conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, year,"
            " upstream_source, last_seen_sync_at, first_seen_sync_at)"
            " VALUES ('movie',?,'Watchmen','2009','imdb',?,?)", (tmdb_id, NOW, NOW))
        tid = cur.lastrowid
        # Pre-existing STANDARD-edition local_files row (different theme_id,
        # different source_kind) — must NOT be what the diagnostic reports.
        conn.execute(
            "INSERT INTO local_files (media_type, tmdb_id, section_id,"
            " edition_key, theme_id, file_path, downloaded_at,"
            " source_video_id, source_kind)"
            " VALUES ('movie',?,'1','',9999,'movies/Watchmen (2009)/theme.mp3',"
            " ?, 'vidstd', 'themerrdb')", (tmdb_id, NOW))
        conn.commit()

    finding = {
        "section_id": "1",
        "section_type": "movie",
        "finding_kind": "content_mismatch",
        "theme_id": tid,
        "file_path": str(src),
        "file_sha256": "cafebabe" * 8,
        "file_size": src.stat().st_size,
        "media_folder": str(media_folder),
    }

    from app.core.adopt import _do_adopt, _verify_adopt_state
    from app.core.editions import edition_key_for_folder
    outcome = _do_adopt(db, finding, s, decided_by="testadmin")
    verify = _verify_adopt_state(
        db, media_type=outcome["media_type"], tmdb_id=outcome["tmdb_id"],
        section_id="1", edition_key=edition_key_for_folder(str(media_folder)),
    )
    assert verify["local_files"]["source_kind"] == "adopt", (
        "v0.50.89: the diagnostic must read back the JUST-ADOPTED edition's "
        "row (source_kind='adopt'), not the sibling standard edition's "
        "pre-existing 'themerrdb' row"
    )


# ── 4. api.py _is_p_row_for_section ─────────────────────────────────────


def test_is_p_row_for_section_scoped_by_edition(tmp_path):
    from app.config import Settings
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    db = s.db_path
    from app.web.api import _is_p_row_for_section

    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        # Standard edition: motif HAS a placement (not a P-row).
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type,"
            " guid_tmdb, title, year, has_theme, local_theme_file,"
            " folder_path, edition_key, plex_independent_theme,"
            " plex_theme_verified_ok, first_seen_at, last_seen_at)"
            " VALUES ('rk-std','1','movie',500,'T',2020,1,0,"
            " '/data/T','',0,1,?,?)", (NOW, NOW))
        conn.execute(
            "INSERT INTO placements (media_type, tmdb_id, section_id,"
            " edition_key, media_folder, placement_kind, placed_at)"
            " VALUES ('movie',500,'1','','/data/T','hardlink',?)", (NOW,))
        # Extended edition: genuinely P — Plex serves its own theme, motif
        # owns NO placement for this edition.
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type,"
            " guid_tmdb, title, year, has_theme, local_theme_file,"
            " folder_path, edition_key, plex_independent_theme,"
            " plex_theme_verified_ok, first_seen_at, last_seen_at)"
            " VALUES ('rk-ext','1','movie',500,'T',2020,1,0,"
            " '/data/T {edition-extended}','extended',0,1,?,?)", (NOW, NOW))
        conn.commit()

        std_is_p = _is_p_row_for_section(
            conn, media_type="movie", tmdb_id=500, section_id="1",
            edition_key="",
        )
        ext_is_p = _is_p_row_for_section(
            conn, media_type="movie", tmdb_id=500, section_id="1",
            edition_key="extended",
        )

    assert std_is_p is False, "standard edition has a motif placement — not P"
    assert ext_is_p is True, (
        "v0.50.89: the extended edition genuinely has no motif placement "
        "and Plex serves its own theme — must be classified P. Pre-fix, "
        "the standard edition's sibling placement masked this via an "
        "edition-blind NOT EXISTS(placements) check"
    )


# ── 5. recovery_v55.py maybe_recover_lost_adopts ────────────────────────


def test_recover_lost_adopts_scoped_to_standard_edition(tmp_path):
    """The historical adopt-event signal predates edition_key entirely, so
    the walker must only ever touch the '' (standard) edition's row — never
    a sibling edition that's since appeared for the same tmdb_id/section."""
    from app.core.recovery_v55 import maybe_recover_lost_adopts
    import hashlib
    import json

    db_path = tmp_path / "motif.db"
    init_db(db_path)
    themes_dir = tmp_path / "themes"
    themes_dir.mkdir()
    theme_file = themes_dir / "theme.mp3"
    theme_file.write_bytes(b"adopted content")
    sha = hashlib.sha256(b"adopted content").hexdigest()

    with get_conn(db_path) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, included,"
            " discovered_at, last_seen_at, themes_subdir)"
            " VALUES ('1', 'Movies', 'movie', 1, ?, ?, 'movies')", (NOW, NOW))
        conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, year,"
            " upstream_source, youtube_url, last_seen_sync_at,"
            " first_seen_sync_at) VALUES ('movie', 700, 'Test', 2020,"
            " 'themoviedb', 'https://example.com/x', ?, ?)", (NOW, NOW))
        # Standard-edition row: themerrdb, matching sha — should reclassify.
        conn.execute(
            "INSERT INTO local_files (media_type, tmdb_id, section_id,"
            " edition_key, file_path, downloaded_at, source_video_id,"
            " source_kind) VALUES ('movie', 700, '1', '', 'theme.mp3',"
            " ?, 'vid', 'themerrdb')", (NOW,))
        # Sibling edition row: ALSO themerrdb with the SAME sha (plausible —
        # both editions could share byte-identical audio) — must be left
        # alone since the historical adopt event can't be proven to be
        # about this edition.
        conn.execute(
            "INSERT INTO local_files (media_type, tmdb_id, section_id,"
            " edition_key, file_path, downloaded_at, source_video_id,"
            " source_kind) VALUES ('movie', 700, '1', 'extended',"
            " 'theme.mp3', ?, 'vid', 'themerrdb')", (NOW,))
        conn.execute(
            "INSERT INTO events (ts, level, component, media_type, tmdb_id,"
            " message, detail) VALUES (?, 'INFO', 'adopt', 'movie', 700,"
            " 'Inline adopt of sidecar at /data/X', ?)",
            (NOW, json.dumps({"sha256": sha, "section_id": "1"})),
        )

    maybe_recover_lost_adopts(db_path, themes_dir)

    with get_conn(db_path) as conn:
        rows = {
            r["edition_key"]: r["source_kind"]
            for r in conn.execute(
                "SELECT edition_key, source_kind FROM local_files "
                "WHERE media_type='movie' AND tmdb_id=700"
            ).fetchall()
        }
    assert rows[""] == "adopt", "the standard edition must be reclassified"
    assert rows["extended"] == "themerrdb", (
        "v0.50.89: a sibling edition must NOT be reclassified by a "
        "historical adopt event that predates edition tracking — pre-fix "
        "the UPDATE had no edition_key filter"
    )
