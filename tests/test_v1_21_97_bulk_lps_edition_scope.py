"""v1.21.97 — BULK LET PLEX SERVE is edition-scoped (like per-row v1.21.67).

The per-row /unplace endpoint scopes the placement delete to the clicked
edition (v1.21.67), but `_bulk_lps_run` deleted placements by (media_type,
tmdb_id, section_id) with NO edition filter — so bulk-LPS on ONE edition of a
multi-edition title unplaced EVERY edition's placement in that section (e.g.
LotR Theatrical select → Extended's separate theme unlinked too). The JS now
sends each row's edition_key; the handler scopes the worklist SELECT, the
placements DELETE, and the local_files/plex_items stamps to it. Absent
edition_key = legacy title-wide (behavior-preserving for old callers).
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest


NOW = "2026-06-04T00:00:00Z"


def _settings(tmp_path):
    from app.config import Settings
    from app.core.db import init_db
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    return s


def _seed(db):
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        # youtube_url='' so the probe stage skips (no network).
        conn.execute(
            "INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source,"
            " last_seen_sync_at, first_seen_sync_at, youtube_url) VALUES"
            " (10,'movie',500,'Q','imdb',?,?,'')", (NOW, NOW))
        for rk, ek, folder in (
            ("t1", "theatrical", "/d/Q (2001) {edition-Theatrical}"),
            ("e1", "extended", "/d/Q (2001) {edition-Extended}"),
        ):
            conn.execute(
                "INSERT INTO plex_items (rating_key, section_id, media_type,"
                " theme_id, guid_tmdb, title, edition_key, folder_path, has_theme,"
                " local_theme_file, first_seen_at, last_seen_at) VALUES (?, '1',"
                "'movie',10,500,'Q',?,?,1,1,?,?)", (rk, ek, folder, NOW, NOW))
            conn.execute(
                "INSERT INTO placements (theme_id, media_type, tmdb_id, section_id,"
                " edition_key, media_folder, placed_at, placement_kind,"
                " plex_refreshed) VALUES (10,'movie',500,'1',?,?,?,'hardlink',1)",
                (ek, folder, NOW))
            conn.execute(
                "INSERT INTO local_files (media_type, tmdb_id, section_id,"
                " edition_key, file_path, downloaded_at, source_video_id,"
                " provenance, source_kind, last_place_attempt_reason) VALUES"
                " ('movie',500,'1',?,?,?, 'v','auto','themerrdb','placed')",
                (ek, f"q-{ek}.mp3", NOW))
        conn.commit()


def _placements(db):
    with sqlite3.connect(db) as conn:
        return sorted(r[0] for r in conn.execute(
            "SELECT edition_key FROM placements WHERE tmdb_id=500"))


def test_bulk_lps_scopes_to_clicked_edition(tmp_path):
    """Bulk LPS on the Extended edition unplaces ONLY Extended — Theatrical's
    separate placement survives (no cross-edition bleed)."""
    from app.web.api import _bulk_lps_run
    s = _settings(tmp_path)
    _seed(s.db_path)
    _bulk_lps_run(s.db_path, s, targets=[{
        "media_type": "movie", "tmdb_id": 500, "section_id": "1",
        "edition_key": "extended"}])
    assert _placements(s.db_path) == ["theatrical"]


def test_bulk_lps_without_edition_is_legacy_title_wide(tmp_path):
    """No edition_key in the target → legacy section-wide unplace (both
    editions), behavior-preserving for old callers."""
    from app.web.api import _bulk_lps_run
    s = _settings(tmp_path)
    _seed(s.db_path)
    _bulk_lps_run(s.db_path, s, targets=[{
        "media_type": "movie", "tmdb_id": 500, "section_id": "1"}])
    assert _placements(s.db_path) == []


def test_v1_21_97_version_pin():
    init_py = (Path(__file__).resolve().parent.parent
               / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
