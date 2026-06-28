"""v1.21.61 — per-edition theme isolation, Phase C2b (LET PLEX SERVE).

The placement half of the user's bug: LET PLEX SERVE / UNPLACE on one edition
deleted EVERY edition's placement (the DELETE was scoped by the bare
(media_type, tmdb_id, section_id)). api_unplace_item now takes the row's
rating_key, resolves its edition_key, and scopes the placements DELETE +
local_files UPDATE to THAT edition. No rating_key = legacy section-wide
fan-out (behavior-preserving for old callers).
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


NOW = "2026-06-04T00:00:00Z"
AUTH = {"X-Authentik-Username": "testadmin"}


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    return TestClient(create_app(s)), s.db_path


def _seed(db):
    """One title (tmdb 800), two editions, each with its OWN placement +
    local_files row."""
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        conn.execute(
            "INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source,"
            " last_seen_sync_at, first_seen_sync_at) VALUES (80,'movie',800,'Q',"
            "'imdb',?,?)", (NOW, NOW))
        for rk, ek, folder in (
            ("rk-std", "", "/data/Movies/Q (2000)"),
            ("rk-ext", "extended", "/data/Movies/Q (2000) {edition-Extended}"),
        ):
            conn.execute(
                "INSERT INTO plex_items (rating_key, section_id, media_type,"
                " theme_id, guid_tmdb, title, edition_key, folder_path,"
                " has_theme, first_seen_at, last_seen_at)"
                " VALUES (?,?, 'movie',80,800,'Q',?,?,0,?,?)",
                (rk, "1", ek, folder, NOW, NOW))
            conn.execute(
                "INSERT INTO placements (theme_id, media_type, tmdb_id,"
                " section_id, edition_key, media_folder, placed_at,"
                " placement_kind, plex_refreshed) VALUES (80,'movie',800,'1',"
                "?,?,?,'hardlink',1)", (ek, folder, NOW))
            conn.execute(
                "INSERT INTO local_files (media_type, tmdb_id, section_id,"
                " edition_key, file_path, downloaded_at, source_video_id,"
                " provenance, source_kind, last_place_attempt_reason) VALUES"
                " ('movie',800,'1',?,?,?, 'v','auto','themerrdb','placed')",
                (ek, f"q-{ek or 'std'}.mp3", NOW))
        conn.commit()


def _editions_with_placement(db):
    with sqlite3.connect(db) as conn:
        return sorted(r[0] for r in conn.execute(
            "SELECT edition_key FROM placements WHERE tmdb_id=800"))


def test_unplace_with_rating_key_scopes_to_one_edition(client):
    """THE fix: unplacing the Extended edition (via its rating_key) removes
    ONLY the Extended placement; the standard placement survives."""
    c, db = client
    _seed(db)
    r = c.post("/api/items/movie/800/unplace?section_id=1&rating_key=rk-ext",
               headers=AUTH)
    assert r.status_code == 200, r.text
    assert _editions_with_placement(db) == [""]  # only standard remains
    # the Extended local_files got the backup_only stamp; standard did NOT.
    with sqlite3.connect(db) as conn:
        reasons = dict(conn.execute(
            "SELECT edition_key, last_place_attempt_reason FROM local_files"
            " WHERE tmdb_id=800"))
    assert reasons["extended"] == "backup_only"
    assert reasons[""] == "placed"  # standard untouched


def test_unplace_standard_edition_leaves_extended(client):
    c, db = client
    _seed(db)
    r = c.post("/api/items/movie/800/unplace?section_id=1&rating_key=rk-std",
               headers=AUTH)
    assert r.status_code == 200, r.text
    assert _editions_with_placement(db) == ["extended"]


def test_unplace_without_rating_key_is_legacy_fanout(client):
    """No rating_key -> the legacy section-wide delete (both editions),
    behavior-preserving for old callers / bulk paths."""
    c, db = client
    _seed(db)
    r = c.post("/api/items/movie/800/unplace?section_id=1", headers=AUTH)
    assert r.status_code == 200, r.text
    assert _editions_with_placement(db) == []  # all editions unplaced
