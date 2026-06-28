"""v1.21.95 — gate the v1.21.93 '' fallback on a single-edition title.

v1.21.93 made LET PLEX SERVE on a metadata-only edition fall back to the
shared '' placement (so the action wasn't a silent no-op). That's correct
when ONE edition maps to '' (Godzilla "Minus Color"). But the user's Watchmen
has THREE metadata editions (Theatrical Cut / Midnight / Director's Cut) —
three Plex items sharing ONE folder + ONE theme.mp3 at edition_key=''. The
'' fallback let LPS-on-one unplace the SHARED placement → all three flipped
(a cross-edition bleed). A hardlink sidecar is folder-level, so these
editions physically share a theme; true independence needs per-rk uploads.

The gate: only fall back to '' when the title+section has exactly ONE
edition (the '' placement unambiguously belongs to it). Multiple editions
sharing '' → no fallback (no bleed).
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


def _theme(conn, tmdb):
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k,"
        " themes_subdir, included, discovered_at, last_seen_at)"
        " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
    conn.execute(
        "INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source,"
        " last_seen_sync_at, first_seen_sync_at) VALUES (80,'movie',?,'W',"
        "'imdb',?,?)", (tmdb, NOW, NOW))


def _item(conn, rk, tmdb, ek, folder):
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, media_type, theme_id,"
        " guid_tmdb, title, edition_key, folder_path, has_theme, first_seen_at,"
        " last_seen_at) VALUES (?, '1','movie',80,?,'W',?,?,1,?,?)",
        (rk, tmdb, ek, folder, NOW, NOW))


def _shared_empty_placement(conn, tmdb, folder):
    conn.execute(
        "INSERT INTO placements (theme_id, media_type, tmdb_id, section_id,"
        " edition_key, media_folder, placed_at, placement_kind, plex_refreshed)"
        " VALUES (80,'movie',?,'1','',?,?,'hardlink',1)", (tmdb, folder, NOW))
    conn.execute(
        "INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key,"
        " file_path, downloaded_at, source_video_id, provenance, source_kind,"
        " last_place_attempt_reason) VALUES ('movie',?,'1','','w.mp3',?, 'v',"
        "'auto','themerrdb','placed')", (tmdb, NOW))


def _placements(db, tmdb):
    with sqlite3.connect(db) as conn:
        return [r[0] for r in conn.execute(
            "SELECT edition_key FROM placements WHERE tmdb_id=?", (tmdb,))]


# ── THE gate: multiple editions sharing one '' placement → no bleed ──


def test_lps_on_shared_multi_edition_does_not_unplace_shared(client):
    """Three metadata editions sharing one '' placement: LPS on one must NOT
    fall back to the shared '' placement (no cross-edition bleed)."""
    c, db = client
    with sqlite3.connect(db) as conn:
        _theme(conn, 13183)
        _item(conn, "417795", 13183, "theatrical cut",
              "/data/Movies/Watchmen (2009) {edition-Theatrical Cut}")
        _item(conn, "417813", 13183, "midnight",
              "/data/Movies/Watchmen (2009) {edition-Midnight}")
        _item(conn, "590328", 13183, "director's cut",
              "/data/Movies/Watchmen (2009) {edition-Director's Cut}")
        _shared_empty_placement(conn, 13183, "/data/Movies/Watchmen (2009)")
        conn.commit()

    r = c.post("/api/items/movie/13183/unplace?section_id=1&rating_key=590328",
               headers=AUTH)
    assert r.status_code == 200, r.text
    # The shared '' placement must SURVIVE — LPS on one edition can't nuke it.
    assert _placements(db, 13183) == [""], (
        "LPS on one of 3 shared-folder editions must not unplace the shared "
        "'' placement")


# ── Regression: single metadata edition still falls back (Godzilla) ──


def test_lps_on_single_metadata_edition_still_falls_back(client):
    """One metadata edition with only a '' placement: the v1.21.93 fallback
    still fires (gate count == 1) so the placement is removed."""
    c, db = client
    with sqlite3.connect(db) as conn:
        _theme(conn, 940721)
        _item(conn, "523969", 940721, "minus color",
              "/data/Movies/Godzilla Minus One (2023) {edition-Minus Color}")
        _shared_empty_placement(conn, 940721,
                                "/data/Movies/Godzilla Minus One (2023)")
        conn.commit()

    r = c.post("/api/items/movie/940721/unplace?section_id=1&rating_key=523969",
               headers=AUTH)
    assert r.status_code == 200, r.text
    assert _placements(db, 940721) == [], (
        "single-edition fallback must still remove the '' placement")


def test_v1_21_95_version_pin():
    init_py = (Path(__file__).resolve().parent.parent
               / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
