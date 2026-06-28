"""v1.24.52 — dashboard RECENTLY ADDED carousel: backend feed + Plex art proxy.

The carousel shows the titles motif most recently placed a theme on, posters
proxied from Plex, click → the INFO card. Two endpoints back it:
  - GET /api/recently-placed: distinct recently-placed titles (newest first),
    each with the rating_key + the INFO-card keys; plex_items media_type 'show'
    is mapped to the frontend's 'tv'.
  - GET /api/plex/art/{rk}: same-origin Plex poster proxy; rating_key must be
    all-digits (blocks path-injection into the metadata URL); token rides the
    X-Plex-Token header server-side.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core.db import get_conn, init_db

NOW = datetime.now(timezone.utc).isoformat(timespec="seconds")
AUTH = {"X-Authentik-Username": "testadmin"}


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.web.api import create_app
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    with sqlite3.connect(s.db_path) as c:
        for sid, title, typ in (("1", "Movies", "movie"), ("2", "Shows", "show")):
            c.execute(
                "INSERT INTO plex_sections (section_id, title, type, is_anime,"
                " is_4k, themes_subdir, included, discovered_at, last_seen_at) "
                "VALUES (?,?,?,0,0,?,1,?,?)", (sid, title, typ, title.lower(), NOW, NOW))
        c.commit()
    return TestClient(create_app(s)), s.db_path


def _theme(c, tid, mt, tmdb, title):
    c.execute("INSERT INTO themes (id, media_type, tmdb_id, title, year, "
              "upstream_source, last_seen_sync_at, first_seen_sync_at) "
              "VALUES (?,?,?,?,'2001','imdb',?,?)", (tid, mt, tmdb, title, NOW, NOW))


def _plex_item(c, *, rk, section, mt, tmdb, title):
    c.execute("INSERT INTO plex_items (rating_key, section_id, media_type, title, "
              "year, guid_tmdb, edition_key, has_theme, first_seen_at, last_seen_at) "
              "VALUES (?,?,?,?,'2001',?,'',1,?,?)", (rk, section, mt, title, tmdb, NOW, NOW))


def _placement(c, *, mt, tmdb, section, rk, placed_at, folder="/x"):
    c.execute("INSERT INTO placements (media_type, tmdb_id, section_id, theme_id, "
              "media_folder, placed_at, placement_kind, plex_rating_key, edition_key) "
              "VALUES (?,?,?,NULL,?,?,'hardlink',?,'')",
              (mt, tmdb, section, folder, placed_at, rk))


# ── the feed ─────────────────────────────────────────────────────────────────

def test_recently_placed_newest_first_and_maps_show_to_tv(client):
    c, db = client
    with get_conn(db) as conn:
        _theme(conn, 1, "movie", 100, "Older Movie")
        _theme(conn, 2, "tv", 200, "Newer Show")
        _plex_item(conn, rk="11", section="1", mt="movie", tmdb=100, title="Older Movie")
        _plex_item(conn, rk="22", section="2", mt="show", tmdb=200, title="Newer Show")
        _placement(conn, mt="movie", tmdb=100, section="1", rk="11", placed_at="2026-01-01T00:00:00")
        _placement(conn, mt="tv", tmdb=200, section="2", rk="22", placed_at="2026-06-01T00:00:00")
        conn.commit()
    items = c.get("/api/recently-placed", headers=AUTH).json()["items"]
    assert [it["title"] for it in items] == ["Newer Show", "Older Movie"]  # placed_at DESC
    assert items[0]["media_type"] == "tv"   # plex_items 'show' → frontend 'tv'
    assert items[0]["rating_key"] == "22"
    assert items[0]["tmdb_id"] == 200 and items[0]["section_id"] == "2"


def test_recently_placed_dedups_by_rating_key_keeping_latest(client):
    c, db = client
    with get_conn(db) as conn:
        _theme(conn, 1, "movie", 100, "Twice Placed")
        _plex_item(conn, rk="11", section="1", mt="movie", tmdb=100, title="Twice Placed")
        # two placement rows (distinct media_folders — distinct PKs) sharing one
        # Plex rating_key → the carousel shows ONE card with the newer date.
        _placement(conn, mt="movie", tmdb=100, section="1", rk="11", placed_at="2026-01-01T00:00:00", folder="/a")
        _placement(conn, mt="movie", tmdb=100, section="1", rk="11", placed_at="2026-05-05T00:00:00", folder="/b")
        conn.commit()
    items = c.get("/api/recently-placed", headers=AUTH).json()["items"]
    assert len(items) == 1
    assert items[0]["placed_at"] == "2026-05-05T00:00:00"


def test_collection_uses_placement_tmdb_id_not_null_guid(client):
    # v1.24.54 bug: collections have a NULL plex_items.guid_tmdb but a synthetic
    # placements.tmdb_id (the id the /collections row + openInfoDialog use). The
    # feed must return the placement id, else the INFO card opens with
    # tmdb_id=null → /api/items/collection/null → 422 (the user's Middle-Earth repro).
    c, db = client
    with get_conn(db) as conn:
        _theme(conn, 1, "collection", -500, "Middle-Earth Collection")
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type, title, "
            "year, guid_tmdb, edition_key, has_theme, first_seen_at, last_seen_at) "
            "VALUES ('77','1','collection','Middle-Earth Collection',NULL,NULL,'',1,?,?)",
            (NOW, NOW))
        _placement(conn, mt="collection", tmdb=-500, section="1", rk="77",
                   placed_at="2026-06-01T00:00:00")
        conn.commit()
    items = c.get("/api/recently-placed", headers=AUTH).json()["items"]
    assert items[0]["media_type"] == "collection"
    assert items[0]["tmdb_id"] == -500  # synthetic placement id, NOT null


def test_recently_placed_excludes_placements_without_rating_key(client):
    c, db = client
    with get_conn(db) as conn:
        _theme(conn, 1, "movie", 100, "No RK")
        _plex_item(conn, rk="11", section="1", mt="movie", tmdb=100, title="No RK")
        c2 = conn
        c2.execute("INSERT INTO placements (media_type, tmdb_id, section_id, theme_id,"
                   " media_folder, placed_at, placement_kind, plex_rating_key, edition_key)"
                   " VALUES ('movie',100,'1',NULL,'/x','2026-01-01T00:00:00','hardlink',NULL,'')")
        conn.commit()
    items = c.get("/api/recently-placed", headers=AUTH).json()["items"]
    assert items == []  # no plex_rating_key → no poster → excluded


# ── the Plex art proxy ───────────────────────────────────────────────────────

def test_art_proxy_rejects_non_numeric_rating_key(client):
    c, _ = client
    # path-injection guard: only all-digit rks reach the Plex metadata URL
    assert c.get("/api/plex/art/abc", headers=AUTH).status_code == 400
    assert c.get("/api/plex/art/12x", headers=AUTH).status_code == 400


def test_art_proxy_404_when_no_plex_configured(client):
    c, _ = client
    # numeric rk is accepted, but with no Plex URL configured the server-side
    # fetch returns nothing → 404 so the carousel shows its placeholder tile.
    assert c.get("/api/plex/art/123", headers=AUTH).status_code == 404


def test_endpoints_and_carousel_are_wired():
    repo = Path(__file__).resolve().parent.parent
    api = (repo / "app" / "web" / "api.py").read_text()
    assert '@app.get("/api/recently-placed")' in api
    assert '@app.get("/api/plex/art/{rating_key}")' in api
    assert 'headers={"X-Plex-Token": settings.plex_token}' in api  # token in header
    js = (repo / "app" / "web" / "static" / "app.js").read_text()
    assert "async function loadRecentlyAdded()" in js
    assert "/api/recently-placed" in js and "/api/plex/art/" in js
    # v1.24.54: auto-scroll toggle
    assert "_setupCarouselAutoScroll" in js and "motif:recentAutoScroll" in js
    html = (repo / "app" / "web" / "templates" / "dashboard.html").read_text()
    assert 'id="recently-added-strip"' in html and "// RECENTLY ADDED" in html
    assert 'id="recent-autoscroll"' in html  # auto-scroll checkbox
