"""v1.23.78 — DROP pill is library-scoped + routes to the right tab.

the user saw "1 DROP" but couldn't find the row anywhere. Root cause: motif's
`themes` table mirrors the WHOLE ThemerrDB catalog, and the DROP count counted
every `tdb_dropped_at` theme — including titles NOT in his Plex library (the
dropped theme was "Noob", a TV show he doesn't own). The sibling UPD count was
already library-scoped (INNER JOIN plex_items + included section); the DROP
count never got that join. Plus the pill href was hardcoded to /movies, so even
an in-library TV/anime drop landed on the wrong tab.

Fix: scope both DROP counts (the SSR topbar + /api/stats) to drops with a
plex_items row in an included section, and add a per-(tab, fourk) breakdown so
the pill routes to the tab that owns the dropped row (mirrors updates.tabs).
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
AUTH = {"X-Authentik-Username": "testadmin"}
TS = "2026-06-17T00:00:00"


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="testadmin", password="testpassword")
    return settings.db_path, TestClient(create_app(settings))


def _theme(conn, media_type, tmdb_id, title, dropped):
    return conn.execute(
        "INSERT INTO themes (media_type, tmdb_id, title, upstream_source, "
        "  last_seen_sync_at, first_seen_sync_at, tdb_dropped_at) "
        "VALUES (?, ?, ?, 'themoviedb', ?, ?, ?)",
        (media_type, tmdb_id, title, TS, TS, dropped),
    ).lastrowid


def _section(conn, sid, title, sectype, is_4k=0, is_anime=0):
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, included, "
        "  is_anime, is_4k, themes_subdir, discovered_at, last_seen_at) "
        "VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?)",
        (sid, title, sectype, is_anime, is_4k,
         f"sub{sid}", TS, TS))


def _pi(conn, rk, sid, media_type, title, tmdb, theme_id):
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, media_type, title, "
        "  guid_tmdb, theme_id, first_seen_at, last_seen_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (rk, sid, media_type, title, tmdb, theme_id, TS, TS))


def _stats_drops(client_):
    db, c = client_
    r = c.get("/api/stats", headers=AUTH)
    assert r.status_code == 200, r.text
    return r.json()["drops"]


# ── behavioral: the count is library-scoped ──


def test_catalog_only_drop_is_not_counted(client):
    """The Noob case: a dropped TV theme with NO library item must NOT count."""
    db, c = client
    with sqlite3.connect(db) as conn:
        _theme(conn, "tv", 61835, "Noob", TS)  # dropped, but not in library
        conn.commit()
    drops = _stats_drops(client)
    assert drops["total"] == 0, "a drop you don't own must not light the badge"
    assert drops["tabs"] == []


def test_in_library_drop_counts_and_routes(client):
    """A dropped theme WITH a library item counts and carries a tab breakdown."""
    db, c = client
    with sqlite3.connect(db) as conn:
        _section(conn, "1", "Movies", "movie")
        tid = _theme(conn, "movie", 100, "Dropped Movie", TS)
        _pi(conn, "rk-m", "1", "movie", "Dropped Movie", 100, tid)
        # plus a catalog-only TV drop that must stay uncounted.
        _theme(conn, "tv", 61835, "Noob", TS)
        conn.commit()
    drops = _stats_drops(client)
    assert drops["total"] == 1, "only the in-library drop counts"
    assert drops["tabs"] == [{"tab": "movies", "fourk": False, "count": 1}]
    assert drops["tab_hint"] == "movies"


def test_anime_drop_routes_to_anime(client):
    """An in-library anime drop routes to the anime tab, not /movies."""
    db, c = client
    with sqlite3.connect(db) as conn:
        _section(conn, "3", "Anime", "show", is_anime=1)
        tid = _theme(conn, "tv", 13916, "Some Anime", TS)
        _pi(conn, "rk-a", "3", "show", "Some Anime", 13916, tid)
        conn.commit()
    drops = _stats_drops(client)
    assert drops["total"] == 1
    assert drops["tabs"][0]["tab"] == "anime"
    assert drops["tab_hint"] == "anime"


# ── source pins ──


def test_ssr_drop_count_is_library_scoped():
    # the topbar SSR count must use the plex_items + included join too.
    i = API_PY.index("AS drops_count")
    block = API_PY[i - 400:i]
    assert "JOIN plex_items pi ON pi.theme_id = t.id" in block
    assert "AND ps.included = 1" in block


def test_drop_badge_routes_in_js():
    i = APP_JS.index("const dropBadge = ")
    body = APP_JS[i:i + 900]
    assert "stats.drops.tabs" in body
    assert "firstDropTab" in body
    assert "tdb_pills=dropped" in body
