"""v0.51.316 — ACCEPT ALL UPDATES / KEEP ALL CURRENT scoped to the library tab.

The bulk bar's count is per tab, but the no-selection path called
/api/updates/count + /api/updates/accept-all globally: on /tv with ONE
pending update the confirm read "Accept 7 pending ThemerrDB updates?"
(the six on /movies included) and OK would have accepted them all. The
three bulk endpoints now take the library page's tab predicate as optional
query params; the JS sends the displayed tab + resolution.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
NOW = "2026-06-11T00:00:00+00:00"
AUTH = {"X-Authentik-Username": "testadmin"}


@pytest.fixture
def admin_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    db = settings.db_path
    init_db(db); init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    return TestClient(create_app(settings)), db


SECTIONS = {  # section_id: (title, type, is_anime, is_4k)
    "1": ("Movies", "movie", 0, 0), "18": ("4K Movies", "movie", 0, 1),
    "2": ("TV Shows", "show", 0, 0), "3": ("Anime", "show", 1, 0),
}


def _seed(db, tmdb_id, section_id):
    """An actionable new_theme_available pending update on an unthemed row
    (the v1.22.62 visible shape), in the given section."""
    title, stype, anime, fourk = SECTIONS[section_id]
    mt = "movie" if stype == "movie" else "tv"
    with sqlite3.connect(db) as conn:
        conn.execute("INSERT OR IGNORE INTO plex_sections (section_id, title, type, is_anime, is_4k, themes_subdir, "
                     "included, discovered_at, last_seen_at) VALUES (?,?,?,?,?,?,1,?,?)",
                     (section_id, title, stype, anime, fourk, f"sub{section_id}", NOW, NOW))
        tid = conn.execute("INSERT INTO themes (media_type, tmdb_id, title, upstream_source, last_seen_sync_at, "
                           "first_seen_sync_at, youtube_url) VALUES (?,?,'X','imdb',?,?,'https://yt/new')",
                           (mt, tmdb_id, NOW, NOW)).lastrowid
        conn.execute("INSERT INTO plex_items (rating_key, section_id, media_type, theme_id, guid_tmdb, title, year, "
                     "has_theme, first_seen_at, last_seen_at) VALUES (?,?,?,?,?,'X',2024,0,?,?)",
                     (f"rk{tmdb_id}", section_id, stype, tid, tmdb_id, NOW, NOW))
        conn.execute("INSERT INTO pending_updates (media_type, tmdb_id, section_id, kind, new_youtube_url, decision, "
                     "detected_at) VALUES (?,?,?,'new_theme_available','https://yt/new','pending',?)",
                     (mt, tmdb_id, section_id, NOW))
        conn.commit()


def _decision(db, tmdb_id):
    with sqlite3.connect(db) as conn:
        return conn.execute("SELECT decision FROM pending_updates WHERE tmdb_id = ?", (tmdb_id,)).fetchone()[0]


def _library(client, db):
    _seed(db, 101, "1"); _seed(db, 102, "1"); _seed(db, 103, "18")   # movies: 2 standard + 1 4K
    _seed(db, 201, "2")                                                  # tv: 1
    _seed(db, 301, "3")                                                  # anime: 1
    return client, db


def test_count_is_global_without_a_tab_and_per_tab_with_one(admin_client):
    client, db = _library(*admin_client)
    g = client.get("/api/updates/count", headers=AUTH).json()
    assert g["pending"] == 5 and g["scope"] is None, "no tab = the legacy global count (topbar UPD)"
    assert client.get("/api/updates/count?tab=movies&fourk=0", headers=AUTH).json()["pending"] == 2
    assert client.get("/api/updates/count?tab=movies&fourk=1", headers=AUTH).json()["pending"] == 1
    assert client.get("/api/updates/count?tab=movies&all_res=1", headers=AUTH).json()["pending"] == 3
    assert client.get("/api/updates/count?tab=tv", headers=AUTH).json()["pending"] == 1
    assert client.get("/api/updates/count?tab=anime", headers=AUTH).json()["pending"] == 1
    assert client.get("/api/updates/count?tab=collections", headers=AUTH).json()["pending"] == 0
    r = client.get("/api/updates/count?tab=movies&fourk=0", headers=AUTH).json()
    assert r["scope"] == {"tab": "movies", "fourk": False, "all_res": False}


def test_unknown_tab_is_a_400_not_a_silent_global(admin_client):
    client, db = _library(*admin_client)
    assert client.get("/api/updates/count?tab=everything", headers=AUTH).status_code == 400
    assert client.post("/api/updates/accept-all?tab=everything", headers=AUTH).status_code == 400
    assert all(_decision(db, t) == "pending" for t in (101, 102, 103, 201, 301))


def test_accept_all_on_tv_leaves_the_movies_rows_pending(admin_client):
    client, db = _library(*admin_client)
    r = client.post("/api/updates/accept-all?tab=tv&fourk=0", headers=AUTH)
    assert r.status_code == 200, r.text
    j = r.json()
    assert j["accepted"] == 1 and j["scope"]["tab"] == "tv", (
        "the user's report: 1 pending on /tv, the confirm said 7 and OK would have accepted the movies too")
    assert _decision(db, 201) == "accepted"
    assert all(_decision(db, t) == "pending" for t in (101, 102, 103, 301))


def test_accept_all_on_movies_respects_the_resolution_chip(admin_client):
    client, db = _library(*admin_client)
    j = client.post("/api/updates/accept-all?tab=movies&fourk=0", headers=AUTH).json()
    assert j["accepted"] == 2
    assert _decision(db, 101) == "accepted" and _decision(db, 102) == "accepted"
    assert _decision(db, 103) == "pending", "the 4K section is a different displayed library"
    j2 = client.post("/api/updates/accept-all?tab=movies&all_res=1", headers=AUTH).json()
    assert j2["accepted"] == 1 and _decision(db, 103) == "accepted"


def test_decline_all_is_scoped_the_same_way(admin_client):
    client, db = _library(*admin_client)
    j = client.post("/api/updates/decline-all?tab=anime", headers=AUTH).json()
    assert j["declined"] == 1 and j["scope"]["tab"] == "anime"
    assert _decision(db, 301) == "declined"
    assert all(_decision(db, t) == "pending" for t in (101, 102, 103, 201))


def test_no_tab_keeps_the_global_behaviour(admin_client):
    client, db = _library(*admin_client)
    j = client.post("/api/updates/accept-all", headers=AUTH).json()
    assert j["accepted"] == 5 and j["scope"] is None


def test_summary_events_name_the_scope():
    # source-level: the events flusher is process-global (bound to the first DB it
    # sees), so a behavioural read of `events` is flaky across the suite. The API
    # response's `scope` is asserted behaviourally above; here the summary
    # log_event of BOTH bulk endpoints must carry the scope in message + detail.
    api = (REPO / "app" / "web" / "api.py").read_text()
    a = api.index("async def api_accept_all_updates("); d = api.index("async def api_decline_all_updates(")
    accept = api[a:api.index("    @app.", a + 10)]; decline = api[d:api.index("    @app.", d + 10)]
    for body in (accept, decline):
        assert 'scope_note = f" in {scope[\'tab\']}" if scope else ""' in body
        assert "{scope_note}" in body and 'detail={"scope": scope}' in body
        assert "{scope_sql}" in body, "the tuples SELECT must carry the tab predicate"


# ── JS: the displayed tab is what gets sent ──────────────────


def test_js_sends_the_displayed_scope_on_both_bulk_paths():
    assert "function libraryUpdatesScopeQs()" in APP_JS
    assert "const tab = (tabEl && tabEl.value) || libraryState.tab || 'movies';" in APP_JS
    assert "&fourk=${libraryState.fourk ? 1 : 0}" in APP_JS and "&all_res=${libraryState.allRes ? 1 : 0}" in APP_JS
    assert APP_JS.count("`/api/updates/count${scopeQs}`") == 2, "both no-selection paths count the displayed tab"
    assert "`/api/updates/accept-all${scopeQs}`" in APP_JS and "`/api/updates/decline-all${scopeQs}`" in APP_JS
    assert "'/api/updates/count'" not in APP_JS and "'/api/updates/accept-all'" not in APP_JS \
        and "'/api/updates/decline-all'" not in APP_JS, "no unscoped bulk call is left"
    assert "in ${scopeLabel}?" in APP_JS and "No pending updates to accept in ${scopeLabel}." in APP_JS


def test_v0_51_316_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "0.51.316: " in init_py
