"""v1.23.38 — "ready to add" excludes dead-URL (red TDB ✗) themes.

the user found the 3 movies the dashboard called "ready to add" (The Abyss,
Lady Ballers, Reality) all show the red TDB ✗ pill — ThemerrDB has a theme
entry but its YouTube URL is dead (video_removed), so it can NOT be
downloaded. "ready to add" implied motif could place a theme; it can't until
the URL updates / probes live.

Fix: ready-to-add also excludes rows whose TDB theme last failed with a
"needs a new URL" failure_kind (the FailureKind.needs_manual_override set:
video_private / video_removed / video_age_restricted / geo_blocked). The
failure clears (→ NULL) on a successful re-sync/probe, so the row re-enters
"ready to add" automatically.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core.downloader import FailureKind


REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
NOW = "2026-06-14T00:00:00"
AUTH = {"X-Authentik-Username": "testadmin"}


@pytest.fixture
def admin_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("MOTIF_PLEX_URL", "http://plex.test:32400")
    monkeypatch.setenv("MOTIF_PLEX_TOKEN", "tok")
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="testadmin", password="testpassword")
    return TestClient(create_app(settings)), settings.db_path


def _theme(conn, tid, tmdb, title, failure_kind=None):
    conn.execute(
        "INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source,"
        " failure_kind, last_seen_sync_at, first_seen_sync_at, youtube_url)"
        " VALUES (?, 'movie', ?, ?, 'imdb', ?, ?, ?, 'https://y/watch?v=V')",
        (tid, tmdb, title, failure_kind, NOW, NOW))


def _item(conn, *, rk, tid, tmdb, title):
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, media_type, theme_id,"
        " guid_imdb, guid_tmdb, title, year, has_theme, plex_independent_theme,"
        " first_seen_at, last_seen_at)"
        " VALUES (?, '1', 'movie', ?, ?, ?, ?, 2012, 0, 0, ?, ?)",
        (rk, tid, f"tt{tmdb}", tmdb, title, NOW, NOW))


def _coverage(client):
    r = client.get("/api/coverage/plex", headers=AUTH)
    assert r.status_code == 200, r.text
    return {it["rating_key"]: it for it in r.json()["movies"]}


def _isaddable(it):
    dead = {"video_private", "video_removed", "video_age_restricted", "geo_blocked"}
    return (it["motif_available"] and not it["has_theme"] and not it["placed"]
            and not (it.get("failure_kind") in dead))


def test_dead_url_theme_is_not_ready_to_add(admin_client):
    client, db = admin_client
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        # DEAD URL: TDB-available, unthemed, unplaced, BUT video_removed.
        _theme(conn, 1, 101, "Dead", failure_kind="video_removed")
        _item(conn, rk="rk-dead", tid=1, tmdb=101, title="Dead")
        # CLEAN: TDB-available, unthemed, unplaced, no failure → ready.
        _theme(conn, 2, 102, "Clean", failure_kind=None)
        _item(conn, rk="rk-clean", tid=2, tmdb=102, title="Clean")
        conn.commit()

    cov = _coverage(client)
    assert cov["rk-dead"]["motif_available"] is True
    assert cov["rk-dead"]["failure_kind"] == "video_removed"
    assert cov["rk-clean"]["failure_kind"] is None

    ready = [rk for rk, it in cov.items() if _isaddable(it)]
    assert ready == ["rk-clean"], ready


def test_coverage_endpoint_exposes_failure_kind(admin_client):
    client, db = admin_client
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        _theme(conn, 1, 201, "X", failure_kind="geo_blocked")
        _item(conn, rk="rk-x", tid=1, tmdb=201, title="X")
        conn.commit()
    cov = _coverage(client)
    assert "failure_kind" in cov["rk-x"]
    assert cov["rk-x"]["failure_kind"] == "geo_blocked"


# ── source / drift guards ──


def test_ssr_ready_excludes_dead_failure_kinds():
    # the SSR ready aggregate must AND-out the dead failure_kind set.
    assert "COALESCE(t.failure_kind, '') NOT IN" in API_PY
    for kind in ("video_private", "video_removed",
                 "video_age_restricted", "geo_blocked"):
        assert f"'{kind}'" in API_PY


def test_dead_set_matches_failurekind_needs_manual_override():
    # drift guard: the dead set the dashboard excludes is exactly
    # FailureKind.needs_manual_override (the canonical "only a new URL fixes
    # this" group). If a kind is added/removed there, update the SSR clause
    # + the JS TDB_DEAD_FAILURES_GLOBAL together.
    canonical = {k.value for k in FailureKind if k.needs_manual_override}
    assert canonical == {"video_private", "video_removed",
                         "video_age_restricted", "geo_blocked"}


def test_js_isaddable_excludes_dead_and_placed():
    # the ready predicate is centralized in isAddable, used by all 3 cards.
    assert "const isAddable = (m) =>" in APP_JS
    i = APP_JS.index("const isAddable = (m) =>")
    body = APP_JS[i:i + 320]
    assert "m.motif_available && !m.has_theme && !m.placed" in body
    assert "TDB_DEAD_FAILURES_GLOBAL.has(m.failure_kind)" in body
    # all three cards route through it.
    assert "data.movies.filter(isAddable)" in APP_JS
    assert "data.tv.filter(isAddable)" in APP_JS
    assert "animeList.filter(isAddable)" in APP_JS
    assert "collectionsList.filter(isAddable)" in APP_JS
