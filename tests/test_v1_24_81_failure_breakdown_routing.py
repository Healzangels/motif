"""v1.24.81 — FAILURE BREAKDOWN bars drill into the owning tab.

Pre-fix every failure-kind bar in the dashboard insights drilled to
/movies?status=failures, but the library's status=failures filter is
media-type-scoped to its tab — so clicking an anime/TV-only failure kind landed
on an EMPTY movies view (same class as the v1.12.11 topbar-FAIL fix). The
/api/dashboard/insights failure rollup now buckets by tab and returns, per kind,
the tab that owns the most of that kind; the JS routes the bar there.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
AUTH = {"X-Authentik-Username": "testadmin"}
NOW = "2026-01-01T00:00:00Z"


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
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="testadmin", password="testpassword")
    conn = sqlite3.connect(settings.db_path)
    # an ANIME section + a TV theme in it carrying an unacked cookies failure
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, included, is_4k, "
        "is_anime, discovered_at, last_seen_at) "
        "VALUES ('9','Anime','show',1,0,1,?,?)", (NOW, NOW))
    conn.execute(
        "INSERT INTO themes (media_type, tmdb_id, title, upstream_source, "
        "failure_kind, last_seen_sync_at, first_seen_sync_at) "
        "VALUES ('tv', 500, 'Anime Show', 'themoviedb', 'cookies_expired', ?, ?)",
        (NOW, NOW))
    tid = conn.execute("SELECT id FROM themes WHERE tmdb_id=500").fetchone()[0]
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, media_type, title, year,"
        " guid_tmdb, theme_id, first_seen_at, last_seen_at) "
        "VALUES ('a1','9','show','Anime Show','2024',500,?,?,?)", (tid, NOW, NOW))
    conn.commit()
    conn.close()
    return TestClient(create_app(settings))


def test_failure_breakdown_routes_to_owning_tab(admin_client):
    data = admin_client.get("/api/dashboard/insights", headers=AUTH).json()
    failures = {f["kind"]: f for f in data["failures"]}
    assert "cookies_expired" in failures, data["failures"]
    # the anime failure routes to /anime, not the hardcoded /movies.
    assert failures["cookies_expired"]["tab"] == "anime"
    assert failures["cookies_expired"]["count"] == 1


def test_js_failure_bar_uses_r_tab():
    # the bar href is keyed on r.tab (with a movies fallback), not hardcoded.
    assert "/${r.tab || 'movies'}?status=failures&fk=" in APP_JS
    assert "`/movies?status=failures&fk=" not in APP_JS
