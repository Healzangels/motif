"""v1.24.53 — dashboard SERVICES panel: GET /api/services.

Live status of motif's two external dependencies: Plex (reachable? + round-trip
latency + friendly name/version) and yt-dlp (running version). The Plex probe is
short-timeout so a down server reports "offline" fast; the token rides the
X-Plex-Token header server-side.
"""
from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core.db import init_db

AUTH = {"X-Authentik-Username": "testadmin"}
REPO = Path(__file__).resolve().parent.parent


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
    return TestClient(create_app(s))


def test_services_reports_ytdlp_version(client):
    data = client.get("/api/services", headers=AUTH).json()
    # yt-dlp is a hard dependency → importable → a version string is reported
    assert isinstance(data["yt_dlp"]["version"], str) and data["yt_dlp"]["version"]


def test_services_plex_unconfigured_is_offline_without_a_network_call(client):
    # No Plex URL in the test settings → the probe is skipped entirely (no
    # network), and the card reports not-configured / offline / no latency.
    plex = client.get("/api/services", headers=AUTH).json()["plex"]
    assert plex["configured"] is False
    assert plex["online"] is False
    assert plex["latency_ms"] is None
    assert plex["name"] is None


def test_services_endpoint_and_panel_are_wired():
    api = (REPO / "app" / "web" / "api.py").read_text()
    assert '@app.get("/api/services")' in api
    assert 'headers={\n                        "X-Plex-Token"' in api or \
           '"X-Plex-Token": settings.plex_token' in api
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert "async function loadServices()" in js and "/api/services" in js
    html = (REPO / "app" / "web" / "templates" / "dashboard.html").read_text()
    assert 'id="services-row"' in html and "// SERVICES" in html
