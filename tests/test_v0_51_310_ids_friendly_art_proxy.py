"""v0.51.310 — IDS-friendly art proxy (the operator's own IP got banned).

Browsing motif through the reverse proxy tripped two IDS scenarios:
a dashboard's burst of extension-less /api/plex/art/{rk} GETs read as
a non-static crawl, and the DESIGNED 404-on-no-art fed the 4xx probing
counter (every artless row on a page emits one). Two fixes:
  1. The proxy gains a canonical .jpg spelling — IDS static
     classification is extension-based. Bare path stays registered
     (same handler; pre-deploy HTML in open tabs references it).
  2. "No art" returns 204, not 404. An <img> fires onerror on 204
     exactly as on 404 (proven live: naturalWidth 0, error handler
     fired), so the placeholder-tile fallback contract is unchanged.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
AUTH = {"X-Authentik-Username": "testadmin"}


@pytest.fixture
def client(tmp_path, monkeypatch):
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    from fastapi.testclient import TestClient
    (tmp_path / "data").mkdir()
    (tmp_path / "motif.yaml").write_text("paths: {}\n")
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    return TestClient(create_app(s))


def test_no_art_is_204_not_404(client):
    # no plex_url configured -> _fetch_plex_art_bytes returns None -> the
    # designed "no art" outcome. Pre-fix: 404 (probing fodder); now 204.
    r = client.get("/api/plex/art/123.jpg", headers=AUTH)
    assert r.status_code == 204, (
        "'no art' is a DESIGNED outcome fired once per artless row — a 4xx "
        "here feeds IDS probing counters on every page view")
    assert r.content == b""
    assert "max-age" in r.headers.get("Cache-Control", ""), (
        "short cache so a later-added poster shows within minutes without "
        "re-hitting Plex per render")


def test_both_route_spellings_are_registered(client):
    # the 400 branch proves each spelling resolves to the SAME handler
    # without needing a live Plex (bad rk short-circuits before any fetch).
    assert client.get("/api/plex/art/abc.jpg", headers=AUTH).status_code == 400
    assert client.get("/api/plex/art/abc", headers=AUTH).status_code == 400
    assert client.get("/api/plex/art/123", headers=AUTH).status_code == 204, (
        "the bare spelling must keep serving pre-deploy HTML in open tabs")


def test_every_js_art_emitter_uses_the_jpg_spelling():
    # drift guard for FUTURE emitters too: every template-literal art URL
    # must carry the .jpg suffix (comments don't use the ${ form).
    sites = [m.start() for m in re.finditer(r"/api/plex/art/\$\{", APP_JS)]
    assert len(sites) >= 3, "the three known emitters must be visible"
    bare = [i for i in sites if ".jpg" not in APP_JS[i:i + 80]]
    assert bare == [], (
        f"extension-less art emitters at offsets {bare} — IDS layers "
        f"classify static-vs-crawl by extension, and poster bursts through "
        f"the bare spelling read as a non-static crawl")


def test_v0_51_310_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "0.51.310: " in init_py
