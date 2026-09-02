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


def test_jpg_route_registers_before_the_bare_one(client):
    # v0.51.311 (review): inspect the route table — the old 400 probes were
    # satisfied by the BARE route swallowing 'abc.jpg', so they could not see
    # the decorator-order trap they claimed to prove.
    art = [r for r in client.app.routes
           if getattr(r, "path", "").startswith("/api/plex/art/")]
    assert [r.path for r in art] == ["/api/plex/art/{rating_key}.jpg",
                                     "/api/plex/art/{rating_key}"], (
        "the .jpg route must register FIRST — Starlette matches in "
        "registration order and {rating_key} greedily eats '123.jpg'")
    assert len({r.endpoint for r in art}) == 1, "one handler, two spellings"
    assert client.get("/api/plex/art/123.jpg", headers=AUTH).status_code == 204
    assert client.get("/api/plex/art/123", headers=AUTH).status_code == 204, (
        "the bare spelling must keep serving pre-deploy HTML in open tabs")


def test_every_js_art_emitter_uses_the_jpg_spelling():
    # drift guard for FUTURE emitters too: every template-literal art URL
    # must carry the .jpg suffix (comments don't use the ${ form).
    # v0.51.311 (review): EVERY code reference (comments stripped) must be a
    # `${...}.jpg` template emitter — the old 80-char forward window accepted
    # a `.jpg` in a trailing comment and ignored string-concatenation forms.
    code = "\n".join(l.split("//", 1)[0] for l in APP_JS.split("\n"))
    refs = re.findall(r"/api/plex/art/", code)
    good = re.findall(r"/api/plex/art/\$\{[^}]*\}\.jpg", code)
    assert len(refs) >= 3, "the three known emitters must be visible"
    assert len(refs) == len(good), (
        f"{len(refs) - len(good)} art reference(s) are not `${{...}}.jpg` "
        f"emitters — IDS layers classify static-vs-crawl by extension, and "
        f"poster bursts through a bare spelling read as a non-static crawl")


def test_v0_51_310_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "0.51.310: " in init_py
