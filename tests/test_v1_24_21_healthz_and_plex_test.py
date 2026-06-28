"""v1.24.21 — real /healthz liveness + Plex TEST CONNECTION.

Holistic-review operability follow-up (Thread 2):

1. /healthz was an unconditional 200 — the Docker healthcheck meant nothing.
   Now it's a real LIVENESS check: DB reachable + (when main.py wired them onto
   app.state) worker threads + scheduler alive → 200, else 503. It deliberately
   does NOT ping Plex — an external dep being down must not make Docker kill us.

2. A Plex "TEST CONNECTION" button: connect with the saved URL + token and
   surface the server identity (or a clean error) so a bad URL/token fails fast
   at config time, not hours later in a sync. Mirrors TEST NOTIFICATION/COOKIES.
"""
from __future__ import annotations

import threading
from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from app.core.auth import create_admin, init_auth_schema
from app.core.db import init_db
from app.core.plex import PlexClient, PlexConfig

REPO = Path(__file__).resolve().parent.parent
ADMIN = {"X-Authentik-Username": "testadmin"}


@pytest.fixture
def app_ctx(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    from app.web.api import create_app
    app = create_app(settings)
    return TestClient(app), db, settings, app


# ── /healthz liveness ────────────────────────────────────────────

def test_healthz_ok_when_db_reachable(app_ctx):
    client, *_ = app_ctx
    r = client.get("/healthz")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "ok"
    assert body["checks"]["db"] is True


def test_healthz_no_auth_required(app_ctx):
    # /healthz is a public liveness probe — Docker hits it unauthenticated.
    client, *_ = app_ctx
    assert client.get("/healthz").status_code == 200


def test_healthz_degraded_when_worker_dead(app_ctx):
    client, _db, _settings, app = app_ctx
    # Simulate main.py wiring with a DEAD worker (unstarted thread → not alive).
    app.state.worker_threads = [threading.Thread(target=lambda: None)]

    class _Sched:
        running = True
    app.state.scheduler = _Sched()
    r = client.get("/healthz")
    assert r.status_code == 503, r.text
    body = r.json()
    assert body["status"] == "degraded"
    assert body["checks"]["worker"] is False
    assert body["checks"]["db"] is True


def test_healthz_ok_with_live_worker_and_scheduler(app_ctx):
    client, _db, _settings, app = app_ctx
    live = threading.Thread(target=lambda: None)
    live.start()  # alive briefly; we check before it finishes via the stub below

    class _LiveThread:
        @staticmethod
        def is_alive():
            return True

    class _Sched:
        running = True
    app.state.worker_threads = [_LiveThread()]
    app.state.scheduler = _Sched()
    live.join()
    r = client.get("/healthz")
    assert r.status_code == 200, r.text
    assert r.json()["checks"] == {"db": True, "worker": True, "scheduler": True}


# ── PlexClient.get_server_info ───────────────────────────────────

def _cfg():
    return PlexConfig(url="http://x", token="t",
                      movie_section="1", tv_section="2")


def test_get_server_info_parses_identity():
    class _Resp:
        status_code = 200
        text = ('{"MediaContainer": {"friendlyName": "Home Plex", '
                '"version": "1.40.1.1", "machineIdentifier": "abc123", '
                '"platform": "Linux"}}')
    with patch.object(PlexClient, "_get", return_value=_Resp()):
        info = PlexClient(_cfg()).get_server_info()
    assert info == {"friendly_name": "Home Plex", "version": "1.40.1.1",
                    "machine_identifier": "abc123", "platform": "Linux"}


def test_get_server_info_none_on_bad_token():
    class _Resp:
        status_code = 401
        text = ""
    with patch.object(PlexClient, "_get", return_value=_Resp()):
        assert PlexClient(_cfg()).get_server_info() is None


def test_get_server_info_none_on_unreachable():
    # _get returns None when httpx raises (server down / bad URL).
    with patch.object(PlexClient, "_get", return_value=None):
        assert PlexClient(_cfg()).get_server_info() is None


# ── /api/admin/test-plex ─────────────────────────────────────────

def test_test_plex_reports_not_configured(app_ctx):
    client, _db, settings, _app = app_ctx
    # fresh settings: plex not configured
    r = client.post("/api/admin/test-plex", headers=ADMIN)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ok"] is False
    assert "not configured" in body["error"]


def test_test_plex_success_reports_server_identity(app_ctx):
    client, _db, settings, _app = app_ctx
    settings._cfg.plex.enabled = True
    settings._cfg.plex.url = "http://plex:32400"
    settings._cfg.plex.token = "tok"

    class _FakePlex:
        def __init__(self, *a, **k): ...
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def get_server_info(self):
            return {"friendly_name": "Home Plex", "version": "1.40.1.1",
                    "machine_identifier": "abc", "platform": "Linux"}

    with patch("app.core.plex.PlexClient", _FakePlex):
        r = client.post("/api/admin/test-plex", headers=ADMIN)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ok"] is True
    assert body["server_name"] == "Home Plex"
    assert body["version"] == "1.40.1.1"


def test_test_plex_unreachable_reports_clean_error(app_ctx):
    client, _db, settings, _app = app_ctx
    settings._cfg.plex.enabled = True
    settings._cfg.plex.url = "http://plex:32400"
    settings._cfg.plex.token = "tok"

    class _FakePlex:
        def __init__(self, *a, **k): ...
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def get_server_info(self):
            return None

    with patch("app.core.plex.PlexClient", _FakePlex):
        r = client.post("/api/admin/test-plex", headers=ADMIN)
    body = r.json()
    assert body["ok"] is False
    assert "could not reach" in body["error"]


def test_test_plex_requires_admin(app_ctx):
    client, *_ = app_ctx
    # no admin header → forward-auth has no principal → unauthorized
    r = client.post("/api/admin/test-plex")
    assert r.status_code in (401, 403), r.status_code


# ── source pins ──────────────────────────────────────────────────

def test_healthz_is_a_real_check_not_a_stub():
    api = (REPO / "app" / "web" / "api.py").read_text()
    assert 'def healthz(request: Request):' in api
    assert '"status": "ok" if healthy else "degraded"' in api
    assert "status_code=200 if healthy else 503" in api


def test_main_wires_liveness_refs_onto_app_state():
    main = (REPO / "app" / "main.py").read_text()
    assert "app.state.worker_threads = worker_threads" in main
    assert "app.state.scheduler = scheduler" in main


def test_test_plex_ui_button_and_handler_present():
    html = (REPO / "app" / "web" / "templates" / "settings.html").read_text()
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert 'id="test-plex-btn"' in html
    assert "// TEST CONNECTION" in html
    assert "function bindTestPlex()" in js
    assert "bindTestPlex();" in js
    assert "/api/admin/test-plex" in js
