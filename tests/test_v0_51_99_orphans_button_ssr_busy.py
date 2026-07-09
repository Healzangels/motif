"""v0.51.99 — SSR-lock the orphan-scan // RUN SCAN button mid-scan.

Same click-flash class as the dashboard v0.51.98 fix: orphans.html rendered
`// RUN SCAN` plain-clickable and only orphans.html's own pollStatus() (which
runs at page init) locked it (runBtn.disabled=true) once it observed
status==='running' — a ~1.5s clickable window on a nav mid-scan. The
/admin/orphans route now passes orphan_scan_running (from _ORPHAN_SCAN_STATE)
so the button paints disabled + the sibling status seeds "scanning…" on first
paint. pollStatus() reconciles idempotently.
"""
from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

REPO = Path(__file__).resolve().parent.parent
ORPHANS_HTML = (REPO / "app" / "web" / "templates" / "orphans.html").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()
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
    return TestClient(create_app(s))


def _btn(html: str) -> str:
    i = html.index('id="orphans-run"')
    start = html.rfind("<button", 0, i)
    end = html.index("</button>", i) + len("</button>")
    return html[start:end]


def _set_scan_status(status: str):
    # The route reads the module-level _ORPHAN_SCAN_STATE global; mutate it,
    # and always restore to idle so the shared global doesn't leak across tests.
    import app.web.api as api
    api._ORPHAN_SCAN_STATE["status"] = status


def test_run_scan_button_disabled_when_scan_running(client):
    _set_scan_status("running")
    try:
        r = client.get("/admin/orphans", headers=AUTH)
        assert r.status_code == 200, r.text
        assert "disabled" in _btn(r.text), _btn(r.text)
        # the sibling status span seeds the live "scanning…" text (matches JS).
        assert "scanning…" in r.text
    finally:
        _set_scan_status("idle")


def test_run_scan_button_clickable_when_idle(client):
    _set_scan_status("idle")
    r = client.get("/admin/orphans", headers=AUTH)
    assert r.status_code == 200, r.text
    assert "disabled" not in _btn(r.text), _btn(r.text)


def test_route_passes_orphan_scan_running_flag():
    # Guard the plumbing: the route must derive the flag from the scan state
    # global so a refactor that drops it re-opens the flash.
    assert '"orphan_scan_running": _ORPHAN_SCAN_STATE.get("status") == "running"' in API_PY
    # and the template must gate the button's disabled attr on it.
    assert "{% if orphan_scan_running %} disabled{% endif %}" in ORPHANS_HTML
