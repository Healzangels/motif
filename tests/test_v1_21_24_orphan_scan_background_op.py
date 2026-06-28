"""v1.21.24 — orphan scan is now a page-scoped background op.

the user (on v1.21.23): the orphan scan no longer locks up motif (v1.21.20),
but "running it still doesn't indicate progress... navigating away then
back seems like it restarts or stops the scan." Root cause: it was a
one-shot synchronous GET — the whole per-row Plex /themes walk ran inside
the request, so there was no progress and the browser aborted it (losing
the result) when you left the page.

Fix (lightweight, page-scoped): POST /api/admin/orphan-scan/start spawns a
daemon thread that runs the scan and reports progress + the final findings
via an in-process state dict; the orphans page polls
GET /api/admin/orphan-scan/status for "X / N checked" and restores the
last result on load. Survives navigation; no schema change.
"""
from __future__ import annotations

import threading
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import app.web.api as apimod
from app.core.auth import create_admin, init_auth_schema
from app.core.db import init_db


REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
FWD_HDR = {"X-Authentik-Username": "testadmin"}


@pytest.fixture(autouse=True)
def _reset_orphan_state():
    # The scan state is module-level (process-shared) — reset between tests.
    apimod._ORPHAN_SCAN_STATE.clear()
    apimod._ORPHAN_SCAN_STATE.update(status="idle")
    yield
    apimod._ORPHAN_SCAN_STATE.clear()
    apimod._ORPHAN_SCAN_STATE.update(status="idle")


def _make_app(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("MOTIF_PLEX_ENABLED", "true")
    monkeypatch.setenv("MOTIF_PLEX_TOKEN", "testtoken")
    monkeypatch.setenv("MOTIF_PLEX_URL", "http://plex.test:32400")
    from app.config import Settings
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="testadmin", password="testpassword")
    return TestClient(apimod.create_app(settings)), settings


def _poll_until(client, pred, tries=100, delay=0.03):
    for _ in range(tries):
        st = client.get("/api/admin/orphan-scan/status", headers=FWD_HDR).json()
        if pred(st):
            return st
        time.sleep(delay)
    return st


def test_status_is_idle_before_any_run(tmp_path, monkeypatch):
    client, _ = _make_app(tmp_path, monkeypatch)
    st = client.get("/api/admin/orphan-scan/status", headers=FWD_HDR).json()
    assert st["status"] == "idle"
    assert not st.get("findings")


def test_start_progress_alreadyrunning_then_done(tmp_path, monkeypatch):
    gate = threading.Event()

    def fake_scan(db_path, plex, *, themes_dir=None, progress_cb=None, **k):
        if progress_cb:
            progress_cb(1, 2)            # report mid-scan progress
        gate.wait(timeout=5)             # hold "running" until the test releases
        if progress_cb:
            progress_cb(2, 2)
        return [{"drift_type": "ok"}, {"drift_type": "no_plex_entries"}]

    import app.core.orphan_scan as orphan_scan
    monkeypatch.setattr(orphan_scan, "scan_plex_upload_placements", fake_scan)

    client, _ = _make_app(tmp_path, monkeypatch)

    # start → returns immediately (background thread)
    r = client.post("/api/admin/orphan-scan/start", headers=FWD_HDR)
    assert r.status_code == 200 and r.json()["started"] is True

    # progress is reported while it runs
    st = _poll_until(client, lambda s: s.get("done") == 1 and s["status"] == "running")
    assert st["status"] == "running"
    assert st["done"] == 1 and st["total"] == 2

    # a second start while running is a no-op (doesn't spawn a 2nd scan)
    r2 = client.post("/api/admin/orphan-scan/start", headers=FWD_HDR)
    assert r2.json() == {"ok": True, "started": False, "already_running": True}

    # release → completes with findings + summary
    gate.set()
    done = _poll_until(client, lambda s: s["status"] == "done")
    assert done["status"] == "done"
    assert done["total"] == 2
    assert len(done["findings"]) == 2
    assert done["summary"] == {"ok": 1, "no_plex_entries": 1}
    assert done["scanned_at"]


def test_failure_surfaces_in_status(tmp_path, monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("plex exploded")

    import app.core.orphan_scan as orphan_scan
    monkeypatch.setattr(orphan_scan, "scan_plex_upload_placements", boom)

    client, _ = _make_app(tmp_path, monkeypatch)
    client.post("/api/admin/orphan-scan/start", headers=FWD_HDR)
    st = _poll_until(client, lambda s: s["status"] in ("failed", "done"))
    assert st["status"] == "failed"
    assert "plex exploded" in (st.get("error") or "")


def test_start_503_when_plex_disabled(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_PLEX_ENABLED", "false")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    from app.config import Settings
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="testadmin", password="testpassword")
    client = TestClient(apimod.create_app(settings))
    r = client.post("/api/admin/orphan-scan/start", headers=FWD_HDR)
    assert r.status_code == 503


def test_routes_and_progress_cb_sourcepins():
    assert '@app.post("/api/admin/orphan-scan/start")' in API_PY
    assert '@app.get("/api/admin/orphan-scan/status")' in API_PY
    # old synchronous endpoint is gone
    assert '@app.get("/api/admin/orphan-scan")' not in API_PY
    # scan accepts the progress callback
    os = (REPO / "app" / "core" / "orphan_scan.py").read_text()
    assert "progress_cb" in os


def test_version_bumped():
    assert '__version__ = "0.' in (REPO / "app" / "__init__.py").read_text()
