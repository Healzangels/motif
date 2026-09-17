"""v0.51.344: a job START whose thread cannot start answers 500 in words and leaves the job startable — RESTORE FROM PLEX, the AnimeThemes sweep, the orphan scan and the loudness audit."""
from __future__ import annotations

import logging
import threading
import time

from fastapi.testclient import TestClient

import app.web.api as api_mod
from app.core import animethemes as at
from app.core import canonical_health as ch
from test_v0_51_158_loudness_audit import _make_app as _loudness_app
from test_v0_51_314_animethemes_resolver import BRIDGE_JSON, FakeAPI
from test_v0_51_325_animethemes_sweep import _client as _at_client
from test_v0_51_325_animethemes_sweep import _reset_state as _reset_at_sweep
from test_v0_51_325_animethemes_sweep import app_env  # noqa: F401 — a fixture
from test_v0_51_342_restore_from_plex_job import (  # noqa: F401 — env is a fixture
    AUTH, CHECK, JOB_THREAD, START, HeldRestore, _finish, _status, env,
)
from test_v1_21_24_orphan_scan_background_op import _make_app as _orphan_app

REPAIR = "/api/admin/canonical-health/repair"
REFUSED = "can't start new thread"


def _refuse_start(monkeypatch, name):
    """Thread.start raises for the named job's thread only; returns the undo."""
    real = threading.Thread.start

    def start(self):
        if self.name == name:
            raise RuntimeError(REFUSED)
        return real(self)
    monkeypatch.setattr(threading.Thread, "start", start)
    return lambda: monkeypatch.setattr(threading.Thread, "start", real)


def _until(get, pred, what):
    end = time.monotonic() + 10
    while True:
        st = get()
        if pred(st):
            return st
        assert time.monotonic() < end, f"timed out waiting for {what}: {st}"
        time.sleep(0.02)


def _join(name):
    for t in threading.enumerate():
        if t.name == name:
            t.join(10)
            assert not t.is_alive(), f"the {name} thread did not finish"


def _refused_with_words(response):
    assert response.status_code == 500, response.text
    detail = response.json()["detail"]
    assert "nothing ran" in detail and REFUSED in detail, detail


def test_restore_from_plex_whose_thread_cannot_start_says_nothing_ran_and_locks_nothing(env, monkeypatch, caplog):
    client, settings, tmp_path, events = env
    monkeypatch.setattr(api_mod, "_CANON_RESTORE_SHUTDOWN", threading.Event())
    monkeypatch.setattr(api_mod, "_CANON_RESTORE_THREAD", None)
    held = HeldRestore(released=True)
    monkeypatch.setattr(ch, "restore_from_plex", held)
    loose = TestClient(client.app, raise_server_exceptions=False)
    allow = _refuse_start(monkeypatch, JOB_THREAD)
    with caplog.at_level(logging.ERROR):
        _refused_with_words(loose.post(START, headers=AUTH))
    assert any("could not start" in r.getMessage() for r in caplog.records if r.levelno == logging.ERROR)
    # v0.51.344: a start that never ran reads failed in words — 'idle' told a watching tab motif had restarted mid-run —
    # and leaves neither a 'running' page nor a thread for exit to join
    st = _status(loose)
    assert st["status"] == "failed" and REFUSED in st["error"] and "nothing ran" in st["error"], st
    assert (st["restored"], bool(st["finished_at"])) == (0, True), st
    assert (loose.post(CHECK, headers=AUTH).status_code, loose.post(REPAIR, headers=AUTH).status_code) == (200, 200)
    assert api_mod.canon_restore_shutdown() is None, "exit would join a thread that never started"
    assert held.calls == []
    allow()
    monkeypatch.setattr(api_mod, "_CANON_RESTORE_SHUTDOWN", threading.Event())
    assert loose.post(START, headers=AUTH).json() == {"ok": True, "started": True}
    assert _finish(loose)["status"] == "done" and len(held.calls) == 1


def test_an_anime_themes_sweep_whose_thread_cannot_start_reads_failed_and_the_next_start_runs(app_env, monkeypatch):
    client, _settings = app_env
    _reset_at_sweep()
    monkeypatch.setattr(api_mod, "log_event", lambda _db, **k: None)
    monkeypatch.setattr(at, "load_bridge", lambda cache_dir, **kw: at.Bridge.from_json(BRIDGE_JSON))
    fake_client = _at_client(FakeAPI())  # built BEFORE the class is patched
    monkeypatch.setattr(at, "AnimeThemesClient", lambda *a, **kw: fake_client)
    loose = TestClient(client.app, raise_server_exceptions=False)
    status = lambda: loose.get("/api/admin/animethemes-sweep/status", headers=AUTH).json()  # noqa: E731
    allow = _refuse_start(monkeypatch, "animethemes-sweep")
    try:
        _refused_with_words(loose.post("/api/admin/animethemes-sweep/start", headers=AUTH))
        st = status()
        # v0.51.344: the page words a failed sweep from its error — a stuck 'running' answered every START already_running
        assert st["status"] == "failed" and REFUSED in st["error"], st
        allow()
        assert loose.post("/api/admin/animethemes-sweep/start", headers=AUTH).json()["started"] is True
        assert _until(status, lambda s: s["status"] != "running", "the sweep to end")["status"] == "done"
        _join("animethemes-sweep")
    finally:
        _reset_at_sweep()


def test_an_orphan_scan_whose_thread_cannot_start_reads_failed_and_the_next_start_runs(tmp_path, monkeypatch):
    import app.core.orphan_scan as orphan_scan
    monkeypatch.setattr(orphan_scan, "scan_plex_upload_placements", lambda db_path, plex, **k: [{"drift_type": "ok"}])
    client, _settings = _orphan_app(tmp_path, monkeypatch)
    loose = TestClient(client.app, raise_server_exceptions=False)
    status = lambda: loose.get("/api/admin/orphan-scan/status", headers=AUTH).json()  # noqa: E731
    with api_mod._ORPHAN_SCAN_LOCK:
        api_mod._ORPHAN_SCAN_STATE.clear()
        api_mod._ORPHAN_SCAN_STATE["status"] = "idle"
    allow = _refuse_start(monkeypatch, "orphan-scan")
    try:
        _refused_with_words(loose.post("/api/admin/orphan-scan/start", headers=AUTH))
        st = status()
        # v0.51.344: a failed start reads failed with its words — not a scan 'running' forever
        assert st["status"] == "failed" and REFUSED in st["error"], st
        allow()
        assert loose.post("/api/admin/orphan-scan/start", headers=AUTH).json()["started"] is True
        assert _until(status, lambda s: s["status"] != "running", "the scan to end")["status"] == "done"
        _join("orphan-scan")
    finally:
        with api_mod._ORPHAN_SCAN_LOCK:
            api_mod._ORPHAN_SCAN_STATE.clear()
            api_mod._ORPHAN_SCAN_STATE["status"] = "idle"


def test_a_loudness_audit_whose_thread_cannot_start_reads_failed_and_the_next_start_runs(tmp_path, monkeypatch):
    from app.core import loudness_audit
    summary = {"to_measure": 0, "measured": 0, "failed": 0, "total_local_bytes": 0, "already_current": 0,
               "skipped_no_sha": 0}
    monkeypatch.setattr(loudness_audit, "run_loudness_audit", lambda db_path, **k: dict(summary))
    loose = TestClient(_loudness_app(tmp_path, monkeypatch).app, raise_server_exceptions=False)
    status = lambda: loose.get("/api/admin/loudness-audit/status", headers=AUTH).json()  # noqa: E731
    with api_mod._LOUDNESS_AUDIT_LOCK:
        api_mod._LOUDNESS_AUDIT_STATE.clear()
        api_mod._LOUDNESS_AUDIT_STATE["status"] = "idle"
    allow = _refuse_start(monkeypatch, "loudness-audit")
    try:
        _refused_with_words(loose.post("/api/admin/loudness-audit/start", headers=AUTH))
        st = status()
        # v0.51.344: a failed start reads failed with its words — not an audit 'running' forever
        assert st["status"] == "failed" and REFUSED in st["error"], st
        allow()
        assert loose.post("/api/admin/loudness-audit/start", headers=AUTH).json()["started"] is True
        assert _until(status, lambda s: s["status"] != "running", "the audit to end")["status"] == "done"
        _join("loudness-audit")
    finally:
        with api_mod._LOUDNESS_AUDIT_LOCK:
            api_mod._LOUDNESS_AUDIT_STATE.clear()
            api_mod._LOUDNESS_AUDIT_STATE["status"] = "idle"
