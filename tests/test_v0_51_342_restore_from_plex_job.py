"""v0.51.342: RESTORE FROM PLEX runs as a page-scoped background job.

  a-e. The POST starts it, and a second start attaches. Status carries progress, then the summary.
       Cancel reaches the bulk. The 409 is decided before the job is claimed. A failure is a named
       state, and the next start is allowed.
  f-i. A marker under config_dir outlives the process: 'running' there reads interrupted, and a
       finished run reads as the last run. Neither a marker nor an audit failure relabels a finished run.
  j-n. RUN CHECK is refused while it runs, and the page SSR-locks. Plex clients come from the config
       and are closed. The endpoints are admin-only, and the marker read stays off the event loop.
  Page: the live bindCanonicalHealth under node — a load mid-run, a poll that fails mid-run, a 504 on
       start, a report landing mid-run, an interrupted run, and a last run the page did not watch.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from _slice_helpers import slice_between
from app.core import canonical_health as ch
from app.core.db import init_db
from test_v0_51_339_canonical_health_restore import (
    _NODE, REPO, _lf, _placement, _report, _row, _run_page, _section, _theme,
)

AUTH = {"X-Authentik-Username": "testadmin"}
START = "/api/admin/canonical-health/restore-from-plex"
STATUS = START + "/status"
CANCEL = START + "/cancel"
CHECK = "/api/admin/canonical-health/check"
JOB_THREAD = "canonical-restore-from-plex"
_HELD: list = []


def _reset_job():
    from app.web import api as api_mod
    with api_mod._CANON_RESTORE_LOCK:
        api_mod._CANON_RESTORE_STATE.clear()
        api_mod._CANON_RESTORE_STATE["status"] = "idle"
    api_mod._CANON_RESTORE_INTERRUPT_LOGGED = False


def _join_job():
    for t in threading.enumerate():
        if t.name == JOB_THREAD:
            t.join(10)
            assert not t.is_alive(), "the restore job thread did not finish"


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.web import api as api_mod
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    (tmp_path / "themes").mkdir(parents=True, exist_ok=True)
    settings._cfg.paths.themes_dir = str(tmp_path / "themes")
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="testadmin", password="testpassword")
    events: list[dict] = []
    monkeypatch.setattr(api_mod, "log_event", lambda _db, **k: events.append(k))
    _reset_job()
    try:
        yield TestClient(api_mod.create_app(settings)), settings, tmp_path, events
    finally:
        for held in _HELD:
            held.release.set()
            held.after_cancel.set()
        _HELD.clear()
        _join_job()
        _reset_job()


class HeldRestore:
    """Stands in for core's restore_from_plex: reports progress, then waits to be released or cancelled."""
    def __init__(self, *, fail=None, released=False, unreachable=False):
        self.release = threading.Event()
        self.entered = threading.Event()
        # v0.51.344: clear after_cancel to hold the run between seeing the cancel and returning — the 'cancelling' window
        self.cancel_seen, self.after_cancel = threading.Event(), threading.Event()
        self.after_cancel.set()
        self.fail = fail
        self.unreachable = unreachable
        self.calls: list[dict] = []
        if released:
            self.release.set()
        _HELD.append(self)

    def __call__(self, db_path, themes_dir, plex_client, *, plex_client_factory=None, progress_cb=None,
                 cancel_check=None, **_kw):
        self.calls.append({"plex_client": plex_client, "factory": plex_client_factory})
        progress_cb(0, 10, {"restored_sidecar": 0, "restored_store": 0, "skipped_count": 0})
        progress_cb(3, 10, {"restored_sidecar": 1, "restored_store": 1, "skipped_count": 1})
        self.entered.set()
        cancelled = False
        while not self.release.wait(0.01):
            if cancel_check():
                cancelled = True
                self.cancel_seen.set()
                self.after_cancel.wait(10)
                break
        if self.fail is not None:
            raise self.fail
        skipped = [{"title": "T1", "media_type": "movie", "tmdb_id": 1, "section_id": "1", "reason": "no_plex_copy"}]
        if cancelled:
            return {"broken": 10, "restored_sidecar": 1, "restored_store": 1, "restored": 2, "skipped": skipped,
                    "cancelled": True, "not_attempted": 7, "plex_unreachable": self.unreachable}
        return {"broken": 10, "restored_sidecar": 5, "restored_store": 4, "restored": 9, "skipped": skipped,
                "cancelled": False, "not_attempted": 0, "plex_unreachable": self.unreachable}


def _status(client):
    r = client.get(STATUS, headers=AUTH)
    assert r.status_code == 200, r.text
    return r.json()


def _wait(client, pred, what):
    end = time.monotonic() + 10
    while True:
        st = _status(client)
        if pred(st):
            return st
        assert time.monotonic() < end, f"timed out waiting for {what}: {st}"
        time.sleep(0.01)


def _finish(client):
    st = _wait(client, lambda s: s["status"] != "running", "the run to finish")
    _join_job()
    return st


def _messages(events):
    return [e["message"] for e in events if str(e.get("message", "")).startswith("Canonical restore from Plex")]


# ── a-e: start, progress, cancel, refusal, failure ───────────────────

def test_a_second_start_attaches_to_the_running_job(env, monkeypatch):
    client, settings, tmp_path, events = env
    held = HeldRestore()
    monkeypatch.setattr(ch, "restore_from_plex", held)
    assert client.post(START, headers=AUTH).json() == {"ok": True, "started": True}
    assert held.entered.wait(10)
    assert client.post(START, headers=AUTH).json() == {"ok": True, "started": False, "already_running": True}
    held.release.set()
    assert _finish(client)["status"] == "done"
    assert len(held.calls) == 1, "a second start ran the bulk again"


def test_status_carries_progress_then_the_summary_the_audit_row_and_the_event(env, monkeypatch):
    client, settings, tmp_path, events = env
    held = HeldRestore()
    monkeypatch.setattr(ch, "restore_from_plex", held)
    client.post(START, headers=AUTH)
    assert held.entered.wait(10)
    st = _wait(client, lambda s: s.get("done") == 3, "progress")
    assert (st["status"], st["total"], st["restored_sidecar"], st["restored_store"], st["skipped_count"]) == (
        "running", 10, 1, 1, 1)
    assert st["cancelling"] is False and st["elapsed_s"] >= 0 and st["actor"] == "testadmin"
    assert "cancel" not in st and "t0" not in st
    marker = settings.config_dir / "canonical_health" / "restore_from_plex.json"
    assert json.loads(marker.read_text())["status"] == "running", "a restart mid-run must find a running marker"
    held.release.set()
    st = _finish(client)
    assert st["status"] == "done"
    assert (st["restored"], st["skipped_count"], st["not_attempted"], st["cancelled"]) == (9, 1, 0, False)
    assert [s["reason"] for s in st["skipped"]] == ["no_plex_copy"]
    assert st["started_at"] <= st["finished_at"]
    assert json.loads(marker.read_text())["status"] == "done"
    with sqlite3.connect(settings.db_path) as conn:
        audits = conn.execute("SELECT actor, details FROM audit_events "
                              "WHERE action = 'canonical_restore_from_plex'").fetchall()
    assert len(audits) == 1 and audits[0][0] == "testadmin"
    assert "skipped" not in json.loads(audits[0][1]), "the audit row carries the counts, not the list"
    assert _messages(events) == [
        "Canonical restore from Plex by testadmin: 5 from sidecars, 4 from Plex's store, 1 skipped of 10 broken"]


def test_cancel_reaches_the_bulk_and_reads_cancelled(env, monkeypatch):
    client, settings, tmp_path, events = env
    held = HeldRestore()
    monkeypatch.setattr(ch, "restore_from_plex", held)
    client.post(START, headers=AUTH)
    assert held.entered.wait(10)
    assert client.post(CANCEL, headers=AUTH).json() == {"ok": True, "cancelling": True}
    st = _finish(client)
    assert (st["status"], st["not_attempted"]) == ("cancelled", 7)
    assert _messages(events) == ["Canonical restore from Plex by testadmin: 1 from sidecars, 1 from Plex's store, "
                                 "1 skipped of 10 broken — cancelled, 7 not tried"]
    assert client.post(CANCEL, headers=AUTH).json() == {"ok": True, "cancelling": False}


def test_status_says_cancelling_between_the_cancel_and_the_run_ending(env, monkeypatch):
    client, settings, tmp_path, events = env
    held = HeldRestore()
    held.after_cancel.clear()
    monkeypatch.setattr(ch, "restore_from_plex", held)
    client.post(START, headers=AUTH)
    assert held.entered.wait(10)
    assert _status(client)["cancelling"] is False
    assert client.post(CANCEL, headers=AUTH).json() == {"ok": True, "cancelling": True}
    assert held.cancel_seen.wait(10), "premise: the run saw the cancel and is still winding down"
    st = _status(client)
    # v0.51.344: the page words '— cancelling…' from this flag, and only its False value was pinned
    assert (st["status"], st["cancelling"]) == ("running", True), st
    held.after_cancel.set()
    assert _finish(client)["status"] == "cancelled"


def test_a_run_plex_never_answered_says_so_in_its_event(env, monkeypatch):
    client, settings, tmp_path, events = env
    monkeypatch.setattr(ch, "restore_from_plex", HeldRestore(released=True, unreachable=True))
    client.post(START, headers=AUTH)
    st = _finish(client)
    assert (st["status"], st["plex_unreachable"]) == ("done", True)
    (message,) = _messages(events)
    # v0.51.344: the events log is where the operator reads why the store rows went untried
    assert message.endswith(" — Plex gave no answer"), message


def test_a_missing_themes_dir_is_refused_before_the_job_is_claimed(env, monkeypatch):
    client, settings, tmp_path, events = env
    held = HeldRestore(released=True)
    monkeypatch.setattr(ch, "restore_from_plex", held)
    settings._cfg.paths.themes_dir = ""
    assert client.post(START, headers=AUTH).status_code == 409
    assert _status(client) == {"status": "idle"}
    assert held.calls == []
    settings._cfg.paths.themes_dir = str(tmp_path / "themes")
    assert client.post(START, headers=AUTH).json()["started"] is True
    assert _finish(client)["status"] == "done"


def test_a_bulk_that_raises_reads_failed_and_the_next_start_runs(env, monkeypatch):
    client, settings, tmp_path, events = env
    monkeypatch.setattr(ch, "restore_from_plex", HeldRestore(fail=RuntimeError("disk vanished"), released=True))
    client.post(START, headers=AUTH)
    st = _finish(client)
    assert st["status"] == "failed" and "disk vanished" in st["error"]
    assert st["restored"] == 2, "the failure names what the run had restored before it stopped"
    warned = [e for e in events if e.get("level") == "WARNING"]
    assert len(warned) == 1 and "failed after 2 restored: disk vanished" in warned[0]["message"]
    assert json.loads((settings.config_dir / "canonical_health" / "restore_from_plex.json").read_text())[
        "status"] == "failed"
    monkeypatch.setattr(ch, "restore_from_plex", HeldRestore(released=True))
    assert client.post(START, headers=AUTH).json()["started"] is True
    assert _finish(client)["status"] == "done"


# ── f-i: the marker ──────────────────────────────────────────────────

def _marker(settings):
    return settings.config_dir / "canonical_health" / "restore_from_plex.json"


def _dead_run(settings, **ago):
    marker = {"status": "running", "started_at": _ago(**(ago or {"days": 3})), "actor": "testadmin"}
    _marker(settings).parent.mkdir(parents=True, exist_ok=True)
    _marker(settings).write_text(json.dumps(marker))
    return marker


def _marker_status(settings, when):
    """os.replace's source is the marker about to land; `when` picks the ones to act on."""
    def check(src, dst):
        return str(dst).endswith("restore_from_plex.json") and when(json.loads(Path(src).read_text())["status"])
    return check


def test_an_interrupted_run_is_reported_once_then_reads_as_a_quiet_last_run(env, monkeypatch, caplog):
    # v0.51.342: reversed — the old test pinned 'interrupted' on every read, an alarm on every visit forever.
    client, settings, tmp_path, events = env
    from app.web import api as api_mod
    dead = _dead_run(settings)
    cut = {**dead, "status": "interrupted"}
    with caplog.at_level(logging.INFO):
        first, second = _status(client), _status(client)
        _reset_job()
        after_restart = _status(TestClient(api_mod.create_app(settings)))
    assert first == {**cut, "first_report": True}
    assert second == after_restart == cut, "the cut-off run was reported as news again"
    assert json.loads(_marker(settings).read_text()) == cut, "the marker still claims a run in progress"
    assert sum("never finished" in r.getMessage() for r in caplog.records) == 1, "the interrupted line is single-shot"
    monkeypatch.setattr(ch, "restore_from_plex", HeldRestore(released=True))
    assert client.post(START, headers=AUTH).json()["started"] is True
    assert _finish(client)["status"] == "done"
    _reset_job()
    assert (_status(client)["status"], json.loads(_marker(settings).read_text())["status"]) == ("done", "done"), \
        "the next run did not replace the cut-off run's marker"


def test_a_cut_off_run_whose_marker_cannot_be_rewritten_still_alarms_once_per_process(env, monkeypatch, caplog):
    client, settings, tmp_path, events = env
    dead = _dead_run(settings)
    real = os.replace
    refused = _marker_status(settings, lambda s: s == "interrupted")

    def replace(src, dst, *a, **k):
        if refused(src, dst):
            raise OSError(28, "No space left on device")
        return real(src, dst, *a, **k)
    monkeypatch.setattr(os, "replace", replace)
    with caplog.at_level(logging.INFO):
        first, second = _status(client), _status(client)
    assert first.get("first_report") is True and "first_report" not in second
    assert second == {**dead, "status": "interrupted"}
    assert any("could not write" in r.getMessage() for r in caplog.records if r.levelno == logging.WARNING)
    assert sum("never finished" in r.getMessage() for r in caplog.records) == 1


class _SpyLock:
    """Wraps a lock: notes when the job thread asks for it, then takes it."""
    def __init__(self, real):
        self.real, self.job_asked = real, threading.Event()

    def __enter__(self):
        if threading.current_thread().name == JOB_THREAD:
            self.job_asked.set()
        return self.real.__enter__()

    def __exit__(self, *exc):
        return self.real.__exit__(*exc)


def test_the_interrupted_rewrite_never_lands_on_a_run_started_while_it_reads(env, monkeypatch):
    client, settings, tmp_path, events = env
    from app.web import api as api_mod
    dead = _dead_run(settings)
    reading, go = threading.Event(), threading.Event()
    real_read = api_mod._canon_restore_read

    def read(s):
        got = real_read(s)
        reading.set()
        assert go.wait(10)
        return got
    monkeypatch.setattr(api_mod, "_canon_restore_read", read)
    spy = _SpyLock(api_mod._CANON_RESTORE_MARKER_LOCK)
    monkeypatch.setattr(api_mod, "_CANON_RESTORE_MARKER_LOCK", spy)
    held = HeldRestore()
    monkeypatch.setattr(ch, "restore_from_plex", held)
    out = {}
    reader = threading.Thread(target=lambda: out.update(st=_status(client)), daemon=True)
    reader.start()
    assert reading.wait(10), "premise: the status read is reading the cut-off run's marker"
    assert client.post(START, headers=AUTH).json()["started"] is True
    # v0.51.344: the reader holds the lock, so the run waits there — no hasattr, no sleep loop
    assert spy.job_asked.wait(10), "the new run never asked for the marker lock"
    go.set()
    reader.join(10)
    assert out["st"] == {**dead, "status": "interrupted", "first_report": True}
    assert held.entered.wait(10)
    on_disk = json.loads(_marker(settings).read_text())
    assert (on_disk["status"], on_disk["started_at"]) != ("interrupted", dead["started_at"]), \
        "the cut-off run's rewrite landed over the marker of the run that just started"
    assert on_disk["status"] == "running"
    held.release.set()
    _finish(client)


def test_a_status_read_that_saw_idle_reports_the_run_claimed_before_it_read_the_marker(env, monkeypatch):
    client, settings, tmp_path, events = env
    from app.web import api as api_mod
    dead = _dead_run(settings)
    held = HeldRestore()
    monkeypatch.setattr(ch, "restore_from_plex", held)
    real_view = api_mod._canon_restore_view
    started: dict = {}

    def view():
        st = real_view()
        if st.get("status") == "idle" and not started:
            # a START lands after the status read saw idle, and its marker is down before the read looks at the file
            starter = threading.Thread(target=lambda: started.update(r=client.post(START, headers=AUTH).json()))
            starter.start()
            starter.join(10)
            assert held.entered.wait(10), "premise: the new run wrote its running marker"
        return st
    monkeypatch.setattr(api_mod, "_canon_restore_view", view)
    st = _status(client)
    assert started["r"]["started"] is True
    assert st["status"] == "running" and st["started_at"] != dead["started_at"], \
        f"the read reported the old cut-off run while a new one was running: {st}"
    on_disk = json.loads(_marker(settings).read_text())
    assert (on_disk["status"], on_disk["started_at"]) == ("running", st["started_at"]), \
        "the new run's marker was rewritten as interrupted"
    held.release.set()
    _finish(client)


class _HoldFinalMarker:
    """Wraps the marker lock: the first run's second take (its final marker) waits for `go` before it takes the lock."""
    def __init__(self, real):
        self.real, self.first, self.takes = real, None, 0
        self.holding, self.go = threading.Event(), threading.Event()

    def __enter__(self):
        me = threading.current_thread()
        if me.name == JOB_THREAD and self.first in (None, me):
            self.first = me
            self.takes += 1
            if self.takes == 2:
                self.holding.set()
                assert self.go.wait(10)
        return self.real.__enter__()

    def __exit__(self, *exc):
        return self.real.__exit__(*exc)


@pytest.mark.parametrize("ending", ["done", "failed"])
def test_a_start_racing_the_finish_keeps_the_new_runs_marker(env, monkeypatch, ending):
    client, settings, tmp_path, events = env
    from app.web import api as api_mod
    first = HeldRestore(released=True, fail=RuntimeError("disk vanished") if ending == "failed" else None)
    second = HeldRestore()
    runs = iter([first, second])
    monkeypatch.setattr(ch, "restore_from_plex", lambda *a, **k: next(runs)(*a, **k))
    hold = _HoldFinalMarker(api_mod._CANON_RESTORE_MARKER_LOCK)
    monkeypatch.setattr(api_mod, "_CANON_RESTORE_MARKER_LOCK", hold)
    in_final, go = hold.holding, hold.go
    client.post(START, headers=AUTH)
    assert in_final.wait(10), "premise: the first run is about to write its final marker"
    (first_thread,) = [t for t in threading.enumerate() if t.name == JOB_THREAD]
    racing = client.post(START, headers=AUTH).json()
    if racing["started"]:
        assert second.entered.wait(10), "premise: the second run's marker is down before the first's lands"
    go.set()
    first_thread.join(10)
    assert not first_thread.is_alive()
    if not racing["started"]:
        assert racing["already_running"] is True
        assert client.post(START, headers=AUTH).json()["started"] is True
        assert second.entered.wait(10)
    assert _status(client)["status"] == "running"
    assert json.loads(_marker(settings).read_text())["status"] == "running", \
        "the finished run's marker overwrote the running one — a restart now would not read interrupted"
    second.release.set()
    assert _finish(client)["status"] == "done"


@pytest.mark.parametrize("remove_fails", [False, True], ids=["removed", "remove-fails"])
def test_a_final_marker_that_cannot_be_written_leaves_no_run_claiming_a_restart(env, monkeypatch, caplog,
                                                                                 remove_fails):
    client, settings, tmp_path, events = env
    real_replace, real_unlink = os.replace, os.unlink
    is_final = _marker_status(settings, lambda s: s != "running")

    def replace(src, dst, *a, **k):
        if is_final(src, dst):
            raise OSError(28, "No space left on device")
        return real_replace(src, dst, *a, **k)

    def unlink(p, *a, **k):
        if remove_fails and str(p).endswith("restore_from_plex.json"):
            raise OSError(13, "Permission denied")
        return real_unlink(p, *a, **k)
    monkeypatch.setattr(os, "replace", replace)
    monkeypatch.setattr(os, "unlink", unlink)
    held = HeldRestore()
    monkeypatch.setattr(ch, "restore_from_plex", held)
    with caplog.at_level(logging.WARNING):
        client.post(START, headers=AUTH)
        assert held.entered.wait(10)
        assert json.loads(_marker(settings).read_text())["status"] == "running", "premise: the running marker landed"
        held.release.set()
        assert _finish(client)["status"] == "done", "a marker failure relabelled the run"
    warned = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert any("could not write" in m for m in warned), warned
    assert not list(_marker(settings).parent.glob(".restore-*")), "the temp marker was left behind"
    if remove_fails:
        assert any("could not remove" in m and "cut off" in m for m in warned), warned
        return
    assert not _marker(settings).exists(), "the running marker outlived the finished run"
    _reset_job()
    assert _status(client) == {"status": "idle"}, "after a restart the finished run read as cut off"


def test_a_finished_run_reads_as_the_last_run_after_a_restart(env, monkeypatch):
    client, settings, tmp_path, events = env
    from app.web import api as api_mod
    monkeypatch.setattr(ch, "restore_from_plex", HeldRestore(released=True))
    client.post(START, headers=AUTH)
    assert _finish(client)["status"] == "done"
    _reset_job()
    fresh = TestClient(api_mod.create_app(settings))
    st = _status(fresh)
    assert (st["status"], st["restored"], st["skipped_count"]) == ("done", 9, 1) and st["finished_at"]


def test_a_marker_that_cannot_be_written_never_fails_the_job(env, monkeypatch, caplog):
    client, settings, tmp_path, events = env
    real = os.replace

    def replace(src, dst, *a, **k):
        if str(dst).endswith("restore_from_plex.json"):
            raise OSError(28, "No space left on device")
        return real(src, dst, *a, **k)
    monkeypatch.setattr(os, "replace", replace)
    monkeypatch.setattr(ch, "restore_from_plex", HeldRestore(released=True))
    with caplog.at_level(logging.WARNING):
        client.post(START, headers=AUTH)
        st = _finish(client)
    assert st["status"] == "done"
    assert any("could not write" in r.getMessage() for r in caplog.records if r.levelno == logging.WARNING)
    assert not list(_marker(settings).parent.glob(".restore-*")), "the temp marker was left behind"


def test_an_audit_failure_does_not_relabel_a_finished_run(env, monkeypatch, caplog):
    client, settings, tmp_path, events = env
    from app.web import api as api_mod

    def boom(*a, **k):
        raise sqlite3.OperationalError("disk I/O error")
    monkeypatch.setattr(api_mod, "_record_audit", boom)
    monkeypatch.setattr(ch, "restore_from_plex", HeldRestore(released=True))
    with caplog.at_level(logging.WARNING):
        client.post(START, headers=AUTH)
        st = _finish(client)
    assert st["status"] == "done"
    assert any("audit row" in r.getMessage() for r in caplog.records if r.levelno == logging.WARNING)


# ── j-n: RUN CHECK, the page, Plex, auth, the event loop ─────────────

def test_run_check_is_refused_while_the_job_runs(env, monkeypatch):
    client, settings, tmp_path, events = env
    held = HeldRestore()
    monkeypatch.setattr(ch, "restore_from_plex", held)
    client.post(START, headers=AUTH)
    assert held.entered.wait(10)
    r = client.post(CHECK, headers=AUTH)
    assert r.status_code == 409 and "RESTORE FROM PLEX is running" in r.json()["detail"]
    held.release.set()
    _finish(client)
    assert client.post(CHECK, headers=AUTH).status_code == 200


def _button(html, element_id):
    m = re.search(r'<button[^>]*\bid="%s"[^>]*>([^<]*)</button>' % re.escape(element_id), html)
    assert m, f"no #{element_id} button"
    return m.group(0), m.group(1)


def _status_span(html):
    m = re.search(r'<span[^>]*\bid="canon-restore-plex-status"[^>]*>', html)
    assert m, "no #canon-restore-plex-status span"
    return m.group(0)


def test_the_page_ssr_locks_restore_while_the_job_runs(env, monkeypatch):
    client, settings, tmp_path, events = env
    idle = client.get("/admin/canonical-health", headers=AUTH).text
    tag, _label = _button(idle, "canon-restore-plex-btn")
    assert " disabled" not in tag and 'style="display:none"' in tag
    assert 'style="display:none"' in _button(idle, "canon-restore-plex-cancel-btn")[0]
    assert "data-running" not in _status_span(idle)
    held = HeldRestore()
    monkeypatch.setattr(ch, "restore_from_plex", held)
    client.post(START, headers=AUTH)
    assert held.entered.wait(10)
    busy = client.get("/admin/canonical-health", headers=AUTH).text
    tag, label = _button(busy, "canon-restore-plex-btn")
    assert " disabled" in tag and "display:none" not in tag and label == "// RESTORING…"
    ctag, clabel = _button(busy, "canon-restore-plex-cancel-btn")
    assert "display:none" not in ctag and clabel == "// CANCEL"
    # v0.51.342: what the binder seeds its running state from — the ssr_running fixture reads this render.
    assert 'data-running="1"' in _status_span(busy)
    held.release.set()
    _finish(client)


def test_the_runner_builds_plex_clients_from_the_config_and_closes_them(env, monkeypatch):
    client, settings, tmp_path, events = env
    from app.web import api as api_mod
    with sqlite3.connect(settings.db_path) as conn:
        _section(conn)
        _theme(conn, 1501)
        _lf(conn, 1501)
        _placement(conn, 1501, "", kind="plex_upload", rk="91501")
        conn.commit()
    settings._cfg.plex.enabled = True
    settings._cfg.plex.url = "http://plex.test:32400"
    settings._cfg.plex.token = "token-for-test"
    made = []

    class FakeClient:
        def __init__(self, cfg, *, plus_mode):
            self.cfg, self.plus_mode, self.closed = cfg, plus_mode, False
            made.append(self)

        def get_themes(self, *, rating_key):
            return {"ok": True, "http_status": 200, "error": None,
                    "body": {"MediaContainer": {"Metadata": [{"ratingKey": "upload://themes/a", "selected": True}]}}}

        def fetch_theme_bytes(self, *, item_rating_key, entry_uri):
            return {"ok": True, "http_status": 200, "bytes": b"store-1501"}

        def close(self):
            self.closed = True
    monkeypatch.setattr(api_mod, "PlexClient", FakeClient)
    client.post(START, headers=AUTH)
    st = _finish(client)
    assert (st["status"], st["restored_store"], st["plex"]) == ("done", 1, True)
    assert made and all(c.closed for c in made), "a Plex client the runner built was left open"
    assert {(c.cfg.url, c.cfg.token, c.plus_mode) for c in made} == {
        ("http://plex.test:32400", "token-for-test", settings.plus_equiv_mode)}
    assert (tmp_path / "themes" / "movies" / "1501" / "theme.mp3").read_bytes() == b"store-1501"


def test_the_job_endpoints_are_admin_only(env):
    client, settings, tmp_path, events = env
    from app.core.auth import create_api_token
    _id, raw = create_api_token(settings.db_path, name="homepage", scope="read")
    read_only = {"Authorization": f"Bearer {raw}"}
    for meth, path in (("post", START), ("get", STATUS), ("post", CANCEL)):
        assert getattr(client, meth)(path).status_code in (401, 403), path
        assert getattr(client, meth)(path, headers=read_only).status_code == 403, path


def test_the_marker_read_runs_off_the_event_loop(env, monkeypatch):
    client, settings, tmp_path, events = env
    from app.web import api as api_mod
    real = api_mod._canon_restore_read
    where = []

    def read(s):
        try:
            asyncio.get_running_loop()
            where.append("event loop")
        except RuntimeError:
            where.append("worker thread")
        return real(s)
    monkeypatch.setattr(api_mod, "_canon_restore_read", read)
    assert _status(client) == {"status": "idle"}
    assert where == ["worker thread"], "a file read on the event loop freezes every request (class 12)"


# ── the page, under node ─────────────────────────────────────────────

def _ago(**delta) -> str:
    return (datetime.now(timezone.utc) - timedelta(**delta)).isoformat(timespec="seconds")


_RUNNING = {"status": "running", "stage": "restoring", "done": 3, "total": 10, "restored_sidecar": 1,
            "restored_store": 1, "skipped_count": 1, "cancelling": False, "elapsed_s": 4.0}


def _finished(**extra):
    return {"status": "done", "broken": 10, "restored": 9, "restored_sidecar": 5, "restored_store": 4,
            "skipped": [{"title": "T1", "media_type": "movie", "tmdb_id": 1, "section_id": "1",
                         "reason": "plex_unreachable"}],
            "skipped_count": 1, "not_attempted": 0, "cancelled": False, "plex_unreachable": True,
            "finished_at": _ago(seconds=5), **extra}


@pytest.mark.skipif(not _NODE, reason="node not installed")
def test_page_loaded_mid_run_attaches_then_shows_the_result(tmp_path):
    page = _report(missing=[_row(1601, "Held Title", "store")], restorable=1)
    s0, s1 = _run_page(tmp_path, [page, _RUNNING, _finished(), _report()], ["tick"])
    btn = s0["canon-restore-plex-btn"]
    assert (btn["text"], btn["disabled"], btn["display"]) == ("// RESTORING…", True, "")
    assert s0["canon-restore-plex-cancel-btn"]["display"] == ""
    assert s0["canon-check-btn"]["disabled"] is True and s0["canon-repair-btn"]["disabled"] is True
    assert s0["canon-restore-plex-status"]["text"] == "restoring 3 / 10 · 2 restored · 1 skipped"
    status = s1["canon-restore-plex-status"]
    assert status["text"] == ("✓ restored 9 (5 from Plex folders, 4 from Plex's store) · "
                              "1 skipped (1 Plex gave no answer — not tried)")
    assert status["className"] == "form-status form-status-ok"
    assert s1["canon-check-btn"]["disabled"] is False and s1["canon-repair-btn"]["disabled"] is False
    assert s1["canon-restore-plex-cancel-btn"]["display"] == "none"
    assert s1["canon-missing-block"]["display"] == "", "a watched run's result stays up once nothing is missing"


@pytest.mark.skipif(not _NODE, reason="node not installed")
def test_a_status_poll_that_fails_mid_run_tries_again_instead_of_dropping_the_run(tmp_path):
    page = _report(missing=[_row(1604, "Blip Title", "store")], restorable=1)
    _s0, s1, s2, s3 = _run_page(tmp_path, [page, _RUNNING, {"__throw": {"status": 502}}, _finished(), _report()],
                                ["tick", "tick", "tick"])
    assert (s1["canon-restore-plex-btn"]["text"], s1["canon-check-btn"]["disabled"]) == ("// RESTORING…", True), \
        "one failed poll ended the run on the page"
    status = s2["canon-restore-plex-status"]
    assert status["text"].startswith("✓ restored 9 ") and status["className"] == "form-status form-status-ok"
    for s in (s2, s3):
        assert (s["canon-check-btn"]["disabled"], s["canon-repair-btn"]["disabled"],
                s["canon-restore-plex-btn"]["disabled"]) == (False, False, False)


@pytest.mark.skipif(not _NODE, reason="node not installed")
def test_a_504_on_start_shows_the_note_then_attaches(tmp_path):
    page = _report(missing=[_row(1602, "Proxy Title", "store")], restorable=1)
    _s0, s1 = _run_page(tmp_path, [page, {"status": "idle"}, {"__throw": {"status": 504}},
                                   dict(_RUNNING, stage="listing", done=0, total=0)], ["canon-restore-plex-btn"])
    texts = s1["canon-restore-plex-status"]["texts"]
    assert any(t.startswith("504: the reverse proxy timed out") for t in texts), texts
    assert s1["canon-restore-plex-status"]["text"] == "listing broken rows…"
    assert s1["canon-restore-plex-btn"]["disabled"] is True and s1["canon-restore-plex-cancel-btn"]["display"] == ""


@pytest.mark.skipif(not _NODE, reason="node not installed")
def test_a_report_landing_mid_run_keeps_the_busy_label(tmp_path):
    page = _report(missing=[_row(1603, "Busy Title", "store")], restorable=4)
    later = _report(missing=[_row(1603, "Busy Title", "store")], restorable=2)
    s0, s1 = _run_page(tmp_path, [page, _RUNNING, later], ["canon-check-btn"])
    for s in (s0, s1):
        assert (s["canon-restore-plex-btn"]["text"], s["canon-restore-plex-btn"]["disabled"]) == ("// RESTORING…", True)
    assert s1["canon-check-btn"]["disabled"] is True, "the check's own finally unlocked CHECK mid-run"


@pytest.fixture
def ssr_running(tmp_path_factory, monkeypatch):
    """The restore controls as /admin/canonical-health renders them while a run is going — what the binder seeds from."""
    # v0.51.344: derived from the render — a hand-written copy seeds a state the template may no longer produce
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.web import api as api_mod
    root = tmp_path_factory.mktemp("ssr-running")
    with monkeypatch.context() as m:
        m.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
        m.setenv("MOTIF_CONFIG_DIR", str(root))
        m.setenv("MOTIF_DATA_DIR", str(root / "data"))
        settings = Settings(config_dir=root, data_dir=root / "data")
        (root / "themes").mkdir(parents=True, exist_ok=True)
        settings._cfg.paths.themes_dir = str(root / "themes")
        init_db(settings.db_path)
        init_auth_schema(settings.db_path)
        create_admin(settings.db_path, username="testadmin", password="testpassword")
        app = api_mod.create_app(settings)
        with api_mod._CANON_RESTORE_LOCK:
            saved = dict(api_mod._CANON_RESTORE_STATE)
            assert saved.get("status") != "running", "a run is going — the fixture would relabel its state"
            api_mod._CANON_RESTORE_STATE["status"] = "running"
        try:
            html = TestClient(app).get("/admin/canonical-health", headers=AUTH).text
        finally:
            with api_mod._CANON_RESTORE_LOCK:
                api_mod._CANON_RESTORE_STATE.clear()
                api_mod._CANON_RESTORE_STATE.update(saved)
    btag, blabel = _button(html, "canon-restore-plex-btn")
    ctag, _clabel = _button(html, "canon-restore-plex-cancel-btn")
    span = re.search(r'(<span[^>]*\bid="canon-restore-plex-status"[^>]*>)([^<]*)</span>', html)
    assert span, "no #canon-restore-plex-status span"
    dataset = {re.sub(r"-([a-z])", lambda g: g.group(1).upper(), k): v
               for k, v in re.findall(r'\sdata-([a-z-]+)="([^"]*)"', span.group(1))}

    def shown(tag):
        return "none" if re.search(r'style="[^"]*display:\s*none', tag) else ""
    return {"canon-restore-plex-btn": {"disabled": bool(re.search(r"\sdisabled\b", btag)), "display": shown(btag),
                                       "text": blabel},
            "canon-restore-plex-cancel-btn": {"display": shown(ctag)},
            "canon-restore-plex-status": {"text": span.group(2), "dataset": dataset}}


@pytest.mark.skipif(not _NODE, reason="node not installed")
def test_a_page_rendered_mid_run_whose_first_poll_fails_keeps_polling(tmp_path, ssr_running):
    page = _report(missing=[_row(1605, "Load Blip", "store")], restorable=3)
    after = _report(missing=[_row(1605, "Load Blip", "store")], restorable=1)
    s0, s1, s2 = _run_page(tmp_path, [page, {"__throw": {"status": 502}}, _RUNNING, _finished(), after],
                           ["tick", "tick"], ssr=ssr_running)
    btn = s0["canon-restore-plex-btn"]
    assert (btn["text"], btn["disabled"], btn["display"]) == ("// RESTORING…", True, ""), \
        "the report relabelled the server-rendered busy button"
    # v0.51.344: a failed first poll leaves the server's words on the line — neither blanked nor replaced
    assert s0["canon-restore-plex-status"]["text"] == ssr_running["canon-restore-plex-status"]["text"] != ""
    assert s0["canon-restore-plex-cancel-btn"]["display"] == ""
    assert (s0["canon-check-btn"]["disabled"], s0["canon-repair-btn"]["disabled"]) == (True, True)
    assert s0["__timers"] == 1, "one failed poll at load left nothing polling the run"
    assert s1["canon-restore-plex-status"]["text"] == "restoring 3 / 10 · 2 restored · 1 skipped"
    status = s2["canon-restore-plex-status"]
    assert status["text"].startswith("✓ restored 9 ") and status["className"] == "form-status form-status-ok"
    btn = s2["canon-restore-plex-btn"]
    assert (btn["text"], btn["disabled"], btn["display"]) == ("// RESTORE FROM PLEX (1)", False, "")
    assert (s2["canon-check-btn"]["disabled"], s2["canon-repair-btn"]["disabled"]) == (False, False)
    assert (s2["canon-restore-plex-cancel-btn"]["display"], s2["__timers"]) == ("none", 0)


@pytest.mark.skipif(not _NODE, reason="node not installed")
def test_a_page_rendered_mid_run_whose_first_poll_is_done_shows_the_result_as_watched(tmp_path, ssr_running):
    page = _report(missing=[_row(1608, "Quick Title", "store")], restorable=1)
    (s0,) = _run_page(tmp_path, [page, _finished(), _report()], [], ssr=ssr_running)
    status = s0["canon-restore-plex-status"]
    # v0.51.344: a run the server rendered as going is this page's own — its result is not a 'last run'
    assert (status["text"], status["className"]) == (
        "✓ restored 9 (5 from Plex folders, 4 from Plex's store) · 1 skipped (1 Plex gave no answer — not tried)",
        "form-status form-status-ok")
    assert (s0["canon-check-btn"]["disabled"], s0["canon-repair-btn"]["disabled"], s0["canon-restore-plex-btn"]["disabled"],
            s0["canon-restore-plex-cancel-btn"]["display"], s0["__timers"]) == (False, False, False, "none", 0)


@pytest.mark.skipif(not _NODE, reason="node not installed")
@pytest.mark.parametrize("load_answer", [{"status": "idle"}, dict(_RUNNING, stage="listing", done=0, total=0)],
                         ids=["idle-before-the-start", "running"])
def test_a_load_poll_answered_after_a_click_start_leaves_one_poll_chain(tmp_path, load_answer):
    page = _report(missing=[_row(1606, "Race Title", "store")], restorable=1)
    held = {"__hold": {"key": "load", "value": load_answer}}
    s0, s1, s2, s3 = _run_page(tmp_path, [page, held, {"ok": True, "started": True}, _RUNNING, _finished(),
                                          _report()], ["canon-restore-plex-btn", "release:load", "tick"])
    assert (s0["__timers"], s1["__timers"]) == (0, 1)
    assert s2["__timers"] == 1, "the load's poll, answered after the start, began a second poll chain"
    btn = s2["canon-restore-plex-btn"]
    assert (btn["text"], btn["disabled"]) == ("// RESTORING…", True), "a stale answer unlocked the running restore"
    assert s2["canon-restore-plex-status"]["text"] == "restoring 3 / 10 · 2 restored · 1 skipped"
    assert s3["canon-restore-plex-status"]["text"] == ("✓ restored 9 (5 from Plex folders, 4 from Plex's store) · "
                                                       "1 skipped (1 Plex gave no answer — not tried)"), \
        "the watched run's result was rewritten as a last run"
    assert s3["__timers"] == 0


@pytest.mark.skipif(not _NODE, reason="node not installed")
def test_the_page_alarms_on_the_first_report_only_and_not_when_nothing_is_restorable(env):
    client, settings, tmp_path, events = env
    _dead_run(settings, days=3)
    first, later = _status(client), _status(client)
    (s0,) = _run_page(tmp_path / "first", [_report(), first], [])
    status = s0["canon-restore-plex-status"]
    assert status["className"] == "form-status form-status-fail"
    assert status["text"].startswith("✗ the run started 3d ago was cut off by a motif restart — RUN CHECK")
    assert s0["canon-missing-block"]["display"] == "", "the one report is shown though nothing is listed"
    (s1,) = _run_page(tmp_path / "later", [_report(), later], [])
    assert s1["canon-missing-block"]["display"] == "none", "nothing restorable, yet the cut-off run held the block open"
    assert (s1["canon-restore-plex-status"]["text"], s1["canon-restore-plex-status"]["className"]) == (
        "last run started 3d ago: cut off by a motif restart", "form-status")
    (s2,) = _run_page(tmp_path / "left", [_report(missing=[_row(1607, "Left Title", "store")], restorable=1),
                                          later], [])
    btn = s2["canon-restore-plex-btn"]
    assert (s2["canon-missing-block"]["display"], btn["display"], btn["disabled"]) == ("", "", False)
    assert s2["canon-restore-plex-status"]["className"] == "form-status"


def test_the_settings_restore_hint_sends_the_operator_to_run_check():
    html = (REPO / "app" / "web" / "templates" / "settings.html").read_text()
    hint = " ".join(slice_between(html, '<a href="/admin/canonical-health">// CANONICAL HEALTH</a>', "</p>").split())
    assert "// RUN CHECK" in hint and "Not checked yet" in hint, hint


@pytest.mark.skipif(not _NODE, reason="node not installed")
def test_an_interrupted_run_on_load_says_restart_and_run_check_first(tmp_path):
    # v0.51.342: the alarm is the first report's — later reads carry no first_report.
    (s0,) = _run_page(tmp_path, [_report(), {"status": "interrupted", "started_at": _ago(hours=2),
                                             "actor": "testadmin", "first_report": True}], [])
    status = s0["canon-restore-plex-status"]
    assert status["className"] == "form-status form-status-fail"
    assert status["text"].startswith("✗ the run started 2h ago was cut off by a motif restart — RUN CHECK")
    assert s0["canon-missing-block"]["display"] == "", "the note is shown though nothing is listed"
    assert s0["canon-restore-plex-btn"]["disabled"] is False


@pytest.mark.skipif(not _NODE, reason="node not installed")
def test_a_last_run_the_page_did_not_watch_is_labelled_and_does_not_force_the_block(tmp_path):
    (s0,) = _run_page(tmp_path, [_report(), _finished(finished_at=_ago(hours=3))], [])
    assert s0["canon-restore-plex-status"]["text"].startswith("last run 3h ago: ✓ restored 9")
    assert s0["canon-missing-block"]["display"] == "none", "an old run's summary must not hold the block open"


@pytest.mark.skipif(not _NODE, reason="node not installed")
def test_a_watched_run_cut_off_without_a_first_report_still_alarms(env, monkeypatch, ssr_running):
    client, settings, tmp_path, events = env
    from app.web import api as api_mod
    monkeypatch.setattr(api_mod, "_CANON_RESTORE_SHUTDOWN", threading.Event())
    monkeypatch.setattr(api_mod, "_CANON_RESTORE_THREAD", None)
    held = HeldRestore()
    monkeypatch.setattr(ch, "restore_from_plex", held)
    assert client.post(START, headers=AUTH).json()["started"] is True
    assert held.entered.wait(10)
    api_mod.canon_restore_shutdown().join(10)
    cut = _status(client)
    assert cut["status"] == "interrupted" and "first_report" not in cut, f"premise: a live cut-off carries no first_report: {cut}"
    _s0, s1 = _run_page(tmp_path / "page", [_report(), _RUNNING, cut, _report()], ["tick"], ssr=ssr_running)
    status = s1["canon-restore-plex-status"]
    # v0.51.344: the watch alone makes this an alarm — first_report belongs to the marker read after a restart;
    # the run's counts follow the words (R2-F6: a cut-off run's skipped rows reach the page)
    assert status["className"] == "form-status form-status-fail", status
    assert re.fullmatch(r"✗ the run started .+ was cut off by a motif restart — RUN CHECK, then RESTORE FROM PLEX "
                        r"restores what is left · restored 2 \(1 from Plex folders, 1 from Plex's store\) · "
                        r"1 skipped \(1 no Plex copy\) · 7 not tried", status["text"]), status
    assert (s1["canon-missing-block"]["display"], s1["__timers"]) == ("", 0)


def _real_ending(client, monkeypatch, ending):
    """A run that really ended failed or cancelled, and the status the endpoint then gives."""
    if ending == "failed":
        monkeypatch.setattr(ch, "restore_from_plex", HeldRestore(fail=RuntimeError("disk vanished"), released=True))
        assert client.post(START, headers=AUTH).json()["started"] is True
    else:
        held = HeldRestore()
        monkeypatch.setattr(ch, "restore_from_plex", held)
        assert client.post(START, headers=AUTH).json()["started"] is True
        assert held.entered.wait(10)
        assert client.post(CANCEL, headers=AUTH).json()["cancelling"] is True
    st = _finish(client)
    assert st["status"] == ending, st
    return st


@pytest.mark.skipif(not _NODE, reason="node not installed")
@pytest.mark.parametrize("ending", ["failed", "cancelled"])
def test_a_real_failed_or_cancelled_run_is_worded_on_load_and_when_watched(env, monkeypatch, ssr_running, ending):
    client, settings, tmp_path, events = env
    st = _real_ending(client, monkeypatch, ending)
    if ending == "failed":
        words = f"✗ restore failed — {st['error']} ({st['restored']} restored before it stopped)"
        cls = "form-status form-status-fail"
    else:
        words = (f"✓ cancelled — restored {st['restored']} ({st['restored_sidecar']} from Plex folders, "
                 f"{st['restored_store']} from Plex's store) · 1 skipped (1 no Plex copy) · "
                 f"{st['not_attempted']} not tried")
        cls = "form-status form-status-ok"
    # v0.51.344: payloads from real runs — the failed and cancelled words were only ever fed hand-written ones
    (loaded,) = _run_page(tmp_path / "loaded", [_report(), st], [])
    status = loaded["canon-restore-plex-status"]
    assert status["text"].startswith("last run ") and status["text"].endswith(": " + words), status
    assert status["className"] == cls
    (watched,) = _run_page(tmp_path / "watched", [_report(), st, _report()], [], ssr=ssr_running)
    status = watched["canon-restore-plex-status"]
    assert (status["text"], status["className"]) == (words, cls)
