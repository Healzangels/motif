"""v0.51.344 integration fixes R1-F1 / R1-F2: exit cancels a RESTORE FROM PLEX at the signal, and records a run it leaves on Plex.

  F1. The server's handle_exit records the signal and cancels the job at once — before uvicorn's drain. The cancel it sends
      never waits for the job lock (the signal handler runs on the event loop's thread, which may hold it), and the job's
      cancel poll still sees it.
  F2. A job still on Plex after exit's deadline: exit itself writes the audit row and event — 'interrupted', or 'cancelled'
      plus a cancelled marker after a user cancel — and the job thread skips its own record after. A job that claimed its
      record first keeps it; exit writes nothing then.
"""
from __future__ import annotations

import json
import logging
import signal
import sqlite3
import threading
import time

import uvicorn

from app.core import canonical_health as ch
from test_v0_51_342_restore_from_plex_job import (  # noqa: F401 — env is the job endpoints' fixture
    AUTH, CANCEL, JOB_THREAD, START, HeldRestore, _marker, _reset_job, _status, env,
)


def _audits(db):
    with sqlite3.connect(db) as conn:
        return [json.loads(d) for (d,) in conn.execute(
            "SELECT details FROM audit_events WHERE action = 'canonical_restore_from_plex' ORDER BY id")]


def _held_on_plex(client, monkeypatch):
    """A run that reported 3/10 rows (2 restored, 1 skipped) and now waits on Plex: it sees a cancel, then holds."""
    from app.web import api as api_mod
    monkeypatch.setattr(api_mod, "_CANON_RESTORE_SHUTDOWN", threading.Event())
    monkeypatch.setattr(api_mod, "_CANON_RESTORE_THREAD", None)
    held = HeldRestore()
    held.after_cancel.clear()
    monkeypatch.setattr(ch, "restore_from_plex", held)
    assert client.post(START, headers=AUTH).json()["started"] is True
    assert held.entered.wait(10), "premise: the job reported progress and is waiting on Plex"
    return api_mod, held


# ── F2: exit records the run it leaves on Plex ────────────────────────

def test_exit_records_a_run_still_on_plex_as_interrupted_and_the_job_skips_its_own_record(env, monkeypatch):
    client, settings, tmp_path, events = env
    api_mod, held = _held_on_plex(client, monkeypatch)
    job = api_mod.canon_restore_shutdown()
    assert job is not None and held.cancel_seen.wait(10), "premise: the job saw the cancel and still waits on Plex"
    assert (_audits(settings.db_path), events) == ([], []), "premise: nothing recorded while the job is on Plex"
    assert api_mod.canon_restore_exit_record() == "interrupted"
    audits = _audits(settings.db_path)
    assert [(a["status"], a["restored"], a["restored_sidecar"], a["restored_store"], a["done"], a["total"],
             a["not_attempted"]) for a in audits] == [("interrupted", 2, 1, 1, 3, 10, 7)], audits
    assert "waiting on Plex" in audits[0]["how"]
    assert [(e["level"], e["message"]) for e in events] == [
        ("WARNING", "Canonical restore from Plex by testadmin was cut off by a motif shutdown after 2 restored — "
                    "RUN CHECK after the restart")]
    assert json.loads(_marker(settings).read_text())["status"] == "running", "the next start must report it cut off"
    assert api_mod.canon_restore_exit_record() is None, "a second exit record wrote a second row"
    st = _status(client)
    assert st["status"] == "interrupted"
    assert not {"marked", "recorded_by", "cancel", "t0", "user_cancel"} & st.keys(), "a private key reached the page"
    held.after_cancel.set()
    job.join(10)
    assert not job.is_alive()
    assert (_audits(settings.db_path), len(events)) == (audits, 1), "the job thread wrote its own record after exit's"
    assert json.loads(_marker(settings).read_text())["status"] == "running"
    assert _status(client)["status"] == "interrupted"


def test_exits_record_waits_a_bounded_time_for_a_database_another_writer_holds(env, monkeypatch, caplog):
    client, settings, tmp_path, events = env
    api_mod, held = _held_on_plex(client, monkeypatch)
    job = api_mod.canon_restore_shutdown()
    assert job is not None and held.cancel_seen.wait(10)
    writer = sqlite3.connect(settings.db_path, timeout=1.0)
    writer.execute("BEGIN IMMEDIATE")  # a sync or scan mid-transaction as docker stops motif
    try:
        with caplog.at_level(logging.WARNING):
            t0 = time.monotonic()
            assert api_mod.canon_restore_exit_record() == "interrupted"
            took = time.monotonic() - t0
    finally:
        writer.rollback()
        writer.close()
    assert took < 3.0, f"exit's record waited {took:.1f} s for the database — docker's grace is 10 s"
    assert [r.getMessage() for r in caplog.records
            if r.levelno == logging.WARNING and "audit row / event could not be written" in r.getMessage()], \
        "a record that gave up on the locked database left no breadcrumb"
    assert json.loads(_marker(settings).read_text())["status"] == "running", "the next start still reports it cut off"
    held.after_cancel.set()
    job.join(10)
    assert not job.is_alive()
    assert (_audits(settings.db_path), events) == ([], []), "the job thread wrote a record after exit had claimed it"


def test_exit_records_a_user_cancelled_run_still_on_plex_as_cancelled_with_its_marker(env, monkeypatch):
    client, settings, tmp_path, events = env
    api_mod, held = _held_on_plex(client, monkeypatch)
    assert client.post(CANCEL, headers=AUTH).json() == {"ok": True, "cancelling": True}
    assert held.cancel_seen.wait(10), "premise: the job saw the user's cancel and still waits on Plex"
    job = api_mod.canon_restore_shutdown()
    assert job is not None
    assert api_mod.canon_restore_exit_record() == "cancelled"
    marker = json.loads(_marker(settings).read_text())
    assert (marker["status"], marker["actor"], marker["cancelled"], marker["restored"], marker["restored_sidecar"],
            marker["restored_store"], marker["skipped_count"], marker["broken"], marker["not_attempted"]) == \
        ("cancelled", "testadmin", True, 2, 1, 1, 1, 10, 7), marker
    assert marker["finished_at"] and marker["started_at"]
    audits = _audits(settings.db_path)
    assert [(a["cancelled"], a["restored"], a["not_attempted"], a.get("status")) for a in audits] == [(True, 2, 7, None)]
    assert "waited on Plex" in audits[0]["how"]
    assert [(e["level"], e["message"]) for e in events] == [
        ("INFO", "Canonical restore from Plex by testadmin: 1 from sidecars, 1 from Plex's store, 1 skipped of 10 broken "
                 "— cancelled, 7 not tried (motif stopped while it waited on Plex)")]
    assert _status(client)["status"] == "cancelled"
    held.after_cancel.set()
    job.join(10)
    assert not job.is_alive()
    assert (_audits(settings.db_path), len(events)) == (audits, 1), "the job thread wrote its own record after exit's"
    assert json.loads(_marker(settings).read_text()) == marker, "the job thread rewrote the marker exit had written"
    # the next start reads the user's cancel as a cancel — not as a run cut off by a restart
    _reset_job()
    assert _status(client) == marker


class _PauseTheJobAfterItsClaim:
    """Wraps _CANON_RESTORE_LOCK: the job thread pauses right after it claims its record, before it writes anything."""
    def __init__(self, real, state):
        self.real, self.state = real, state
        self.claimed, self.go, self.paused = threading.Event(), threading.Event(), False

    def acquire(self, *a, **k):
        return self.real.acquire(*a, **k)

    def release(self):
        return self.real.release()

    def __enter__(self):
        return self.real.__enter__()

    def __exit__(self, *exc):
        out = self.real.__exit__(*exc)
        if threading.current_thread().name == JOB_THREAD and not self.paused and self.state.get("recorded_by") == "job":
            self.paused = True
            self.claimed.set()
            assert self.go.wait(10)
        return out


def test_a_job_that_claimed_its_record_first_keeps_it_and_exit_writes_nothing(env, monkeypatch):
    client, settings, tmp_path, events = env
    from app.web import api as api_mod
    assert api_mod.canon_restore_exit_record() is None, "nothing runs, so there is nothing to record"
    api_mod, held = _held_on_plex(client, monkeypatch)
    pause = _PauseTheJobAfterItsClaim(api_mod._CANON_RESTORE_LOCK, api_mod._CANON_RESTORE_STATE)
    monkeypatch.setattr(api_mod, "_CANON_RESTORE_LOCK", pause)
    job = api_mod.canon_restore_shutdown()
    held.after_cancel.set()
    try:
        assert pause.claimed.wait(10), "premise: the job claimed its record and is about to write it"
        assert _status(client)["status"] == "running", "premise: the claim came before the job's state moved"
        assert api_mod.canon_restore_exit_record() is None, "exit recorded a run whose job had claimed the record"
        assert (_audits(settings.db_path), events) == ([], []), "exit wrote a record the job was about to write"
    finally:
        pause.go.set()
    job.join(10)
    assert not job.is_alive()
    assert [(a["status"], a["restored"]) for a in _audits(settings.db_path)] == [("interrupted", 2)]
    assert [e["level"] for e in events] == ["WARNING"]
    assert api_mod.canon_restore_exit_record() is None, "a finished run is not exit's to record"
    assert len(_audits(settings.db_path)) == 1 and len(events) == 1


# ── F1: the signal handler ────────────────────────────────────────────

def test_the_servers_exit_handler_records_the_signal_and_cancels_the_job_before_the_drain(env, monkeypatch):
    client, settings, tmp_path, events = env
    from app import main as main_mod
    api_mod, held = _held_on_plex(client, monkeypatch)
    server = main_mod._Server(uvicorn.Config(client.app))
    assert (server.signalled_at, server.restore_job, server.should_exit) == (None, None, False)
    before = time.monotonic()
    server.handle_exit(signal.SIGTERM, None)
    first = server.signalled_at
    assert before <= first <= time.monotonic(), "the signal's time was not recorded"
    assert server.should_exit is True, "uvicorn's own exit was not asked for"
    assert server.restore_job is not None and server.restore_job.name == JOB_THREAD
    assert held.cancel_seen.wait(10), "the job did not see the cancel the signal handler sent"
    server.handle_exit(signal.SIGTERM, None)
    assert server.signalled_at == first, "a second signal moved exit's clock"
    held.after_cancel.set()
    server.restore_job.join(10)
    assert not server.restore_job.is_alive()
    assert _status(client)["status"] == "interrupted"


def test_the_shutdown_cancel_never_waits_for_the_job_lock_and_the_cancel_poll_still_sees_it(env, monkeypatch):
    client, settings, tmp_path, events = env
    api_mod, held = _held_on_plex(client, monkeypatch)
    held.after_cancel.set()
    out: dict = {}
    done = threading.Event()
    lock = api_mod._CANON_RESTORE_LOCK
    assert lock.acquire(timeout=10), "premise: the test holds the job lock, as the event loop's thread may at the signal"
    try:
        t = threading.Thread(target=lambda: (out.update(job=api_mod.canon_restore_shutdown()), done.set()), daemon=True)
        t.start()
        assert done.wait(2), "canon_restore_shutdown waited for the job lock — from the signal handler that is a deadlock"
        assert out["job"] is not None and out["job"].name == JOB_THREAD
        assert api_mod._CANON_RESTORE_SHUTDOWN.is_set()
        assert api_mod._CANON_RESTORE_STATE.get("cancel") is not True, "premise: the flag could not be set under the held lock"
    finally:
        lock.release()
    assert held.cancel_seen.wait(10), "the job's cancel poll did not see the shutdown the handler could only signal"
    out["job"].join(10)
    assert not out["job"].is_alive()
    assert _status(client)["status"] == "interrupted"
    assert [e["level"] for e in events] == ["WARNING"], "the job's own cut-off record"
