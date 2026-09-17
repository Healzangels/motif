"""v0.51.344 integration fixes R2-F6 / R2-F12 / R3-F5: what a cut-off RESTORE FROM PLEX leaves behind, and who reads it.

  R2-F6.  The cut-off path dropped the run's skipped rows: the marker stayed the bare start marker and the audit row
          carried counts only, so the rows Plex answered for but exit refused (motif_exiting) never reached CANONICAL
          HEALTH, though the page words that reason. The marker now carries the run's counts and skipped rows, the audit
          row a per-reason histogram, and the restart's page names them. Exit's own record carries its last poll's
          counts, and a summary that arrives after it amends the marker with the rows.
  R2-F12. A START whose thread could not start rolled the state to 'idle' — the one answer a watching tab reads as
          "motif restarted before it could record one". It is a named failure with words now.
  R3-F5.  The marker lives beside the database, not in it: a database restored at boot discards it with one line, so the
          page no longer reports a run the restored database never saw as this install's last run.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import threading
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core import canonical_health as ch
from app.core import db_backup
from app.core.auth import create_admin, init_auth_schema
from app.core.db import init_db
from test_v0_51_339_canonical_health_restore import _NODE, _report, _run_page
from test_v0_51_342_canonical_health_changed import _boot, _main_lines
from test_v0_51_342_restore_from_plex_job import (  # noqa: F401 — env and ssr_running are fixtures
    AUTH, JOB_THREAD, START, HeldRestore, _finish, _marker, _reset_job, _status, env, ssr_running,
)
from test_v0_51_344_exit_record import _audits
from test_v0_51_344_job_start_failure import REFUSED, _refuse_start

_REFUSED = [{"title": f"T{t}", "media_type": "movie", "tmdb_id": t, "section_id": "1", "reason": "motif_exiting"}
            for t in (601, 602)]
_CUT_OFF = "was cut off by a motif restart — RUN CHECK, then RESTORE FROM PLEX restores what is left"
_EXITING = "2 motif was shutting down — not written"


class _RefusedAtExit(HeldRestore):
    """A run the shutdown cancels: one row restored, two that Plex answered after exit closed publishing, one not tried."""
    def __call__(self, db_path, themes_dir, plex_client, *, plex_client_factory=None, progress_cb=None,
                 cancel_check=None, **_kw):
        self.calls.append({})
        progress_cb(1, 4, {"restored_sidecar": 1, "restored_store": 0, "skipped_count": 0})
        self.entered.set()
        while not self.release.wait(0.01):
            if cancel_check():
                self.cancel_seen.set()
                self.after_cancel.wait(10)
                break
        return {"broken": 4, "restored_sidecar": 1, "restored_store": 0, "restored": 1, "skipped": list(_REFUSED),
                "cancelled": True, "not_attempted": 1, "plex_unreachable": False}


def _shutdown_ready(monkeypatch):
    from app.web import api as api_mod
    monkeypatch.setattr(api_mod, "_CANON_RESTORE_SHUTDOWN", threading.Event())
    monkeypatch.setattr(api_mod, "_CANON_RESTORE_THREAD", None)
    return api_mod


def _page_text(tmp_path, st, ssr=None):
    responses = [_report(), st] + ([_report()] if ssr else [])
    (snap,) = _run_page(tmp_path, responses, [], ssr=ssr)
    return snap["canon-restore-plex-status"]


# ── R2-F6: the cut-off run's rows reach the marker, the audit row and the restart's page ──

@pytest.mark.skipif(not _NODE, reason="node not installed")
def test_a_cut_off_runs_marker_carries_its_skipped_rows_and_the_restart_page_names_the_rows_exit_refused(env, monkeypatch):
    client, settings, tmp_path, events = env
    api_mod = _shutdown_ready(monkeypatch)
    held = _RefusedAtExit()
    monkeypatch.setattr(ch, "restore_from_plex", held)
    assert client.post(START, headers=AUTH).json()["started"] is True
    assert held.entered.wait(10)
    started = _status(client)["started_at"]
    job = api_mod.canon_restore_shutdown()
    assert job is not None
    job.join(10)
    assert not job.is_alive()
    marker = json.loads(_marker(settings).read_text())
    assert (marker["status"], marker["started_at"], marker["actor"]) == ("running", started, "testadmin"), \
        "the marker must still say running, so the next start reports the run cut off"
    assert (marker["restored"], marker["restored_sidecar"], marker["not_attempted"], marker["skipped_count"],
            marker["skipped"]) == (1, 1, 1, 2, _REFUSED), marker
    (audit,) = _audits(settings.db_path)
    assert (audit["status"], audit["restored"], audit["not_attempted"], audit["skipped_reasons"]) == \
        ("interrupted", 1, 1, {"motif_exiting": 2}), audit
    assert "skipped" not in audit, "the audit row carries the histogram, not the list"
    live = _status(client)
    assert (live["status"], live["skipped"], live["restored"]) == ("interrupted", _REFUSED, 1), \
        "a tab polling in the exit window reads the rows too"
    # the restart: the marker is the last run, reported once with first_report, then quietly
    _reset_job()
    first = _status(client)
    assert (first["status"], first["first_report"], first["skipped"], first["restored"]) == \
        ("interrupted", True, _REFUSED, 1), first
    status = _page_text(tmp_path / "first", first)
    assert status["className"] == "form-status form-status-fail"
    assert status["text"].startswith("✗ the run started ") and _CUT_OFF in status["text"], status
    assert status["text"].endswith(f" · restored 1 (1 from Plex folders, 0 from Plex's store) · 2 skipped ({_EXITING}) "
                                   "· 1 not tried"), status
    later = _status(client)
    assert "first_report" not in later and later["skipped"] == _REFUSED
    status = _page_text(tmp_path / "later", later)
    assert status["className"] == "form-status"
    assert status["text"].startswith("last run started ") and status["text"].endswith(
        f": cut off by a motif restart · restored 1 (1 from Plex folders, 0 from Plex's store) · 2 skipped ({_EXITING}) "
        "· 1 not tried"), status


@pytest.mark.skipif(not _NODE, reason="node not installed")
def test_exits_record_carries_its_counts_and_the_summary_that_arrives_after_it_amends_the_marker(env, monkeypatch):
    client, settings, tmp_path, events = env
    api_mod = _shutdown_ready(monkeypatch)
    held = HeldRestore()
    held.after_cancel.clear()
    monkeypatch.setattr(ch, "restore_from_plex", held)
    assert client.post(START, headers=AUTH).json()["started"] is True
    assert held.entered.wait(10)
    job = api_mod.canon_restore_shutdown()
    assert job is not None and held.cancel_seen.wait(10), "premise: the job saw the cancel and still waits on Plex"
    assert api_mod.canon_restore_exit_record() == "interrupted"
    marker = json.loads(_marker(settings).read_text())
    assert (marker["status"], marker["restored"], marker["restored_sidecar"], marker["restored_store"],
            marker["skipped_count"], marker["not_attempted"]) == ("running", 2, 1, 1, 1, 7), marker
    assert "skipped" not in marker, "premise: exit's record knows the count, not the rows"
    # the restart's page, from a count-only marker: the count shows, with no reasons to word
    status = _page_text(tmp_path / "counts", {**marker, "status": "interrupted", "first_report": True})
    assert status["text"].endswith(" · restored 2 (1 from Plex folders, 1 from Plex's store) · 1 skipped · 7 not tried"), \
        status
    # Plex answers before the process is gone: the run's own summary amends the marker, and nothing else
    audits, n_events = _audits(settings.db_path), len(events)
    held.after_cancel.set()
    job.join(10)
    assert not job.is_alive()
    amended = json.loads(_marker(settings).read_text())
    assert (amended["status"], amended["started_at"], amended["actor"]) == ("running", marker["started_at"], "testadmin")
    assert [s["reason"] for s in amended["skipped"]] == ["no_plex_copy"] and amended["skipped_count"] == 1, amended
    assert (_audits(settings.db_path), len(events)) == (audits, n_events), "the late summary wrote a second record"
    _reset_job()
    first = _status(client)
    assert first["status"] == "interrupted" and first["first_report"] is True
    status = _page_text(tmp_path / "rows", first)
    assert status["text"].endswith(" · restored 2 (1 from Plex folders, 1 from Plex's store) · 1 skipped (1 no Plex copy) "
                                   "· 7 not tried"), status


def test_a_cut_off_run_whose_start_marker_never_landed_writes_no_counts_marker(env, monkeypatch, caplog):
    client, settings, tmp_path, events = env
    api_mod = _shutdown_ready(monkeypatch)
    import os
    real = os.replace

    def refuse(src, dst, *a, **k):
        if str(dst).endswith("restore_from_plex.json"):
            raise OSError(28, "No space left on device")
        return real(src, dst, *a, **k)
    monkeypatch.setattr(os, "replace", refuse)
    held = _RefusedAtExit()
    monkeypatch.setattr(ch, "restore_from_plex", held)
    with caplog.at_level(logging.WARNING):
        assert client.post(START, headers=AUTH).json()["started"] is True
        assert held.entered.wait(10)
        job = api_mod.canon_restore_shutdown()
        job.join(10)
    assert not job.is_alive()
    assert not _marker(settings).exists(), "a counts marker landed for a run whose start marker never did"
    (audit,) = _audits(settings.db_path)
    assert audit["skipped_reasons"] == {"motif_exiting": 2}, "the audit row still carries the reasons"
    assert sum("could not write" in r.getMessage() for r in caplog.records) == 1, \
        "the start marker's failure is the one write refused — a counts write was attempted for a run with no marker"


# ── R2-F12: a start that never ran is a named failure, not 'idle' ─────

@pytest.mark.skipif(not _NODE, reason="node not installed")
def test_a_start_whose_thread_cannot_start_reads_failed_in_words_and_a_watching_tab_never_claims_a_restart(
        env, monkeypatch, ssr_running):
    client, settings, tmp_path, events = env
    api_mod = _shutdown_ready(monkeypatch)
    held = HeldRestore(released=True)
    monkeypatch.setattr(ch, "restore_from_plex", held)
    loose = TestClient(client.app, raise_server_exceptions=False)
    allow = _refuse_start(monkeypatch, JOB_THREAD)
    assert loose.post(START, headers=AUTH).status_code == 500
    st = _status(loose)
    assert st["status"] == "failed" and REFUSED in st["error"] and "nothing ran" in st["error"], st
    assert not {"marked", "recorded_by", "cancel", "t0", "user_cancel"} & st.keys(), "a private key reached the page"
    words = f"✗ restore failed — {st['error']} (0 restored before it stopped)"
    watched = _page_text(tmp_path / "watched", st, ssr=ssr_running)
    assert (watched["text"], watched["className"]) == (words, "form-status form-status-fail"), watched
    assert "restarted" not in watched["text"]
    loaded = _page_text(tmp_path / "loaded", st)
    assert loaded["text"].startswith("last run ") and loaded["text"].endswith(": " + words), loaded
    allow()
    assert loose.post(START, headers=AUTH).json() == {"ok": True, "started": True}, "the failure locked the next start"
    assert _finish(loose)["status"] == "done" and len(held.calls) == 1


# ── R3-F5: a database restored at boot discards the marker ───────────

def _install(tmp_path, monkeypatch, *, marker):
    """A config dir whose live database has a restore staged, and a RESTORE FROM PLEX marker beside it."""
    cd = tmp_path / "cfg"
    cd.mkdir()
    monkeypatch.setenv("MOTIF_COOKIES_FILE", str(cd / "cookies.txt"))
    src = tmp_path / "src.db"
    init_db(src)
    init_auth_schema(src)
    create_admin(src, username="testadmin", password="testpassword")
    snap = tmp_path / "snap.db"
    db_backup.vacuum_into(src, snap)
    live = cd / "motif.db"
    init_db(live)
    db_backup.stage_restore(live, snap)
    path = cd / "canonical_health" / "restore_from_plex.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(marker))
    return cd, path


def _status_after_boot(cd, monkeypatch):
    from app.config import Settings
    from app.web import api as api_mod
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(cd))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(cd / "data"))
    settings = Settings(config_dir=cd, data_dir=cd / "data")
    _reset_job()
    try:
        return _status(TestClient(api_mod.create_app(settings)))
    finally:
        _reset_job()


def test_a_database_restored_at_boot_discards_the_last_restore_from_plex_marker_and_says_so(tmp_path, monkeypatch,
                                                                                            caplog):
    caplog.set_level(logging.INFO)
    marker = {"status": "running", "started_at": "2026-09-15T17:50:00", "actor": "testadmin"}
    cd, path = _install(tmp_path, monkeypatch, marker=marker)
    _boot(monkeypatch, cd)
    assert _main_lines(caplog, logging.WARNING, "Database restored at boot"), "premise: the staged restore applied"
    assert not path.exists(), "the marker outlived the database its run was recorded in"
    (line,) = _main_lines(caplog, logging.WARNING, "discarded the last RESTORE FROM PLEX run's result")
    assert "running" in line and "2026-09-15T17:50:00" in line and "testadmin" in line, line
    assert "the database this restore replaced" in line and "shows no last run" in line, line
    assert _status_after_boot(cd, monkeypatch) == {"status": "idle"}, \
        "the page reported a run the restored database never saw as this install's last run"
    # the next boot has nothing staged and nothing to discard
    caplog.clear()
    _boot(monkeypatch, cd)
    assert _main_lines(caplog, logging.WARNING, "discarded the last RESTORE FROM PLEX") == []


def test_a_boot_without_a_restore_keeps_the_marker(tmp_path, monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    marker = {"status": "done", "started_at": "2026-09-15T17:50:00", "finished_at": "2026-09-15T17:55:00",
              "actor": "testadmin", "broken": 3, "restored": 3, "restored_sidecar": 3, "restored_store": 0,
              "skipped": [], "skipped_count": 0, "not_attempted": 0, "cancelled": False, "plex_unreachable": False}
    cd, path = _install(tmp_path, monkeypatch, marker=marker)
    db_backup.restore_pending_path(cd / "motif.db").unlink()  # nothing staged after all
    _boot(monkeypatch, cd)
    assert _main_lines(caplog, logging.WARNING, "Database restored at boot") == [], "premise: no restore applied"
    assert json.loads(path.read_text()) == marker, "a boot with no restore touched the marker"
    assert _main_lines(caplog, logging.WARNING, "discarded the last RESTORE FROM PLEX") == []


def test_a_marker_that_cannot_be_removed_is_reported_and_boot_goes_on(tmp_path, monkeypatch, caplog):
    from app.config import Settings
    from app.web import api as api_mod
    cd = tmp_path / "cfg"
    (cd / "canonical_health").mkdir(parents=True)
    path = cd / "canonical_health" / "restore_from_plex.json"
    path.write_text(json.dumps({"status": "running", "started_at": "2026-09-15T17:50:00", "actor": "testadmin"}))
    real = Path.unlink

    def unlink(self, *a, **k):
        if self == path:
            raise PermissionError(13, "Permission denied")
        return real(self, *a, **k)
    monkeypatch.setattr(Path, "unlink", unlink)
    settings = Settings(config_dir=cd, data_dir=cd / "data")
    with caplog.at_level(logging.WARNING):
        assert api_mod.canon_restore_forget(settings) is None
    assert path.exists()
    (line,) = [r.getMessage() for r in caplog.records if "could not remove" in r.getMessage()]
    assert "Permission denied" in line and "RUN CHECK" in line, line
    monkeypatch.setattr(Path, "unlink", real)
    assert api_mod.canon_restore_forget(settings)["status"] == "running"
    assert not path.exists() and api_mod.canon_restore_forget(settings) is None
