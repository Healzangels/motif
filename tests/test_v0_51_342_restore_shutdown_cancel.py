"""v0.51.342: RESTORE FROM PLEX stops when asked — a cancel reaches the shared-path tail, and a motif shutdown is a cut-off."""
from __future__ import annotations

import json
import logging
import os
import signal
import socket
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import test_v0_51_342_restore_pool as rp
from app.core import canonical_health as ch
from test_v0_51_342_restore_from_plex_job import (  # noqa: F401 — env is the job endpoints' fixture
    AUTH, CANCEL, JOB_THREAD, START, STATUS, HeldRestore, _join_job, _marker, _messages, _reset_job, _status, _wait,
    env,
)
from test_v0_51_342_restore_pool import SHARED, FakePlex, _gate_spy, _lf_cols, _seed_store

REPO = Path(__file__).resolve().parents[1]
NO_ANSWER = {"ok": False, "http_status": None, "error": "transport: ConnectError('refused')", "body": None}


@pytest.fixture(autouse=True)
def fresh_shutdown_state(monkeypatch):
    from app.web import api as api_mod
    monkeypatch.setattr(api_mod, "_CANON_RESTORE_SHUTDOWN", threading.Event())
    monkeypatch.setattr(api_mod, "_CANON_RESTORE_THREAD", None)


def _plex_on(settings):
    settings._cfg.plex.enabled = True
    settings._cfg.plex.url = "http://plex.test:32400"
    settings._cfg.plex.token = "token-for-test"


def _seed_rows(db, tmdbs, *, file_path=None):
    with sqlite3.connect(db) as conn:
        rp._section(conn)
        for tmdb in tmdbs:
            rp._theme(conn, tmdb)
            rp._lf(conn, tmdb, file_path=file_path)
            rp._placement(conn, tmdb, "", kind="plex_upload", rk=f"9{tmdb}")
        conn.commit()


class DeadThenHung(FakePlex):
    """Gives no answer to the first themes request; every later one hangs until `never` is set."""
    def __init__(self, record, never, **kw):
        super().__init__(**kw)
        self.record, self.never = record, never

    def get_themes(self, *, rating_key):
        if not self.record(rating_key):
            self.never.wait(30)
        return dict(NO_ANSWER)


def _tail_hooks(monkeypatch):
    """The second shared-path fetch backs off 30 s and sets `backing` as it starts; Plex calls after `cancel` are late."""
    monkeypatch.setattr(ch, "_PLEX_BACKOFF_BASE_S", 30.0)
    monkeypatch.setattr(ch, "_PLEX_BACKOFF_CAP_S", 30.0)
    lock = threading.Lock()
    calls: dict = {"asked": [], "late": [], "befores": 0}
    backing, cancel, never = threading.Event(), threading.Event(), threading.Event()

    def record(rk):
        with lock:
            calls["late" if cancel.is_set() else "asked"].append(rk)
            return len(calls["asked"]) + len(calls["late"]) == 1

    def on_before():
        with lock:
            calls["befores"] += 1
            if calls["befores"] == 2:
                backing.set()
    monkeypatch.setattr(ch, "_PlexGate", _gate_spy(on_before))
    return calls, backing, cancel, never, record


# ── 1: a cancel reaches the shared-path tail ─────────────────────────

@pytest.mark.parametrize("n", [3, 2], ids=["a-later-row-waits", "the-last-row"])
def test_a_cancel_during_a_shared_path_backoff_wakes_it_and_plex_is_never_asked(tmp_path, monkeypatch, n):
    db, themes = _seed_store(tmp_path, n=n, shared=tuple(range(601, 601 + n)))
    calls, backing, cancel, never, record = _tail_hooks(monkeypatch)
    out: dict = {}
    t = threading.Thread(target=lambda: out.update(res=ch.restore_from_plex(
        db, themes, DeadThenHung(record, never), cancel_check=cancel.is_set)), daemon=True)
    t.start()
    try:
        assert backing.wait(10), "premise: the second shared-path row backed off after the first gave no answer"
        cancel.set()
        t.join(10)
        stuck = t.is_alive()
    finally:
        never.set()
        t.join(10)
    assert calls["late"] == [], "a shared-path row asked Plex after the cancel"
    assert not stuck, "the cancel did not wake the shared-path row's backoff"
    res = out["res"]
    assert calls["asked"] == ["9601"]
    assert [s["tmdb_id"] for s in res["skipped"]] == [601] and res["skipped"][0]["reason"].startswith(
        "plex_themes:transport")
    assert (res["cancelled"], res["restored"], res["not_attempted"]) == (True, 0, n - 1), \
        "the summary must read cancelled and count the rows the cancel left untried"
    assert [p for p in themes.rglob("*") if p.is_file()] == [], "a file was written after the cancel"
    assert all(_lf_cols(db, t, ("canonical_present",)) == (0,) for t in range(601, 601 + n)), \
        "a row was stamped after the cancel"


def test_a_pool_backoff_never_asks_for_the_cancel_the_runs_loop_does(tmp_path, monkeypatch):
    # a worker that saw the cancel first ended every fetch before the loop did — the loop never waited on them again
    n, workers = 16, 4
    db, themes = _seed_store(tmp_path, n=n)
    monkeypatch.setattr(ch, "_PLEX_BACKOFF_BASE_S", 30.0)
    monkeypatch.setattr(ch, "_PLEX_BACKOFF_CAP_S", 30.0)
    lock, first = threading.Lock(), threading.Barrier(workers)
    begun, backing, asked, askers = set(), set(), [], []
    in_backoff, looped = threading.Event(), threading.Event()

    def no_answer():
        # every worker's first fetch gives no answer once all are in, so each one's next fetch backs off
        with lock:
            k = len(asked)
            asked.append(looped.is_set())
        if k < workers:
            first.wait(10)
            return True
        return False

    def on_before():
        me = threading.get_ident()
        with lock:
            if me in begun:
                backing.add(me)
                if len(backing) == workers:
                    in_backoff.set()
            begun.add(me)
    monkeypatch.setattr(ch, "_PlexGate", _gate_spy(on_before))
    real_wait = ch.wait

    def wait(fs, *a, **k):
        # a loop wait that began with every worker backing off and ended on its timeout: a worker had a full turn to ask
        full = in_backoff.is_set()
        got = real_wait(fs, *a, **k)
        if full and not got.done:
            looped.set()
        return got
    monkeypatch.setattr(ch, "wait", wait)

    def cancel_check():
        with lock:
            askers.append(threading.current_thread().name)
        return looped.is_set()
    out: dict = {}
    t = threading.Thread(target=lambda: out.update(res=ch.restore_from_plex(
        db, themes, None, workers=workers, plex_client_factory=lambda: FakePlex(no_answer=no_answer),
        cancel_check=cancel_check)), name="the-run", daemon=True)
    t.start()
    t.join(30)
    assert not t.is_alive(), "the cancel waited out a backoff"
    assert looped.is_set(), "premise: the run's loop waited a whole turn with every worker backing off"
    assert set(askers) == {"the-run"}, "a pool worker asked for the cancel — only the run's loop may"
    assert asked == [False] * workers, "a fetch asked Plex after the cancel"
    res = out["res"]
    assert (res["cancelled"], res["restored"], res["not_attempted"]) == (True, 0, n - workers)


def test_a_cancel_during_the_last_shared_path_rows_backoff_reads_cancelled_on_the_job(env, monkeypatch):
    client, settings, tmp_path, events = env
    from app.web import api as api_mod
    _seed_rows(settings.db_path, (601, 602), file_path=SHARED)
    _plex_on(settings)
    calls, backing, cancel, never, record = _tail_hooks(monkeypatch)
    made = []

    class Client(DeadThenHung):
        def __init__(self, cfg, *, plus_mode):
            super().__init__(record, never)
            made.append(self)
    monkeypatch.setattr(api_mod, "PlexClient", Client)
    assert client.post(START, headers=AUTH).json()["started"] is True
    try:
        assert backing.wait(10), "premise: the last shared-path row backed off after the first gave no answer"
        cancel.set()
        assert client.post(CANCEL, headers=AUTH).json() == {"ok": True, "cancelling": True}
        st = _wait(client, lambda s: s["status"] != "running", "the cancelled run to end")
    finally:
        never.set()
    _join_job()
    assert calls["late"] == [], "the last shared-path row asked Plex after the cancel"
    assert (st["status"], st["cancelled"], st["not_attempted"], st["restored"]) == ("cancelled", True, 1, 0)
    assert json.loads(_marker(settings).read_text())["status"] == "cancelled"
    assert _messages(events) == ["Canonical restore from Plex by testadmin: 0 from sidecars, 0 from Plex's store, "
                                 "1 skipped of 2 broken — cancelled, 1 not tried"]
    assert made and all(c.closed for c in made)


# ── 2: motif's shutdown cancels the job, and the run reads cut off ───

def test_motif_shutdown_cancels_the_running_job_which_reads_cut_off_not_cancelled(env, monkeypatch, caplog):
    client, settings, tmp_path, events = env
    from app.web import api as api_mod
    held = HeldRestore()
    monkeypatch.setattr(ch, "restore_from_plex", held)
    assert client.post(START, headers=AUTH).json()["started"] is True
    assert held.entered.wait(10)
    started = _status(client)["started_at"]
    with caplog.at_level(logging.WARNING):
        job = api_mod.canon_restore_shutdown()
        assert job is not None and job.name == JOB_THREAD, "the exit path needs the running job's thread to join"
        job.join(10)
    assert not job.is_alive(), "the shutdown did not cancel the running job"
    assert _status(client)["status"] == "interrupted", "a run stopped by the shutdown read as cancelled or done"
    assert json.loads(_marker(settings).read_text()) == {"status": "running", "started_at": started,
                                                         "actor": "testadmin"}, \
        "the marker must still say running, so the next start reports the run cut off"
    assert _messages(events) == [] and [e for e in events if e.get("level") == "WARNING"] == []
    with sqlite3.connect(settings.db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM audit_events "
                            "WHERE action = 'canonical_restore_from_plex'").fetchone()[0] == 0
    assert any("cut off by a motif shutdown" in r.getMessage() for r in caplog.records)
    assert api_mod.canon_restore_shutdown() is None, "with no run, the exit path has nothing to join"
    _reset_job()
    assert _status(client) == {"status": "interrupted", "started_at": started, "actor": "testadmin",
                               "first_report": True}


# ── 3: a pool refused by interpreter shutdown is a cut-off ───────────

@pytest.mark.parametrize("shutting_down", [True, False], ids=["motif-shutting-down", "no-shutdown"])
def test_a_pool_that_refuses_new_work_is_a_cut_off_only_while_motif_shuts_down(env, monkeypatch, shutting_down):
    client, settings, tmp_path, events = env
    from app.web import api as api_mod
    _seed_rows(settings.db_path, range(601, 613))
    _plex_on(settings)
    monkeypatch.setattr(api_mod, "PlexClient", lambda cfg, *, plus_mode: FakePlex())
    submits = []

    class ExitingPool(ThreadPoolExecutor):
        """Exit began after the first fill: a later submit is refused the way concurrent.futures refuses it."""
        def submit(self, fn, /, *a, **k):
            submits.append(fn)
            if len(submits) > 2 * ch.RESTORE_PLEX_WORKERS:
                if shutting_down:
                    api_mod.canon_restore_shutdown()
                raise RuntimeError("cannot schedule new futures after interpreter shutdown")
            return super().submit(fn, *a, **k)
    monkeypatch.setattr(ch, "ThreadPoolExecutor", ExitingPool)
    assert client.post(START, headers=AUTH).json()["started"] is True
    st = _wait(client, lambda s: s["status"] != "running", "the run to end")
    _join_job()
    assert len(submits) == 2 * ch.RESTORE_PLEX_WORKERS + 1, "premise: the refill after the first fill was refused"
    marker = json.loads(_marker(settings).read_text())
    warned = [e["message"] for e in events if e.get("level") == "WARNING"]
    if not shutting_down:
        assert (st["status"], marker["status"]) == ("failed", "failed"), "a real failure must still read failed"
        assert "cannot schedule new futures" in st["error"]
        assert len(warned) == 1 and "cannot schedule new futures after interpreter shutdown" in warned[0]
        return
    assert (st["status"], marker["status"], warned) == ("interrupted", "running", []), \
        "a refusal from interpreter shutdown was reported as a failure"
    _reset_job()
    assert _status(client) == {**marker, "status": "interrupted", "first_report": True}


# ── 2, in a real motif: SIGTERM mid-restore ──────────────────────────

_CHILD = r"""
import sys, threading
sys.path.insert(0, sys.argv[1])
import app.core.canonical_health as ch
from app.web import api
print("child imports " + ch.__file__, flush=True)
joining, seen = threading.Event(), []
real_wait, real_publish, real_shutdown = ch.wait, ch._publish_store_bytes, api.canon_restore_shutdown

def wait(fs, *a, **k):
    # the second pool wait after the cancel flag comes after the loop saw it and dropped the queued fetches
    if api._CANON_RESTORE_STATE.get("cancel"):
        seen.append(1)
        if len(seen) == 2:
            print("PROBE the pool saw the cancel", flush=True)
    return real_wait(fs, *a, **k)

def publish(*a, **k):
    # a write the exit path does not wait for is frozen here when the interpreter finalizes
    joining.wait()
    return real_publish(*a, **k)

def shutdown():
    job = real_shutdown()
    if job is not None:
        real_join = job.join
        def join(timeout=None):
            joining.set()
            return real_join(timeout)
        job.join = join
    return job

ch.wait, ch._publish_store_bytes, api.canon_restore_shutdown = wait, publish, shutdown
from app.main import main
sys.exit(main())
"""


def _hung_plex():
    """A Plex whose theme listings hang until `release`; a listing asked after the release is recorded as late."""
    state = {"listings": [], "late": [], "release": threading.Event(), "lock": threading.Lock()}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            path = self.path.split("?")[0]
            if path.startswith("/library/metadata/") and path.endswith("/themes"):
                rk = path.split("/")[3]
                with state["lock"]:
                    state["late" if state["release"].is_set() else "listings"].append(rk)
                state["release"].wait(30)
                body = json.dumps({"MediaContainer": {"Metadata": [
                    {"ratingKey": f"upload://themes/{rk}", "selected": True}]}}).encode()
            elif path.endswith("/file"):
                body = b"ID3-store-bytes" * 64
            else:
                body = b'{"MediaContainer": {"size": 0}}'
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *a):
            return None
    srv = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, state


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


_NO_PROXY = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def _http(port, method, path):
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", method=method,
                                 data=b"" if method == "POST" else None, headers=AUTH)
    with _NO_PROXY.open(req, timeout=10) as r:
        return json.loads(r.read())


def _up(port):
    try:
        with _NO_PROXY.open(f"http://127.0.0.1:{port}/healthz", timeout=2) as r:
            return r.status == 200
    except OSError:
        return False


def test_a_motif_stopped_mid_restore_finishes_its_in_flight_rows_and_the_next_start_reads_cut_off(tmp_path,
                                                                                                  monkeypatch):
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web import api as api_mod
    cfg, data, themes = tmp_path / "config", tmp_path / "data", tmp_path / "themes"
    cfg.mkdir()
    themes.mkdir()
    db = cfg / "motif.db"
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    _seed_rows(db, range(601, 613))
    srv, plex = _hung_plex()
    web_port = _free_port()
    env_vars = {k: v for k, v in os.environ.items() if not k.startswith("MOTIF_")}
    env_vars.update(MOTIF_CONFIG_DIR=str(cfg), MOTIF_DATA_DIR=str(data), MOTIF_THEMES_DIR=str(themes),
                    MOTIF_COOKIES_FILE=str(cfg / "cookies.txt"), MOTIF_PLEX_ENABLED="true",
                    MOTIF_PLEX_URL=f"http://127.0.0.1:{srv.server_address[1]}", MOTIF_PLEX_TOKEN="token-for-test",
                    MOTIF_WEB_HOST="127.0.0.1", MOTIF_WEB_PORT=str(web_port), MOTIF_TRUST_FORWARD_AUTH="true",
                    MOTIF_FORWARD_AUTH_ALLOWED_IPS="127.0.0.1")
    log_path = tmp_path / "motif.out"

    def log_text():
        return log_path.read_text(errors="replace")

    def until(pred, what, seconds=60):
        end = time.monotonic() + seconds
        while not pred():
            assert proc.poll() is None, f"motif exited while waiting for {what}:\n{log_text()[-4000:]}"
            assert time.monotonic() < end, f"timed out waiting for {what}:\n{log_text()[-4000:]}"
            time.sleep(0.05)

    with log_path.open("w") as out:
        proc = subprocess.Popen([sys.executable, "-c", _CHILD, str(REPO)], cwd=str(REPO), env=env_vars,
                                stdout=out, stderr=subprocess.STDOUT)
    try:
        until(lambda: _up(web_port), "motif to come up")
        assert _http(web_port, "POST", START)["started"] is True
        until(lambda: len(plex["listings"]) >= ch.RESTORE_PLEX_WORKERS, "every Plex worker to wait on Plex")
        proc.send_signal(signal.SIGTERM)
        until(lambda: "PROBE the pool saw the cancel" in log_text(), "motif's shutdown to cancel the running restore")
        plex["release"].set()
        proc.wait(timeout=60)
    finally:
        plex["release"].set()
        if proc.poll() is None:
            proc.kill()
            proc.wait(10)
        srv.shutdown()
        srv.server_close()
    text = log_text()
    assert f"child imports {REPO / 'app' / 'core' / 'canonical_health.py'}" in text, "the child ran another tree"
    assert proc.returncode == 0, text[-4000:]
    assert plex["late"] == [], "Plex was asked for a queued row after motif began shutting down"
    assert "cut off by a motif shutdown" in text, "exit did not wait for the restore job to finish"
    assert "restore from Plex failed" not in text and "cannot schedule new futures" not in text
    with sqlite3.connect(db) as conn:
        stamped = {r[0] for r in conn.execute("SELECT tmdb_id FROM local_files WHERE canonical_present = 1")}
    on_disk = {int(p.parent.name) for p in themes.rglob("theme.mp3")}
    assert on_disk == stamped, "exit left a canonical written without its stamp"
    assert len(stamped) == ch.RESTORE_PLEX_WORKERS, "the rows Plex was already answering were not all restored"
    marker = json.loads((cfg / "canonical_health" / "restore_from_plex.json").read_text())
    assert marker["status"] == "running", "the run must read cut off on the next start, not cancelled or done"
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(cfg))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(data))
    monkeypatch.setattr(api_mod, "log_event", lambda _db, **k: None)
    _reset_job()
    try:
        st = TestClient(api_mod.create_app(Settings(config_dir=cfg, data_dir=data))).get(STATUS, headers=AUTH).json()
    finally:
        _reset_job()
    assert st == {**marker, "status": "interrupted", "first_report": True}


def test_a_shared_path_backoff_with_no_cancel_waits_its_length_then_goes_on(tmp_path, monkeypatch):
    # v0.51.342: the tail's polling wait must still END without a cancel — a one-slice or endless wait both passed every other test.
    db, themes = _seed_store(tmp_path, n=2, shared=(601, 602))
    monkeypatch.setattr(ch, "_PLEX_BACKOFF_BASE_S", 0.6)
    monkeypatch.setattr(ch, "_PLEX_CANCEL_POLL_S", 0.05)
    lock, stop = threading.Lock(), threading.Event()
    asked, polls = [], []

    def no_answer():
        with lock:
            asked.append(time.monotonic())
            return len(asked) == 1

    def cancel_check():
        polls.append(time.monotonic())
        return stop.is_set()

    out: dict = {}
    t = threading.Thread(target=lambda: out.update(res=ch.restore_from_plex(
        db, themes, FakePlex(no_answer=no_answer), cancel_check=cancel_check)), daemon=True)
    t.start()
    try:
        t.join(10)
        stuck = t.is_alive()
    finally:
        stop.set()
        t.join(10)
    assert not stuck, "a shared-path backoff with no cancel never ended"
    assert len(asked) == 2 and asked[1] - asked[0] >= 0.55, "the shared-path row did not wait out its backoff"
    assert len(polls) >= 6, "the shared-path backoff did not poll the cancel while it waited"
    res = out["res"]
    assert (res["cancelled"], res["not_attempted"], res["restored_store"]) == (False, 0, 1)
