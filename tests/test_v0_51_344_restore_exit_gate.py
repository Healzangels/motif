"""v0.51.344 PB-065: once exit closes publishing, a Plex answer arriving later starts no write the interpreter could freeze."""
from __future__ import annotations

import json
import os
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from app.core import canonical_health as ch
from app.core.db import init_db
from test_v0_51_342_restore_from_plex_job import START
from test_v0_51_342_restore_pool import FakePlex, _lf_cols, _seed_store
from test_v0_51_342_restore_shutdown_cancel import _free_port, _http, _seed_rows, _up

REPO = Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def open_publishing(monkeypatch):
    monkeypatch.setattr(ch, "_PUBLISH_CLOSED", threading.Event())
    monkeypatch.setattr(ch, "_PUBLISH_LOCK", threading.Lock())


def test_no_store_publish_starts_after_exit_closes_publishing(tmp_path):
    db, themes = _seed_store(tmp_path, n=2)
    lock, asked = threading.Lock(), []
    both_asked, answer, cancel = threading.Event(), threading.Event(), threading.Event()

    def on_fetch(rk):
        with lock:
            asked.append(rk)
            if len(asked) == 2:
                both_asked.set()
        answer.wait(10)
    out: dict = {}
    run = threading.Thread(target=lambda: out.update(res=ch.restore_from_plex(
        db, themes, None, workers=2, plex_client_factory=lambda: FakePlex(on_fetch=on_fetch),
        cancel_check=cancel.is_set)), daemon=True)
    run.start()
    try:
        assert both_asked.wait(10), "premise: both fetches are waiting on Plex"
        cancel.set()
        assert ch.close_publishing(1.0) is True, "nothing was mid-write, so closing must not time out"
    finally:
        answer.set()
    run.join(10)
    assert not run.is_alive()
    assert [s["reason"] for s in out["res"]["skipped"]] == ["motif_exiting"] * 2
    assert [p for p in themes.rglob("*") if p.is_file()] == [], "a publish wrote after exit closed publishing"
    assert [_lf_cols(db, t, ("canonical_present", "file_sha256")) for t in (601, 602)] == [(0, "0" * 64)] * 2, \
        "a refused publish still recorded its bytes"


def test_closing_waits_for_the_publish_already_writing_and_that_row_lands_whole(tmp_path, monkeypatch):
    db, themes = _seed_store(tmp_path, n=1)
    stamping, go = threading.Event(), threading.Event()
    real = ch._stamp_restored

    def stamp(*a, **k):
        stamping.set()
        go.wait(10)
        return real(*a, **k)
    monkeypatch.setattr(ch, "_stamp_restored", stamp)
    run = threading.Thread(target=lambda: ch.restore_from_plex(db, themes, FakePlex()), daemon=True)
    run.start()
    closed: dict = {}
    closer = threading.Thread(target=lambda: closed.update(ok=ch.close_publishing(5.0)), daemon=True)
    try:
        assert stamping.wait(10), "premise: the publish is between its replace and its stamp"
        closer.start()
        closer.join(0.2)
        assert closer.is_alive(), "closing returned while a publish was still writing"
        assert ch.close_publishing(0.05) is False, "a close that timed out behind a write must say so"
    finally:
        go.set()
    closer.join(10)
    run.join(10)
    assert closed == {"ok": True}
    assert (themes / "movies" / "601" / "theme.mp3").is_file()
    assert _lf_cols(db, 601, ("canonical_present",)) == (1,), "the publish closing waited for did not land whole"


class _GateHeldOpenOnRead(threading.Event):
    """The closed gate: its first read under the publish lock takes the state, then holds that answer until `resume` is set."""
    def __init__(self):
        super().__init__()
        self.reading, self.resume = threading.Event(), threading.Event()
        self.resume_never_came = False
        self.under_the_lock = lambda: True  # v0.51.344: the bulk's loop reads the gate outside the lock — those reads pass

    def is_set(self):
        seen = super().is_set()
        if not self.reading.is_set() and self.under_the_lock():
            self.reading.set()
            self.resume_never_came = not self.resume.wait(10)
        return seen


class _LockReportingContention:
    """The publish lock: an acquire that finds it held sets `contended` before it waits, as a real lock would."""
    def __init__(self, contended: threading.Event):
        self._lock, self._contended = threading.Lock(), contended

    def acquire(self, blocking=True, timeout=-1):
        if self._lock.acquire(False):
            return True
        self._contended.set()
        return self._lock.acquire(blocking, timeout)

    def release(self):
        self._lock.release()

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *exc):
        self.release()


def _publish_state(db, themes):
    """Everything a store publish writes: its database rows, and every file under the themes dir with its bytes."""
    with sqlite3.connect(db) as conn:
        rows = [conn.execute(f"SELECT * FROM {t} ORDER BY tmdb_id").fetchall() for t in ("local_files", "placements")]
    return rows, {p.relative_to(themes).as_posix(): p.read_bytes() for p in sorted(themes.rglob("*")) if p.is_file()}


def test_a_publish_reading_the_gate_open_as_exit_closes_writes_nothing_after_close_returns_true(tmp_path, monkeypatch):
    db, themes = _seed_store(tmp_path, n=1)
    gate = _GateHeldOpenOnRead()
    monkeypatch.setattr(ch, "_PUBLISH_CLOSED", gate)
    # a close that finds the lock held is waiting on this publish: only then does the gate's read answer, so the
    # publish reads "open" and runs on however it is placed against the lock, and the close sees whichever order it has
    lock = _LockReportingContention(gate.resume)
    gate.under_the_lock = lock._lock.locked
    monkeypatch.setattr(ch, "_PUBLISH_LOCK", lock)
    out: dict = {}
    run = threading.Thread(target=lambda: out.update(res=ch.restore_from_plex(db, themes, FakePlex())), daemon=True)
    run.start()
    try:
        assert gate.reading.wait(10), "premise: the publish is reading the gate"
        closed = ch.close_publishing(10.0)
        at_close = _publish_state(db, themes)
    finally:
        gate.resume.set()
    run.join(10)
    assert not run.is_alive()
    assert not gate.resume_never_came, "premise: the gate's read was released by the close, not by its hang guard"
    assert closed is True, "the one publish finished its write, so closing must not time out"
    assert _publish_state(db, themes) == at_close, \
        "a publish wrote after close_publishing returned True — it read the gate outside the lock close waits on"
    assert out["res"]["restored_store"] == 1 and (themes / "movies" / "601" / "theme.mp3").is_file()
    assert _lf_cols(db, 601, ("canonical_present",)) == (1,), "the publish that read the gate open did not land whole"


_CHILD = r"""
import sys, time
sys.path.insert(0, sys.argv[1])
import app.core.canonical_health as ch
import app.main as motif_main
print("child imports " + ch.__file__, flush=True)
motif_main._RESTORE_EXIT_WAIT_S = float(sys.argv[2])
real_stamp = ch._stamp_restored

def stamp(*a, **k):
    # a slow re-hash and BEGIN IMMEDIATE: the window a finalizing interpreter freezes
    time.sleep(0.3)
    return real_stamp(*a, **k)

ch._stamp_restored = stamp
sys.exit(motif_main.main())
"""


def _answering_plex(n):
    """A Plex whose theme listings each wait for their own release, in arrival order."""
    state = {"listings": [], "releases": [threading.Event() for _ in range(n)], "lock": threading.Lock()}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            path = self.path.split("?")[0]
            if path.startswith("/library/metadata/") and path.endswith("/themes"):
                rk = path.split("/")[3]
                with state["lock"]:
                    k = len(state["listings"])
                    state["listings"].append(rk)
                if k < n:
                    state["releases"][k].wait(30)
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


def test_a_plex_answer_after_exits_deadline_writes_nothing_exit_leaves_unstamped(tmp_path):
    from app.core.auth import create_admin, init_auth_schema
    cfg, data, themes = tmp_path / "config", tmp_path / "data", tmp_path / "themes"
    cfg.mkdir()
    themes.mkdir()
    db = cfg / "motif.db"
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    _seed_rows(db, (601, 602))
    srv, plex = _answering_plex(2)
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
        proc = subprocess.Popen([sys.executable, "-c", _CHILD, str(REPO), "0.5"], cwd=str(REPO), env=env_vars,
                                stdout=out, stderr=subprocess.STDOUT)
    try:
        until(lambda: _up(web_port), "motif to come up")
        assert _http(web_port, "POST", START)["started"] is True
        until(lambda: len(plex["listings"]) == 2, "both rows to wait on Plex")
        proc.send_signal(signal.SIGTERM)
        until(lambda: "motif stopped" in log_text(), "motif to pass its exit deadline")
        # Plex answers after the deadline, the second row a little after the first: exit waits on the pool's
        # threads, then finalizes while the first row's publish would still be stamping
        time.sleep(0.3)
        plex["releases"][0].set()
        time.sleep(0.15)
        plex["releases"][1].set()
        proc.wait(timeout=60)
    finally:
        for release in plex["releases"]:
            release.set()
        if proc.poll() is None:
            proc.kill()
            proc.wait(10)
        srv.shutdown()
        srv.server_close()
    text = log_text()
    assert f"child imports {REPO / 'app' / 'core' / 'canonical_health.py'}" in text, "the child ran another tree"
    assert proc.returncode == 0, text[-4000:]
    assert "RESTORE FROM PLEX was still waiting on Plex" in text and "no new write starts" in text, text[-4000:]
    with sqlite3.connect(db) as conn:
        stamped = {r[0] for r in conn.execute("SELECT tmdb_id FROM local_files WHERE canonical_present = 1")}
    on_disk = {int(p.parent.name) for p in themes.rglob("theme.mp3")}
    assert on_disk == stamped, "a Plex answer after exit's deadline wrote a canonical the exit left unstamped"
    assert list(themes.rglob("*.part")) == []
