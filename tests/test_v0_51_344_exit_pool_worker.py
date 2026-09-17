"""v0.51.344 integration fix R3-F10: exit is bounded when a RESTORE FROM PLEX pool worker is inside a Plex read that never ends.

  The deadline closed publishing, exit recorded the run and logged 'motif stopped' — then the interpreter joined the
  restore pool's worker threads (concurrent.futures registers them with threading's exit) and sat inside the worker's
  Plex read until Plex answered or its 60 s timeout fired. docker's SIGKILL came first: an orderly log beside a container
  that died hard. main() now leaves at once when the job is still on Plex after its own record, its log flushed.
"""
from __future__ import annotations

import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

from app.core.auth import create_admin, init_auth_schema
from app.core.db import init_db
from test_v0_51_342_canonical_round3 import _DOCKER_STOP_GRACE_S, _stop_grace_premise, _trickling_plex
from test_v0_51_342_restore_from_plex_job import START
from test_v0_51_342_restore_shutdown_cancel import _free_port, _http, _seed_rows, _up

REPO = Path(__file__).resolve().parents[1]

_CHILD = r"""
import sys, threading
sys.path.insert(0, sys.argv[1])
import app.core.canonical_health as ch
import app.main as motif_main
print("child imports " + motif_main.__file__ + " " + ch.__file__, flush=True)
real_fetch = ch._fetch_from_plex_store

def fetch(plex_client, r):
    print("PROBE fetch on " + threading.current_thread().name, flush=True)
    return real_fetch(plex_client, r)
ch._fetch_from_plex_store = fetch
sys.exit(motif_main.main())
"""


def test_a_motif_stopped_while_a_pool_fetch_never_ends_exits_inside_dockers_stop_grace(tmp_path):
    assert _stop_grace_premise(REPO) == ([], [], []), "premise: a deploy file sets its own stop grace or signal"
    cfg, data, themes = tmp_path / "config", tmp_path / "data", tmp_path / "themes"
    cfg.mkdir()
    themes.mkdir()
    db = cfg / "motif.db"
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    _seed_rows(db, (601, 602))  # each row its own path: the pool's legs, not the serial shared-path tail
    srv, plex = _trickling_plex()
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
    exited = None
    try:
        until(lambda: _up(web_port), "motif to come up")
        assert _http(web_port, "POST", START)["started"] is True
        until(lambda: len(plex["listings"]) == 2, "both rows' Plex requests to hang on pool workers")
        sent = time.monotonic()
        proc.send_signal(signal.SIGTERM)
        # Plex is NOT released: the read a pool worker sits in never ends inside this window
        try:
            proc.wait(timeout=_DOCKER_STOP_GRACE_S + 5)
            exited = time.monotonic() - sent
        except subprocess.TimeoutExpired:
            exited = None
    finally:
        plex["release"].set()
        try:
            proc.wait(timeout=30)
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait(10)
            srv.shutdown()
            srv.server_close()
    text = log_text()
    assert f"child imports {REPO / 'app' / 'main.py'} {REPO / 'app' / 'core' / 'canonical_health.py'}" in text, \
        "the child ran another tree"
    assert "PROBE fetch on restore-from-plex_" in text, "premise: the hung Plex read is on a pool worker thread"
    assert "no new write starts" in text and "motif stopped" in text, text[-4000:]
    assert text.index("no new write starts") < text.index("motif stopped")
    assert exited is not None, \
        f"motif did not exit within {_DOCKER_STOP_GRACE_S + 5:.0f} s of SIGTERM: the interpreter waited for the pool " \
        f"worker's Plex read after 'motif stopped'\n{text[-3000:]}"
    assert exited < _DOCKER_STOP_GRACE_S, \
        f"exit took {exited:.2f} s after SIGTERM — past docker stop's {_DOCKER_STOP_GRACE_S:.0f} s grace, so it is SIGKILLed"
    assert proc.returncode == 0, text[-4000:]
    assert "leaves without waiting" in text and text.index("leaves without waiting") < text.index("motif stopped"), \
        "exit did not say it left the pool worker's Plex read behind"
    with sqlite3.connect(db) as conn:
        audits = [json.loads(d) for (d,) in conn.execute(
            "SELECT details FROM audit_events WHERE action = 'canonical_restore_from_plex'")]
    assert [(a["status"], a["restored"], a["not_attempted"]) for a in audits] == [("interrupted", 0, 2)], audits
    assert json.loads((cfg / "canonical_health" / "restore_from_plex.json").read_text())["status"] == "running", \
        "the next start must report the run cut off"
