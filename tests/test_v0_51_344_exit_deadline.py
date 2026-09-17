"""v0.51.344 integration fix R1-F1: exit's RESTORE FROM PLEX deadline is anchored at the signal, so a docker stop fits its grace.

  A backup download the client stopped reading held uvicorn's drain in front of the deadline, and a worker mid-download
  (yt-dlp takes no cancel callback) held a 10 s join in front of the restore's — either way the publish gate closed after
  docker's SIGKILL. Both now run inside one deadline from the signal. R1-F2 rides along: the run exit leaves waiting on
  Plex gets its audit row and event from exit itself.
"""
from __future__ import annotations

import json
import os
import signal
import socket
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.core.db import init_db
from test_v0_51_342_canonical_round3 import _DOCKER_STOP_GRACE_S, _stop_grace_premise, _trickling_plex
from test_v0_51_342_restore_from_plex_job import START
from test_v0_51_342_restore_pool import SHARED
from test_v0_51_342_restore_shutdown_cancel import _free_port, _http, _seed_rows, _up

REPO = Path(__file__).resolve().parents[1]

_CHILD = r"""
import sys, time
sys.path.insert(0, sys.argv[1])
import app.core.canonical_health as ch
import app.main as motif_main
print("child imports " + motif_main.__file__ + " " + ch.__file__, flush=True)
if sys.argv[2] == "busy-worker":
    from app.core import worker as worker_mod

    def download(self, job):
        # yt-dlp takes no cancel callback: a download claimed before the signal runs on through exit
        print("PROBE the worker claimed the download", flush=True)
        time.sleep(30)
    worker_mod.Worker._do_download = download
sys.exit(motif_main.main())
"""


def _hold_a_download(port, name):
    """A backup download whose client reads the headers and then nothing: the response stays in flight until closed."""
    s = socket.create_connection(("127.0.0.1", port), timeout=10)
    s.sendall((f"GET /api/admin/database-backup/download/{name} HTTP/1.1\r\nHost: 127.0.0.1\r\n"
               "X-Authentik-Username: testadmin\r\n\r\n").encode())
    head = b""
    while b"\r\n\r\n" not in head:
        chunk = s.recv(4096)
        assert chunk, "the download closed before its headers arrived"
        head += chunk
    assert head.startswith(b"HTTP/1.1 200"), head[:200]
    return s


def _stopped_mid_restore(tmp_path, hold):
    """motif stopped while RESTORE FROM PLEX waits on a Plex listing that never ends, with `hold` in its way."""
    assert _stop_grace_premise(REPO) == ([], [], []), "premise: a deploy file sets its own stop grace or signal"
    from app.core.auth import create_admin, init_auth_schema
    cfg, data, themes = tmp_path / "config", tmp_path / "data", tmp_path / "themes"
    cfg.mkdir()
    themes.mkdir()
    db = cfg / "motif.db"
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    _seed_rows(db, (601, 602), file_path=SHARED)
    with sqlite3.connect(db) as conn:
        if hold == "slow-download":
            # a backup larger than the loopback socket buffers: the download stalls once its client stops reading
            conn.execute("CREATE TABLE pad (b BLOB)")
            conn.executemany("INSERT INTO pad VALUES (?)", [(os.urandom(1 << 20),) for _ in range(24)])
        else:
            conn.execute("INSERT INTO jobs (job_type, media_type, tmdb_id, section_id, status, created_at) "
                         "VALUES ('download', 'movie', 700, '1', 'pending', ?)",
                         (datetime.now(timezone.utc).isoformat(timespec="seconds"),))
        conn.commit()
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
        proc = subprocess.Popen([sys.executable, "-c", _CHILD, str(REPO), hold], cwd=str(REPO), env=env_vars,
                                stdout=out, stderr=subprocess.STDOUT)
    held = None
    try:
        until(lambda: _up(web_port), "motif to come up")
        if hold == "slow-download":
            name = _http(web_port, "POST", "/api/admin/database-backup")["backup"]["name"]
        else:
            until(lambda: "PROBE the worker claimed the download" in log_text(), "a worker to claim the download")
        assert _http(web_port, "POST", START)["started"] is True
        until(lambda: plex["listings"], "the shared-path row's Plex request to hang")
        if hold == "slow-download":
            held = _hold_a_download(web_port, name)
        sent = time.monotonic()
        proc.send_signal(signal.SIGTERM)
        until(lambda: "motif stopped" in log_text(), "motif to pass its exit deadline", seconds=15)
        took = time.monotonic() - sent
    finally:
        plex["release"].set()
        if held is not None:
            held.close()
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
    assert proc.returncode == 0, text[-4000:]
    return text, took, db, cfg


def _records(db):
    with sqlite3.connect(db) as conn:
        audits = [json.loads(d) for (d,) in conn.execute(
            "SELECT details FROM audit_events WHERE action = 'canonical_restore_from_plex' ORDER BY id")]
        events = conn.execute("SELECT level, message FROM events WHERE message LIKE "
                              "'Canonical restore from Plex by testadmin%' ORDER BY rowid").fetchall()
    return audits, events


@pytest.mark.parametrize("hold", ["slow-download", "busy-worker"])
def test_a_motif_stopped_mid_restore_closes_its_publish_gate_inside_dockers_stop_grace(tmp_path, hold):
    text, took, db, cfg = _stopped_mid_restore(tmp_path, hold)
    if hold == "slow-download":
        assert "timeout graceful shutdown exceeded" in text, "premise: the unread download was still in flight at the signal"
    assert "RESTORE FROM PLEX was still waiting on Plex" in text and "no new write starts" in text, text[-4000:]
    assert text.index("no new write starts") < text.index("motif stopped")
    assert took < _DOCKER_STOP_GRACE_S, \
        f"exit took {took:.2f} s after SIGTERM — past docker stop's {_DOCKER_STOP_GRACE_S:.0f} s grace, so it is SIGKILLed"
    # R1-F2: the job thread never reaches its own record — exit wrote it, inside the same deadline
    audits, events = _records(db)
    assert [(a["status"], a["restored"], a["not_attempted"]) for a in audits] == [("interrupted", 0, 2)], audits
    assert events == [("WARNING", "Canonical restore from Plex by testadmin was cut off by a motif shutdown after "
                                  "0 restored — RUN CHECK after the restart")], events
    assert "cut off by a motif shutdown" in text and text.index("cut off by a motif shutdown") < text.index("motif stopped")
    assert json.loads((cfg / "canonical_health" / "restore_from_plex.json").read_text())["status"] == "running", \
        "the next start must report the run cut off"
