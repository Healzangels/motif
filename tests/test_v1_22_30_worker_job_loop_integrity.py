"""v1.22.30 (audit) — worker job-loop integrity: 4 silent-failure fixes.

From the full-codebase audit (Tag B — worker job-loop):

1. downloader.download_theme returned a pre-existing theme.mp3 as a SUCCESSFUL
   DownloadResult even when it was 0-byte (a prior download crashed mid-ffmpeg /
   OOM / power-loss). The worker then recorded local_files + hardlinked a broken
   theme into Plex behind a green check. Now a 0-byte pre-existing file is
   removed + re-downloaded; only a >0-byte file short-circuits.

2. worker.run() ran a table-wide `running -> pending` reclaim at the TOP of
   every worker thread's loop. _supervised re-enters run() on ANY thread crash
   (not just boot), so a download thread restarting mid-life reset EVERY live
   sibling's in-flight job to pending -> a sibling re-claimed + re-ran it
   (duplicate downloads, double placement). The reclaim moved to
   _reclaim_orphan_jobs, called ONCE by start_worker before spawning threads.

3. TokenBucket with rate=0 (env MOTIF_DL_RATE_HOUR=0 or a hand-edited
   motif.yaml — load() never runs validate()) made `wait = deficit / _fill_rate`
   a ZeroDivisionError that killed the download thread on its 2nd acquire.
   _fill_rate is now floored at 1/period.

4. _dispatch sent an unknown job_type into _mark_failed (the retry ladder),
   burning the attempt budget re-dispatching a job no handler can ever run.
   Now _mark_failed_terminal — fail it once, no retry.
"""
from __future__ import annotations

import re
import sqlite3
from pathlib import Path

from app.core.worker import TokenBucket, _reclaim_orphan_jobs

REPO = Path(__file__).resolve().parent.parent
WORKER_PY = (REPO / "app" / "core" / "worker.py").read_text()
DOWNLOADER_PY = (REPO / "app" / "core" / "downloader.py").read_text()

_PY_COMMENT_RE = re.compile(r"#.*$", re.MULTILINE)


def _strip_comments(s: str) -> str:
    return _PY_COMMENT_RE.sub("", s)


# ── (3) TokenBucket: rate=0 must not divide by zero ───────────


def test_token_bucket_zero_rate_no_div_zero():
    # Pre-fix _fill_rate = 0/3600 = 0 -> the 2nd acquire's `deficit /
    # _fill_rate` raised ZeroDivisionError. Floored to 1/period.
    b = TokenBucket(rate=0.0, capacity=1, period=3600.0)
    assert b._fill_rate == 1.0 / 3600.0, (
        "rate=0 must floor _fill_rate to 1/period, never 0")


def test_token_bucket_positive_rate_unchanged():
    # A normal rate is unaffected by the floor.
    b = TokenBucket(rate=120.0, capacity=120, period=3600.0)
    assert b._fill_rate == 120.0 / 3600.0


def test_token_bucket_acquire_does_not_raise_at_zero_rate():
    # The first acquire drains the single token; the important part is that
    # constructing + the initial acquire path never trips ZeroDivisionError.
    b = TokenBucket(rate=0.0, capacity=1, period=3600.0)
    b.acquire(1)  # capacity token available immediately, returns at once


# ── (2) _reclaim_orphan_jobs behavioral ───────────────────────


def _fresh_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "motif.db"
    from app.core.db import init_db
    init_db(db_path)
    return db_path


def test_reclaim_resets_running_jobs_to_pending(tmp_path):
    db_path = _fresh_db(tmp_path)
    ts = "2026-06-08T00:00:00+00:00"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO jobs (job_type, status, created_at, started_at) "
            "VALUES ('download', 'running', ?, ?)", (ts, ts))
        conn.execute(
            "INSERT INTO jobs (job_type, status, created_at, started_at) "
            "VALUES ('sync', 'running', ?, ?)", (ts, ts))
        # a non-running job must be untouched
        conn.execute(
            "INSERT INTO jobs (job_type, status, created_at) "
            "VALUES ('place', 'pending', ?)", (ts,))
        conn.commit()

    _reclaim_orphan_jobs(db_path)

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT status, started_at FROM jobs WHERE job_type IN "
            "('download','sync') ORDER BY job_type").fetchall()
        # both reclaimed -> pending, started_at cleared
        assert all(r[0] == "pending" and r[1] is None for r in rows), rows
        place = conn.execute(
            "SELECT status FROM jobs WHERE job_type='place'").fetchone()
        assert place[0] == "pending"  # was already pending, unchanged


def test_reclaim_marks_running_sync_runs_failed(tmp_path):
    db_path = _fresh_db(tmp_path)
    ts = "2026-06-08T00:00:00+00:00"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO sync_runs (started_at, status) VALUES (?, 'running')",
            (ts,))
        conn.execute(
            "INSERT INTO sync_runs (started_at, status) VALUES (?, 'success')",
            (ts,))
        conn.commit()

    _reclaim_orphan_jobs(db_path)

    with sqlite3.connect(db_path) as conn:
        states = [r[0] for r in conn.execute(
            "SELECT status FROM sync_runs ORDER BY id").fetchall()]
        assert states == ["failed", "success"], states
        err = conn.execute(
            "SELECT error FROM sync_runs WHERE status='failed'").fetchone()[0]
        assert "restarted mid-sync" in (err or "")


def test_reclaim_is_idempotent(tmp_path):
    db_path = _fresh_db(tmp_path)
    ts = "2026-06-08T00:00:00+00:00"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO jobs (job_type, status, created_at, started_at) "
            "VALUES ('download', 'running', ?, ?)", (ts, ts))
        conn.commit()
    _reclaim_orphan_jobs(db_path)
    _reclaim_orphan_jobs(db_path)  # second run is a no-op
    with sqlite3.connect(db_path) as conn:
        st = conn.execute(
            "SELECT status FROM jobs WHERE job_type='download'").fetchone()[0]
        assert st == "pending"


# ── (2) source pins: reset moved out of run() into start_worker ─


def test_run_no_longer_resets_jobs_table_wide():
    # The destructive table-wide reset SQL must NOT live in run() anymore —
    # only inside the one-time _reclaim_orphan_jobs helper.
    run_start = WORKER_PY.index("def run(self) -> None:")
    run_body = WORKER_PY[run_start:run_start + 900]
    assert "UPDATE jobs SET status = 'pending'" not in run_body, (
        "v1.22.30: the running->pending reset must NOT run per-thread in run()")


def test_start_worker_calls_reclaim_before_spawn():
    sw_start = WORKER_PY.index("def start_worker(")
    # The reclaim call must appear before the first thread .start().
    reclaim_at = WORKER_PY.index("_reclaim_orphan_jobs(settings.db_path)", sw_start)
    first_start = WORKER_PY.index(".start()", sw_start)
    assert reclaim_at < first_start, (
        "v1.22.30: _reclaim_orphan_jobs must run before any thread spawns")


# ── (4) unknown job type -> terminal, not retry ───────────────


def test_unknown_job_type_is_terminal():
    disp_start = WORKER_PY.index("def _dispatch(self")
    disp_body = _strip_comments(WORKER_PY[disp_start:disp_start + 1200])
    # The else branch must use the terminal marker, not the retry ladder.
    assert 'self._mark_failed_terminal(job["id"], f"unknown job type' in disp_body, (
        "v1.22.30: unknown job_type must fail terminally (no retry)")


# ── (1) downloader 0-byte guard ───────────────────────────────


def test_downloader_zero_byte_guard():
    fn = DOWNLOADER_PY.index("def download_theme(")
    body = DOWNLOADER_PY[fn:fn + 4000]
    # Short-circuit only when the existing file is non-empty.
    assert "_existing_size > 0" in body, (
        "v1.22.30: a pre-existing theme.mp3 must be size-checked before reuse")
    # The stale 0-byte file is removed so the re-download can proceed.
    assert "stale 0-byte" in body and ".unlink()" in body, (
        "v1.22.30: a 0-byte pre-existing file must be removed + re-downloaded")


def test_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
