"""v1.24.20 — heartbeat-aware fast-reclaim of orphaned sync/plex_enum jobs.

A hung or dead sync left its `jobs` row in status='running' until the 120min
runtime backstop, pinning `themerrdb_sync_in_flight > 0` and wedging the
dashboard SYNC button disabled — with "no ops running", because the op_progress
stale-sweep had already emptied the LIVE OPS drawer. the user's repro: the button
stuck at "// SYNCING THEMERRDB…" long after the sync finished.

`_stuck_job_sweep` now reclaims a running sync/plex_enum job within minutes once
its op_progress heartbeat is stale/terminal/missing. A HEALTHY long sync keeps
its heartbeat fresh and is untouched (the 120min backstop still applies to it,
unchanged).
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone

from app.core import scheduler
from app.core.db import init_db


def _iso(dt):
    return dt.isoformat(timespec="seconds")


def _now():
    return datetime.now(timezone.utc)


def _db(tmp_path):
    db = tmp_path / "motif.db"
    init_db(db)
    return db


def _seed_job(db, *, job_type="sync", started_min_ago):
    started = _iso(_now() - timedelta(minutes=started_min_ago))
    with sqlite3.connect(db) as conn:
        cur = conn.execute(
            "INSERT INTO jobs (job_type, status, created_at, started_at) "
            "VALUES (?, 'running', ?, ?)", (job_type, started, started))
        conn.commit()
        return cur.lastrowid


def _seed_op(db, *, op_id="tdb_sync", status="running", updated_min_ago):
    upd = _iso(_now() - timedelta(minutes=updated_min_ago))
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO op_progress (op_id, kind, status, started_at, "
            " updated_at) VALUES (?, ?, ?, ?, ?)",
            (op_id, op_id, status, upd, upd))
        conn.commit()


def _status(db, job_id):
    with sqlite3.connect(db) as conn:
        return conn.execute(
            "SELECT status FROM jobs WHERE id=?", (job_id,)).fetchone()[0]


# ── heartbeat fast-reclaim fires when the op is dead ──────────────

def test_reclaims_sync_when_op_progress_missing(tmp_path):
    db = _db(tmp_path)
    jid = _seed_job(db, started_min_ago=20)  # past the 15min grace
    scheduler._stuck_job_sweep(db)           # no op_progress row at all
    assert _status(db, jid) == "failed"


def test_reclaims_sync_when_op_progress_terminal(tmp_path):
    db = _db(tmp_path)
    jid = _seed_job(db, started_min_ago=20)
    _seed_op(db, status="done", updated_min_ago=16)
    scheduler._stuck_job_sweep(db)
    assert _status(db, jid) == "failed"


def test_running_op_with_stale_heartbeat_is_NOT_reclaimed(tmp_path):
    # v1.24.23 regression guard: a still-RUNNING op whose heartbeat is stale is
    # a HEALTHY slow phase (e.g. a >15min first-run git clone emits no heartbeat),
    # NOT an orphan — it must NOT be reclaimed. Only terminal/missing ops are.
    db = _db(tmp_path)
    jid = _seed_job(db, started_min_ago=30)
    _seed_op(db, status="running", updated_min_ago=20)  # stale but RUNNING
    scheduler._stuck_job_sweep(db)
    assert _status(db, jid) == "running", (
        "a stale-but-running op (healthy slow clone) must NOT be reclaimed")


def test_reclaims_plex_enum_too(tmp_path):
    db = _db(tmp_path)
    jid = _seed_job(db, job_type="plex_enum", started_min_ago=20)
    _seed_op(db, op_id="plex_enum", status="failed", updated_min_ago=2)  # terminal
    scheduler._stuck_job_sweep(db)
    assert _status(db, jid) == "failed"


# ── the safety case: a HEALTHY long sync is NOT reclaimed ─────────

def test_healthy_long_sync_with_fresh_heartbeat_survives(tmp_path):
    db = _db(tmp_path)
    # running 30min (well past the 15min grace) but under the 120min backstop,
    # with a fresh heartbeat → genuinely working, must NOT be reclaimed.
    jid = _seed_job(db, started_min_ago=30)
    _seed_op(db, status="running", updated_min_ago=1)
    scheduler._stuck_job_sweep(db)
    assert _status(db, jid) == "running", (
        "a healthy long sync with a live heartbeat must NOT be reclaimed")


def test_recent_sync_within_grace_survives_even_if_op_missing(tmp_path):
    db = _db(tmp_path)
    # under the 15min grace: the job-running → start_progress startup window —
    # op may be missing or show the prior run's terminal row. Must not reclaim.
    jid = _seed_job(db, started_min_ago=5)
    _seed_op(db, status="done", updated_min_ago=4)  # prior run's terminal row
    scheduler._stuck_job_sweep(db)
    assert _status(db, jid) == "running"


# ── the 120min runtime backstop still fires (unchanged) ──────────

def test_runtime_backstop_still_reclaims_at_120min(tmp_path):
    db = _db(tmp_path)
    jid = _seed_job(db, started_min_ago=121)             # over sync threshold
    _seed_op(db, status="running", updated_min_ago=0)    # even with a fresh op
    scheduler._stuck_job_sweep(db)
    assert _status(db, jid) == "failed"


# ── non-singleton job types unaffected by the heartbeat path ─────

def test_download_job_not_subject_to_heartbeat_path(tmp_path):
    db = _db(tmp_path)
    # a download running 20min: under its 30min threshold, and 'download' is NOT
    # a heartbeat-tracked type — only the runtime threshold governs it.
    jid = _seed_job(db, job_type="download", started_min_ago=20)
    scheduler._stuck_job_sweep(db)
    assert _status(db, jid) == "running"


# ── source pins ──────────────────────────────────────────────────

def test_heartbeat_constants_present():
    from pathlib import Path
    src = (Path(__file__).resolve().parent.parent
           / "app" / "core" / "scheduler.py").read_text()
    assert "_HEARTBEAT_STALE_MIN = 15" in src
    assert '_JOB_OPID = {"sync": "tdb_sync", "plex_enum": "plex_enum"}' in src
    assert "over_threshold or hb_dead" in src
