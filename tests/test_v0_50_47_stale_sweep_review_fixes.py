"""v0.50.47 — code-review fixes for the v0.50.41 stale-sweep rewrite.

Three issues the review surfaced in reset_stale_on_boot / sweep_stuck:

  A1 — _finalize_stale_detail closed the in-flight stage at the REAP time (`now`),
       not the op's last-progress time. sweep_stuck reaps 90 min after a stall, so
       the crashed op's final stage recorded ~90 min of pure idle and dwarfed the
       RUN INSIGHT waterfall — the opposite of the accuracy the teardown exists for.
       Fix: close at the row's updated_at.
  B1 — the per-row UPDATE matched on op_id only; with sweep_stuck running while
       workers are LIVE, a row that finishes between the SELECT and its UPDATE got
       stomped back to 'failed'. Fix: re-guard the UPDATE on the non-terminal status.
  B2 — the per-row loop wasn't transactional (autocommit), so a mid-loop crash left
       a partial sweep. Fix: wrap in one transaction.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from app.core import progress
from app.core.db import init_db

REPO = Path(__file__).resolve().parent.parent
PROG = (REPO / "app" / "core" / "progress.py").read_text()


@pytest.fixture()
def db(tmp_path: Path) -> Path:
    p = tmp_path / "motif.db"
    init_db(p)
    return p


# ── A1: stage closed at last-progress, not reap time (behavioral) ──

def test_reaped_stage_closes_at_updated_at_not_reap_time(db):
    # an op that ran its 'fetch' stage T0..T0+10s, then stalled. updated_at is the
    # last progress (T0+10s); the reap fires ~a day later (real now_iso()).
    t0 = "2026-06-28T00:00:00+00:00"
    t_last = "2026-06-28T00:00:10+00:00"
    detail = json.dumps({
        "stage_timings": [],
        "_stage_key": "fetch",
        "_stage_label": "Fetching",
        "_stage_started": t0,
    })
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO op_progress "
            "  (op_id, kind, status, started_at, updated_at, detail_json) "
            "VALUES (?, ?, 'running', ?, ?, ?)",
            ("plex_enum", "plex_enum", t0, t_last, detail),
        )
    progress.reset_stale_on_boot(db)
    with sqlite3.connect(db) as conn:
        out = json.loads(conn.execute(
            "SELECT detail_json FROM op_progress WHERE op_id='plex_enum'"
        ).fetchone()[0])
    timings = out["stage_timings"]
    assert len(timings) == 1
    # closed at updated_at (10s), NOT at the reap `now` (~a day = tens of thousands s)
    assert timings[0]["seconds"] == 10.0, timings
    assert "_stage_key" not in out  # trackers still cleaned up


# ── B1: a row that finishes before its UPDATE is not stomped ───────

def test_status_guard_protects_a_concurrently_finished_row(db):
    # Simulate the race outcome directly: sweep_stuck SELECTed this op while stale,
    # but by UPDATE time it's already 'done'. The status-guarded UPDATE must no-op.
    # (We assert the guard exists in source AND that a 'done' row is never reaped.)
    done_at = "2026-06-03T00:00:00+00:00"
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO op_progress "
            "  (op_id, kind, status, started_at, updated_at, finished_at) "
            "VALUES ('d1', 'tdb_sync', 'done', ?, ?, ?)",
            (done_at, done_at, done_at),
        )
    progress.sweep_stuck(db)
    with sqlite3.connect(db) as conn:
        status = conn.execute(
            "SELECT status FROM op_progress WHERE op_id='d1'").fetchone()[0]
    assert status == "done"


def test_both_sweeps_reguard_status_on_the_update():
    # the per-row UPDATE re-checks the non-terminal status at write time (TOCTOU).
    # `  AND status IN (...)` (leading indent) is unique to the two UPDATE guards;
    # the SELECTs use `WHERE status IN (...)`.
    assert PROG.count("  AND status IN ('pending', 'running', 'cancelling')") == 2


def test_both_sweeps_are_transactional():
    # the reap loop is wrapped in one transaction so it stays all-or-nothing
    assert PROG.count("with transaction(conn):") >= 2
    # guarded so the empty (no stale rows) case takes no write lock
    assert PROG.count("if rows:") == 2
