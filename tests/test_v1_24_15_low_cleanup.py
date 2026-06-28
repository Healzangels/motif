"""v1.24.15 — holistic-review LOW cleanup batch (wave 3/4 — the safe items).

#9  deorphan merge_orphan_collisions: breadcrumb UNCONDITIONALLY on a non-dry
    pass (was gated on rep.merged → a merged-0 destructive pass left no audit
    entry) + an amplifier-sweep abort cap (v1.18.10 class) on the DELETE-heavy
    walker.
#10 sync drop stamp + companion pending_updates DELETE wrapped in one
    transaction (were separate autocommits → a crash between them left a
    stamped-dropped theme with its moot pending row).
#11 op_progress RUNTIME stuck-sweep (progress.sweep_stuck + a 15-min scheduler
    job) — a raised finish_progress left an op 409-locked + a LIVE OPS phantom
    until container restart; jobs had _stuck_job_sweep, op_progress only a boot
    reset.
#12 corrected the inaccurate bulk cloud-backup gate comment.

(#1 migration idempotency + #8 PROMOTE lock-decomposition deferred to dedicated
passes — boot-critical / large-restructure, not live on the v67 deploy.)
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DEORPHAN_PY = (REPO / "app" / "core" / "deorphan.py").read_text()
SYNC_PY = (REPO / "app" / "core" / "sync.py").read_text()
SCHED_PY = (REPO / "app" / "core" / "scheduler.py").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


# ── #11 op_progress runtime stuck-sweep (behavioral) ─────────

def test_sweep_stuck_reaps_only_idle_ops(tmp_path):
    from app.core.db import init_db
    from app.core import progress
    db = tmp_path / "m.db"
    init_db(db)
    assert progress.try_acquire(db, "stale-op", "tdb_sync") is True
    assert progress.try_acquire(db, "fresh-op", "tdb_sync") is True
    # backdate the stale op far past the idle threshold.
    with sqlite3.connect(db) as conn:
        conn.execute(
            "UPDATE op_progress SET updated_at = ? WHERE op_id = 'stale-op'",
            ("2026-01-01T00:00:00+00:00",))
        conn.commit()

    reclaimed = progress.sweep_stuck(db, max_idle_minutes=90)
    assert reclaimed == 1, "only the idle op should be reaped"

    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        status = {r["op_id"]: r["status"]
                  for r in conn.execute("SELECT op_id, status FROM op_progress")}
    assert status["stale-op"] == "failed"
    assert status["fresh-op"] in ("pending", "running"), (
        "a live (recently-updated) op must NOT be reaped")


def test_op_progress_stuck_sweep_is_scheduled():
    assert "_sweep_stuck_op_progress" in SCHED_PY
    assert 'id="op_progress_stuck_sweep"' in SCHED_PY
    assert "op_progress.sweep_stuck(db_path)" in SCHED_PY


# ── #9 deorphan amplifier guard + unconditional breadcrumb ───

def test_deorphan_merge_has_amplifier_cap_and_breadcrumb():
    assert "_MERGE_ABORT_CAP = 250" in DEORPHAN_PY
    assert "if rep.merged >= _MERGE_ABORT_CAP:" in DEORPHAN_PY
    # breadcrumb no longer gated on rep.merged.
    assert "if not dry_run and rep.merged:" not in DEORPHAN_PY
    i = DEORPHAN_PY.index("Collision-merge consolidated")
    head = DEORPHAN_PY[i - 200:i]
    assert "if not dry_run:" in head


# ── #10 sync stamp/clear atomicity ───────────────────────────

def test_full_walk_stamp_clear_is_transactional():
    i = SYNC_PY.index("def _detect_and_stamp_drops_full_walk(")
    body = SYNC_PY[i:i + 4500]
    assert "with get_conn(db_path) as conn, transaction(conn):" in body
    # the stamp UPDATE + the companion DELETE both live under it.
    assert "UPDATE themes SET tdb_dropped_at = ?" in body
    assert "DELETE FROM pending_updates" in body


# ── #12 cloud-backup gate comment corrected ──────────────────

def test_cloud_backup_comment_corrected():
    assert "NARROWER than the\n    // per-row" in APP_JS
    assert "Gate matches the per-row SOURCE" not in APP_JS
