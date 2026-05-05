"""Live progress surface for long-running ops (TDB sync, Plex enum).

Backs the ops side-drawer in the UI: workers call `update_progress`
at every natural checkpoint they're already iterating across, the
/api/progress endpoint reads the latest state, and the UI renders
per-stage timeline + smoothed throughput + ETA.

Cancel protocol: API marks `status='cancelling'`; the worker calls
`is_cancelled` at each checkpoint and exits cleanly when True, then
calls `finish_progress(status='cancelled')` on the way out.

Schema lives in `op_progress` (db.py v33). One row per op, keyed by
op_id. Convention: `tdb_sync` for the singleton TDB sync; for Plex
enum we use one parent op `plex_enum` plus per-section children
`plex_enum:{section_id}` so the drawer can show stacked mini-bars.
"""
from __future__ import annotations

import json
import logging
from collections import deque
from pathlib import Path
from typing import Any

from .db import get_conn
from .events import now_iso

log = logging.getLogger(__name__)

# Rolling histories live in detail_json so a worker restart loses no
# more than the last few seconds of throughput data. Sized for the UI
# sparkline (30 samples, ~30s at 1s polling) and activity feed.
_THROUGHPUT_HISTORY = 30
_ACTIVITY_HISTORY = 5


def start_progress(
    db_path: Path,
    op_id: str,
    kind: str,
    *,
    stage: str | None = None,
    stage_label: str | None = None,
    stage_total: int = 0,
    processed_est: int = 0,
) -> None:
    """Insert (or reset) a progress row at op start.

    Resets any prior row for the same op_id — caller invariant is
    "only one of this op runs at a time", which holds for TDB sync
    (single-tenant) and Plex enum (worker serializes them).
    """
    now = now_iso()
    detail = json.dumps({"activity": [], "throughput": []})
    with get_conn(db_path) as conn:
        conn.execute(
            """INSERT INTO op_progress
                 (op_id, kind, status, started_at, updated_at,
                  stage, stage_label, stage_total, processed_est, detail_json)
               VALUES (?, ?, 'running', ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(op_id) DO UPDATE SET
                   kind = excluded.kind,
                   status = 'running',
                   started_at = excluded.started_at,
                   updated_at = excluded.updated_at,
                   finished_at = NULL,
                   stage = excluded.stage,
                   stage_label = excluded.stage_label,
                   stage_current = 0,
                   stage_total = excluded.stage_total,
                   processed_total = 0,
                   processed_est = excluded.processed_est,
                   error_count = 0,
                   detail_json = excluded.detail_json""",
            (op_id, kind, now, now,
             stage, stage_label, stage_total, processed_est, detail),
        )


def update_progress(
    db_path: Path,
    op_id: str,
    *,
    stage: str | None = None,
    stage_label: str | None = None,
    stage_current: int | None = None,
    stage_total: int | None = None,
    processed_total: int | None = None,
    processed_est: int | None = None,
    error_count: int | None = None,
    activity: str | None = None,
) -> None:
    """Patch the running row. Only fields passed are updated.

    `activity` strings push onto a rolling deque in detail_json; UI
    fades them in latest-first. Throughput samples are derived from
    the (processed_total, updated_at) delta — recorded automatically
    when processed_total advances.
    """
    now = now_iso()
    with get_conn(db_path) as conn:
        row = conn.execute(
            "SELECT updated_at, processed_total, detail_json "
            "FROM op_progress WHERE op_id = ?",
            (op_id,),
        ).fetchone()
        if row is None:
            return  # op was cleared; treat as no-op
        try:
            detail = json.loads(row["detail_json"] or "{}")
        except (TypeError, ValueError):
            detail = {}
        activity_buf = deque(detail.get("activity", []),
                             maxlen=_ACTIVITY_HISTORY)
        throughput_buf = deque(detail.get("throughput", []),
                               maxlen=_THROUGHPUT_HISTORY)
        if activity:
            activity_buf.appendleft({"ts": now, "msg": activity})
        # Throughput sample whenever processed_total advances. Rate
        # in items/sec; UI smooths across the buffer.
        if (processed_total is not None
                and processed_total > (row["processed_total"] or 0)):
            try:
                from datetime import datetime
                prev_ts = datetime.fromisoformat(
                    row["updated_at"].replace("Z", "+00:00"))
                now_dt = datetime.fromisoformat(now.replace("Z", "+00:00"))
                dt = max((now_dt - prev_ts).total_seconds(), 0.001)
                delta = processed_total - (row["processed_total"] or 0)
                throughput_buf.append({"ts": now,
                                       "rate": round(delta / dt, 3)})
            except (ValueError, AttributeError):
                pass
        detail["activity"] = list(activity_buf)
        detail["throughput"] = list(throughput_buf)
        sets = ["updated_at = ?"]
        args: list[Any] = [now]
        for col, val in [
            ("stage", stage),
            ("stage_label", stage_label),
            ("stage_current", stage_current),
            ("stage_total", stage_total),
            ("processed_total", processed_total),
            ("processed_est", processed_est),
            ("error_count", error_count),
        ]:
            if val is not None:
                sets.append(f"{col} = ?")
                args.append(val)
        sets.append("detail_json = ?")
        args.append(json.dumps(detail))
        args.append(op_id)
        conn.execute(
            f"UPDATE op_progress SET {', '.join(sets)} WHERE op_id = ?",
            args,
        )


def finish_progress(
    db_path: Path,
    op_id: str,
    *,
    status: str = "done",
    error_message: str | None = None,
) -> None:
    """Mark the op finished. status ∈ {'done', 'failed', 'cancelled'}.

    Row survives in the table for ~24h so the drawer's idle state
    can show the most-recent completion summary; the periodic sweep
    in `prune_finished` drops anything older.
    """
    now = now_iso()
    with get_conn(db_path) as conn:
        if error_message:
            row = conn.execute(
                "SELECT detail_json FROM op_progress WHERE op_id = ?",
                (op_id,),
            ).fetchone()
            try:
                detail = json.loads(row["detail_json"] or "{}") if row else {}
            except (TypeError, ValueError):
                detail = {}
            detail["error_message"] = error_message
            conn.execute(
                "UPDATE op_progress SET status = ?, finished_at = ?, "
                "updated_at = ?, detail_json = ? WHERE op_id = ?",
                (status, now, now, json.dumps(detail), op_id),
            )
        else:
            conn.execute(
                "UPDATE op_progress SET status = ?, finished_at = ?, "
                "updated_at = ? WHERE op_id = ?",
                (status, now, now, op_id),
            )


def is_cancelled(db_path: Path, op_id: str) -> bool:
    """Worker checkpoint: returns True if the API marked this op
    'cancelling'. Worker should bail at the next safe boundary,
    then call finish_progress(status='cancelled').
    """
    with get_conn(db_path) as conn:
        row = conn.execute(
            "SELECT status FROM op_progress WHERE op_id = ?",
            (op_id,),
        ).fetchone()
    return bool(row and row["status"] == "cancelling")


def request_cancel(db_path: Path, op_id: str) -> bool:
    """API endpoint: mark a running op for cancellation. Returns
    True if the op was running (and is now cancelling), False if
    no running row was found.
    """
    now = now_iso()
    with get_conn(db_path) as conn:
        cur = conn.execute(
            "UPDATE op_progress SET status = 'cancelling', updated_at = ? "
            "WHERE op_id = ? AND status = 'running'",
            (now, op_id),
        )
    return cur.rowcount > 0


def load_active(db_path: Path) -> list[dict]:
    """Read all running/cancelling rows + the most recent finished
    row per kind (so the drawer can show "last completion" summaries
    in idle state). Ordered by started_at DESC.
    """
    with get_conn(db_path) as conn:
        active = conn.execute(
            "SELECT * FROM op_progress "
            "WHERE status IN ('running', 'cancelling') "
            "ORDER BY started_at DESC"
        ).fetchall()
        # Most recent finished per kind, last 24h.
        finished = conn.execute(
            """SELECT * FROM op_progress
                 WHERE status IN ('done', 'failed', 'cancelled')
                   AND finished_at IS NOT NULL
                   AND finished_at > datetime('now', '-24 hours')
                 ORDER BY finished_at DESC
                 LIMIT 10"""
        ).fetchall()
    out: list[dict] = []
    for row in list(active) + list(finished):
        d = dict(row)
        try:
            d["detail"] = json.loads(d.pop("detail_json") or "{}")
        except (TypeError, ValueError):
            d["detail"] = {}
        out.append(d)
    return out


def prune_finished(db_path: Path) -> int:
    """Drop finished rows older than 24h. Cheap; called from the
    /api/progress read path so we don't need a dedicated sweeper.
    """
    with get_conn(db_path) as conn:
        cur = conn.execute(
            "DELETE FROM op_progress "
            "WHERE status IN ('done', 'failed', 'cancelled') "
            "  AND finished_at IS NOT NULL "
            "  AND finished_at < datetime('now', '-24 hours')"
        )
    return cur.rowcount
