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


def set_detail_field(db_path: Path, op_id: str, key: str, value: Any) -> None:
    """Patch a single arbitrary key into the running op's detail_json
    without touching the standard fields (activity / throughput / stage).

    v1.12.121 (Phase A): used to flag `fallback_active=True` when the
    snapshot path couldn't reach codeload and the run fell back to the
    remote per-item HTTP path. Surfaced through /api/progress so the
    status bar can render a sticky // FALLBACK indicator on the most
    recent finished tdb_sync row until the next successful run.

    Idempotent on missing op_id (no-op).
    """
    with get_conn(db_path) as conn:
        row = conn.execute(
            "SELECT detail_json FROM op_progress WHERE op_id = ?",
            (op_id,),
        ).fetchone()
        if row is None:
            return
        try:
            detail = json.loads(row["detail_json"] or "{}")
        except (TypeError, ValueError):
            detail = {}
        detail[key] = value
        conn.execute(
            "UPDATE op_progress SET detail_json = ? WHERE op_id = ?",
            (json.dumps(detail), op_id),
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

    v1.12.109: also synthesizes queue ops for download / place / scan
    job types straight off the jobs table — those don't write
    op_progress rows (the worker already tracks them via job
    lifecycle), but the UI wants the same op-card / op-mini render
    treatment so the legacy "QUEUED · 3R / 5P" topbar text can
    retire. Synthesized rows are read-only (cancel-all isn't wired);
    per-job cancel still lives at /queue.
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
        # Synthesized queue rows. One row per job_type with at least
        # one pending or running job. Counts drive the mini-bar
        # label; bar runs in indeterminate-shimmer mode (stage_total=0)
        # since a queue depth changes faster than a fixed-total
        # progress bar can usefully represent.
        # v1.12.118: also synthesize refresh/relink/adopt so those job
        # types flow through the ops drawer instead of the legacy
        # "REFRESH PENDING · N" / "QUEUED · NR / NP" status text.
        # 'sync' and 'plex_enum' have real op_progress rows so they're
        # excluded here.
        queue_counts = conn.execute(
            """SELECT job_type,
                      SUM(CASE WHEN status='running' THEN 1 ELSE 0 END) AS running_n,
                      SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) AS pending_n
                 FROM jobs
                WHERE status IN ('pending','running')
                  AND job_type IN ('download','place','scan',
                                   'refresh','relink','adopt')
                GROUP BY job_type"""
        ).fetchall()
        # v1.13.18 (6C): pull the running download job ids so we can
        # blend yt-dlp's per-job progress fraction into the synthesized
        # download_queue card. Only active 'running' download jobs
        # contribute — pending jobs haven't started, can't have progress.
        running_dl_jobs = [
            row["id"] for row in conn.execute(
                "SELECT id FROM jobs "
                "WHERE job_type = 'download' AND status = 'running'"
            ).fetchall()
        ]
    out: list[dict] = []
    for row in list(active) + list(finished):
        d = dict(row)
        try:
            d["detail"] = json.loads(d.pop("detail_json") or "{}")
        except (TypeError, ValueError):
            d["detail"] = {}
        out.append(d)
    out.extend(_synthesize_queue_ops(queue_counts, running_dl_jobs))
    return out


_QUEUE_BURST_HW: dict[str, int] = {}

# v1.13.18 (6C): per-job download fraction (0.0-1.0). Updated by
# yt-dlp's progress_hooks (wired in worker.py); read by
# _synthesize_queue_ops to enrich the download_queue card with real
# percentage. Single threaded writes (one worker job at a time) so a
# plain dict suffices — GIL covers the read in synthesize. Cleared
# on job finish/cancel so stale entries don't linger.
_DOWNLOAD_PROGRESS: dict[int, float] = {}


def set_download_progress(job_id: int, fraction: float) -> None:
    """v1.13.18: yt-dlp progress hook entrypoint. fraction in [0, 1]."""
    if 0.0 <= fraction <= 1.0:
        _DOWNLOAD_PROGRESS[job_id] = fraction


def clear_download_progress(job_id: int) -> None:
    """v1.13.18: drop the job's progress entry on terminal state."""
    _DOWNLOAD_PROGRESS.pop(job_id, None)


def _synthesize_queue_ops(counts, running_dl_jobs=None) -> list[dict]:
    """Build virtual op rows from jobs counts. Mirrors the shape of
    a real op_progress row so the UI doesn't need a separate render
    path. Status='running' whenever any job is running; 'pending'
    when only queued (worker not yet picked up).

    v1.13.21: running_dl_jobs is the list of currently-running download
    job ids the caller already pulled from the jobs table. Pre-fix this
    function used the name as a free variable inherited from the caller's
    frame, which raised NameError at runtime as soon as queue_counts had
    any rows (any pending/running job triggered it). The /api/progress
    handler 500'd, the drawer fell back to its empty state, and no status
    bars showed during a download (or scan, place, refresh — anything
    that lands in the synthesized-queue path). Default to [] so callers
    that only care about non-download queues can keep calling with one
    arg.

    v1.13.4: track a per-job-type high-water mark so we can express
    queue progress as (hw - remaining) / hw — a real number bar
    fill instead of v1.12.124's indeterminate shimmer. The HW
    resets to 0 when both running + pending hit 0 (queue drained).
    Pre-fix the bar pulsed full-width without a number; the user
    couldn't tell whether a long refresh queue was 10% or 90%
    through.
    """
    if running_dl_jobs is None:
        running_dl_jobs = []
    label_map = {
        "download": ("DOWNLOAD QUEUE", "Downloading themes"),
        "place":    ("PLACE QUEUE",    "Placing themes into Plex"),
        "scan":     ("DISK SCAN",      "Scanning canonical themes"),
        # v1.12.118: refresh = post-place Plex metadata nudges (~30s
        # delay) so Plex re-scans the folder and picks up the sidecar.
        # relink = repair stale placements (folder paths Plex moved).
        # adopt = bulk-adopt sidecars motif found via scan.
        "refresh":  ("REFRESH QUEUE",  "Nudging Plex to re-scan"),
        "relink":   ("RELINK QUEUE",   "Re-linking moved placements"),
        "adopt":    ("ADOPT QUEUE",    "Adopting sidecars"),
    }
    now = now_iso()
    out: list[dict] = []
    # v1.13.4: bookkeeping pass — reset HW for any job_type that
    # has fully drained since the last poll, so the next burst
    # starts fresh from 0. The counts query only returns job_types
    # with at least one in-flight row, so absence here = drained.
    seen_active = {row["job_type"] for row in counts
                   if (row["running_n"] or 0) + (row["pending_n"] or 0) > 0}
    for jt in list(_QUEUE_BURST_HW.keys()):
        if jt not in seen_active:
            del _QUEUE_BURST_HW[jt]
    for row in counts:
        jt = row["job_type"]
        running_n = row["running_n"] or 0
        pending_n = row["pending_n"] or 0
        if running_n + pending_n == 0:
            continue
        remaining = running_n + pending_n
        # Update high-water if the queue has grown (new jobs landed
        # mid-burst). Never decreases until the queue fully drains.
        prior_hw = _QUEUE_BURST_HW.get(jt, 0)
        hw = max(prior_hw, remaining)
        _QUEUE_BURST_HW[jt] = hw
        completed_in_burst = max(0, hw - remaining)
        kind_label, stage_label = label_map.get(jt, (jt.upper(), jt))
        # Detail label encodes counts so the mini-bar's stage_label
        # carries the info the legacy "QUEUED · NR / MP" text used
        # to. Bar itself runs indeterminate (stage_total=0).
        # v1.13.30: drop the count suffix when the burst is a single
        # op. Pre-fix the topbar read "NUDGING PLEX TO RE-SCAN — 1
        # QUEUED" while a lone nudge was the only thing in flight —
        # the user read "queued" as "queued behind something else"
        # rather than "this lone item is in the queue". Singular
        # bursts drop the count; the bar/percent carries the
        # running-vs-pending state on its own.
        # v1.13.44: clearer wording when nothing is running yet —
        # "Downloading themes — queued" reads as a contradiction
        # (downloading AND queued at once). Swap to a queue-leading
        # phrasing so the action label matches the actual state.
        # Once the worker picks up a job, the running phrasing
        # ("Downloading themes …") takes over.
        queued_label_map = {
            "DOWNLOAD QUEUE": "Theme download queued",
            "PLACE QUEUE":    "Place into Plex queued",
            "DISK SCAN":      "Disk scan queued",
            "REFRESH QUEUE":  "Plex refresh queued",
            "RELINK QUEUE":   "Re-link queued",
            "ADOPT QUEUE":    "Adopt queued",
        }
        queued_label = queued_label_map.get(kind_label, f"{stage_label} queued")
        if running_n + pending_n == 1:
            stage = stage_label if running_n else queued_label
        elif running_n and pending_n:
            stage = f"{stage_label} — {running_n} running, {pending_n} queued"
        elif running_n:
            stage = f"{stage_label} — {running_n} running"
        else:
            stage = f"{queued_label} ({pending_n})"
        out.append({
            "op_id": f"queue:{jt}",
            "kind": f"{jt}_queue",
            "status": "running" if running_n else "pending",
            "started_at": now,
            "updated_at": now,
            "finished_at": None,
            "stage": jt,
            "stage_label": stage,
            # v1.13.4: real progress via high-water mark. stage_total
            # is the burst's HW (max remaining we've seen since the
            # queue last drained); stage_current is what's completed
            # so far in this burst. A 5-job burst with 1 done shows
            # 1/5 = 20%, ticks to 5/5 as the worker drains.
            #
            # v1.13.18 (6C): downloads now carry yt-dlp's per-job
            # progress fraction via _DOWNLOAD_PROGRESS. Compute the
            # active-job's fraction and blend it into bar_pct so the
            # bar fills smoothly during a single yt-dlp call (the
            # original 0→100 jump complaint). Counter still reads as
            # integer "X / Y done"; bar_pct in detail is preferred
            # by the renderer when present.
            #
            # Other queue ops (place, refresh, scan, relink, adopt)
            # don't have intermediate progress, so they get the v1.13.4
            # HW-based bar — and a counter "0/1" → "1/1" so the user
            # sees something change instead of a static shimmer.
            "stage_current": completed_in_burst,
            "stage_total": hw,
            "processed_total": completed_in_burst,
            "processed_est": hw,
            "error_count": 0,
            "detail": _build_queue_detail(jt, completed_in_burst, hw, running_dl_jobs),
        })
    return out


def _build_queue_detail(job_type, completed_in_burst, hw, running_dl_jobs):
    """v1.13.18 (6C): assemble the detail block for a synthesized
    queue op. For downloads, blend yt-dlp's real per-job progress
    into a smooth bar_pct; for other queue types the bar uses
    stage_current/stage_total directly (no detail.bar_pct → renderer
    falls back to the integer ratio).
    """
    detail = {"activity": [], "throughput": [], "synthetic": True}
    if job_type == "download" and hw > 0:
        # active_progress = avg fraction across currently-running jobs
        # (typically just one in a single-worker setup, but safe with N).
        fractions = [
            _DOWNLOAD_PROGRESS.get(jid)
            for jid in running_dl_jobs
            if _DOWNLOAD_PROGRESS.get(jid) is not None
        ]
        if fractions:
            avg_frac = sum(fractions) / len(fractions)
            # Blend: completed jobs are full bars; active jobs get
            # avg_frac. Each running job contributes avg_frac of one
            # job's worth toward the burst total.
            running_count = len(fractions)
            blended = (completed_in_burst + avg_frac * running_count) / hw
            detail["bar_pct"] = max(0.0, min(1.0, blended))
    return detail


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
