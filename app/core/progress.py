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

import threading

from .db import get_conn, transaction
from .events import now_iso

log = logging.getLogger(__name__)

# Rolling histories live in detail_json so a worker restart loses no
# more than the last few seconds of throughput data. Sized for the UI
# sparkline (30 samples, ~30s at 1s polling) and activity feed.
_THROUGHPUT_HISTORY = 30
_ACTIVITY_HISTORY = 5

# v1.18.99: silent-fail audit close-out (class 9 + v1.17.11 hot-path
# sub-pattern). update_progress's throughput-sample block parses
# row["updated_at"] + now via fromisoformat to compute dt. Pre-fix
# a ValueError/AttributeError silently dropped the sample, making
# the sparkline jagged with no operator signal. update_progress is
# called per-tick during plex_enum / bulk_probe / etc., so per-row
# warning would drown logs — warn once then debug.
_THROUGHPUT_PARSE_WARNED: bool = False


def try_acquire(
    db_path: Path,
    op_id: str,
    kind: str,
) -> bool:
    """v1.15.37: atomic check-and-claim for singleton background ops.

    Returns True if the caller acquired the slot (no concurrent
    op with this op_id was already running/pending/cancelling),
    False otherwise. The acquired row lands in status='pending'
    so a subsequent `start_progress` from the worker thread
    transitions it to 'running' as normal.

    Pre-fix the route handlers used a non-atomic two-step
    pattern: `load_active(...)` to check + `threading.Thread(...)`
    to spawn the worker that calls `start_progress`. Two
    near-simultaneous requests could both pass the check before
    either thread reached `start_progress`; the second's
    `ON CONFLICT DO UPDATE` would then RESET the first's row,
    making the first thread's progress writes invisible. This
    helper closes the race by doing the check + the claim
    inside one BEGIN IMMEDIATE.

    The route handler should still 409 on `False`; the thread
    spawned on `True` is responsible for `start_progress` (which
    transitions pending → running) and the eventual
    `finish_progress`.
    """
    now = now_iso()
    detail = json.dumps({"activity": [], "throughput": []})
    with get_conn(db_path) as conn, transaction(conn):
        row = conn.execute(
            "SELECT status FROM op_progress WHERE op_id = ?",
            (op_id,),
        ).fetchone()
        if row is not None and row["status"] in (
                "running", "pending", "cancelling"):
            return False
        conn.execute(
            """INSERT INTO op_progress
                 (op_id, kind, status, started_at, updated_at,
                  stage, stage_label, stage_total, processed_est,
                  detail_json)
               VALUES (?, ?, 'pending', ?, ?, NULL, NULL, 0, 0, ?)
               ON CONFLICT(op_id) DO UPDATE SET
                   kind = excluded.kind,
                   status = 'pending',
                   started_at = excluded.started_at,
                   updated_at = excluded.updated_at,
                   finished_at = NULL,
                   stage = NULL,
                   stage_label = NULL,
                   stage_current = 0,
                   stage_total = 0,
                   processed_total = 0,
                   processed_est = 0,
                   error_count = 0,
                   detail_json = excluded.detail_json""",
            (op_id, kind, now, now, detail),
        )
    return True


def _secs_between(start_iso: str | None, end_iso: str | None) -> float | None:
    """v1.21.27: seconds between two ISO timestamps, or None if unparseable.
    Used to record per-stage durations for the drawer's RUN INSIGHT
    waterfall."""
    if not start_iso or not end_iso:
        return None
    from datetime import datetime
    try:
        a = datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
        b = datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
        return max((b - a).total_seconds(), 0.0)
    except (ValueError, AttributeError):
        return None


def _close_stage_timing(detail: dict, now: str) -> None:
    """v1.21.27: append the currently-tracked stage's elapsed time to
    detail['stage_timings'] and clear the in-flight tracker. No-op if no
    stage is being tracked. Centralised so start/update/finish stay in sync."""
    key = detail.get("_stage_key")
    started = detail.get("_stage_started")
    if not key or not started:
        return
    secs = _secs_between(started, now)
    if secs is None:
        return
    timings = detail.get("stage_timings", [])
    timings.append({
        "stage": key,
        "label": detail.get("_stage_label") or key,
        "seconds": round(secs, 1),
    })
    detail["stage_timings"] = timings


def _begin_stage_timing(detail: dict, stage: str,
                        stage_label: str | None, now: str) -> None:
    """v1.21.27: start tracking a new stage's clock. NOTE: these _stage_*
    keys are part of the live wire contract — they're sent in the op detail
    and read by ops.js (_renderWaterfall keys off _stage_key/_stage_started
    for the growing in-flight bar). finish_progress pops them so finished
    rows are clean. Don't strip them mid-run despite the _ prefix."""
    detail["_stage_started"] = now
    detail["_stage_key"] = stage
    detail["_stage_label"] = stage_label or stage


def start_progress(
    db_path: Path,
    op_id: str,
    kind: str,
    *,
    stage: str | None = None,
    stage_label: str | None = None,
    stage_total: int = 0,
    processed_est: int = 0,
    activity: str | None = None,
) -> None:
    """Insert (or reset) a progress row at op start.

    Resets any prior row for the same op_id — caller invariant is
    "only one of this op runs at a time", which holds for TDB sync
    (single-tenant) and Plex enum (worker serializes them).

    v1.23.32: `activity` seeds the first activity-feed line so the opening
    stage (the fetch/connect step, which has no countable % — stage_total=0,
    indeterminate shimmer) still SAYS what it's doing, instead of reading as a
    sparse %-less step before the first detailed one. the user's report.
    """
    now = now_iso()
    # v1.21.27: seed the stage-timing tracker so the first stage's clock
    # starts at op start (its duration lands in stage_timings on the next
    # transition / at finish), powering the RUN INSIGHT waterfall.
    detail_obj: dict = {"activity": [], "throughput": [], "stage_timings": []}
    if stage is not None:
        _begin_stage_timing(detail_obj, stage, stage_label, now)
    if activity:
        detail_obj["activity"] = [{"ts": now, "msg": activity}]
    detail = json.dumps(detail_obj)
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
        # v0.50.39: floor dt at 1.0s, not 0.001s. A fast batch (e.g. a 10k-row
        # upsert) can advance processed_total twice within a millisecond, and the
        # old 0.001s floor turned that into rate≈400000/s — which then polluted the
        # RUN INSIGHT peak/avg, the live items/sec pill, the ETA, AND the sparkline
        # (the user: "400000 peak/s · 207301 avg/s" on a 10,514-item/77s run that
        # really averaged ~136/s). Flooring at 1.0s caps a sub-second burst at its
        # delta (items-in-≈1s) instead of extrapolating it to an impossible rate, so
        # `rate` is a sane items/sec everywhere it's consumed. Genuine ≥1s gaps are
        # unaffected (dt is exact).
        if (processed_total is not None
                and processed_total > (row["processed_total"] or 0)):
            try:
                from datetime import datetime
                prev_ts = datetime.fromisoformat(
                    row["updated_at"].replace("Z", "+00:00"))
                now_dt = datetime.fromisoformat(now.replace("Z", "+00:00"))
                dt = max((now_dt - prev_ts).total_seconds(), 1.0)
                delta = processed_total - (row["processed_total"] or 0)
                throughput_buf.append({"ts": now,
                                       "rate": round(delta / dt, 3)})
            except (ValueError, AttributeError) as e:
                # v1.18.99: silent-fail audit close-out. Pre-fix
                # this swallow dropped one throughput sample with
                # no signal — sparkline got jagged + operator
                # couldn't tell why. Class-9 + v1.17.11 hot-path
                # combo: warn once then debug. Failure modes:
                # malformed updated_at in DB (corruption / manual
                # edit), or row["updated_at"] is None on a
                # pre-migration row.
                global _THROUGHPUT_PARSE_WARNED
                if not _THROUGHPUT_PARSE_WARNED:
                    log.warning(
                        "progress: throughput-sample timestamp "
                        "parse failed (%s: %s) on op_id=%s — "
                        "dropping this sample. Will not log "
                        "subsequent occurrences in this process; "
                        "check op_progress.updated_at for "
                        "malformed values.",
                        type(e).__name__, e, op_id,
                    )
                    _THROUGHPUT_PARSE_WARNED = True
                else:
                    log.debug(
                        "throughput parse skip (op_id=%s): %s",
                        op_id, e,
                    )
        detail["activity"] = list(activity_buf)
        detail["throughput"] = list(throughput_buf)
        # v1.21.27: stage-transition timing for the RUN INSIGHT waterfall.
        # When the incoming stage differs from the tracked one, close out the
        # previous stage's elapsed and start the new one's clock. A central
        # place → every multi-stage op gets a timing breakdown for free.
        if stage is not None:
            cur_key = detail.get("_stage_key")
            if cur_key is None:
                _begin_stage_timing(detail, stage, stage_label, now)
            elif stage != cur_key:
                _close_stage_timing(detail, now)
                _begin_stage_timing(detail, stage, stage_label, now)
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
        row = conn.execute(
            "SELECT detail_json FROM op_progress WHERE op_id = ?",
            (op_id,),
        ).fetchone()
        if row is None:
            return  # op was cleared; idempotent no-op
        try:
            detail = json.loads(row["detail_json"] or "{}")
        except (TypeError, ValueError):
            detail = {}
        # v1.21.27: close out the final in-flight stage's duration (RUN
        # INSIGHT waterfall), then drop the internal trackers so they don't
        # linger in the finished detail_json.
        _close_stage_timing(detail, now)
        detail.pop("_stage_started", None)
        detail.pop("_stage_key", None)
        detail.pop("_stage_label", None)
        if error_message:
            detail["error_message"] = error_message
        conn.execute(
            "UPDATE op_progress SET status = ?, finished_at = ?, "
            "updated_at = ?, detail_json = ? WHERE op_id = ?",
            (status, now, now, json.dumps(detail), op_id),
        )


def reset_stale_on_boot(db_path: Path) -> int:
    """v1.21.47: flip every non-terminal op_progress row to 'failed' at
    startup; returns the count reclaimed.

    op_progress rows drive the LIVE OPS panel. If a previous motif
    process died mid-op (deploy / SIGKILL / OOM / crash), the row
    survives as status in ('pending','running','cancelling') with no
    live thread to finish it. The panel then renders the phantom
    forever — ELAPSED grows as wall-clock since started_at while
    stage_current stays frozen — and neither // CANCEL nor a restart
    clears it, because no process owns the row. Worse, the stale row
    BLOCKS a fresh op of the same op_id: try_acquire returns False
    while a 'running'/'pending'/'cancelling' row exists. the user hit
    both when a v1.21.46 redeploy killed a tdb_sync at 96% — the
    phantom sat "CANCELLING" across a restart and couldn't be cleared
    or re-run from the UI.

    This is the missing third boot reconciliation: main.py's jobs
    zombie sweep + worker.py's sync_runs sweep already do the same for
    their tables. Called once at startup BEFORE any worker thread
    starts, so nothing can be legitimately running — the same safety
    argument as those two. finished_at is COALESCEd so a row that
    somehow already has one keeps it. Idempotent: a clean shutdown
    leaves no non-terminal rows, so this no-ops."""
    now = now_iso()
    with get_conn(db_path) as conn:
        cur = conn.execute(
            "UPDATE op_progress SET status = 'failed', "
            "  finished_at = COALESCE(finished_at, ?), "
            "  updated_at = ? "
            "WHERE status IN ('pending', 'running', 'cancelling')",
            (now, now),
        )
        return cur.rowcount


def sweep_stuck(db_path: Path, max_idle_minutes: int = 90) -> int:
    """v1.24.15 (holistic review): RUNTIME counterpart to reset_stale_on_boot.

    reset_stale_on_boot flips every non-terminal row at startup (safe — no
    worker is running yet). But a finish_progress() that RAISES mid-teardown
    (it has no `database is locked` retry ladder, unlike the worker's
    _safe_mark) leaves an op 'running'/'cancelling' with no live thread — and
    that 409-locks the op_id (try_acquire returns False) + shows a phantom in
    LIVE OPS until the next CONTAINER RESTART. jobs get _stuck_job_sweep every
    15 min; op_progress had only the boot reset.

    Unlike the boot reset this must NOT touch a legitimately-running op, so it
    only reaps rows whose `updated_at` is older than max_idle_minutes. progress()
    bumps updated_at on every step, so a live op is never stale; the threshold
    is generous (90 min) to clear even a long sync/enum/bulk op's idle headroom.
    Idempotent: a clean state has no stale non-terminal rows → no-op."""
    from datetime import datetime, timedelta, timezone
    cutoff = (datetime.now(timezone.utc)
              - timedelta(minutes=max_idle_minutes)).isoformat(timespec="seconds")
    now = now_iso()
    with get_conn(db_path) as conn:
        cur = conn.execute(
            "UPDATE op_progress SET status = 'failed', "
            "  finished_at = COALESCE(finished_at, ?), updated_at = ? "
            "WHERE status IN ('pending', 'running', 'cancelling') "
            "  AND updated_at < ?",
            (now, now, cutoff),
        )
        return cur.rowcount


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
        # v1.13.45: also pull the active title so the topbar mini-bar
        # can read "Downloading: <title>" instead of the generic
        # "Downloading themes — 1 running, 7 queued". Single-active is
        # the typical case (one worker thread); on multi-worker setups
        # we surface the most-recently-started for now.
        dl_rows = conn.execute(
            "SELECT j.id, t.title "
            "  FROM jobs j "
            "  LEFT JOIN themes t "
            "    ON t.media_type = j.media_type "
            "   AND t.tmdb_id = j.tmdb_id "
            " WHERE j.job_type = 'download' "
            "   AND j.status = 'running' "
            " ORDER BY j.id DESC"
        ).fetchall()
        running_dl_jobs = [row["id"] for row in dl_rows]
        running_dl_titles = [row["title"] for row in dl_rows
                             if row["title"]]
    out: list[dict] = []
    for row in list(active) + list(finished):
        d = dict(row)
        try:
            d["detail"] = json.loads(d.pop("detail_json") or "{}")
        except (TypeError, ValueError):
            d["detail"] = {}
        out.append(d)
    out.extend(_synthesize_queue_ops(
        queue_counts, running_dl_jobs,
        running_dl_titles=running_dl_titles,
    ))
    # v1.13.87: synthesize a card for plex_enum jobs that are
    # PENDING (queued behind a running one). plex_enum has real
    # op_progress rows for its RUNNING phase, so it's deliberately
    # excluded from _synthesize_queue_ops. But the pending phase
    # is invisible to the drawer otherwise — the user's repro:
    # click REFRESH on lib A, navigate to lib B and click REFRESH
    # again, the second job pendings behind the first and the
    # drawer shows nothing about it. The (X of Y) suffix in the
    # topbar mini-bar (v1.13.27) hints at it but truncates at
    # narrow widths. A dedicated card removes ambiguity:
    #
    #   Drawer:
    #     // PLEX REFRESH (running) — Reconciling placement paths
    #     // PLEX REFRESH (QUEUED) (pending) — N library refresh queued
    #
    # Render path: ops.js KIND_LABEL.plex_enum_pending →
    # 'PLEX REFRESH (QUEUED)', tone='plex' (same family as the
    # running card so they read as related).
    # v1.14.77: gate the synth so it only emits when there's a
    # REAL queue, not during the brief click → worker-pickup
    # window for a single job. Pre-fix the user saw the topbar
    # flash "1 LIBRARY REFRESH QUEUED" for ~1-2s after clicking
    # a single REFRESH — the worker has up to a 2s idle wait
    # before claiming the job, and during that gap pending=1
    # / running=0 fired this synth even though there's nothing
    # actually queued behind anything. The optimistic
    # placeholder ('// REFRESHING ...') already covers the
    # gap with the right framing; the synth's "queued"
    # framing is wrong when nothing's running yet. Real-queue
    # cases (pending behind a running, OR 2+ pending stacked)
    # still emit.
    with get_conn(db_path) as conn:
        pending_running_row = conn.execute(
            "SELECT "
            " SUM(CASE WHEN job_type='plex_enum' AND status='pending' "
            "          THEN 1 ELSE 0 END) AS pending_n, "
            " SUM(CASE WHEN job_type='plex_enum' AND status='running' "
            "          THEN 1 ELSE 0 END) AS running_n, "
            # v1.14.92: also pull sync / scan running counts so the
            # gate below can fire when a plex_enum is queued behind
            # any long-worker job (not just another plex_enum).
            " SUM(CASE WHEN job_type IN ('sync','scan') "
            "           AND status='running' THEN 1 ELSE 0 END) "
            "          AS other_long_running_n "
            "FROM jobs "
            "WHERE status IN ('pending','running')"
        ).fetchone()
    plex_enum_pending_n = pending_running_row["pending_n"] or 0
    plex_enum_running_n = pending_running_row["running_n"] or 0
    plex_enum_other_long_running_n = (
        pending_running_row["other_long_running_n"] or 0
    )
    # Real-queue conditions:
    #   - 2+ pending stacked (genuine queue depth)
    #   - 1+ pending AND 1+ running plex_enum (queued behind a sibling)
    #   - 1+ pending AND 1+ running sync / scan (v1.14.92: queued
    #     behind a different long-worker job — the user's repro:
    #     SYNC THEMERRDB running + click REFRESH on one library →
    #     1 plex_enum pending behind sync, no badge / no synth card
    #     because the gate only considered plex_enum-vs-plex_enum)
    is_real_queue = (
        plex_enum_pending_n >= 2
        or (
            plex_enum_pending_n >= 1
            and (
                plex_enum_running_n >= 1
                or plex_enum_other_long_running_n >= 1
            )
        )
    )
    if is_real_queue:
        n = plex_enum_pending_n
        # v1.15.2: unified queued-state label format
        # `<work> queued behind <blocker>`. Pre-fix this synth's
        # label was count-forward ("3 library refreshes queued")
        # while tdb_sync_pending's was passive-explanatory
        # ("Waiting for X to finish") — three different formats
        # for the "queued behind something" state across synth
        # rows. The new format puts the work first (what's
        # waiting) and the blocker second (why), uniform across
        # plex_enum_pending + tdb_sync_pending.
        plural = "" if n == 1 else "es"
        if plex_enum_running_n >= 1:
            blocker = "another Plex refresh"
        else:
            blocker = "the running sync / scan"
        stage = (
            f"{n} library refresh{plural} queued behind {blocker}"
        )
        synth_now = now_iso()
        out.append({
            "op_id": "queue:plex_enum_pending",
            "kind": "plex_enum_pending",
            "status": "pending",
            "started_at": synth_now,
            "updated_at": synth_now,
            "finished_at": None,
            "stage": "queued",
            "stage_label": stage,
            "stage_total": 0,
            "stage_current": 0,
            "processed_total": 0,
            "error_count": 0,
            # v1.14.84: structured pending count for the topbar
            # "+N QUEUED" badge. Pre-fix the only signal of
            # queue depth was the "(N of M)" suffix on the
            # mini-bar's label, which the user read as
            # "phase N of M for this section" (his mental
            # model: per-section work-phase progress) instead
            # of "this is job N of M in the queue." The badge
            # uses queue_depth to render as a separate pill —
            # plex-tone, sibling to the mini-bar — so the
            # signal can't be misread as section-internal.
            #
            # v1.15.2: detail.synthetic=true so the drawer's
            # cancel-button gate (ops.js:480) hides the dead
            # CANCEL button. Pre-fix the synth pending rows
            # rendered a CANCEL button that did nothing —
            # cancellation only flows through real op_progress
            # rows, which these are NOT. The user-facing
            # "// per-job cancel via /queue" note replaces it.
            "detail": {"queue_depth": n, "synthetic": True},
        })
    # v1.14.90: same shape as plex_enum_pending — synthesize a
    # card for SYNC jobs that are PENDING (queued behind another
    # long-worker job). Pre-fix the user reported clicking
    # SYNC THEMERRDB while a Plex refresh was running showed
    # nothing in the drawer — only the dash button's lock + the
    # "// SYNCING THEMERRDB…" label hinted at the click landing.
    # The sync job sits in the jobs table as 'pending' until the
    # long-worker (single-threaded for sync/plex_enum/scan)
    # frees up. Without a synth row the drawer's drawer-empty
    # message lies for the whole gap.
    #
    # Real-queue conditions (mirror plex_enum_pending):
    #   - sync pending while sync running (rare, but possible
    #     during a click-while-cron-cleanup edge)
    #   - sync pending while plex_enum / scan running (the
    #     canonical the user case)
    with get_conn(db_path) as conn:
        sync_pending_row = conn.execute(
            "SELECT "
            " SUM(CASE WHEN job_type='sync' AND status='pending' "
            "          THEN 1 ELSE 0 END) AS sync_pending_n, "
            " SUM(CASE WHEN job_type='sync' AND status='running' "
            "          THEN 1 ELSE 0 END) AS sync_running_n, "
            " SUM(CASE WHEN job_type IN ('plex_enum','scan') "
            "           AND status='running' THEN 1 ELSE 0 END) "
            "          AS other_long_running_n "
            "FROM jobs "
            "WHERE status IN ('pending','running')"
        ).fetchone()
    sync_pending_n = sync_pending_row["sync_pending_n"] or 0
    sync_running_n = sync_pending_row["sync_running_n"] or 0
    other_long_running_n = sync_pending_row["other_long_running_n"] or 0
    # Sync-queued only when there's actually something blocking
    # it. Pure pending (no other long worker running) means the
    # worker just hasn't picked it up yet (same brief window
    # plex_enum_pending guards against in v1.14.77).
    sync_is_queued = (
        sync_pending_n >= 1
        and (sync_running_n >= 1 or other_long_running_n >= 1)
    )
    if sync_is_queued:
        synth_now = now_iso()
        # v1.15.2: unified queued-state label format. See the
        # plex_enum_pending block above for the convention
        # rationale. The blocking job's kind shapes the blocker
        # phrase so the operator knows WHY it's queued.
        if sync_running_n >= 1:
            blocker = "another sync run"
        else:
            blocker = "the running Plex refresh / scan"
        stage = f"ThemerrDB sync queued behind {blocker}"
        out.append({
            "op_id": "queue:tdb_sync_pending",
            "kind": "tdb_sync_pending",
            "status": "pending",
            "started_at": synth_now,
            "updated_at": synth_now,
            "finished_at": None,
            "stage": "queued",
            "stage_label": stage,
            "stage_total": 0,
            "stage_current": 0,
            "processed_total": 0,
            "error_count": 0,
            # v1.15.2: detail.synthetic=true — same fix as the
            # plex_enum_pending block above. Hides the dead
            # CANCEL button on the drawer card.
            "detail": {
                "queue_depth": sync_pending_n,
                "synthetic": True,
            },
        })
    return out


_QUEUE_BURST_HW: dict[str, int] = {}

# v1.13.66: per-kind previous-tick remaining count. Used to detect
# mid-burst growth — if the user enqueues another batch while the
# current one is draining, `remaining` doesn't necessarily exceed
# `prior_hw` (e.g. HW=50, completed=30, then +7 arrive: remaining
# 20→27, both still below 50). Without delta tracking the bar froze
# at /50 even though the real total work was 57. Mirrors v1.13.29's
# client-side fix for plex_enum (`grew = max(0, current - prev)`).
_QUEUE_BURST_PREV_REMAINING: dict[str, int] = {}

# v1.13.47: per-kind first-seen timestamp for the active burst.
# Synthesized queue ops were emitting `started_at = now_iso()` on
# every poll — so the drawer card's ELAPSED counter reset to 0
# every second instead of climbing through the burst. Track when
# the kind first showed up in this drain cycle and reuse that
# value across subsequent synth rounds; cleared alongside
# _QUEUE_BURST_HW when the queue fully drains.
_QUEUE_BURST_START: dict[str, str] = {}

# v1.13.50: empty-tick counter per kind. Pre-fix one transient
# poll where the worker had just finished one job and not yet
# pulled the next (counts query returns no rows for that
# job_type) cleared both _QUEUE_BURST_HW[jt] and
# _QUEUE_BURST_START[jt], so the next poll stamped a fresh
# burst_start = now and ELAPSED visibly reset to 0s. Same risk
# on a transient `database is locked` retry that returns empty
# counts. Hold burst state across up to _BURST_GRACE_TICKS empty
# polls before clearing — at the 1s active poll cadence this is
# ~3 seconds of grace before we declare the burst drained.
_QUEUE_BURST_EMPTY_TICKS: dict[str, int] = {}
_BURST_GRACE_TICKS = 3

# v1.13.18 (6C): per-job download fraction (0.0-1.0). Updated by
# yt-dlp's progress_hooks (wired in worker.py); read by
# _synthesize_queue_ops to enrich the download_queue card with real
# percentage. Cleared on job finish/cancel so stale entries don't linger.
# v1.15.37: lock added. Pre-fix the read pattern in _synthesize_queue_ops
# was a list-comprehension with TWO `_DOWNLOAD_PROGRESS.get(jid)` calls
# per item (one for value, one for None-check). Between the two get()s
# a worker thread could pop() the entry → list-length-mismatch
# downstream → bad bar_pct math. The GIL protects individual dict ops
# but NOT the read-then-recheck pattern. Lock acquired in writers
# (set/clear) AND in the reader (snapshot helper below).
_DOWNLOAD_PROGRESS: dict[int, float] = {}
_DOWNLOAD_PROGRESS_LOCK = threading.Lock()


def set_download_progress(job_id: int, fraction: float) -> None:
    """v1.13.18: yt-dlp progress hook entrypoint. fraction in [0, 1]."""
    if 0.0 <= fraction <= 1.0:
        with _DOWNLOAD_PROGRESS_LOCK:
            _DOWNLOAD_PROGRESS[job_id] = fraction


def clear_download_progress(job_id: int) -> None:
    """v1.13.18: drop the job's progress entry on terminal state."""
    with _DOWNLOAD_PROGRESS_LOCK:
        _DOWNLOAD_PROGRESS.pop(job_id, None)


def snapshot_download_progress(job_ids: list[int]) -> dict[int, float]:
    """v1.15.37: atomic read of multiple job_ids' download fractions.

    Returns a dict {job_id: fraction} containing only the ids that
    have a recorded fraction at the moment of the lock. The reader
    can then iterate the result without worrying about a worker
    thread popping an entry mid-iteration.

    Mirrors the per-iteration pattern in _synthesize_queue_ops's
    list comprehension but does the lookups under the lock so the
    snapshot is consistent.
    """
    with _DOWNLOAD_PROGRESS_LOCK:
        return {
            jid: _DOWNLOAD_PROGRESS[jid]
            for jid in job_ids
            if jid in _DOWNLOAD_PROGRESS
        }


def sweep_download_progress(active_job_ids: list[int]) -> None:
    """v1.15.105: drop entries whose job_id is no longer a running
    download. Pre-fix the dict had no TTL — if a worker crashed
    outside the finally that calls clear_download_progress (or
    `_supervised` restart left an entry orphaned), the dict grew
    monotonically across the process lifetime. AUDIT_WORKER.md M7
    flagged this as a long-lived-process leak risk.

    Called once per /api/progress render (1Hz) from
    _synthesize_queue_ops, passing the authoritative running-jobs
    list. O(n) where n = dict size, which in practice never
    exceeds CONCURRENCY (typically 1-3).
    """
    active = set(active_job_ids)
    with _DOWNLOAD_PROGRESS_LOCK:
        stale = [jid for jid in _DOWNLOAD_PROGRESS if jid not in active]
        for jid in stale:
            _DOWNLOAD_PROGRESS.pop(jid, None)


def _synthesize_queue_ops(counts, running_dl_jobs=None, *,
                          running_dl_titles=None) -> list[dict]:
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
    # v1.15.105: sweep stale _DOWNLOAD_PROGRESS entries whose job
    # is no longer running. Defends against a worker that died
    # outside the finally that calls clear_download_progress (or
    # any other path that leaks an entry). AUDIT_WORKER.md M7.
    sweep_download_progress(running_dl_jobs)
    label_map = {
        "download": ("DOWNLOAD QUEUE", "Downloading themes"),
        "place":    ("PLACE QUEUE",    "Placing themes into Plex"),
        "scan":     ("DISK SCAN",      "Scanning canonical themes"),
        # v1.12.118: refresh = post-place Plex metadata nudges (~30s
        # delay) so Plex re-scans the folder and picks up the sidecar.
        # relink = repair stale placements (folder paths Plex moved).
        # adopt = bulk-adopt sidecars motif found via scan.
        # v1.13.80: kind label REFRESH QUEUE → RE-SCAN QUEUE so the
        # drawer reads the same verb (RE-SCAN) as the queued/running
        # stage labels and the v1.13.71 taxonomy decision (SYNC = TDB
        # pull, REFRESH = Plex section enumeration, RE-SCAN =
        # post-place per-folder nudge). Pre-fix the kind said
        # "REFRESH QUEUE" while the stage said "Plex re-scan queued"
        # — same op described with two different verbs in the same
        # drawer card. Job type stays 'refresh' (DB schema constraint,
        # internal name) — only the label changes.
        "refresh":  ("RE-SCAN QUEUE",  "Nudging Plex to re-scan"),
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
    # v1.13.50: empty-tick counter — a kind that was active last
    # poll but absent this poll might just be in the worker's
    # between-jobs gap (or a transient DB-lock that returned empty
    # counts). Hold burst state across up to _BURST_GRACE_TICKS
    # empty observations before clearing. Reset the counter as
    # soon as the kind reappears.
    for jt in list(_QUEUE_BURST_HW.keys()):
        if jt in seen_active:
            _QUEUE_BURST_EMPTY_TICKS.pop(jt, None)
            continue
        ticks = _QUEUE_BURST_EMPTY_TICKS.get(jt, 0) + 1
        if ticks >= _BURST_GRACE_TICKS:
            del _QUEUE_BURST_HW[jt]
            _QUEUE_BURST_EMPTY_TICKS.pop(jt, None)
            # v1.13.66: drop the prev-remaining map alongside HW so
            # the next burst computes its grown-by delta from a fresh 0.
            _QUEUE_BURST_PREV_REMAINING.pop(jt, None)
        else:
            _QUEUE_BURST_EMPTY_TICKS[jt] = ticks
    # v1.13.47: clear per-kind first-seen timestamps in lockstep
    # with the HW dict so a fresh burst gets a fresh start_at.
    # v1.13.50: respect the same grace — only drop the start
    # stamp once HW itself has been dropped (i.e. the kind has
    # been absent for _BURST_GRACE_TICKS in a row).
    for jt in list(_QUEUE_BURST_START.keys()):
        if jt not in _QUEUE_BURST_HW:
            del _QUEUE_BURST_START[jt]
    for row in counts:
        jt = row["job_type"]
        running_n = row["running_n"] or 0
        pending_n = row["pending_n"] or 0
        if running_n + pending_n == 0:
            continue
        remaining = running_n + pending_n
        # Update high-water if the queue has grown (new jobs landed
        # mid-burst). Never decreases until the queue fully drains.
        # v1.13.66: detect mid-burst growth via a remaining-count
        # delta instead of `remaining > prior_hw`. Pre-fix a second
        # bulk batch arriving while the first was draining (e.g.
        # HW=50 with 30 already done, then user clicks bulk-download
        # again for 7 more — remaining jumps 20→27 but stays below
        # 50, so HW didn't grow). Now `grew = max(0, remaining -
        # prev_remaining)` adds the late-arriving batch to HW so the
        # bar reaches 100% only when ALL enqueued work is done. Same
        # pattern as the v1.13.29 fix in app.js for plex_enum.
        prior_hw = _QUEUE_BURST_HW.get(jt, 0)
        prior_remaining = _QUEUE_BURST_PREV_REMAINING.get(jt, 0)
        grew = max(0, remaining - prior_remaining)
        hw = max(prior_hw + grew, remaining)
        _QUEUE_BURST_HW[jt] = hw
        _QUEUE_BURST_PREV_REMAINING[jt] = remaining
        # v1.13.47: stamp first-seen timestamp on first observation
        # of this kind in the current drain cycle. Subsequent polls
        # re-use the value so the drawer card's ELAPSED meta climbs
        # through the burst instead of resetting every second.
        burst_start = _QUEUE_BURST_START.get(jt)
        if burst_start is None:
            burst_start = now
            _QUEUE_BURST_START[jt] = burst_start
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
        # v1.13.71: REFRESH QUEUE label `Plex refresh queued` →
        # `Plex re-scan queued`. Disambiguates the post-place
        # metadata nudge from the user-triggered // REFRESH PLEX
        # action introduced in v1.13.70 (different scope, different
        # surface, different trigger). Three-verb taxonomy now:
        # SYNC (TDB pull) / REFRESH (Plex section enumeration) /
        # RE-SCAN (per-folder metadata nudge that fires after
        # every successful place).
        queued_label_map = {
            "DOWNLOAD QUEUE": "Theme download queued",
            "PLACE QUEUE":    "Place into Plex queued",
            "DISK SCAN":      "Disk scan queued",
            "RE-SCAN QUEUE":  "Plex re-scan queued",
            "RELINK QUEUE":   "Re-link queued",
            "ADOPT QUEUE":    "Adopt queued",
        }
        queued_label = queued_label_map.get(kind_label, f"{stage_label} queued")
        # v1.13.45: for downloads, surface the currently-running
        # theme's title in the running-state stage_label so the
        # topbar mini-bar reads "Downloading: <title>" without
        # forcing the user to click into the drawer.
        # v1.14.19: suffix changed from `+N more` to `+N running`
        # to disambiguate from the `(N queued)` suffix.
        # v1.14.21: list up to 2 concurrent titles inline so the
        # default download_concurrency=2 setup shows both names
        # rather than "+1 running" with the second name hidden.
        # the user's repro: card read
        # "Downloading: (500) Days of Summer +1 running (26 queued)"
        # — the +1 running was technically correct but the user
        # couldn't see WHICH second download was happening. Now:
        #   1 running:    "Downloading: (500) Days of Summer"
        #   2 running:    "Downloading: (500) Days of Summer, Avatar"
        #   3+ running:   "Downloading: A, B +N running"
        # Cap at 2 inline so the headline doesn't overflow on
        # higher-concurrency setups; the "+N running" suffix only
        # fires when listing inline would actually be too noisy.
        # running_dl_titles is sorted newest-first.
        running_label = stage_label
        if jt == "download" and running_n and running_dl_titles:
            titles_inline = running_dl_titles[:2]
            extras = running_n - len(titles_inline)
            joined = ", ".join(titles_inline)
            if extras > 0:
                running_label = f"Downloading: {joined} +{extras} running"
            else:
                running_label = f"Downloading: {joined}"
            # v1.14.71: append "(finalizing)" when the active job's
            # yt-dlp progress hit >=99% but the worker hasn't yet
            # flipped jobs.status to 'done'. yt-dlp emits 100% when
            # the network transfer completes, but post-processing
            # (mux, hash, sidecar write, place enqueue) can take
            # several more seconds — pre-fix the mini-bar sat at
            # "Downloading: X" 100% for that whole window with no
            # cue that anything was still happening, and the user
            # reported "long sitting at 100% download."
            # Only fires for single-download bursts (running_n == 1)
            # to avoid false positives in the concurrent-downloads
            # case where one job at 99% next to one at 30% would
            # otherwise mislabel the burst as "(finalizing)".
            if running_n == 1:
                try:
                    # v1.15.37: snapshot under lock — pre-fix the
                    # two `.get(jid)` calls per item could race the
                    # writer thread's `pop()` (one returns the
                    # value, the next returns None) producing a
                    # length-mismatch downstream.
                    snap = snapshot_download_progress(running_dl_jobs)
                    fractions = list(snap.values())
                    if fractions and min(fractions) >= 0.99:
                        running_label = f"{running_label} (finalizing)"
                except Exception as e:  # noqa: BLE001 — decorative, never load-bearing
                    # v1.18.99: silent-fail audit close-out. The
                    # (finalizing) suffix is genuinely cosmetic;
                    # the failure doesn't affect correctness — but
                    # a log.debug breadcrumb preserves the failure
                    # mode for debug-level operators investigating
                    # missing UI hints. No warn (would drown logs
                    # for a purely visual feature) + no flag (the
                    # path only fires when running_n == 1, not a
                    # hot loop). Per class-9 minimum: any except
                    # needs at least a breadcrumb.
                    log.debug(
                        "progress: (finalizing) label skipped — "
                        "snapshot_download_progress failed: %s", e,
                    )
        # v1.15.5: drop the inline "(N queued)" suffix from
        # download_queue when it has running + pending. The
        # +N QUEUED topbar badge (powered by detail.queue_depth)
        # surfaces the count separately for visual parity with
        # plex_enum_pending / tdb_sync_pending. The drawer's
        # download_queue card still has the count in the
        # counter (X / Y done) so no info is lost. Other queue
        # kinds (place/scan/refresh/relink/adopt) keep the
        # inline suffix since they don't get the badge surface.
        if running_n + pending_n == 1:
            stage = running_label if running_n else queued_label
        elif running_n and pending_n:
            if jt == "download":
                stage = running_label
            else:
                stage = f"{running_label} ({pending_n} queued)"
        elif running_n:
            stage = running_label if jt == "download" else f"{stage_label} — {running_n} running"
        else:
            stage = f"{queued_label} ({pending_n})"
        out.append({
            "op_id": f"queue:{jt}",
            "kind": f"{jt}_queue",
            "status": "running" if running_n else "pending",
            "started_at": burst_start,
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
            "detail": _build_queue_detail(
                jt, completed_in_burst, hw, running_dl_jobs,
                pending_n=pending_n, running_n=running_n,
            ),
        })
    return out


def _build_queue_detail(
    job_type, completed_in_burst, hw, running_dl_jobs,
    *,
    pending_n: int = 0,
    running_n: int = 0,
):
    """v1.13.18 (6C): assemble the detail block for a synthesized
    queue op. For downloads, blend yt-dlp's real per-job progress
    into a smooth bar_pct; for other queue types the bar uses
    stage_current/stage_total directly (no detail.bar_pct → renderer
    falls back to the integer ratio).

    v1.15.5: also expose `queue_depth` (= pending count) on the
    download queue when pending is queued behind running. The
    topbar's +N QUEUED badge reads this to render a separate
    visual signal for queued downloads — parity with how
    plex_enum_pending and tdb_sync_pending surface via the same
    badge. the user: "I would like to update the way we handle
    multiple downloads queued up similar to the way we show
    queued up refresh jobs or sync and refresh jobs."
    """
    detail = {"activity": [], "throughput": [], "synthetic": True}
    if job_type == "download" and hw > 0:
        # active_progress = avg fraction across currently-running jobs
        # (typically just one in a single-worker setup, but safe with N).
        # v1.15.37: snapshot under lock — same race fix as in
        # _synthesize_queue_ops above.
        fractions = list(snapshot_download_progress(running_dl_jobs).values())
        if fractions:
            avg_frac = sum(fractions) / len(fractions)
            # Blend: completed jobs are full bars; active jobs get
            # avg_frac. Each running job contributes avg_frac of one
            # job's worth toward the burst total.
            running_count = len(fractions)
            blended = (completed_in_burst + avg_frac * running_count) / hw
            detail["bar_pct"] = max(0.0, min(1.0, blended))
    # v1.15.5: queue_depth surfaces the queued (pending) count for
    # the topbar +N QUEUED badge. Only meaningful when something is
    # actually running with more queued behind — pure-pending
    # cases (running=0) are already conveyed by the mini-bar's
    # stage_label ("Theme download queued (N)") and don't need
    # the separate badge.
    if running_n > 0 and pending_n > 0:
        detail["queue_depth"] = pending_n
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
