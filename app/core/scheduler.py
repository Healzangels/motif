"""
Scheduler. Uses APScheduler's BackgroundScheduler to run:
- Daily ThemerrDB sync (cron, default 13:00 UTC)
- Hourly placement retry sweep (re-attempts placement for items whose
  Plex folders may have appeared since the last download)
"""
from __future__ import annotations

import logging
from pathlib import Path

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from ..config import Settings
from .db import get_conn, transaction
from .events import log_event, now_iso

log = logging.getLogger(__name__)


def _enqueue_sync(db_path: Path) -> None:
    # v1.22.33 (audit): BEGIN IMMEDIATE around the check-then-insert.
    # Pre-fix the SELECT-guard + INSERT ran in autocommit, so two enqueuers
    # racing in the same instant (cron tick vs a manual /api/sync/now, or two
    # scheduler ticks) could BOTH pass the "no existing pending/running sync"
    # check and BOTH insert → a duplicate sync job. The write lock serializes
    # them: the loser blocks at BEGIN IMMEDIATE, then its SELECT sees the row.
    with get_conn(db_path) as conn, transaction(conn):
        # Don't double-enqueue
        existing = conn.execute(
            "SELECT id FROM jobs WHERE job_type = 'sync' AND status IN ('pending','running')"
        ).fetchone()
        if existing:
            return
        # v1.21.19: stamp trigger='cron' so _do_sync can apply the
        # cron-only post-sync-enum sub-toggle (auto_enum_after_cron_sync).
        # The manual /api/sync/now path stamps trigger='manual'.
        conn.execute(
            """INSERT INTO jobs (job_type, payload, status, created_at, next_run_at)
               VALUES ('sync', '{"trigger": "cron"}', 'pending', ?, ?)""",
            (now_iso(), now_iso()),
        )
    log_event(db_path, level="INFO", component="scheduler",
              message="Enqueued daily ThemerrDB sync")


def _refresh_sections_job(settings: "Settings") -> None:
    """Re-discover Plex sections daily so newly-added libraries appear."""
    if not (settings.plex_enabled and settings.plex_url and settings.plex_token):
        return
    try:
        from .plex import PlexClient, PlexConfig
        from .sections import refresh_sections
        cfg = PlexConfig(
            url=settings.plex_url, token=settings.plex_token,
            movie_section=settings.plex_movie_section,
            tv_section=settings.plex_tv_section, enabled=True,
        )
        with PlexClient(cfg, plus_mode=settings.plus_equiv_mode) as plex:  # type: ignore[arg-type]
            refresh_sections(
                settings.db_path, plex,
                excluded_titles=settings.plex_excluded_titles,
                included_titles=settings.plex_included_titles,
            )
        log_event(settings.db_path, level="INFO", component="scheduler",
                  message="Refreshed Plex section cache")
    except Exception as e:
        log_event(settings.db_path, level="WARNING", component="scheduler",
                  message=f"Plex section refresh failed: {e}")


def _retry_pending_placements(db_path: Path) -> None:
    """Re-enqueue placement for downloaded themes that haven't been placed yet.

    This catches the case where a movie was downloaded but the corresponding
    Plex folder didn't exist at the time, then was added later (Radarr import,
    etc.).

    v1.13.76: skip rows whose last placement attempt failed for a reason
    that won't change without user action — `plex_has_theme` (Plex serves
    its own theme; motif won't overwrite without explicit force) and
    `existing_theme:*` (a sidecar already exists in the Plex folder; same).
    Pre-fix these rows got re-enqueued every hour for the lifetime of
    the install, the place worker silently DEBUG-logged the skip, and
    the user only saw `Retry sweep enqueued 1 placement jobs` followed
    by `Indexed /data/media/movies` with no follow-up `Placed in...`
    line — pure puzzle. `no_match` keeps retrying because it's
    legitimately transient (Radarr import lag — the folder appears later).
    v1.21.44: `placement_error:*` is now ALSO skipped — a real hardlink/
    copy I/O failure (disk full / EPERM / cross-FS) is marked job=failed
    + lights the FAIL dot (v1.21.41), so re-sweeping it hourly would
    create a fresh unacked failed job every hour. Surface once + stick;
    recovery is the operator fixing the fault + a manual/next-sync place.

    v1.18.94: also skip rows where the last_place_attempt_reason is
    `plex_rejected:*` AND the row has 2+ failed place jobs in the
    last 24h. Worker stamps `plex_rejected:HTTP_X` in
    `_do_place_collection` when Plex returns an error on upload
    (sub-20MB cases that v1.18.70's size-rejection permanent-fail
    doesn't catch). First failure stays auto-retryable (might be a
    transient Plex hiccup); a second within 24h locks the row out
    of the sweep until user intervention. the user's repro: an 11.7MB
    Fate/stay night theme that consistently HTTP 500'd from Plex
    re-cycled through ~7min amber UI every hour, forever, until
    manually cancelled.
    """
    # v1.24.13 (holistic review): one BEGIN IMMEDIATE around the dedup
    # SELECT + the INSERT loop. Pre-fix this ran in autocommit, so a concurrent
    # place-enqueue (manual PUSH / REPLACE / ACCEPT via the API, or the place
    # worker finishing a job) could land between the NOT-EXISTS dedup check and
    # the INSERT — both paths saw "no in-flight place job" and both enqueued a
    # duplicate place job (jobs has no UNIQUE constraint to catch it). Mirrors
    # _enqueue_sync (v1.22.33). Idempotent placement upsert so not corrupting,
    # but the dup means a second hardlink + a duplicate "theme placed" notify.
    with get_conn(db_path) as conn, transaction(conn):
        rows = conn.execute(
            """
            SELECT lf.media_type, lf.tmdb_id, lf.section_id, lf.edition_key
            FROM local_files lf
            LEFT JOIN placements p
              ON p.media_type = lf.media_type
             AND p.tmdb_id = lf.tmdb_id
             AND p.section_id = lf.section_id
             AND p.edition_key = lf.edition_key
            WHERE p.media_folder IS NULL
              AND NOT EXISTS (
                  SELECT 1 FROM jobs j
                  WHERE j.job_type = 'place' AND j.media_type = lf.media_type
                    AND j.tmdb_id = lf.tmdb_id
                    AND j.section_id = lf.section_id
                    -- v1.22.87: per-edition dedup (the v1.21.82 payload
                    -- convention) — pre-fix edition A's pending/stuck
                    -- place job deferred edition B's retry sweep-over-
                    -- sweep even though B's placement was the missing
                    -- one. Mirrors the edition-aware placements JOIN
                    -- above.
                    AND COALESCE(CASE WHEN json_valid(j.payload) THEN
                          json_extract(j.payload, '$.edition_key')
                        END, '') = lf.edition_key
                    AND j.status IN ('pending', 'running')
              )
              -- v1.13.76: don't re-enqueue rows whose last place
              -- attempt skipped for a permanent reason. User-driven
              -- actions (PURGE, REPLACE TDB, ACCEPT UPDATE) re-enqueue
              -- via the API directly, so this filter doesn't hide
              -- legitimate retries — it just stops the hourly
              -- silent-skip loop.
              -- v1.19.21: also skip 'backup_only' reason. The bulk
              -- DOWNLOAD TDB BACKUP action explicitly downloads with
              -- auto_place=false — user intent is "have a backup
              -- file ready, don't touch Plex". Pre-v1.19.21 the
              -- retry sweep saw local_files+no placement and
              -- enqueued a place job anyway, defeating the backup-
              -- only intent. Worker stamps 'backup_only' after a
              -- bulk_backup download so the sweep can recognize the
              -- intent and respect it. User can still PUSH TO PLEX
              -- manually to flip intent.
              -- v1.21.44 (review of v1.21.41/M1): also skip
              -- 'placement_error:*'. A real hardlink/copy I/O failure
              -- (disk full / EPERM / cross-FS) is now marked job=failed
              -- + lights the topbar FAIL dot. Re-sweeping it hourly would
              -- create a FRESH unacked failed job every hour (the FAIL
              -- dot re-lights after the user acks it, and failed jobs pile
              -- up unbounded). the user's call: surface it once + let it
              -- stick; recovery is the operator fixing the fault + a
              -- manual/next-sync re-place, not an hourly auto-retry churn.
              AND (lf.last_place_attempt_reason IS NULL
                   OR (lf.last_place_attempt_reason != 'plex_has_theme'
                       AND lf.last_place_attempt_reason != 'backup_only'
                       AND lf.last_place_attempt_reason NOT LIKE 'existing_theme:%'
                       AND lf.last_place_attempt_reason NOT LIKE 'placement_error:%'))
              -- v1.18.94: cap repeat-Plex-rejection re-sweeps. First
              -- failure passes (might be transient); 2+ failed place
              -- jobs in the last 24h on a row stamped plex_rejected:*
              -- means the same bytes keep getting rejected — stop
              -- burning amber UI every hour and wait for the user.
              AND NOT (
                  lf.last_place_attempt_reason LIKE 'plex_rejected:%'
                  AND (
                      SELECT COUNT(*) FROM jobs j2
                      WHERE j2.job_type = 'place'
                        AND j2.media_type = lf.media_type
                        AND j2.tmdb_id = lf.tmdb_id
                        AND j2.section_id = lf.section_id
                        AND j2.status = 'failed'
                        AND j2.finished_at IS NOT NULL
                        -- v1.19.5 (silent prod-bug from v1.18.94):
                        -- compare via julianday() not lex strings.
                        -- Workers write finished_at via now_iso()
                        -- which produces ISO-T format with +00:00
                        -- timezone suffix
                        -- (`2026-05-24T01:32:18+00:00`). SQLite's
                        -- `datetime('now', '-24 hours')` produces
                        -- space-separated format
                        -- (`2026-05-23 01:32:18`). String comparison
                        -- is lexicographic: T (0x54) > space (0x20),
                        -- so an ISO-T timestamp on the SAME date
                        -- as the boundary always compares "greater"
                        -- regardless of actual time. Result:
                        -- v1.18.94's lockout silently miscounted
                        -- failures depending on time-of-day.
                        -- julianday() converts both sides to a
                        -- numeric Julian day fraction — comparison
                        -- is then format-agnostic + correct.
                        AND julianday(j2.finished_at)
                            > julianday('now', '-24 hours')
                  ) >= 2
              )
            LIMIT 500
            """
        ).fetchall()
        for r in rows:
            # v1.12.81: include section_id when enqueueing the place job.
            # Pre-fix the worker rejected these with "place job missing
            # section_id (v1.11.0 requires per-section routing)" and the
            # job stuck in status='failed' counted toward the topbar's
            # red FAIL dot indefinitely. The dup-detection above + the
            # placements LEFT JOIN are likewise per-section so a
            # placement existing in section A no longer suppresses a
            # legitimate need-to-place in section B.
            # v1.21.56 (B3): carry the row's edition_key into the place job
            # so the worker re-places the SAME edition (not the standard
            # one). '' for standard rows = today's '{}'-equivalent payload.
            import json
            _retry_payload = json.dumps({"edition_key": r["edition_key"]})
            conn.execute(
                """INSERT INTO jobs (job_type, media_type, tmdb_id, section_id,
                                     payload, status, created_at, next_run_at)
                   VALUES ('place', ?, ?, ?, ?, 'pending', ?, ?)""",
                (r["media_type"], r["tmdb_id"], r["section_id"],
                 _retry_payload, now_iso(), now_iso()),
            )
        # v1.18.94: count rows the v1.18.94 gate locked out so the
        # operator sees WHY the sweep didn't enqueue them. Without
        # this, a row that hits the 2-failure threshold silently
        # vanishes from the queue and the user has no signal that
        # auto-retry is paused. Per CLAUDE.md class-9
        # (silent-defensive-catch) + the v1.18.5 cold-path-needs-
        # MORE-logging rule: defensive paths must log WHY they
        # fired. Single summary event per sweep run avoids per-row
        # hourly spam while still surfacing the lockout class.
        locked_rows = conn.execute(
            """
            SELECT lf.media_type, lf.tmdb_id, lf.section_id
            FROM local_files lf
            LEFT JOIN placements p
              ON p.media_type = lf.media_type
             AND p.tmdb_id = lf.tmdb_id
             AND p.section_id = lf.section_id
             AND p.edition_key = lf.edition_key
            WHERE p.media_folder IS NULL
              AND NOT EXISTS (
                  SELECT 1 FROM jobs j
                  WHERE j.job_type = 'place' AND j.media_type = lf.media_type
                    AND j.tmdb_id = lf.tmdb_id
                    AND j.section_id = lf.section_id
                    -- v1.22.87: per-edition dedup (the v1.21.82 payload
                    -- convention) — pre-fix edition A's pending/stuck
                    -- place job deferred edition B's retry sweep-over-
                    -- sweep even though B's placement was the missing
                    -- one. Mirrors the edition-aware placements JOIN
                    -- above.
                    AND COALESCE(CASE WHEN json_valid(j.payload) THEN
                          json_extract(j.payload, '$.edition_key')
                        END, '') = lf.edition_key
                    AND j.status IN ('pending', 'running')
              )
              AND lf.last_place_attempt_reason LIKE 'plex_rejected:%'
              AND (
                  SELECT COUNT(*) FROM jobs j2
                  WHERE j2.job_type = 'place'
                    AND j2.media_type = lf.media_type
                    AND j2.tmdb_id = lf.tmdb_id
                    AND j2.section_id = lf.section_id
                    AND j2.status = 'failed'
                    AND j2.finished_at IS NOT NULL
                    -- v1.20.20: julianday() to match the lockout GATE
                    -- at line ~166. This summary query (added in the
                    -- same v1.18.94 feature to REPORT which rows the
                    -- gate blocked) was never migrated off the
                    -- lexicographic compare the v1.19.5 fix corrected
                    -- in the gate. Left un-migrated, the two queries
                    -- disagree on the 24h boundary by time-of-day —
                    -- the operator-facing "N rows blocked" event names
                    -- rows the gate actually re-enqueued. The pair must
                    -- use identical comparison logic.
                    AND julianday(j2.finished_at)
                        > julianday('now', '-24 hours')
              ) >= 2
            LIMIT 500
            """
        ).fetchall()
    if rows:
        log_event(db_path, level="INFO", component="scheduler",
                  message=f"Retry sweep enqueued {len(rows)} placement jobs")
    # v1.18.94: emit the lockout summary OUTSIDE the conn-with block
    # so the events flusher's separate connection doesn't contend
    # with our writer. Always run when locked_rows > 0, even if the
    # main sweep enqueued 0 — operators care about the lockout
    # signal independently of normal sweep activity.
    n_locked = len(locked_rows)
    if n_locked:
        sample = ", ".join(
            f"({r['media_type']},{r['tmdb_id']})"
            for r in locked_rows[:5]
        )
        more = f" + {n_locked - 5} more" if n_locked > 5 else ""
        log_event(
            db_path, level="INFO", component="scheduler",
            message=(
                f"Retry sweep: {n_locked} row(s) blocked from "
                f"auto-retry — repeated Plex upload rejection in "
                f"last 24h. Sample: {sample}{more}. Manual "
                f"REPLACE TDB / PURGE re-enqueues; otherwise the "
                f"gate clears 24h after the most recent failure."
            ),
        )


def _restore_lost_placements(settings: "Settings") -> None:
    """v1.24.26: auto-re-place a motif-owned sidecar theme whose theme.mp3 is
    VERIFIED gone from its Plex folder (placements.theme_present=0, stamped by
    verify_placement_health) when the canonical still exists in /config/themes.

    The common cause is a Plex delete+re-add (new rating_key): the folder's
    theme.mp3 vanishes but motif's canonical survives, and the v1.18.90 reaper
    silently skips the loss (the surviving placement/canonical route it to the
    'other_fallback' tier). This sweep restores it from backup automatically +
    fires theme_auto_restored. Gating mirrors _retry_pending_placements so it
    never fights an intentional removal: a LET PLEX SERVE / UNPLACE deletes the
    placement row (so it can't reach this query); backup_only / plex_has_theme /
    existing_theme / placement_error reasons are skipped; the placement folder
    must be a LIVE Plex item's folder (so we never place into a stale/wrong dir
    or create a divergent row before reconcile moves it); and each candidate is
    RE-STAT'd here so a stamp gone stale since the last enum (or a mount blip)
    can't cause a spurious re-place.

    v1.24.29: also covers stale plex_upload placements — the same delete+re-add
    that wipes a sidecar destroys an uploaded theme's rating_key, leaving it
    served by nothing (v1.24.28 stamps theme_present=0). Those re-PUSH to the new
    live rk (gated on a re-verified-dead old rk + a live bonded plex_items row to
    upload to) through the same place-job + notification path."""
    import json
    db_path = settings.db_path
    to_place: list = []
    # v1.24.32: per-the user toggles — gate the file-based vs API re-upload
    # branches independently. getattr-default True so a config without the
    # fields (older yaml) keeps the v1.24.26/.29 always-on behavior.
    _pl = getattr(settings.cfg, "placement", None)
    do_sidecar = getattr(_pl, "auto_restore_sidecar", True)
    do_upload = getattr(_pl, "auto_restore_plex_upload", True)
    if not (do_sidecar or do_upload):
        return
    try:
        with get_conn(db_path) as conn:
            cands = conn.execute(
                """
                SELECT p.media_type, p.tmdb_id, p.section_id, p.edition_key,
                       p.media_folder, p.placement_kind,
                       COALESCE(t.title, '') AS title,
                       COALESCE(t.year, '')  AS year
                FROM placements p
                JOIN local_files lf
                  ON lf.media_type = p.media_type AND lf.tmdb_id = p.tmdb_id
                 AND lf.section_id = p.section_id
                 AND lf.edition_key = p.edition_key
                LEFT JOIN themes t
                  ON t.media_type = p.media_type AND t.tmdb_id = p.tmdb_id
                WHERE p.theme_present = 0
                  AND p.placement_kind != 'plex_upload'
                  AND p.media_folder IS NOT NULL AND p.media_folder != ''
                  AND (lf.last_place_attempt_reason IS NULL
                       OR (lf.last_place_attempt_reason != 'plex_has_theme'
                           AND lf.last_place_attempt_reason != 'backup_only'
                           AND lf.last_place_attempt_reason
                               NOT LIKE 'existing_theme:%'
                           AND lf.last_place_attempt_reason
                               NOT LIKE 'placement_error:%'))
                  -- v1.24.35 (review #2): never auto-restore when motif's own
                  -- canonical is CONFIRMED gone (canonical_present=0). The place
                  -- would raise "source file missing" WITHOUT stamping a skip
                  -- reason, so the NULL reason keeps satisfying this gate and the
                  -- sweep re-enqueues a doomed job + fires a false "restored"
                  -- notification every hour. NULL (unverified) stays eligible.
                  AND COALESCE(lf.canonical_present, 1) != 0
                  AND EXISTS (
                      SELECT 1 FROM plex_items pi
                      WHERE pi.folder_path = p.media_folder
                        AND pi.section_id = p.section_id
                        AND (CASE pi.media_type WHEN 'show' THEN 'tv'
                                 ELSE pi.media_type END) = p.media_type
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM jobs j
                      WHERE j.job_type = 'place' AND j.media_type = p.media_type
                        AND j.tmdb_id = p.tmdb_id AND j.section_id = p.section_id
                        AND COALESCE(CASE WHEN json_valid(j.payload) THEN
                              json_extract(j.payload, '$.edition_key') END,
                            '') = p.edition_key
                        AND j.status IN ('pending', 'running')
                  )
                LIMIT 200
                """
            ).fetchall() if do_sidecar else []
            # v1.24.29: stale plex_uploads — the plex_upload twin of the sidecar
            # case. verify_placement_health (v1.24.28) stamps theme_present=0
            # when the uploaded rating_key was destroyed by a Plex delete+re-add.
            # There's no on-disk sidecar to stat, so the SQL gate IS the
            # authority: the placement's plex_rating_key is genuinely dead
            # (re-verified here, not just trusting the stamp) AND a LIVE
            # plex_items row is now bonded to the theme (the v1.24.27 re-link ran,
            # giving the worker a current rk to re-upload to). Same skip-reason +
            # in-flight dedup gating as the sidecar query.
            pu_cands = conn.execute(
                """
                SELECT p.media_type, p.tmdb_id, p.section_id, p.edition_key,
                       p.media_folder, p.placement_kind,
                       COALESCE(t.title, '') AS title,
                       COALESCE(t.year, '')  AS year
                FROM placements p
                JOIN local_files lf
                  ON lf.media_type = p.media_type AND lf.tmdb_id = p.tmdb_id
                 AND lf.section_id = p.section_id
                 AND lf.edition_key = p.edition_key
                JOIN themes t
                  ON t.media_type = p.media_type AND t.tmdb_id = p.tmdb_id
                WHERE p.placement_kind = 'plex_upload'
                  AND p.plex_rating_key IS NOT NULL
                  AND p.theme_present = 0
                  AND (lf.last_place_attempt_reason IS NULL
                       OR (lf.last_place_attempt_reason != 'plex_has_theme'
                           AND lf.last_place_attempt_reason != 'backup_only'
                           AND lf.last_place_attempt_reason
                               NOT LIKE 'existing_theme:%'
                           AND lf.last_place_attempt_reason
                               NOT LIKE 'placement_error:%'))
                  -- v1.24.35 (review #2): never auto-restore when motif's own
                  -- canonical is CONFIRMED gone (canonical_present=0). The place
                  -- would raise "source file missing" WITHOUT stamping a skip
                  -- reason, so the NULL reason keeps satisfying this gate and the
                  -- sweep re-enqueues a doomed job + fires a false "restored"
                  -- notification every hour. NULL (unverified) stays eligible.
                  AND COALESCE(lf.canonical_present, 1) != 0
                  AND NOT EXISTS (
                      SELECT 1 FROM plex_items pi
                      WHERE pi.rating_key = p.plex_rating_key
                  )
                  AND EXISTS (
                      SELECT 1 FROM plex_items pi
                      WHERE pi.theme_id = t.id
                        AND pi.section_id = p.section_id
                        AND pi.edition_key = p.edition_key
                        AND (CASE pi.media_type WHEN 'show' THEN 'tv'
                                 ELSE pi.media_type END) = p.media_type
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM jobs j
                      WHERE j.job_type = 'place' AND j.media_type = p.media_type
                        AND j.tmdb_id = p.tmdb_id AND j.section_id = p.section_id
                        AND COALESCE(CASE WHEN json_valid(j.payload) THEN
                              json_extract(j.payload, '$.edition_key') END,
                            '') = p.edition_key
                        AND j.status IN ('pending', 'running')
                  )
                LIMIT 200
                """
            ).fetchall() if do_upload else []
        # Authoritative re-stat: act only on sidecars still genuinely missing.
        # A present file = a stamp stale since the last enum (skip, the next
        # enum re-stamps theme_present=1); an OSError = indeterminate mount blip
        # (skip — never auto-act on an uncertain read).
        for r in cands:
            try:
                if (Path(r["media_folder"]) / "theme.mp3").is_file():
                    continue
            except OSError:
                continue
            to_place.append(r)
        # v1.24.29: plex_uploads have no on-disk sidecar — the SQL rk-liveness
        # gate above is their authority, so accept them directly.
        to_place.extend(pu_cands)
        enqueued: list = []
        if to_place:
            with get_conn(db_path) as conn, transaction(conn):
                for r in to_place:
                    # v0.50.89 (audit MEDIUM): re-check the in-flight-job dedup
                    # HERE, inside the same transaction as the INSERT below —
                    # pre-fix the only check was the SELECT above, run on a
                    # separate connection/transaction with a filesystem stat
                    # loop in between. A manual PUSH/REPLACE/ACCEPT landing a
                    # competing place job for this row in that window was
                    # invisible to this sweep, which then inserted a
                    # duplicate (jobs has no UNIQUE constraint to catch it —
                    # same TOCTOU class already closed for _enqueue_sync
                    # v1.22.33 and _retry_pending_placements v1.24.13). The
                    # filesystem re-stat above still happens OUTSIDE this
                    # transaction, by design (never hold a write lock during
                    # I/O); only the dedup check needed to move.
                    still_clear = conn.execute(
                        """SELECT 1 FROM jobs
                            WHERE job_type = 'place' AND media_type = ?
                              AND tmdb_id = ? AND section_id = ?
                              AND COALESCE(CASE WHEN json_valid(payload) THEN
                                    json_extract(payload, '$.edition_key') END,
                                  '') = ?
                              AND status IN ('pending', 'running')""",
                        (r["media_type"], r["tmdb_id"], r["section_id"],
                         r["edition_key"]),
                    ).fetchone()
                    if still_clear:
                        continue
                    conn.execute(
                        """INSERT INTO jobs (job_type, media_type, tmdb_id,
                                             section_id, payload, status,
                                             created_at, next_run_at)
                           VALUES ('place', ?, ?, ?, ?, 'pending', ?, ?)""",
                        (r["media_type"], r["tmdb_id"], r["section_id"],
                         # v1.24.35 (review #1): carry the placement's kind so a
                         # stale plex_upload re-PUSHES via the API instead of
                         # falling to the global default_method — which (default
                         # 'file') would silently re-deploy it as a sidecar, NOT
                         # a re-upload. A sidecar row re-hardlinks ('file'), so it
                         # doesn't upload when default_method='api' either.
                         # v1.24.38 (review #6): reason='auto_restore' tags the
                         # job so the worker fires theme_auto_restored on the
                         # place ACTUALLY landing (see _do_place) — replacing the
                         # premature enqueue-time dispatch that used to claim
                         # "restored" before the job ran.
                         json.dumps({"edition_key": r["edition_key"],
                                     "kind": ("api" if r["placement_kind"]
                                              == "plex_upload" else "file"),
                                     "reason": "auto_restore"}),
                         now_iso(), now_iso()),
                    )
                    enqueued.append(r)
    except Exception as e:
        log.warning("restore-lost-placements sweep failed: %s", e)
        return
    if not enqueued:
        return
    titles = []
    for r in enqueued:
        lbl = r["title"] or f"{r['media_type']} {r['tmdb_id']}"
        if r["year"]:
            lbl += f" ({r['year']})"
        titles.append(lbl)
    n = len(titles)
    head = ", ".join(titles[:8]) + (f" + {n - 8} more" if n > 8 else "")
    # This logs the ENQUEUE (re-deploy queued), which is accurate as an event.
    log_event(db_path, level="WARNING", component="scheduler",
              message=f"Auto-restore: re-deploying {n} theme(s) whose Plex "
                      f"copy went missing (sidecar deleted or uploaded "
                      f"rating_key destroyed by a re-add): {head}")
    # v1.24.38 (review #6): the theme_auto_restored notification USED to fire
    # here — at enqueue, before the place jobs ran — so a place that later
    # failed (e.g. the canonical was gone, an upload 500'd) still claimed
    # success. It now fires from the worker (_do_place / _do_place_collection)
    # on the place ACTUALLY landing, tagged via the payload reason='auto_restore'
    # and coalesced so a re-add burst still collapses to one summary.


_RELEASE_API = "https://api.github.com/repos/Healzangels/motif/releases/latest"
_RELEASE_CACHE_FILENAME = "release.json"


# v1.13.11: per-job-type "this should never run longer than" thresholds
# for the runtime stuck-job sweep. Conservative — set to a multiple of
# the worst observed duration so a slow run never gets euthanized
# mid-flight. Anything not in the map uses _STUCK_JOB_DEFAULT_MIN.
_STUCK_JOB_THRESHOLDS_MIN = {
    "sync":      120,
    "plex_enum": 120,
    "scan":       60,
    "download":   30,
    "place":      10,
    "refresh":    10,
    "relink":     10,
    "adopt":      15,
}
_STUCK_JOB_DEFAULT_MIN = 60

# v1.24.20: fast-reclaim for the long-threshold singleton-op job types
# (sync→tdb_sync, plex_enum→plex_enum). When the parent op_progress row is
# TERMINAL or MISSING but the JOB row is still 'running', the job is orphaned —
# reclaim it in minutes instead of waiting out the 120min runtime backstop.
# Pre-fix a hung sync pinned themerrdb_sync_in_flight>0 for up to 2h, wedging
# the dashboard SYNC button disabled with "no ops running" (the user's repro:
# _sweep_stuck_op_progress had already driven the op terminal / pruned it).
# v1.24.23: the signal is the op's TERMINAL/MISSING state, NOT a stale heartbeat
# — a healthy slow phase (e.g. a >15min first-run git clone, one blocking
# dulwich call) emits no heartbeat, so a stale-but-running op must NOT be killed.
# This constant is the GRACE before the op-state check applies, which also
# covers the brief job-running → start_progress startup window.
_HEARTBEAT_STALE_MIN = 15
_JOB_OPID = {"sync": "tdb_sync", "plex_enum": "plex_enum"}


def _stuck_job_sweep(db_path: Path) -> None:
    """v1.13.11: scan jobs.status='running' for rows whose started_at is
    older than the per-job-type threshold and reset them to 'failed'
    with a clear last_error. This is the runtime counterpart to the
    boot-time zombie sweep in main.py (which only fires on process
    restart) — a worker that hangs in C-extension land but doesn't
    die would otherwise leave a permanent ghost row + spinning UI.

    Threshold map is conservative; sync/plex_enum get 120 minutes,
    place gets 10 minutes. Anything not in the map uses 60 minutes.

    Idempotent: an already-failed row is a no-op. Safe to run on a
    cadence (default: every 15 minutes via the scheduler).
    """
    from datetime import datetime, timedelta, timezone
    now = datetime.now(timezone.utc)
    cutoffs = {
        jt: (now - timedelta(minutes=mins)).isoformat(timespec="seconds")
        for jt, mins in _STUCK_JOB_THRESHOLDS_MIN.items()
    }
    default_cutoff = (now - timedelta(minutes=_STUCK_JOB_DEFAULT_MIN)) \
        .isoformat(timespec="seconds")
    try:
        with get_conn(db_path) as conn:
            stuck = conn.execute(
                "SELECT id, job_type, started_at FROM jobs "
                "WHERE status = 'running' AND started_at IS NOT NULL"
            ).fetchall()
            # v1.24.20: op_progress state for the singleton-op job types
            # (sync→tdb_sync, plex_enum→plex_enum). v1.24.23: a job is orphaned
            # only when its op is TERMINAL or MISSING (a still-running op — even
            # one quiet through a long blocking phase — is left alone). hb_cut is
            # the grace: the op-state check applies only once the JOB has run past
            # _HEARTBEAT_STALE_MIN.
            hb_cut = (now - timedelta(minutes=_HEARTBEAT_STALE_MIN)) \
                .isoformat(timespec="seconds")
            op_by_id = {
                r["op_id"]: r for r in conn.execute(
                    "SELECT op_id, status FROM op_progress "
                    "WHERE op_id IN ('tdb_sync', 'plex_enum')"
                ).fetchall()
            }
            killed = 0
            for row in stuck:
                jt = row["job_type"]
                started = row["started_at"] or ""
                cutoff = cutoffs.get(jt, default_cutoff)
                over_threshold = started < cutoff
                # v1.24.20: fast-reclaim a sync/plex_enum job whose op_progress is
                # TERMINAL or MISSING — it's orphaned (the user's hung-sync repro
                # pinned themerrdb_sync_in_flight>0 for up to 2h, wedging the
                # dashboard SYNC button).
                # v1.24.23: do NOT reclaim a still-RUNNING op even if its heartbeat
                # is stale. A healthy slow phase emits NO heartbeat (a >15min
                # first-run git clone is a single blocking dulwich call), so a
                # stale-running op would be a FALSE kill mid-clone. A truly-hung op
                # is driven terminal by _sweep_stuck_op_progress (then caught here);
                # the 120min runtime backstop covers anything that never went
                # terminal. The grace on started_at also covers the job-running →
                # start_progress window (op may briefly show the prior run's row).
                hb_dead = False
                op_id = _JOB_OPID.get(jt)
                if op_id and started < hb_cut:
                    op = op_by_id.get(op_id)
                    op_live = (op is not None
                               and op["status"] not in (
                                   "done", "failed", "cancelled"))
                    hb_dead = not op_live
                if over_threshold or hb_dead:
                    threshold = _STUCK_JOB_THRESHOLDS_MIN.get(
                        jt, _STUCK_JOB_DEFAULT_MIN)
                    # v1.22.44 (audit): the sweep sets status='failed'
                    # TERMINALLY (it does NOT route through _mark_failed's
                    # attempts/backoff ladder), so the prior "will retry per
                    # backoff schedule" was inaccurate — a hung worker can't be
                    # safely resumed mid-flight. Recovery is the next sync
                    # re-detecting the unthemed row (auto-download) or a manual
                    # re-queue. If the worker actually finished slowly, its
                    # local_files write already landed; only the FAIL dot is
                    # cosmetic (its _mark_done no-ops against this 'failed' row).
                    msg = (
                        f"stuck_job_sweep: {jt} job "
                        + ("op_progress terminal/gone while job still "
                           "running — orphaned worker"
                           if hb_dead and not over_threshold
                           else f"exceeded {threshold}min runtime — "
                                "worker likely hung")
                        + ". Reset to failed (terminal); re-queue via "
                        "next sync / manual action if still needed."
                    )
                    # v1.13.15: re-check status='running' inside the
                    # UPDATE so a worker that finished naturally between
                    # our SELECT (line above) and this UPDATE doesn't
                    # get its legitimate 'done'/'cancelled' overwritten
                    # to 'failed'. cur.rowcount tells us whether we
                    # actually killed something or the worker beat us.
                    cur = conn.execute(
                        # v1.23.99 (audit): overwrite last_error, don't COALESCE.
                        # A re-claimed job carries last_error from its PRIOR
                        # (transient) failure — the re-claim doesn't clear it — so
                        # COALESCE(last_error, ?) kept that stale error and MASKED
                        # the accurate "worker likely hung" message for this
                        # terminal sweep. The sweep only hits status='running'
                        # jobs that blew the runtime threshold, so "hung" is the
                        # correct terminal reason regardless of a prior attempt.
                        "UPDATE jobs SET status = 'failed', "
                        "                finished_at = ?, "
                        "                last_error = ? "
                        "WHERE id = ? AND status = 'running'",
                        (now_iso(), msg, row["id"]),
                    )
                    if cur.rowcount:
                        killed += 1
        if killed:
            log_event(db_path, level="WARNING", component="scheduler",
                      message=f"Stuck-job sweep reset {killed} runaway job(s)")
    except Exception as e:
        log.warning("Stuck-job sweep failed: %s", e)


def _prune_op_progress(db_path: Path) -> None:
    """v1.13.54: drop op_progress rows older than 24h.

    Pre-fix this DELETE ran on every /api/progress poll (1Hz active),
    even when nothing was old enough to drop — needless writer-lock
    contention against any real worker activity. The 24h TTL is loose
    enough that a 30-minute sweep cadence is fine.
    """
    from . import progress as op_progress
    try:
        purged = op_progress.prune_finished(db_path)
    except Exception as e:
        log.warning("op_progress prune failed: %s", e)
        return
    if purged:
        log.debug("op_progress prune dropped %d row(s)", purged)


def _sweep_stuck_op_progress(db_path: Path) -> None:
    """v1.24.15 (holistic review): runtime reaper for op_progress rows that a
    dead/raised finish_progress left non-terminal. Mirrors _stuck_job_sweep (for
    jobs) — op_progress previously had only a boot-time reset, so a stuck op
    409-locked its op_id + showed a LIVE OPS phantom until container restart.
    Only reaps rows idle past progress.sweep_stuck's generous threshold."""
    from . import progress as op_progress
    try:
        killed = op_progress.sweep_stuck(db_path)
    except Exception as e:
        log.warning("op_progress stuck-sweep failed: %s", e)
        return
    if killed:
        log.warning("op_progress stuck-sweep reclaimed %d stale op(s)", killed)


def _prune_tvdb_lookup_cache(db_path: Path) -> None:
    """v1.17.9: daily sweep of expired tvdb_lookup_cache rows.

    The cache is used by both TMDB and TVDB clients via
    `INSERT OR REPLACE` with an `expires_at` column. Read paths
    already skip expired rows — but pre-v1.17.9 nothing ever
    actually deleted them. Every distinct (kind, title, year)
    miss added a new row that lived forever once expired,
    creating a slow monotonic growth pattern caught by the
    DB-hygiene audit.

    Cheap one-shot DELETE keyed on expires_at; no-op when nothing
    is expired. Logged at INFO when rows are actually purged so
    the operator can see the table staying bounded.
    """
    try:
        with get_conn(db_path) as conn:
            cur = conn.execute(
                "DELETE FROM tvdb_lookup_cache "
                "WHERE expires_at < datetime('now')"
            )
            n = cur.rowcount or 0
    except Exception as e:
        log.warning("tvdb_lookup_cache prune failed: %s", e)
        return
    if n:
        log_event(db_path, level="INFO", component="scheduler",
                  message=f"Pruned {n} expired tvdb_lookup_cache row(s)")


def _prune_events(db_path: Path) -> None:
    """v1.17.9: daily sweep of `events` rows older than 30 days.

    The events table is the operator-facing rotating log (Recent
    Events panel + /events page). Code comments in db.py and
    api.py call it "rotating" but pre-v1.17.9 nothing actually
    rotated it — the only DELETE was the user-driven per-row
    CLEAR. After 109 `log_event` callsites and years of activity
    the table can grow into the hundreds of thousands of rows on
    a busy install. audit_events is the long-lived parallel log
    (untouched).

    30-day retention matches the cap on the /events `since` query
    param (api.py:14735) — the UI cannot meaningfully surface
    older rows.
    """
    try:
        with get_conn(db_path) as conn:
            cur = conn.execute(
                "DELETE FROM events WHERE ts < datetime('now', '-30 days')"
            )
            n = cur.rowcount or 0
    except Exception as e:
        log.warning("events prune failed: %s", e)
        return
    if n:
        log_event(db_path, level="INFO", component="scheduler",
                  message=f"Pruned {n} events row(s) older than 30 days")


def _prune_notifications(db_path: Path) -> None:
    """v0.51.152: daily sweep of the in-app `notifications` inbox. The topbar
    drawer only ever shows UNDISMISSED rows, so a dismissed row is pure DB weight
    — pruned 7 days after dismissal; any row (even undismissed) rotates out after
    30 days so an operator who never opens the inbox can't grow it unbounded
    (mirrors the events 30-day cap). Delegates to notify_inbox, which owns the
    table."""
    try:
        from .notify_inbox import prune_notifications
        n = prune_notifications(db_path)
    except Exception as e:
        log.warning("notifications prune failed: %s", e)
        return
    if n:
        log_event(db_path, level="INFO", component="scheduler",
                  message=f"Pruned {n} notification inbox row(s)")


def _prune_history(db_path: Path) -> None:
    """v1.17.11: unified retention prune for the five append-forever
    history tables flagged in the v1.17.9 audit (Tier B).

    Per-table windows are tuned to keep recently-relevant rows for
    UI surfaces (dashboard sparkline, ops drawer) while bounding
    growth:

      jobs                  done/failed/cancelled  > 30 days
      sync_runs             success/failed         > 90 days
      scan_runs             complete/failed/cxl    > 90 days
      scan_findings         (cascades from scan_runs ON DELETE CASCADE)
      local_files_history   saved_at               > 180 days

    All deletes run in one transaction so a single counter
    summary lands per sweep. Idempotent + cheap when nothing
    matches the window.

    Why one function not five jobs:
    - Single writer lock per sweep instead of five.
    - One summary log per night, not five.
    - The retention strategy is one cohesive policy — keeping
      them together makes it obvious what gets retained how long.

    The 90d window for sync_runs / scan_runs is calibrated to
    cover the user's dashboard sparkline horizon (current default
    is "last 90 days of syncs") with a couple days of buffer.
    If that horizon ever grows, bump this window in lock-step
    so the sparkline doesn't suddenly truncate after a prune.
    """
    cutoffs = {
        "jobs_days":                30,
        "sync_runs_days":           90,
        "scan_runs_days":           90,
        "local_files_history_days": 180,
    }
    deleted = {"jobs": 0, "sync_runs": 0, "scan_runs": 0,
               "local_files_history": 0}
    try:
        with get_conn(db_path) as conn:
            cur = conn.execute(
                "DELETE FROM jobs "
                "WHERE status IN ('done','failed','cancelled') "
                "  AND finished_at IS NOT NULL "
                f"  AND finished_at < datetime('now', '-{cutoffs['jobs_days']} days')"
            )
            deleted["jobs"] = cur.rowcount or 0
            cur = conn.execute(
                "DELETE FROM sync_runs "
                "WHERE status IN ('success','failed') "
                "  AND finished_at IS NOT NULL "
                f"  AND finished_at < datetime('now', '-{cutoffs['sync_runs_days']} days')"
            )
            deleted["sync_runs"] = cur.rowcount or 0
            cur = conn.execute(
                "DELETE FROM scan_runs "
                "WHERE status IN ('complete','failed','cancelled') "
                "  AND finished_at IS NOT NULL "
                f"  AND finished_at < datetime('now', '-{cutoffs['scan_runs_days']} days')"
            )
            deleted["scan_runs"] = cur.rowcount or 0
            # scan_findings cascades from scan_runs via ON DELETE
            # CASCADE — no explicit prune needed.
            cur = conn.execute(
                "DELETE FROM local_files_history "
                f"WHERE saved_at < datetime('now', '-{cutoffs['local_files_history_days']} days')"
            )
            deleted["local_files_history"] = cur.rowcount or 0
    except Exception as e:
        log.warning("prune_history failed: %s", e)
        return
    total = sum(deleted.values())
    if total:
        # INFO so the operator sees ongoing bounded-growth on a
        # nightly basis. The detail breakdown helps debugging if
        # one table's retention ever looks wrong.
        log_event(
            db_path, level="INFO", component="scheduler",
            message=(
                f"Pruned {total} history row(s) "
                f"(jobs={deleted['jobs']}, "
                f"sync_runs={deleted['sync_runs']}, "
                f"scan_runs={deleted['scan_runs']}, "
                f"local_files_history={deleted['local_files_history']})"
            ),
        )


def _cleanup_sessions_job(db_path: Path) -> None:
    """v1.13.10 (#14): daily sweep of expired auth_sessions rows.

    Pre-fix `cleanup_expired_sessions` only ran once at boot
    (main.py:125), so long-running containers accumulated dead
    session rows indefinitely between restarts. Cheap UPDATE
    keyed on idx_auth_sessions_expires; no-op when nothing's
    expired. Logged at INFO when rows are actually purged so the
    operator can see the table staying bounded.
    """
    from .auth import cleanup_expired_sessions
    try:
        purged = cleanup_expired_sessions(db_path)
    except Exception as e:
        log.warning("Session cleanup job failed: %s", e)
        return
    if purged:
        log_event(db_path, level="INFO", component="scheduler",
                  message=f"Purged {purged} expired session(s)")


def _sweep_placement_temps_job(db_path: Path) -> None:
    """v0.51.104: daily cleanup of orphaned theme.mp3.motif-tmp files left in
    media folders by interrupted atomic placements (a crash/restart between the
    hardlink-copy and the os.replace). Normally cleaned on the next placement to
    that folder, but a folder that never gets re-placed (edition drift, a switch
    to plex_upload) keeps the orphan. No-op when there are none."""
    from .plex_enum import sweep_stale_placement_temps
    try:
        removed = sweep_stale_placement_temps(db_path)
    except Exception as e:  # noqa: BLE001 — a hygiene sweep must never crash the scheduler
        log.warning("Stale .motif-tmp sweep job failed: %s", e)
        return
    if removed:
        log_event(db_path, level="INFO", component="scheduler",
                  message=f"Removed {removed} stale .motif-tmp placement temp(s)")


def _daily_health_passes_job(settings: "Settings") -> None:
    """v0.51.107: a daily FULL-TABLE run of the two verify_*_health passes,
    decoupled from the enum. v0.51.101 moved both passes INSIDE run_plex_enum's
    per-section scope, which left two gaps (found in code review):
      - the plex_upload staleness 0-stamp — the RE-PUSH detector — claims to
        'run unconditionally' but is now skipped ENTIRELY on a no-work enum
        (every section delta-gated), so a Plex delete+re-add that destroys an
        uploaded rating_key goes undetected until some section next changes; and
      - with both auto_enum toggles off NO enum fires at all, so canonical +
        placement health never re-stamp and a broken row can lag indefinitely.
    A table-wide (section_ids=None) re-stamp once a day restores the pre-v0.51.101
    guarantee regardless of enum config. Best-effort — a health-pass failure must
    never crash the scheduler."""
    from .plex_enum import verify_canonical_health, verify_placement_health
    db_path = settings.db_path
    try:
        ph = verify_placement_health(db_path)  # section_ids=None → table-wide
        if ph.get("missing") or ph.get("plex_upload_stale"):
            log_event(db_path, level="INFO", component="scheduler",
                      message=(f"Daily health: {ph['missing']} sidecar + "
                               f"{ph.get('plex_upload_stale', 0)} plex_upload "
                               f"placement(s) need attention (table-wide)"))
    except Exception as e:  # noqa: BLE001 — a hygiene pass must never crash the scheduler
        log.warning("Daily placement-health pass failed: %s", e)
    themes_dir = settings.themes_dir
    if themes_dir is None:
        return  # themes_dir not configured yet → nothing canonical to verify
    try:
        ch = verify_canonical_health(db_path, themes_dir)  # table-wide
        if ch.get("missing"):
            log_event(db_path, level="INFO", component="scheduler",
                      message=(f"Daily health: {ch['missing']} canonical "
                               f"theme.mp3 file(s) missing from motif storage "
                               f"(table-wide)"))
    except Exception as e:  # noqa: BLE001
        log.warning("Daily canonical-health pass failed: %s", e)


# v1.13.10 (#15): track consecutive release-check failures across
# scheduler invocations so we can surface persistent breakage
# (renamed repo, sustained 5xx, missing API token, etc.) without
# spamming the log on every transient blip. WARN once when we cross
# the threshold; reset on the next success. Module-level state is
# fine — there's only one scheduler per process.
_RELEASE_CHECK_FAIL_STREAK = {"count": 0, "warned": False}
_RELEASE_CHECK_FAIL_THRESHOLD = 3


def _check_release_update(settings: "Settings") -> None:
    """v1.13.8 (#8): poll GitHub's releases API for the latest stable
    motif release and cache it so the topbar can render an upgrade
    suffix when current < latest. Daily cadence stays well under
    GitHub's 60-req/hr unauthenticated limit. Failures (network /
    rate-limit / DNS) write nothing — the cache file simply ages,
    and the topbar suffix stays hidden until a future poll succeeds.

    Cached payload at <db_dir>/cache/release.json:
      {
        "checked_at": "...",
        "tag_name":   "v1.13.7",
        "html_url":   "https://github.com/.../releases/tag/v1.13.7",
        "name":       "v1.13.7",
      }

    Pre-release / draft entries are filtered out. The endpoint
    /releases/latest already excludes drafts and pre-releases by
    definition (returns the most recent stable). No extra filtering
    needed unless we ever want to surface RCs explicitly.
    """
    import json
    import httpx
    cache_dir = settings.db_path.parent / "cache"
    try:
        cache_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        log.debug("release check: cache dir mkdir failed: %s", e)
        return
    target = cache_dir / _RELEASE_CACHE_FILENAME

    def _record_failure(reason: str) -> None:
        # v1.13.10 (#15): elevate persistent failures to WARN once per
        # streak so a misconfigured repo / sustained 5xx is visible in
        # logs. Single transient blip stays at DEBUG.
        _RELEASE_CHECK_FAIL_STREAK["count"] += 1
        if (_RELEASE_CHECK_FAIL_STREAK["count"] >= _RELEASE_CHECK_FAIL_THRESHOLD
                and not _RELEASE_CHECK_FAIL_STREAK["warned"]):
            log.warning(
                "release check has failed %d consecutive times (%s) — "
                "topbar update suffix will stay hidden until this clears",
                _RELEASE_CHECK_FAIL_STREAK["count"], reason,
            )
            _RELEASE_CHECK_FAIL_STREAK["warned"] = True

    def _record_success() -> None:
        if (_RELEASE_CHECK_FAIL_STREAK["count"]
                or _RELEASE_CHECK_FAIL_STREAK["warned"]):
            log.info("release check recovered after %d failure(s)",
                     _RELEASE_CHECK_FAIL_STREAK["count"])
        _RELEASE_CHECK_FAIL_STREAK["count"] = 0
        _RELEASE_CHECK_FAIL_STREAK["warned"] = False

    try:
        with httpx.Client(timeout=10.0,
                          headers={"User-Agent": "motif-release-check/1.0",
                                   "Accept": "application/vnd.github+json"}) as client:
            r = client.get(_RELEASE_API)
        if r.status_code != 200:
            log.debug("release check: HTTP %s", r.status_code)
            _record_failure(f"HTTP {r.status_code}")
            return
        data = r.json()
        if data.get("draft") or data.get("prerelease"):
            # v1.13.15: prerelease/draft is treated as connectivity-success
            # (the endpoint answered, the streak resets) but NOT as
            # update-available. The /releases/latest endpoint normally
            # excludes both — if we land here it's the rare case where
            # the repo's only published releases are prereleases, so the
            # topbar suffix legitimately stays hidden. No log spam.
            _record_success()
            return
        payload = {
            "checked_at": now_iso(),
            "tag_name": str(data.get("tag_name") or "").strip(),
            "html_url": str(data.get("html_url") or "").strip(),
            "name": str(data.get("name") or data.get("tag_name") or "").strip(),
        }
        if not payload["tag_name"]:
            _record_failure("response missing tag_name")
            return
        target.write_text(json.dumps(payload))
        _record_success()
        # v1.17.4: release_available notification with tag-based
        # dedupe. The release check fires once per day (per the
        # scheduler cron), so without dedupe a user with the toggle
        # ON would see a release-available ping daily until they
        # upgraded. Tag-based dedupe means each NEW release fires
        # exactly once (then quiet until the next new release),
        # surviving motif process restarts via the persisted
        # `last_value` in runtime_settings.
        try:
            from .. import __version__ as motif_version
            from . import notify as _notify
            from . import notify_dedupe as _dedupe
            # Skip if the detected tag matches the running motif —
            # no upgrade available, no point pinging.
            # v0.51.231 (audit): was `payload["tag_name"] != running`, i.e. INEQUALITY.
            # motif's channel model (nightly = dev, release = last shipped tag) means the
            # running version is routinely AHEAD of the latest GitHub Release, so
            # publishing v0.51.220 while the box runs 0.51.227 pushed "🆕 v0.51.220
            # available — you're running v0.51.227": an upgrade ping pointing BACKWARDS.
            # Tag-keyed dedupe fires it once per release and never self-corrects. The
            # sibling at api.py's /api/release/latest already compared parsed tuples, so
            # the topbar stayed silent while the push nagged — mirror drift. Both now use
            # the shared is_newer().
            from .versioning import is_newer
            running = f"v{motif_version}"
            if is_newer(payload["tag_name"], running) and _dedupe.should_fire(
                    settings.db_path, "release_available",
                    edge_value=payload["tag_name"]):
                _notify.dispatch(
                    settings.db_path, settings.cfg.notifications,
                    event_kind="release_available",
                    # v1.17.19: 🆕 emoji prefix; keep "motif" so the
                    # release attribution is unambiguous ("vX.Y.Z
                    # available" alone is too vague).
                    title=f"🆕 motif {payload['tag_name']} available",
                    # v1.19.92: dropped the "A new motif release is
                    # available on GitHub: {tag}" line — it restated
                    # the title + repeated the version. Body carries
                    # only the new facts: running version + notes link.
                    body=(
                        f"You're running {running}. "
                        f"Release notes: {payload['html_url']}"
                    ),
                )
                _dedupe.record_fire(
                    settings.db_path, "release_available",
                    value=payload["tag_name"])
        except Exception as e:
            log.debug("notify dispatch (release_available) suppressed: %s", e)
    except Exception as e:
        log.debug("release check: failed: %s", e)
        _record_failure(f"{type(e).__name__}: {e}")


def _scheduled_database_backup(settings: "Settings") -> None:
    """v1.23.16: nightly motif.db snapshot + retention prune.

    The cron is fixed at scheduler boot (settings.db_backup_cron), but
    `enabled` + `retention` are read FRESH here so toggling the feature
    or changing retention takes effect on the next tick WITHOUT a
    restart (mirrors _refresh_sections_job's runtime plex_enabled
    gate). A disabled feature is a clean no-op."""
    if not settings.db_backup_enabled:
        return
    from datetime import datetime, timezone
    from . import db_backup
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    try:
        bf = db_backup.create_backup(
            settings.db_path, settings.config_dir, now_stamp=stamp)
    except FileExistsError:
        # A manual backup already landed this exact second — harmless.
        log.debug("scheduled backup: same-second snapshot exists, skipping")
        return
    except Exception as e:
        # Cold-path (v1.18.5 rule): surface WHY a scheduled backup
        # didn't produce a file so a silent disk-full / perms fault is
        # visible in the operator log instead of a black box.
        log_event(settings.db_path, level="WARNING", component="backup",
                  message=f"Scheduled database backup failed: {e}")
        return
    removed = db_backup.prune_backups(
        settings.config_dir, settings.db_backup_retention)
    msg = f"Scheduled database backup created: {bf.name}"
    if removed:
        msg += f" · pruned {len(removed)} old"
    log_event(settings.db_path, level="INFO", component="backup", message=msg)


def start_scheduler(settings: Settings) -> BackgroundScheduler:
    # v0.50.89: APScheduler's default misfire_grace_time is 1s, so any job
    # whose fire time was straddled by a container restart (a daily sync at
    # 1pm while motif was down for a redeploy) is judged "misfired" and
    # silently skipped on resume. Set a 1h grace so a job due during a short
    # outage still runs once the scheduler comes back, and coalesce=True so a
    # longer outage that straddled several fires of a frequent interval job
    # collapses into a single catch-up run instead of a startup burst.
    scheduler = BackgroundScheduler(
        timezone="UTC", daemon=True,
        job_defaults={"misfire_grace_time": 3600, "coalesce": True},
    )

    # Daily sync
    # v1.15.117: validate cron parts by constructing the trigger
    # inside try/except. Pre-fix the only check was `len(parts) != 5`
    # — non-numeric components like `MOTIF_SYNC_CRON="0 13 1 * 99"`
    # (day_of_week=99) passed that gate but raised ValueError inside
    # CronTrigger, propagating out of start_scheduler and crashing
    # startup AFTER the worker thread was already running. The
    # fallback now catches the construction error, logs it, and
    # uses the canonical default — keeping motif boot-clean even
    # when the env var is mistyped.
    cron_parts = settings.sync_cron.split()
    fallback_cron = "0 13 * * *"
    if len(cron_parts) != 5:
        log.error("Invalid MOTIF_SYNC_CRON %r (wrong field count) — "
                  "falling back to '%s'", settings.sync_cron, fallback_cron)
        cron_parts = fallback_cron.split()
    minute, hour, dom, month, dow = cron_parts

    try:
        sync_trigger = CronTrigger(
            minute=minute, hour=hour, day=dom, month=month,
            day_of_week=dow, timezone="UTC",
        )
    except (ValueError, TypeError) as e:
        log.error(
            "Invalid MOTIF_SYNC_CRON %r (CronTrigger rejected: %s) — "
            "falling back to '%s'",
            settings.sync_cron, e, fallback_cron,
        )
        minute, hour, dom, month, dow = fallback_cron.split()
        sync_trigger = CronTrigger(
            minute=minute, hour=hour, day=dom, month=month,
            day_of_week=dow, timezone="UTC",
        )

    scheduler.add_job(
        _enqueue_sync, args=[settings.db_path],
        trigger=sync_trigger,
        id="daily_sync", replace_existing=True, max_instances=1,
    )

    # Hourly placement retry
    scheduler.add_job(
        _retry_pending_placements, args=[settings.db_path],
        trigger=IntervalTrigger(hours=1),
        id="placement_retry", replace_existing=True, max_instances=1,
    )

    # v1.24.26: hourly auto-restore of motif-owned sidecars whose Plex
    # theme.mp3 went missing (the delete+re-add case the v1.18.90 reaper
    # silently skips as 'other_fallback'). Re-places from the surviving
    # canonical + fires theme_auto_restored.
    scheduler.add_job(
        _restore_lost_placements, args=[settings],
        trigger=IntervalTrigger(hours=1),
        id="restore_lost_placements", replace_existing=True, max_instances=1,
    )

    # v1.13.8 (#8): once-a-day GitHub release check. Runs 17 minutes
    # past the hour at 04:17 UTC — deliberately avoiding 0/15/30/45
    # so this never collides with cron runs that target whole-quarter
    # marks. Result populates <db_dir>/cache/release.json which the
    # topbar reads via /api/release/latest.
    scheduler.add_job(
        _check_release_update, args=[settings],
        trigger=CronTrigger(minute="17", hour="4", timezone="UTC"),
        id="release_check", replace_existing=True, max_instances=1,
    )

    # v1.13.10 (#14): daily expired-session purge at 03:00 UTC. Runs
    # before the release check (04:17) and well clear of the default
    # sync (13:00) so writer contention is impossible.
    scheduler.add_job(
        _cleanup_sessions_job, args=[settings.db_path],
        trigger=CronTrigger(minute="0", hour="3", timezone="UTC"),
        id="session_cleanup", replace_existing=True, max_instances=1,
    )

    # v1.17.9: daily DB-hygiene sweeps at 03:05 / 03:10 UTC. Slotted
    # right after session_cleanup (03:00) and well clear of release
    # check (04:17) + default sync (13:00) so each writer-touching
    # job has the lock to itself. Both jobs no-op when nothing's
    # expired/old. Hygiene audit (DB sweep findings #1 + #2).
    scheduler.add_job(
        _prune_tvdb_lookup_cache, args=[settings.db_path],
        trigger=CronTrigger(minute="5", hour="3", timezone="UTC"),
        id="tvdb_lookup_cache_prune",
        replace_existing=True, max_instances=1,
    )
    scheduler.add_job(
        _prune_events, args=[settings.db_path],
        trigger=CronTrigger(minute="10", hour="3", timezone="UTC"),
        id="events_prune", replace_existing=True, max_instances=1,
    )
    # v0.51.152: notifications inbox retention — 03:12, between events_prune
    # (03:10) and prune_history (03:15).
    scheduler.add_job(
        _prune_notifications, args=[settings.db_path],
        trigger=CronTrigger(minute="12", hour="3", timezone="UTC"),
        id="notifications_prune", replace_existing=True, max_instances=1,
    )

    # v1.17.11: unified history prune for five append-forever
    # tables (jobs, sync_runs, scan_runs, scan_findings via
    # cascade, local_files_history). Slotted at 03:15 UTC after
    # events_prune (03:10) and well clear of release_check (04:17)
    # + daily_sync (13:00). Hygiene audit Tier B.
    scheduler.add_job(
        _prune_history, args=[settings.db_path],
        trigger=CronTrigger(minute="15", hour="3", timezone="UTC"),
        id="history_prune", replace_existing=True, max_instances=1,
    )

    # v0.51.104: daily cleanup of orphaned theme.mp3.motif-tmp files (interrupted
    # atomic placements). Slotted 03:20 UTC after the DB-hygiene prunes (03:05-
    # 03:15), clear of release check (04:17) + sync (13:00). It's an FS walk, not
    # a writer-lock job, so it doesn't contend with the DB prunes above; no-op
    # when there are no orphans.
    scheduler.add_job(
        _sweep_placement_temps_job, args=[settings.db_path],
        trigger=CronTrigger(minute="20", hour="3", timezone="UTC"),
        id="placement_temp_sweep", replace_existing=True, max_instances=1,
    )

    # v0.51.107: daily FULL-TABLE health re-stamp (placement + canonical),
    # decoupled from the enum. v0.51.101 scoped both health passes to the walked
    # section(s), which skips them on a no-work enum and never runs them at all
    # when both auto_enum toggles are off — so the plex_upload RE-PUSH detector
    # + the broken-row re-stamp could lag indefinitely. This restores the
    # unconditional daily coverage. Slotted 03:25 UTC after the .motif-tmp sweep
    # (03:20), clear of the 04:xx jobs + 13:00 sync.
    scheduler.add_job(
        _daily_health_passes_job, args=[settings],
        trigger=CronTrigger(minute="25", hour="3", timezone="UTC"),
        id="daily_health_passes", replace_existing=True, max_instances=1,
    )

    # v1.13.54: op_progress prune every 30 minutes. Was firing on
    # every /api/progress poll (1Hz active); now off the read path.
    scheduler.add_job(
        _prune_op_progress, args=[settings.db_path],
        trigger=IntervalTrigger(minutes=30),
        id="op_progress_prune", replace_existing=True, max_instances=1,
    )

    # v1.23.16: scheduled database backup. The job is ALWAYS
    # registered (so enabling the feature later takes effect on the
    # next tick without a restart — the job no-ops when disabled); the
    # cron is read once here, mirroring daily_sync's boot-fixed
    # schedule + try/except CronTrigger fallback (a mistyped cron must
    # not crash startup). Default 04:00 UTC is clear of the 03:xx
    # hygiene prunes + 04:17 release check + 13:00 sync.
    bk_fallback = "0 4 * * *"
    bk_parts = (settings.db_backup_cron or bk_fallback).split()
    if len(bk_parts) != 5:
        log.error("Invalid database_backup.cron %r (wrong field count) — "
                  "falling back to '%s'", settings.db_backup_cron, bk_fallback)
        bk_parts = bk_fallback.split()
    try:
        bk_trigger = CronTrigger(
            minute=bk_parts[0], hour=bk_parts[1], day=bk_parts[2],
            month=bk_parts[3], day_of_week=bk_parts[4], timezone="UTC",
        )
    except (ValueError, TypeError) as e:
        log.error("Invalid database_backup.cron %r (CronTrigger rejected: "
                  "%s) — falling back to '%s'",
                  settings.db_backup_cron, e, bk_fallback)
        bk_parts = bk_fallback.split()
        bk_trigger = CronTrigger(
            minute=bk_parts[0], hour=bk_parts[1], day=bk_parts[2],
            month=bk_parts[3], day_of_week=bk_parts[4], timezone="UTC",
        )
    scheduler.add_job(
        _scheduled_database_backup, args=[settings],
        trigger=bk_trigger,
        id="database_backup", replace_existing=True, max_instances=1,
    )

    # v1.13.11: stuck-job sweep every 15 minutes. Per-job-type
    # thresholds reset jobs.status='running' rows whose started_at is
    # older than the worst plausible runtime, so a hung worker doesn't
    # leave a permanent spinner + blocked queue between container
    # restarts.
    scheduler.add_job(
        _stuck_job_sweep, args=[settings.db_path],
        trigger=IntervalTrigger(minutes=15),
        id="stuck_job_sweep", replace_existing=True, max_instances=1,
    )

    # v1.24.15 (holistic review): runtime op_progress stuck-sweep, sibling of
    # the jobs sweep above. op_progress had only a boot reset, so a raised
    # finish_progress left an op 409-locked + a LIVE OPS phantom until restart.
    scheduler.add_job(
        _sweep_stuck_op_progress, args=[settings.db_path],
        trigger=IntervalTrigger(minutes=15),
        id="op_progress_stuck_sweep", replace_existing=True, max_instances=1,
    )

    # Daily Plex section discovery (catches newly-added libraries).
    # Scheduled 1 hour before the sync so sync sees fresh section list.
    # v1.14.54: skip the section_refresh cron entirely when the sync
    # hour is non-numeric (`*`, `*/N`, `1,13`, etc.). Pre-fix the
    # ternary parsed as:
    #     section_minute = minute
    #     section_hour = str(...) if hour.isdigit() else hour
    # — so a non-numeric hour passed straight through unchanged, and
    # section_refresh fired at the SAME wall-clock as daily_sync.
    # Both writer-heavy → BEGIN IMMEDIATE contention → one job loses
    # the race and partial-fails with OperationalError. Comment also
    # lied ("30 minutes before"; the code computes 1 hour before).
    # Fixed-offset fallback (e.g. always at 12:00 UTC) would surprise
    # operators who set wildcards expecting "no specific schedule";
    # skipping with a warning is honest. Numeric hours keep the
    # (hour - 1) % 24 offset.
    if hour.isdigit():
        section_minute = minute
        section_hour = str((int(hour) - 1) % 24)
        # v0.51.16 (audit #25): hour 0 wraps to 23 — the PREVIOUS calendar
        # day. Keeping restricted day fields then fires the refresh ~23h
        # AFTER the sync, not 1h before (a Sunday 00:30 sync got its
        # refresh Sunday 23:30). Drop dom/dow in that case: daily at
        # 23:mm always lands 1h before the sync day's run, at the cost
        # of extra idempotent refreshes on off days. (A restricted month
        # still mis-times the month's FIRST sync — rare enough to leave.)
        section_dom, section_dow = dom, dow
        if int(hour) == 0 and (dom != "*" or dow != "*"):
            section_dom, section_dow = "*", "*"
            log.info(
                "scheduler: sync hour 0 wraps section_refresh to 23:%s "
                "the previous day — dropping day restrictions (dom=%r "
                "dow=%r) so the refresh runs daily, 1h before each sync",
                section_minute, dom, dow,
            )
        scheduler.add_job(
            _refresh_sections_job, args=[settings],
            trigger=CronTrigger(minute=section_minute, hour=section_hour,
                                day=section_dom, month=month,
                                day_of_week=section_dow, timezone="UTC"),
            id="section_refresh", replace_existing=True, max_instances=1,
        )
    else:
        log.warning(
            "scheduler: section_refresh cron skipped because sync hour "
            "is non-numeric (%r). Pre-v1.14.54 the cron silently ran "
            "in lock-step with daily_sync, causing BEGIN IMMEDIATE "
            "contention. Set a numeric hour in MOTIF_SYNC_CRON to "
            "re-enable the daily section refresh.",
            hour,
        )

    scheduler.start()
    log.info(
        "Scheduler started: daily sync at %s UTC, placement retry hourly, "
        "section refresh 1h before sync, release check 04:17, "
        "session cleanup 03:00, stuck-job sweep every 15min",
        settings.sync_cron,
    )
    # v1.13.8: kick the release check once at startup (in a background
    # thread so motif boot isn't delayed if GitHub is slow). Without
    # this, a freshly-deployed install waits up to 24h for the daily
    # cron before showing an upgrade notice.
    import threading
    threading.Thread(
        target=_check_release_update, args=(settings,),
        daemon=True, name="motif-release-check-bootstrap",
    ).start()
    return scheduler
