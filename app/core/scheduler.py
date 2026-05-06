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
from .db import get_conn
from .events import log_event, now_iso

log = logging.getLogger(__name__)


def _enqueue_sync(db_path: Path) -> None:
    with get_conn(db_path) as conn:
        # Don't double-enqueue
        existing = conn.execute(
            "SELECT id FROM jobs WHERE job_type = 'sync' AND status IN ('pending','running')"
        ).fetchone()
        if existing:
            return
        conn.execute(
            """INSERT INTO jobs (job_type, status, created_at, next_run_at)
               VALUES ('sync', 'pending', ?, ?)""",
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
    """
    with get_conn(db_path) as conn:
        rows = conn.execute(
            """
            SELECT lf.media_type, lf.tmdb_id, lf.section_id
            FROM local_files lf
            LEFT JOIN placements p
              ON p.media_type = lf.media_type
             AND p.tmdb_id = lf.tmdb_id
             AND p.section_id = lf.section_id
            WHERE p.media_folder IS NULL
              AND NOT EXISTS (
                  SELECT 1 FROM jobs j
                  WHERE j.job_type = 'place' AND j.media_type = lf.media_type
                    AND j.tmdb_id = lf.tmdb_id
                    AND j.section_id = lf.section_id
                    AND j.status IN ('pending', 'running')
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
            conn.execute(
                """INSERT INTO jobs (job_type, media_type, tmdb_id, section_id,
                                     payload, status, created_at, next_run_at)
                   VALUES ('place', ?, ?, ?, '{}', 'pending', ?, ?)""",
                (r["media_type"], r["tmdb_id"], r["section_id"],
                 now_iso(), now_iso()),
            )
    if rows:
        log_event(db_path, level="INFO", component="scheduler",
                  message=f"Retry sweep enqueued {len(rows)} placement jobs")


_RELEASE_API = "https://api.github.com/repos/Healzangels/motif/releases/latest"
_RELEASE_CACHE_FILENAME = "release.json"


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
    try:
        with httpx.Client(timeout=10.0,
                          headers={"User-Agent": "motif-release-check/1.0",
                                   "Accept": "application/vnd.github+json"}) as client:
            r = client.get(_RELEASE_API)
        if r.status_code != 200:
            log.debug("release check: HTTP %s", r.status_code)
            return
        data = r.json()
        if data.get("draft") or data.get("prerelease"):
            return
        payload = {
            "checked_at": now_iso(),
            "tag_name": str(data.get("tag_name") or "").strip(),
            "html_url": str(data.get("html_url") or "").strip(),
            "name": str(data.get("name") or data.get("tag_name") or "").strip(),
        }
        if not payload["tag_name"]:
            return
        target.write_text(json.dumps(payload))
    except Exception as e:
        log.debug("release check: failed: %s", e)


def start_scheduler(settings: Settings) -> BackgroundScheduler:
    scheduler = BackgroundScheduler(timezone="UTC", daemon=True)

    # Daily sync
    cron_parts = settings.sync_cron.split()
    if len(cron_parts) != 5:
        log.error("Invalid MOTIF_SYNC_CRON %r — falling back to '0 13 * * *'",
                  settings.sync_cron)
        cron_parts = "0 13 * * *".split()
    minute, hour, dom, month, dow = cron_parts

    scheduler.add_job(
        _enqueue_sync, args=[settings.db_path],
        trigger=CronTrigger(minute=minute, hour=hour, day=dom, month=month,
                            day_of_week=dow, timezone="UTC"),
        id="daily_sync", replace_existing=True, max_instances=1,
    )

    # Hourly placement retry
    scheduler.add_job(
        _retry_pending_placements, args=[settings.db_path],
        trigger=IntervalTrigger(hours=1),
        id="placement_retry", replace_existing=True, max_instances=1,
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

    # Daily Plex section discovery (catches newly-added libraries).
    # Scheduled 30 minutes before the sync so sync sees fresh section list.
    section_minute, section_hour = minute, str((int(hour) - 1) % 24) if hour.isdigit() else hour
    scheduler.add_job(
        _refresh_sections_job, args=[settings],
        trigger=CronTrigger(minute=section_minute, hour=section_hour, day=dom, month=month,
                            day_of_week=dow, timezone="UTC"),
        id="section_refresh", replace_existing=True, max_instances=1,
    )

    scheduler.start()
    log.info("Scheduler started: daily sync at %s UTC, placement retry hourly, section refresh 1h before sync",
             settings.sync_cron)
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
