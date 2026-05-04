"""
Motif application entry point.

Wires together:
1. Database initialization
2. Background worker thread for the job queue
3. APScheduler for periodic syncs and placement retries
4. FastAPI/Uvicorn for the web UI and JSON API

A single SIGTERM/SIGINT triggers a clean shutdown of all three.
"""
from __future__ import annotations

import logging
import signal
import sys
import threading

import uvicorn

from .config import get_settings
from .core.db import init_db
from .core.auth import init_auth_schema, cleanup_expired_sessions
from .core.events import log_event
from .core.scheduler import start_scheduler
from .core.worker import start_worker
from .web.api import create_app


def configure_logging(level: str) -> None:
    """Configure root logging once. Format mirrors the CRT-style UI."""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-7s %(name)-22s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )
    # Quiet down chatty third-party loggers
    logging.getLogger("apscheduler").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("yt_dlp").setLevel(logging.WARNING)


def _bootstrap_config_file(settings) -> None:
    """On first run, write a seed motif.yaml from the env var state.

    This handles two cases:
      1. Fresh deploy: motif.yaml doesn't exist, env vars hold the
         user's preferences. We snapshot env into YAML so subsequent
         UI edits can persist.
      2. Migration from v1.3.x: same — env vars get snapshotted; the
         user can then edit themes_dir on the UI to switch from the
         old /themes mount to the new /data/media/themes path.

    After this, the env vars continue to override (12-factor behavior),
    but anything NOT set in env is now editable from /settings.
    """
    cf = settings.config_file
    if cf.exists():
        return
    log = logging.getLogger("motif.main")
    log.info("motif.yaml not present — seeding from env + defaults")
    settings.save(settings.cfg, updated_by="bootstrap")


def main() -> int:
    settings = get_settings()
    configure_logging(settings.log_level)
    log = logging.getLogger("motif.main")

    # Verify config dir exists (it might be a fresh appdata mount)
    settings.config_dir.mkdir(parents=True, exist_ok=True)

    # Seed motif.yaml on first run if missing (also handles v1.3.x migration)
    _bootstrap_config_file(settings)

    # v1.11.0: only the themes_dir root is created here. Per-section
    # subdirs (themes_dir/<themes_subdir>) are created on demand by the
    # download / adopt / upload paths, after Plex section discovery has
    # populated plex_sections.themes_subdir. Pre-creating them at boot
    # would require enumerating Plex first, and we want the web UI up
    # even on a totally fresh install.
    if settings.is_paths_ready():
        td = settings.themes_dir
        try:
            td.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            log.error("Failed to create themes_dir %s: %s", td, e)

    log.info("motif starting")
    log.info("  config_dir   = %s", settings.config_dir)
    log.info("  data_dir     = %s", settings.data_dir)
    log.info("  themes_dir   = %s",
             settings.themes_dir if settings.is_paths_ready() else "(not configured — visit /settings)")
    log.info("  cookies_file = %s (%s)", settings.cookies_file,
             "present" if settings.cookies_file.exists() else "MISSING")
    log.info("  plex_url     = %s", settings.plex_url or "(disabled)")
    log.info("  forward_auth = %s", settings.trust_forward_auth)

    # Surface env overrides so the operator knows what's locked
    overrides = settings.env_overrides()
    if overrides:
        log.info("  env overrides active: %d settings (will not be editable from UI)",
                 len(overrides))
        for path, var in overrides.items():
            log.debug("    %s ← %s", path, var)

    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    purged = cleanup_expired_sessions(settings.db_path)
    if purged:
        log.info("Purged %d expired session(s)", purged)

    # v1.12.81: one-shot cleanup of place jobs that pre-date the
    # scheduler / plex_enum section_id fix. Worker rejects them as
    # `_JobPermanentFailure("place job missing section_id (v1.11.0
    # requires per-section routing)")`, leaving them stuck in
    # status='failed' which the topbar's q.failed counter renders
    # as a perma-red FAIL dot. Cancel them on startup so the dot
    # clears once the bad enqueue paths are no longer creating
    # new ones. Pure cleanup — no side effects beyond the jobs
    # row and matches the worker's normal _mark_cancelled
    # behavior. Safe to keep in perpetuity (idempotent — only
    # touches jobs with NULL section_id, which the new enqueue
    # paths never produce).
    try:
        from .core.db import get_conn
        from .core.events import now_iso
        with get_conn(settings.db_path) as conn:
            cur = conn.execute(
                """UPDATE jobs SET status = 'cancelled', finished_at = ?
                   WHERE job_type IN ('place', 'download')
                     AND section_id IS NULL
                     AND status IN ('pending', 'failed')""",
                (now_iso(),),
            )
            if cur.rowcount:
                log.info(
                    "Cancelled %d stuck section_id-less job(s) "
                    "(pre-v1.12.81 scheduler/plex_enum bug)",
                    cur.rowcount,
                )
    except Exception as e:
        log.warning("Section_id-less job cleanup skipped: %s", e)

    # v1.11.0: legacy startup data migrations (relocate_legacy_canonical_files,
    # backfill_hash_match_provenance) are gone — the per-section themes layout
    # is a fresh-start release; the DB is repopulated from upstream sources by
    # sync + plex_enum + scan after the v17 → v18 migration's hard-stop fires.

    # v1.11.58: bring existing sections forward to the location-path-
    # derived themes_subdir naming. Idempotent — sections already on
    # the new naming are no-ops; rename failures (target exists, etc.)
    # are logged and skipped so a partial migration never half-updates
    # local_files.file_path. Only runs when themes_dir is configured.
    if settings.is_paths_ready():
        try:
            from .core.sections import migrate_themes_subdirs_inplace
            migrated = migrate_themes_subdirs_inplace(
                settings.db_path, settings.themes_dir,
            )
            if migrated:
                log.info("Themes subdir migration: %d section(s) updated",
                         migrated)
        except Exception as e:
            log.warning("Themes subdir migration failed at startup: %s", e)

    # Auto-discover Plex sections at startup so /libraries works even before
    # the first sync. Failures here are non-fatal (Plex might just be down).
    if settings.plex_enabled and settings.plex_url and settings.plex_token:
        try:
            from .core.plex import PlexClient, PlexConfig
            from .core.sections import refresh_sections
            cfg = PlexConfig(
                url=settings.plex_url, token=settings.plex_token,
                movie_section=settings.plex_movie_section,
                tv_section=settings.plex_tv_section, enabled=True,
            )
            with PlexClient(cfg, plus_mode=settings.plus_equiv_mode) as plex:  # type: ignore[arg-type]
                sections = refresh_sections(
                    settings.db_path, plex,
                    excluded_titles=settings.plex_excluded_titles,
                    included_titles=settings.plex_included_titles,
                )
            log.info("Plex sections discovered: %d (managed: %d)",
                     len(sections), sum(1 for s in sections if s.get("included")))
        except Exception as e:
            log.warning("Plex section discovery failed: %s", e)

    log_event(settings.db_path, level="INFO", component="main",
              message="motif started")

    # Force the session key to be created/loaded so it's ready for the web layer
    settings.resolve_session_key()

    # Start workers (long + general — see start_worker docstring).
    stop_event = threading.Event()
    worker_threads = start_worker(settings, stop_event)

    # Start scheduler
    scheduler = start_scheduler(settings)

    # Wire shutdown
    def shutdown(signum, _frame):
        log.info("Received signal %s — shutting down", signum)
        stop_event.set()
        try:
            scheduler.shutdown(wait=False)
        except Exception:
            pass
        log_event(settings.db_path, level="INFO", component="main",
                  message="motif shutting down")
        # Uvicorn will receive its own signal handler; it'll stop the server
        # When uvicorn exits we proceed to thread-join below
    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    # Build FastAPI app
    app = create_app(settings)

    # Run uvicorn in foreground — it handles its own signal trapping for the HTTP
    # server, and our custom handlers above will fire for the worker/scheduler.
    config = uvicorn.Config(
        app,
        host=settings.web_host,
        port=settings.web_port,
        log_config=None,        # we configured logging already
        access_log=False,
        proxy_headers=settings.trust_forward_auth,
        forwarded_allow_ips="*" if settings.trust_forward_auth else None,
    )
    server = uvicorn.Server(config)
    try:
        server.run()
    finally:
        stop_event.set()
        for _t in worker_threads:
            _t.join(timeout=10.0)
        log.info("motif stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
