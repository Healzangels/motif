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

from . import __version__
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
    # v1.14.64: dulwich.protocol dumps the full git ref advertisement
    # at DEBUG (~600 lines per fetch on the ThemerrDB repo) plus
    # the per-byte packfile receive frames. dulwich.config logs
    # gitconfig file lookups twice per sync. Both are diagnostic-
    # only — the sync.py module-level INFO logs already carry the
    # operator-visible signal ("Sync run #N: git mirror acquired"
    # etc.). httpcore is httpx's underlying library and emits its
    # own connection / send / receive frames at DEBUG; quieting
    # httpx (above) without httpcore is a half-fix. the user repro:
    # /queue logs view flooded after enabling DEBUG to diagnose
    # the v1.14.62 button-lock-label bug.
    logging.getLogger("dulwich").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    # v1.23.92 (SECURITY): apprise logs the FULL Discord/Slack/etc. webhook
    # URL — INCLUDING the secret token — at DEBUG ("Discord POST URL:
    # https://discord.com/api/webhooks/<id>/<token>") plus the Loaded-URL +
    # payload frames. An operator running at DEBUG (e.g. to diagnose a sync)
    # would write that credential to stdout/the log file on every notify.
    # motif's own `notify` logger already carries the operator-visible signal
    # ("Notification sent: <kind> | {sent_ok, sent_fail}"), so clamp apprise to
    # WARNING — its failures still surface, but the credential-bearing DEBUG/INFO
    # frames never emit, no matter the configured log level. (Defense in depth:
    # the webhook should still be treated as compromised + rotated if it ever
    # appeared in a DEBUG log.)
    logging.getLogger("apprise").setLevel(logging.WARNING)


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


def _probe_writability(settings, log) -> None:
    """v1.22.4: boot-time writability probe for config_dir + themes_dir.

    A non-root container on a permission-enforcing share (Unraid shfs runs with
    `default_permissions`) silently can't write dirs its uid doesn't own — and
    the failure only surfaces hours later as a crash-looping download (the user's
    1000-vs-99 incident: every write EACCES'd for a week before the root cause
    was found). Surface it LOUDLY at boot instead, naming the process uid/gid
    AND the dir's owner/mode so the fix (PUID/PGID or share ownership) is
    obvious. Class-9 cold-path: a permission gap with no breadcrumb reads as
    'configured fine' when it isn't. Probe only — never raises."""
    import os
    uid = os.getuid() if hasattr(os, "getuid") else -1
    gid = os.getgid() if hasattr(os, "getgid") else -1
    targets = [("config_dir", settings.config_dir)]
    _ready = settings.is_paths_ready()
    if _ready:
        targets.append(("themes_dir", settings.themes_dir))
    for name, path in targets:
        try:
            path.mkdir(parents=True, exist_ok=True)
            probe = path / ".motif-write-probe"
            probe.write_bytes(b"")
            probe.unlink()
        except OSError as e:
            try:
                st = path.stat()
                owned = f"{st.st_uid}:{st.st_gid} mode={oct(st.st_mode & 0o777)}"
            except OSError:
                owned = "unknown"
            log.error(
                "  WRITABILITY: %s (%s) is NOT writable — motif runs as "
                "uid=%s gid=%s, but the directory is owned %s (%s). Downloads, "
                "placement, and backups WILL FAIL until this matches. Fix: set "
                "the container PUID/PGID to the directory's owner, or chown the "
                "share. See the README 'Permissions / PUID-PGID' section.",
                name, path, uid, gid, owned, e,
            )


def main() -> int:
    settings = get_settings()
    configure_logging(settings.log_level)
    log = logging.getLogger("motif.main")

    # Verify config dir exists (it might be a fresh appdata mount)
    settings.config_dir.mkdir(parents=True, exist_ok=True)

    # v1.23.17/.18: apply a staged database restore as the VERY FIRST
    # thing that touches the DB file, before any event-log call (which
    # spawns the events-flusher thread that opens its own long-lived
    # sqlite connection — v1.23.18 code-review race: a connection open
    # before the swap would hold the old inode + a stale WAL across the
    # os.replace). The swap is clean here: no open FDs, stale WAL
    # removed, current db safety-copied first; init_db (further down)
    # then migrates the restored file forward if needed.
    from datetime import datetime, timezone
    from .core import db_backup as _db_backup
    _restore = _db_backup.apply_pending_restore(
        settings.db_path, settings.config_dir,
        now_stamp=datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S"))
    if _restore and _restore.get("applied"):
        log.warning("Database restored at boot (schema v%s; safety backup: %s)",
                    _restore.get("schema_version"),
                    _restore.get("safety_backup") or "(none)")
    elif _restore and _restore.get("error"):
        log.error("Staged restore not applied: %s", _restore["error"])

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

    # v1.24.49: stamp the running version on the first boot line so pasted logs
    # carry it (the topbar shows it in the UI, but logs had no version marker).
    log.info("motif v%s starting", __version__)
    log.info("  config_dir   = %s", settings.config_dir)
    log.info("  data_dir     = %s", settings.data_dir)
    log.info("  themes_dir   = %s",
             settings.themes_dir if settings.is_paths_ready() else "(not configured — visit /settings)")
    # v1.22.4: confirm motif can actually WRITE its dirs before the worker
    # starts queuing downloads — a uid/owner mismatch otherwise fails silently.
    _probe_writability(settings, log)
    log.info("  cookies_file = %s (%s)", settings.cookies_file,
             "present" if settings.cookies_file.exists() else "MISSING")
    log.info("  plex_url     = %s", settings.plex_url or "(disabled)")
    log.info("  forward_auth = %s", settings.trust_forward_auth)
    # v1.21.16 / v1.24.12: forward-auth trusting X-Authentik-Username from ANY
    # peer was an admin-bypass footgun (an empty forward_auth_allowed_ips used
    # to skip the IP gate entirely, so a direct :5309 request carrying the
    # header was full admin). v1.24.12 made _resolve_principal FAIL CLOSED:
    # with trust on but an empty allowlist, the header is no longer trusted —
    # forward-auth is a no-op until the operator sets the proxy IP. We still
    # surface the misconfiguration LOUDLY at boot so it isn't silent (class-9:
    # a forward-auth that silently authenticates nobody reads as "broken login"
    # without a breadcrumb).
    if settings.trust_forward_auth and not settings.forward_auth_allowed_ips:
        _warn = (
            "forward_auth is ON but forward_auth_allowed_ips is EMPTY — as of "
            "v1.24.12 motif FAILS CLOSED and will NOT trust the "
            "X-Authentik-Username header from any peer, so forward-auth is "
            "effectively DISABLED (only API-token / session login works). "
            "Set forward_auth_allowed_ips to your proxy's IP to enable it."
        )
        log.warning("  SECURITY: %s", _warn)
        log_event(settings.db_path, level="WARNING", component="auth",
                  message="forward_auth on with empty IP allowlist — "
                          "forward-auth disabled (fail-closed) until "
                          "forward_auth_allowed_ips is set")
    # v1.15.16: surface the running yt-dlp version so operators can tell
    # what's installed without exec-ing into the container. yt-dlp ships
    # frequent updates to stay ahead of YouTube's anti-bot moves; an old
    # version is the most common cause of "every probe rate-limits" or
    # "extractor failed" surprises that aren't motif's fault.
    try:
        import yt_dlp.version as _yt_dlp_ver
        log.info("  yt_dlp       = %s", _yt_dlp_ver.__version__)
    except Exception:
        log.info("  yt_dlp       = (not importable)")
    # v1.15.100: surface user-supplied path translations (MOTIF_PATH_TRANSLATIONS
    # env var) at startup so operators can verify they're wired up. Without
    # this, a typo in the env var would silently leave plex_enum's
    # `_candidate_local_paths` returning indeterminate (None) on every
    # folder for non-standard Unraid mount layouts — the latent Phantom-M
    # class the v1.15.90/.91/.92 fixes mitigate symptomatically.
    from .core.plex_enum import _parse_env_path_translations
    _user_translations = _parse_env_path_translations()
    if _user_translations:
        log.info("  path translations (user): %d pair(s) from MOTIF_PATH_TRANSLATIONS",
                 len(_user_translations))
        for host, container in _user_translations:
            log.info("    %s → %s", host, container)

    # Surface env overrides so the operator knows what's locked
    overrides = settings.env_overrides()
    if overrides:
        log.info("  env overrides active: %d settings (will not be editable from UI)",
                 len(overrides))
        for path, var in overrides.items():
            log.debug("    %s ← %s", path, var)

    # v1.13.8: log non-themes_dir validation errors at startup so a
    # hand-edited motif.yaml with an out-of-range value (e.g.,
    # sync.source="garbage") surfaces immediately instead of failing
    # opaquely on the next sync. themes_dir is intentionally
    # excluded — first-run setup hasn't asked the user for a path
    # yet, so missing is expected.
    config_errors = settings.validate_current(require_themes_dir=False)
    config_errors = [e for e in config_errors if "themes_dir" not in e]
    if config_errors:
        log.warning("config validation surfaced %d issue(s):",
                    len(config_errors))
        for e in config_errors:
            log.warning("  · %s", e)

    init_db(settings.db_path)
    # v1.21.0: the recovery_v55 boot walkers are RETIRED from the
    # startup path. They were one-shot repairs for historical bugs
    # (v1.18.0 FK-cascade data loss, v1.18.10 override wipe, the
    # adopt / placement / sha256 / linkage backfills); on every
    # current install their runtime_settings markers are set, so they
    # ran once long ago and have been no-op skips ever since — 16 INFO
    # "marker already set" lines per boot (the user's noise complaint).
    # The walkers REMAIN in app/core/recovery_v55.py (callable +
    # tested) as an archived safety net; they are simply no longer
    # auto-invoked. Re-wire a specific walker here if a historical bug
    # class is ever re-introduced. Full file deletion deferred to a
    # real v1.21.x .99 line-close.
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

    # v1.24.34: one-shot edition-coverage backfill (re-wired walker — the
    # v1.21.0 retirement note says re-wire a specific one when a historical bug
    # class re-surfaces, and this one does: pre-edition-awareness '' canonicals
    # block the v1.24.28-29 RP push/auto-recovery for tagged editions, the Two
    # Towers 409). Marker-gated one-shot, so it runs once + respects a later
    # deliberate UNMANAGE. Defensive per class-9 — a failure must not block boot.
    try:
        from .core.recovery_v55 import maybe_backfill_edition_canonicals
        maybe_backfill_edition_canonicals(settings.db_path)
    except Exception as e:
        log.warning("edition-coverage backfill skipped: %s", e)

    # v0.50.90: auto-resolve imdb-bearing orphans on boot when a TMDB key is
    # present. Orphans minted before a key was configured (or before the key
    # resolved) otherwise stay "tmdb: orphan" forever — the de-orphan walker
    # was manual-only (a hidden admin POST). Fire-and-forget on a daemon
    # thread so boot isn't blocked by the TMDB round-trips; the walker is
    # idempotent (re-keyed rows drop out of its selection) and no-ops fast
    # when there's nothing to resolve. Defensive per class-9 — never block
    # boot on it.
    try:
        from .core.deorphan import resolve_orphans_in_background
        if settings.tmdb_api_key:
            resolve_orphans_in_background(
                settings.db_path, api_key=settings.tmdb_api_key,
                trigger="boot")
    except Exception as e:
        log.warning("deorphan boot resolution skipped: %s", e)

    # v1.12.113: zombie running-job sweep. If a previous motif process
    # died mid-job (SIGKILL, OOM, container crash), jobs.status='running'
    # rows survive — but the worker thread that owned them is gone. Pre-fix
    # those rows lingered indefinitely; the v1.12.109 ops mini-bar would
    # render "1 running, N queued" forever even though nothing was actually
    # running. Worker can't be holding any 'running' row at this point in
    # startup (it hasn't started yet), so flipping every running row to
    # 'failed' is safe and clears the ghost.
    try:
        from .core.events import now_iso  # noqa: F811 — defensive re-import
        with get_conn(settings.db_path) as conn:
            cur = conn.execute(
                # v1.12.123: SQL fix — adjacent string literals don't
                # concatenate inside a triple-quoted Python string, so
                # the prior 'session_expired: motif process restarted '
                # 'while this job was running' became two consecutive
                # SQL string literals (invalid syntax). The zombie sweep
                # silently skipped on every startup since v1.12.113,
                # leaving 'running' jobs stuck in the table — which kept
                # themerrdb_sync_in_flight non-zero and stuck the
                # dashboard SYNC button on // SYNCING THEMERRDB… until
                # a manual DB poke. Single-line literal here.
                "UPDATE jobs SET status = 'failed', "
                "                finished_at = ?, "
                "                last_error = COALESCE(last_error, "
                "  'session_expired: motif process restarted while this job was running') "
                "WHERE status = 'running'",
                (now_iso(),),
            )
            if cur.rowcount:
                log.info(
                    "Reset %d zombie running job(s) to failed "
                    "(prior motif process died mid-job)",
                    cur.rowcount,
                )
                # v1.17.4: worker_restarted notification fires only
                # when the boot-time zombie sweep found stuck-running
                # rows — the signal "previous shutdown was unclean."
                # A clean shutdown leaves no zombies; cur.rowcount==0
                # means routine motif restart, no ping needed.
                #
                # No dedupe needed here — a zombie sweep that actually
                # FOUND something is rare (SIGKILL, OOM, container
                # crash) and the operator wants to know about each one.
                try:
                    from .core import notify as _notify
                    _notify.dispatch(
                        settings.db_path,
                        settings.cfg.notifications,
                        event_kind="worker_restarted",
                        # v1.17.19: ⚠️ emoji prefix + drop "motif:".
                        title="⚠️ Worker restarted (unclean shutdown)",
                        body=(
                            f"Reset {cur.rowcount} zombie running job(s) at "
                            f"startup — the previous motif process died "
                            f"mid-job (SIGKILL, OOM, or container crash). "
                            f"The affected jobs were marked failed with "
                            f"`session_expired` and will retry per the "
                            f"backoff schedule."
                        ),
                    )
                except Exception as e:
                    log.debug(
                        "notify dispatch (worker_restarted) suppressed: %s",
                        e)
    except Exception as e:
        log.warning("Zombie running-job sweep skipped: %s", e)

    # v1.21.47: stale op_progress sweep — the missing THIRD boot
    # reconciliation (jobs above, sync_runs in worker.py). op_progress
    # rows drive the LIVE OPS panel; a process that died mid-op (e.g. a
    # tdb_sync killed by a mid-sync redeploy) leaves a phantom
    # 'running'/'cancelling' row that the panel renders forever — ELAPSED
    # grows as wall-clock, stage_current frozen — and neither // CANCEL
    # nor a restart can clear it (no live thread owns it). It also blocks
    # try_acquire from starting a fresh op of the same id. Safe here: no
    # worker thread has started, so nothing is legitimately running.
    try:
        from .core import progress as _progress
        n_stale = _progress.reset_stale_on_boot(settings.db_path)
        if n_stale:
            log.info(
                "Marked %d stale op_progress row(s) failed at startup "
                "(prior motif process died mid-op)",
                n_stale,
            )
    except Exception as e:
        log.warning("Stale op_progress sweep skipped: %s", e)

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

        # v1.14.97: one-shot rename sweep for the v1.14.94 colon-
        # handling fix. the user: "1.14.94 folder name but can we make
        # it so it updates existing?" — pre-fix only NEW downloads
        # got the corrected `Title - Subtitle (year)` form; existing
        # folders kept the legacy `Title- Subtitle (year)` shape.
        # The migration targets exactly the v1.14.94 signature
        # (folder matches the pre-v1.14.94 sanitizer's output for
        # the row's title) so unrelated title drift doesn't trigger
        # spurious renames. Idempotent + cheap (single SQL + only
        # walks rows whose canonical actually changed).
        try:
            from .core.sections import migrate_v1_14_94_colon_folders
            colon_stats = migrate_v1_14_94_colon_folders(
                settings.db_path, settings.themes_dir,
            )
            if (colon_stats["renamed"] or colon_stats["db_repaired"]
                or colon_stats["conflict_skipped"]):
                log.info(
                    "v1.14.94 colon-folder migration: %d renamed, "
                    "%d DB pointers repaired, %d skipped (conflict), "
                    "%d skipped (canonical missing), %d skipped (drift)",
                    colon_stats["renamed"],
                    colon_stats["db_repaired"],
                    colon_stats["conflict_skipped"],
                    colon_stats["canonical_missing"],
                    colon_stats["drift_skipped"],
                )
        except Exception as e:
            log.warning(
                "v1.14.94 colon-folder migration failed at startup: %s", e,
            )

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
        # v1.17.9: log on scheduler.shutdown failure. A swallowed
        # exception here leaves apscheduler's executor threads alive
        # through the rest of teardown — the next "motif shutting
        # down" log line is then a lie. Hygiene audit (class 9):
        # bare `except: pass` made shutdown hangs indistinguishable
        # from clean exits in postmortem.
        try:
            scheduler.shutdown(wait=False)
        except Exception as e:  # noqa: BLE001
            log.warning(
                "scheduler.shutdown() raised — apscheduler threads "
                "may still be alive: %s", e,
            )
        # v1.20.63: drain any pending coalesced notification tails
        # before exit — the trailing-window timers are daemon threads
        # killed mid-batch otherwise, silently dropping a bulk place's
        # summary if the redeploy lands inside the 120s window
        # (class-9 silent loss).
        try:
            from .core import notify as _notify
            _notify.flush_all_coalesced()
        except Exception as e:  # noqa: BLE001
            log.warning("notify.flush_all_coalesced() raised: %s", e)
        log_event(settings.db_path, level="INFO", component="main",
                  message="motif shutting down")
        # Uvicorn will receive its own signal handler; it'll stop the server
        # When uvicorn exits we proceed to thread-join below
    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    # Build FastAPI app
    app = create_app(settings)

    # v1.24.21: expose the worker threads + scheduler to the /healthz handler
    # so the Docker healthcheck reflects real liveness (DB + worker + scheduler),
    # not an unconditional 200. The handler reads these defensively via getattr,
    # so a TestClient app (no main.py wiring) just reports the DB check.
    app.state.worker_threads = worker_threads
    app.state.scheduler = scheduler

    # Run uvicorn in foreground — it handles its own signal trapping for the HTTP
    # server, and our custom handlers above will fire for the worker/scheduler.
    config = uvicorn.Config(
        app,
        host=settings.web_host,
        port=settings.web_port,
        log_config=None,        # we configured logging already
        access_log=False,
        proxy_headers=settings.trust_forward_auth,
        # v0.51.76 (security audit — XFF-spoof): honor X-Forwarded-For only from the
        # configured trusted proxy IP(s), not "*". With "*", uvicorn overwrites
        # request.client.host with the attacker-controlled leftmost XFF token, so a
        # direct-to-:5309 attacker can spoof an allowlisted IP + X-Authentik-Username
        # and bypass forward-auth. Empty (default) keeps the historical "*" for
        # backward-compat; set web.forward_auth_trusted_proxies to NPM's container IP
        # to close it (a direct attacker's real peer IP then fails the allowlist).
        forwarded_allow_ips=(
            (",".join(settings.forward_auth_trusted_proxies) or "*")
            if settings.trust_forward_auth else None
        ),
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
