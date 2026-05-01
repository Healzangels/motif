"""
Background worker.

Consumes from the `jobs` table and dispatches to the right handler:
- 'sync': run a full ThemerrDB sync
- 'download': fetch the theme MP3 to the local library
- 'place': place theme files into Plex media folders + trigger refresh
- 'refresh': trigger Plex analyze for an item (rare, manual)
- 'relink': retry hardlinking for placements that fell back to copy

A simple in-process token-bucket rate limiter caps download throughput.
Worker holds no long-lived DB connection — all writes are short transactions.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from ..config import Settings
from .canonical import canonical_theme_subdir
from .db import get_conn, transaction
from .downloader import (
    CookiesMissingError, DownloadError, FailureKind, download_theme,
)
from .events import log_event, now_iso
from .placement import FolderIndex, place_theme
from .plex import PlexClient, PlexConfig
from .runtime import is_dry_run
from .sync import run_sync

log = logging.getLogger(__name__)


class _JobPermanentFailure(Exception):
    """v1.10.40: raised by job handlers when retry won't fix it
    (e.g. yt-dlp says the video is removed). The worker treats
    this as terminal — status='failed' immediately, no backoff
    schedule, no further attempts. The user is expected to
    provide a manual override or accept the failure."""


# -------- Token bucket --------

class TokenBucket:
    """Thread-safe token bucket. `rate` tokens are added per `period` seconds,
    capped at `capacity`. acquire() blocks until a token is available."""

    def __init__(self, rate: float, capacity: int, period: float = 3600.0):
        self._capacity = capacity
        # tokens added per second
        self._fill_rate = rate / period
        self._tokens = float(capacity)
        self._last = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self, tokens: int = 1) -> None:
        while True:
            with self._lock:
                now = time.monotonic()
                elapsed = now - self._last
                self._tokens = min(self._capacity, self._tokens + elapsed * self._fill_rate)
                self._last = now
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return
                deficit = tokens - self._tokens
                wait = deficit / self._fill_rate
            time.sleep(min(wait, 30.0))


# -------- Job dispatching --------

@dataclass
class Worker:
    settings: Settings
    stop_event: threading.Event
    bucket: TokenBucket
    _folder_index_movies: FolderIndex | None = None
    _folder_index_tv: FolderIndex | None = None
    _index_built_at: float = 0.0

    def __post_init__(self):
        # Stale after 5 minutes — rebuilt on demand
        self._index_ttl = 300.0

    # -- Index management --

    def _index_for(self, media_type: str) -> FolderIndex:
        """Build (or reuse) a FolderIndex for the given media type by
        unioning all managed Plex sections of the matching type.

        Movie sections (Plex type='movie') feed the movie index; show
        sections feed the TV index. The user's "Anime" section, being type
        'show', falls into TV — so an anime show's theme lands in the
        anime section's path, not in the regular TV root."""
        now = time.monotonic()
        if now - self._index_built_at > self._index_ttl:
            from .sections import list_sections  # avoid import cycle at top
            sections = list_sections(self.settings.db_path)
            movie_roots: list[Path] = []
            show_roots: list[Path] = []
            for s in sections:
                if not s.get("included"):
                    continue
                paths = s.get("location_paths") or []
                for p in paths:
                    pp = Path(p)
                    if s["type"] == "movie":
                        movie_roots.append(pp)
                    elif s["type"] == "show":
                        show_roots.append(pp)
            self._folder_index_movies = FolderIndex.build_multi(
                movie_roots, plus_mode=self.settings.plus_equiv_mode,  # type: ignore[arg-type]
            )
            self._folder_index_tv = FolderIndex.build_multi(
                show_roots, plus_mode=self.settings.plus_equiv_mode,  # type: ignore[arg-type]
            )
            self._index_built_at = now
        return self._folder_index_movies if media_type == "movie" else self._folder_index_tv  # type: ignore[return-value]

    def _plex_client(self) -> PlexClient | None:
        if not self.settings.plex_enabled or not self.settings.plex_url or not self.settings.plex_token:
            return None
        cfg = PlexConfig(
            url=self.settings.plex_url,
            token=self.settings.plex_token,
            movie_section=self.settings.plex_movie_section,
            tv_section=self.settings.plex_tv_section,
            enabled=True,
        )
        return PlexClient(cfg, plus_mode=self.settings.plus_equiv_mode)  # type: ignore[arg-type]

    # -- Loop --

    def run(self) -> None:
        log.info("Worker loop starting")
        while not self.stop_event.is_set():
            job = self._claim_next_job()
            if job is None:
                # No work — back off briefly
                self.stop_event.wait(2.0)
                continue
            try:
                self._dispatch(job)
            except _JobPermanentFailure as e:
                # v1.10.40: handler classified the error as permanent
                # (e.g. YouTube video removed). Skip retry — mark
                # the job 'failed' immediately. The themes row's
                # failure_kind is already persisted by the handler.
                log.info("Job %s permanently failed: %s", job["id"], e)
                self._mark_failed_terminal(job["id"], str(e))
            except Exception as e:
                log.exception("Job %s crashed: %s", job["id"], e)
                self._mark_failed(job["id"], str(e))
        log.info("Worker loop stopped")

    def _claim_next_job(self) -> sqlite3.Row | None:
        with get_conn(self.settings.db_path) as conn, transaction(conn):
            # v1.10.59: probe jobs are explicitly deprioritized so a
            # large sync-fed probe backlog can never starve real work
            # (downloads/place/refresh/etc.). next_run_at gating still
            # applies, but among due jobs non-probes always win, then
            # ties break by id (FIFO).
            row = conn.execute(
                """
                SELECT * FROM jobs
                WHERE status = 'pending'
                  AND (next_run_at IS NULL OR next_run_at <= ?)
                ORDER BY CASE job_type WHEN 'probe' THEN 1 ELSE 0 END,
                         id ASC
                LIMIT 1
                """,
                (now_iso(),),
            ).fetchone()
            if row is None:
                return None
            conn.execute(
                """
                UPDATE jobs SET status = 'running', started_at = ?, attempts = attempts + 1
                WHERE id = ?
                """,
                (now_iso(), row["id"]),
            )
            return row

    def _mark_done(self, job_id: int) -> None:
        with get_conn(self.settings.db_path) as conn:
            conn.execute(
                "UPDATE jobs SET status = 'done', finished_at = ?, last_error = NULL WHERE id = ?",
                (now_iso(), job_id),
            )

    def _mark_failed_terminal(self, job_id: int, err: str) -> None:
        """v1.10.40: mark a job 'failed' immediately, bypassing the
        attempts-vs-max_attempts retry logic. Used for permanent
        failures where retrying is pointless (video removed/private/
        geo-blocked/age-restricted) — the user has to provide a
        manual override URL or accept the failure.
        """
        with get_conn(self.settings.db_path) as conn:
            conn.execute(
                "UPDATE jobs SET status = 'failed', finished_at = ?, "
                "                last_error = ? "
                "WHERE id = ?",
                (now_iso(), err, job_id),
            )

    def _mark_failed(self, job_id: int, err: str) -> None:
        with get_conn(self.settings.db_path) as conn:
            row = conn.execute(
                "SELECT attempts, max_attempts FROM jobs WHERE id = ?", (job_id,),
            ).fetchone()
            if row is None:
                return
            if row["attempts"] >= row["max_attempts"]:
                conn.execute(
                    "UPDATE jobs SET status = 'failed', finished_at = ?, last_error = ? WHERE id = ?",
                    (now_iso(), err, job_id),
                )
            else:
                # Exponential backoff: 1m, 5m, 25m, ...
                backoff_minutes = 5 ** (row["attempts"] - 1)
                next_run = (datetime.now(timezone.utc) + timedelta(minutes=backoff_minutes)).isoformat(timespec="seconds")
                conn.execute(
                    """
                    UPDATE jobs SET status = 'pending', last_error = ?, next_run_at = ?,
                                    started_at = NULL
                    WHERE id = ?
                    """,
                    (err, next_run, job_id),
                )

    def _dispatch(self, job: sqlite3.Row) -> None:
        jt = job["job_type"]
        log.info("Job %d (%s) starting", job["id"], jt)
        if jt == "sync":
            self._do_sync(job)
        elif jt == "download":
            self._do_download(job)
        elif jt == "place":
            self._do_place(job)
        elif jt == "refresh":
            self._do_refresh(job)
        elif jt == "relink":
            self._do_relink(job)
        elif jt == "scan":
            self._do_scan(job)
        elif jt == "adopt":
            self._do_adopt(job)
        elif jt == "plex_enum":
            self._do_plex_enum(job)
        elif jt == "probe":
            self._do_probe(job)
        else:
            self._mark_failed(job["id"], f"unknown job type: {jt}")
            return
        self._mark_done(job["id"])

    # -- Job handlers --

    def _do_plex_enum(self, job: sqlite3.Row) -> None:
        """Re-enumerate Plex library items.

        Payload may carry {"section_id": "..."} to scope to one section
        (per-section REFRESH from /settings#plex). Default: every managed
        section.
        """
        from .plex_enum import run_plex_enum
        from .plex import PlexConfig
        if not (self.settings.plex_enabled and self.settings.plex_url
                and self.settings.plex_token):
            log_event(self.settings.db_path, level="WARNING", component="plex_enum",
                      message="Skipped: Plex not configured")
            return
        only_section: str | None = None
        try:
            payload = json.loads(job["payload"] or "{}")
            if isinstance(payload, dict) and payload.get("section_id"):
                only_section = str(payload["section_id"])
        except (TypeError, ValueError):
            pass
        cfg = PlexConfig(
            url=self.settings.plex_url,
            token=self.settings.plex_token,
            movie_section=self.settings.plex_movie_section,
            tv_section=self.settings.plex_tv_section,
        )
        run_plex_enum(self.settings.db_path, cfg, only_section_id=only_section)

    def _do_sync(self, job: sqlite3.Row) -> None:
        # Optional payload overrides:
        #   {"auto_place": true|false} — per-sync override of the global
        #     placement.auto_place setting for downloads enqueued by this run.
        #   {"enqueue_downloads": false} — metadata-only sync; do NOT enqueue
        #     any download jobs at all (downloads are user-initiated from the
        #     /movies, /tv, or /coverage pages).
        auto_place_override: bool | None = None
        enqueue_downloads = True
        try:
            payload = json.loads(job["payload"] or "{}")
            if "auto_place" in payload:
                auto_place_override = bool(payload["auto_place"])
            if payload.get("enqueue_downloads") is False:
                enqueue_downloads = False
        except (TypeError, ValueError):
            pass
        run_sync(
            self.settings.db_path, self.settings.motifdb_base_url,
            auto_place_override=auto_place_override,
            enqueue_downloads=enqueue_downloads,
        )
        # Auto-enqueue a plex_enum after every sync so the unified browse view
        # stays current. Dedupe so concurrent syncs don't pile up.
        with get_conn(self.settings.db_path) as conn:
            existing = conn.execute(
                "SELECT 1 FROM jobs WHERE job_type = 'plex_enum' "
                "AND status IN ('pending','running')"
            ).fetchone()
            if not existing:
                conn.execute(
                    "INSERT INTO jobs (job_type, payload, status, created_at, next_run_at) "
                    "VALUES ('plex_enum', '{}', 'pending', ?, ?)",
                    (now_iso(), now_iso()),
                )

    def _do_scan(self, job: sqlite3.Row) -> None:
        """Run a Plex folder scan. Payload optionally carries
        {'initiated_by': 'username'}."""
        from .scanner import run_scan
        if not self.settings.is_paths_ready():
            log_event(self.settings.db_path, level="WARNING",
                      component="scan",
                      message="Skipped: themes_dir not configured.")
            raise RuntimeError("themes_dir not configured")

        payload = {}
        if job["payload"]:
            try:
                payload = json.loads(job["payload"])
            except Exception:
                pass
        initiated_by = payload.get("initiated_by", "system")
        run_scan(
            self.settings.db_path, self.settings,
            initiated_by=initiated_by,
            cancel_check=self.stop_event.is_set,
        )

    def _do_adopt(self, job: sqlite3.Row) -> None:
        """Apply a per-finding adoption decision. Payload carries
        {'finding_id': N, 'decision': 'adopt'|..., 'decided_by': 'username'}."""
        from .adopt import adopt_finding
        if not self.settings.is_paths_ready():
            log_event(self.settings.db_path, level="WARNING",
                      component="adopt",
                      message="Skipped: themes_dir not configured.")
            raise RuntimeError("themes_dir not configured")

        if not job["payload"]:
            raise RuntimeError("adopt job missing payload")
        payload = json.loads(job["payload"])
        adopt_finding(
            self.settings.db_path,
            finding_id=int(payload["finding_id"]),
            decision=str(payload["decision"]),
            decided_by=payload.get("decided_by", "system"),
            settings=self.settings,
        )

    def _do_download(self, job: sqlite3.Row) -> None:
        media_type = job["media_type"]
        tmdb_id = job["tmdb_id"]

        # Bail early if themes_dir hasn't been configured yet (first-run
        # state). The job stays pending and gets retried; once the user
        # sets the path in /settings, downloads will pick up.
        if not self.settings.is_paths_ready():
            log_event(
                self.settings.db_path, level="WARNING", component="download",
                media_type=media_type, tmdb_id=tmdb_id,
                message="Skipped: themes_dir not configured. Set it on /settings.",
            )
            raise RuntimeError("themes_dir not configured")

        # Look up theme + any user override
        with get_conn(self.settings.db_path) as conn:
            theme = conn.execute(
                "SELECT * FROM themes WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            ).fetchone()
            override = conn.execute(
                "SELECT * FROM user_overrides WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            ).fetchone()

        if theme is None:
            raise RuntimeError(f"theme not in DB: {media_type}/{tmdb_id}")

        yt_url = override["youtube_url"] if override else theme["youtube_url"]
        if not yt_url:
            # v1.10.56: missing URL = nothing to retry. Pre-1.10.56 this
            # raised RuntimeError which went through the retry-with-
            # backoff path (5 → 25 → 125 minutes) — leaving the row's
            # spinner stuck and other actions disabled for ~2 hours
            # before the job finally settled to 'failed'. Permanent-
            # failure short-circuits to status='failed' immediately.
            raise _JobPermanentFailure(
                "no YouTube URL configured for this theme — "
                "SET URL or UPLOAD MP3 to provide a source"
            )

        # Re-extract video ID in case override URL is in a different format
        from .sync import extract_video_id
        vid = extract_video_id(yt_url)
        if not vid:
            raise _JobPermanentFailure(
                f"could not extract YouTube video ID from {yt_url}"
            )

        # Dry-run: log what we would do and stop here (no YouTube hit, no rate-limit token consumed)
        if is_dry_run(self.settings.db_path, default=self.settings.dry_run_default):
            preview_dir = ((self.settings.movies_themes_dir if media_type == "movie"
                            else self.settings.tv_themes_dir)
                           / canonical_theme_subdir(theme["title"], theme["year"]))
            log_event(
                self.settings.db_path, level="INFO", component="dryrun",
                media_type=media_type, tmdb_id=tmdb_id,
                message=f"DRY-RUN: would download '{theme['title']}' from {yt_url}",
                detail={"video_id": vid, "url": yt_url,
                        "would_save_to": str(preview_dir / "theme.mp3")},
            )
            return  # job marked done by caller

        # Apply rate limit
        self.bucket.acquire()

        media_root = (self.settings.movies_themes_dir if media_type == "movie"
                      else self.settings.tv_themes_dir)
        out_dir = media_root / canonical_theme_subdir(theme["title"], theme["year"])

        # If a previously downloaded theme.mp3 was for a different video (e.g.
        # ThemerrDB URL changed since last sync), unlink the stale one so the
        # downloader's "already exists" short-circuit doesn't keep us pinned
        # to the old audio.
        existing_local = None
        with get_conn(self.settings.db_path) as conn:
            existing_local = conn.execute(
                "SELECT source_video_id FROM local_files "
                "WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            ).fetchone()
        target_mp3 = out_dir / "theme.mp3"
        if (target_mp3.exists() and existing_local
                and existing_local["source_video_id"] != vid):
            try:
                target_mp3.unlink()
            except OSError as e:
                log.warning("Could not invalidate stale theme.mp3 %s: %s", target_mp3, e)

        try:
            result = download_theme(
                youtube_url=yt_url,
                video_id=vid,
                output_dir=out_dir,
                cookies_file=self.settings.cookies_file,
                audio_quality=self.settings.download_audio_quality,
            )
        except DownloadError as e:
            kind = e.kind if hasattr(e, "kind") else FailureKind.UNKNOWN
            # Persist the failure on the theme row so the UI can show a badge.
            # v1.10.50: when the new failure_kind differs from what was
            # previously acked, clear failure_acked_at so the user gets
            # alerted on the new condition. Same kind as before keeps
            # the existing ack (no spam).
            with get_conn(self.settings.db_path) as conn:
                conn.execute(
                    """UPDATE themes SET failure_kind = ?, failure_message = ?,
                                          failure_at = ?,
                                          failure_acked_at = CASE
                                              WHEN failure_kind = ? THEN failure_acked_at
                                              ELSE NULL
                                          END
                       WHERE media_type = ? AND tmdb_id = ?""",
                    (kind.value, str(e)[:500], now_iso(),
                     kind.value, media_type, tmdb_id),
                )
                # v1.10.40: also cancel any pending follow-up place job
                # we might have already enqueued (e.g. force_place=true
                # from REPLACE w/ TDB) — without the file, place can't
                # do anything useful.
                conn.execute(
                    """UPDATE jobs SET status = 'cancelled', finished_at = ?
                       WHERE job_type = 'place' AND media_type = ? AND tmdb_id = ?
                         AND status IN ('pending','running')""",
                    (now_iso(), media_type, tmdb_id),
                )
            level = "ERROR" if kind == FailureKind.COOKIES_EXPIRED else "WARNING"
            log_event(
                self.settings.db_path, level=level, component="download",
                media_type=media_type, tmdb_id=tmdb_id,
                message=f"Download failed for '{theme['title']}': {kind.human}",
                detail={"kind": kind.value, "youtube_url": yt_url,
                        "needs_manual_override": kind.needs_manual_override,
                        "raw": str(e)[:300]},
            )
            # v1.10.40: video-side permanent failures (removed/private/
            # age-restricted/geo-blocked) — retry won't help. Bypass
            # the worker's backoff schedule and mark the job 'failed'
            # immediately by raising _JobPermanentFailure. Transient
            # kinds (network/cookies/unknown) keep the original
            # retry-with-backoff behavior via plain `raise`.
            if kind.needs_manual_override:
                raise _JobPermanentFailure(str(e)) from e
            raise

        # Success — clear any previous failure on this theme.
        # v1.10.50: also drop failure_acked_at since the row is no
        # longer failing at all. A future re-failure starts fresh
        # (alerts again).
        with get_conn(self.settings.db_path) as conn:
            conn.execute(
                """UPDATE themes SET failure_kind = NULL, failure_message = NULL,
                                      failure_at = NULL, failure_acked_at = NULL
                   WHERE media_type = ? AND tmdb_id = ? AND failure_kind IS NOT NULL""",
                (media_type, tmdb_id),
            )

        # Record the local file
        rel_path = str(result.file_path.relative_to(self.settings.themes_dir))
        provenance = "manual" if override else "auto"
        # source_kind drives the T/U/A badge (v1.10.12). 'url' when the
        # download was driven by a user_overrides row; 'themerrdb'
        # otherwise.
        source_kind = "url" if override else "themerrdb"
        with get_conn(self.settings.db_path) as conn, transaction(conn):
            conn.execute(
                """
                INSERT INTO local_files
                    (media_type, tmdb_id, file_path, file_sha256, file_size,
                     downloaded_at, source_video_id, provenance, source_kind)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(media_type, tmdb_id) DO UPDATE SET
                    file_path = excluded.file_path,
                    file_sha256 = excluded.file_sha256,
                    file_size = excluded.file_size,
                    downloaded_at = excluded.downloaded_at,
                    source_video_id = excluded.source_video_id,
                    provenance = excluded.provenance,
                    source_kind = excluded.source_kind
                """,
                (media_type, tmdb_id, rel_path, result.file_sha256,
                 result.file_size, now_iso(), vid, provenance, source_kind),
            )
            # Decide whether to auto-place. Per-job payload override wins;
            # otherwise fall back to the global placement.auto_place setting.
            # When auto_place is False, the staged file lands in /pending and
            # the user approves placement manually.
            #
            # v1.10.9: payload may also carry force_place=true (set by the
            # REPLACE-WITH-THEMERRDB row action in the unified library
            # browse). Propagates into the place job's payload as force=true
            # so place_theme overwrites any existing sidecar without the
            # plex_has_theme guard kicking in.
            auto_place = self.settings.auto_place_default
            force_place = False
            try:
                payload = json.loads(job["payload"] or "{}")
                if "auto_place" in payload:
                    auto_place = bool(payload["auto_place"])
                if payload.get("force_place"):
                    force_place = True
                    auto_place = True  # force implies auto
            except (TypeError, ValueError):
                pass
            if auto_place:
                place_payload = '{"force":true,"reason":"replace_with_themerrdb"}' if force_place else "{}"
                conn.execute(
                    """
                    INSERT INTO jobs (job_type, media_type, tmdb_id, payload, status,
                                      created_at, next_run_at)
                    VALUES ('place', ?, ?, ?, 'pending', ?, ?)
                    """,
                    (media_type, tmdb_id, place_payload, now_iso(), now_iso()),
                )

        log_event(
            self.settings.db_path, level="INFO", component="download",
            media_type=media_type, tmdb_id=tmdb_id,
            message=f"Downloaded theme for {theme['title']}",
            detail={"size": result.file_size, "video_id": vid},
        )

    def _do_place(self, job: sqlite3.Row) -> None:
        media_type = job["media_type"]
        tmdb_id = job["tmdb_id"]

        if not self.settings.is_paths_ready():
            log_event(
                self.settings.db_path, level="WARNING", component="place",
                media_type=media_type, tmdb_id=tmdb_id,
                message="Skipped: themes_dir not configured. Set it on /settings.",
            )
            raise RuntimeError("themes_dir not configured")

        with get_conn(self.settings.db_path) as conn:
            theme = conn.execute(
                "SELECT * FROM themes WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            ).fetchone()
            local = conn.execute(
                "SELECT * FROM local_files WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            ).fetchone()

        # In dry-run, the local file may not exist yet (download was skipped).
        # We can still report what placement *would* have done by running the
        # folder-index lookup without touching anything.
        dry = is_dry_run(self.settings.db_path, default=self.settings.dry_run_default)

        if theme is None:
            raise RuntimeError("theme missing")

        if dry:
            # Just resolve the target folder and Plex theme presence, log it
            from .placement import find_target_folder
            index = self._index_for(media_type)
            find = find_target_folder(
                index, title=theme["title"], year=theme["year"] or "",
                edition_raw="",
                strict_edition=self.settings.strict_edition_match,
                plus_mode=self.settings.plus_equiv_mode,  # type: ignore[arg-type]
            )
            plex_has = False
            if self.settings.plex_enabled and self.settings.plex_token:
                plex_client = self._plex_client()
                if plex_client:
                    try:
                        plex_has = plex_client.has_theme(
                            media_type=media_type, title=theme["title"],
                            year=theme["year"] or "", edition_raw="",
                        )
                    finally:
                        plex_client.close()

            if plex_has:
                msg = f"DRY-RUN: would skip '{theme['title']}' — Plex already has theme"
            elif find.folder is None:
                msg = f"DRY-RUN: no target folder for '{theme['title']}': {find.reason}"
            else:
                msg = f"DRY-RUN: would place theme for '{theme['title']}' at {find.folder}/theme.mp3"
            log_event(
                self.settings.db_path, level="INFO", component="dryrun",
                media_type=media_type, tmdb_id=tmdb_id,
                message=msg,
                detail={
                    "target_folder": str(find.folder) if find.folder else None,
                    "match_kind": find.kind,
                    "plex_already_has_theme": plex_has,
                },
            )
            return

        if local is None:
            raise RuntimeError("local file missing (download not yet complete)")

        source_file = self.settings.themes_dir / local["file_path"]
        if not source_file.exists():
            raise RuntimeError(f"source file missing: {source_file}")

        # Optional payload override: {"force": true} bypasses the
        # plex_has_theme skip. /pending APPROVE writes this so the user's
        # explicit "yes, replace the existing theme" actually does that.
        force_overwrite = False
        try:
            payload = json.loads(job["payload"] or "{}")
            if isinstance(payload, dict) and payload.get("force"):
                force_overwrite = True
        except (TypeError, ValueError):
            pass

        index = self._index_for(media_type)
        plex_client = self._plex_client()
        try:
            outcome = place_theme(
                media_type=media_type,
                title=theme["title"],
                year=theme["year"] or "",
                edition_raw="",  # TODO: future: per-edition placement
                source_file=source_file,
                index=index,
                plex=plex_client,
                strict_edition=self.settings.strict_edition_match,
                plus_mode=self.settings.plus_equiv_mode,  # type: ignore[arg-type]
                skip_if_plex_has_theme=not force_overwrite,
                force_overwrite=force_overwrite,
                analyze_after=self.settings.plex_analyze_after,
            )
        finally:
            if plex_client:
                plex_client.close()

        if outcome.placed:
            # Read provenance from the local_files row that was the source
            with get_conn(self.settings.db_path) as conn:
                lf_row = conn.execute(
                    "SELECT provenance FROM local_files WHERE media_type = ? AND tmdb_id = ?",
                    (media_type, tmdb_id),
                ).fetchone()
                placement_provenance = lf_row["provenance"] if lf_row else "auto"
                conn.execute(
                    """
                    INSERT INTO placements
                        (media_type, tmdb_id, media_folder, placed_at,
                         placement_kind, plex_rating_key, plex_refreshed, provenance)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(media_type, tmdb_id, media_folder) DO UPDATE SET
                        placed_at = excluded.placed_at,
                        placement_kind = excluded.placement_kind,
                        plex_rating_key = excluded.plex_rating_key,
                        plex_refreshed = excluded.plex_refreshed,
                        provenance = excluded.provenance
                    """,
                    (media_type, tmdb_id, str(outcome.target_folder), now_iso(),
                     outcome.kind, outcome.plex_rating_key,
                     1 if outcome.plex_refreshed else 0, placement_provenance),
                )
                # v1.10.46: Plex's first refresh sometimes lands before
                # the filesystem watcher / local-media-assets agent has
                # re-stat'd the folder, so the new theme.mp3 isn't
                # picked up. The user then has to manually click
                # 'Refresh Metadata' in Plex to force the pickup.
                # Schedule a second refresh ~10s out to give Plex's
                # filesystem cache time to commit; plex.refresh is
                # cheap and idempotent on Plex's side.
                if outcome.plex_rating_key and self.settings.plex_analyze_after:
                    delayed_iso = (
                        datetime.now(timezone.utc)
                        + timedelta(seconds=10)
                    ).isoformat(timespec="seconds")
                    conn.execute(
                        """INSERT INTO jobs (job_type, media_type, tmdb_id, payload,
                                              status, created_at, next_run_at)
                           VALUES ('refresh', ?, ?, ?, 'pending', ?, ?)""",
                        (media_type, tmdb_id,
                         json.dumps({"rating_key": outcome.plex_rating_key,
                                     "reason": "post_place_double_refresh"}),
                         now_iso(), delayed_iso),
                    )
        log_event(
            self.settings.db_path,
            level="INFO" if outcome.placed else "DEBUG",
            component="place", media_type=media_type, tmdb_id=tmdb_id,
            message=(f"Placed in {outcome.target_folder}" if outcome.placed
                     else f"Skipped placement: {outcome.reason}"),
            detail={"reason": outcome.reason, "kind": outcome.kind},
        )

    def _do_refresh(self, job: sqlite3.Row) -> None:
        plex = self._plex_client()
        if not plex:
            return
        try:
            payload = json.loads(job["payload"] or "{}")
            rk = payload.get("rating_key")
            if rk:
                plex.refresh(rk)
        finally:
            plex.close()

    def _do_probe(self, job: sqlite3.Row) -> None:
        """v1.10.45: probe a single ThemerrDB youtube_url for availability
        and stamp themes.failure_kind so the library's TDB pill paints
        red/amber for dead/cookied URLs ahead of any download attempt.

        Replaces the v1.10.43 inline _probe_phase + 100-cap. Sync now
        enqueues one probe job per new + url_changed entry; the worker
        chews through them between higher-priority work (downloads/
        place/etc.). Probe jobs are deprioritized via next_run_at = +60s
        at enqueue time so claim_next_job (which orders by next_run_at)
        won't pick a probe over a download whose next_run_at = now.
        """
        from .downloader import probe_youtube_url, FailureKind  # local import
        media_type = job["media_type"]
        tmdb_id = job["tmdb_id"]
        if media_type is None or tmdb_id is None:
            return
        with get_conn(self.settings.db_path) as conn:
            theme = conn.execute(
                "SELECT youtube_url FROM themes "
                "WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            ).fetchone()
        if theme is None or not theme["youtube_url"]:
            return  # row gone or no URL — nothing to probe
        try:
            failure = probe_youtube_url(
                theme["youtube_url"],
                cookies_file=self.settings.cookies_file,
            )
        except Exception as e:
            log.debug("probe failed for %s/%s: %s", media_type, tmdb_id, e)
            return
        with get_conn(self.settings.db_path) as conn, transaction(conn):
            if failure is None:
                # URL is reachable + playable. Clear failure_kind +
                # failure_acked_at so the row's TDB pill goes green
                # and any future re-failure alerts again.
                conn.execute(
                    "UPDATE themes SET failure_kind = NULL, "
                    "                  failure_message = NULL, "
                    "                  failure_at = NULL, "
                    "                  failure_acked_at = NULL "
                    "WHERE media_type = ? AND tmdb_id = ? "
                    "  AND failure_kind IS NOT NULL",
                    (media_type, tmdb_id),
                )
            else:
                # v1.10.59: probe-detected failures are auto-acked.
                # The TDB pill (driven by failure_kind) still paints
                # red/amber so the user sees broken/cookied URLs, but
                # they don't push the topbar's failures badge or land
                # in the Failures tab — those should only flag work
                # the user actually triggered. A real download failure
                # later (with the same kind) preserves the ack via the
                # existing CASE. A different kind clears the ack so
                # the genuine new failure surfaces.
                ts = now_iso()
                conn.execute(
                    "UPDATE themes SET failure_kind = ?, "
                    "                  failure_message = ?, "
                    "                  failure_at = ?, "
                    "                  failure_acked_at = CASE "
                    "                      WHEN failure_kind = ? THEN COALESCE(failure_acked_at, ?) "
                    "                      ELSE ? "
                    "                  END "
                    "WHERE media_type = ? AND tmdb_id = ?",
                    (failure.value, f"sync probe: {failure.human}",
                     ts, failure.value, ts, ts, media_type, tmdb_id),
                )

    def _do_relink(self, job: sqlite3.Row) -> None:
        """Retry hardlinking for placements that fell back to copy.

        Walks every placement_kind='copy' row (or a specific one when
        media_type/tmdb_id are set on the job), and tries os.link() again
        from the source theme file. If the link succeeds, the existing copy
        is atomically replaced. If it still fails (still cross-FS), the
        copy is left in place.
        """
        import os
        import shutil

        if not self.settings.is_paths_ready():
            log_event(
                self.settings.db_path, level="WARNING", component="relink",
                message="Skipped: themes_dir not configured.",
            )
            return

        media_type = job["media_type"]
        tmdb_id = job["tmdb_id"]

        with get_conn(self.settings.db_path) as conn:
            if media_type and tmdb_id:
                rows = conn.execute(
                    """SELECT p.*, lf.file_path
                       FROM placements p
                       JOIN local_files lf
                         ON lf.media_type = p.media_type AND lf.tmdb_id = p.tmdb_id
                       WHERE p.media_type = ? AND p.tmdb_id = ?
                         AND p.placement_kind = 'copy'""",
                    (media_type, tmdb_id),
                ).fetchall()
            else:
                rows = conn.execute(
                    """SELECT p.*, lf.file_path
                       FROM placements p
                       JOIN local_files lf
                         ON lf.media_type = p.media_type AND lf.tmdb_id = p.tmdb_id
                       WHERE p.placement_kind = 'copy'"""
                ).fetchall()

        relinked = 0
        still_copy = 0
        errors = 0
        for r in rows:
            src = self.settings.themes_dir / r["file_path"]
            dst = Path(r["media_folder"]) / "theme.mp3"
            if not src.exists():
                errors += 1
                continue
            if not dst.exists():
                # Destination disappeared (movie folder deleted?) — clean up the row
                with get_conn(self.settings.db_path) as conn:
                    conn.execute(
                        """DELETE FROM placements
                           WHERE media_type = ? AND tmdb_id = ? AND media_folder = ?""",
                        (r["media_type"], r["tmdb_id"], r["media_folder"]),
                    )
                continue

            tmp = dst.with_suffix(dst.suffix + ".relink-tmp")
            if tmp.exists():
                try:
                    tmp.unlink()
                except OSError:
                    pass

            try:
                os.link(src, tmp)
            except OSError as e:
                # Still cross-filesystem — leave it as a copy
                still_copy += 1
                log.debug("relink still cross-FS for %s: %s", dst, e)
                continue

            # Hardlink succeeded — atomically replace the copy with the link
            try:
                os.replace(tmp, dst)
            except OSError as e:
                errors += 1
                log.warning("relink replace failed for %s: %s", dst, e)
                try: tmp.unlink()
                except OSError: pass
                continue

            with get_conn(self.settings.db_path) as conn:
                conn.execute(
                    """UPDATE placements SET placement_kind = 'hardlink', placed_at = ?
                       WHERE media_type = ? AND tmdb_id = ? AND media_folder = ?""",
                    (now_iso(), r["media_type"], r["tmdb_id"], r["media_folder"]),
                )
            relinked += 1

        log_event(
            self.settings.db_path, level="INFO", component="relink",
            message=f"Relink sweep complete: {relinked} converted, "
                    f"{still_copy} still cross-FS, {errors} errors",
            detail={"relinked": relinked, "still_copy": still_copy, "errors": errors},
        )


# -------- Entry point --------

def start_worker(settings: Settings, stop_event: threading.Event) -> threading.Thread:
    bucket = TokenBucket(
        rate=float(settings.download_rate_per_hour),
        capacity=max(1, settings.download_rate_per_hour),
        period=3600.0,
    )
    w = Worker(settings=settings, stop_event=stop_event, bucket=bucket)
    t = threading.Thread(target=w.run, name="motif-worker", daemon=True)
    t.start()
    return t
