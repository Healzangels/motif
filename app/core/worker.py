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
import os
import shutil
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


def _safe_link(src: Path, dst: Path) -> None:
    """v1.11.0: hardlink src → dst; fall back to copy if hardlink isn't
    supported (e.g. cross-filesystem). Caller has already ensured
    dst.parent exists and dst doesn't (or has been removed)."""
    try:
        os.link(src, dst)
    except OSError as e:
        log.info("Hardlink failed (%s); copying %s → %s", e, src, dst)
        shutil.copy2(src, dst)


class _JobPermanentFailure(Exception):
    """v1.10.40: raised by job handlers when retry won't fix it
    (e.g. yt-dlp says the video is removed). The worker treats
    this as terminal — status='failed' immediately, no backoff
    schedule, no further attempts. The user is expected to
    provide a manual override or accept the failure."""


class _JobCancelled(Exception):
    """v1.11.36: raised by handlers (or check_cancel) when the user
    requested cancellation via POST /api/jobs/{id}/cancel. The
    dispatch loop catches this and marks the job 'cancelled' with
    last_error='cancelled by user', no retry."""


def _is_cancelled(db_path: Path, job_id: int) -> bool:
    """Cheap point-check the cooperative cancel flag. Long-running
    job handlers (sync, plex_enum, scan) call this between safe yield
    points and raise _JobCancelled when it returns True."""
    try:
        with get_conn(db_path) as conn:
            row = conn.execute(
                "SELECT cancel_requested FROM jobs WHERE id = ?",
                (job_id,),
            ).fetchone()
        return bool(row and row["cancel_requested"])
    except Exception:
        return False  # never let cancel-check itself break a job


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
    # v1.11.36: optional job_type filter. When set, _claim_next_job
    # only picks jobs whose job_type is in this set. Used to split
    # the worker pool into a "long" worker (sync / plex_enum / scan)
    # and a "general" worker (download / place / refresh / relink /
    # adopt) so a long-running sync no longer blocks per-item work.
    # When None the worker claims any job (single-worker mode).
    job_type_filter: tuple[str, ...] | None = None
    name: str = "motif-worker"
    # v1.11.0: per-section FolderIndex cache. Keyed by section_id so
    # placement only ever sees folders from the section that owns the
    # current download/place job — no more cross-section confusion.
    _folder_index_by_section: dict[str, FolderIndex] | None = None
    _index_built_at: float = 0.0

    def __post_init__(self):
        # Stale after 5 minutes — rebuilt on demand
        self._index_ttl = 300.0
        self._folder_index_by_section = {}

    # -- Index management --

    def _index_for_section(self, section_id: str) -> FolderIndex:
        """v1.11.0: build (or reuse) a FolderIndex scoped to one Plex
        section's location_paths. Lazy + TTL-cached per section_id so a
        thousand back-to-back place jobs in the same section reuse one
        index, but the next section starts clean."""
        now = time.monotonic()
        if now - self._index_built_at > self._index_ttl:
            self._folder_index_by_section = {}
            self._index_built_at = now
        cached = self._folder_index_by_section.get(section_id) if self._folder_index_by_section else None
        if cached is not None:
            return cached
        from .sections import list_sections
        roots: list[Path] = []
        for s in list_sections(self.settings.db_path):
            if s["section_id"] != section_id:
                continue
            for p in (s.get("location_paths") or []):
                roots.append(Path(p))
            break
        idx = FolderIndex.build_multi(
            roots, plus_mode=self.settings.plus_equiv_mode,  # type: ignore[arg-type]
        )
        if self._folder_index_by_section is None:
            self._folder_index_by_section = {}
        self._folder_index_by_section[section_id] = idx
        return idx

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
        # v1.11.6: zombie-job recovery. If a worker crashed (or the
        # container was kill-9'd) mid-job, the row is left as
        # status='running' and never gets reclaimed by claim_next_job
        # (which only looks at status='pending'). Reset every running
        # job to pending on startup so they re-enter the queue. Probes
        # in particular were observed stuck in 'running' for 40+ min
        # after a sandbox restart with no progress.
        try:
            with get_conn(self.settings.db_path) as conn:
                cur = conn.execute(
                    "UPDATE jobs SET status = 'pending', started_at = NULL "
                    "WHERE status = 'running'"
                )
                if cur.rowcount:
                    log.info("Reclaimed %d orphan 'running' job(s) at startup",
                             cur.rowcount)
        except Exception as e:
            log.warning("Orphan-job recovery failed at startup: %s", e)
        while not self.stop_event.is_set():
            job = self._claim_next_job()
            if job is None:
                # No work — back off briefly
                self.stop_event.wait(2.0)
                continue
            try:
                # v1.11.36: pending jobs that were cancel_requested
                # before they ever ran flip straight to 'cancelled'.
                if _is_cancelled(self.settings.db_path, job["id"]):
                    raise _JobCancelled()
                self._dispatch(job)
            except _JobCancelled:
                log.info("Job %s cancelled by user", job["id"])
                self._mark_cancelled(job["id"])
            except _JobPermanentFailure as e:
                log.info("Job %s permanently failed: %s", job["id"], e)
                self._mark_failed_terminal(job["id"], str(e))
            except Exception as e:
                log.exception("Job %s crashed: %s", job["id"], e)
                self._mark_failed(job["id"], str(e))
        log.info("Worker loop stopped")

    def _claim_next_job(self) -> sqlite3.Row | None:
        # v1.11.36: optional job_type_filter scopes claim to the worker's
        # responsibilities. SQLite serializes the BEGIN IMMEDIATE so two
        # workers can claim from the same queue without double-claiming
        # — they just race for the lock and the loser sees the row's
        # status flipped to 'running' before its own SELECT runs.
        sql = (
            "SELECT * FROM jobs "
            "WHERE status = 'pending' "
            "  AND (next_run_at IS NULL OR next_run_at <= ?)"
        )
        params: list = [now_iso()]
        if self.job_type_filter:
            placeholders = ",".join("?" for _ in self.job_type_filter)
            sql += f" AND job_type IN ({placeholders})"
            params.extend(self.job_type_filter)
        sql += " ORDER BY id ASC LIMIT 1"
        with get_conn(self.settings.db_path) as conn, transaction(conn):
            row = conn.execute(sql, params).fetchone()
            if row is None:
                return None
            conn.execute(
                "UPDATE jobs SET status = 'running', started_at = ?, "
                "                attempts = attempts + 1 "
                "WHERE id = ?",
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

    def _mark_cancelled(self, job_id: int) -> None:
        """v1.11.36: terminal cancellation — sets status='cancelled'
        with a recognizable last_error message and clears the
        cancel_requested flag so a future re-run doesn't immediately
        cancel again."""
        with get_conn(self.settings.db_path) as conn:
            conn.execute(
                "UPDATE jobs SET status = 'cancelled', "
                "                finished_at = ?, "
                "                last_error = 'cancelled by user', "
                "                cancel_requested = 0 "
                "WHERE id = ?",
                (now_iso(), job_id),
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
        run_plex_enum(
            self.settings.db_path, cfg, only_section_id=only_section,
            cancel_check=lambda jid=job["id"]: _is_cancelled(self.settings.db_path, jid),
        )

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
            cancel_check=lambda jid=job["id"]: _is_cancelled(self.settings.db_path, jid),
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
        # v1.11.36: cancel-check now ORs the per-job cancel_requested
        # flag with the worker's stop_event so user-cancellation works
        # alongside the existing shutdown semantics.
        jid = job["id"]
        db_path = self.settings.db_path
        stop = self.stop_event
        run_scan(
            db_path, self.settings,
            initiated_by=initiated_by,
            cancel_check=lambda: stop.is_set() or _is_cancelled(db_path, jid),
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
        section_id = job["section_id"]

        # v1.11.0: download jobs are per-section. A section_id is required
        # so we know which staging subdir to write into.
        if not section_id:
            raise _JobPermanentFailure(
                "download job missing section_id (v1.11.0 requires per-section routing)"
            )

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

        # Look up theme + any user override + the section's themes_subdir.
        with get_conn(self.settings.db_path) as conn:
            theme = conn.execute(
                "SELECT * FROM themes WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            ).fetchone()
            override = conn.execute(
                "SELECT * FROM user_overrides WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            ).fetchone()
            section_row = conn.execute(
                "SELECT themes_subdir FROM plex_sections WHERE section_id = ?",
                (section_id,),
            ).fetchone()
            if section_row is None or not section_row["themes_subdir"]:
                raise _JobPermanentFailure(
                    f"section {section_id} has no themes_subdir"
                )
            section_subdir = section_row["themes_subdir"]

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

        media_root = self.settings.section_themes_dir_by_subdir(section_subdir)
        if media_root is None:
            raise _JobPermanentFailure(
                f"section_themes_dir_by_subdir returned None for {section_subdir!r}"
            )
        out_dir = media_root / canonical_theme_subdir(theme["title"], theme["year"])

        # Dry-run: log what we would do and stop here (no YouTube hit, no rate-limit token consumed)
        if is_dry_run(self.settings.db_path, default=self.settings.dry_run_default):
            log_event(
                self.settings.db_path, level="INFO", component="dryrun",
                media_type=media_type, tmdb_id=tmdb_id,
                message=f"DRY-RUN: would download '{theme['title']}' from {yt_url}",
                detail={"video_id": vid, "url": yt_url,
                        "section_id": section_id,
                        "would_save_to": str(out_dir / "theme.mp3")},
            )
            return  # job marked done by caller

        # v1.11.0: before paying for a YouTube fetch + transcode, look for a
        # sibling local_files row for the same (media_type, tmdb_id, vid) in
        # any other section. If found, hardlink the existing file into this
        # section's staging dir — one physical download per item, N inode-
        # shared copies across the sections that own it.
        sibling = None
        with get_conn(self.settings.db_path) as conn:
            sibling = conn.execute(
                "SELECT file_path, file_sha256, file_size, source_video_id, "
                "       provenance, source_kind, downloaded_at "
                "FROM local_files "
                "WHERE media_type = ? AND tmdb_id = ? AND section_id != ? "
                "  AND source_video_id = ? LIMIT 1",
                (media_type, tmdb_id, section_id, vid),
            ).fetchone()
        if sibling is not None:
            sibling_abs = self.settings.themes_dir / sibling["file_path"]
            if sibling_abs.is_file():
                target_mp3 = out_dir / "theme.mp3"
                try:
                    out_dir.mkdir(parents=True, exist_ok=True)
                    if target_mp3.exists():
                        # Already linked? if so we're idempotent. Otherwise
                        # replace the stale local file.
                        try:
                            if target_mp3.stat().st_ino == sibling_abs.stat().st_ino:
                                pass  # already the same inode
                            else:
                                target_mp3.unlink()
                                _safe_link(sibling_abs, target_mp3)
                        except OSError:
                            target_mp3.unlink()
                            _safe_link(sibling_abs, target_mp3)
                    else:
                        _safe_link(sibling_abs, target_mp3)
                    rel_path = str(target_mp3.relative_to(self.settings.themes_dir))
                    self._record_local_file(
                        media_type=media_type, tmdb_id=tmdb_id,
                        section_id=section_id, rel_path=rel_path,
                        sha256=sibling["file_sha256"],
                        size=sibling["file_size"],
                        video_id=vid,
                        provenance=("manual" if override else sibling["provenance"]),
                        source_kind=("url" if override else (sibling["source_kind"] or "themerrdb")),
                        job_payload=job["payload"],
                    )
                    log_event(
                        self.settings.db_path, level="INFO", component="download",
                        media_type=media_type, tmdb_id=tmdb_id,
                        message=f"Hardlinked sibling theme for '{theme['title']}' into section {section_id}",
                        detail={"section_id": section_id, "video_id": vid,
                                "sibling_path": sibling["file_path"]},
                    )
                    return
                except OSError as e:
                    log.warning(
                        "Sibling-hardlink failed for %s/%s into %s, falling back to download: %s",
                        media_type, tmdb_id, section_id, e,
                    )
                    # fall through to the actual download

        # Apply rate limit (only when we'll really hit YouTube)
        self.bucket.acquire()

        # If a previously downloaded theme.mp3 in THIS section was for a
        # different video (e.g. ThemerrDB URL changed since last sync), unlink
        # the stale one so the downloader's "already exists" short-circuit
        # doesn't keep us pinned to the old audio.
        with get_conn(self.settings.db_path) as conn:
            existing_local = conn.execute(
                "SELECT source_video_id FROM local_files "
                "WHERE media_type = ? AND tmdb_id = ? AND section_id = ?",
                (media_type, tmdb_id, section_id),
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
                # for THIS section — without the file, place can't do
                # anything useful. Sibling-section places can still run
                # if their own download succeeded.
                conn.execute(
                    """UPDATE jobs SET status = 'cancelled', finished_at = ?
                       WHERE job_type = 'place' AND media_type = ? AND tmdb_id = ?
                         AND section_id = ? AND status IN ('pending','running')""",
                    (now_iso(), media_type, tmdb_id, section_id),
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

        # Record the local file (per-section). source_kind drives the
        # T/U/A badge (v1.10.12): 'url' when the download was driven by
        # a user_overrides row; 'themerrdb' otherwise.
        rel_path = str(result.file_path.relative_to(self.settings.themes_dir))
        self._record_local_file(
            media_type=media_type, tmdb_id=tmdb_id, section_id=section_id,
            rel_path=rel_path, sha256=result.file_sha256, size=result.file_size,
            video_id=vid,
            provenance=("manual" if override else "auto"),
            source_kind=("url" if override else "themerrdb"),
            job_payload=job["payload"],
        )

        log_event(
            self.settings.db_path, level="INFO", component="download",
            media_type=media_type, tmdb_id=tmdb_id,
            message=f"Downloaded theme for {theme['title']}",
            detail={"size": result.file_size, "video_id": vid,
                    "section_id": section_id},
        )

    def _record_local_file(
        self, *, media_type: str, tmdb_id: int, section_id: str,
        rel_path: str, sha256: str | None, size: int | None,
        video_id: str, provenance: str, source_kind: str,
        job_payload: str | None,
    ) -> None:
        """v1.11.0: shared write path for the post-download bookkeeping.

        Writes a per-section local_files row, clears any failure on the
        theme (the file exists now, regardless of which section landed it),
        then enqueues a place job for THIS section if auto_place is on.
        Used by both the real download path and the sibling-hardlink
        short-circuit so they stay in lockstep.
        """
        with get_conn(self.settings.db_path) as conn, transaction(conn):
            conn.execute(
                """UPDATE themes SET failure_kind = NULL, failure_message = NULL,
                                      failure_at = NULL, failure_acked_at = NULL
                   WHERE media_type = ? AND tmdb_id = ? AND failure_kind IS NOT NULL""",
                (media_type, tmdb_id),
            )
            conn.execute(
                """
                INSERT INTO local_files
                    (media_type, tmdb_id, section_id, file_path, file_sha256, file_size,
                     downloaded_at, source_video_id, provenance, source_kind)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(media_type, tmdb_id, section_id) DO UPDATE SET
                    file_path = excluded.file_path,
                    file_sha256 = excluded.file_sha256,
                    file_size = excluded.file_size,
                    downloaded_at = excluded.downloaded_at,
                    source_video_id = excluded.source_video_id,
                    provenance = excluded.provenance,
                    source_kind = excluded.source_kind
                """,
                (media_type, tmdb_id, section_id, rel_path, sha256, size,
                 now_iso(), video_id, provenance, source_kind),
            )
            # Decide whether to auto-place. Per-job payload override wins;
            # otherwise fall back to the global placement.auto_place setting.
            auto_place = self.settings.auto_place_default
            force_place = False
            try:
                payload = json.loads(job_payload or "{}")
                if "auto_place" in payload:
                    auto_place = bool(payload["auto_place"])
                if payload.get("force_place"):
                    force_place = True
                    auto_place = True  # force implies auto
            except (TypeError, ValueError):
                pass
            if auto_place:
                place_payload = (
                    '{"force":true,"reason":"replace_with_themerrdb"}'
                    if force_place else "{}"
                )
                conn.execute(
                    """
                    INSERT INTO jobs (job_type, media_type, tmdb_id, section_id,
                                      payload, status, created_at, next_run_at)
                    VALUES ('place', ?, ?, ?, ?, 'pending', ?, ?)
                    """,
                    (media_type, tmdb_id, section_id, place_payload,
                     now_iso(), now_iso()),
                )

    def _do_place(self, job: sqlite3.Row) -> None:
        media_type = job["media_type"]
        tmdb_id = job["tmdb_id"]
        section_id = job["section_id"]

        # v1.11.0: place jobs are per-section. The job's section_id picks
        # both which staging file to read AND which folder index to walk.
        if not section_id:
            raise _JobPermanentFailure(
                "place job missing section_id (v1.11.0 requires per-section routing)"
            )

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
                "SELECT * FROM local_files "
                "WHERE media_type = ? AND tmdb_id = ? AND section_id = ?",
                (media_type, tmdb_id, section_id),
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
            index = self._index_for_section(section_id)
            find = find_target_folder(
                index, title=theme["title"], year=theme["year"] or "",
                edition_raw="",
                strict_edition=self.settings.strict_edition_match,
                plus_mode=self.settings.plus_equiv_mode,  # type: ignore[arg-type]
            )
            # v1.11.39: read has_theme from plex_items instead of
            # firing live Plex API calls during a dry-run preview.
            plex_mt = "show" if media_type == "tv" else "movie"
            with get_conn(self.settings.db_path) as conn:
                pi_row = conn.execute(
                    "SELECT has_theme FROM plex_items "
                    "WHERE guid_tmdb = ? AND media_type = ? "
                    "  AND section_id = ? LIMIT 1",
                    (tmdb_id, plex_mt, section_id),
                ).fetchone()
            plex_has = bool(pi_row and pi_row["has_theme"])

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
                    "section_id": section_id,
                    "plex_already_has_theme": plex_has,
                },
            )
            return

        if local is None:
            raise RuntimeError(
                f"local file missing for section {section_id} "
                "(download not yet complete?)"
            )

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

        index = self._index_for_section(section_id)
        plex_client = self._plex_client()
        # v1.11.39: pre-resolve rating_key + has_theme from plex_items
        # so place_theme doesn't have to hit Plex for them. Saves two
        # Plex API calls per placement (resolve_rating_key +
        # item_has_theme); plex_items is the cached view from
        # plex_enum and already carries both. Plex still gets called
        # ONCE per placement for the post-place refresh — that's the
        # only call we actually need.
        plex_mt = "show" if media_type == "tv" else "movie"
        with get_conn(self.settings.db_path) as conn:
            pi = conn.execute(
                "SELECT rating_key, has_theme FROM plex_items "
                "WHERE guid_tmdb = ? AND media_type = ? "
                "  AND section_id = ? "
                "  AND folder_path = (SELECT media_folder FROM placements "
                "                       WHERE media_type = ? AND tmdb_id = ? "
                "                         AND section_id = ? LIMIT 1) "
                "LIMIT 1",
                (tmdb_id, plex_mt, section_id,
                 media_type, tmdb_id, section_id),
            ).fetchone()
            if pi is None:
                # Folder-path fallback hit nothing (no placement yet for
                # this row) — pick any plex_items row in this section
                # that matches the tmdb id.
                pi = conn.execute(
                    "SELECT rating_key, has_theme FROM plex_items "
                    "WHERE guid_tmdb = ? AND media_type = ? AND section_id = ? "
                    "LIMIT 1",
                    (tmdb_id, plex_mt, section_id),
                ).fetchone()
        cached_rk = pi["rating_key"] if pi else None
        cached_has_theme = bool(pi and pi["has_theme"])
        try:
            outcome = place_theme(
                media_type=media_type,
                title=theme["title"],
                year=theme["year"] or "",
                edition_raw="",  # TODO: future: per-edition placement
                source_file=source_file,
                index=index,
                plex=plex_client,
                cached_rk=cached_rk,
                cached_has_theme=cached_has_theme,
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
            with get_conn(self.settings.db_path) as conn:
                placement_provenance = local["provenance"] or "auto"
                conn.execute(
                    """
                    INSERT INTO placements
                        (media_type, tmdb_id, section_id, media_folder, placed_at,
                         placement_kind, plex_rating_key, plex_refreshed, provenance)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(media_type, tmdb_id, section_id, media_folder) DO UPDATE SET
                        placed_at = excluded.placed_at,
                        placement_kind = excluded.placement_kind,
                        plex_rating_key = excluded.plex_rating_key,
                        plex_refreshed = excluded.plex_refreshed,
                        provenance = excluded.provenance
                    """,
                    (media_type, tmdb_id, section_id, str(outcome.target_folder),
                     now_iso(), outcome.kind, outcome.plex_rating_key,
                     1 if outcome.plex_refreshed else 0, placement_provenance),
                )
                # v1.10.46: Plex's first refresh sometimes lands before
                # the filesystem watcher / local-media-assets agent has
                # re-stat'd the folder, so the new theme.mp3 isn't
                # picked up. The user then has to manually click
                # 'Refresh Metadata' in Plex to force the pickup.
                # v1.11.24: schedule THREE follow-up refreshes (10s,
                # 30s, 90s) in addition to the immediate refresh in
                # place_theme. A single +10s delay wasn't always
                # enough on TV-show sections — Plex's local-media-
                # assets agent occasionally took 30-60s to notice the
                # new theme.mp3, so the user had to manually click
                # 'Refresh Metadata' to force pickup. Three retries
                # at increasing intervals give every reasonable
                # filesystem-cache + agent-debounce timing a chance
                # to land before the user has to do anything.
                if outcome.plex_rating_key and self.settings.plex_analyze_after:
                    base = datetime.now(timezone.utc)
                    for delay_s in (10, 30, 90):
                        delayed_iso = (
                            base + timedelta(seconds=delay_s)
                        ).isoformat(timespec="seconds")
                        conn.execute(
                            """INSERT INTO jobs (job_type, media_type, tmdb_id, section_id,
                                                  payload, status, created_at, next_run_at)
                               VALUES ('refresh', ?, ?, ?, ?, 'pending', ?, ?)""",
                            (media_type, tmdb_id, section_id,
                             json.dumps({"rating_key": outcome.plex_rating_key,
                                         "reason": f"post_place_refresh_+{delay_s}s"}),
                             now_iso(), delayed_iso),
                        )
        # v1.11.24: stamp the place outcome on the local_files row so
        # /pending can render the actual reason ('existing_theme:',
        # 'plex_has_theme', 'no_match', 'placement_error:') instead of
        # falling through to the generic 'Awaiting placement — the
        # worker should pick it up shortly'. Also propagate hints to
        # plex_items where applicable so the existing reason logic
        # (which reads pi.local_theme_file / pi.has_theme) gets a
        # nudge.
        try:
            with get_conn(self.settings.db_path) as conn:
                conn.execute(
                    "UPDATE local_files SET last_place_attempt_at = ?, "
                    "                       last_place_attempt_reason = ? "
                    "WHERE media_type = ? AND tmdb_id = ? AND section_id = ?",
                    (now_iso(),
                     ("placed" if outcome.placed else (outcome.reason or "unknown")),
                     media_type, tmdb_id, section_id),
                )
                if not outcome.placed and outcome.target_folder:
                    plex_type = "show" if media_type == "tv" else "movie"
                    folder_str = str(outcome.target_folder)
                    if (outcome.reason or "").startswith("existing_theme:"):
                        conn.execute(
                            "UPDATE plex_items SET local_theme_file = 1 "
                            "WHERE folder_path = ? AND media_type = ?",
                            (folder_str, plex_type),
                        )
                    elif outcome.reason == "plex_has_theme":
                        conn.execute(
                            "UPDATE plex_items SET has_theme = 1, "
                            "                     local_theme_file = 0 "
                            "WHERE folder_path = ? AND media_type = ?",
                            (folder_str, plex_type),
                        )
        except Exception as e:
            log.debug("place outcome stamp failed: %s", e)
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
                         ON lf.media_type = p.media_type
                        AND lf.tmdb_id = p.tmdb_id
                        AND lf.section_id = p.section_id
                       WHERE p.media_type = ? AND p.tmdb_id = ?
                         AND p.placement_kind = 'copy'""",
                    (media_type, tmdb_id),
                ).fetchall()
            else:
                rows = conn.execute(
                    """SELECT p.*, lf.file_path
                       FROM placements p
                       JOIN local_files lf
                         ON lf.media_type = p.media_type
                        AND lf.tmdb_id = p.tmdb_id
                        AND lf.section_id = p.section_id
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
                           WHERE media_type = ? AND tmdb_id = ?
                             AND section_id = ? AND media_folder = ?""",
                        (r["media_type"], r["tmdb_id"], r["section_id"],
                         r["media_folder"]),
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
                       WHERE media_type = ? AND tmdb_id = ?
                         AND section_id = ? AND media_folder = ?""",
                    (now_iso(), r["media_type"], r["tmdb_id"],
                     r["section_id"], r["media_folder"]),
                )
            relinked += 1

        log_event(
            self.settings.db_path, level="INFO", component="relink",
            message=f"Relink sweep complete: {relinked} converted, "
                    f"{still_copy} still cross-FS, {errors} errors",
            detail={"relinked": relinked, "still_copy": still_copy, "errors": errors},
        )


# -------- Entry point --------

_LONG_JOB_TYPES = ("sync", "plex_enum", "scan")
_GENERAL_JOB_TYPES = ("download", "place", "refresh", "relink", "adopt")


def start_worker(settings: Settings, stop_event: threading.Event) -> list[threading.Thread]:
    """v1.11.36: spawn TWO worker threads instead of one.

    - 'long' worker handles sync / plex_enum / scan. These each run for
      tens of seconds to minutes; pre-fix they blocked every download
      / place behind them.
    - 'general' worker handles download / place / refresh / relink /
      adopt — short, frequent jobs that benefit from running in
      parallel with the long jobs.

    SQLite WAL handles write serialization; sync writes themes,
    plex_enum writes plex_items, downloads / places write local_files
    / placements — all disjoint. Each worker has its own TokenBucket
    + FolderIndex cache so they don't share mutable state.
    """
    threads: list[threading.Thread] = []
    for label, types in (("long", _LONG_JOB_TYPES),
                         ("general", _GENERAL_JOB_TYPES)):
        bucket = TokenBucket(
            rate=float(settings.download_rate_per_hour),
            capacity=max(1, settings.download_rate_per_hour),
            period=3600.0,
        )
        w = Worker(settings=settings, stop_event=stop_event, bucket=bucket,
                   job_type_filter=types, name=f"motif-worker-{label}")
        t = threading.Thread(target=w.run, name=f"motif-worker-{label}", daemon=True)
        t.start()
        threads.append(t)
    return threads
