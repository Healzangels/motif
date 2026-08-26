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
from .downloader import DownloadError, FailureKind, download_theme
from .editions import edition_key_for_folder
from .events import log_event, now_iso
from .placement import FolderIndex, place_theme
from .plex import PlexClient, PlexConfig
from .runtime import is_dry_run
from .sync import run_sync, resolve_new_theme_pending_update
from .sync import release_sync_held_downloads, release_held_downloads

log = logging.getLogger(__name__)


# v1.18.98: hot-path warn-once flag per v1.17.11 sub-pattern.
# 9 worker sites parse `job["payload"]` JSON via `json.loads`; pre-fix
# each had a silent `except (TypeError, ValueError): pass` swallow.
# A corrupted payload silently changed job behavior in subtle ways
# (section_id dropped → unscoped sync; force flag dropped → won't
# replace existing; clear_plex_api_rks dropped → stranded entries
# not cleaned up). Per CLAUDE.md class-9 silent-defensive-catch:
# defensive paths need a log breadcrumb. Per v1.17.11 hot-path
# sub-pattern: log.warning the FIRST occurrence (operator sees it
# at boot or on the first repro) + log.debug subsequent ones to
# avoid drowning docker logs if a corruption class fires across
# many jobs at once. Flag resets only on process restart — matches
# the cadence at which a fix deploy can land.
# v1.23.62 (audit #18): keyed by `where` (the call-site label), not a single
# process-wide bool — a corrupt payload in one handler used to mute the WARNING
# for EVERY other handler too, so a download `edition_key`/`force` corruption
# could be permanently silenced by an unrelated earlier corrupt payload. Each
# distinct call-site now gets its own first-occurrence WARNING.
_PAYLOAD_PARSE_WARNED: set[str] = set()


def _safe_payload_parse(raw: str | None, *, where: str) -> dict:
    """v1.18.98: parse a job payload JSON column with breadcrumb on
    failure. Returns {} on parse error OR non-dict result so call
    sites can use `payload.get(...)` unconditionally without
    redundant isinstance checks.

    `where` is a short label (typically the calling method name) so
    the WARNING includes enough context to find the corrupt job.

    Payload writes are owned by motif — we json.dumps everything we
    INSERT — so a parse failure here suggests DB corruption, a
    writer bug, or someone manually editing the jobs table. None of
    those should ever happen silently."""
    try:
        parsed = json.loads(raw or "{}")
    except (TypeError, ValueError) as e:
        if where not in _PAYLOAD_PARSE_WARNED:
            log.warning(
                "worker: failed to parse job payload at %s "
                "(%s: %s) — falling back to empty dict. Will not "
                "log subsequent occurrences from this call-site in "
                "this process; payload writes are owned by motif, so "
                "a parse failure here suggests DB corruption, a writer "
                "bug, or hand-edited jobs.payload. Inspect the "
                "raw payload column for recent jobs to diagnose.",
                where, type(e).__name__, e,
            )
            _PAYLOAD_PARSE_WARNED.add(where)
        else:
            log.debug("payload parse failed at %s: %s", where, e)
        return {}
    if not isinstance(parsed, dict):
        return {}
    return parsed


def _safe_link(src: Path, dst: Path) -> None:
    """v1.11.0: hardlink src → dst; fall back to copy if hardlink isn't
    supported (e.g. cross-filesystem). Caller has already ensured
    dst.parent exists and dst doesn't (or has been removed)."""
    try:
        os.link(src, dst)
    except OSError as e:
        # v1.14.57: bumped log.info → log.warning. The fallback to
        # shutil.copy2 doubles motif's disk footprint per affected
        # row (the file lives independently in /themes/ AND the Plex
        # folder rather than sharing one inode), so a misconfigured
        # Unraid mount that puts /themes on one disk and /data on
        # another silently doubles disk use across the whole library.
        # WARNING surfaces the systemic case in /queue's logs view;
        # still safe operationally because shutil.copy2 IS the
        # documented fallback. AUDIT_WORKER L2.
        log.warning("Hardlink failed (%s); copying %s → %s", e, src, dst)
        shutil.copy2(src, dst)


def _disk_free_mb(path: Path) -> int | None:
    """v1.13.11: return free space (MB) on the filesystem hosting `path`,
    or None if the lookup fails (path missing, permission denied, etc).
    Walks up the parent chain so a not-yet-created subdir still resolves
    to its parent's filesystem.

    v1.13.15: emit a DEBUG log when we walk past the original path so
    a misconfigured themes_dir (typo, broken mount) is diagnosable. The
    walk-up itself is still the right behavior for the legitimate
    "themes_dir parent exists, target subdir created on first write"
    case — we just want operators to see when the answer comes from a
    different filesystem than they think.
    """
    p = path
    for i in range(10):
        try:
            usage = shutil.disk_usage(p)
            if i > 0:
                log.debug(
                    "_disk_free_mb: %s unreachable, reporting free space "
                    "from %s (walked %d level(s))", path, p, i,
                )
            return int(usage.free // (1024 * 1024))
        except (OSError, FileNotFoundError):
            if p.parent == p:
                return None
            p = p.parent
    return None


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


class _JobTransient(Exception):
    """v1.14.54: raised by handlers when the failure is a
    recoverable environmental condition the operator can fix (low
    disk, network down, Plex unreachable), not a per-job issue.
    The dispatch loop re-queues with a longer next_run_at WITHOUT
    consuming an attempt — so the job survives a long outage
    window without permanently failing after the ~6-minute
    max_attempts=3 default budget.

    `retry_after_seconds` controls the back-off; defaults to 1
    hour for low-disk / similar slow-recovery conditions."""

    def __init__(self, message: str, retry_after_seconds: int = 3600):
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


# v1.19.94: Plex's theme-upload size ceiling. The API rejects
# oversized theme bytes with HTTP 500 (multipart AND raw-body both
# 500 in the same instant — a deterministic rejection, not a
# transient hiccup). An upload at/above this size that 500s is
# treated as a size rejection: movie/TV rows with a folder fall back
# to a sidecar (themed via HL/C); collections / folderless rows fail
# TERMINALLY (no retry). Observed ceiling is ~10MB, NOT the ~25MB the
# v1.18.68-94 comments cited — the user's 2026-05-29 bulk place: 9.4MB
# (Unnamed Memory) uploaded fine while 10.5MB (The Demon Sword Master
# of Excalibur Academy) + the v1.18.94 11.7MB (Fate/stay night) both
# consistently 500'd. The old 20MB threshold let those mid-size files
# take the 3×-retry RuntimeError path, holding "PLACE INTO PLEX
# QUEUED" at ~97% for ~6min per bulk place before terminal-failing.
_PLEX_THEME_UPLOAD_CEILING_MB = 10

# v0.51.203 (Phase 3): download jobs motif enqueues automatically, with no user click —
# the sync/enum auto-download of an unthemed row ('new') and the auto re-download when a
# TDB theme's source URL changes ('url_changed'). Both fire ONLY when
# auto_download_new_themes_for_unthemed_rows is on (sync.py:1333 + :1589, plex_enum.py:3217).
# Every other _enqueue_download reason is a user action (// DOWNLOAD, bulk, ACCEPT, SET URL).
# The loudness gate keys on this set to route auto-picks through normalize_auto_added.
_AUTO_ADDED_DOWNLOAD_REASONS = frozenset({"new", "url_changed"})


def _should_condition_download(settings, payload: dict) -> bool:
    """v0.51.203 (Phase 3): does a freshly-downloaded theme get loudness-conditioned
    before placement? An explicit per-job choice (SET URL's payload.normalize) wins both
    ways; otherwise a motif-initiated auto-pick uses normalize_auto_added and a
    user-triggered download (// DOWNLOAD FROM TDB, bulk) uses normalize_on_download.
    Pure decision (no I/O) so its branches are unit-tested directly."""
    norm_job = payload.get("normalize")
    if norm_job is not None:
        return bool(norm_job)
    if payload.get("reason") in _AUTO_ADDED_DOWNLOAD_REASONS:
        return settings.normalize_auto_added
    return settings.normalize_on_download


def _downscale_audio_to_fit(src_path: "Path", target_bytes: int) -> bytes | None:
    # v1.24.45: re-encode an MP3 down to <= ~target_bytes so an over-ceiling
    # COLLECTION theme can still upload (collections have no sidecar fallback —
    # the metadata-store upload is the only path). Duration-aware: picks the
    # highest bitrate that lands under target so quality loss is the minimum
    # needed. Returns the re-encoded bytes, or None if ffmpeg is unavailable /
    # the encode failed (caller then uploads the original + surfaces over_ceiling).
    # ffmpeg is already in the container for FFmpegExtractAudio + the notify
    # thumbnail resize. Runs in the place-worker thread (not the event loop), so
    # the blocking subprocess is fine (bug-class 12).
    # v1.24.46: docstring → # comments per CLAUDE.md (single-line comments only).
    import shutil
    import subprocess
    import tempfile
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        log.warning("_downscale_audio_to_fit: ffmpeg not found — cannot shrink "
                    "over-ceiling theme %s", src_path)
        return None
    duration = None
    ffprobe = shutil.which("ffprobe")
    if ffprobe:
        try:
            out = subprocess.run(
                [ffprobe, "-v", "error", "-show_entries", "format=duration",
                 "-of", "default=noprint_wrappers=1:nokey=1", str(src_path)],
                capture_output=True, text=True, timeout=30)
            duration = float(out.stdout.strip())
        except Exception as e:
            # v0.51.249: class-9 breadcrumb — this function's sibling failure
            # paths already log (missing ffmpeg, re-encode failure) and this one
            # did not. On a >~10.4min theme the 128kbps fallback still exceeds
            # the Plex ceiling, so the re-upload 500s again and the log shows
            # over_ceiling with no trace that the duration probe — the input
            # that would have picked a fitting bitrate — is what failed.
            log.warning("downscale: ffprobe duration probe failed for %s (%s) "
                        "— falling back to the default bitrate guess", src_path, e)
            duration = None
    # Highest bitrate that fits target; clamp so a very long theme doesn't go to
    # an absurd bitrate (if even the floor can't fit, the upload still 500s and
    # we surface over_ceiling). 48 kbps floor keeps a recognizable theme.
    if duration and duration > 0:
        kbps = int(target_bytes * 8 / duration / 1000)
    else:
        kbps = 128  # no duration → a safe default that fits any normal theme
    kbps = max(48, min(kbps, 320))
    tmp_out = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".mp3", delete=False) as tmp:
            tmp_out = tmp.name
        subprocess.run(
            [ffmpeg, "-y", "-loglevel", "error", "-i", str(src_path),
             "-vn", "-b:a", f"{kbps}k", "-f", "mp3", tmp_out],
            capture_output=True, timeout=180, check=True)
        data = Path(tmp_out).read_bytes()
        return data if data else None
    except Exception as e:  # noqa: BLE001
        log.warning("_downscale_audio_to_fit: ffmpeg re-encode failed (%s) for %s",
                    e, src_path)
        return None
    finally:
        if tmp_out:
            try:
                Path(tmp_out).unlink(missing_ok=True)
            except OSError:
                pass

# v1.23.94: a DOWNLOAD job's `reason` → the theme_pushed reason its chained
# FORCE-PLACE job should report, so the notification reflects the ACTUAL user
# action instead of a blanket "REPLACE TDB". "REPLACE TDB" (the default for any
# unlisted reason) is reserved for actions that genuinely swap the active source
# TO TDB: ACCEPT UPDATE (upstream_update_accepted / bulk_update_accepted) and the
# explicit REPLACE TDB action ('replace_with_themerrdb'). A central table because
# the worker can't see the user action — only the download reason — and the
# vocabulary kept drifting: v1.23.91 mapped only 'bulk_select', v1.23.93 missed
# the per-row SET URL ('manual_url') + CSV import ('bulk_import') force-place
# downloads, which then mislabelled as REPLACE TDB (code-review-found). Values are
# keys of notify_content._PUSH_REASON_LABEL. Keep in sync with the _enqueue_download
# `reason=` call sites + the inline force_place download payloads (adopt.py:542,
# api.py manual-url / bulk-import).
_DOWNLOAD_PUSH_REASON: dict[str, str] = {
    "bulk_select": "download_tdb",   # bulk DOWNLOAD TDB
    "bulk_missing": "download_tdb",  # bulk DOWNLOAD MISSING
    "manual": "download_tdb",        # single RE-DOWNLOAD / DOWNLOAD TDB
    "override": "set_url",           # SET URL (/override dialog) — user URL
    "manual_url": "set_url",         # SET URL (per-row /manual-url) — user URL
    "bulk_import": "set_url",        # CSV import of user theme URLs (SRC=U)
}


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
    except Exception as e:
        # v1.15.105: leave a breadcrumb on persistent failures (DB
        # lock storms, schema corruption). Pre-fix this swallowed
        # silently — operators had no signal that cancel-checks
        # were failing. AUDIT_WORKER.md L1.
        log.debug("cancel-check failed for job %s: %s", job_id, e)
        return False  # never let cancel-check itself break a job


def _truncate_err(msg: str, n: int) -> str:
    """v1.15.105: UTF-8-safe-ish failure-message truncation with a
    visible ellipsis when truncation actually happens. Pre-fix the
    `str(e)[:N]` / `err_text[:N]` slicing pattern at 4 sites would
    chop yt-dlp error messages mid-grapheme on non-ASCII inputs
    (SoundCloud titles with emoji, international Plex error text).
    The slice is still by code-point — Python str slicing is, and
    that's correct given JSON-encoded downstream storage — but the
    `...` suffix tells the UI/operator the message was cut.
    AUDIT_WORKER.md M6."""
    return f"{msg[:n - 3]}..." if len(msg) > n else msg


def _should_enum_after_sync(
    auto_enum: bool, auto_enum_cron: bool, trigger: str | None,
) -> bool:
    """v1.21.19: decide whether a finished ThemerrDB sync triggers the
    post-sync Plex enumeration. The main toggle (auto_enum) covers BOTH
    triggers; the cron-only sub-toggle (auto_enum_cron) re-enables it for
    the SCHEDULED sync alone when the main toggle is off. A 6-case truth
    table worth pinning so the cron-vs-manual split can't silently drift:

        main  sub   trigger   → enum?
        ON    *     *         → True   (main covers everything)
        OFF   ON    cron      → True   (sub re-enables cron only)
        OFF   ON    manual    → False
        OFF   ON    None      → False  (legacy job, treat as not-cron)
        OFF   OFF   *         → False
    """
    return bool(auto_enum) or (trigger == "cron" and bool(auto_enum_cron))


def _sections_for_theme(conn, media_type: str, tmdb_id: int) -> list[str]:
    """v1.22.45: the Plex-section titles a (media_type, tmdb_id) theme is in,
    so the sync-summary notification can group New/Updated under the library
    each theme belongs to. Matches on guid_tmdb OR pi.theme_id linkage (the
    anime cohort has guid_tmdb NULL — v1.22.17/.39 class). CASE bridges
    'tv'↔'show'. A title in two sections (Movies + 4K Movies) returns both."""
    plex_mt = "show" if media_type == "tv" else (
        "collection" if media_type == "collection" else "movie")
    rows = conn.execute(
        "SELECT DISTINCT ps.title FROM plex_items pi "
        "  JOIN plex_sections ps ON ps.section_id = pi.section_id "
        "  LEFT JOIN themes t ON t.id = pi.theme_id "
        " WHERE pi.media_type = ? "
        "   AND (pi.guid_tmdb = ? OR (t.tmdb_id = ? AND t.media_type = ?)) "
        " ORDER BY ps.title",
        (plex_mt, tmdb_id, tmdb_id, media_type),
    ).fetchall()
    return [r["title"] for r in rows]


def _build_sync_section_buckets(conn, items):
    """v1.22.45: bucket a capped list of (media_type, tmdb_id, title, year)
    sync items by Plex-section title. Returns (buckets, shown) where buckets
    maps section title → ['Title (Year)', …] and shown is the count of
    DISTINCT items placed (each item counts once even when it spans sections).
    Items with no resolvable section fall under 'Other' (shouldn't happen —
    callers pre-gate on in-Plex — but never drop a row silently)."""
    buckets: dict = {}
    shown = 0
    for media_type, tmdb_id, title, year in items:
        sections = _sections_for_theme(conn, media_type, tmdb_id) or ["Other"]
        label = f"{title} ({year})" if year else title
        shown += 1
        for section in sections:
            buckets.setdefault(section, []).append(label)
    return buckets, shown


# -------- Token bucket --------

class TokenBucket:
    """Thread-safe token bucket. `rate` tokens are added per `period` seconds,
    capped at `capacity`. acquire() blocks until a token is available."""

    def __init__(self, rate: float, capacity: int, period: float = 3600.0):
        self._capacity = capacity
        # tokens added per second.
        # v1.22.30: floor rate at 1/period so _fill_rate is never 0. A
        # download_rate_per_hour of 0 (env MOTIF_DL_RATE_HOUR=0, or a
        # hand-edited motif.yaml — load() never runs validate(), so the
        # /settings ">= 1" check is bypassed) made `wait = deficit /
        # _fill_rate` a ZeroDivisionError that killed the download thread on
        # its SECOND acquire. Clamp so a misconfig throttles hard (1/hr)
        # instead of crashing the worker.
        self._fill_rate = max(rate, 1.0) / period
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

def apply_job_rollback(db_path: Path, job: "sqlite3.Row", err_text: str) -> None:
    """v0.51.11: shared rollback runner, extracted from
    Worker._run_rollback_safe so the API cancel paths can invoke it too.
    Round-4 audit found that CANCEL (per-job pending-cancel, bulk
    cancel-pending, worker _JobCancelled / pre-yt-dlp checkpoint) skipped
    the rollback recipe that ACCEPT UPDATE / REVERT stamp at enqueue time,
    stranding the destructive prep (override deleted, decision='accepted')
    with no theme — the same half-applied state v1.20.12 fixed for the
    terminal-FAILURE path, reachable via cancel. Self-guards: returns
    immediately (no DB connection opened) when the job carries no
    rollback recipe, so it is safe to call on every cancelled job.
    """
    try:
        payload_raw = job["payload"]
        if not payload_raw:
            return
        payload = json.loads(payload_raw)
        rb = payload.get("rollback")
        if not isinstance(rb, dict):
            return
        kind = rb.get("kind")
        mt = job["media_type"]
        tmdb = job["tmdb_id"]
        section_id = rb.get("section_id") or job["section_id"] or ""
        # v1.22.21 (edition audit): the destructive ACCEPT/REVERT prep
        # was edition-scoped (the endpoint deletes/re-pends only the
        # clicked edition's rows), but the rollback recipe dropped the
        # edition — so on terminal download failure the re-pend + override
        # restore hit ALL editions of the title-section, and the override
        # INSERT (which omitted the edition_key column) landed on the ''
        # row. Net: a non-'' edition's user URL was deleted by the
        # endpoint then RESTORED TO THE WRONG (standard) edition — silent
        # data-loss. The endpoints now stamp rollback['edition_key'];
        # default '' keeps old in-flight jobs at standard-edition scope.
        _rb_edition = rb.get("edition_key", "")
        if kind == "accept_update":
            # Flip pending_updates.decision back to 'pending' so the
            # row's blue ! glyph returns and the user can choose
            # again, AND (when one existed) restore the user_overrides
            # row that ACCEPT UPDATE deleted.
            # v1.20.12: the decision re-pend now runs UNCONDITIONALLY
            # (was nested inside `if replaced`). An accept with NO
            # override to restore — a SRC=— new_theme row, or a
            # pure-T upstream change with no user URL — got rollback
            # =None pre-fix and so stayed stuck decision='accepted'
            # with no theme on disk and no recovery surface (sync
            # only re-emits new_theme on is_new). Silent-bug audit
            # HIGH-1. Restoring the deleted override is the additive,
            # override-only half.
            # v1.12.116: wrap both writes in one transaction() so a
            # crash between them can't leave the row half-rolled-back.
            replaced = rb.get("replaced_user_url")
            # v1.19.36: preserve user_overrides.intent across the
            # rollback (older recipes / no-override accepts default
            # to 'replace').
            prior_intent = rb.get("prior_intent") or "replace"
            with get_conn(db_path) as conn, \
                    transaction(conn):
                conn.execute(
                    """UPDATE pending_updates SET
                          decision = 'pending',
                          decision_at = NULL,
                          decision_by = NULL,
                          replaced_user_url = NULL
                       WHERE media_type = ? AND tmdb_id = ?
                         AND section_id = ? AND edition_key = ?
                         AND decision = 'accepted'""",
                    (mt, tmdb, section_id, _rb_edition),
                )
                if replaced:
                    conn.execute(
                        """INSERT INTO user_overrides
                             (media_type, tmdb_id, youtube_url, set_at,
                              set_by, note, section_id, edition_key, intent)
                           VALUES (?, ?, ?, ?, 'system', ?, ?, ?, ?)
                           ON CONFLICT(media_type, tmdb_id, section_id, edition_key) DO UPDATE SET
                               youtube_url = excluded.youtube_url,
                               set_at = excluded.set_at,
                               set_by = excluded.set_by,
                               note = excluded.note,
                               intent = excluded.intent""",
                        (mt, tmdb, replaced, now_iso(),
                         "rollback after ACCEPT UPDATE download "
                         f"failed: {_truncate_err(err_text, 120)}",
                         section_id, _rb_edition, prior_intent),
                    )
            log.info(
                "rollback: re-pended accept-update failure on "
                "%s/%s/%s%s", mt, tmdb, section_id,
                " + restored override" if replaced else "")
        elif kind == "revert":
            # Restore the previous_urls row that REVERT consumed,
            # AND restore the prior override (or clear if there
            # wasn't one). Together this puts the row back where
            # it was before the user clicked REVERT.
            # v1.12.116: transaction() wraps all writes so a partial
            # rollback (override restored but previous_urls chain
            # not yet) can't leave the row in a half-state.
            prior_override = rb.get("prior_override_url")
            prior_prev = rb.get("prior_previous_url_row")  # dict or None
            with get_conn(db_path) as conn, \
                    transaction(conn):
                if prior_override:
                    # v1.19.36: preserve intent across REVERT
                    # rollback too. Same drift class as the
                    # ACCEPT UPDATE rollback fix above.
                    prior_intent = (
                        rb.get("prior_intent") or "replace"
                    )
                    conn.execute(
                        """INSERT INTO user_overrides
                             (media_type, tmdb_id, youtube_url, set_at,
                              set_by, note, section_id, edition_key, intent)
                           VALUES (?, ?, ?, ?, 'system', ?, ?, ?, ?)
                           ON CONFLICT(media_type, tmdb_id, section_id, edition_key) DO UPDATE SET
                               youtube_url = excluded.youtube_url,
                               set_at = excluded.set_at,
                               set_by = excluded.set_by,
                               note = excluded.note,
                               intent = excluded.intent""",
                        (mt, tmdb, prior_override, now_iso(),
                         "rollback after REVERT download failed: "
                         f"{_truncate_err(err_text, 120)}",
                         section_id, _rb_edition, prior_intent),
                    )
                else:
                    conn.execute(
                        "DELETE FROM user_overrides "
                        "WHERE media_type = ? AND tmdb_id = ? "
                        "  AND section_id = ? AND edition_key = ?",
                        (mt, tmdb, section_id, _rb_edition),
                    )
                if prior_prev:
                    conn.execute(
                        """INSERT INTO previous_urls
                             (media_type, tmdb_id, section_id,
                              youtube_url, kind, captured_at,
                              hidden_url, hidden_kind, hidden_captured_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                           ON CONFLICT(media_type, tmdb_id, section_id) DO UPDATE SET
                               youtube_url = excluded.youtube_url,
                               kind = excluded.kind,
                               captured_at = excluded.captured_at,
                               hidden_url = excluded.hidden_url,
                               hidden_kind = excluded.hidden_kind,
                               hidden_captured_at = excluded.hidden_captured_at""",
                        (mt, tmdb, section_id,
                         prior_prev.get("youtube_url"),
                         prior_prev.get("kind"),
                         prior_prev.get("captured_at"),
                         prior_prev.get("hidden_url"),
                         prior_prev.get("hidden_kind"),
                         prior_prev.get("hidden_captured_at")),
                    )
                else:
                    conn.execute(
                        "DELETE FROM previous_urls "
                        "WHERE media_type = ? AND tmdb_id = ? "
                        "  AND section_id = ?",
                        (mt, tmdb, section_id),
                    )
            log.info(
                "rollback: restored revert state on %s/%s/%s",
                mt, tmdb, section_id)
    except Exception as e:
        log.warning("rollback runner failed for job %s: %s",
                    job["id"], e)


def _cond_columns(cond: dict | None, sha256: str | None) -> tuple:
    """v0.51.188: the 11 loudness/normalize columns for the post-download upsert, in the
    INSERT's order.

    A raw download (conditioning off, or it declined) still stamps loudness when the
    measurement succeeded — the audit would only have to measure it again otherwise —
    but leaves norm_state NULL, because nothing was gained.

    loudness_measured_sha256 pins the measurement to the bytes it was taken from: that
    key is what the v0.51.169 staleness gate reads before computing a gain, and a
    measurement recorded against the wrong sha drives the gain off stale data."""
    if not cond:
        return (None,) * 11
    now = now_iso()
    if not cond.get("ok"):
        # measured but not gained (e.g. a silent theme), or not measurable at all.
        li = cond.get("loudness_i")
        return (li, cond.get("true_peak"), None, now if li is not None else None,
                sha256 if li is not None else None,
                None, None, None, None, None, None)
    if not cond.get("changed"):
        # already at target: measured, honest, and NOT normalized — there is no undo tag
        # on it, so calling it normalized would make // UNDO promise a restore it cannot
        # perform.
        return (cond["loudness_i"], cond["true_peak"], cond.get("lra"), now, sha256,
                None, None, None, None, None, None)
    return (cond["loudness_i"], cond["true_peak"], cond.get("lra"), now, sha256,
            "normalized", cond["applied_db"], cond["target"], now,
            cond.get("orig_sha256"), cond.get("orig_pcm_sha256"))


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
    # v1.11.94: settings revision the cache was built against. Any
    # divergence from settings.revision means a settings save/reload
    # happened — bust the cache so the next placement uses the new
    # plex_sections / plus_equiv_mode / etc. Without this, the user
    # could enable a section, kick a placement, and have it use the
    # pre-enable section list for up to the 5-min TTL.
    _index_settings_revision: int = -1

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
        cur_rev = self.settings.revision
        if (now - self._index_built_at > self._index_ttl
                or cur_rev != self._index_settings_revision):
            self._folder_index_by_section = {}
            self._index_built_at = now
            self._index_settings_revision = cur_rev
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
        # v1.22.30: the zombie-job reclaim that used to live here moved to
        # start_worker's _reclaim_orphan_jobs (ONE-TIME, pre-spawn). It must
        # NOT run per-thread: _supervised re-enters run() on any thread crash,
        # and a table-wide running→pending reset here would nuke every live
        # sibling's in-flight job. See _reclaim_orphan_jobs for the full why.
        while not self.stop_event.is_set():
            # v1.11.51: catch *every* exception around the claim +
            # bookkeeping path so a transient sqlite3.OperationalError
            # ('database is locked') doesn't kill the worker thread.
            # Pre-fix the long sync's batch flushes contended with the
            # general worker's BEGIN IMMEDIATE long enough for one
            # claim attempt to time out at busy_timeout=30s; the
            # OperationalError propagated out of run() and the thread
            # died for the rest of the process lifetime, leaving every
            # download / place / adopt job queue-stuck until the next
            # container restart.
            try:
                job = self._claim_next_job()
            except sqlite3.OperationalError as e:
                if "locked" in str(e).lower():
                    log.warning("Claim hit DB-lock contention; backing off 5s")
                    self.stop_event.wait(5.0)
                    continue
                raise
            except Exception as e:
                log.exception("Claim crashed: %s — backing off 5s", e)
                self.stop_event.wait(5.0)
                continue
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
                self._safe_mark(self._mark_cancelled, job["id"])
                # v0.51.11 (round-4 audit HIGH): cancel must run the rollback
                # recipe too. ACCEPT UPDATE / REVERT deleted the override +
                # flipped decision='accepted' at enqueue time; without this the
                # cancelled job strands that half-applied state (no theme, no
                # !UPD retry surface) — the terminal-failure branch below has
                # rolled back since v1.12.114 but cancel never did. No-op for
                # jobs carrying no recipe.
                self._run_rollback_safe(job, "cancelled by user")
            except _JobPermanentFailure as e:
                log.info("Job %s permanently failed: %s", job["id"], e)
                self._safe_mark(self._mark_failed_terminal, job["id"], str(e))
                self._run_rollback_safe(job, str(e))
            except _JobTransient as e:
                # v1.14.54: environmental backoff that DOES NOT consume
                # retry attempts. Low disk on Unraid can outlast the
                # ~6-minute max_attempts=3 budget under `mover` runs,
                # and pre-fix a sync that hit low disk failed hundreds
                # of downloads permanently — operator had to manually
                # re-enqueue them all once disk freed up.
                log.info("Job %s transient (retry in %ds): %s",
                         job["id"], e.retry_after_seconds, e)
                self._safe_mark(self._mark_transient, job["id"],
                                str(e), e.retry_after_seconds)
            except Exception as e:
                log.exception("Job %s crashed: %s", job["id"], e)
                # v1.12.114: only run rollback on FINAL retry exhaustion.
                # _mark_failed checks attempts vs max_attempts; if attempts
                # haven't been used up the job goes back to 'pending' for
                # retry, and rolling back prematurely would discard a
                # download that's about to succeed on next attempt.
                # v1.22.86: guard this read — it runs INSIDE the crash
                # handler, so a 'database is locked' here escaped run()
                # entirely: the job stayed status='running' (reclaimed
                # only by the aged stuck-job sweep) and the terminal
                # rollback never ran. On read failure fall through to
                # the marks with row=None (rollback skipped — the
                # conservative choice; retry handles the rest).
                row = None
                try:
                    with get_conn(self.settings.db_path) as conn:
                        row = conn.execute(
                            "SELECT attempts, max_attempts, status "
                            "FROM jobs WHERE id = ?",
                            (job["id"],),
                        ).fetchone()
                except sqlite3.OperationalError as _ce:
                    log.warning("crash-handler attempts read failed for "
                                "job %s: %s", job["id"], _ce)
                self._safe_mark(self._mark_failed, job["id"], str(e))
                if row and row["attempts"] >= row["max_attempts"]:
                    self._run_rollback_safe(job, str(e))
            finally:
                # v1.13.18 (6C): always clear the download-progress
                # dict entry for this job. Only download jobs ever
                # populate it, but pop-with-default is a no-op for
                # other job types.
                try:
                    from . import progress as _progress_cleanup
                    _progress_cleanup.clear_download_progress(job["id"])
                except Exception as e:
                    # v1.15.35: log at DEBUG. Pre-fix bare `pass`
                    # hid progress-state leaks if clear_download_
                    # progress raised (DB locked, corrupted state).
                    # The job is still marked done/failed; only the
                    # in-memory progress map may have a stale entry.
                    log.debug(
                        "Worker: clear_download_progress(%s) failed: %s",
                        job["id"], e,  # v0.51.294: sqlite3.Row has no .get — the breadcrumb itself crashed the worker thread
                    )
        log.info("Worker loop stopped")

    def _safe_mark(self, fn, *args) -> None:
        """v1.11.51: book-keeping helpers (_mark_done / _mark_failed /
        _mark_cancelled) all open their own get_conn → BEGIN IMMEDIATE.
        If the writer lock is contended the BEGIN raises 'database is
        locked' and the exception propagates up to run(), killing the
        worker thread. Retry with a short backoff so transient
        contention doesn't take the worker down between jobs.
        """
        delays = (0.25, 1.0, 4.0, 10.0)
        for d in delays:
            try:
                fn(*args)
                return
            except sqlite3.OperationalError as e:
                if "locked" not in str(e).lower():
                    raise
                log.warning("Bookkeeping (%s) DB-locked; retrying in %.1fs",
                            fn.__name__, d)
                self.stop_event.wait(d)
        # v0.51.16 (audit #24): last attempt — pre-fix this re-raised on the
        # theory that "run()'s OperationalError catch above will absorb it",
        # but that catch only wraps _claim_next_job. An escape from the
        # _mark_done call landed in the generic crash handler, which
        # _mark_failed-ed (re-pended) a job whose work had ALREADY completed
        # — a duplicate re-run; an escape from the except-block marks killed
        # the worker thread outright, the exact v1.11.51 failure this helper
        # exists to prevent. Locked-on-final-attempt now logs loud and gives
        # up: the row stays 'running' and the aged stuck-job sweep reclaims
        # it. Non-locked OperationalErrors still propagate (schema bugs must
        # stay loud — the class-8 rule).
        try:
            fn(*args)
        except sqlite3.OperationalError as e:
            if "locked" not in str(e).lower():
                raise
            log.error(
                "Bookkeeping (%s) still DB-locked after %d retries — giving "
                "up; job row left for the stuck-job sweep: %s",
                fn.__name__, len(delays), e,
            )

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
        # v1.20.40: plain FIFO again. v1.20.38 prioritized non-download
        # jobs here to interleave place-as-you-go on a single worker, but
        # that STARVED downloads whenever a large place backlog existed
        # (the worker drained every queued place before any download).
        # Place-as-you-go is now achieved structurally instead — a
        # dedicated place worker runs CONCURRENTLY with the download
        # worker(s) (see start_worker), so each segregated worker only
        # ever sees one tier and id-ASC is the right within-tier order.
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
        # v1.14.25: WHERE status='running' guard prevents the natural
        # completion of a force-cancelled job from silently flipping
        # the row back to 'done'. Pre-fix the user clicked × FORCE
        # → row went 'cancelled', then the worker's in-flight handler
        # finished and this UPDATE-by-id flipped it back. Audit ref
        # AUDIT_WORKER.md H1.
        with get_conn(self.settings.db_path) as conn:
            cur = conn.execute(
                "UPDATE jobs SET status = 'done', finished_at = ?, last_error = NULL "
                "WHERE id = ? AND status = 'running'",
                (now_iso(), job_id),
            )
            if cur.rowcount == 0:
                # The handler already moved status off 'running' (a
                # force-cancel, or a v1.21.41 placement_error terminal-
                # fail) — no-op the bookkeeping. log at INFO so this is
                # greppable when investigating "why did the row stay
                # cancelled / failed".
                log.info(
                    "_mark_done no-op for job %d (terminal status already "
                    "set by handler)",
                    job_id,
                )

    def _mark_failed_terminal(self, job_id: int, err: str) -> None:
        """v1.10.40: mark a job 'failed' immediately, bypassing the
        attempts-vs-max_attempts retry logic. Used for permanent
        failures where retrying is pointless (video removed/private/
        geo-blocked/age-restricted) — the user has to provide a
        manual override URL or accept the failure.

        v1.14.25: WHERE status='running' guard. See _mark_done.
        """
        with get_conn(self.settings.db_path) as conn:
            cur = conn.execute(
                "UPDATE jobs SET status = 'failed', finished_at = ?, "
                "                last_error = ? "
                "WHERE id = ? AND status = 'running'",
                (now_iso(), err, job_id),
            )
            if cur.rowcount == 0:
                log.info(
                    "_mark_failed_terminal no-op for job %d "
                    "(likely force-cancelled)",
                    job_id,
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
        # v1.14.25: WHERE status='running' guards on both branches
        # so a force-cancelled row doesn't silently flip back to
        # 'failed' (or worse, 'pending' via the retry-with-backoff
        # path) when its handler subsequently throws. Pre-fix the
        # retry branch was the most damaging: × FORCE → row went
        # 'cancelled' → handler raised → this set status='pending'
        # + scheduled a retry on the row the user explicitly killed.
        # Audit ref: AUDIT_WORKER.md H1.
        with get_conn(self.settings.db_path) as conn:
            row = conn.execute(
                "SELECT attempts, max_attempts FROM jobs WHERE id = ?", (job_id,),
            ).fetchone()
            if row is None:
                return
            if row["attempts"] >= row["max_attempts"]:
                cur = conn.execute(
                    "UPDATE jobs SET status = 'failed', finished_at = ?, last_error = ? "
                    "WHERE id = ? AND status = 'running'",
                    (now_iso(), err, job_id),
                )
            else:
                # Exponential backoff: 1m, 5m (max_attempts=3 → only
                # 2 retries reach this branch, see AUDIT_WORKER.md M3).
                backoff_minutes = 5 ** (row["attempts"] - 1)
                next_run = (datetime.now(timezone.utc) + timedelta(minutes=backoff_minutes)).isoformat(timespec="seconds")
                cur = conn.execute(
                    """
                    UPDATE jobs SET status = 'pending', last_error = ?, next_run_at = ?,
                                    started_at = NULL
                    WHERE id = ? AND status = 'running'
                    """,
                    (err, next_run, job_id),
                )
            if cur.rowcount == 0:
                log.info(
                    "_mark_failed no-op for job %d (likely force-cancelled)",
                    job_id,
                )

    def _mark_transient(self, job_id: int, err: str,
                        retry_after_seconds: int) -> None:
        """v1.14.54: re-queue a running job WITHOUT consuming an
        attempt. Mirrors `_mark_failed`'s WHERE status='running'
        guard so a force-cancelled row doesn't get resurrected.

        The `attempts = attempts - 1` decrement undoes the claim-
        phase increment so retry budget is preserved for an
        actual per-job failure. Low-disk / network-out / etc.
        can bounce indefinitely under this path."""
        with get_conn(self.settings.db_path) as conn:
            next_run = (datetime.now(timezone.utc)
                        + timedelta(seconds=retry_after_seconds)
                        ).isoformat(timespec="seconds")
            cur = conn.execute(
                """
                UPDATE jobs SET status = 'pending', last_error = ?,
                                next_run_at = ?, started_at = NULL,
                                attempts = MAX(0, attempts - 1)
                WHERE id = ? AND status = 'running'
                """,
                (err, next_run, job_id),
            )
            if cur.rowcount == 0:
                log.info(
                    "_mark_transient no-op for job %d (likely force-cancelled)",
                    job_id,
                )

    def _run_rollback_safe(self, job: sqlite3.Row, err_text: str) -> None:
        """v1.12.114: post-failure recovery for actions that destroyed
        prior state before queueing the download. ACCEPT UPDATE deletes
        the user_overrides row before the new TDB download runs; REVERT
        swaps the previous_urls chain. If those downloads fail terminally,
        the user is stranded in a half-applied state — their old
        URL/override is gone, the new one didn't materialize.

        The action site stamps `payload['rollback']` with the recovery
        recipe at enqueue time. This method reads it on terminal failure
        and undoes the destructive prep so the row goes back to its
        pre-action state. Wrapped in a try/except so a buggy rollback
        can't crash the worker — the original failure already surfaces
        via failure_kind, the user just keeps the half-applied state if
        rollback can't run.
        """
        apply_job_rollback(self.settings.db_path, job, err_text)

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
            # v1.22.30: terminal, not _mark_failed. An unknown job_type is a
            # permanent condition — no handler exists and none appears at
            # runtime — so the retry ladder just burns the attempt budget
            # re-dispatching a job that can never succeed. Fail it terminally.
            self._mark_failed_terminal(job["id"], f"unknown job type: {jt}")
            return
        # v1.22.86: route the SUCCESS mark through the _safe_mark retry
        # ladder too — a 'database is locked' on this one bare UPDATE
        # escaped into run()'s crash handler, which re-pended the
        # already-successful job → duplicate download/place execution +
        # duplicate notifications. Every failure-path mark got the
        # ladder in v1.11.51; the success mark didn't.
        self._safe_mark(self._mark_done, job["id"])

    # -- Job handlers --

    def _do_plex_enum(self, job: sqlite3.Row) -> None:
        """Re-enumerate Plex library items.

        Payload may carry {"section_id": "..."} to scope to one section
        (per-section REFRESH from /settings#plex). Default: every managed
        section.

        v1.18.4: also honors {"collections_only": true} — when set,
        skips the main /library/sections/{id}/all walk + sidecar
        stat + verify_theme_claims and ONLY refreshes the section's
        collection rows. ~10x faster on installs with thousands of
        movies/shows but only ~100 collections per section.
        """
        from .plex_enum import run_plex_enum
        from .plex import PlexConfig
        if not (self.settings.plex_enabled and self.settings.plex_url
                and self.settings.plex_token):
            log_event(self.settings.db_path, level="WARNING", component="plex_enum",
                      message="Skipped: Plex not configured")
            return
        only_section: str | None = None
        collections_only = False
        skip_collections = False
        payload = _safe_payload_parse(
            job["payload"], where="_do_plex_enum",
        )
        if payload.get("section_id"):
            only_section = str(payload["section_id"])
        if payload.get("collections_only"):
            collections_only = True
        # v1.23.77: items-only library-tab refresh — the movies/tv/anime
        # REFRESH FROM PLEX buttons skip the collections walk. Mutually
        # exclusive with collections_only at every call site.
        if payload.get("skip_collections"):
            skip_collections = True
        # v0.51.129: a user-initiated REFRESH stamps force=true so the enum
        # bypasses the contentChangedAt-skip and actually walks the section —
        # letting the v0.51.128 reaper miss-counter advance on demand (two
        # REFRESH clicks reap a genuinely-removed item + clear its phantom-P).
        # Cron + post-sync cascade jobs omit it → force stays False → skip kept.
        force_enum = bool(payload.get("force"))
        cfg = PlexConfig(
            url=self.settings.plex_url,
            token=self.settings.plex_token,
            movie_section=self.settings.plex_movie_section,
            tv_section=self.settings.plex_tv_section,
        )
        run_plex_enum(
            self.settings.db_path, cfg, only_section_id=only_section,
            collections_only=collections_only,
            skip_collections=skip_collections,
            force=force_enum,
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
        payload = _safe_payload_parse(
            job["payload"], where="_do_sync",
        )
        if "auto_place" in payload:
            auto_place_override = bool(payload["auto_place"])
        if payload.get("enqueue_downloads") is False:
            enqueue_downloads = False
        # v1.21.19: 'cron' (scheduler) | 'manual' (/api/sync/now) | None
        # (legacy job queued before this change). Drives the cron-only
        # post-sync-enum sub-toggle below.
        sync_trigger = payload.get("trigger")
        # v1.21.11: the auto-download opt-in WINS over metadata_only.
        # the user: a MANUALLY-kicked sync (the dashboard SYNC button sends
        # metadata_only=true → enqueue_downloads=False) should auto-
        # acquire new SRC=— themes the SAME as a scheduled cron sync when
        # the toggle is on — automation shouldn't be cron-only. After
        # v1.21.10 enqueue_downloads only gates the toggle-gated SRC=—
        # auto-download, so forcing it here can't trigger any non-opt-in
        # download (T / U / M / A rows still prompt, never auto-download).
        if self.settings.cfg.sync.auto_download_new_themes_for_unthemed_rows:
            enqueue_downloads = True
        # v1.17.0: capture sync start time so the post-sync notify
        # can scope its "themes added by sync" query to themes
        # created within this run's window.
        from . import notify as _notify
        from .events import now_iso as _now_iso
        sync_started_at = _now_iso()
        try:
            # v1.19.71: thread the opt-in auto_download_new_themes
            # setting through to sync. Default False — sync writes
            # pending_updates with kind='new_theme_available'
            # instead of silently downloading. Operator sees the
            # blue !UPD glyph and decides per-row.
            sync_stats = run_sync(
                self.settings.db_path, self.settings.motifdb_base_url,
                auto_place_override=auto_place_override,
                enqueue_downloads=enqueue_downloads,
                auto_download_new_themes=(
                    self.settings.cfg.sync
                    .auto_download_new_themes_for_unthemed_rows
                ),
                cancel_check=lambda jid=job["id"]: _is_cancelled(self.settings.db_path, jid),
                source=self.settings.sync_source,
                database_url=self.settings.sync_database_url,
                git_url=self.settings.sync_git_url,
                git_branch=self.settings.sync_git_branch,
            )
        except _JobCancelled:
            # v1.23.62 (audit #5/#15): a user-cancelled sync is NOT a failure.
            # The broad `except Exception` below would catch _JobCancelled (an
            # Exception subclass) and (a) fire a spurious "❌ Sync failed"
            # notification and (b) RELEASE the held auto-downloads — running the
            # parked downloads anyway, defeating the cancel. Skip both: leave the
            # held downloads at the sentinel (the NEXT sync's release sweeps them,
            # sync.py:750) and propagate so the dispatch loop marks 'cancelled'.
            raise
        except Exception as e:
            # v1.23.23: a sync that raised mid-run may have parked held
            # auto-downloads (themes discovered before the failure). Release
            # them so the discovered work isn't orphaned at the sentinel —
            # there's no summary to order against on the failure path.
            try:
                release_sync_held_downloads(self.settings.db_path)
            except Exception as _re:
                log.debug("release_sync_held_downloads (failure) raised: %s", _re)
            # v1.17.0: best-effort sync_failed notification. Dispatcher
            # never raises so this won't mask the underlying failure;
            # we re-raise so the worker's normal failure handling
            # (jobs.status='failed', retry logic) takes over.
            try:
                _notify.dispatch(
                    self.settings.db_path,
                    self.settings.cfg.notifications,
                    event_kind="sync_failed",
                    # v1.17.19: ❌ emoji prefix + dropped redundant
                    # "motif" — bot username already attributes.
                    # v1.23.82: adopt the "Motif sync — {state}" stem so the
                    # pair reads uniformly with sync_completed ("Motif sync —
                    # no changes" / "— N new · M updated"); the ❌ stays so a
                    # failure still stands out in the notification list.
                    title="❌ Motif sync — failed",
                    # v1.19.92: dropped the leading "ThemerrDB sync did
                    # not complete." line — it restated the title. Body
                    # leads with the error + the recovery path (info the
                    # title can't hold).
                    body=(
                        f"Error: {e}\n\n"
                        f"motif will retry on the next cron tick "
                        f"(see /settings → SCHEDULE for cadence). "
                        f"If failures persist, check /queue for the "
                        f"job's `note` column + /settings → SYNC for "
                        f"reachability of the configured source."
                    ),
                )
            except Exception as e:
                # v1.17.1 (class 9): log breadcrumb on the failure-
                # notification path. Pre-fix a swallowed ImportError /
                # dispatch raise here would be indistinguishable from
                # "notify infra worked but no service was configured."
                log.debug("notify dispatch (sync_failed) suppressed: %s", e)
            raise
        # v1.17.0: sync_completed notification with stats summary.
        # Counts come straight from SyncStats which the sync run
        # populates as it walks the upstream.
        # v1.18.64: body reworked to address the user's feedback —
        # the old "{N} items processed, {X} new, {Y} updated" line
        # was ambiguous in three ways:
        #   1. "processed" reads like "all entries walked" but in
        #      the git-differential path it's the diff size (small
        #      and confusing when nothing changed) — reword to
        #      "items checked in this sync window".
        #   2. Updated titles weren't listed — only a count. Mirrors
        #      the new-themes notification (which lists new titles)
        #      by pulling stats.updated_titles into a bulleted list.
        #   3. The all-zeros case read like sync didn't run — the
        #      title now states the outcome explicitly (v1.19.92
        #      moved it out of the body to kill the duplication).
        # body_format='markdown' so Discord/Slack/Telegram render
        # the bullets correctly.
        try:
            new_count = sync_stats.new_count
            upd_count = sync_stats.updated_count
            # v1.19.92: notification copy standard — the body carries
            # ONLY what the title doesn't. The title already states the
            # outcome ("no changes (N checked)" / "N new · M updated");
            # the pre-fix body restated it almost verbatim (the user's
            # 2026-05-29 sample: title "no changes (4581 checked)" +
            # body "No changes detected (scanned 4581 upstream items)"
            # — the same fact twice). So the body now lists the actual
            # UPDATED titles (info the title can't hold) + any error
            # count, then closes with the ✅ line the user asked for in
            # v1.19.55. A no-change sync gets just the ✅ closer.
            body_lines: list[str] = []
            has_changes = new_count != 0 or upd_count != 0
            # v1.21.6: fold the New/Updated title detail INTO this
            # single sync_completed message — each section gated by its
            # existing per-section toggle. Pre-fix a changed sync fired
            # up to THREE notifications (this one embedded the Updated
            # list, PLUS standalone themes_added_by_sync +
            # themes_updated_by_sync messages repeating it). the user:
            # "2 notifications for sync completes ... really want to get
            # notifications cleaned up." Now the toggles control whether
            # titles appear in the summary; they no longer fire a
            # second message.
            _events = self.settings.cfg.notifications.events
            # v1.22.45: group New + Updated under Plex-section sub-headers
            # (Movies / 4K Movies / TV Shows / Anime / Anime Movies …) so the
            # operator can tell which library each theme belongs to at a glance
            # instead of guessing from the title. _build_sync_section_buckets
            # resolves each item's section(s); format_section_grouped_lines
            # renders the markdown block.
            from .notify_content import format_section_grouped_lines
            if (has_changes and new_count > 0
                    and _events.get("themes_added_by_sync", False)):
                # v1.19.70: only titles actually in the user's Plex
                # library — TDB-only discoveries aren't actionable.
                # themes.tmdb_id is INTEGER, plex_items.guid_tmdb TEXT
                # (SQLite implicit-converts); CASE bridges 'tv'↔'show'.
                # v1.22.45: widened EXISTS with `OR pi.theme_id = themes.id`
                # so anime new themes (guid_tmdb NULL) are listed, not
                # just counted (the v1.22.17/.39 guid_tmdb-blind class).
                try:
                    with get_conn(self.settings.db_path) as _sconn:
                        new_rows = _sconn.execute(
                            "SELECT media_type, tmdb_id, title, year "
                            "FROM themes "
                            "WHERE first_seen_sync_at >= ? "
                            "  AND EXISTS ( "
                            "    SELECT 1 FROM plex_items pi "
                            "     WHERE (pi.guid_tmdb = themes.tmdb_id "
                            "            AND CASE pi.media_type "
                            "                  WHEN 'show' THEN 'tv' "
                            "                  ELSE pi.media_type END "
                            "                = themes.media_type) "
                            "        OR pi.theme_id = themes.id "
                            "  ) "
                            "ORDER BY first_seen_sync_at ASC LIMIT 11",
                            (sync_started_at,),
                        ).fetchall()
                        if new_rows:
                            _n_items = [
                                (r["media_type"], r["tmdb_id"],
                                 r["title"], r["year"])
                                for r in new_rows[:10]
                            ]
                            _n_buckets, _n_shown = _build_sync_section_buckets(
                                _sconn, _n_items)
                            body_lines.extend(format_section_grouped_lines(
                                "🎵 New:", _n_buckets, new_count, _n_shown))
                except Exception as e:
                    log.warning(
                        "sync summary new-titles query failed: %s", e)
            if (has_changes and upd_count > 0
                    and _events.get("themes_updated_by_sync", False)
                    and sync_stats.updated_titles):
                if body_lines:
                    body_lines.append("")
                try:
                    with get_conn(self.settings.db_path) as _uconn:
                        _u_items = [
                            (_mt, _tid, _t, _y)
                            for (_mt, _tid, _t, _y)
                            in sync_stats.updated_titles[:10]
                        ]
                        _u_buckets, _u_shown = _build_sync_section_buckets(
                            _uconn, _u_items)
                        body_lines.extend(format_section_grouped_lines(
                            "🔄 Updated:", _u_buckets, upd_count, _u_shown))
                except Exception as e:
                    log.warning(
                        "sync summary updated-titles grouping failed: %s", e)
            # Title-summary parts — also reused by the title block below.
            parts = []
            if new_count:
                parts.append(f"{new_count} new")
            if upd_count:
                parts.append(f"{upd_count} updated")
            if sync_stats.errors:
                # v1.23.81: proper plural ("1 error" / "2 errors") —
                # the informal "error(s)" suffix drifted from the
                # codebase's clean pluralization elsewhere.
                _err_word = "error" if sync_stats.errors == 1 else "errors"
                body_lines.append(
                    f"⚠ {sync_stats.errors} {_err_word} during sync.")
            # ✅ closer at the END (the user's v1.19.55 request). Blank
            # separator only when the body has preceding content, so a
            # no-change sync isn't "\n✅ Sync complete" with a leading
            # gap.
            if body_lines:
                body_lines.append("")
            body_lines.append("✅ Sync complete")
            sync_summary = "\n".join(body_lines)
            # v1.19.55: title moved from "✅ Sync complete" → a
            # neutral summary so the body's ✅ at the end reads
            # as the natural closing line. Discord previews show
            # the title prominently; the summary in the title is
            # more informative for at-a-glance scanning than the
            # repeated "sync complete" string.
            if new_count == 0 and upd_count == 0:
                # v1.23.79: a no-change sync's "(N checked)" count is ALWAYS the
                # tracked ThemerrDB catalog size (COUNT(*) FROM themes), never the
                # git-diff size. The v1.23.28 diff-size count jittered run-
                # to-run: an EMPTY diff fell back to the catalog count (~6193)
                # while a churn diff that touched nothing motif-relevant printed
                # the diff size (~5618) — two scales under one word "checked", so
                # consecutive "no changes" messages looked like the catalog shrank
                # (the user's 6193-vs-5618 confusion). The catalog count is stable +
                # answers the real question: "motif tracks N themes and none had a
                # relevant change this sync."
                # v1.23.30 (#2 code-review): skip the count when the sync had
                # errors — an errored run mustn't claim a clean full-catalog
                # verify; the ⚠ errors line in the body carries the real signal.
                checked = 0
                if not sync_stats.errors:
                    try:
                        with get_conn(self.settings.db_path) as _cc:
                            checked = _cc.execute(
                                "SELECT COUNT(*) FROM themes").fetchone()[0]
                    except Exception as e:
                        log.debug(
                            "sync notify catalog count failed: %s", e)
                # v1.24.16: "no CATALOG changes", not bare "no changes". This
                # headline reflects ONLY the ThemerrDB catalog diff (new/updated
                # themes). The post-sync plex_enum runs AFTER this dispatch (it's
                # enqueued as separate jobs below) and can auto-theme a
                # newly-seen UNTHEMED Plex row from a PRE-EXISTING catalog theme
                # → a `theme_added` card lands right after a "no changes" summary,
                # reading as a contradiction (the user's DragonHeart repro: catalog
                # unchanged, but an unthemed row got auto-themed in the same run).
                # Scoping the word to "catalog" makes the two coherent.
                # v0.50.72: name the count's UNIT — it's the ThemerrDB catalog
                # size (grows as themes are added to ThemerrDB), NOT a match
                # against the motif library. Pre-fix "(6201 checked)" read as if
                # 6201 things in the user's library were checked; the number
                # creeping up run-to-run looked unexplained (the user). Comma-
                # formatted for readability at that scale.
                title_text = (
                    f"Motif sync — no catalog changes "
                    f"({checked:,} ThemerrDB themes checked)"
                    if checked else "Motif sync — no catalog changes")
            else:
                title_text = f"Motif sync — {' · '.join(parts)}"
            _notify.dispatch(
                self.settings.db_path,
                self.settings.cfg.notifications,
                event_kind="sync_completed",
                title=title_text,
                body=sync_summary,
                body_format="markdown",
            )
        except Exception as e:
            # v1.17.1 (class 9): see sync_failed swallow above.
            log.debug("notify dispatch (sync_completed) suppressed: %s", e)
        # v1.23.23: the summary has now been dispatched — release the
        # auto-downloads run_sync parked at the hold sentinel so they place
        # AFTER it (the user: the per-theme cards were racing ahead of the
        # "N new" summary). The download pipeline latency keeps theme_added a
        # step behind the already-dispatched summary. Best-effort: a failure
        # here only delays the holds to the next sync's release.
        try:
            _released = release_sync_held_downloads(self.settings.db_path)
            if _released:
                log.debug("released %d sync-held download(s)", _released)
        except Exception as e:
            log.debug("release_sync_held_downloads (post-summary) raised: %s", e)
        # v1.12.126: post-sync plex_enum is now opt-in via
        # sync.auto_enum_after_sync (default True). The TDB sync's own
        # resolve_theme_ids step already links new TDB rows to existing
        # plex_items; the auto-enum is for picking up PLEX-side changes
        # (new titles, moved folders, manual sidecar adds/removes) and
        # re-verifying plex_theme_uri claims via the TTL HEAD probe.
        # Daily cron syncs need it (no human around to manually click
        # REFRESH); users who want manual control on the dashboard
        # button can disable.
        # v1.21.19: the main toggle governs BOTH triggers; the cron-only
        # sub-toggle (auto_enum_after_cron_sync) re-enables the enum for
        # the SCHEDULED sync alone when the main toggle is off — so a user
        # can keep the daily cron self-healing while NOT paying the enum
        # cost on every manual // SYNC click.
        do_post_sync_enum = _should_enum_after_sync(
            self.settings.sync_auto_enum_after_sync,
            self.settings.sync_auto_enum_after_cron_sync,
            sync_trigger,
        )
        # v1.13.25: enqueue ONE plex_enum job per included section
        # instead of a single global empty-payload job. Pre-fix the
        # cascade ran as one job whose payload had no section_id, so
        # /api/stats's `plex_enum_active[tab][variant]` map (built by
        # joining jobs.payload→section_id with plex_sections) was
        # all-false during the cascade — every UI signal that depends
        # on per-tab activity (library button locks, dash SYNC lock
        # via globalEnumPipeline) failed to fire mid-cascade. Per-
        # section jobs match the manual SCAN ALL flow exactly, light
        # up enumTabsActive correctly, and let the dash SYNC lock
        # hold through the entire sync→enum pipeline.
        if do_post_sync_enum:
            # v1.13.28: read+write inside BEGIN IMMEDIATE so a cron
            # tick and a manual click landing within the same
            # millisecond can't both observe the pre-insert
            # pending_section_ids snapshot and double-queue the
            # cascade. SQLite's autocommit (isolation_level=None on
            # get_conn) doesn't serialize the reads otherwise.
            with get_conn(self.settings.db_path) as conn:
                with transaction(conn):
                    included = conn.execute(
                        "SELECT section_id FROM plex_sections WHERE included = 1"
                    ).fetchall()
                    pending_section_ids = {
                        r["section_id"] for r in conn.execute(
                            "SELECT json_extract(payload, '$.section_id') AS section_id "
                            "FROM jobs WHERE job_type = 'plex_enum' "
                            "AND status IN ('pending','running')"
                        ).fetchall() if r["section_id"] is not None
                    }
                    for s in included:
                        sid = s["section_id"]
                        if sid in pending_section_ids:
                            continue
                        conn.execute(
                            "INSERT INTO jobs (job_type, payload, status, created_at, next_run_at) "
                            "VALUES ('plex_enum', ?, 'pending', ?, ?)",
                            (json.dumps({"section_id": sid, "scope": "cascade"}),
                             now_iso(), now_iso()),
                        )
                        # v1.15.109: track just-inserted section_ids
                        # so a duplicate row in `included` (schema
                        # has section_id PK so unreachable today,
                        # but defensive against a future schema
                        # regression or a `included` list build that
                        # ever returns duplicates) doesn't fire two
                        # cascade jobs for the same section.
                        # AUDIT_WORKER.md M4.
                        pending_section_ids.add(sid)

    def _do_scan(self, job: sqlite3.Row) -> None:
        """Run a Plex folder scan. Payload optionally carries
        {'initiated_by': 'username'}."""
        from .scanner import run_scan
        if not self.settings.is_paths_ready():
            log_event(self.settings.db_path, level="WARNING",
                      component="scan",
                      message="Skipped: themes_dir not configured.")
            raise _JobTransient("themes_dir not configured — set it on /settings",
                                retry_after_seconds=3600)  # v0.51.294: was RuntimeError — it burned the 3-attempt budget in ~6min and terminal-failed, contradicting its own log line

        payload = {}
        if job["payload"]:
            try:
                payload = json.loads(job["payload"])
            except Exception as e:
                # v1.13.50: log the swallow with the job id so a
                # corrupted payload row leaves a breadcrumb. Pre-
                # fix `except Exception: pass` lost initiated_by /
                # force_place / only_section_id silently — a
                # truncated write before crash would silently
                # change the job's behavior on next pickup.
                log.warning(
                    "Job %d payload parse failed: %s (raw=%r)",
                    job["id"], e, job["payload"][:200],
                )
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
            raise _JobTransient("themes_dir not configured — set it on /settings",
                                retry_after_seconds=3600)  # v0.51.294: was RuntimeError — it burned the 3-attempt budget in ~6min and terminal-failed, contradicting its own log line

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
            raise _JobTransient("themes_dir not configured — set it on /settings",
                                retry_after_seconds=3600)  # v0.51.294: was RuntimeError — it burned the 3-attempt budget in ~6min and terminal-failed, contradicting its own log line

        # v1.13.11: pre-download free-space guard. yt-dlp + ffmpeg can
        # eat several hundred MB of working space mid-extract; on a full
        # filesystem motif used to silently spam yt-dlp errors and rack
        # up retries.
        # v1.14.54: raise _JobTransient (was RuntimeError) so retries
        # don't consume the max_attempts=3 budget. Pre-fix the
        # exponential backoff (1m → 5m) burned through the budget in
        # ~6 minutes — on Unraid the `mover` runs can hold the disk
        # in low-disk state longer than that, permanently failing
        # hundreds of downloads in one sync that the operator then
        # had to manually re-enqueue. With _JobTransient the job
        # bounces hourly without consuming attempts, surviving
        # arbitrary outage windows.
        # 0 = guard disabled (operator opts out).
        min_mb = self.settings.min_free_disk_mb
        if min_mb > 0:
            td = self.settings.themes_dir
            free_mb = _disk_free_mb(td) if td is not None else None
            if free_mb is not None and free_mb < min_mb:
                msg = (
                    f"low disk: {free_mb}MB free on {td} "
                    f"(min {min_mb}MB) — free space and the download will retry"
                )
                log_event(
                    self.settings.db_path, level="WARNING",
                    component="download", media_type=media_type,
                    tmdb_id=tmdb_id, section_id=section_id, message=msg,
                )
                # v1.17.4: disk_low notification with 12h rate-limit
                # dedupe. Pre-fix every download job that hit the guard
                # logged WARNING + raised _JobTransient — a bulk
                # download with N rows hitting the guard would produce
                # N WARNING lines. The rate-limit caps to ~2 notifications/
                # day while disk stays low, which is enough nudging
                # without spam. Edge-trigger was considered but needs
                # a "disk recovered" reset path that would add
                # scope; the rate-limit is simpler and gives roughly
                # the same UX.
                try:
                    from . import notify as _notify
                    from . import notify_dedupe as _dedupe
                    if _dedupe.should_fire(
                            self.settings.db_path, "disk_low",
                            rate_limit_seconds=12 * 3600):
                        _notify.dispatch(
                            self.settings.db_path,
                            self.settings.cfg.notifications,
                            event_kind="disk_low",
                            # v1.23.84: 🗄️ emoji (was 💾 v1.17.19, which
                            # collided with theme_backed_up's 💾); "motif:"
                            # prefix dropped v1.17.19.
                            title=f"🗄️ Low disk space — {free_mb}MB free",
                            # v1.19.92: dropped the repeated "{free_mb}MB
                            # free" — the title already carries it. Body
                            # adds the dir + threshold + retry behavior.
                            body=(
                                f"Themes directory {td} is below the "
                                f"{min_mb}MB minimum guard. Downloads will "
                                f"keep retrying hourly until space frees up."
                            ),
                        )
                        _dedupe.record_fire(
                            self.settings.db_path, "disk_low")
                except Exception as e:
                    log.debug(
                        "notify dispatch (disk_low) suppressed: %s", e)
                raise _JobTransient(msg, retry_after_seconds=3600)

        # v1.21.92: resolve the download's edition BEFORE the override
        # lookup so the override query is edition-scoped. The enqueue side
        # stamps edition_key into the payload (adopt/REPLACE TDB, bulk,
        # per-row download all carry the triggering rating_key's edition).
        # Pre-fix the override query was edition-blind — a REPLACE TDB on
        # the Uncut edition matched the Odex Dub edition's user-URL override
        # (both at section 3) and downloaded the WRONG video into Uncut's
        # folder. out_dir was already edition-aware; the URL resolution was
        # not. Reused for the staging path below.
        _dl_edition_key = (
            _safe_payload_parse(job["payload"], where="_do_download")
            .get("edition_key", "") or "")

        # Look up theme + any user override + the section's themes_subdir.
        with get_conn(self.settings.db_path) as conn:
            theme = conn.execute(
                "SELECT * FROM themes WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            ).fetchone()
            # v1.12.72: per-section override fetch. Try the row's
            # specific section first; if none, fall back to the
            # legacy global ('') row that applies to any section
            # without a more-specific override. Lets users with
            # multi-section libraries pin different YouTube URLs
            # per edition while keeping pre-migration overrides
            # working as global defaults.
            override = conn.execute(
                "SELECT * FROM user_overrides "
                "WHERE media_type = ? AND tmdb_id = ? AND section_id = ? "
                "AND edition_key = ?",
                (media_type, tmdb_id, section_id, _dl_edition_key),
            ).fetchone()
            if override is None:
                override = conn.execute(
                    "SELECT * FROM user_overrides "
                    "WHERE media_type = ? AND tmdb_id = ? AND section_id = '' "
                    "AND edition_key = ?",
                    (media_type, tmdb_id, _dl_edition_key),
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
        # v1.18.2: collections nest under a `collections/` parent so
        # the themes tree reads `themes/<section_subdir>/...` for
        # regular media and `themes/collections/<section_subdir>/...`
        # for collection themes. Keeps the staging tree
        # legible at a glance — the user's request: "let's have all
        # themes go to collections folder then broken further into
        # movies, tv, anime like our normal folder structure just
        # within a collections folder." themes_subdir is already
        # per-section (Movies / 4K Movies / TV Shows / Anime / etc.)
        # so the existing leaf shape transposes cleanly under the
        # new collections/ parent.
        if media_type == "collection":
            media_root = media_root.parent / "collections" / media_root.name
        # v1.21.54 (B2a): per-edition downloads stage under a separate
        # leaf so an Extended theme doesn't overwrite the standard one.
        # _dl_edition_key resolved up-front (v1.21.92) for the override
        # scope; reused here for the staging path.
        out_dir = media_root / canonical_theme_subdir(
            theme["title"], theme["year"], _dl_edition_key)

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
            # v1.14.55: ORDER BY downloaded_at DESC so the LIMIT 1
            # winner is always the most-recent sibling. Pre-fix
            # SQLite picked any row (no ORDER BY); on multi-section
            # rows where a re-download landed in only one section,
            # the link could come from a stale sibling with mismatched
            # file_sha256/file_size — propagating into the local_files
            # row written for THIS section. Single-tenant low-impact
            # but the contract was wrong.
            # edition-blind OK (v1.21.94): content-addressed by
            # source_video_id — any edition's row for the same vid holds
            # byte-identical theme.mp3, so reusing it is correct. The
            # written row's edition_key comes from the payload via
            # _record_local_file; the hardlink target is the edition-aware
            # out_dir. Cross-section by design (section_id != ?).
            sibling = conn.execute(
                "SELECT file_path, file_sha256, file_size, source_video_id, "
                "       provenance, source_kind, downloaded_at "
                "FROM local_files "
                "WHERE media_type = ? AND tmdb_id = ? AND section_id != ? "
                "  AND source_video_id = ? "
                "ORDER BY downloaded_at DESC LIMIT 1",
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
                            _same_inode = (target_mp3.stat().st_ino
                                           == sibling_abs.stat().st_ino)
                        except OSError:
                            _same_inode = False
                        if not _same_inode:
                            # v1.22.73: link to a temp sibling then atomic
                            # replace — unlink-then-link was the v1.22.40
                            # destroy-before-fail class: a copy fallback
                            # failing (ENOSPC) after the unlink left
                            # local_files tracking a file that no longer
                            # exists, and the fall-through download's
                            # should_unlink stash never engaged.
                            _sib_tmp = target_mp3.with_name(
                                "theme.mp3.sib.tmp")
                            if _sib_tmp.exists():
                                _sib_tmp.unlink()
                            _safe_link(sibling_abs, _sib_tmp)
                            os.replace(_sib_tmp, target_mp3)
                    else:
                        _safe_link(sibling_abs, target_mp3)
                    rel_path = str(target_mp3.relative_to(self.settings.themes_dir))
                    self._record_local_file(
                        media_type=media_type, tmdb_id=tmdb_id,
                        section_id=section_id, rel_path=rel_path,
                        sha256=sibling["file_sha256"],
                        size=sibling["file_size"],
                        video_id=vid,
                        # v1.12.37: derive provenance + source_kind
                        # from the CURRENT presence of a user override,
                        # not from the sibling row's stale values. The
                        # pre-fix code inherited the sibling's values
                        # which was wrong whenever the row's source had
                        # changed since the sibling was recorded — most
                        # visibly after ACCEPT UPDATE deleted
                        # user_overrides on a U row, where the sibling
                        # still carried 'manual'/'url' and the new
                        # canonical inherited that even though it was
                        # downloaded with no override active. The row
                        # classified as U forever despite the canonical
                        # actually coming from the TDB URL.
                        provenance=("manual" if override else "auto"),
                        source_kind=("url" if override else "themerrdb"),
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

        # v0.51.280 (feature-brief D): adaptive-mode cooldown gate.
        # v0.51.294 (holistic review): hoisted ABOVE bucket.acquire() — a
        # cooldown bounce used to first BLOCK on a shared rate token and
        # then discard it on the no-op re-queue, starving fixed-rate slots. Fixed mode
        # (the default) skips this entirely — behavior byte-identical. A
        # provider in cooldown re-queues the job via the v1.14.54 attempt-free
        # seam BEFORE anything is touched (no stash, no download), jittered so
        # a same-provider queue cannot thunder back in one tick.
        if self.settings.download_rate_mode == "adaptive":
            from .provider_health import check_cooldown, provider_for_url
            _ph_provider = provider_for_url(yt_url)
            _cd = check_cooldown(self.settings.db_path, _ph_provider,
                                 base_rate=float(self.settings.download_rate_per_hour))
            if _cd > 0:
                self._mark_transient(
                    job["id"],
                    f"provider {_ph_provider} cooling down ({_cd}s)", _cd)
                return
        # Apply rate limit (only when we'll really hit YouTube)
        self.bucket.acquire()

        # v1.14.25: cooperative cancel checkpoint between bucket
        # acquire and the actual yt-dlp call. Pre-fix the user could
        # CANCEL a queued download but if the worker had just
        # acquired its rate-limit token the job would proceed
        # anyway — yt-dlp itself accepts no cancel callback so
        # mid-download cancel still requires × FORCE, but at least
        # bail at this safe yield point so a CANCEL clicked during
        # the rate-limit wait doesn't waste a bandwidth slot.
        # Audit ref: AUDIT_WORKER.md H2.
        if _is_cancelled(self.settings.db_path, job["id"]):
            log.info(
                "Job %d (download) cancelled at pre-yt-dlp checkpoint",
                job["id"],
            )
            self._mark_cancelled(job["id"])
            # v0.51.11 (round-4 audit HIGH): roll back the ACCEPT/REVERT prep on
            # cancel here too — this checkpoint marks+returns (no _JobCancelled
            # raised), so the run-loop's rollback branch never fires for it.
            self._run_rollback_safe(job, "cancelled at pre-yt-dlp checkpoint")
            return

        # If a previously downloaded theme.mp3 in THIS section was for a
        # different video (e.g. ThemerrDB URL changed since last sync), unlink
        # the stale one so the downloader's "already exists" short-circuit
        # doesn't keep us pinned to the old audio.
        # v1.11.99: also unlink (always) when the job carries
        # user_initiated_mismatch=true. Mismatch-flow downloads must
        # write to a fresh inode so the placement file's hardlink is
        # broken and the placement keeps its original content. Without
        # the explicit unlink, write_bytes via yt-dlp's postproc into
        # a still-hardlinked file would silently mutate the placement.
        payload = _safe_payload_parse(
            job["payload"], where="_do_download",
        )
        is_mismatch_job = bool(payload.get("user_initiated_mismatch"))
        with get_conn(self.settings.db_path) as conn:
            existing_local = conn.execute(
                "SELECT source_video_id FROM local_files "
                "WHERE media_type = ? AND tmdb_id = ? AND section_id = ? "
                "AND edition_key = ?",
                (media_type, tmdb_id, section_id, _dl_edition_key),
            ).fetchone()
        target_mp3 = out_dir / "theme.mp3"
        should_unlink = target_mp3.exists() and (
            is_mismatch_job
            or (existing_local and existing_local["source_video_id"] != vid)
        )
        # v1.22.40 (audit): rename-aside instead of destroy. should_unlink fires
        # on a source_video_id change — the TDB URL now points at a DIFFERENT
        # (often dead/private) video — so the re-download below frequently
        # FAILS. Pre-fix we unlinked theme.mp3 FIRST, leaving the row with no
        # canonical on disk (local_files points at nothing; a later place can't
        # hardlink). Now we stash it + restore on a failed download (below in
        # the except) + drop it on success. The atomic rename also defeats
        # download_theme's "already exists" reuse short-circuit, like the unlink.
        _stale_backup = None
        if should_unlink:
            _stale_candidate = target_mp3.with_name("theme.mp3.stale")
            try:
                target_mp3.replace(_stale_candidate)
                _stale_backup = _stale_candidate
            except OSError as e:
                log.warning("Could not stash stale theme.mp3 %s: %s", target_mp3, e)

        # v1.13.18 (6C): yt-dlp progress hook → shared dict so
        # /api/progress can render real % during a single-job
        # download. Cleared in finally so a failed/cancelled job
        # doesn't leave stale fractions visible across the next run.
        from . import progress as _progress
        job_id = job["id"]
        try:
            result = download_theme(
                youtube_url=yt_url,
                video_id=vid,
                output_dir=out_dir,
                cookies_file=self.settings.cookies_file,
                audio_quality=self.settings.download_audio_quality,
                # v1.13.53: route bypass settings through to yt-dlp
                # so users can pull geo-blocked videos via proxy /
                # XFF spoofing.
                geo_bypass=self.settings.download_geo_bypass,
                geo_bypass_country=self.settings.download_geo_bypass_country,
                proxy_url=self.settings.download_proxy_url,
                progress_callback=lambda f: _progress.set_download_progress(job_id, f),
            )
        except DownloadError as e:
            # v0.51.280: report the classified outcome (both modes; see above).
            try:
                from .provider_health import provider_for_url, report_outcome
                report_outcome(
                    self.settings.db_path, provider_for_url(yt_url), e.kind,
                    base_rate=float(self.settings.download_rate_per_hour),
                    min_rate=float(self.settings.download_adaptive_min_per_hour),
                    max_rate=float(self.settings.download_adaptive_max_per_hour))
            except Exception as _phe:  # noqa: BLE001
                log.warning("provider-health report failed: %s", _phe)
            # v1.22.40 (audit): the re-download failed — restore the stashed
            # canonical so the row keeps its working theme.mp3 on disk instead
            # of being left themeless after a dead-URL change.
            if _stale_backup is not None:
                try:
                    _stale_backup.replace(target_mp3)
                except OSError as _re:
                    log.warning("Could not restore stashed theme.mp3 %s: %s",
                                target_mp3, _re)
                _stale_backup = None
            kind = e.kind if hasattr(e, "kind") else FailureKind.UNKNOWN
            # Persist the failure on the theme row so the UI can show a badge.
            # v1.10.50: when the new failure_kind differs from what was
            # previously acked, clear failure_acked_at so the user gets
            # alerted on the new condition. Same kind as before keeps
            # the existing ack (no spam).
            # v1.14.55: wrap the four-statement sequence (snapshot
            # prior, UPDATE themes failure, conditional sfa DELETE,
            # cancel pending place job) in an explicit transaction
            # so concurrent readers (api_recovery_options, /api/stats,
            # the topbar poll, library list) don't observe an
            # intermediate state where themes.failure_kind is set
            # but the orphan place job is still pending — which
            # produced confusing "Skipped placement: source file
            # missing" events the user couldn't explain. get_conn is
            # autocommit (isolation_level=None) so without
            # transaction(conn) each execute commits independently.
            with get_conn(self.settings.db_path) as conn, transaction(conn):
                # v1.13.81: snapshot the prior failure_kind BEFORE the
                # UPDATE so we can decide whether per-section sfa rows
                # need to drop too. The CASE in the UPDATE clears the
                # title-global failure_acked_at when kind changes;
                # the per-section ack table must follow the same rule
                # — otherwise sections that were per-section-acked
                # with the OLD kind stay invisible to the FAIL pill
                # under the NEW kind. Audit P0 #2.
                prior = conn.execute(
                    "SELECT failure_kind FROM themes "
                    "WHERE media_type = ? AND tmdb_id = ?",
                    (media_type, tmdb_id),
                ).fetchone()
                prior_kind = prior["failure_kind"] if prior else None
                conn.execute(
                    """UPDATE themes SET failure_kind = ?, failure_message = ?,
                                          failure_at = ?,
                                          failure_acked_at = CASE
                                              WHEN failure_kind = ? THEN failure_acked_at
                                              ELSE NULL
                                          END
                       WHERE media_type = ? AND tmdb_id = ?""",
                    (kind.value, _truncate_err(str(e), 500), now_iso(),
                     kind.value, media_type, tmdb_id),
                )
                # Mirror the CASE: drop sfa only when kind actually
                # changes. First-time failures (prior_kind=None) and
                # same-kind re-failures both leave per-section acks
                # alone — same-spam-no-resurface contract.
                if prior_kind is not None and prior_kind != kind.value:
                    conn.execute(
                        """DELETE FROM section_failure_acks
                           WHERE media_type = ? AND tmdb_id = ?""",
                        (media_type, tmdb_id),
                    )
                # v1.10.40: also cancel any pending follow-up place job
                # for THIS section — without the file, place can't do
                # anything useful. Sibling-section places can still run
                # if their own download succeeded.
                # v1.22.73: edition-scoped via the payload (the v1.21.82
                # convention — jobs has no edition_key column). Pre-fix a
                # failed Extended download also cancelled the standard
                # edition's pending place whose own download succeeded:
                # theme downloaded, never placed until the hourly sweep.
                conn.execute(
                    """UPDATE jobs SET status = 'cancelled', finished_at = ?
                       WHERE job_type = 'place' AND media_type = ? AND tmdb_id = ?
                         AND section_id = ? AND status IN ('pending','running')
                         AND COALESCE(CASE WHEN json_valid(payload) THEN
                               json_extract(payload, '$.edition_key')
                             END, '') = ?""",
                    (now_iso(), media_type, tmdb_id, section_id,
                     _dl_edition_key),
                )
            level = "ERROR" if kind == FailureKind.COOKIES_EXPIRED else "WARNING"
            log_event(
                self.settings.db_path, level=level, component="download",
                media_type=media_type, tmdb_id=tmdb_id,
                message=f"Download failed for '{theme['title']}': {kind.human}",
                detail={"kind": kind.value, "youtube_url": yt_url,
                        "needs_manual_override": kind.needs_manual_override,
                        "raw": _truncate_err(str(e), 300)},
            )
            # v1.17.4: cookies_needed notification with 6h rate-limit.
            # Cookies-expired is a config issue (motif's cookies.txt
            # is stale / missing), not a per-row problem — the
            # operator needs to be told ONCE per ~6h window. Without
            # the rate-limit, a sync that hits 50 cookies-expired
            # downloads would ping the user 50 times. Cookies are
            # rarely "expired" briefly — once they go bad, every
            # subsequent download fails until the operator drops a
            # fresh cookies.txt. 6h gives a nudge cycle that's
            # noticeable but not spammy.
            if kind == FailureKind.COOKIES_EXPIRED:
                try:
                    from . import notify as _notify
                    from . import notify_dedupe as _dedupe
                    if _dedupe.should_fire(
                            self.settings.db_path, "cookies_needed",
                            rate_limit_seconds=6 * 3600):
                        _notify.dispatch(
                            self.settings.db_path,
                            self.settings.cfg.notifications,
                            event_kind="cookies_needed",
                            # v1.17.19: 🍪 emoji + drop "motif:".
                            title="🍪 YouTube cookies expired or missing",
                            # v1.19.92: dropped the leading "YouTube
                            # rejected … cookies_expired" line — it
                            # restated the title. Body leads with the
                            # fix (info the title can't hold).
                            body=(
                                f"Drop a fresh cookies.txt into /config "
                                f"(or your configured paths.cookies_file) "
                                f"and motif's next download attempt will "
                                f"recover. First detected on '"
                                f"{theme['title']}' — other rows will "
                                f"fail the same way until cookies are "
                                f"refreshed."
                            ),
                        )
                        _dedupe.record_fire(
                            self.settings.db_path, "cookies_needed")
                except Exception as ex:
                    log.debug(
                        "notify dispatch (cookies_needed) suppressed: %s",
                        ex)
            # v1.10.40: video-side permanent failures (removed/private/
            # age-restricted/geo-blocked) — retry won't help. Bypass
            # the worker's backoff schedule and mark the job 'failed'
            # immediately by raising _JobPermanentFailure. Transient
            # kinds (network/cookies/unknown) keep the original
            # retry-with-backoff behavior via plain `raise`.
            if kind.needs_manual_override:
                raise _JobPermanentFailure(str(e)) from e
            raise
        except Exception:
            # v1.22.86: restore the stash on ANY other failure too —
            # pre-fix only DownloadError restored, so an OSError out of
            # the post-download stat/sha or a mkdir failure left
            # theme.mp3 MISSING with theme.mp3.stale beside it, and a
            # retry's should_unlink check (target gone) never
            # re-stashed or restored.
            if _stale_backup is not None:
                try:
                    _stale_backup.replace(target_mp3)
                except OSError as _re:
                    log.warning("Could not restore stashed theme.mp3 "
                                "%s: %s", target_mp3, _re)
                _stale_backup = None
            raise

        # v0.51.280: feed the health state machine (BOTH modes — fixed mode
        # collects the evidence adaptive mode would act on). Never fatal.
        try:
            from .provider_health import provider_for_url, report_outcome
            report_outcome(
                self.settings.db_path, provider_for_url(yt_url), None,
                base_rate=float(self.settings.download_rate_per_hour),
                min_rate=float(self.settings.download_adaptive_min_per_hour),
                max_rate=float(self.settings.download_adaptive_max_per_hour))
        except Exception as e:  # noqa: BLE001 — health must not fail a download
            log.warning("provider-health report failed: %s", e)
        # v1.22.40 (audit): download succeeded — the stashed old canonical is
        # no longer active. v0.51.277 (feature-brief B): instead of dropping it,
        # hand the inode to the revision recorder — a MOVE into
        # themes_dir/.revisions/, zero copies, keep-last-2 retention. A
        # byte-identical redownload records nothing (the recorder dedupes on
        # the incoming sha) and the stash is then discarded here as before.
        if _stale_backup is not None:
            from .revisions import capture_revision
            try:
                _rev_id = capture_revision(
                    self.settings.db_path, self.settings.themes_dir,
                    media_type=media_type, tmdb_id=tmdb_id,
                    section_id=section_id, edition_key=_dl_edition_key or "",
                    reason="replaced_by_download",
                    stashed_file=_stale_backup,
                    incoming_sha=result.file_sha256)
            except Exception as e:  # noqa: BLE001 — history must not fail the download
                log.warning("revision capture failed for %s/%s: %s — old "
                            "canonical dropped without history",
                            media_type, tmdb_id, e)
                _rev_id = None
            if _rev_id is None and _stale_backup.exists():
                try:
                    _stale_backup.unlink()
                except OSError:
                    pass
        # Record the local file (per-section). source_kind drives the
        # T/U/A badge (v1.10.12): 'url' when the download was driven by
        # a user_overrides row; 'themerrdb' otherwise.
        rel_path = str(result.file_path.relative_to(self.settings.themes_dir))
        # v1.15.101: log the decision INPUTS so a future
        # provenance="manual" mystery has visible breadcrumbs.
        # the user reported a row where local_files.provenance stayed
        # "manual" after multiple REPLACE TDB calls — REPLACE TDB
        # deletes user_overrides, so the worker SHOULD see
        # override=None and write provenance="auto". The mystery
        # is what override (if any) is being picked up. This log
        # line reveals the exact (section_id, override-URL,
        # source-section) tuple the worker saw when picking
        # provenance — next reproduction has a paper trail.
        prov = "manual" if override else "auto"
        if prov == "manual" and override:
            log.info(
                "_do_download: provenance='manual' for %s/%s section=%s "
                "(override section=%s, url=%s)",
                media_type, tmdb_id, section_id,
                override["section_id"],
                override["youtube_url"],
            )
        # v0.51.188: condition the loudness BEFORE recording/placing. This is the cheap
        # half: Plex ingests theme.mp3 at scan time and then plays its own copy, so a
        # theme normalized before its FIRST placement needs no propagation at all — the
        # only copy Plex ever ingests is the conditioned one. (Re-normalizing an
        # already-placed theme is the expensive half; that's // NORMALIZE + a re-upload,
        # v0.51.185.) Default OFF: this mutates downloaded audio.
        sha256, size = result.file_sha256, result.file_size
        cond = None
        # v0.51.201 (Tag 6) + v0.51.203 (Phase 3): the on-arrival gate — an explicit
        # per-job choice (SET URL) wins, else auto-picks vs user downloads split by toggle.
        # Decision extracted to _should_condition_download so its 6 branches are unit-tested.
        _do_condition = _should_condition_download(self.settings, payload)
        if _do_condition:
            from .loudness_apply import condition_new_download
            cond = condition_new_download(
                result.file_path,
                target_lufs=self.settings.loudness_target_lufs)
            if cond.get("ok") and cond.get("changed"):
                # the file changed under us — record the POST-gain bytes, or the row's
                # sha/size describe a file that no longer exists and every staleness
                # check keyed on them (the v0.51.169 audit, health passes) reads wrong.
                sha256, size = cond["file_sha256"], result.file_path.stat().st_size
                log.info("download: conditioned %s/%s to %s LUFS (%+.1f dB) before "
                         "placement — Plex only ever ingests the leveled copy",
                         media_type, tmdb_id, cond["loudness_i"], cond["applied_db"])
            elif cond and not cond.get("ok"):
                # never let a loudness step fail a download: a raw theme beats no theme.
                log.warning("download: %s/%s arrived but could not be conditioned (%s) — "
                            "recording the raw download", media_type, tmdb_id,
                            cond.get("reason"))
        self._record_local_file(
            media_type=media_type, tmdb_id=tmdb_id, section_id=section_id,
            rel_path=rel_path, sha256=sha256, size=size,
            video_id=vid,
            provenance=prov,
            source_kind=("url" if override else "themerrdb"),
            job_payload=job["payload"],
            conditioned=cond,
        )

        log_event(
            self.settings.db_path, level="INFO", component="download",
            media_type=media_type, tmdb_id=tmdb_id, section_id=section_id,
            message=f"Downloaded theme for {theme['title']}",
            detail={"size": result.file_size, "video_id": vid,
                    "section_id": section_id},
        )

    def _record_local_file(
        self, *, media_type: str, tmdb_id: int, section_id: str,
        rel_path: str, sha256: str | None, size: int | None,
        video_id: str, provenance: str, source_kind: str,
        job_payload: str | None,
        conditioned: dict | None = None,
    ) -> None:
        """v1.11.0: shared write path for the post-download bookkeeping.

        Writes a per-section local_files row; clears the theme's
        failure flag ONLY when source_kind='themerrdb' (v1.13.74 —
        a user-URL or adopt success doesn't tell us anything about
        the TDB URL's health); then enqueues a place job for THIS
        section if auto_place is on. Shared by the real download
        path and the sibling-hardlink short-circuit so they stay in
        lockstep.
        """
        # v1.11.99: parse payload once up front so we can branch on
        # user_initiated_mismatch (set by api_manual_url when a
        # placement already exists for this row).
        payload = _safe_payload_parse(
            job_payload, where="_record_local_file",
        )
        is_mismatch_job = bool(payload.get("user_initiated_mismatch"))
        # v1.21.54 (B2a): the edition this download belongs to (resolved at
        # enqueue from the triggering rating_key via editions.py). '' = the
        # standard edition = today's behavior; no enqueue site sets a
        # non-'' value until C2, so this is inert in production for now.
        edition_key = payload.get("edition_key", "") or ""

        # v1.21.46: set inside the backup branch below, dispatched AFTER
        # the transaction commits so the Apprise HTTP call doesn't hold
        # the DB lock and enrich_item reads the committed stamp.
        did_backup_stamp = False

        with get_conn(self.settings.db_path) as conn, transaction(conn):
            # v1.13.74: only clear failure_kind when the successful
            # download was for the TDB URL itself. Pre-fix this clear
            # was unconditional, so a successful user-URL or adopt
            # download (source_kind in {'url','adopt','upload'}) wiped
            # the prior TDB-failure flag. The TDB pill (which reads
            # failure_kind via computeTdbPill) then flipped from red
            # `TDB ✗` back to green `TDB` even though the TDB URL
            # was still demonstrably dead. Reproduction: U row → user
            # clicks REPLACE-W-TDB → TDB download fails (video_removed)
            # → user clicks REVERT → user-URL download succeeds → red
            # pill became green. Now the TDB-failure state survives
            # the user-URL success, matching the schema's v1.10.50
            # design intent: "TDB pill still paints red so the user
            # knows the TDB-side URL is broken".
            if source_kind == "themerrdb":
                conn.execute(
                    """UPDATE themes SET failure_kind = NULL, failure_message = NULL,
                                          failure_at = NULL, failure_acked_at = NULL
                       WHERE media_type = ? AND tmdb_id = ? AND failure_kind IS NOT NULL""",
                    (media_type, tmdb_id),
                )
                # v1.13.81: also drop matching section_failure_acks
                # rows. The themes-level clear is title-global; the
                # per-section table must follow suit, otherwise
                # subsequent failures land with stale sfa rows still
                # marking those sections as "acked" — the FAIL pill
                # tab breakdown + library SQL filter on `sfa.acked_at
                # IS NULL` and silently exclude them. Audit P0 #1.
                conn.execute(
                    """DELETE FROM section_failure_acks
                       WHERE media_type = ? AND tmdb_id = ?""",
                    (media_type, tmdb_id),
                )
            elif source_kind in ("url", "upload"):
                # v1.14.8: a successful user-override download (SET URL
                # or UPLOAD MP3) functionally resolves the per-section
                # ATTN axis even though the underlying TDB URL may
                # still be broken. Pre-fix the row kept rendering the
                # red ⚠ glyph and counting in `N FAIL` indefinitely
                # because nothing in the success path acked the
                # section — the user had to click ACK FAILURE
                # manually after providing their working override.
                # the user's repro: SET URL → user URL downloads + places
                # → INFO dialog says "✓ RESOLVED — TDB UNAVAILABLE"
                # but the row pill still glyphs red ⚠.
                #
                # Mirror v1.10.50's design intent: the title-global
                # `failure_kind` (drives the red TDB ✗ pill) survives —
                # the user should still see "TDB-side URL is broken".
                # Only the per-section ATTN ack lands. Stamped with
                # `acked_by='auto:user_override'` so manual vs
                # synthetic acks are distinguishable in the audit log.
                conn.execute(
                    """INSERT INTO section_failure_acks
                       (media_type, tmdb_id, section_id, acked_at, acked_by)
                       SELECT ?, ?, ?, ?, 'auto:user_override'
                       WHERE EXISTS (
                           SELECT 1 FROM themes t
                           WHERE t.media_type = ?
                             AND t.tmdb_id = ?
                             AND t.failure_kind IS NOT NULL
                             AND t.failure_acked_at IS NULL
                       )
                       ON CONFLICT(media_type, tmdb_id, section_id) DO NOTHING""",
                    (media_type, tmdb_id, section_id, now_iso(),
                     media_type, tmdb_id),
                )
            mismatch_value = "pending" if is_mismatch_job else None
            conn.execute(
                """
                INSERT INTO local_files
                    (media_type, tmdb_id, section_id, edition_key, file_path,
                     file_sha256, file_size,
                     downloaded_at, source_video_id, provenance, source_kind,
                     mismatch_state,
                     loudness_i, loudness_tp, loudness_lra, loudness_measured_at,
                     loudness_measured_sha256, norm_state, norm_gain_db, norm_target,
                     norm_at, norm_orig_sha256, norm_orig_pcm_sha256)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(media_type, tmdb_id, section_id, edition_key) DO UPDATE SET
                    file_path = excluded.file_path,
                    file_sha256 = excluded.file_sha256,
                    file_size = excluded.file_size,
                    downloaded_at = excluded.downloaded_at,
                    source_video_id = excluded.source_video_id,
                    provenance = excluded.provenance,
                    source_kind = excluded.source_kind,
                    mismatch_state = excluded.mismatch_state,
                    -- v0.51.188: a re-download REPLACES the bytes, so its loudness and
                    -- normalize state replace the old row's too. Carrying a stale
                    -- norm_state='normalized' onto fresh raw bytes would tell // UNDO to
                    -- un-gain a file that was never gained.
                    -- v0.51.233 (audit): that premise holds for a REAL re-download but NOT
                    -- for the two paths that return without replacing anything —
                    -- download_theme short-circuits when the expected mp3 already exists
                    -- (any same-source re-download, e.g. // DOWNLOAD TDB BACKUP with
                    -- normalize-on-download off), and the sibling-hardlink branch passes no
                    -- `conditioned` at all. Both then wrote (None,)*11 over a row that was
                    -- genuinely leveled: motif reported it raw while the file on disk was
                    -- still gained AND still carried mp3gain's APEv2 undo tag, and // UNDO
                    -- refuses on norm_state != 'normalized' — the original audio was no
                    -- longer restorable through motif. Gate on the sha: identical bytes
                    -- mean nothing was re-encoded or re-gained, so the existing loudness
                    -- and normalize state still describe this exact file.
                    loudness_i = CASE WHEN local_files.file_sha256 IS NOT excluded.file_sha256 THEN excluded.loudness_i
                        ELSE local_files.loudness_i END,
                    loudness_tp = CASE WHEN local_files.file_sha256 IS NOT excluded.file_sha256 THEN excluded.loudness_tp
                        ELSE local_files.loudness_tp END,
                    loudness_lra = CASE WHEN local_files.file_sha256 IS NOT excluded.file_sha256 THEN excluded.loudness_lra
                        ELSE local_files.loudness_lra END,
                    loudness_measured_at = CASE WHEN local_files.file_sha256 IS NOT excluded.file_sha256 THEN excluded.loudness_measured_at
                        ELSE local_files.loudness_measured_at END,
                    loudness_measured_sha256 = CASE WHEN local_files.file_sha256 IS NOT excluded.file_sha256 THEN excluded.loudness_measured_sha256
                        ELSE local_files.loudness_measured_sha256 END,
                    norm_state = CASE WHEN local_files.file_sha256 IS NOT excluded.file_sha256 THEN excluded.norm_state
                        ELSE local_files.norm_state END,
                    norm_gain_db = CASE WHEN local_files.file_sha256 IS NOT excluded.file_sha256 THEN excluded.norm_gain_db
                        ELSE local_files.norm_gain_db END,
                    norm_target = CASE WHEN local_files.file_sha256 IS NOT excluded.file_sha256 THEN excluded.norm_target
                        ELSE local_files.norm_target END,
                    norm_at = CASE WHEN local_files.file_sha256 IS NOT excluded.file_sha256 THEN excluded.norm_at
                        ELSE local_files.norm_at END,
                    norm_orig_sha256 = CASE WHEN local_files.file_sha256 IS NOT excluded.file_sha256 THEN excluded.norm_orig_sha256
                        ELSE local_files.norm_orig_sha256 END,
                    norm_orig_pcm_sha256 = CASE WHEN local_files.file_sha256 IS NOT excluded.file_sha256 THEN excluded.norm_orig_pcm_sha256
                        ELSE local_files.norm_orig_pcm_sha256 END
                """,
                (media_type, tmdb_id, section_id, edition_key, rel_path,
                 sha256, size,
                 now_iso(), video_id, provenance, source_kind, mismatch_value,
                 *_cond_columns(conditioned, sha256)),
            )
            # v1.20.10: motif now HOLDS this TDB theme (local_files just
            # written) — resolve any pending new_theme_available update for
            # this section so the blue !UPD pill clears on download SUCCESS,
            # not optimistically at click time (Finding 2: a click-time clear
            # left the pill wrong after a failed backup). Scoped to TDB
            # downloads — a user-URL / upload success doesn't mean the user
            # took TDB's published theme. Self-heals: any TDB download path
            # (ACCEPT UPDATE / DOWNLOAD TDB BACKUP / REPLACE TDB) clears it.
            if source_kind == "themerrdb":
                resolve_new_theme_pending_update(
                    conn, media_type=media_type, tmdb_id=tmdb_id,
                    section_id=section_id, decided_by="auto:download",
                )
            # v1.11.99: skip auto-place on mismatch jobs. The new
            # canonical sits in /themes; the placement file in the Plex
            # folder is intentionally untouched. User resolves via the
            # row's PLACE menu (PUSH TO PLEX / ADOPT FROM PLEX /
            # KEEP MISMATCH) or /pending.
            if is_mismatch_job:
                return

            # Decide whether to auto-place. Per-job payload override wins;
            # otherwise fall back to the global placement.auto_place setting.
            auto_place = self.settings.auto_place_default
            force_place = False
            if "auto_place" in payload:
                auto_place = bool(payload["auto_place"])
            if payload.get("force_place"):
                force_place = True
                auto_place = True  # force implies auto
            if auto_place:
                # v1.21.63 (C2c): propagate the download's edition_key into
                # the chained place job so REDOWNLOAD's auto-place targets
                # the SAME edition's folder (not the standard ''). '' =
                # today's payload exactly.
                _place_obj: dict = {}
                if force_place:
                    _place_obj["force"] = True
                    # v1.23.91/93/94: the chained place job's theme_pushed reason
                    # tracks the DOWNLOAD's reason (via _DOWNLOAD_PUSH_REASON)
                    # instead of a blanket "replace_with_themerrdb", so the
                    # notification reflects the real action — DOWNLOAD TDB /
                    # SET URL / REPLACE TDB. Unlisted reasons (ACCEPT UPDATE, the
                    # REPLACE TDB action) fall to the REPLACE-TDB default.
                    _place_obj["reason"] = _DOWNLOAD_PUSH_REASON.get(
                        payload.get("reason"), "replace_with_themerrdb")
                if edition_key:
                    _place_obj["edition_key"] = edition_key
                # v1.23.46: carry the bulk flag onto the chained place job so
                # the theme_added/pushed notification it fires coalesces into
                # one list. The download→place chain otherwise drops all action
                # context (only the reason is threaded, via _DOWNLOAD_PUSH_REASON
                # above), so this is the ONLY way the place-side dispatch knows it
                # came from a bulk action.
                if payload.get("bulk"):
                    _place_obj["bulk"] = True
                place_payload = json.dumps(_place_obj)
                conn.execute(
                    """
                    INSERT INTO jobs (job_type, media_type, tmdb_id, section_id,
                                      payload, status, created_at, next_run_at)
                    VALUES ('place', ?, ?, ?, ?, 'pending', ?, ?)
                    """,
                    (media_type, tmdb_id, section_id, place_payload,
                     now_iso(), now_iso()),
                )
            else:
                # v1.19.21: backup-only intent. Pre-fix the hourly
                # retry sweep saw local_files + no placement and
                # enqueued a place job anyway, defeating the user's
                # "DOWNLOAD TDB BACKUP" intent. Stamp
                # last_place_attempt_reason='backup_only' so the
                # sweep recognizes the intent and skips. The reason
                # is the same shape the sweep ALREADY filters on
                # for 'plex_has_theme' / 'existing_theme:*'. Repro:
                # Indiana Jones (movie/335977 sec=1) — download
                # came from bulk_backup, then the retry sweep
                # enqueued place jobs that hit plex_has_theme +
                # never installed.
                # v1.19.32: also stamp for ACCEPT UPDATE on a P-row
                # (api_accept_update P-row branch enqueues with this
                # reason + auto_place=False). Same intent shape —
                # the user accepted the new TDB URL as a fresh
                # backup; Plex keeps serving its own theme. Without
                # the stamp, the BK badge wouldn't surface AND the
                # hourly retry sweep would re-enqueue a place job
                # on next tick, defeating the P-preservation fix.
                # v1.19.33: extended to cover four more backup-intent
                # writers that were silently NULL'ing the stamp,
                # defeating the BK badge + retry-skip contract for
                # users who didn't go through bulk_backup. The audit
                # surfaced two pre-existing v1.19.21 gaps —
                # `manual_url` (SET URL + KEEP AS BACKUP, api.py
                # ~10947 auto_place=False branch) and `manual_backup`
                # (per-row DOWNLOAD TDB BACKUP, api.py ~16826) — plus
                # two new v1.19.33 paths for REVERT on a P-row.
                # One-conceptual-surface-multiple-writers (CLAUDE.md
                # class 9 sub-pattern): every writer that lands a
                # local_file with backup intent must reach this stamp.
                payload_reason = payload.get("reason") or ""
                if payload_reason in (
                    "bulk_backup",
                    "manual_backup",
                    "manual_url",
                    # v1.21.36: CSV import "download as backup" (the
                    # force_place=false variant, default on P-rows since
                    # v1.21.34). Pre-fix its 'bulk_import' reason was
                    # absent here, so an auto_place=off install never got
                    # the backup_only stamp → no UB badge + retry-sweep
                    # churn. The replace variant uses 'bulk_import' and
                    # forces auto_place=True, so it never reaches here.
                    "bulk_import_backup",
                    "upstream_update_accepted_p_backup",
                    "revert_to_user_url_p_backup",
                    "revert_to_themerrdb_url_p_backup",
                ):
                    conn.execute(
                        "UPDATE local_files SET "
                        "  last_place_attempt_at = ?, "
                        "  last_place_attempt_reason = 'backup_only' "
                        "WHERE media_type = ? AND tmdb_id = ? "
                        "  AND section_id = ? AND edition_key = ?",
                        (now_iso(), media_type, tmdb_id, section_id,
                         edition_key),
                    )
                    did_backup_stamp = True

        # v1.21.46: a theme just DOWNLOADED as a backup (Plex / the
        # existing theme keeps serving — nothing was placed). Pre-fix
        # this was silent: theme_added/theme_pushed in _do_place are
        # gated on `outcome.placed`, so a backup never fired any
        # notification. the user's report: a bulk ACCEPT on newly-added
        # TDB P-rows landed N backups (link state TB) with no ping.
        # Coalesced so a bulk collapses to one summary while the first
        # backup pings immediately via the leading edge; theme_backed_up
        # uses the 60s window (notify._COALESCE_WINDOW_BY_KIND). Outside
        # the transaction so the Apprise HTTP call never holds the lock.
        if did_backup_stamp:
            try:
                from . import notify as _notify
                from . import notify_content as _nc
                ctx = _nc.enrich_item(
                    self.settings.db_path,
                    media_type=media_type, tmdb_id=tmdb_id,
                    section_id=section_id,
                    edition_key=edition_key,  # v1.21.75: name the cut
                )
                _notify.dispatch_coalesced(
                    self.settings.db_path,
                    self.settings.cfg.notifications,
                    event_kind="theme_backed_up",
                    item_label=_nc.format_theme_backed_up_item(ctx),
                    single_title=_nc.format_theme_backed_up_title(ctx),
                    single_body=_nc.format_theme_backed_up_body(ctx),
                    batch_title_fn=_nc.format_theme_backed_up_batch_title,
                    batch_body_fn=_nc.format_theme_backed_up_batch_body,
                    body_format="markdown",
                    single_attach_url=_nc.attachment_thumb_url(ctx),
                    # v1.23.46: bulk only when the download was bulk-flagged.
                    bulk=bool(payload.get("bulk")),
                    # v1.23.75: the item's library, for batch section grouping.
                    section=ctx.get("section_label") or "",
                )
            except Exception as e:
                log.debug(
                    "notify dispatch (theme_backed_up) suppressed: %s", e)

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
            raise _JobTransient("themes_dir not configured — set it on /settings",
                                retry_after_seconds=3600)  # v0.51.294: was RuntimeError — it burned the 3-attempt budget in ~6min and terminal-failed, contradicting its own log line

        with get_conn(self.settings.db_path) as conn:
            theme = conn.execute(
                "SELECT * FROM themes WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            ).fetchone()
            # v1.21.65: read the SOURCE file for THIS edition — prefer the
            # edition's own local_files, fall back to the shared '' row — so
            # a per-edition place job (post-C2c per-edition downloads) stages
            # the right theme instead of an arbitrary sibling's. '' rows
            # resolve the same shared row as before (behavior-preserving).
            _src_edition = _safe_payload_parse(
                job["payload"], where="_do_place (edition src)",
            ).get("edition_key", "") or ""
            local = conn.execute(
                "SELECT * FROM local_files "
                "WHERE media_type = ? AND tmdb_id = ? AND section_id = ? "
                "  AND edition_key IN (?, '') "
                "ORDER BY (edition_key = ?) DESC LIMIT 1",
                (media_type, tmdb_id, section_id, _src_edition,
                 _src_edition),
            ).fetchone()
        # v1.22.73: the edition of the local_files row that actually serves
        # this place. The read above falls back to the shared '' row, but
        # the post-outcome local_files stamps keyed the requested/landed
        # edition — when the '' row served, those UPDATEs matched 0 rows
        # (stale mismatch_state + last_place_attempt_reason; the hourly
        # sweep's p.edition_key = lf.edition_key join then re-enqueued
        # place jobs indefinitely).
        _lf_edition = (local["edition_key"] if local is not None
                       else _src_edition)

        # In dry-run, the local file may not exist yet (download was skipped).
        # We can still report what placement *would* have done by running the
        # folder-index lookup without touching anything.
        dry = is_dry_run(self.settings.db_path, default=self.settings.dry_run_default)

        if theme is None:
            raise RuntimeError("theme missing")

        # v1.18.0: collections take the HTTP-upload path. No folder
        # exists for a collection (Plex stores its theme inside its
        # metadata bundle, not on disk), so FolderIndex + place_theme
        # both make zero sense for them. Delegate the entire place
        # flow — dry-run handling, force_overwrite parsing, source
        # file read, rk resolution, placements upsert, notification
        # firing — to a collection-specific helper that mirrors the
        # surrounding shape but swaps the hardlink for a
        # PlexClient.upload_collection_theme() POST.
        if media_type == "collection":
            self._do_place_collection(job=job, theme=theme, local=local)
            return

        # v1.18.23: kind-aware dispatch for movie/TV rows.
        # Per-job payload "kind": 'file' (sidecar hardlink) or
        # 'api' (Plex metadata upload). When absent, falls back to
        # the global setting `placement.default_method`. The
        # PUSH TO PLEX (FILE) / PUSH TO PLEX (API) menu items
        # write the kind explicitly; auto-place from sync uses
        # the global default. The `_do_place_collection` helper
        # already handles every API-upload concern (rk resolve,
        # placements upsert with media_folder='', notification
        # dispatch), so this branch delegates to it for movie/TV
        # too — the helper is media_type-agnostic at the SQL/HTTP
        # level. The misleading collection-specific function
        # name is preserved for back-compat with v1.18.0+ call
        # sites; a future tag can rename it to _do_plex_upload.
        _place_pp = _safe_payload_parse(
            job["payload"], where="_do_place (kind dispatch)",
        )
        payload_kind = _place_pp.get("kind")
        # v1.21.55 (B2b): the edition this place job targets (resolved at
        # enqueue from the triggering rating_key via editions.py). '' =
        # standard = today; no enqueue site sets non-'' until C2, so
        # place_theme still gets edition_raw='' in production (inert).
        place_edition_key = _place_pp.get("edition_key", "") or ""
        effective_kind = payload_kind or (
            self.settings.default_placement_method
        )
        if effective_kind == "api":
            self._do_place_collection(job=job, theme=theme, local=local)
            return

        if dry:
            # Just resolve the target folder and Plex theme presence, log it
            from .placement import find_target_folder
            index = self._index_for_section(section_id)
            find = find_target_folder(
                index, title=theme["title"], year=theme["year"] or "",
                edition_raw=place_edition_key,
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
        payload = _safe_payload_parse(
            job["payload"], where="_do_place (force flag)",
        )
        if payload.get("force"):
            force_overwrite = True

        index = self._index_for_section(section_id)
        # v0.51.16 (audit #23): the PlexClient (eager httpx.Client) is created
        # BELOW, just before place_theme — it used to be created here, ~160
        # lines and several early returns/raises before the only finally that
        # closed it, leaking a client + open sockets on every cancel-checkpoint
        # bail or DB error in the pi-resolution block. Nothing in that span
        # uses it.
        # v1.11.39: pre-resolve rating_key + has_theme from plex_items
        # so place_theme doesn't have to hit Plex for them.
        # v1.11.68: also resolve through pi.theme_id (the v1.11.43 /
        # v1.11.64 stamp) so orphan-backed rows (SET URL / UPLOAD MP3
        # / adopt without a TDB match — synthetic-negative tmdb_id)
        # correctly find their plex_items row. Pre-fix the SELECT was
        # WHERE guid_tmdb=? which only worked for non-orphan rows;
        # orphans matched nothing, cached_rk stayed None, and
        # placement fell back to FolderIndex title+year matching that
        # ignored the {edition-4K} suffix on the actual Plex folder.
        # Also pull folder_path so place_theme can target that folder
        # directly when available — skips the strict_edition mismatch
        # entirely for known-rk placements.
        plex_mt = "show" if media_type == "tv" else "movie"
        # v1.19.21: pull plex_theme_verified_ok in both SELECTs
        # — needed for the stale-cache override below (force
        # placement when has_theme=1 but verified_ok=0).
        # v1.21.66 / v1.24.18 (edition sweep #1): resolve cached_rk for the job's
        # OWN edition, not an arbitrary sibling. Pre-v1.21.66 REPLACE TDB / PUSH
        # resolved whichever rk LIMIT 1 returned (the user: REPLACE TDB on "Sam
        # Takes a Step" uploaded into the Theatrical rk). v1.21.66 scoped TAGGED
        # editions but left the STANDARD '' edition on the unscoped path via
        # `if place_edition_key` — so a standard-edition place on a multi-edition
        # shared-section title STILL grabbed a sibling (theme hardlinked into the
        # wrong folder + wrong-rk refresh + a split-brain re-enqueue loop). '' is
        # a REAL edition: scope by PRESENCE, always. The '' fallback below covers
        # a default-'' payload on a title with no '' row (the held carve-out).
        _ed_clause = " AND pi.edition_key = ?"
        _ed_clause2 = " AND edition_key = ?"
        _ed_params = (place_edition_key,)
        with get_conn(self.settings.db_path) as conn:
            # v0.51.253: ORDER BY liveness on all three resolves below. A disk
            # dropping out and coming back makes Plex DELETE the items and
            # re-add them with NEW rating keys; the reaper holds the dead rows
            # for _REAP_MISS_THRESHOLD enums (v0.51.128 anti-glitch grace), so
            # the table legitimately holds a dead row and a live row for the
            # same title. resolve_theme_ids links BOTH to the theme, so the
            # JOIN matched both and a bare LIMIT 1 returned whichever SQLite
            # yielded first — the OLDER, dead one. Every upload then 404'd
            # against a rating key Plex had already deleted (the user's
            # 2026-08-09 disk incident: ~80 movies, Pacific Rim rk 137499 dead
            # vs 738854 live, both theme_id=2474). consecutive_missing=0 rows
            # are the ones Plex returned THIS enum; last_seen_at breaks ties.
            pi = conn.execute(
                """SELECT pi.rating_key, pi.has_theme, pi.folder_path,
                          pi.local_theme_file, pi.plex_independent_theme,
                          pi.plex_theme_verified_ok
                   FROM plex_items pi
                   INNER JOIN themes t
                     ON t.id = pi.theme_id
                    AND t.media_type = ?
                    AND t.tmdb_id = ?
                   WHERE pi.section_id = ?""" + _ed_clause + """
                   ORDER BY pi.consecutive_missing ASC,
                            pi.last_seen_at DESC
                   LIMIT 1""",
                (media_type, tmdb_id, section_id) + _ed_params,
            ).fetchone()
            if pi is None:
                # Fall back to the v1.11.39 path (covers freshly-
                # downloaded T rows whose pi.theme_id may not have
                # caught up yet via resolve_theme_ids).
                pi = conn.execute(
                    "SELECT rating_key, has_theme, folder_path, "
                    "       local_theme_file, plex_independent_theme, "
                    "       plex_theme_verified_ok "
                    "FROM plex_items "
                    "WHERE guid_tmdb = ? AND media_type = ? "
                    "  AND section_id = ?" + _ed_clause2
                    + " ORDER BY consecutive_missing ASC, last_seen_at DESC"
                    " LIMIT 1",
                    (tmdb_id, plex_mt, section_id) + _ed_params,
                ).fetchone()
            if pi is None and not place_edition_key:
                # v1.24.18: a STANDARD ('') place matched no '' plex_items row
                # (e.g. a default-'' payload on a title whose only folder is
                # tagged {edition-X}, or a stale pre-edition job). Fall back to
                # the UNSCOPED resolve so a single-folder title still lands.
                # v1.24.24 (code-review): only fall back when there is EXACTLY ONE
                # candidate. An arbitrary LIMIT-1 pick across 2+ tagged siblings
                # (LotR: Theatrical/Extended/Sam, no '' row) would hardlink the
                # theme into the WRONG folder + refresh the wrong rk (cached_folder
                # _path bypasses find_target_folder). When ambiguous, leave pi=None
                # → place_theme's find_target_folder cleanly no-matches the tagged
                # folders under strict_edition. TAGGED editions still never fall
                # back (v1.21.66 strict: a tagged miss must not grab a sibling).
                # v0.51.253: count the ambiguity over LIVE rows only. The
                # v1.24.24 guard is about multi-EDITION siblings, but a
                # re-added title (dead row + live row, same edition) also
                # reads as 2 candidates and bailed — turning a recoverable
                # place into a no-match. Two LIVE rows is still genuinely
                # ambiguous and still bails. When NOTHING is live (a whole-
                # section Plex glitch), fall back to the pre-v0.51.253
                # count so a transient miss doesn't newly strand a place.
                _cands = conn.execute(
                    "SELECT rating_key, has_theme, folder_path, "
                    "       local_theme_file, plex_independent_theme, "
                    "       plex_theme_verified_ok, consecutive_missing "
                    "FROM plex_items "
                    "WHERE guid_tmdb = ? AND media_type = ? "
                    "  AND section_id = ? "
                    "ORDER BY consecutive_missing ASC, last_seen_at DESC "
                    "LIMIT 3",
                    (tmdb_id, plex_mt, section_id),
                ).fetchall()
                _live = [c for c in _cands if not c["consecutive_missing"]]
                if len(_live) == 1:
                    pi = _live[0]
                elif not _live and len(_cands) == 1:
                    pi = _cands[0]
        cached_rk = pi["rating_key"] if pi else None
        cached_has_theme = bool(pi and pi["has_theme"])
        cached_folder_path = (pi["folder_path"] if pi else None) or None
        # v1.19.21: don't trust stale Plex cache. When Plex CLAIMS
        # to have a theme (has_theme=1) but our own HEAD
        # verification said the claim is broken (verified_ok=0),
        # the cache is lying — Plex's /library/metadata/{rk}/theme
        # endpoint returns 404 even though the metadata XML
        # says a theme exists. Force the placement: motif's file
        # is good, the only thing stopping playback is Plex's
        # stale cache, and a fresh placement + refresh will
        # repair the cache. Mirrors the SRC SQL's v1.12.112
        # "verified_ok=0 demotes 'P' to '-'" logic — same
        # signal used both for classification and for action.
        # Repro: the user's Indiana Jones (movie/335977 sec=1)
        # had cached has_theme=1 + verified_ok=0 + no placement;
        # every place job was being skipped indefinitely.
        if cached_has_theme and pi is not None:
            cached_verified_ok = pi["plex_theme_verified_ok"] \
                if "plex_theme_verified_ok" in pi.keys() else None
            if cached_verified_ok == 0:
                log.info(
                    "place %s/%s sec=%s: Plex cached has_theme=1 "
                    "but plex_theme_verified_ok=0 — cache is "
                    "stale, forcing placement to repair",
                    media_type, tmdb_id, section_id,
                )
                cached_has_theme = False  # treat as unset
        # v1.13.55: pre-place +P capture. If Plex currently serves
        # its own theme (has_theme=1) AND there's no sidecar yet
        # (local_theme_file=0) AND we haven't observed
        # plex_independent_theme yet, stamp it 1 BEFORE the place
        # runs. Pre-fix a row that was P (Plex serves cloud /
        # themerr-plex embed) and went through REPLACE TDB lost
        # the +P composite signal: motif's sidecar landed,
        # local_theme_file flipped to 1, and the next plex_enum
        # batch's indep_observation became None (sidecar=1
        # branch), so COALESCE preserved the prior NULL forever.
        # The frontend renders the +P yellow dot only when
        # plex_independent_theme === 1, so the row appeared as
        # bare T with no composite indicator. Capturing here at
        # the latest observable moment closes the window.
        # v1.22.18 (audit MED #9): gate on cached_has_theme, not the raw
        # pi["has_theme"]. The stale-cache override above demotes
        # cached_has_theme to False when Plex CLAIMS has_theme=1 but our
        # HEAD verify said verified_ok=0 (the claim is a lie — /theme
        # 404s). Pre-fix the +P stamp read the raw has_theme=1 and marked
        # the row plex_independent_theme=1 anyway, so a row whose Plex
        # theme is actually broken rendered the +P yellow dot. Indiana
        # Jones (movie/335977) repro: has_theme=1 + verified_ok=0.
        if (pi is not None and cached_rk
                and cached_has_theme
                and not pi["local_theme_file"]
                and pi["plex_independent_theme"] is None):
            try:
                with get_conn(self.settings.db_path) as conn:
                    conn.execute(
                        "UPDATE plex_items "
                        "   SET plex_independent_theme = 1 "
                        " WHERE rating_key = ? "
                        "   AND plex_independent_theme IS NULL "
                        "   AND has_theme = 1 "
                        "   AND local_theme_file = 0 "
                        "   AND COALESCE(plex_theme_verified_ok, 1) <> 0",
                        (cached_rk,),
                    )
            except Exception as e:
                # Capture is best-effort — never fail the place
                # because a +P backfill UPDATE hit a race.
                log.debug("pre-place +P capture failed for rk=%s: %s",
                          cached_rk, e)
        # v1.14.25: cooperative cancel checkpoint after FolderIndex
        # build / pre-place capture, before the actual hardlink + Plex
        # nudge. The FolderIndex build can take a few seconds on a
        # cold cache (10K-folder library); a CANCEL clicked during
        # that window deserves to bail before the place + refresh
        # I/O fires. Audit ref: AUDIT_WORKER.md H2.
        if _is_cancelled(self.settings.db_path, job["id"]):
            log.info(
                "Job %d (place) cancelled at pre-place_theme checkpoint",
                job["id"],
            )
            self._mark_cancelled(job["id"])
            return
        # v0.51.16 (audit #23): created HERE — first use is the call below,
        # and the finally that closes it now covers its whole lifetime.
        plex_client = self._plex_client()
        try:
            outcome = place_theme(
                media_type=media_type,
                title=theme["title"],
                year=theme["year"] or "",
                edition_raw=place_edition_key,  # v1.21.55 (B2b): per-edition
                source_file=source_file,
                index=index,
                plex=plex_client,
                cached_rk=cached_rk,
                cached_has_theme=cached_has_theme,
                cached_folder_path=cached_folder_path,
                strict_edition=self.settings.strict_edition_match,
                plus_mode=self.settings.plus_equiv_mode,  # type: ignore[arg-type]
                skip_if_plex_has_theme=not force_overwrite,
                force_overwrite=force_overwrite,
                analyze_after=self.settings.plex_analyze_after,
            )
        finally:
            if plex_client:
                plex_client.close()

        # v0.50.89 (audit LOW): deliberately NO cancel checkpoint between
        # here and the DB writes below. place_theme()'s hardlink/copy is
        # atomic (temp+rename) and has already landed on disk by this
        # point — a checkpoint here would mean CANCEL can produce a file
        # that's genuinely placed with no local_files/placements row
        # describing it, which is a worse inconsistency than the narrow
        # "cancel didn't take effect in time" UX gap it would close. The
        # one real checkpoint stays pre-place_theme (above), before any
        # disk I/O commits.
        if outcome.placed:
            # v1.20.13: wrap the placements UPSERT + mismatch-clear in one
            # transaction (was bare autocommit). Pre-fix a concurrent
            # reader — the library SQL, /api/stats, or a sibling worker's
            # place job — could land in the gap and see the placement row
            # present (LINK=HL/C) while local_files.mismatch_state was
            # still 'pending' (DL amber + ≠): a transient inconsistent
            # render. The sibling collection path (below) already wraps the
            # same logical write; this one was missed by v1.15.104 (audit
            # round 3 MED).
            with get_conn(self.settings.db_path) as conn, transaction(conn):
                placement_provenance = local["provenance"] or "auto"
                # v1.21.55 (B2b): key the placement by the edition of the
                # folder it PHYSICALLY landed in — so Phase C's library JOIN
                # (plex_items.edition_key, also folder-derived) matches the
                # placement to the right edition row. '' for a standard
                # folder = today.
                placed_edition_key = edition_key_for_folder(
                    str(outcome.target_folder))
                conn.execute(
                    """
                    INSERT INTO placements
                        (media_type, tmdb_id, section_id, edition_key,
                         media_folder, placed_at,
                         placement_kind, plex_rating_key, plex_refreshed, provenance)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(media_type, tmdb_id, section_id, media_folder, edition_key) DO UPDATE SET
                        placed_at = excluded.placed_at,
                        placement_kind = excluded.placement_kind,
                        plex_rating_key = excluded.plex_rating_key,
                        plex_refreshed = excluded.plex_refreshed,
                        provenance = excluded.provenance
                    """,
                    (media_type, tmdb_id, section_id, placed_edition_key,
                     str(outcome.target_folder),
                     now_iso(), outcome.kind, outcome.plex_rating_key,
                     1 if outcome.plex_refreshed else 0, placement_provenance),
                )
                # v1.11.99: a successful place pushes canonical → placement,
                # so by definition canonical now matches placement and any
                # prior mismatch ('pending' or 'acked') is resolved. Clear
                # the flag so the row's DL goes back to green and the LINK
                # column flips from ≠ back to = / C.
                # v1.21.55 (B2b): scope to the placed edition's local_files
                # row so an Extended place doesn't clear the standard row's
                # mismatch flag.
                # v1.22.18 (audit MED #11) keyed this on placed_edition_key
                # (the physical landing folder, matching the placements
                # INSERT). v1.22.73 supersedes: local_files stamps must hit
                # the SOURCE row the place read — _lf_edition, possibly the
                # shared '' fallback. The placed/requested editions may have
                # no local_files row at all, making the clear a 0-row no-op
                # (stale mismatch_state → hourly sweep re-enqueued forever).
                # The placements INSERT above keeps placed_edition_key.
                conn.execute(
                    """UPDATE local_files SET mismatch_state = NULL
                       WHERE media_type = ? AND tmdb_id = ? AND section_id = ?
                         AND edition_key = ?""",
                    (media_type, tmdb_id, section_id, _lf_edition),
                )
            # v1.18.71: post-success Plex API teardown for the
            # api→file atomic-transition pattern. SWITCH PLACEMENT
            # [api→file] and REPLACE [kind=file] used to call
            # plex.delete_theme() SYNCHRONOUSLY in the endpoint
            # (api.py) — pre-fix, if the sidecar place job then
            # failed (cross-FS + disk-full + permissions), the
            # Plex API selection was already cleared with no
            # auto-recovery (row themeless until manual SWITCH
            # back). v1.18.71 mirrors the v1.18.68 atomic pattern
            # in reverse: endpoints thread the rks via the
            # `clear_plex_api_rks` payload field, this block
            # runs delete_theme(rk) ONLY after the sidecar place
            # succeeds. Best-effort — a Plex API failure here
            # doesn't fail the job (sidecar already landed, row
            # is themed). The next plex_enum will catch any
            # stranded API entries.
            _payload = _safe_payload_parse(
                job["payload"],
                where="_do_place (clear_plex_api_rks)",
            )
            _rks_to_clear = _payload.get("clear_plex_api_rks") or []
            if _rks_to_clear:
                _td_plex = self._plex_client()
                if _td_plex is None:
                    log.warning(
                        "_do_place: clear_plex_api_rks=%s requested "
                        "but Plex client unavailable — skipping",
                        _rks_to_clear,
                    )
                else:
                    try:
                        for _rk in _rks_to_clear:
                            try:
                                _td_ok = _td_plex.delete_theme(
                                    rating_key=str(_rk))
                                # v1.19.79 (Opus-4.8 audit MED):
                                # delete_theme is best-effort and its
                                # False return was only logged at INFO,
                                # so a systemic failure (the documented
                                # v1.18.0→v1.18.36 plural-/themes silent
                                # 404 class) would never surface. Warn
                                # on False; INFO on success.
                                log.log(
                                    logging.INFO if _td_ok
                                    else logging.WARNING,
                                    "_do_place[api→file]: rk=%s "
                                    "delete_theme=%s (post-place)",
                                    _rk, _td_ok,
                                )
                            except Exception as _e:  # noqa: BLE001
                                log.warning(
                                    "_do_place[api→file]: rk=%s "
                                    "delete_theme raised %s — "
                                    "continuing (sidecar landed)",
                                    _rk, _e,
                                )
                    finally:
                        _td_plex.close()
            # v1.10.46: Plex's first refresh sometimes lands before
            # the filesystem watcher / local-media-assets agent has
            # re-stat'd the folder, so the new theme.mp3 isn't
            # picked up. The user then has to manually click
            # 'Refresh Metadata' in Plex to force the pickup.
            # v1.11.24: schedule follow-up refreshes for Plex's
            # local-media-assets agent debounce.
            # v1.11.59: narrowed to a single +30s follow-up.
            # v1.14.12: dropped the +30s safety-net refresh
            # ENTIRELY. The inline `plex.refresh(rk)` inside
            # placement.place_theme already issues the per-item
            # refresh (PUT /library/metadata/{rk}/refresh?force=1)
            # at place time — that's the same Plex API call the
            # safety net was firing again 30 seconds later.
            #
            # At bulk scale the queued safety-net jobs were the
            # perceived "long nudge process": 100 places →
            # 100 inline refreshes (fast) PLUS 100 queued refresh
            # jobs draining serially through the rate-limited
            # worker (slow, visible in /queue). Net effect was
            # 200 Plex refresh calls per 100 places, with the
            # second 100 contributing nothing on the common path
            # (Plex's agent picked up the theme on the first
            # call already).
            #
            # Trade-off: in the rare case Plex's agent debounces
            # past the inline refresh, motif no longer auto-
            # retries. The theme.mp3 is on disk; Plex will pick
            # it up on its own next agent scan, OR the user
            # hits `// REFRESH MOVIES` (per-section enum) for
            # an immediate manual nudge. Same fallback that
            # already covered every other post-place edge case.
            #
            # The `_do_refresh` handler stays in place — explicit
            # refresh ops (e.g. from RE-DOWNLOAD TDB recovery)
            # still enqueue refresh jobs and route through it.
        # v1.11.24: stamp the place outcome on the local_files row so
        # /pending can render the actual reason ('existing_theme:',
        # 'plex_has_theme', 'no_match', 'placement_error:') instead of
        # falling through to the generic 'Awaiting placement — the
        # worker should pick it up shortly'. Also propagate hints to
        # plex_items where applicable so the existing reason logic
        # (which reads pi.local_theme_file / pi.has_theme) gets a
        # nudge.
        try:
            # v1.15.104: wrap the multi-UPDATE outcome stamp in a
            # transaction. Pre-fix this block ran up to 3 separate
            # execute() calls (local_files + optionally plex_items)
            # outside any transaction, so a partial write was
            # observable to concurrent readers. Under heavy load
            # (lots of place jobs concurrent with plex_enum reads),
            # a reader could see local_files.last_place_attempt_*
            # updated but plex_items.local_theme_file still stale.
            # AUDIT_WORKER.md M2.
            with get_conn(self.settings.db_path) as conn, \
                    transaction(conn):
                # v1.19.61: PS-with-DL → BK unification. Pre-fix a
                # plex_has_theme skip stamped 'plex_has_theme' and the
                # row rendered LINK=PS (Plex serves, motif standing by
                # incidentally). But the row's actual state — motif
                # has a downloaded canonical file, no placement, Plex
                # serves — is functionally identical to an explicit
                # backup. v1.19.61 stamps 'backup_only' in this case
                # so the row uniformly classifies as BK in the link
                # column, surfaces PROMOTE TO ACTIVE on the recovery
                # card (api.py:16660 BK-state probe), and lights up
                # the v1.18.90 reaper's backup_ready notification on
                # theme loss. the user's "86 EIGHTY-SIX" repro.
                _stamp_reason = "placed" if outcome.placed else (
                    outcome.reason or "unknown")
                if _stamp_reason == "plex_has_theme":
                    _stamp_reason = "backup_only"
                # v1.22.73: _lf_edition — the row that served (see the read).
                conn.execute(
                    "UPDATE local_files SET last_place_attempt_at = ?, "
                    "                       last_place_attempt_reason = ? "
                    "WHERE media_type = ? AND tmdb_id = ? AND section_id = ? "
                    "  AND edition_key = ?",
                    (now_iso(), _stamp_reason,
                     media_type, tmdb_id, section_id, _lf_edition),
                )
                if not outcome.placed and outcome.target_folder:
                    plex_type = "show" if media_type == "tv" else "movie"
                    # v1.15.91: switched from `WHERE folder_path = ?` to a
                    # natural-key lookup (theme_id, fallback to guid_tmdb +
                    # media_type, scoped by section_id). Same parallel
                    # fix as v1.15.90's unplace handler. On multi-volume
                    # setups, plex_items.folder_path stores Plex's host
                    # path while outcome.target_folder is motif's
                    # FolderIndex-resolved container path. The strings
                    # diverge, so the pre-v1.15.91 WHERE matched 0 rows
                    # and motif's plex_items state stayed inconsistent
                    # with the place outcome until the next plex_enum
                    # catch-up (which itself fails on Unraid setups
                    # where _candidate_local_paths' translation table
                    # doesn't cover the user's mount layout — returns
                    # indeterminate and preserves the stale value).
                    theme_id_row = conn.execute(
                        "SELECT id FROM themes "
                        "WHERE media_type = ? AND tmdb_id = ?",
                        (media_type, tmdb_id),
                    ).fetchone()
                    theme_id_pk = theme_id_row["id"] if theme_id_row else None
                    pi_where_parts: list[str] = []
                    pi_args: list = []
                    if theme_id_pk is not None:
                        pi_where_parts.append("theme_id = ?")
                        pi_args.append(theme_id_pk)
                    pi_where_parts.append(
                        "(guid_tmdb = ? AND media_type = ?)"
                    )
                    pi_args.extend([str(tmdb_id), plex_type])
                    pi_where = " OR ".join(pi_where_parts)
                    # Section-scope so a per-edition place outcome
                    # doesn't stamp sibling sections' plex_items rows.
                    pi_where = f"({pi_where}) AND section_id = ?"
                    pi_args.append(section_id)
                    # v1.22.18 (audit MED #8): also EDITION-scope when the
                    # job named an edition — pre-fix a skipped-place outcome
                    # on one edition stamped local_theme_file=1 / has_theme
                    # on EVERY sibling edition's plex_items row in the
                    # section (same tmdb_id, distinct {edition-X} folders).
                    # '' = legacy/default resolution → section-wide as
                    # before (we don't know which edition it targeted).
                    if place_edition_key:
                        pi_where += " AND edition_key = ?"
                        pi_args.append(place_edition_key)
                    if (outcome.reason or "").startswith("existing_theme:"):
                        conn.execute(
                            f"UPDATE plex_items SET local_theme_file = 1 "
                            f"WHERE {pi_where}",
                            pi_args,
                        )
                    elif outcome.reason == "plex_has_theme":
                        conn.execute(
                            f"UPDATE plex_items SET has_theme = 1, "
                            f"                     local_theme_file = 0 "
                            f"WHERE {pi_where}",
                            pi_args,
                        )
        except Exception as e:
            # v1.15.94: bumped from log.debug to log.warning. The
            # outcome stamp is bookkeeping: UPDATE local_files's
            # last_place_attempt_at + UPDATE plex_items's
            # local_theme_file / has_theme based on outcome.reason.
            # The place job itself already succeeded (or failed in
            # a tracked way) — but a swallowed stamp failure leaves
            # motif's plex_items state diverged from the actual
            # place outcome. On Unraid setups where plex_enum can't
            # reach the folder (the v1.15.90-92 path-mismatch
            # class), there's no catch-up: the stale flag persists
            # until manual intervention. log.warning makes the
            # failure visible in default-INFO production logs so
            # operators can investigate. The except stays — the
            # place's correctness isn't at stake, only the
            # bookkeeping accuracy.
            log.warning("place outcome stamp failed: %s", e)
        # v1.13.76: skip outcomes now log at INFO (was DEBUG) so the
        # user sees "Skipped placement: plex_has_theme" / "no_match"
        # / "existing_theme:foo" without enabling debug logging.
        # Pre-fix the silent skip + hourly retry sweep produced the
        # confusing "Retry sweep enqueued 1 placement jobs" → "Job
        # NNN (place) starting" → (silence) → "Job NNN+1 (place)
        # starting" loop with no visible reason. The retry sweep
        # itself now also filters permanent skips out of the
        # re-enqueue (scheduler.py), so this INFO line should fire
        # at most once per row per state change.
        # v1.18.79: when the placement was skipped due to
        # `plex_has_theme` AND the row's user_override has
        # intent='replace', upgrade the log level to WARNING —
        # the user EXPLICITLY chose to replace Plex's theme
        # and motif silently refused. Pre-v1.18.79 this fired
        # at INFO and was indistinguishable from the legitimate
        # intent='backup' skip (where Plex-serves is the intended
        # outcome). Class-9-style breadcrumb on a user-intent-
        # denied path. the user's 1941-era confusion partially
        # rooted in this: a SET URL on a P-row got silently
        # skipped, with the INFO log hidden among hundreds of
        # other INFO lines.
        _place_log_level = "INFO"
        _intent_for_log: str | None = None
        if (not outcome.placed
                and outcome.reason == "plex_has_theme"):
            # edition-blind OK (v1.21.94): the intent read below ONLY picks the
            # breadcrumb log LEVEL (INFO vs WARNING) for the plex_has_theme
            # skip — no placement/download behavior; reading a sibling
            # edition's intent at worst mis-colors one log line.
            try:
                with get_conn(self.settings.db_path) as _conn:
                    _ovr_row = _conn.execute(
                        "SELECT intent FROM user_overrides "
                        "WHERE media_type = ? AND tmdb_id = ? "
                        "  AND section_id = ?",
                        (media_type, tmdb_id, section_id),
                    ).fetchone()
                    if _ovr_row is None:
                        # edition-blind OK (v1.21.94): '' fallback of the
                        # log-level-only intent read above.
                        _ovr_row = _conn.execute(
                            "SELECT intent FROM user_overrides "
                            "WHERE media_type = ? AND tmdb_id = ? "
                            "  AND section_id = ''",
                            (media_type, tmdb_id),
                        ).fetchone()
                if _ovr_row is not None:
                    _intent_for_log = _ovr_row["intent"]
                    if _intent_for_log == "replace":
                        _place_log_level = "WARNING"
            except Exception as e:  # noqa: BLE001
                # Failure to read intent is a class-9 trap on
                # the breadcrumb that fixes class-9 — log debug
                # so future debugging has a thread.
                log.debug(
                    "_do_place: intent lookup failed on "
                    "plex_has_theme skip for %s/%s: %s",
                    media_type, tmdb_id, e,
                )
        _place_msg = (
            f"Placed in {outcome.target_folder}" if outcome.placed
            else f"Skipped placement: {outcome.reason}"
        )
        if (_place_log_level == "WARNING"
                and not outcome.placed):
            _place_msg = (
                f"Skipped placement: {outcome.reason} — but "
                f"user_override intent='replace' is set. Plex "
                f"is blocking motif's URL. User can click "
                f"// PROMOTE TO ACTIVE on the row's INFO card "
                f"to force-deploy, OR // MARK AS BACKUP to "
                f"convert intent and silence this warning."
            )
        # v1.21.41 (audit M1): a placement_error reason is a REAL I/O
        # failure (disk full / EPERM / cross-FS copy error), NOT an
        # intentional skip. Surface it as a WARNING with failure-wording
        # (not "Skipped placement", which reads like the plex_has_theme
        # skip) and mark the job FAILED below so the topbar FAIL dot
        # lights. The hourly retry sweep (last_place_attempt_reason=
        # 'placement_error:...') stays the single recovery path —
        # _mark_failed_terminal avoids a second (backoff-ladder) retry.
        _placement_io_failed = (
            not outcome.placed
            and (outcome.reason or "").startswith("placement_error"))
        if _placement_io_failed:
            _place_log_level = "WARNING"
            _place_msg = f"Placement FAILED: {outcome.reason}"
        _place_detail = {
            "reason": outcome.reason, "kind": outcome.kind,
        }
        if _intent_for_log is not None:
            _place_detail["override_intent"] = _intent_for_log
        log_event(
            self.settings.db_path,
            level=_place_log_level,
            component="place", media_type=media_type, tmdb_id=tmdb_id,
            section_id=section_id,
            message=_place_msg,
            detail=_place_detail,
        )
        if _placement_io_failed:
            # v1.21.41: light the topbar FAIL dot. Terminal (no backoff
            # ladder) — the hourly retry sweep is the single recovery
            # path. dispatch's trailing _mark_done then no-ops via its
            # WHERE status='running' guard (v1.14.25).
            self._mark_failed_terminal(
                job["id"], f"placement failed: {outcome.reason}")
        # v1.17.4: theme_added notification (per-row, OFF by default).
        # Fires when a place job actually places a theme — the moment
        # the user sees the theme appear in Plex. Per-row by design;
        # operators with small libraries may enable, larger libraries
        # should pair with `themes_added_by_sync` (aggregate) instead.
        # No dedupe — the opt-in toggle is the dedupe.
        # v1.17.12: rich body via notify_content.enrich_item +
        # format_theme_added_body — title + year + thumbnail +
        # source provenance + URL instead of bare "movie/347201"
        # + verbose Plex-mechanics tail. Markdown format so Discord
        # / Slack / Telegram render the thumbnail inline.
        if outcome.placed:
            try:
                from . import notify as _notify
                from . import notify_content as _nc
                ctx = _nc.enrich_item(
                    self.settings.db_path,
                    media_type=media_type, tmdb_id=tmdb_id,
                    section_id=section_id,
                    edition_key=place_edition_key,  # v1.21.75: name the cut
                )
                # v1.19.55: discriminate theme_added (genuine new
                # placement) from theme_pushed (re-deployment of
                # an existing theme: PUSH TO PLEX, REPLACE TDB,
                # PROMOTE TO ACTIVE, etc.). the user's feedback:
                # "we need to update theme added to take into
                # account theme source changed like push via
                # side car or swapping to via api as right now
                # they all trigger the same added notication."
                #
                # Discriminator: `force` / `force_place` flags or
                # any explicit `reason` in the payload mean
                # user-initiated re-deploy. Sync-driven and
                # natural placements have empty payload (no
                # `reason` / `force`).
                # v1.19.80 (Opus-4.8 audit LOW): route through the
                # canonical _safe_payload_parse breadcrumb helper
                # instead of a bare except-swallow. The helper returns
                # {} on parse error OR non-dict, so the isinstance
                # guards are no longer needed.
                _p = _safe_payload_parse(
                    job["payload"],
                    where="_do_place (theme_pushed discriminator)",
                )
                _reason = _p.get("reason") or None
                _is_pushed = bool(
                    _p.get("force") or _p.get("force_place") or _reason
                )
                if _reason == "auto_restore":
                    # v1.24.38 (review #6): the scheduler's hourly auto-restore
                    # sweep used to fire theme_auto_restored at ENQUEUE — before
                    # this place ran — so a place that later failed still claimed
                    # "restored". Fire it HERE on the place actually landing.
                    # bulk=True so a Plex-re-add burst coalesces to one summary
                    # (a lone restore flushes as the rich single).
                    _notify.dispatch_coalesced(
                        self.settings.db_path,
                        self.settings.cfg.notifications,
                        event_kind="theme_auto_restored",
                        single_item_ctx=ctx,  # v0.51.151: click-through identity
                        item_label=_nc.format_theme_auto_restored_item(ctx),
                        single_title=_nc.format_theme_auto_restored_title(ctx),
                        single_body=_nc.format_theme_auto_restored_body(ctx),
                        batch_title_fn=_nc.format_theme_auto_restored_batch_title,
                        batch_body_fn=_nc.format_theme_auto_restored_batch_body,
                        body_format="markdown",
                        single_attach_url=_nc.attachment_thumb_url(ctx),
                        bulk=True,
                        section=ctx.get("section_label") or "",
                    )
                elif _is_pushed:
                    # v1.19.95: coalesce — a bulk place collapses to
                    # one summary; a lone push still sends the rich
                    # single notification with the URL embed.
                    _notify.dispatch_coalesced(
                        self.settings.db_path,
                        self.settings.cfg.notifications,
                        event_kind="theme_pushed",
                        item_label=_nc.format_theme_pushed_item(
                            ctx, reason=_reason,
                        ),
                        single_title=_nc.format_theme_pushed_title(
                            ctx, reason=_reason,
                            # v1.21.6: cached_has_theme captured the
                            # row's pre-place Plex state (post stale-
                            # cache adjustment). REPLACE TDB on a row
                            # with no prior theme says "set", not
                            # "replaced" — the user's The Last Starfighter
                            # repro (brand-new TDB theme read as a
                            # replacement when nothing was there).
                            had_prior_theme=cached_has_theme,
                        ),
                        single_body=_nc.format_theme_pushed_body(
                            ctx, reason=_reason,
                        ),
                        batch_title_fn=_nc.format_theme_pushed_batch_title,
                        batch_body_fn=_nc.format_theme_pushed_batch_body,
                        body_format="markdown",
                        single_attach_url=_nc.attachment_thumb_url(ctx),
                        # v1.23.46: coalesce only when the bulk flag rode in
                        # from the bulk download (propagated onto this place
                        # job in _do_download); a lone PUSH stays a rich single.
                        bulk=bool(_p.get("bulk")),
                        # v1.23.75: library, for batch section grouping.
                        section=ctx.get("section_label") or "",
                    )
                else:
                    _notify.dispatch_coalesced(
                        self.settings.db_path,
                        self.settings.cfg.notifications,
                        event_kind="theme_added",
                        single_item_ctx=ctx,  # v0.51.151: click-through identity
                        item_label=_nc.format_theme_added_item(ctx),
                        single_title=_nc.format_theme_added_title(ctx),
                        single_body=_nc.format_theme_added_body(ctx),
                        batch_title_fn=_nc.format_theme_added_batch_title,
                        batch_body_fn=_nc.format_theme_added_batch_body,
                        body_format="markdown",
                        single_attach_url=_nc.attachment_thumb_url(ctx),
                        bulk=bool(_p.get("bulk")),
                        # v1.23.75: library, for batch section grouping.
                        section=ctx.get("section_label") or "",
                    )
            except Exception as e:
                log.debug("notify dispatch (theme_added/pushed) suppressed: %s", e)

    def _do_place_collection(
        self, *, job: sqlite3.Row, theme: sqlite3.Row,
        local: sqlite3.Row | None,
    ) -> None:
        """v1.18.0: HTTP-upload place for media_type='collection'.

        Plex collections have no folder on disk — the theme lives
        inside Plex's metadata bundle and is set via a binary POST
        to /library/metadata/{rating_key}/themes. Themerr-plex used
        this same endpoint via python-plexapi's
        Collection.uploadTheme(). Phase 2's
        PlexClient.upload_collection_theme is the in-process
        equivalent.

        Mirrors _do_place's shape:
          1. dry-run preview (just log what would happen)
          2. require local file exists
          3. parse force_overwrite from payload
          4. resolve plex_items row for the collection (rk, has_theme)
          5. skip if plex_has_theme and not force_overwrite
          6. read mp3 bytes from source_file
          7. POST via PlexClient.upload_collection_theme
          8. upsert placements row with placement_kind='plex_upload'
             and media_folder='' (schema v55 widens both CHECKs)
          9. stamp last_place_attempt_at on local_files
         10. fire theme_added notification

        On any failure raises RuntimeError so the worker's retry
        ladder absorbs it the same way it does for movie/show
        place failures.
        """
        media_type = job["media_type"]
        tmdb_id = job["tmdb_id"]
        section_id = job["section_id"]

        dry = is_dry_run(self.settings.db_path,
                         default=self.settings.dry_run_default)

        # v1.18.10: reverted v1.18.9's default-force-overwrite for
        # collections. the user's preference: "treated same as movies
        # as if plex has a theme that's best case." Plex-served
        # themes (themerr-plex cloud embeds, Plex Pass) should be
        # respected the same way they are for movies/shows — motif
        # downloads the canonical as a backup but doesn't overwrite
        # Plex's. The associated "PL=await flashing amber" symptom
        # is fixed separately below: the skip path now stamps
        # plex_independent_theme=1 so the row renders as PS state
        # (Plex Serving) rather than nagging !P attention.
        # Payload force=true (e.g. /pending APPROVE) still wins.
        force_overwrite = False
        payload = _safe_payload_parse(
            job["payload"], where="_do_place_collection (force flag)",
        )
        if payload.get("force"):
            force_overwrite = True
        # v1.21.55 (B2b): the edition this upload targets (resolved at
        # enqueue from rating_key). '' for true collections (never
        # multi-edition) = today; inert until C2 wires enqueue.
        _coll_payload_edition = payload.get("edition_key", "") or ""
        # v1.22.73: the edition of the local_files row that serves this
        # upload — `local` came from _do_place's IN (?, '') fallback read,
        # so when the shared '' row serves a tagged edition's push, stamps
        # keyed on the payload edition matched 0 rows (see _do_place).
        _coll_lf_edition = (local["edition_key"] if local is not None
                            else _coll_payload_edition)

        # Resolve plex_items row — same theme_id JOIN + fallback as
        # _do_place. Collections in plex_items have
        # media_type='collection' (Phase 2 enumerate_collections_for_section
        # writes it directly).
        # v1.19.21: pull plex_theme_verified_ok for the stale-
        # cache override below.
        # v1.21.66 / v1.24.18: resolve the rk for the payload edition's own
        # plex_items row, not an arbitrary sibling (the path that uploaded
        # the user's "Sam Takes a Step" theme into the Theatrical rk). Scope by
        # PRESENCE not truthiness — '' is a REAL edition (the standard one), so
        # the pre-v1.24.18 `if _coll_payload_edition` left it on the unscoped
        # path (the _do_place sibling bug). Collections are single-edition today
        # so this is a no-op for them, but kept symmetric with _do_place. The
        # ''-only fallback below covers a payload edition that matches no row.
        _ce_clause = " AND pi.edition_key = ?"
        _ce_clause2 = " AND edition_key = ?"
        _ce_params = (_coll_payload_edition,)
        # v0.51.253: prefer the LIVE plex_items row — THE 2026-08-09 incident.
        # A disk dropped and came back; Plex deleted the items and re-added
        # them with new rating keys, and the v0.51.128 reaper holds the dead
        # rows for 2 enums as anti-glitch grace, so the table legitimately
        # held a dead row + a live row per title. resolve_theme_ids linked
        # BOTH to the theme, so this JOIN matched both and a bare LIMIT 1
        # returned the older, DEAD one — every upload 404'd against a rating
        # key Plex had already deleted (~80 movies; Pacific Rim dead 137499 /
        # live 738854, both theme_id=2474). This is the upload path the bulk
        # PUSH actually takes; _do_place's own resolve below is the FILE path
        # and needs the identical ordering (kind='api' returns before it).
        with get_conn(self.settings.db_path) as conn:
            pi = conn.execute(
                """SELECT pi.rating_key, pi.has_theme,
                          pi.plex_theme_verified_ok
                   FROM plex_items pi
                   INNER JOIN themes t
                     ON t.id = pi.theme_id
                    AND t.media_type = ?
                    AND t.tmdb_id = ?
                   WHERE pi.section_id = ?""" + _ce_clause + """
                   ORDER BY pi.consecutive_missing ASC,
                            pi.last_seen_at DESC
                   LIMIT 1""",
                (media_type, tmdb_id, section_id) + _ce_params,
            ).fetchone()
            if pi is None:
                # v1.18.23: generalized fallback. Pre-fix this
                # hardcoded 'collection'; now maps themes.media_type
                # → plex_items.media_type ('tv' → 'show') so the
                # fallback works when this helper is invoked for
                # movie/TV API push (kind='api' dispatch in
                # _do_place above).
                pi_media_type_fallback = (
                    "show" if media_type == "tv" else media_type
                )
                pi = conn.execute(
                    "SELECT rating_key, has_theme, "
                    "       plex_theme_verified_ok "
                    "FROM plex_items "
                    "WHERE guid_tmdb = ? AND media_type = ? "
                    "  AND section_id = ?" + _ce_clause2
                    + " ORDER BY consecutive_missing ASC, last_seen_at DESC"
                    " LIMIT 1",
                    (tmdb_id, pi_media_type_fallback, section_id)
                    + _ce_params,
                ).fetchone()
            if pi is None and not _coll_payload_edition:
                # v1.24.18: STANDARD ('') payload matched no '' row — fall back to
                # the UNSCOPED resolve (mirrors _do_place). v1.24.24 (code-review):
                # only when EXACTLY ONE candidate — no arbitrary pick across 2+
                # siblings. Tagged payloads still never fall back (v1.21.66 strict).
                pi_media_type_fallback = (
                    "show" if media_type == "tv" else media_type)
                # v0.51.253: count the ambiguity over LIVE rows only — the
                # v1.24.24 guard targets multi-EDITION siblings, but a
                # re-added title's dead+live pair also read as 2 candidates
                # and got refused. Two LIVE rows is still ambiguous. When
                # nothing is live, keep the old count so a section-wide Plex
                # glitch doesn't newly strand a place.
                _cands = conn.execute(
                    "SELECT rating_key, has_theme, "
                    "       plex_theme_verified_ok, consecutive_missing "
                    "FROM plex_items "
                    "WHERE guid_tmdb = ? AND media_type = ? "
                    "  AND section_id = ? "
                    "ORDER BY consecutive_missing ASC, last_seen_at DESC "
                    "LIMIT 3",
                    (tmdb_id, pi_media_type_fallback, section_id),
                ).fetchall()
                _live = [c for c in _cands if not c["consecutive_missing"]]
                if len(_live) == 1:
                    pi = _live[0]
                elif not _live and len(_cands) == 1:
                    pi = _cands[0]
        cached_rk = pi["rating_key"] if pi else None
        cached_has_theme = bool(pi and pi["has_theme"])
        # v1.19.21: stale-cache override (same logic as _do_place).
        # When has_theme=1 + verified_ok=0, Plex's cache is lying.
        # Force placement.
        if cached_has_theme and pi is not None:
            cached_verified_ok = pi["plex_theme_verified_ok"]
            if cached_verified_ok == 0:
                log.info(
                    "_do_place_collection %s/%s sec=%s: Plex "
                    "cached has_theme=1 but verified_ok=0 — "
                    "cache is stale, forcing upload to repair",
                    media_type, tmdb_id, section_id,
                )
                cached_has_theme = False
        # v1.18.9: explicit state-log at the start of every
        # _do_place_collection invocation. Pre-fix the "PL stalls
        # flashing amber" symptom the user reported was a black-box
        # failure mode — no signal in docker logs about what state
        # the worker resolved, whether the upload fired, or why a
        # skip path took over. Now: every collection-place job
        # emits its resolved (rk, has_theme, force_overwrite,
        # local-file-present) tuple BEFORE deciding skip vs
        # upload. Pairs with the verbose upload_collection_theme
        # logging in plex.py.
        # v0.51.253: label this `job=`, not `rk=`. It is the JOB id, and the
        # mislabel actively misled during the 2026-08-09 dead-rk incident —
        # "rk=7621" next to "cached_rk=137499" reads as two rating keys.
        log.info(
            "_do_place_collection: job=%s media_type=%s tmdb_id=%s "
            "section_id=%s cached_rk=%s cached_has_theme=%s "
            "force_overwrite=%s local_present=%s",
            job["id"], media_type, tmdb_id, section_id,
            cached_rk, cached_has_theme, force_overwrite,
            local is not None,
        )

        if dry:
            if cached_rk is None:
                msg = (f"DRY-RUN: no Plex collection row for "
                       f"'{theme['title']}' (sec={section_id})")
            elif cached_has_theme and not force_overwrite:
                msg = (f"DRY-RUN: would skip '{theme['title']}' "
                       f"— Plex already has collection theme")
            else:
                msg = (f"DRY-RUN: would upload theme for "
                       f"collection '{theme['title']}' to "
                       f"/library/metadata/{cached_rk}/themes")
            log_event(
                self.settings.db_path, level="INFO", component="dryrun",
                media_type=media_type, tmdb_id=tmdb_id,
                message=msg,
                detail={
                    "rating_key": cached_rk,
                    "section_id": section_id,
                    "plex_already_has_theme": cached_has_theme,
                },
            )
            return

        if local is None:
            raise RuntimeError(
                f"local file missing for collection section "
                f"{section_id} (download not yet complete?)"
            )
        if cached_rk is None:
            raise RuntimeError(
                f"no Plex collection rating_key for "
                f"(collection, {tmdb_id}, sec={section_id})"
            )

        # Skip if Plex already has a theme and the caller didn't
        # ask to force-replace.
        if cached_has_theme and not force_overwrite:
            reason = "plex_has_theme"
            log_event(
                self.settings.db_path, level="INFO", component="place",
                media_type=media_type, tmdb_id=tmdb_id,
                section_id=section_id,
                message=f"Skipped placement: {reason}",
                detail={"reason": reason, "kind": "plex_upload"},
            )
            try:
                # v1.21.8 (audit MED): wrap the two stamps in a
                # transaction so a concurrent reader can't observe
                # local_files updated while plex_items is still stale
                # (transient inconsistent render) — the movie/show
                # equivalent got this in v1.15.104; this collection
                # path (v1.18.10) re-introduced the gap.
                with get_conn(self.settings.db_path) as conn, transaction(conn):
                    # v1.19.61: stamp 'backup_only' (not the log
                    # reason 'plex_has_theme') for PS→BK unification.
                    # See worker.py:2334 main-stamp marker for the
                    # full v1.19.61 rationale. Collections share the
                    # same conceptual state (motif has downloaded
                    # content, Plex serves) so this stamp aligns.
                    conn.execute(
                        "UPDATE local_files SET "
                        "  last_place_attempt_at = ?, "
                        "  last_place_attempt_reason = 'backup_only' "
                        "WHERE media_type = ? AND tmdb_id = ? "
                        "  AND section_id = ? AND edition_key = ?",
                        (now_iso(),
                         media_type, tmdb_id, section_id,
                         _coll_lf_edition),
                    )
                    # v1.18.10: stamp plex_independent_theme=1 on
                    # the collection's plex_items row so the
                    # library UI renders LPS (Plex Serving) state
                    # — green DL dot, PS link badge, no nagging
                    # !P attention glyph. Pre-fix the skip left
                    # plex_independent_theme NULL, so the row
                    # rendered "downloaded but not placed"
                    # (PL=await amber) and the !P attention
                    # surfaced — exactly what the user flagged as
                    # "stalls and flashing amber PL." Same +P
                    # capture v1.13.55 introduced for the
                    # movie/show LPS path, applied to collection
                    # rows. Idempotent: only writes when the
                    # column is NULL so a legitimate post-PURGE
                    # NULL reset stays preserved.
                    if cached_rk:
                        conn.execute(
                            "UPDATE plex_items "
                            "   SET plex_independent_theme = 1 "
                            " WHERE rating_key = ? "
                            "   AND plex_independent_theme IS NULL "
                            "   AND has_theme = 1",
                            (str(cached_rk),),
                        )
            except Exception as e:
                log.warning("place outcome stamp failed: %s", e)
            return

        # Read the mp3 bytes. Cancel checkpoint before the network
        # call so a CANCEL clicked during a slow disk read still
        # bails cleanly.
        source_file = self.settings.themes_dir / local["file_path"]
        if not source_file.exists():
            raise RuntimeError(f"source file missing: {source_file}")
        if _is_cancelled(self.settings.db_path, job["id"]):
            log.info(
                "Job %d (place/collection) cancelled at pre-upload checkpoint",
                job["id"],
            )
            self._mark_cancelled(job["id"])
            return
        try:
            audio_bytes = source_file.read_bytes()
        except OSError as e:
            raise RuntimeError(f"read failed: {e}") from e

        # v1.24.45: a collection theme over Plex's ~10MB upload ceiling has NO
        # sidecar fallback (collections have no folder on disk) — the metadata-
        # store upload is the only path, so an over-ceiling theme terminal-failed
        # with nothing placed (the user's Middle-Earth Collection: 12.5MB → HTTP
        # 500). Re-encode it down to fit so it actually deploys. Movies keep their
        # full-quality sidecar fallback; this folderless path is the one that
        # benefits. If the re-encode can't help (no ffmpeg / theme too long even
        # at the bitrate floor), we upload the original and surface over_ceiling.
        _ceiling_bytes = _PLEX_THEME_UPLOAD_CEILING_MB * 1024 * 1024
        # v0.51.16 (audit #2): the v1.24.45 downscale is for the FOLDERLESS
        # collection path only — exactly what the comment above promises
        # ("Movies keep their full-quality sidecar fallback"), but the check
        # below had no media_type gate, so an over-ceiling movie/TV API push
        # was silently re-encoded to a lower bitrate and uploaded, making the
        # v1.18.69 full-quality sidecar fallback unreachable. Movie/TV rows
        # over the ceiling now skip the downscale AND the doomed ~10MB POST
        # (v1.21.99), synthesizing the (ok=False, http_status=None) outcome so
        # the post-upload sidecar fallback below fires with the ORIGINAL bytes'
        # size_mb.
        _skip_doomed_upload = (
            len(audio_bytes) >= _ceiling_bytes and media_type != "collection")
        if _skip_doomed_upload:
            log.info(
                "_do_place_collection: %s theme %.1fMB exceeds Plex's ~%dMB "
                "upload ceiling — skipping the doomed POST; taking the "
                "full-quality sidecar fallback (rk=%s)",
                media_type, len(audio_bytes) / (1024 * 1024),
                _PLEX_THEME_UPLOAD_CEILING_MB, cached_rk)
        if len(audio_bytes) >= _ceiling_bytes and not _skip_doomed_upload:
            _small = _downscale_audio_to_fit(
                source_file, int(_ceiling_bytes * 0.85))
            # v1.24.46: accept the re-encode only if it actually FITS the ceiling.
            # The v1.24.45 gate compared to len(audio_bytes) (smaller than the
            # ORIGINAL), so a still-over-ceiling shrink — a long theme clamps to
            # the 48kbps floor and stays >10MB — was uploaded and 500'd anyway:
            # a guaranteed-doomed ~10MB POST. When we have a measured re-encode
            # that's STILL over the ceiling, short-circuit to over_ceiling now and
            # skip the round-trip. (No-ffmpeg / encode-fail → _small is None → fall
            # through to the optimistic upload; the post-500 handler surfaces it.)
            if _small and len(_small) < _ceiling_bytes:
                log.info(
                    "_do_place_collection: theme %.1fMB exceeds Plex's ~%dMB "
                    "ceiling — re-encoded to %.1fMB to fit (rk=%s)",
                    len(audio_bytes) / (1024 * 1024),
                    _PLEX_THEME_UPLOAD_CEILING_MB,
                    len(_small) / (1024 * 1024), cached_rk)
                # v1.24.47: the UPLOADED bytes now differ from the canonical on
                # disk; local_files.file_sha256/size deliberately still describe
                # the canonical (unchanged). A later re-push deterministically
                # re-downscales the same canonical, so any future "served ==
                # canonical" reconcile must expect this for over-ceiling collections.
                audio_bytes = _small
            elif _small:
                _oc_note = (
                    "collection theme re-encoded to %.1fMB but still over Plex's "
                    "~%dMB upload ceiling (theme too long even at the bitrate "
                    "floor) — shorten / replace the theme" % (
                        len(_small) / (1024 * 1024),
                        _PLEX_THEME_UPLOAD_CEILING_MB))
                log.warning(
                    "_do_place_collection: %s (rk=%s) — skipping doomed upload",
                    _oc_note, cached_rk)
                # Same local_files reason stamp + terminal-fail the post-500
                # handler writes, so the scheduler retry-skip gate
                # (LIKE 'plex_rejected:%' + 2-failure lockout) is unchanged.
                try:
                    with get_conn(self.settings.db_path) as conn:
                        conn.execute(
                            "UPDATE local_files SET "
                            "  last_place_attempt_at = ?, "
                            "  last_place_attempt_reason = 'plex_rejected:over_ceiling' "
                            "WHERE media_type = ? AND tmdb_id = ? "
                            "  AND section_id = ? AND edition_key = ?",
                            (now_iso(), media_type, tmdb_id, section_id,
                             _coll_lf_edition),
                        )
                except Exception as e:
                    log.warning(
                        "_do_place_collection: failed to stamp over_ceiling "
                        "reason for (%s, %s, sec=%s): %s",
                        media_type, tmdb_id, section_id, e)
                raise _JobPermanentFailure(_oc_note)

        if _skip_doomed_upload:
            # v0.51.16 (audit #2): synthesized over-ceiling outcome — same
            # (500, None)-class shape the fallback gate below accepts.
            ok, http_status, body_snip = (
                False, None, "over upload ceiling — POST skipped (v0.51.16)")
        else:
            plex = self._plex_client()
            if plex is None:
                raise RuntimeError("Plex client unavailable")
            try:
                # v1.18.68: upload_collection_theme returns a 3-tuple
                # (ok, http_status, body_snippet) so the failure note
                # can include actionable triage info.
                ok, http_status, body_snip = plex.upload_collection_theme(
                    rating_key=str(cached_rk),
                    audio_bytes=audio_bytes,
                    content_type="audio/mpeg",
                )
            finally:
                plex.close()
        size_mb = len(audio_bytes) / (1024 * 1024)
        # v1.18.69: track whether we ended up serving via sidecar
        # fallback rather than the API upload. Drives downstream
        # branch decisions (skip the v1.18.59 leftover-sidecar
        # cleanup since we just CREATED the sidecar; placements row
        # carries kind='hardlink' / 'copy' instead of 'plex_upload';
        # log_event uses WARNING + a fallback-reason payload).
        fell_back_kind: str | None = None
        fell_back_folder: str | None = None
        # v1.20.15: did we manage to nudge Plex to serve the fallback
        # sidecar? Drives an HONEST plex_refreshed below (audit MED-1).
        fell_back_refreshed = False
        if not ok:
            # v1.18.68: enrich the RuntimeError with HTTP status +
            # body size so the failed-job note in /logs surfaces
            # the actual rejection shape. Pre-fix the generic
            # "Plex collection upload failed for rk=X" gave the
            # operator no signal whether the cause was Plex
            # rejecting the bytes, a network error, or motif
            # bug. the user's repro: a 27MB file 500'd silently and
            # he had to dig through docker logs to find the HTTP
            # status. Big-file detection: Plex theme uploads have
            # an empirical ceiling (~10MB — see
            # _PLEX_THEME_UPLOAD_CEILING_MB). Surface the hint when
            # the body is at/over the ceiling AND we got rejected —
            # saves the user the diagnostic step.
            status_str = (f"HTTP {http_status}"
                          if http_status is not None else "no response")
            note = (
                f"Plex rejected upload (rk={cached_rk}, "
                f"{status_str}, size={size_mb:.1f}MB)"
            )

            # v1.18.69: size-rejection sidecar fallback for movie/TV.
            # When a movie/TV row's API upload fails AND looks size-
            # related (HTTP 500 + size ≥ the ceiling), hardlink/copy
            # the canonical to the Plex media folder as a sidecar
            # rather than raise. the user's ask: "let's have it fallback
            # to placing or hardlinking the theme … instead of the PU
            # state with a warning maybe in logs". Outcome: row is
            # themed via the sidecar route (LINK=HL or C), user sees
            # a WARNING event in HISTORY explaining the fallback.
            # Collections have NO media folder (the v1.18.0 plex_upload
            # sentinel uses media_folder=''), so no fallback possible
            # there — collection failures still raise the enriched
            # error from v1.18.68.
            # v1.19.94: threshold lowered 20→_PLEX_THEME_UPLOAD_CEILING_MB
            # (10) to match the real Plex ceiling — see the constant.
            # v1.19.99: also fall back when http_status is None — Plex
            # never answered (read timeout / connection reset under
            # load). A folder-backed movie/TV row can always be themed
            # via a local sidecar, so a no-response large upload should
            # fall back rather than burn 3 retries re-POSTing the bytes
            # and terminal-fail with nothing placed (code-review #4).
            # size_rejection (the no-fallback permanent-fail below)
            # stays HTTP-500-only — a bare timeout is more likely
            # transient, so folderless/collection rows still retry it.
            if (media_type != "collection"
                    and http_status in (500, None)
                    and size_mb >= _PLEX_THEME_UPLOAD_CEILING_MB):
                try:
                    with get_conn(self.settings.db_path) as conn:
                        pi_row = conn.execute(
                            "SELECT folder_path FROM plex_items "
                            "WHERE rating_key = ? LIMIT 1",
                            (str(cached_rk),),
                        ).fetchone()
                    folder_path = (pi_row["folder_path"]
                                   if pi_row else None)
                    # v1.22.15: resolve the Plex-reported folder_path (host
                    # view — /mnt/user/... on the user's Unraid) to a container-
                    # visible directory BEFORE writing. Pre-fix Path(folder_path)
                    # / "theme.mp3" + a parent dir create CREATED a phantom
                    # container-local tree that Plex never sees, then reported
                    # success — the over-ceiling theme silently went nowhere
                    # (audit #1). Mirror place_theme's _candidate_local_paths
                    # resolution; the real Plex media folder already exists on
                    # the /data mount, so no mkdir.
                    _fb_dir = None
                    if folder_path:
                        from .plex_enum import _candidate_local_paths
                        _fb_dir = next(
                            (c for c in _candidate_local_paths(folder_path)
                             if c.is_dir()), None)
                        if _fb_dir is None:
                            log.warning(
                                "_do_place_collection: size-fallback folder "
                                "not reachable in container for rk=%s "
                                "(folder_path=%s) — cannot place sidecar; "
                                "leaving as terminal failure",
                                cached_rk, folder_path)
                    if _fb_dir is not None:
                        from .placement import _safe_link_or_copy
                        dst = _fb_dir / "theme.mp3"
                        fell_back_kind = _safe_link_or_copy(
                            source_file, dst)
                        fell_back_folder = str(_fb_dir)
                        # v1.20.15: the fallback bypasses placement.
                        # place_theme (which refreshes Plex inline), so
                        # nudge Plex now — else it won't serve the fresh
                        # sidecar until the next section-wide refresh, yet
                        # plex_refreshed was hardcoded =1 below (a false
                        # claim). Best-effort + isolated: a refresh
                        # failure leaves the placed sidecar intact (Plex
                        # picks it up on the next section refresh) and
                        # just records plex_refreshed=0 (audit MED-1).
                        if self.settings.plex_analyze_after:
                            try:
                                _rplex = self._plex_client()
                                if _rplex is not None:
                                    try:
                                        fell_back_refreshed = bool(
                                            _rplex.refresh(str(cached_rk)))
                                    finally:
                                        _rplex.close()
                            except Exception as _re:  # noqa: BLE001
                                log.warning(
                                    "_do_place_collection: sidecar-"
                                    "fallback Plex refresh failed for "
                                    "rk=%s: %s (Plex will serve it on the "
                                    "next section refresh)",
                                    cached_rk, _re)
                        log.warning(
                            "_do_place_collection: API upload %s "
                            "(rk=%s, size=%.1fMB, status=%s) — fell "
                            "back to %s sidecar at %s",
                            ("rejected (too large)" if http_status == 500
                             else "got no response (timeout)"),
                            cached_rk, size_mb, http_status,
                            fell_back_kind, dst,
                        )
                except Exception as e:  # noqa: BLE001
                    # Fallback itself failed (FS error, missing dir,
                    # permission). Log + drop through to the
                    # original RuntimeError so the operator sees
                    # both failure modes in the docker log.
                    log.warning(
                        "_do_place_collection: sidecar fallback "
                        "failed for rk=%s: %s — raising original "
                        "upload error",
                        cached_rk, e,
                    )

            if fell_back_kind is None:
                # v1.18.70: classify the failure shape so the worker
                # picks the right retry behavior:
                #   * size-rejection (HTTP 500 + size ≥ the ceiling on
                #     a collection, or any case where the fallback
                #     couldn't fire) — _JobPermanentFailure. Plex
                #     consistently rejects the same bytes; retrying
                #     every minute for 6 minutes burns 3 attempts
                #     before terminal-fail, surfacing "PLACE INTO
                #     PLEX QUEUED" in the dashboard the whole time.
                #     the user's repro: a 25.8MB collection-or-failed-
                #     fallback row stalled for ~5min before he
                #     gave up and canceled manually.
                #   * everything else — RuntimeError (transient
                #     retry path, max_attempts=3, exponential
                #     backoff). Covers HTTP 502/503 + network
                #     glitches + Plex restarts.
                # v1.19.94: threshold lowered 20→ceiling (10) so
                # mid-size 500s (10.5MB Demon Sword, 11.7MB Fate)
                # terminal-fail fast instead of holding the place
                # queue at ~97% through 3 backoff cycles.
                size_rejection = (size_mb >= _PLEX_THEME_UPLOAD_CEILING_MB
                                  and http_status == 500)
                if size_rejection:
                    note += (
                        " — large files often fail Plex theme "
                        "upload (observed ~10MB ceiling); try a "
                        "shorter clip"
                    )
                elif body_snip and len(body_snip) < 120:
                    note += f" — {body_snip}"
                # v1.18.94: stamp last_place_attempt_reason so the
                # hourly retry sweep (scheduler._retry_pending_placements)
                # can pattern-skip rows that have been Plex-rejected
                # repeatedly. the user's repro: an 11.7MB movie file
                # (Fate/stay night, rk=687556) consistently 500'd from
                # Plex but fell below the v1.18.70 size-rejection
                # threshold (20MB), so each hourly sweep re-enqueued
                # a fresh 3-attempt cycle that burned ~7min amber UI
                # before terminal-failing, forever. The stamp pairs
                # with the sweep filter to break the cycle after 2
                # recent failures — first one stays auto-retryable
                # (might be transient Plex hiccup), second one locks
                # the row out until user intervention.
                status_token = (http_status if http_status is not None
                                else "none")
                # v1.24.45: a distinct reason for a size rejection that survived
                # the auto-downscale (theme too long even at the bitrate floor, or
                # no ffmpeg) so the library row surfaces "too large for Plex" — a
                # collection can't fall back to a sidecar, so it stays unthemed
                # until the user shortens / replaces the theme. Kept under the
                # `plex_rejected:` prefix so the scheduler retry-skip gate
                # (LIKE 'plex_rejected:%' + 2-failure lockout) is UNCHANGED — a
                # bare reason would re-open the doomed retry loop (class-9
                # contract drift). The generic reason stays for non-size 500s.
                _rejection_reason = ("plex_rejected:over_ceiling" if size_rejection
                                     else f"plex_rejected:HTTP_{status_token}")
                try:
                    with get_conn(self.settings.db_path) as conn:
                        conn.execute(
                            "UPDATE local_files SET "
                            "  last_place_attempt_at = ?, "
                            "  last_place_attempt_reason = ? "
                            "WHERE media_type = ? AND tmdb_id = ? "
                            "  AND section_id = ? AND edition_key = ?",
                            (now_iso(), _rejection_reason,
                             media_type, tmdb_id, section_id,
                             _coll_lf_edition),
                        )
                except Exception as e:
                    log.warning(
                        "_do_place_collection: failed to stamp "
                        "plex_rejected reason for (%s, %s, sec=%s): %s",
                        media_type, tmdb_id, section_id, e,
                    )
                if size_rejection:
                    raise _JobPermanentFailure(note)
                raise RuntimeError(note)

        # v1.18.68: upload confirmed — only NOW remove sidecar
        # paths the SWITCH PLACEMENT endpoint queued for
        # post-upload cleanup. Pre-fix the endpoint unlinked the
        # sidecar synchronously BEFORE enqueueing the place job;
        # a Plex-side upload failure (the user's 27MB → HTTP 500
        # case) left the row with no sidecar AND no successful
        # Plex upload → unthemed until manual PUSH SIDECAR.
        # Moving the unlink here makes SWITCH PLACEMENT atomic:
        # upload succeeds → unlink fires; upload fails → sidecar
        # survives → row stays in its pre-switch state.
        # v1.18.69: skip when we fell back to a sidecar — the
        # payload-listed sidecar IS the file we just wrote (same
        # folder); removing it would undo the fallback.
        if fell_back_kind is None:
            payload = _safe_payload_parse(
                job["payload"],
                where="_do_place_collection (sidecar_cleanup)",
            )
            sidecars_to_remove = (
                payload.get("remove_sidecar_paths") or []
            )
            for side_path in sidecars_to_remove:
                try:
                    p = Path(side_path)
                    if p.is_file():
                        p.unlink()
                        log.info(
                            "_do_place_collection: removed sidecar "
                            "%s after successful upload (rk=%s)",
                            p, cached_rk,
                        )
                except OSError as e:
                    log.warning(
                        "_do_place_collection: could not unlink "
                        "sidecar %s after upload: %s", side_path, e,
                    )

        # Upsert placements row. media_folder='' marks this as the
        # non-folder HTTP-upload path; placement_kind='plex_upload'
        # is the schema v55 widening's third placement kind.
        # v1.18.69: when we fell back to sidecar (API upload too
        # large), the placements row reflects the sidecar route
        # — media_folder = the Plex folder, placement_kind =
        # 'hardlink'/'copy', so LINK pill renders as HL/C instead
        # of PU. This is what makes the row themed via the
        # sidecar route post-fallback.
        with get_conn(self.settings.db_path) as conn, transaction(conn):
            placement_provenance = (
                local["provenance"] if local["provenance"] else "auto"
            )
            _placed_kind = fell_back_kind or "plex_upload"
            _placed_folder = fell_back_folder or ""
            # v1.21.55 (B2b): key the placement by the physical fallback
            # folder's edition when we fell back to a real sidecar folder,
            # else by the payload edition (plex_upload has no folder).
            _coll_edition_key = (
                edition_key_for_folder(_placed_folder) if _placed_folder
                else _coll_payload_edition)
            # v1.20.15: plex_refreshed must reflect reality, not a
            # hardcoded 1. The normal API-upload path (no fallback) IS
            # served by the upload itself → 1. The sidecar-fallback path
            # is served only if the refresh above actually nudged Plex →
            # fell_back_refreshed (audit MED-1).
            _placed_refreshed = 1 if fell_back_kind is None else (
                1 if fell_back_refreshed else 0)
            conn.execute(
                """
                INSERT INTO placements
                    (media_type, tmdb_id, section_id, edition_key,
                     media_folder,
                     placed_at, placement_kind, plex_rating_key,
                     plex_refreshed, provenance)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(media_type, tmdb_id, section_id, media_folder, edition_key)
                DO UPDATE SET
                    placed_at = excluded.placed_at,
                    placement_kind = excluded.placement_kind,
                    plex_rating_key = excluded.plex_rating_key,
                    plex_refreshed = excluded.plex_refreshed,
                    provenance = excluded.provenance
                """,
                (media_type, tmdb_id, section_id, _coll_edition_key,
                 _placed_folder,
                 now_iso(), _placed_kind, cached_rk,
                 _placed_refreshed, placement_provenance),
            )
            conn.execute(
                "UPDATE local_files SET "
                "  last_place_attempt_at = ?, "
                "  last_place_attempt_reason = 'placed', "
                "  mismatch_state = NULL "
                "WHERE media_type = ? AND tmdb_id = ? "
                "  AND section_id = ? AND edition_key = ?",
                (now_iso(), media_type, tmdb_id, section_id,
                 _coll_lf_edition),
            )
            # Sync plex_items so the row's has_theme reflects the
            # successful upload — the next plex_enum will re-confirm
            # but we shouldn't make the user wait for that to see
            # the row update.
            conn.execute(
                "UPDATE plex_items SET has_theme = 1 "
                "WHERE rating_key = ?",
                (str(cached_rk),),
            )

        # v1.18.59: clean up any leftover sidecar at the Plex media
        # folder for movie/TV rows that just went through the API
        # upload path. Mirrors the v1.18.36 SWITCH file→api cleanup
        # but extends to ALL plex_upload paths (not just SWITCH).
        #
        # the user's Titans (2018) repro: row was direct-placed via
        # API (no SWITCH event in history) but a leftover theme.<ext>
        # sidecar surfaced in the orphan scan. The sidecar likely
        # came from a prior placement / themerr-plex / manual file
        # at the Plex media folder. Without cleanup, every direct
        # API placement on a row that ALREADY has a sidecar creates
        # an orphan_sidecar_on_disk drift entry.
        #
        # Collections skip this entirely — they have no media folder
        # (the v1.18.0 plex_upload sentinel uses `media_folder=''`
        # but the row's Plex media folder IS the empty string too).
        # Movies/TV have plex_items.folder_path set; that's where
        # the sidecar would live.
        # v1.18.69: skip the leftover-sidecar sweep on fallback —
        # the "leftover" the sweep would find IS the sidecar we
        # just wrote as the fallback. Removing it would re-create
        # the unthemed state the fallback was designed to avoid.
        if (media_type != "collection" and cached_rk
                and fell_back_kind is None):
            folder_path = None
            try:
                from .plex_enum import find_theme_sidecar_path
                with get_conn(self.settings.db_path) as conn:
                    pi_row = conn.execute(
                        "SELECT folder_path FROM plex_items "
                        "WHERE rating_key = ? LIMIT 1",
                        (str(cached_rk),),
                    ).fetchone()
                folder_path = pi_row["folder_path"] if pi_row else None
                if folder_path:
                    side = find_theme_sidecar_path(folder_path)
                    if side:
                        try:
                            side.unlink()
                            log.info(
                                "_do_place_collection[api]: removed "
                                "leftover sidecar %s for %s/%s "
                                "section_id=%s after API upload",
                                side, media_type, tmdb_id, section_id,
                            )
                        except OSError as e:
                            log.warning(
                                "_do_place_collection[api]: could not "
                                "unlink leftover sidecar %s: %s",
                                side, e,
                            )
            except Exception as e:  # noqa: BLE001
                # Cleanup is best-effort. The orphan scan will
                # surface any sidecar we missed; don't fail the
                # place job because of cleanup trouble.
                log.warning(
                    "_do_place_collection[api]: sidecar cleanup "
                    "scan failed for %s/%s: %s",
                    media_type, tmdb_id, e,
                )
            # v1.23.26: drop the superseded sidecar PLACEMENT RECORD for this
            # edition — not just its file (the v1.18.59 sweep above). Pre-fix
            # the unlink left the hardlink/copy placement row, which then read
            # theme_present=0 (false red PL dot) AND collided with the new
            # plex_upload row in the two-tier library JOIN (dual-placement
            # fan-out: the user's Avatar/Borat/Flow/Zootopia rows rendered cyan
            # or red depending on which placement the query surfaced, and never
            # surfaced under NEEDS WORK). The API upload supersedes the sidecar
            # for this edition, so the record is stale. Best-effort (class-9):
            # plex_enum's verify_placement_health prunes any we miss.
            #
            # v1.23.30 (#5 code-review): scope the DELETE to media_folder =
            # folder_path — the folder the sweep above just emptied. Pre-fix
            # the DELETE removed EVERY non-plex_upload record for the edition,
            # so a record whose media_folder pointed at a DIFFERENT folder (a
            # not-yet-reconciled move) was dropped while its theme.mp3 survived
            # on disk — orphaning a live file. Records at other folders are
            # left for verify_placement_health, which only prunes a CONFIRMED-
            # missing file. Skip entirely when folder_path is unknown.
            try:
                if folder_path:
                    # v1.23.31: shared predicate with verify_placement_health's
                    # prune so the two can't drift (#8 review).
                    from .plex_enum import delete_superseded_sidecar_placement
                    with get_conn(self.settings.db_path) as conn:
                        n = delete_superseded_sidecar_placement(
                            conn, media_type, tmdb_id, section_id,
                            folder_path, _coll_edition_key)
                        if n:
                            log.info(
                                "_do_place[api]: dropped %d superseded "
                                "sidecar placement record(s) for %s/%s "
                                "section=%s edition=%r after API upload",
                                n, media_type, tmdb_id,
                                section_id, _coll_edition_key)
            except Exception as e:  # noqa: BLE001
                log.warning(
                    "_do_place_collection[api]: superseded-record cleanup "
                    "failed for %s/%s: %s", media_type, tmdb_id, e)

        # v1.18.69: emit a WARNING event on fallback so the row's
        # HISTORY tab in MOTIF INFO shows the operator WHY the
        # placement_kind isn't plex_upload. the user's ask: "saying
        # the current theme is too large to be uploaded via api
        # fallback to local sidecar required". The detail.reason
        # captures the trigger so future analytics / filters
        # can target the fallback population.
        if fell_back_kind is not None:
            # v1.19.99: cause-aware HISTORY/LOGS line so it's obvious
            # WHY this row is a local sidecar and NOT an API upload —
            # either over Plex's ~10MB theme ceiling (HTTP 500) or no
            # response (timeout). http_status is 500 or None here (the
            # only two the fallback gate above accepts).
            _too_large = http_status == 500
            # v1.20.0 (rollover audit): hedge the 500 attribution — a
            # Plex 500 is USUALLY the ~10MB theme ceiling at this size,
            # but can be a size-unrelated internal error (DB lock, agent
            # crash). "likely" keeps the HISTORY line + analytics honest.
            _cause = (
                "likely over Plex's ~10MB theme-upload ceiling (HTTP 500)"
                if _too_large
                else "got no response from Plex (timeout / connection reset)"
            )
            log_event(
                self.settings.db_path, level="WARNING",
                component="place",
                media_type=media_type, tmdb_id=tmdb_id,
                section_id=section_id,
                message=(
                    f"Plex API theme upload ({size_mb:.1f}MB) {_cause} "
                    f"— fell back to a local {fell_back_kind} sidecar at "
                    f"{fell_back_folder}. Row is themed from that sidecar "
                    f"(LINK={fell_back_kind}). "
                    f"NOT uploaded to Plex's metadata store."
                ),
                detail={
                    "reason": ("api_upload_too_large" if _too_large
                               else "api_upload_no_response"),
                    "kind": fell_back_kind,
                    "fallback_from": "plex_upload",
                    "size_mb": round(size_mb, 1),
                    "http_status": http_status,
                    "rating_key": cached_rk,
                },
            )
        else:
            log_event(
                self.settings.db_path, level="INFO", component="place",
                media_type=media_type, tmdb_id=tmdb_id,
                section_id=section_id,
                # v0.51.56: was "collection theme" — the plex_upload/API path
                # serves movies + TV too (v1.23.65), so name the transport, not
                # the media type (the structured media_type field already
                # carries movie/tv/collection).
                message=(f"Uploaded theme to Plex (API) for "
                         f"'{theme['title']}' "
                         f"(rk={cached_rk})"),
                detail={"reason": None, "kind": "plex_upload",
                        "rating_key": cached_rk},
            )
        # theme_added notification — same shape as _do_place's
        # success path so the Apprise dispatch logic stays
        # consistent across media types.
        # v1.19.55: same theme_added vs theme_pushed
        # discrimination as the _do_place mirror site above.
        # Collections are usually re-uploaded via API on
        # PUSH/REPLACE/PROMOTE, so the discriminator (force /
        # force_place / reason) applies equally here.
        try:
            from . import notify as _notify
            from . import notify_content as _nc
            ctx = _nc.enrich_item(
                self.settings.db_path,
                media_type=media_type, tmdb_id=tmdb_id,
                section_id=section_id,
                edition_key=_coll_payload_edition,  # v1.21.75: name the cut
            )
            # v1.19.80 (Opus-4.8 audit LOW): route through the
            # canonical _safe_payload_parse breadcrumb helper instead
            # of a bare except-swallow (mirrors _do_place above).
            _p = _safe_payload_parse(
                job["payload"],
                where="_do_place_collection (theme_pushed discriminator)",
            )
            _reason = _p.get("reason") or None
            _is_pushed = bool(
                _p.get("force") or _p.get("force_place") or _reason
            )
            if _reason == "auto_restore":
                # v1.24.38 (review #6): mirror the _do_place site — a stale
                # plex_upload collection re-PUSHED by the auto-restore sweep
                # pings theme_auto_restored on the upload ACTUALLY landing, not
                # at enqueue. bulk=True coalesces a burst.
                _notify.dispatch_coalesced(
                    self.settings.db_path,
                    self.settings.cfg.notifications,
                    event_kind="theme_auto_restored",
                    single_item_ctx=ctx,  # v0.51.151: click-through identity
                    item_label=_nc.format_theme_auto_restored_item(ctx),
                    single_title=_nc.format_theme_auto_restored_title(ctx),
                    single_body=_nc.format_theme_auto_restored_body(ctx),
                    batch_title_fn=_nc.format_theme_auto_restored_batch_title,
                    batch_body_fn=_nc.format_theme_auto_restored_batch_body,
                    body_format="markdown",
                    single_attach_url=_nc.attachment_thumb_url(ctx),
                    bulk=True,
                    section=ctx.get("section_label") or "",
                )
            elif _is_pushed:
                # v1.19.95: coalesce bulk collection-place bursts —
                # see the matching _do_place site.
                _notify.dispatch_coalesced(
                    self.settings.db_path,
                    self.settings.cfg.notifications,
                    event_kind="theme_pushed",
                    item_label=_nc.format_theme_pushed_item(
                        ctx, reason=_reason,
                    ),
                    single_title=_nc.format_theme_pushed_title(
                        ctx, reason=_reason,
                        # v1.21.13: the collection / plex_upload place
                        # path was the v1.21.6 gap — it never threaded
                        # had_prior_theme, so a brand-new theme placed
                        # via plex_upload (a movie with no on-disk media
                        # folder, or a collection) still said "replaced".
                        # the user's "13 (2010)" repro: first-ever theme,
                        # via REPLACE TDB, shown as "Theme replaced".
                        had_prior_theme=cached_has_theme,
                    ),
                    single_body=_nc.format_theme_pushed_body(
                        ctx, reason=_reason,
                    ),
                    batch_title_fn=_nc.format_theme_pushed_batch_title,
                    batch_body_fn=_nc.format_theme_pushed_batch_body,
                    body_format="markdown",
                    single_attach_url=_nc.attachment_thumb_url(ctx),
                    # v1.23.46: bulk only when a bulk download propagated it.
                    bulk=bool(_p.get("bulk")),
                    # v1.23.75: library, for batch section grouping.
                    section=ctx.get("section_label") or "",
                )
            else:
                _notify.dispatch_coalesced(
                    self.settings.db_path,
                    self.settings.cfg.notifications,
                    event_kind="theme_added",
                    single_item_ctx=ctx,  # v0.51.151: click-through identity
                    item_label=_nc.format_theme_added_item(ctx),
                    single_title=_nc.format_theme_added_title(ctx),
                    single_body=_nc.format_theme_added_body(ctx),
                    batch_title_fn=_nc.format_theme_added_batch_title,
                    batch_body_fn=_nc.format_theme_added_batch_body,
                    body_format="markdown",
                    single_attach_url=_nc.attachment_thumb_url(ctx),
                    bulk=bool(_p.get("bulk")),
                    # v1.23.75: library, for batch section grouping.
                    section=ctx.get("section_label") or "",
                )
        except Exception as e:
            log.debug("notify dispatch (theme_added/pushed/collection) suppressed: %s", e)

    def _do_refresh(self, job: sqlite3.Row) -> None:
        # v1.11.59: dropped the has_theme dedup that v1.11.57 added.
        # has_theme=1 just means 'Plex has SOME theme', not 'Plex
        # picked up motif's new theme', so on a REPLACE w/ TDB
        # against a P-source row the dedup short-circuited the
        # follow-up nudge even when Plex still hadn't ingested our
        # file. The schedule below also drops to a single follow-up,
        # which removes the queue-stacking the dedup was trying to
        # paper over. See _do_place for the new schedule.
        plex = self._plex_client()
        if not plex:
            # v1.20.20: breadcrumb before the early return (v1.18.5
            # cold-path rule). Every sibling handler logs WHY it
            # no-ops; this one silently marked the refresh job ✓ DONE
            # when Plex is unconfigured, so an operator running the
            # RE-DOWNLOAD TDB recovery saw success with no nudge fired.
            log.info("refresh job: Plex not configured — skipping nudge "
                     "(rk=%s)", (job["payload"] or ""))
            return
        try:
            # v1.14.52: payload-parse errors are PERMANENT (the
            # payload string was malformed at enqueue time and
            # won't fix itself on retry). Pre-fix the bare `pass`
            # silently treated parse failures as success → job
            # marked done by _dispatch despite no refresh actually
            # firing. Same shape applies to the missing-rating_key
            # case: a refresh job without rk has nothing to
            # operate on; raising surfaces the broken enqueue
            # path instead of silently logging ✓ DONE.
            try:
                payload = json.loads(job["payload"] or "{}")
            except (json.JSONDecodeError, TypeError) as e:
                raise _JobPermanentFailure(
                    f"refresh job payload unparseable: {e}") from e
            rk = payload.get("rating_key")
            if not rk:
                raise _JobPermanentFailure(
                    "refresh job missing rating_key in payload")
            # v1.20.20: surface a failed refresh instead of the silent
            # fake-✓-DONE the v1.14.52 marker above warned about.
            # plex.refresh() returns False when BOTH the refresh PUT
            # and the analyze fallback come back non-2xx (stale/changed
            # rk, Plex momentarily unreachable). It's a best-effort
            # nudge (and _do_place issues its own inline refresh), so we
            # don't raise/retry — but the operator needs a breadcrumb
            # that this explicit refresh didn't land, since the
            # RE-DOWNLOAD TDB recovery path relies on it (class-9 /
            # class-11: best-effort returns nothing asserts on).
            refreshed = plex.refresh(rk)
            if not refreshed:
                log.warning("refresh job: plex.refresh(rk=%s) returned "
                            "False — Plex did not confirm the nudge", rk)
                log_event(
                    self.settings.db_path, level="WARNING",
                    component="refresh",
                    message=(
                        f"Refresh nudge not confirmed by Plex (rk={rk}) "
                        f"— theme may not be picked up; re-run if needed"
                    ),
                )
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
        # v1.14.55: removed function-scope `import os` / `import shutil`
        # — both are already imported at module top (lines 18-19). The
        # re-imports were a leftover from when this method's body was
        # extracted from a smaller helper.
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
                # v1.21.94: join on edition_key too — a copy→hardlink relink
                # for a multi-edition title must pull THIS edition's source
                # file. The edition-blind join cross-products both editions'
                # local_files, so a placement could relink to a sibling
                # edition's theme.mp3.
                rows = conn.execute(
                    """SELECT p.*, lf.file_path
                       FROM placements p
                       JOIN local_files lf
                         ON lf.media_type = p.media_type
                        AND lf.tmdb_id = p.tmdb_id
                        AND lf.section_id = p.section_id
                        AND lf.edition_key = p.edition_key
                       WHERE p.media_type = ? AND p.tmdb_id = ?
                         AND p.placement_kind = 'copy'""",
                    (media_type, tmdb_id),
                ).fetchall()
            else:
                # v1.14.57: cap the bulk sweep at 5000 rows per
                # invocation. Pre-fix the unbounded SELECT walked
                # every placement_kind='copy' row in one pass —
                # fine for typical libraries (<1k copy rows) but
                # on a freshly-imported large library where every
                # placement landed cross-FS, a single sweep could
                # do thousands of os.link attempts in one BEGIN
                # IMMEDIATE block while holding the writer lock.
                # 5000 is generous for a normal homelab; the next
                # scheduled retry covers any overflow. Cooperative-
                # cancel checkpoint inside the loop (v1.14.25) still
                # lets the user abort sooner. AUDIT_WORKER P3.
                # v1.14.60: ORDER BY p.placed_at ASC so successive
                # sweeps walk older rows first AND a CANCEL → re-
                # enqueue cycle on a >5000-row library makes
                # forward progress. Pre-fix the LIMIT 5000 had no
                # ORDER BY → SQLite's implementation-defined order
                # could deterministically re-pick the same 5000
                # rows after a cancel, livelocking the user before
                # the tail of the library was ever reached.
                # v1.21.94: edition_key in the join (see the targeted branch
                # above) so the bulk copy-relink sweep can't cross-link a
                # sibling edition's source file into this placement.
                rows = conn.execute(
                    """SELECT p.*, lf.file_path
                       FROM placements p
                       JOIN local_files lf
                         ON lf.media_type = p.media_type
                        AND lf.tmdb_id = p.tmdb_id
                        AND lf.section_id = p.section_id
                        AND lf.edition_key = p.edition_key
                       WHERE p.placement_kind = 'copy'
                       ORDER BY p.placed_at ASC
                       LIMIT 5000"""
                ).fetchall()

        relinked = 0
        still_copy = 0
        errors = 0
        # v1.14.25: cooperative cancel checkpoint inside the per-row
        # loop. _do_relink can walk thousands of placement rows on a
        # bulk relink — pre-fix the user clicking CANCEL had to wait
        # for every row to be processed (could be minutes). Each
        # row's processing is short (one os.link try + maybe a
        # shutil.copy2 + DB UPDATE), so checking _is_cancelled
        # at top-of-loop adds at most one DB read per row and lets
        # CANCEL bail the bulk early. Audit ref: AUDIT_WORKER.md H2.
        job_id = job["id"]
        # v1.22.86 (amplifier-sweep, v1.18.10 class): cap missing-dst
        # placement deletes per sweep. A flapping /data mount makes
        # EVERY destination look missing (src on another volume stays
        # readable) — pre-fix one Storage relink during a mount outage
        # could wipe thousands of placements rows for files that still
        # exist on the real mount.
        _relink_missing_cap = max(50, len(rows) // 4)
        _missing_deleted = 0
        _relink_cap_warned = False
        for r in rows:
            if _is_cancelled(self.settings.db_path, job_id):
                log.info(
                    "Job %d (relink) cancelled mid-loop after %d rows",
                    job_id, relinked + still_copy + errors,
                )
                self._mark_cancelled(job_id)
                return
            src = self.settings.themes_dir / r["file_path"]
            dst = Path(r["media_folder"]) / "theme.mp3"
            if not src.exists():
                errors += 1
                continue
            if not dst.exists():
                if _missing_deleted >= _relink_missing_cap:
                    if not _relink_cap_warned:
                        log.warning(
                            "relink: %d missing-destination placements "
                            "already cleaned this sweep (cap %d) — leaving "
                            "the rest tracked. If /data was unmounted, "
                            "re-run the relink after the mount returns.",
                            _missing_deleted, _relink_missing_cap)
                        _relink_cap_warned = True
                    errors += 1
                    continue
                _missing_deleted += 1
                # Destination disappeared (movie folder deleted?) — clean up the row
                with get_conn(self.settings.db_path) as conn:
                    conn.execute(
                        """DELETE FROM placements
                           WHERE media_type = ? AND tmdb_id = ?
                             AND section_id = ? AND media_folder = ?
                             AND edition_key = ?""",
                        (r["media_type"], r["tmdb_id"], r["section_id"],
                         r["media_folder"], r["edition_key"]),
                    )
                continue

            tmp = dst.with_suffix(dst.suffix + ".relink-tmp")
            if tmp.exists():
                try:
                    tmp.unlink()
                except OSError as e:
                    # v1.17.1 (class 9): pre-fix swallowed the
                    # tmp-cleanup OSError silently. The subsequent
                    # os.link() will fail with FileExistsError and
                    # the outer handler catches it, so functional
                    # fallback works — but a permissions / FS-quirk
                    # failure here is invisible. log.debug surfaces
                    # it without spamming default-INFO logs.
                    log.debug("relink tmp pre-clean failed: %s", e)

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
                # v1.15.35: log temp-file cleanup failures at DEBUG.
                # Pre-fix bare `pass` left .relink-tmp staging files
                # on disk if cleanup unlink failed (file locked,
                # permissions). Next relink attempt would detect +
                # overwrite, so still LOW-impact, but the operator
                # can now correlate stale temp files with the
                # debug log if disk inspection turns one up.
                try:
                    tmp.unlink()
                except OSError as cleanup_err:
                    log.debug(
                        "relink: tmp file cleanup failed at %s: %s",
                        tmp, cleanup_err,
                    )
                continue

            with get_conn(self.settings.db_path) as conn:
                # v1.22.18 (audit MED #10): mirror the DELETE above's
                # edition scope. media_folder is per-edition distinct for
                # sidecar placements today, but plex_upload editions share
                # media_folder='' — without edition_key this UPDATE would
                # stamp every sharing edition. Keep the PK fully pinned.
                conn.execute(
                    """UPDATE placements SET placement_kind = 'hardlink', placed_at = ?
                       WHERE media_type = ? AND tmdb_id = ?
                         AND section_id = ? AND media_folder = ?
                         AND edition_key = ?""",
                    (now_iso(), r["media_type"], r["tmdb_id"],
                     r["section_id"], r["media_folder"], r["edition_key"]),
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
# v1.20.40: the old single "general" pool is split so placement runs
# CONCURRENTLY with downloading. Download workers do nothing but pull
# from YouTube/SoundCloud/etc. (the slow, rate-limited tier); the place
# worker hardlinks finished themes into Plex + the cheap post-place
# follow-ups (refresh/relink/adopt). A finished download enqueues a
# place job that the idle place worker picks up immediately — so themes
# go live as they download, and neither tier can starve the other.
_DOWNLOAD_JOB_TYPES = ("download",)
_PLACE_JOB_TYPES = ("place", "refresh", "relink", "adopt")
# v0.51.249: _GENERAL_JOB_TYPES removed — the v1.20.40 pool split orphaned it and
# its comment ("kept for callers/tests") was false: zero references anywhere.


def _reclaim_orphan_jobs(db_path: Path) -> None:
    """v1.22.30: ONE-TIME orphan-job reclaim, run by start_worker BEFORE any
    worker thread spawns.

    v1.11.6 ran this at the top of EVERY worker's run() loop. But run() is
    also re-entered by _supervised whenever a thread crashes and restarts —
    at ANY point in the process lifetime, not just boot. A single download
    thread restarting at hour 5 would then reset EVERY sibling's in-flight
    'running' job (the long worker's active sync, the other download threads'
    active downloads) back to 'pending' → an idle sibling re-claims and
    RE-RUNS it: duplicate downloads, double placement, wasted bandwidth.

    Running it once at process startup reclaims genuine prior-crash zombies
    without ever touching a job a live sibling is currently executing. A
    thread that hangs mid-job without exiting the process is still reclaimed
    by the scheduler's _stuck_job_sweep (aged 'running' → 'failed' → retried)."""
    try:
        with get_conn(db_path) as conn:
            cur = conn.execute(
                "UPDATE jobs SET status = 'pending', started_at = NULL "
                "WHERE status = 'running'"
            )
            if cur.rowcount:
                log.info("Reclaimed %d orphan 'running' job(s) at startup",
                         cur.rowcount)
            # v1.11.50: also reset orphan sync_runs rows (their own status
            # field only run_sync flips). A worker that died mid-sync left the
            # row 'running' forever — mark failed so the UI is honest.
            cur2 = conn.execute(
                "UPDATE sync_runs SET status = 'failed', "
                "  finished_at = COALESCE(finished_at, ?), "
                "  error = COALESCE(error, 'worker restarted mid-sync') "
                "WHERE status = 'running'",
                (now_iso(),),
            )
            if cur2.rowcount:
                log.info("Marked %d orphan sync_runs row(s) failed at startup",
                         cur2.rowcount)
            # v1.23.23: a worker that died mid-sync (between parking a held
            # auto-download and the post-summary release) left download jobs
            # at the hold sentinel, which the claim query skips forever.
            # Release them at boot so the discovered themes aren't stranded —
            # the summary they were ordered behind is long gone, so nothing to
            # wait for. v1.23.31: shares release_held_downloads' statement with
            # the end-of-sync release so the sentinel contract can't drift.
            _held = release_held_downloads(conn)
            if _held:
                log.info("Released %d orphan sync-held download(s) at startup",
                         _held)
    except Exception as e:
        log.warning("Orphan-job recovery failed at startup: %s", e)


def start_worker(settings: Settings, stop_event: threading.Event) -> list[threading.Thread]:
    """v1.11.36: spawn one 'long' worker + N 'general' workers.

    - 'long' worker handles sync / plex_enum / scan. These each run for
      tens of seconds to minutes; pre-fix they blocked every download
      / place behind them.
    - 'general' worker(s) handle download / place / refresh / relink /
      adopt — short, frequent jobs that benefit from running in
      parallel with the long jobs.

    SQLite WAL handles write serialization; sync writes themes,
    plex_enum writes plex_items, downloads / places write local_files
    / placements — all disjoint. Each worker has its own FolderIndex
    cache (per-section, per-thread) so they don't share that mutable
    state.

    v1.13.91: TWO related changes that close the perf-audit-flagged
    "concurrency knob is a placebo" bug:

    1. **Shared TokenBucket across all workers** (was: one bucket
       per thread with full capacity). Pre-fix `rate=60` with two
       threads gave aggregate 120/hr because each thread had its own
       bucket. The setting was dishonest — bumping it didn't
       linearly bump real throughput. One shared bucket means the
       rate setting actually means rate.

    2. **Spawn `settings.download_concurrency` general workers**
       (was: hardcoded to 1 general worker). The UI knob persisted
       to config but was never read — bumping it 1→3 did nothing.
       Now it spawns the requested number of general workers, all
       sharing the rate bucket. Combined with #1, this lets latency-
       bound jobs (cookies-fetch, signature solve, slow CDN) run in
       parallel without the rate setting drifting.

    The 'long' worker stays a single thread — multi-thread sync /
    plex_enum would compete on writer locks for the same tables.
    Only general jobs get the parallelism boost.
    """
    threads: list[threading.Thread] = []

    # v1.22.30: reclaim prior-crash zombie jobs ONCE, before any thread spawns.
    # Pre-fix each thread did this in run() → a restarting thread reset live
    # siblings' jobs to pending (duplicate-download race). See the helper.
    _reclaim_orphan_jobs(settings.db_path)

    def _supervised(worker: "Worker", label: str) -> None:
        """v1.11.51: outer-most supervisor. The run() loop catches
        OperationalError + Exception around claim/dispatch (no thread
        death from DB lock contention), but if anything ever DOES
        escape — e.g. an OSError on the events queue, an error inside
        a bookkeeping path that the inner catch missed — we don't
        want the worker to silently disappear for the rest of the
        process lifetime. Loop the run() call until the stop_event
        fires, with a short backoff between failures so a persistent
        bug doesn't busy-spin.
        """
        while not stop_event.is_set():
            try:
                worker.run()
                return  # clean exit (stop_event set)
            except Exception as e:
                log.exception("Worker %s crashed at top level: %s — "
                              "restarting in 10s", label, e)
                stop_event.wait(10.0)

    # v1.13.91: ONE bucket shared across every worker that downloads.
    # The long worker doesn't download via the bucket (sync/plex_enum/
    # scan don't acquire), but giving it the same bucket is harmless
    # and keeps the wiring uniform.
    shared_bucket = TokenBucket(
        rate=float(settings.download_rate_per_hour),
        capacity=max(1, settings.download_rate_per_hour),
        period=3600.0,
    )

    # Long worker: single thread (multi-thread would contend on
    # writer locks against itself for sync/plex_enum/scan).
    long_w = Worker(settings=settings, stop_event=stop_event,
                    bucket=shared_bucket,
                    job_type_filter=_LONG_JOB_TYPES,
                    name="motif-worker-long")
    long_t = threading.Thread(target=_supervised, args=(long_w, "long"),
                              name="motif-worker-long", daemon=True)
    long_t.start()
    threads.append(long_t)

    # v1.13.91: spawn N download workers from settings.download_concurrency.
    # Clamp to [1, 8] — 1 floor preserves single-worker mode,
    # 8 ceiling keeps SQLite writer-lock contention manageable
    # (downloads run mostly outside the lock; place/refresh briefly
    # acquire it, so concurrent claimants are safe with WAL).
    general_n = max(1, min(8, settings.download_concurrency or 1))
    log.info("Starting %d download worker thread(s) + 1 place worker "
             "(download_concurrency=%d, rate_per_hour=%d shared)", general_n,
             settings.download_concurrency or 1,
             settings.download_rate_per_hour)
    for i in range(general_n):
        label = f"download-{i + 1}" if general_n > 1 else "download"
        w = Worker(settings=settings, stop_event=stop_event,
                   bucket=shared_bucket,
                   job_type_filter=_DOWNLOAD_JOB_TYPES,
                   name=f"motif-worker-{label}")
        t = threading.Thread(target=_supervised, args=(w, label),
                             name=f"motif-worker-{label}", daemon=True)
        t.start()
        threads.append(t)

    # v1.20.40: a single dedicated place worker, running CONCURRENTLY with
    # the download worker(s). Placement is a ~ms hardlink (vs a ~50s
    # download) so one thread keeps the place queue drained as fast as
    # downloads feed it — themes go live as they finish downloading, and
    # a backlog of queued places can no longer block downloads from
    # starting (the v1.20.38 starvation). It shares the bucket but never
    # acquires it (only _do_download does), so it's unthrottled. Single
    # thread keeps writer-lock contention against the download workers low.
    place_w = Worker(settings=settings, stop_event=stop_event,
                     bucket=shared_bucket,
                     job_type_filter=_PLACE_JOB_TYPES,
                     name="motif-worker-place")
    place_t = threading.Thread(target=_supervised, args=(place_w, "place"),
                               name="motif-worker-place", daemon=True)
    place_t.start()
    threads.append(place_t)
    return threads
