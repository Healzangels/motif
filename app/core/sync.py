"""
ThemerrDB sync engine.

ThemerrDB publishes its database to the gh-pages branch daily at UTC 12:00.
The published structure is:
  /movies/pages.json                  -> {"count": N, "pages": M, "imdb_count": K}
  /movies/all_page_<n>.json           -> [{"id": tmdb_id, "imdb_id": "tt...", "title": "..."}, ...]
  /movies/imdb/<imdb_id>.json         -> full TMDB-shaped record + youtube_theme_url
  /movies/themoviedb/<tmdb_id>.json   -> same shape, keyed by tmdb_id (some lack imdb_id)
  /tv_shows/...                       -> identical layout

Sync strategy:
1. Fetch all pages and build a set of {(media_type, tmdb_id)}.
2. For each tmdb_id, fetch the per-item JSON (preferring imdb/ over themoviedb/).
3. Compare youtube_theme_added/youtube_theme_edited against what's in our DB.
   - New items: insert + enqueue download.
   - Changed YouTube URL: update + enqueue re-download.
   - Otherwise: just update last_seen_sync_at.
4. Items in our DB that didn't appear in this sync are NOT deleted — upstream
   may have transient drops, and we want our local copy to survive.
"""
from __future__ import annotations

import json
import logging
import random
import re
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator

import httpx

from .db import get_conn, transaction
from .events import log_event, now_iso
from . import progress as op_progress
from .normalize import titles_equal

log = logging.getLogger(__name__)


# ----- HTTP client config -----

_HTTP_TIMEOUT = httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0)
# v1.11.49: parallel per-item fetches use up to _FETCH_WORKERS concurrent
# requests; bump max_connections so the pool isn't the bottleneck. GitHub
# Pages happily handles ~32 concurrent connections from one client; we
# stay well under any sane CDN throttle.
_HTTP_LIMITS = httpx.Limits(max_connections=40, max_keepalive_connections=20)
_FETCH_WORKERS = 16


def _make_client() -> httpx.Client:
    return httpx.Client(
        timeout=_HTTP_TIMEOUT,
        limits=_HTTP_LIMITS,
        follow_redirects=True,
        headers={"User-Agent": "motif-sync/1.0 (homelab)"},
    )


# ----- Helpers -----

_VID_RE = re.compile(r"(?:v=|youtu\.be/|/embed/)([A-Za-z0-9_-]{11})")


def extract_video_id(url: str | None) -> str | None:
    if not url:
        return None
    m = _VID_RE.search(url)
    return m.group(1) if m else None


@dataclass
class SyncStats:
    movies_seen: int = 0
    tv_seen: int = 0
    new_count: int = 0
    updated_count: int = 0
    errors: int = 0


# ----- Sync logic -----


def _fetch_index(client: httpx.Client, base_url: str, media_path: str) -> list[dict]:
    """Fetch every page and return the merged list of {id, imdb_id, title}."""
    pages_url = f"{base_url}/{media_path}/pages.json"
    log.info("Fetching index: %s", pages_url)
    r = client.get(pages_url)
    r.raise_for_status()
    meta = r.json()
    total_pages = int(meta.get("pages", 0))
    if total_pages == 0:
        return []

    items: list[dict] = []
    for n in range(1, total_pages + 1):
        page_url = f"{base_url}/{media_path}/all_page_{n}.json"
        try:
            pr = client.get(page_url)
            pr.raise_for_status()
            items.extend(pr.json())
        except httpx.HTTPError as e:
            log.warning("Failed to fetch %s: %s", page_url, e)
        # Tiny politeness delay every 25 pages
        if n % 25 == 0:
            time.sleep(0.5)
    return items


def _fetch_item(
    client: httpx.Client,
    base_url: str,
    media_path: str,
    *,
    imdb_id: str | None,
    tmdb_id: int,
) -> dict | None:
    """Fetch the per-item record. Prefer the imdb/ folder when imdb_id is known."""
    candidates = []
    if imdb_id:
        candidates.append(f"{base_url}/{media_path}/imdb/{imdb_id}.json")
    candidates.append(f"{base_url}/{media_path}/themoviedb/{tmdb_id}.json")

    last_err: Exception | None = None
    for url in candidates:
        try:
            r = client.get(url)
            if r.status_code == 404:
                continue
            r.raise_for_status()
            return r.json()
        except httpx.HTTPError as e:
            last_err = e
            continue
    if last_err:
        log.debug("Item fetch failed (tmdb=%s): %s", tmdb_id, last_err)
    return None


def _upsert_theme(
    conn: sqlite3.Connection,
    *,
    media_type: str,
    tmdb_id: int,
    record: dict,
    upstream_source: str,
    sync_ts: str,
) -> tuple[bool, bool, str | None]:
    """Insert or update one theme row.
    Returns (is_new, url_changed, old_video_id_if_changed)."""
    imdb_id = record.get("imdb_id") or None
    # TMDB uses different field names for movies vs TV: title/release_date for
    # movies, name/first_air_date for TV. ThemerrDB stores records as-is, so
    # accept either shape.
    title = record.get("title") or record.get("name") or ""
    original_title = record.get("original_title") or record.get("original_name") or None
    rd = record.get("release_date") or record.get("first_air_date") or ""
    year = rd[:4] if rd else None
    yt_url = record.get("youtube_theme_url") or None
    yt_vid = extract_video_id(yt_url)
    yt_added = record.get("youtube_theme_added") or None
    yt_edited = record.get("youtube_theme_edited") or None

    existing = conn.execute(
        "SELECT youtube_video_id, youtube_edited_at FROM themes "
        "WHERE media_type = ? AND tmdb_id = ?",
        (media_type, tmdb_id),
    ).fetchone()

    is_new = existing is None
    url_changed = False
    old_vid: str | None = None

    # Orphan promotion: before inserting a new theme, check if there's a
    # plex_orphan row with negative tmdb_id matching this real record by
    # imdb_id, or by (title, year). If so, promote it: update the orphan's
    # tmdb_id to the real value, propagate the change to FK'd tables, and
    # treat this as an UPDATE rather than an INSERT.
    if is_new:
        orphan = None
        if imdb_id:
            orphan = conn.execute(
                """SELECT id, tmdb_id FROM themes
                   WHERE upstream_source = 'plex_orphan'
                     AND media_type = ? AND imdb_id = ?
                   LIMIT 1""",
                (media_type, imdb_id),
            ).fetchone()
        if orphan is None and title and year:
            # Pull every plex_orphan candidate for this media_type+year (cheap
            # filter) and select the one whose normalized title matches. This
            # handles things like "(500) Days of Summer" vs "500 Days of
            # Summer" or roman-numeral / word-number variations that exact
            # case-insensitive comparison would miss.
            # v1.11.93: ORDER BY id ASC so the candidate set is
            # iterated in a stable insert-order. Without it SQLite is
            # free to return rows in any order, which means two sync
            # runs against an unchanged DB could pick *different*
            # orphans to promote when more than one matches (rare but
            # possible: same title+year duplicated across sections).
            # Picking deterministically also makes log replay /
            # debug reproducible.
            orphan_candidates = conn.execute(
                """SELECT id, tmdb_id, title FROM themes
                   WHERE upstream_source = 'plex_orphan'
                     AND media_type = ? AND year = ?
                   ORDER BY id ASC""",
                (media_type, year),
            ).fetchall()
            for cand in orphan_candidates:
                if titles_equal(cand["title"] or "", title):
                    orphan = cand
                    break
        if orphan:
            old_tmdb = orphan["tmdb_id"]
            log.info("Promoting orphan theme id=%d (%s/%d) → real tmdb_id=%d",
                     orphan["id"], media_type, old_tmdb, tmdb_id)
            # Migrate the orphan row's tmdb_id and FK'd tables in one txn.
            # The row keeps its `id`; only tmdb_id changes.
            #
            # v1.11.71: use PRAGMA defer_foreign_keys = ON instead of
            # foreign_keys = OFF. The latter is documented as a no-op
            # when issued inside an open transaction (we're already
            # inside _flush_sync_batch's BEGIN IMMEDIATE), so FK
            # enforcement stayed live and the first UPDATE themes
            # crashed with 'FOREIGN KEY constraint failed' — the
            # existing local_files / placements / user_overrides rows
            # still pointed at (media_type, old_tmdb) but the parent
            # had just moved to (media_type, tmdb_id). defer_foreign_keys
            # IS allowed inside a txn and defers all checks to COMMIT,
            # at which point we've updated the children too and the
            # final state is consistent.
            conn.execute("PRAGMA defer_foreign_keys = ON")
            # v1.11.73: clear any leftover rows at the TARGET (media_type,
            # tmdb_id) before moving the orphan's rows there. Without this
            # the UPDATE collides with phantom rows from prior failed sync
            # attempts / DB-state edge cases (e.g. a user_overrides row
            # stuck at (movie, 252) from an earlier code path) and the
            # whole sync run dies on UNIQUE constraint failed. UNIQUE
            # is NOT covered by defer_foreign_keys; v1.11.71's deferred
            # FK fix only covered foreign-key violations.
            #
            # The orphan represents the user's most recent action
            # (SET URL / UPLOAD / adopt happened recently); any
            # pre-existing rows at the target tmdb are stale leftovers.
            # Drop them so the UPDATEs always succeed.
            conn.execute(
                "DELETE FROM local_files WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            )
            conn.execute(
                "DELETE FROM placements WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            )
            conn.execute(
                "DELETE FROM pending_updates WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            )
            conn.execute(
                "DELETE FROM user_overrides WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            )
            conn.execute(
                "UPDATE themes SET tmdb_id = ?, upstream_source = ? "
                "WHERE id = ?",
                (tmdb_id, upstream_source, orphan["id"]),
            )
            conn.execute(
                "UPDATE local_files SET tmdb_id = ? WHERE media_type = ? AND tmdb_id = ?",
                (tmdb_id, media_type, old_tmdb),
            )
            conn.execute(
                "UPDATE placements SET tmdb_id = ? WHERE media_type = ? AND tmdb_id = ?",
                (tmdb_id, media_type, old_tmdb),
            )
            conn.execute(
                "UPDATE pending_updates SET tmdb_id = ? WHERE media_type = ? AND tmdb_id = ?",
                (tmdb_id, media_type, old_tmdb),
            )
            conn.execute(
                "UPDATE user_overrides SET tmdb_id = ? WHERE media_type = ? AND tmdb_id = ?",
                (tmdb_id, media_type, old_tmdb),
            )
            existing = conn.execute(
                "SELECT youtube_video_id, youtube_edited_at FROM themes "
                "WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            ).fetchone()
            is_new = False

    # v1.10.32: precompute the normalized title so the library JOIN's
    # title-fallback (when Plex GUIDs disagree with ThemerrDB) can
    # match without re-running normalize_title() at query time.
    try:
        from .normalize import normalize_title
        title_norm = normalize_title(title or "")
    except Exception:
        title_norm = (title or "").lower()

    if is_new:
        conn.execute(
            """
            INSERT INTO themes (
                media_type, tmdb_id, imdb_id, title, original_title, year, release_date,
                youtube_url, youtube_video_id, youtube_added_at, youtube_edited_at,
                upstream_source, raw_json, last_seen_sync_at, first_seen_sync_at,
                title_norm
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                media_type, tmdb_id, imdb_id, title, original_title, year, rd or None,
                yt_url, yt_vid, yt_added, yt_edited,
                upstream_source, _safe_json(record), sync_ts, sync_ts,
                title_norm,
            ),
        )
    else:
        old_vid = existing["youtube_video_id"]
        url_changed = (yt_vid != old_vid) and yt_vid is not None
        # If the user has set a manual override for this theme, do NOT silently
        # overwrite themes.youtube_url with the new upstream value — the
        # override is the user's authoritative choice. Keep the previous
        # upstream URL on the row; the new one will be surfaced via
        # pending_updates so the user can decide whether to revert.
        has_override = conn.execute(
            "SELECT 1 FROM user_overrides WHERE media_type = ? AND tmdb_id = ?",
            (media_type, tmdb_id),
        ).fetchone() is not None
        if has_override and url_changed:
            conn.execute(
                """
                UPDATE themes SET
                    imdb_id = ?, title = ?, original_title = ?, year = ?, release_date = ?,
                    upstream_source = ?, raw_json = ?, last_seen_sync_at = ?,
                    title_norm = ?,
                    -- v1.13.1 (Phase C): if the item was previously
                    -- dropped from TDB and has now reappeared, clear
                    -- the dropped flag automatically — re-adding is
                    -- the canonical "undo drop" signal.
                    tdb_dropped_at = NULL
                WHERE media_type = ? AND tmdb_id = ?
                """,
                (
                    imdb_id, title, original_title, year, rd or None,
                    upstream_source, _safe_json(record), sync_ts,
                    title_norm,
                    media_type, tmdb_id,
                ),
            )
            return is_new, url_changed, old_vid
        conn.execute(
            """
            UPDATE themes SET
                imdb_id = ?, title = ?, original_title = ?, year = ?, release_date = ?,
                youtube_url = ?, youtube_video_id = ?, youtube_added_at = ?,
                youtube_edited_at = ?, upstream_source = ?, raw_json = ?,
                last_seen_sync_at = ?, title_norm = ?,
                tdb_dropped_at = NULL
            WHERE media_type = ? AND tmdb_id = ?
            """,
            (
                imdb_id, title, original_title, year, rd or None,
                yt_url, yt_vid, yt_added, yt_edited,
                upstream_source, _safe_json(record), sync_ts, title_norm,
                media_type, tmdb_id,
            ),
        )

    return is_new, url_changed, old_vid


def _safe_json(record: dict) -> str:
    import json
    try:
        return json.dumps(record, ensure_ascii=False)
    except (TypeError, ValueError):
        return "{}"


def _enqueue_download(
    conn: sqlite3.Connection,
    *,
    media_type: str,
    tmdb_id: int,
    reason: str,
    auto_place: bool | None = None,
    force_place: bool = False,
    only_section_id: str | None = None,
    rollback: dict | None = None,
) -> int:
    """v1.11.0: enqueue one download job per (media_type, tmdb_id, section)
    where this item lives — sections discovered via plex_items joined to
    plex_sections (only INCLUDED sections). Items present in 0 sections
    don't get any download job (nothing to drop the file into yet); the
    next sync after plex_enum will pick them up.

    Dedupes against any existing pending/running download for the same
    (item, section). Returns the count of jobs actually enqueued.

    `auto_place=None` means "use the global setting"; True/False writes
    an explicit override into the job payload that the worker honors.

    v1.12.43: `force_place=True` adds force_place to the payload so the
    chained place job runs with force=True, bypassing the worker's
    skip_if_plex_has_theme guard. Used by ACCEPT UPDATE so the new TDB
    theme actually overwrites the existing user-source theme.mp3 in the
    Plex folder. Pre-fix the place job ran but skipped with
    "plex_has_theme" because Plex still cached the old theme as
    has_theme=true — the file got downloaded into canonical but never
    replaced the placement file in /movies/, leaving Plex playing the
    old U theme.

    v1.12.114: `rollback` carries optional recovery state the worker
    reads on permanent download failure. Used by ACCEPT UPDATE
    (rollback to user_overrides + decision='pending') and REVERT
    (rollback to prior previous_urls + override). Without rollback,
    a download failure leaves motif in the post-action state with
    the old state already destroyed — user has no easy recovery.
    """
    plex_type = "show" if media_type == "tv" else "movie"
    # v1.12.46: scope to a single section when only_section_id is
    # set (e.g., ACCEPT UPDATE clicked from a specific library
    # row should only re-theme that section's edition, not fan
    # out to every section that owns the title).
    if only_section_id is not None:
        sections = conn.execute(
            """SELECT DISTINCT pi.section_id
               FROM plex_items pi
               JOIN plex_sections ps ON ps.section_id = pi.section_id
               WHERE pi.guid_tmdb = ?
                 AND pi.media_type = ?
                 AND pi.section_id = ?
                 AND ps.included = 1""",
            (tmdb_id, plex_type, only_section_id),
        ).fetchall()
    else:
        sections = conn.execute(
            """SELECT DISTINCT pi.section_id
               FROM plex_items pi
               JOIN plex_sections ps ON ps.section_id = pi.section_id
               WHERE pi.guid_tmdb = ?
                 AND pi.media_type = ?
                 AND ps.included = 1""",
            (tmdb_id, plex_type),
        ).fetchall()
    if not sections:
        # Nothing to enqueue — the item isn't in any included Plex
        # section yet. Common during the window between sync (which
        # creates the themes row) and plex_enum (which populates
        # plex_items). User-initiated callers (manual_url, redownload,
        # override, accept_update, etc.) should treat the 0 return as
        # "queued nothing — Plex hasn't seen this item yet" and surface
        # that to the user.
        log.info(
            "_enqueue_download: no included Plex section owns "
            "(%s, %s) — skipping (reason=%s)",
            media_type, tmdb_id, reason,
        )
        return 0
    enqueued = 0
    for sec in sections:
        section_id = sec["section_id"]
        existing = conn.execute(
            """
            SELECT id FROM jobs
            WHERE job_type = 'download' AND media_type = ? AND tmdb_id = ?
              AND section_id = ? AND status IN ('pending', 'running')
            """,
            (media_type, tmdb_id, section_id),
        ).fetchone()
        if existing:
            continue
        payload: dict = {"reason": reason}
        if auto_place is not None:
            payload["auto_place"] = bool(auto_place)
        if force_place:
            payload["force_place"] = True
        if rollback is not None:
            # v1.12.114: stamped per-section so the worker can restore
            # the right override / previous_urls row on failure.
            payload["rollback"] = {**rollback, "section_id": section_id}
        conn.execute(
            """
            INSERT INTO jobs (job_type, media_type, tmdb_id, section_id,
                              payload, status, created_at, next_run_at)
            VALUES ('download', ?, ?, ?, ?, 'pending', ?, ?)
            """,
            (media_type, tmdb_id, section_id, json.dumps(payload),
             now_iso(), now_iso()),
        )
        enqueued += 1
    return enqueued


# ----- Public entry point -----

# How many fetched items to process per write transaction. Mirrors the
# plex_enum batching from v1.9.6: smaller batches release the BEGIN
# IMMEDIATE lock more often, letting concurrent API writers (log_event
# in particular) make progress while a multi-thousand-item sync is
# running. Pre-1.9.9 sync did one transaction per item, ~10K transactions
# end-to-end, which soft-locked the API for the duration.
_SYNC_BATCH = 50


def _plex_supplies_theme(
    conn: sqlite3.Connection, media_type: str, tmdb_id: int,
) -> bool:
    """v1.10.16: True iff Plex's own agent supplies a theme for this
    title and motif doesn't manage the file. Used by sync to skip the
    auto-download for P-agent items so we honour the user's 'Plex-first'
    preference.

    Conditions (all must hold):
      - plex_items row exists for this (media_type, tmdb_id) — the
        title is in the user's Plex library.
      - pi.has_theme = 1 — Plex says the item has a theme.
      - pi.local_theme_file = 0 — there's no theme.mp3 sidecar at the
        Plex folder.
      - no local_files row — motif hasn't downloaded for this item.
      - no placements row — motif hasn't placed for this item.
    Falls back to False (= proceed with download) if plex_items hasn't
    been enumerated yet (fresh install) — safe default.
    """
    row = conn.execute(
        """SELECT 1
             FROM plex_items pi
             LEFT JOIN local_files lf
               ON lf.media_type = ? AND lf.tmdb_id = ?
             LEFT JOIN placements p
               ON p.media_type = ? AND p.tmdb_id = ?
            WHERE pi.guid_tmdb = ?
              AND pi.media_type = (CASE ? WHEN 'tv' THEN 'show' ELSE 'movie' END)
              AND pi.has_theme = 1
              AND pi.local_theme_file = 0
              AND lf.file_path IS NULL
              AND p.media_folder IS NULL
            LIMIT 1""",
        (media_type, tmdb_id, media_type, tmdb_id, tmdb_id, media_type),
    ).fetchone()
    return row is not None


# v1.11.17: probe enqueue + handler removed entirely. The TDB pill
# color is still driven by failure_kind from real download attempts;
# the speculative probe pass added 25-50 minutes of yt-dlp work per
# sync without a meaningful UX gain.


def _flush_sync_batch(
    db_path,
    batch: list[tuple[str, int, dict, str]],
    *,
    sync_ts: str,
    enqueue_downloads: bool,
    auto_place_override: bool | None,
    stats: SyncStats,
) -> None:
    """Process one batch of fetched ThemerrDB records inside a single txn.

    `batch` is a list of (media_type, tmdb_id, record, upstream_source).
    """
    if not batch:
        return
    with get_conn(db_path) as conn, transaction(conn):
        for media_type, tmdb_id, record, upstream_source in batch:
            is_new, url_changed, old_video_id = _upsert_theme(
                conn,
                media_type=media_type,
                tmdb_id=tmdb_id,
                record=record,
                upstream_source=upstream_source,
                sync_ts=sync_ts,
            )
            # v1.10.16: Plex-first default — when Plex is already supplying
            # a theme for this title (P-agent state) and motif doesn't
            # manage the file yet, skip the auto-download. The user can
            # still kick a download manually from the row's primary
            # button (which prompts for confirmation per v1.10.14).
            # 'auto' here means motif initiated the enqueue; manual
            # /redownload calls bypass this gate.
            plex_supplies = _plex_supplies_theme(conn, media_type, tmdb_id)
            if is_new:
                stats.new_count += 1
                # v1.12.17: cross-sync match detection. If motif (or Plex
                # via the local_theme_file flag) already has theme
                # content for this title, surface a blue update pill
                # via pending_updates instead of auto-downloading. Lets
                # the user explicitly choose to switch from their
                # existing theme (M sidecar / U manual URL / A adopted)
                # to motif-managed TDB content. Pre-fix the sync just
                # auto-downloaded over the existing local theme.
                has_local_files = conn.execute(
                    "SELECT 1 FROM local_files WHERE media_type = ? AND tmdb_id = ?",
                    (media_type, tmdb_id),
                ).fetchone() is not None
                has_override = conn.execute(
                    "SELECT 1 FROM user_overrides WHERE media_type = ? AND tmdb_id = ?",
                    (media_type, tmdb_id),
                ).fetchone() is not None
                plex_mt_for_sidecar = "show" if media_type == "tv" else "movie"
                has_sidecar = conn.execute(
                    """SELECT 1 FROM plex_items
                        WHERE guid_tmdb = ? AND media_type = ?
                          AND local_theme_file = 1
                        LIMIT 1""",
                    (tmdb_id, plex_mt_for_sidecar),
                ).fetchone() is not None
                yt_url = record.get("youtube_theme_url")
                if yt_url and (has_local_files or has_override or has_sidecar):
                    # Cross-match: prompt instead of fetch.
                    # v1.12.99: schema v31 added section_id to the PK.
                    # Sync writes the title-global '' row; per-section
                    # accept/decline rows (created on user action) take
                    # precedence over '' via the library SQL's COALESCE
                    # resolution. ON CONFLICT now matches the full PK.
                    yt_vid = extract_video_id(yt_url)
                    conn.execute(
                        """
                        INSERT INTO pending_updates (
                            media_type, tmdb_id, section_id, old_video_id, new_video_id,
                            old_youtube_url, new_youtube_url,
                            upstream_edited_at, detected_at, decision
                        ) VALUES (?, ?, '', NULL, ?, NULL, ?, ?, ?, 'pending')
                        ON CONFLICT(media_type, tmdb_id, section_id) DO UPDATE SET
                            new_video_id = excluded.new_video_id,
                            new_youtube_url = excluded.new_youtube_url,
                            upstream_edited_at = excluded.upstream_edited_at,
                            detected_at = excluded.detected_at,
                            decision = CASE
                                WHEN pending_updates.decision = 'declined'
                                THEN 'declined'
                                ELSE 'pending'
                            END
                        """,
                        (
                            media_type, tmdb_id, yt_vid, yt_url,
                            record.get("youtube_theme_edited"), sync_ts,
                        ),
                    )
                elif enqueue_downloads and not plex_supplies:
                    _enqueue_download(
                        conn, media_type=media_type, tmdb_id=tmdb_id,
                        reason="new", auto_place=auto_place_override,
                    )
            elif url_changed:
                stats.updated_count += 1
                already_have = conn.execute(
                    "SELECT 1 FROM local_files WHERE media_type = ? AND tmdb_id = ?",
                    (media_type, tmdb_id),
                ).fetchone() is not None
                has_override = conn.execute(
                    "SELECT 1 FROM user_overrides WHERE media_type = ? AND tmdb_id = ?",
                    (media_type, tmdb_id),
                ).fetchone() is not None
                if already_have or has_override:
                    # v1.12.99: schema v31 added section_id to the PK.
                    # Sync writes the title-global '' row.
                    yt_vid = extract_video_id(record.get("youtube_theme_url"))
                    conn.execute(
                        """
                        INSERT INTO pending_updates (
                            media_type, tmdb_id, section_id, old_video_id, new_video_id,
                            old_youtube_url, new_youtube_url,
                            upstream_edited_at, detected_at, decision
                        ) VALUES (?, ?, '', ?, ?, ?, ?, ?, ?, 'pending')
                        ON CONFLICT(media_type, tmdb_id, section_id) DO UPDATE SET
                            new_video_id = excluded.new_video_id,
                            new_youtube_url = excluded.new_youtube_url,
                            upstream_edited_at = excluded.upstream_edited_at,
                            detected_at = excluded.detected_at,
                            decision = CASE
                                WHEN pending_updates.decision = 'declined'
                                THEN 'declined'
                                ELSE 'pending'
                            END
                        """,
                        (
                            media_type, tmdb_id,
                            old_video_id, yt_vid,
                            None, record.get("youtube_theme_url"),
                            record.get("youtube_theme_edited"),
                            sync_ts,
                        ),
                    )
                else:
                    # Same Plex-first gate as the is_new branch above:
                    # don't pull a fresh download for a P-agent item.
                    if enqueue_downloads and not plex_supplies:
                        _enqueue_download(
                            conn, media_type=media_type, tmdb_id=tmdb_id,
                            reason="url_changed",
                            auto_place=auto_place_override,
                        )


# ----- v1.12.121 Phase A: snapshot path -----
#
# The remote path makes one HTTP request per item against gh-pages — at
# ~10k items that's the dominant cost of a sync. The snapshot path pulls
# a single tarball of LizardByte/ThemerrDB's `database` branch from
# codeload.github.com, extracts it to a temp dir, and resolves every
# item from the local tree. Same JSON shape, same upsert path; ~1000×
# fewer HTTP requests + free differential sync once Phase B lands.
#
# Failure here is non-fatal — `run_sync` falls through to the remote
# path and flags `op_progress.detail.fallback_active=True` so the
# status bar can surface "last sync used the fallback".

import io
import os
import tarfile
import tempfile
from contextlib import contextmanager


class _SnapshotError(RuntimeError):
    """Snapshot acquisition (download/extract/validate) failed.
    Caller should fall back to the remote path."""


class _SnapshotUnchanged(Exception):
    """Codeload returned 304 Not Modified — the tarball at HEAD of
    the database branch is byte-identical to what we last pulled.
    NOT a failure: caller should skip the entire upsert pipeline
    (no need to re-walk 5k items whose data hasn't changed) and
    proceed to the prune sweeps. v1.12.126 Phase A.5."""


# Hard-coded ceiling on the extracted tree. Catches a runaway tarball
# (e.g. zip-bomb-style nested archives that decompress into hundreds of
# GB) before it fills the disk. The real database branch sits at ~52
# MB extracted; a 500 MB ceiling gives 10× headroom and still flags
# anything pathological. v1.12.121.
_SNAPSHOT_MAX_BYTES = 500 * 1024 * 1024
# Reject archives with absurd member counts. Real branch has ~30k
# members; 200k catches a degenerate millions-of-empty-files tarball
# without false-positiving on legitimate growth.
_SNAPSHOT_MAX_MEMBERS = 200_000


class _DatabaseSnapshot:
    """Codeload tarball view of the ThemerrDB `database` branch.

    Lifecycle:
      snap = _DatabaseSnapshot(db_path, op_id, url, cancel_check)
      snap.acquire(client)              # download + extract + validate
      try:
          idx = snap.index("movies")    # local read of pages.json + all_page_*.json
          rec = snap.fetch_item("movies", imdb_id="tt...", tmdb_id=123)
      finally:
          snap.release()                # rmtree the temp dir

    Cancel check is polled between download chunks and between
    extracted members so a // CANCEL during snapshot acquisition
    bails within ~1MB / ~1k files.

    The cached tarball lives at <db_dir>/cache/themerrdb-database.tar.gz.
    On a successful pull it survives so a future sync that fails to
    re-download can fall back to the prior snapshot (Phase B will
    use it as the diff baseline).
    """

    def __init__(
        self,
        db_path: Path,
        op_id: str,
        tar_url: str,
        cancel_check,
    ):
        self.db_path = Path(db_path)
        self.op_id = op_id
        self.tar_url = tar_url
        self._cancel_check = cancel_check
        # Where the tarball is cached. Lives next to motif.db, not under
        # themes_dir — it's operational state, not user content.
        self.cache_dir = self.db_path.parent / "cache"
        self.tar_path = self.cache_dir / "themerrdb-database.tar.gz"
        # v1.12.126 Phase A.5: conditional-GET state lives next to the
        # cached tarball. JSON file: {"etag": "...", "last_modified": "..."}.
        # On the next sync we send these as If-None-Match + If-
        # Modified-Since; codeload returns 304 if the database branch
        # head hasn't moved, and we skip the whole upsert pipeline.
        self.meta_path = self.cache_dir / "themerrdb-database.tar.gz.meta"
        # Populated on acquire().
        self._tmpdir: tempfile.TemporaryDirectory | None = None
        self.root: Path | None = None
        # v1.12.126: True when codeload returned 304 (no upstream change).
        # Caller checks via is_unchanged() and skips index/fetch.
        self._unchanged = False

    # -- acquisition --

    def is_unchanged(self) -> bool:
        return self._unchanged

    def acquire(self, client: httpx.Client) -> None:
        """Download + extract + validate. Raises _SnapshotError on
        any failure that should trigger fallback to the remote path.

        v1.12.126 Phase A.5: when codeload returns 304 Not Modified
        the download short-circuits via _SnapshotUnchanged, which
        acquire() catches and translates into self._unchanged=True.
        Caller checks via is_unchanged() and skips the upsert
        pipeline. NOT a failure — the data is still fresh, just
        identical to what we already have.

        Cancellation propagates through unchanged: a // CANCEL during
        snapshot acquisition raises _JobCancelled, which is the
        sync-wide bail signal — wrapping it as _SnapshotError would
        spuriously trigger the fallback path (and stick the warn-tone
        pill on the idle indicator) instead of ending the run cleanly
        as 'cancelled'.
        """
        # Imported lazily to keep the worker→sync→worker cycle tractable.
        from .worker import _JobCancelled
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        try:
            self._download(client)
            self._extract()
            self._validate()
        except _SnapshotUnchanged:
            # 304 short-circuit. Don't release — there's no tmpdir
            # to clean (we never extracted). is_unchanged() now True.
            self._unchanged = True
            return
        except _JobCancelled:
            self.release()
            raise
        except _SnapshotError:
            self.release()
            raise
        except Exception as e:
            self.release()
            raise _SnapshotError(
                f"snapshot acquire failed: {type(e).__name__}: {e}"
            ) from e

    def release(self) -> None:
        if self._tmpdir is not None:
            try:
                self._tmpdir.cleanup()
            except Exception as e:
                log.warning("snapshot tmpdir cleanup failed: %s", e)
            self._tmpdir = None
            self.root = None

    # -- stages --

    def _download(self, client: httpx.Client) -> None:
        op_progress.update_progress(
            self.db_path, self.op_id,
            stage="snapshot_download",
            stage_label="Downloading ThemerrDB snapshot",
            stage_current=0, stage_total=0,
            activity=f"GET {self.tar_url}",
        )
        # v1.12.126 Phase A.5: conditional GET. If we have ETag /
        # Last-Modified from the prior successful download AND the
        # cached tarball is still on disk, send the If-None-Match /
        # If-Modified-Since headers. codeload returns 304 if the
        # tree hasn't changed — at which point we short-circuit
        # the entire sync (skip extract + index + fetch + flush).
        # Headers are only useful when we'd actually use the cache,
        # so gate on tar_path existing too — protects against the
        # case where the user wiped the cache dir but the meta file
        # somehow survived.
        cond_headers: dict[str, str] = {}
        if self.tar_path.is_file() and self.meta_path.is_file():
            try:
                meta = json.loads(self.meta_path.read_text())
                if meta.get("etag"):
                    cond_headers["If-None-Match"] = meta["etag"]
                if meta.get("last_modified"):
                    cond_headers["If-Modified-Since"] = meta["last_modified"]
            except (OSError, ValueError) as e:
                log.debug("snapshot meta unreadable: %s", e)
        # Stream to a sibling .partial file then atomic-rename. Crash
        # mid-download leaves only the .partial; the next run discards
        # it and starts fresh.
        partial = self.tar_path.with_suffix(self.tar_path.suffix + ".partial")
        try:
            with client.stream("GET", self.tar_url, headers=cond_headers) as r:
                if r.status_code == 304:
                    # Tree unchanged. Update mtime on the meta file so
                    # future "is the cache fresh" checks (Phase B may
                    # add one) see the latest verification time.
                    op_progress.update_progress(
                        self.db_path, self.op_id,
                        activity="Snapshot unchanged (304) — skipping pipeline",
                    )
                    log.info(
                        "snapshot: codeload returned 304 (tree unchanged); "
                        "skipping extract + upsert"
                    )
                    raise _SnapshotUnchanged()
                if r.status_code != 200:
                    raise _SnapshotError(
                        f"codeload returned HTTP {r.status_code}"
                    )
                # Capture validators for the next conditional GET. Save
                # them only after the body streams successfully (below)
                # — a corrupted/truncated download must NOT advance
                # the meta or we'd 304 onto a half-broken cached tarball
                # next time.
                fresh_etag = r.headers.get("etag")
                fresh_last_mod = r.headers.get("last-modified")
                total = int(r.headers.get("content-length") or 0)
                if total > _SNAPSHOT_MAX_BYTES:
                    raise _SnapshotError(
                        f"snapshot Content-Length {total} exceeds ceiling "
                        f"{_SNAPSHOT_MAX_BYTES}"
                    )
                op_progress.update_progress(
                    self.db_path, self.op_id,
                    stage_current=0, stage_total=total,
                )
                received = 0
                last_emit = time.monotonic()
                with partial.open("wb") as fp:
                    for chunk in r.iter_bytes(chunk_size=64 * 1024):
                        if self._cancel_check():
                            from .worker import _JobCancelled
                            raise _JobCancelled()
                        fp.write(chunk)
                        received += len(chunk)
                        if received > _SNAPSHOT_MAX_BYTES:
                            raise _SnapshotError(
                                f"snapshot download exceeded ceiling "
                                f"{_SNAPSHOT_MAX_BYTES} bytes"
                            )
                        if (time.monotonic() - last_emit) > 0.5:
                            op_progress.update_progress(
                                self.db_path, self.op_id,
                                stage_current=received,
                                stage_total=total,
                            )
                            last_emit = time.monotonic()
            os.replace(partial, self.tar_path)
            # v1.12.126 Phase A.5: persist conditional-GET validators
            # only after the body lands. A truncated download must
            # not advance the meta — next run would otherwise 304
            # against a half-broken cached tarball.
            if fresh_etag or fresh_last_mod:
                meta_payload: dict[str, str] = {}
                if fresh_etag:
                    meta_payload["etag"] = fresh_etag
                if fresh_last_mod:
                    meta_payload["last_modified"] = fresh_last_mod
                try:
                    self.meta_path.write_text(json.dumps(meta_payload))
                except OSError as e:
                    log.warning("snapshot meta write failed: %s", e)
            op_progress.update_progress(
                self.db_path, self.op_id,
                stage_current=received, stage_total=received,
                activity=f"Snapshot downloaded ({received} bytes)",
            )
        except _SnapshotError:
            partial.unlink(missing_ok=True)
            raise
        except _SnapshotUnchanged:
            # 304 path — no partial to clean up, no error to wrap.
            partial.unlink(missing_ok=True)
            raise
        except httpx.HTTPError as e:
            partial.unlink(missing_ok=True)
            raise _SnapshotError(f"download HTTP error: {e}") from e
        except BaseException:
            # Catches _JobCancelled (via Exception) and KeyboardInterrupt
            # so the half-written .partial doesn't linger across runs.
            # Re-raises untouched — acquire() decides how to dispatch.
            partial.unlink(missing_ok=True)
            raise

    def _extract(self) -> None:
        op_progress.update_progress(
            self.db_path, self.op_id,
            stage="snapshot_extract",
            stage_label="Extracting snapshot",
            stage_current=0, stage_total=0,
            activity="Extracting tarball",
        )
        self._tmpdir = tempfile.TemporaryDirectory(prefix="motif-tdb-")
        dest = Path(self._tmpdir.name)
        # Strict member filter: reject anything that isn't a regular
        # file or directory under <prefix>/(movies|tv_shows)/, and
        # refuse symlinks/hardlinks/devices outright. Path components
        # are checked for `..` and absolute roots.
        allowed_subdirs = ("movies", "tv_shows")
        try:
            with tarfile.open(self.tar_path, mode="r:gz") as tf:
                # Enumerate first so we can report stage_total.
                members = tf.getmembers()
                if len(members) > _SNAPSHOT_MAX_MEMBERS:
                    raise _SnapshotError(
                        f"snapshot has {len(members)} members "
                        f"(ceiling {_SNAPSHOT_MAX_MEMBERS})"
                    )
                op_progress.update_progress(
                    self.db_path, self.op_id,
                    stage_current=0, stage_total=len(members),
                )
                # Detect the prefix dir (e.g. "ThemerrDB-database/").
                # codeload tarballs always have a single top-level
                # directory named "<repo>-<branch>".
                prefix: str | None = None
                for m in members:
                    head = m.name.split("/", 1)[0]
                    if head and not head.startswith("."):
                        prefix = head
                        break
                if prefix is None:
                    raise _SnapshotError("snapshot has no top-level directory")

                extracted = 0
                last_emit = time.monotonic()
                total_bytes = 0
                for i, m in enumerate(members):
                    if i % 256 == 0 and self._cancel_check():
                        from .worker import _JobCancelled
                        raise _JobCancelled()
                    if not (m.isreg() or m.isdir()):
                        # symlink, hardlink, fifo, device, char — reject.
                        continue
                    # Strip the codeload prefix.
                    if not m.name.startswith(prefix + "/") and m.name != prefix:
                        continue
                    rel = m.name[len(prefix) + 1:] if m.name != prefix else ""
                    if not rel:
                        continue
                    # Tar-slip defenses.
                    if rel.startswith("/") or ".." in rel.split("/"):
                        raise _SnapshotError(
                            f"snapshot member has unsafe path: {m.name!r}"
                        )
                    # Only keep movies/ and tv_shows/. Everything else
                    # (root README.md, .github/, etc.) is dropped.
                    if not any(
                        rel == sub or rel.startswith(sub + "/")
                        for sub in allowed_subdirs
                    ):
                        continue
                    target = dest / rel
                    # Final paranoia: resolved target must stay under dest.
                    if not str(target.resolve()).startswith(
                        str(dest.resolve()) + os.sep
                    ) and target.resolve() != dest.resolve():
                        raise _SnapshotError(
                            f"snapshot member resolves outside dest: {m.name!r}"
                        )
                    if m.isdir():
                        target.mkdir(parents=True, exist_ok=True)
                    else:
                        target.parent.mkdir(parents=True, exist_ok=True)
                        f = tf.extractfile(m)
                        if f is None:
                            continue
                        data = f.read()
                        total_bytes += len(data)
                        if total_bytes > _SNAPSHOT_MAX_BYTES:
                            raise _SnapshotError(
                                f"extracted size exceeds ceiling "
                                f"{_SNAPSHOT_MAX_BYTES}"
                            )
                        with target.open("wb") as fp:
                            fp.write(data)
                    extracted += 1
                    if (time.monotonic() - last_emit) > 0.5:
                        op_progress.update_progress(
                            self.db_path, self.op_id,
                            stage_current=extracted,
                            stage_total=len(members),
                        )
                        last_emit = time.monotonic()
                op_progress.update_progress(
                    self.db_path, self.op_id,
                    stage_current=extracted, stage_total=len(members),
                    activity=f"Extracted {extracted} files ({total_bytes} bytes)",
                )
        except tarfile.TarError as e:
            raise _SnapshotError(f"tar extraction failed: {e}") from e
        # Sanity check: at least one of the expected media subdirs
        # must exist after extraction.
        if not any((dest / sub).is_dir() for sub in allowed_subdirs):
            raise _SnapshotError(
                f"snapshot missing expected dirs: {allowed_subdirs}"
            )
        self.root = dest

    def _validate(self) -> None:
        """Smoke check: read pages.json + one per-item file from the
        movies tree. Confirm the JSON shape still has `youtube_theme_url`
        addressable on a record. Catches schema drift before we
        upsert thousands of malformed rows."""
        if self.root is None:
            raise _SnapshotError("validate before extract")
        movies_dir = self.root / "movies"
        pages = movies_dir / "pages.json"
        if not pages.is_file():
            raise _SnapshotError("snapshot missing movies/pages.json")
        try:
            meta = json.loads(pages.read_text())
        except (OSError, ValueError) as e:
            raise _SnapshotError(f"pages.json unreadable: {e}") from e
        if not isinstance(meta, dict) or "pages" not in meta:
            raise _SnapshotError("pages.json shape unexpected")
        # Read first page; sample one record.
        first_page = movies_dir / "all_page_1.json"
        if first_page.is_file():
            try:
                entries = json.loads(first_page.read_text())
            except (OSError, ValueError) as e:
                raise _SnapshotError(f"all_page_1.json unreadable: {e}") from e
            if entries and isinstance(entries, list):
                sample = entries[0]
                tmdb_id = sample.get("id")
                imdb_id = sample.get("imdb_id")
                rec = self._read_item("movies", imdb_id=imdb_id, tmdb_id=tmdb_id)
                if rec is None:
                    raise _SnapshotError(
                        f"sample item not resolvable in snapshot "
                        f"(tmdb={tmdb_id}, imdb={imdb_id})"
                    )
                # Don't require a value (may legitimately be null);
                # do require the key be present so the upsert path
                # can read it.
                if "youtube_theme_url" not in rec:
                    raise _SnapshotError(
                        "sample item missing youtube_theme_url field "
                        "(schema drift?)"
                    )

    # -- read-side, mirrors the remote helpers --

    def index(self, media_path: str) -> list[dict]:
        if self.root is None:
            raise _SnapshotError("index before acquire")
        media_dir = self.root / media_path
        pages_file = media_dir / "pages.json"
        if not pages_file.is_file():
            return []
        try:
            meta = json.loads(pages_file.read_text())
        except (OSError, ValueError) as e:
            raise _SnapshotError(f"{pages_file}: {e}") from e
        total_pages = int(meta.get("pages", 0))
        items: list[dict] = []
        for n in range(1, total_pages + 1):
            page_file = media_dir / f"all_page_{n}.json"
            try:
                items.extend(json.loads(page_file.read_text()))
            except (OSError, ValueError) as e:
                log.warning("snapshot page %s unreadable: %s", page_file, e)
        return items

    def fetch_item(
        self, media_path: str, *,
        imdb_id: str | None, tmdb_id: int,
    ) -> dict | None:
        return self._read_item(media_path, imdb_id=imdb_id, tmdb_id=tmdb_id)

    def _read_item(
        self, media_path: str, *,
        imdb_id: str | None, tmdb_id: int | None,
    ) -> dict | None:
        if self.root is None:
            return None
        media_dir = self.root / media_path
        candidates = []
        if imdb_id:
            candidates.append(media_dir / "imdb" / f"{imdb_id}.json")
        if tmdb_id is not None:
            candidates.append(media_dir / "themoviedb" / f"{tmdb_id}.json")
        for path in candidates:
            try:
                if path.is_file():
                    return json.loads(path.read_text())
            except (OSError, ValueError) as e:
                log.debug("snapshot item %s unreadable: %s", path, e)
                continue
        return None


# ----- v1.13.0 Phase B: git differential -----
#
# Phase A pulls the full database tarball every sync regardless of how
# much actually changed. On a quiet day TDB might publish 5-50 changed
# items; we still re-walk all 5,178. Phase B keeps a bare git mirror
# of the LizardByte/ThemerrDB `database` branch via dulwich and
# fetches only the delta. First run shallow-clones (~30 MB pack);
# subsequent runs fetch ~tens-of-KB pack-deltas.
#
# Failure here is non-fatal — run_sync falls through to Phase A
# (snapshot tarball) and flags fallback_active accordingly. If the
# snapshot path also fails, the existing fallback continues to remote.


class _GitMirrorError(RuntimeError):
    """Git mirror acquisition failed (network, dulwich error, repo
    corruption). Caller should fall back to the snapshot/remote
    path."""


class _GitMirrorUnchanged(Exception):
    """Fetch returned no new commits — branch HEAD on the remote is
    identical to our last successful sync. NOT a failure: caller
    should skip the upsert pipeline and proceed to prune sweeps."""


# Hard-coded ceilings on the bare repo. Catches a repo that has
# grown unreasonably large (fetched megabytes of binary blobs we
# don't actually want) before it fills disk.
_GIT_MIRROR_MAX_BYTES = 500 * 1024 * 1024
# Max number of changed paths we'll process in one run. Real branch
# changes are typically a handful per day; tens of thousands would be
# a re-import event we should fall back from rather than try to
# digest in one transaction. Phase A's snapshot path can replay the
# whole tree if we land in this case.
_GIT_MIRROR_MAX_CHANGES = 50_000


class _GitMirror:
    """Bare-git-repo view of the ThemerrDB `database` branch.

    Lifecycle:
      mirror = _GitMirror(db_path, op_id, repo_url, branch, cancel_check)
      mirror.acquire(client)        # clone (first run) or fetch (subsequent)
      if mirror.is_unchanged():
          # remote HEAD didn't move; skip upsert, run prune only.
          ...
      else:
          changes = mirror.list_changes()    # ChangeSet
          for media_type, path in changes.changed:
              record = mirror.read_json(path)
              ...
          mirror.commit_sync_ok()           # advance MOTIF_LAST_SYNC

    The repo lives at <db_dir>/cache/themerrdb-database.git/. Last-
    known-synced HEAD is persisted at .../MOTIF_LAST_SYNC alongside
    the repo files (single-line text: 40-char hex SHA). On any
    inconsistency (missing file, SHA not present in repo) we treat
    the next acquire as a first run so we can re-establish a known
    baseline.
    """

    def __init__(
        self,
        db_path: Path,
        op_id: str,
        repo_url: str,
        branch: str,
        cancel_check,
    ):
        self.db_path = Path(db_path)
        self.op_id = op_id
        self.repo_url = repo_url
        self.branch = branch
        self.branch_ref = f"refs/heads/{branch}".encode()
        self._cancel_check = cancel_check
        self.cache_dir = self.db_path.parent / "cache"
        self.repo_path = self.cache_dir / "themerrdb-database.git"
        self.last_sync_path = self.repo_path / "MOTIF_LAST_SYNC"
        # Populated by acquire().
        self._repo = None  # type: ignore[assignment]
        self._old_head: bytes | None = None
        self._new_head: bytes | None = None
        self._unchanged = False

    # -- public API --

    def is_unchanged(self) -> bool:
        return self._unchanged

    def acquire(self, _client_unused=None) -> None:
        """Clone (first run) or fetch (subsequent). On fetch with no
        new commits, raises _GitMirrorUnchanged via internal flag.

        The httpx client param is accepted-but-ignored for parity with
        _DatabaseSnapshot.acquire's signature; dulwich uses its own
        HTTP transport.

        Raises _GitMirrorError on any failure that should trigger
        fallback to the snapshot path.
        """
        from .worker import _JobCancelled
        try:
            self.cache_dir.mkdir(parents=True, exist_ok=True)
            if self._repo_is_initialized():
                self._fetch()
            else:
                self._clone()
            self._post_acquire_validate()
        except _JobCancelled:
            self._close()
            raise
        except _GitMirrorError:
            self._close()
            raise
        except Exception as e:
            self._close()
            raise _GitMirrorError(
                f"git mirror acquire failed: {type(e).__name__}: {e}"
            ) from e

    def list_changes(self):
        """Compute the diff between MOTIF_LAST_SYNC (or empty) and
        new HEAD. Returns _ChangeSet with three lists of relative
        paths under the branch root.

        On a first run (no prior MOTIF_LAST_SYNC), returns every
        path in the new tree as 'added' — caller treats it as a
        full-index walk equivalent to Phase A.
        """
        from dulwich.diff_tree import tree_changes
        if self._repo is None or self._new_head is None:
            raise _GitMirrorError("list_changes before acquire")
        op_progress.update_progress(
            self.db_path, self.op_id,
            stage="git_diff", stage_label="Computing git diff",
            stage_current=0, stage_total=0,
            activity=f"diff {self._summary_sha(self._old_head)}..."
                     f"{self._summary_sha(self._new_head)}",
        )
        new_commit = self._repo[self._new_head]
        if self._old_head is None:
            # First run — every path is "new". Walk the new tree to
            # collect all blob paths so the upsert path knows what
            # to read. Same scope as Phase A's full-index walk.
            return _ChangeSet(
                added=self._walk_tree_paths(new_commit.tree),
                modified=[],
                removed=[],
            )
        old_commit = self._repo[self._old_head]
        added: list[str] = []
        modified: list[str] = []
        removed: list[str] = []
        n = 0
        for ch in tree_changes(self._repo.object_store, old_commit.tree, new_commit.tree):
            if n >= _GIT_MIRROR_MAX_CHANGES:
                raise _GitMirrorError(
                    f"git diff produced > {_GIT_MIRROR_MAX_CHANGES} "
                    f"changed paths — falling back"
                )
            n += 1
            old_path = (ch.old.path.decode() if ch.old and ch.old.path else None)
            new_path = (ch.new.path.decode() if ch.new and ch.new.path else None)
            if ch.type == "add":
                if new_path:
                    added.append(new_path)
            elif ch.type == "modify":
                if new_path:
                    modified.append(new_path)
            elif ch.type == "delete":
                if old_path:
                    removed.append(old_path)
            elif ch.type == "rename":
                # Treat rename as remove + add for our purposes.
                if old_path:
                    removed.append(old_path)
                if new_path:
                    added.append(new_path)
        return _ChangeSet(added=added, modified=modified, removed=removed)

    def read_json(self, rel_path: str) -> dict | None:
        """Read a file from the new HEAD's tree, parse as JSON.
        Returns None if the path doesn't exist in the tree (caller
        should treat as "ThemerrDB no longer publishes this item")."""
        from dulwich.objects import Tree as DTree
        if self._repo is None or self._new_head is None:
            return None
        commit = self._repo[self._new_head]
        try:
            cur = self._repo[commit.tree]
        except KeyError:
            return None
        for part in rel_path.encode().split(b"/"):
            if not isinstance(cur, DTree):
                return None
            try:
                _mode, sha = cur[part]
            except KeyError:
                return None
            cur = self._repo[sha]
        if isinstance(cur, DTree):
            return None
        try:
            return json.loads(cur.as_raw_string())
        except (ValueError, AttributeError):
            return None

    def commit_sync_ok(self) -> None:
        """Persist the new HEAD as MOTIF_LAST_SYNC and advance the
        local tracking ref atomically. Call only after the upsert
        pipeline completes successfully — committing too early would
        either skip items (if MOTIF_LAST_SYNC advances) or
        misclassify the next run as is_unchanged (if the ref
        advances but MOTIF_LAST_SYNC doesn't and the prior SHA gets
        GC'd from the shallow object store).

        v1.13.6: ref-write moved here from _fetch so a crash mid-
        pipeline leaves MOTIF_LAST_SYNC, the local ref, AND the
        previous successful HEAD all in sync. The new objects from
        the fetch are kept in dulwich's object store regardless of
        ref state, so list_changes / read_json continue to work for
        the rest of THIS run.
        """
        if self._new_head is None:
            return
        # Advance the local ref first so the file write below is the
        # last commit-point — if writing the file fails the ref still
        # advances, and on the next run _read_last_sync()'s soft
        # fallback to the ref (line ~1488) keeps motif behavior
        # equivalent to a successful write.
        if self._repo is not None:
            try:
                self._repo.refs[self.branch_ref] = self._new_head
            except Exception as e:
                log.warning(
                    "git mirror: ref %s write failed: %s",
                    self.branch_ref, e)
        try:
            self.last_sync_path.write_text(self._new_head.decode())
        except OSError as e:
            log.warning("git mirror: MOTIF_LAST_SYNC write failed: %s", e)

    def release(self) -> None:
        """Close repo handles. Repo files stay on disk — they're
        the cache for the next sync."""
        self._close()

    def _close(self) -> None:
        if self._repo is not None:
            try:
                self._repo.close()
            except Exception:
                pass
            self._repo = None

    # -- stages --

    def _repo_is_initialized(self) -> bool:
        return (self.repo_path / "config").is_file() or (self.repo_path / "HEAD").is_file()

    def _clone(self) -> None:
        from dulwich.porcelain import clone
        op_progress.update_progress(
            self.db_path, self.op_id,
            stage="git_fetch",
            stage_label="Cloning ThemerrDB mirror (first run)",
            stage_current=0, stage_total=0,
            activity=f"git clone --bare --depth 1 -b {self.branch} {self.repo_url}",
        )
        # Wipe any partial state from a previous failed clone so we
        # don't try to fetch into a half-initialized dir.
        if self.repo_path.exists():
            import shutil
            shutil.rmtree(self.repo_path, ignore_errors=True)
        # Shallow clone keeps history bounded. dulwich's clone is
        # synchronous; cancel-check fires after it returns.
        clone(
            self.repo_url,
            str(self.repo_path),
            bare=True,
            branch=self.branch.encode(),
            depth=1,
        )
        if self._cancel_check():
            from .worker import _JobCancelled
            raise _JobCancelled()
        self._open_repo()
        self._old_head = None  # first run: no previous baseline
        new_head = self._repo.refs.as_dict().get(self.branch_ref)
        if new_head is None:
            raise _GitMirrorError(
                f"clone succeeded but branch {self.branch!r} ref missing"
            )
        self._new_head = new_head
        op_progress.update_progress(
            self.db_path, self.op_id,
            stage_current=1, stage_total=1,
            activity=f"Cloned {self.branch} at "
                     f"{self._summary_sha(new_head)}",
        )

    def _fetch(self) -> None:
        from dulwich.porcelain import fetch
        op_progress.update_progress(
            self.db_path, self.op_id,
            stage="git_fetch",
            stage_label="Fetching ThemerrDB delta",
            stage_current=0, stage_total=0,
            activity=f"git fetch {self.repo_url}",
        )
        self._open_repo()
        # Read prior HEAD from MOTIF_LAST_SYNC. Missing or out-of-store
        # → treat as first run (old_head=None) so list_changes returns
        # the full tree as 'added'. This is also the crash-recovery
        # path: if a prior run advanced the local ref via fetch but
        # crashed before commit_sync_ok wrote MOTIF_LAST_SYNC, we
        # must NOT diff from the ref (would give is_unchanged on the
        # next run with no new commits, silently dropping the
        # never-processed delta). Pre-v1.13.6 the soft fallback to
        # refs/heads/<branch> did exactly that — replaced now with a
        # full-walk recovery so the crashed-mid-pipeline state can be
        # safely retried.
        old_head = self._read_last_sync()
        self._old_head = old_head
        # The actual fetch.
        try:
            res = fetch(str(self.repo_path), self.repo_url, depth=1)
        except Exception as e:
            raise _GitMirrorError(f"dulwich fetch failed: {e}") from e
        if self._cancel_check():
            from .worker import _JobCancelled
            raise _JobCancelled()
        new_head = res.refs.get(self.branch_ref)
        if new_head is None:
            raise _GitMirrorError(
                f"fetch did not return branch {self.branch!r}; refs="
                f"{list(res.refs.keys())[:5]}"
            )
        # v1.13.6: do NOT advance the local tracking ref here. Wait for
        # commit_sync_ok() to write both MOTIF_LAST_SYNC and the ref
        # atomically, so a crash between fetch and upsert keeps both
        # at the prior good SHA. Pre-fix the ref advanced immediately
        # but MOTIF_LAST_SYNC didn't; on the next run _read_last_sync()
        # would fail (the prior SHA was GC'd from the shallow store)
        # and fall back to the now-advanced ref, treating the
        # unprocessed delta as is_unchanged and silently dropping it.
        # The new objects from this fetch are kept in the object store
        # by dulwich whether or not the ref points at them, so
        # list_changes / read_json work fine for the rest of this run.
        self._new_head = new_head
        if old_head is not None and old_head == new_head:
            # No new commits. is_unchanged() True; caller skips upsert.
            self._unchanged = True
            op_progress.update_progress(
                self.db_path, self.op_id,
                activity=f"No new commits since {self._summary_sha(old_head)}",
            )
            return
        op_progress.update_progress(
            self.db_path, self.op_id,
            stage_current=1, stage_total=1,
            activity=f"Fetched {self._summary_sha(old_head) if old_head else 'first run'}"
                     f" → {self._summary_sha(new_head)}",
        )

    def _post_acquire_validate(self) -> None:
        """Defense in depth: confirm the new HEAD's tree carries the
        expected media subdirs (movies/, tv_shows/) before we let
        list_changes loose. Catches a branch that's been renamed or
        repurposed without breaking the sync silently."""
        if self._new_head is None or self._unchanged:
            return
        from dulwich.objects import Tree as DTree
        try:
            commit = self._repo[self._new_head]
            tree = self._repo[commit.tree]
        except KeyError as e:
            raise _GitMirrorError(f"new HEAD tree missing: {e}") from e
        if not isinstance(tree, DTree):
            raise _GitMirrorError("new HEAD tree is not a tree object")
        names = {entry.path for entry in tree.iteritems()}
        if b"movies" not in names and b"tv_shows" not in names:
            raise _GitMirrorError(
                "branch HEAD missing both movies/ and tv_shows/ — "
                "schema drift or wrong branch"
            )

    # -- helpers --

    def _open_repo(self) -> None:
        from dulwich.repo import Repo
        if self._repo is None:
            self._repo = Repo(str(self.repo_path))

    def _read_last_sync(self) -> bytes | None:
        if not self.last_sync_path.is_file():
            return None
        try:
            txt = self.last_sync_path.read_text().strip()
        except OSError as e:
            # v1.13.10 (#16): an unreadable MOTIF_LAST_SYNC silently
            # triggers full-walk recovery, which is correct behavior
            # but invisible to the operator. Surface it once at WARN
            # so a corrupt FS / permissions issue is debuggable.
            log.warning(
                "git mirror: MOTIF_LAST_SYNC unreadable (%s) — "
                "falling back to full-walk recovery", e)
            return None
        if not txt:
            log.warning(
                "git mirror: MOTIF_LAST_SYNC is empty — falling back "
                "to full-walk recovery")
            return None
        if len(txt) != 40 or any(c not in "0123456789abcdef" for c in txt.lower()):
            log.warning(
                "git mirror: MOTIF_LAST_SYNC contents %r are not a "
                "valid SHA — falling back to full-walk recovery",
                txt[:64])
            return None
        sha = txt.encode()
        # Verify the SHA is actually present in the repo. A leftover
        # MOTIF_LAST_SYNC from a partially-deleted cache would
        # otherwise cause diff_tree to KeyError.
        try:
            _ = self._repo[sha]
        except KeyError:
            log.warning(
                "git mirror: MOTIF_LAST_SYNC %s missing from repo "
                "object store — treating as first run", txt[:8])
            return None
        return sha

    def _walk_tree_paths(self, tree_sha: bytes, prefix: bytes = b"") -> list[str]:
        """Yield every blob path under the tree, relative to the
        branch root. Used on first-run when there's no diff
        baseline."""
        from dulwich.objects import Tree as DTree
        out: list[str] = []
        try:
            tree = self._repo[tree_sha]
        except KeyError:
            return out
        if not isinstance(tree, DTree):
            return out
        for entry in tree.iteritems():
            full = (prefix + b"/" + entry.path) if prefix else entry.path
            obj = self._repo[entry.sha]
            if isinstance(obj, DTree):
                out.extend(self._walk_tree_paths(entry.sha, full))
            else:
                out.append(full.decode())
        return out

    @staticmethod
    def _summary_sha(sha: bytes | None) -> str:
        if sha is None:
            return "(none)"
        return sha[:8].decode() if isinstance(sha, bytes) else str(sha)[:8]


@dataclass
class _ChangeSet:
    """Three lists of repo-relative paths produced by _GitMirror.list_changes."""
    added: list[str] = field(default_factory=list)
    modified: list[str] = field(default_factory=list)
    removed: list[str] = field(default_factory=list)

    @property
    def changed(self) -> list[str]:
        """Convenience: paths that need to be re-read (added + modified)."""
        return self.added + self.modified

    @property
    def is_empty(self) -> bool:
        return not (self.added or self.modified or self.removed)

    @property
    def total(self) -> int:
        return len(self.added) + len(self.modified) + len(self.removed)


def _classify_git_path(rel_path: str) -> tuple[str, str | None, int | None] | None:
    """Translate a `database`-branch-relative path into the
    (media_type, imdb_id, tmdb_id) it represents. Returns None for
    paths we don't process (pages.json, all_page_*.json, README, etc.).

    v1.13.0 (Phase B): only the per-item JSON files under
    movies/{imdb,themoviedb}/ and tv_shows/{imdb,themoviedb}/ feed
    the upsert path. The pages.json + all_page_*.json files are
    Phase A's index-walk artifacts; we don't need them in the
    differential path.
    """
    parts = rel_path.split("/")
    if len(parts) != 3:
        return None
    top, kind, leaf = parts
    if not leaf.endswith(".json"):
        return None
    if top == "movies":
        media_type = "movie"
    elif top == "tv_shows":
        media_type = "tv"
    else:
        return None
    stem = leaf[:-5]  # strip .json
    if kind == "imdb":
        return (media_type, stem, None)
    if kind == "themoviedb":
        try:
            return (media_type, None, int(stem))
        except ValueError:
            return None
    return None


def _run_git_differential_upsert(
    db_path,
    git_mirror: "_GitMirror",
    stats: SyncStats,
    *,
    sync_ts: str,
    enqueue_downloads: bool,
    auto_place_override: bool | None,
    cancel_check,
) -> None:
    """Walk the git mirror's change set, read each changed item's
    JSON from the new HEAD's tree, and feed (media_type, tmdb_id,
    record, upstream_source) tuples into _flush_sync_batch — the
    same plumbing Phase A's full-index walk uses, but scoped to the
    delta. Updates stats.movies_seen / tv_seen / new_count /
    updated_count / errors as it goes.

    Removes are NOT processed yet — Phase A's "items in DB that
    didn't appear in this sync are NOT deleted" rule still holds;
    a true tdb-removed UI affordance is Phase C.

    Marks the run successful via git_mirror.commit_sync_ok() at the
    end so the next sync diffs from this HEAD. If the upsert raises
    we deliberately do NOT advance MOTIF_LAST_SYNC — the next sync
    will re-process the same delta, which is idempotent (themes
    upsert is keyed on (media_type, tmdb_id)).
    """
    changes = git_mirror.list_changes()
    op_progress.update_progress(
        db_path, "tdb_sync",
        stage="git_apply",
        stage_label=f"Applying {changes.total} changed paths",
        stage_current=0,
        stage_total=len(changes.changed),
        activity=(f"git diff: {len(changes.added)} added, "
                  f"{len(changes.modified)} modified, "
                  f"{len(changes.removed)} removed"),
    )
    if changes.is_empty:
        # Tree changed (e.g. a README edit) but no per-item paths
        # we care about. Skip upsert; commit so next run has the
        # latest baseline.
        git_mirror.commit_sync_ok()
        return

    seen: set[tuple[str, int]] = set()
    batch: list[tuple[str, int, dict, str]] = []
    completed = 0
    last_ui = time.monotonic()
    total = len(changes.changed)
    for rel_path in changes.changed:
        if cancel_check():
            from .worker import _JobCancelled
            raise _JobCancelled()
        completed += 1
        classification = _classify_git_path(rel_path)
        if classification is None:
            continue
        media_type, imdb_id, tmdb_id = classification
        record = git_mirror.read_json(rel_path)
        if record is None:
            stats.errors += 1
            continue
        # The record's `id` field is the TMDB id regardless of
        # whether we landed via the imdb/ or themoviedb/ subtree.
        try:
            real_tmdb = int(tmdb_id if tmdb_id is not None
                            else record.get("id"))
        except (TypeError, ValueError):
            stats.errors += 1
            continue
        # Dedupe: a record can show up via both imdb and themoviedb
        # paths in the same commit (LizardByte mirrors the same
        # JSON). Process once.
        key = (media_type, real_tmdb)
        if key in seen:
            continue
        seen.add(key)
        if media_type == "movie":
            stats.movies_seen += 1
        else:
            stats.tv_seen += 1
        upstream_source = "imdb" if imdb_id else "themoviedb"
        batch.append((media_type, real_tmdb, record, upstream_source))
        if len(batch) >= _SYNC_BATCH:
            _flush_sync_batch(
                db_path, batch, sync_ts=sync_ts,
                enqueue_downloads=enqueue_downloads,
                auto_place_override=auto_place_override,
                stats=stats,
            )
            batch.clear()
            time.sleep(0.05)  # yield writer lock briefly
        if (time.monotonic() - last_ui) > 0.3:
            last_ui = time.monotonic()
            op_progress.update_progress(
                db_path, "tdb_sync",
                stage_current=completed,
                stage_total=total,
                processed_total=stats.movies_seen + stats.tv_seen,
                error_count=stats.errors,
            )
    if batch:
        _flush_sync_batch(
            db_path, batch, sync_ts=sync_ts,
            enqueue_downloads=enqueue_downloads,
            auto_place_override=auto_place_override,
            stats=stats,
        )
        batch.clear()
    op_progress.update_progress(
        db_path, "tdb_sync",
        stage_current=total, stage_total=total,
        processed_total=stats.movies_seen + stats.tv_seen,
        error_count=stats.errors,
        activity=(f"Applied {len(seen)} unique items "
                  f"({stats.new_count} new, {stats.updated_count} updated)"),
    )
    git_mirror.commit_sync_ok()


# v1.13.1 (Phase C): drop detection. Catches items LizardByte
# stopped publishing — the per-item JSON disappears from the
# database branch. Sync stamps tdb_dropped_at; the UI surfaces a
# gray TDB◌ pill and ACK DROP / CONVERT TO MANUAL actions in the
# SOURCE menu so the user can decide what to do.
_DROP_SAFETY_PCT = 0.05
_DROP_SAFETY_FLOOR = 50  # never block when total drops are tiny


def _detect_and_stamp_drops_full_walk(
    db_path,
    *,
    sync_ts: str,
    media_types_seen: set[str],
) -> int:
    """Snapshot/remote-path drop detection. After a successful
    full-tree walk both transports update last_seen_sync_at on every
    upserted row; anything in themes whose last_seen_sync_at < this
    run's sync_ts AND upstream_source != 'plex_orphan' AND
    tdb_dropped_at IS NULL was implicitly removed upstream.

    Only stamps if the implied drop count stays within
    _DROP_SAFETY_PCT of the surviving catalog (per media_type).
    A LizardByte regression that mass-removes items would otherwise
    cascade: motif would mark everything as dropped, the user
    would either ignore (continuing to use stale data) or panic-
    purge (catastrophic). Bail loud, let the operator review.

    Returns the number of rows stamped across all media_types.
    """
    if not media_types_seen:
        return 0
    stamped_total = 0
    with get_conn(db_path) as conn:
        for mt in sorted(media_types_seen):
            current_n = conn.execute(
                "SELECT COUNT(*) FROM themes "
                "WHERE media_type = ? AND upstream_source != 'plex_orphan'",
                (mt,),
            ).fetchone()[0] or 0
            implied_n = conn.execute(
                """SELECT COUNT(*) FROM themes
                    WHERE media_type = ?
                      AND upstream_source != 'plex_orphan'
                      AND tdb_dropped_at IS NULL
                      AND last_seen_sync_at < ?""",
                (mt, sync_ts),
            ).fetchone()[0] or 0
            if implied_n == 0:
                continue
            cap = max(_DROP_SAFETY_FLOOR,
                      int(current_n * _DROP_SAFETY_PCT))
            if implied_n > cap:
                log.warning(
                    "drop detection: %s implied_drops=%d exceeds cap=%d "
                    "(catalog=%d) — skipping stamp; review upstream",
                    mt, implied_n, cap, current_n,
                )
                continue
            cur = conn.execute(
                """UPDATE themes SET tdb_dropped_at = ?
                    WHERE media_type = ?
                      AND upstream_source != 'plex_orphan'
                      AND tdb_dropped_at IS NULL
                      AND last_seen_sync_at < ?""",
                (sync_ts, mt, sync_ts),
            )
            stamped_total += cur.rowcount or 0
    return stamped_total


def _detect_and_stamp_drops_git(
    db_path,
    git_mirror: "_GitMirror",
    *,
    sync_ts: str,
) -> int:
    """Git-path drop detection. The change set's `removed` list is
    the authoritative signal — but a single (media_type, tmdb_id)
    can have BOTH an imdb/<id>.json AND a themoviedb/<id>.json path
    in the tree. Removal of one doesn't necessarily mean TDB
    stopped publishing the item — it might just be a re-key. Only
    stamp dropped if NO surviving path in the new tree references
    this (media_type, tmdb_id).

    Same 5%-of-catalog safety cap as the full-walk path. Returns
    the number of rows stamped.
    """
    changes = git_mirror.list_changes()
    if not changes.removed:
        return 0
    candidates: dict[tuple[str, int], dict] = {}
    for rel_path in changes.removed:
        cls = _classify_git_path(rel_path)
        if cls is None:
            continue
        media_type, imdb_id, tmdb_id = cls
        # When the removed path was an imdb-keyed file we don't yet
        # know the tmdb_id (the JSON is gone). Resolve from themes.
        if tmdb_id is None and imdb_id is not None:
            with get_conn(db_path) as conn:
                row = conn.execute(
                    "SELECT tmdb_id FROM themes "
                    "WHERE media_type = ? AND imdb_id = ?",
                    (media_type, imdb_id),
                ).fetchone()
            if row is None:
                continue
            tmdb_id = int(row["tmdb_id"])
        if tmdb_id is None:
            continue
        candidates.setdefault((media_type, tmdb_id), {})
    # For each candidate, verify NO surviving path in the new tree
    # still references it. If imdb/<id>.json went away but
    # themoviedb/<id>.json is still there, the item is NOT dropped.
    actually_dropped: list[tuple[str, int]] = []
    for (mt, tmdb_id) in candidates:
        # Look up the imdb_id we'd expect from themes for the
        # imdb-path probe.
        with get_conn(db_path) as conn:
            row = conn.execute(
                "SELECT imdb_id FROM themes "
                "WHERE media_type = ? AND tmdb_id = ?",
                (mt, tmdb_id),
            ).fetchone()
        imdb_id_from_db = row["imdb_id"] if row else None
        media_path = "movies" if mt == "movie" else "tv_shows"
        survives_tmdb = git_mirror.read_json(
            f"{media_path}/themoviedb/{tmdb_id}.json") is not None
        survives_imdb = (imdb_id_from_db is not None) and (
            git_mirror.read_json(
                f"{media_path}/imdb/{imdb_id_from_db}.json") is not None
        )
        if not (survives_tmdb or survives_imdb):
            actually_dropped.append((mt, tmdb_id))
    if not actually_dropped:
        return 0
    # Safety cap (per media_type).
    by_type: dict[str, list[int]] = {}
    for mt, tmdb_id in actually_dropped:
        by_type.setdefault(mt, []).append(tmdb_id)
    stamped_total = 0
    with get_conn(db_path) as conn:
        for mt, ids in by_type.items():
            current_n = conn.execute(
                "SELECT COUNT(*) FROM themes "
                "WHERE media_type = ? AND upstream_source != 'plex_orphan'",
                (mt,),
            ).fetchone()[0] or 0
            cap = max(_DROP_SAFETY_FLOOR,
                      int(current_n * _DROP_SAFETY_PCT))
            if len(ids) > cap:
                log.warning(
                    "drop detection (git): %s implied_drops=%d exceeds "
                    "cap=%d (catalog=%d) — skipping stamp",
                    mt, len(ids), cap, current_n,
                )
                continue
            for tmdb_id in ids:
                cur = conn.execute(
                    """UPDATE themes SET tdb_dropped_at = ?
                        WHERE media_type = ? AND tmdb_id = ?
                          AND upstream_source != 'plex_orphan'
                          AND tdb_dropped_at IS NULL""",
                    (sync_ts, mt, tmdb_id),
                )
                stamped_total += cur.rowcount or 0
    return stamped_total


def run_sync(db_path, base_url: str, *,
             auto_place_override: bool | None = None,
             enqueue_downloads: bool = True,
             cancel_check=lambda: False,
             source: str = "remote",
             database_url: str | None = None,
             git_url: str | None = None,
             git_branch: str | None = None) -> SyncStats:
    """Run a full ThemerrDB sync. Returns stats.

    `auto_place_override=None` lets the worker fall back to the global
    placement.auto_place setting when each download finishes. Setting it to
    True or False stamps that decision onto every download enqueued by this
    run (so a "download only" sync always lands in /pending regardless of
    the global default, and vice versa).

    `enqueue_downloads=False` (v1.7+ default of the dashboard SYNC button)
    runs sync metadata-only: themes rows are upserted but no download jobs
    are enqueued. The user triggers downloads explicitly from /movies, /tv,
    or /coverage. pending_updates rows are still recorded for items whose
    ThemerrDB URL changed since last sync.
    """
    stats = SyncStats()
    sync_ts = now_iso()

    with get_conn(db_path) as conn:
        # v1.13.2 (#1): stamp transport at start so the dashboard
        # sparkline can color this run by which path was requested,
        # even if the run later cascades to a slower tier (the
        # fallback target lands in fallback_reason at finish).
        run_id = conn.execute(
            "INSERT INTO sync_runs (started_at, status, transport) "
            "VALUES (?, 'running', ?)",
            (sync_ts, source),
        ).lastrowid

    log_event(db_path, level="INFO", component="sync", message=f"Sync run #{run_id} started")

    # v1.12.106: live progress surface for the ops side-drawer. One row
    # in op_progress, upserted at every natural checkpoint already
    # iterated by the existing log path. The drawer renders stage
    # timeline + smoothed throughput + ETA from these writes.
    op_progress.start_progress(
        db_path, op_id="tdb_sync", kind="tdb_sync",
        stage="index", stage_label="Connecting to ThemerrDB",
    )

    # Combined cancel: worker's existing cancel_check (job cancellation
    # via jobs.status='cancelled') OR the user clicking // CANCEL in
    # the ops drawer (op_progress.status='cancelling'). Either flips
    # the worker's cancellation path; we keep _JobCancelled as the
    # bail signal so partial-stats persistence (line 1056+) still
    # fires uniformly.
    def _cancel_check():
        return cancel_check() or op_progress.is_cancelled(db_path, "tdb_sync")

    snapshot: _DatabaseSnapshot | None = None
    git_mirror: _GitMirror | None = None
    try:
        with _make_client() as client:
            # v1.13.0 (Phase B): git mirror path. When sync.source =
            # "git", maintain a bare git mirror of the database
            # branch and fetch only the delta. First run shallow-
            # clones; subsequent runs fetch + diff. On _GitMirrorError
            # we cascade to the snapshot path; on snapshot failure
            # we cascade to remote. Three-tier fallback chain so a
            # single transport failure can't take down the daily sync.
            if source == "git":
                resolved_git_url = git_url or (
                    "https://github.com/LizardByte/ThemerrDB.git"
                )
                resolved_branch = git_branch or "database"
                try:
                    git_mirror = _GitMirror(
                        db_path, "tdb_sync",
                        resolved_git_url, resolved_branch,
                        _cancel_check,
                    )
                    git_mirror.acquire(client)
                    if git_mirror.is_unchanged():
                        log_event(
                            db_path, level="INFO", component="sync",
                            message=f"Sync run #{run_id}: git mirror "
                                    f"unchanged — skipping upsert",
                        )
                        op_progress.set_detail_field(
                            db_path, "tdb_sync", "no_changes", True,
                        )
                    else:
                        log_event(
                            db_path, level="INFO", component="sync",
                            message=f"Sync run #{run_id}: git mirror "
                                    f"acquired from {resolved_git_url}",
                        )
                except _GitMirrorError as e:
                    log_event(
                        db_path, level="WARNING", component="sync",
                        message=f"Sync run #{run_id}: git mirror "
                                f"failed ({e}); cascading to snapshot",
                    )
                    op_progress.set_detail_field(
                        db_path, "tdb_sync", "fallback_active", True,
                    )
                    op_progress.set_detail_field(
                        db_path, "tdb_sync", "fallback_reason",
                        f"git: {e}",
                    )
                    op_progress.update_progress(
                        db_path, "tdb_sync",
                        activity=f"Git mirror unavailable — using "
                                 f"snapshot fallback ({e})",
                    )
                    git_mirror = None
                    source = "database"  # cascade

            # v1.12.121 (Phase A): snapshot path. When sync.source =
            # "database", try to pull a single tarball of LizardByte's
            # database branch up-front. On any failure, log + flag
            # fallback_active and fall through to the remote per-item
            # HTTP path so the run still completes.
            if source == "database":
                tar_url = database_url or (
                    "https://codeload.github.com/LizardByte/ThemerrDB/"
                    "tar.gz/database"
                )
                try:
                    snapshot = _DatabaseSnapshot(
                        db_path, "tdb_sync", tar_url, _cancel_check,
                    )
                    snapshot.acquire(client)
                    if snapshot.is_unchanged():
                        log_event(
                            db_path, level="INFO", component="sync",
                            message=f"Sync run #{run_id}: snapshot "
                                    f"unchanged (304) — skipping upsert",
                        )
                        op_progress.set_detail_field(
                            db_path, "tdb_sync", "no_changes", True,
                        )
                    else:
                        log_event(
                            db_path, level="INFO", component="sync",
                            message=f"Sync run #{run_id}: snapshot acquired "
                                    f"from {tar_url}",
                        )
                except _SnapshotError as e:
                    log_event(
                        db_path, level="WARNING", component="sync",
                        message=f"Sync run #{run_id}: snapshot path "
                                f"failed ({e}); falling back to remote",
                    )
                    op_progress.set_detail_field(
                        db_path, "tdb_sync", "fallback_active", True,
                    )
                    op_progress.set_detail_field(
                        db_path, "tdb_sync", "fallback_reason", str(e),
                    )
                    op_progress.update_progress(
                        db_path, "tdb_sync",
                        activity=f"Snapshot unavailable — using remote "
                                 f"fallback ({e})",
                    )
                    snapshot = None

            # v1.13.0 (Phase B): git differential path. When the git
            # mirror reports new commits, diff the trees and walk
            # ONLY changed paths — typically tens of items vs. the
            # 5k+ full-index walk. Skips the index/fetch loop entirely
            # for both media types; resolve + prune sweeps still run.
            if (git_mirror is not None
                    and not git_mirror.is_unchanged()):
                _run_git_differential_upsert(
                    db_path, git_mirror, stats, sync_ts=sync_ts,
                    enqueue_downloads=enqueue_downloads,
                    auto_place_override=auto_place_override,
                    cancel_check=_cancel_check,
                )

            # v1.12.126 Phase A.5: skip the per-media-type upsert pipeline
            # entirely when codeload reported 304 (no upstream change).
            # Stats stay zero on this path; the resolve + prune sweeps
            # below still run since they depend on local DB state, not
            # TDB state. is_unchanged is only set for the snapshot path
            # OR the git path; remote-fallback runs always do the full
            # upsert.
            skip_upsert = (
                (snapshot is not None and snapshot.is_unchanged())
                or (git_mirror is not None)
            )
            media_iter = (() if skip_upsert
                          else (("movie", "movies"), ("tv", "tv_shows")))
            for media_type, media_path in media_iter:
                stage_key = f"index_{media_type}"
                op_progress.update_progress(
                    db_path, "tdb_sync",
                    stage=stage_key,
                    stage_label=f"Fetching {media_type} index",
                    stage_current=0, stage_total=0,
                    activity=f"Fetching {media_type} index from ThemerrDB",
                )
                if snapshot is not None:
                    index = snapshot.index(media_path)
                else:
                    index = _fetch_index(client, base_url, media_path)
                log.info("ThemerrDB %s: %d items in index", media_type, len(index))
                if media_type == "movie":
                    stats.movies_seen = len(index)
                else:
                    stats.tv_seen = len(index)

                # v1.11.49: parallelize per-item HTTP fetches. Pre-fix the
                # tight loop did a sequential `_fetch_item` per index entry
                # — at ~50ms RTT to GitHub Pages this added up to many
                # minutes for libraries with thousands of titles. The
                # bottleneck was network, not DB; httpx.Client is
                # thread-safe so a small ThreadPoolExecutor over the
                # index yields ~10-20× speedup without any rewrite of
                # the per-item logic.
                #
                # Cancellation: the executor's submission loop checks
                # cancel_check before each submit and bails fast; in-flight
                # workers continue (kill-switch via shutdown(cancel_futures)
                # leaves them queued, the index walk just stops feeding new
                # ones). Since each fetch is ~30s timeout max, the worker
                # pool drains within seconds of cancel.
                batch: list[tuple[str, int, dict, str]] = []
                from .worker import _JobCancelled

                def _do_fetch(entry):
                    tmdb_id = entry.get("id")
                    if tmdb_id is None:
                        return None
                    imdb_id = entry.get("imdb_id") or None
                    if snapshot is not None:
                        record = snapshot.fetch_item(
                            media_path,
                            imdb_id=imdb_id, tmdb_id=int(tmdb_id),
                        )
                    else:
                        record = _fetch_item(
                            client, base_url, media_path,
                            imdb_id=imdb_id, tmdb_id=int(tmdb_id),
                        )
                    if record is None:
                        return ("error", int(tmdb_id))
                    upstream_source = "imdb" if imdb_id else "themoviedb"
                    return (media_type, int(tmdb_id), record, upstream_source)

                fetch_stage = f"fetch_{media_type}"
                op_progress.update_progress(
                    db_path, "tdb_sync",
                    stage=fetch_stage,
                    stage_label=f"Fetching {media_type} themes",
                    stage_current=0, stage_total=len(index),
                    processed_est=stats.movies_seen + stats.tv_seen,
                )
                with ThreadPoolExecutor(
                    max_workers=_FETCH_WORKERS,
                    thread_name_prefix=f"motif-sync-{media_type}",
                ) as ex:
                    futures = []
                    for entry in index:
                        if _cancel_check():
                            ex.shutdown(cancel_futures=True)
                            raise _JobCancelled()
                        futures.append(ex.submit(_do_fetch, entry))
                    # v1.11.76: visible per-item progress so the user can
                    # see the sync is making forward progress (and how
                    # fast). Pre-fix the only sync logs were the
                    # opening 'index' line and the closing 'finished'
                    # line — a slow connection looked indistinguishable
                    # from a hang.
                    completed = 0
                    last_log = time.monotonic()
                    last_ui = time.monotonic()
                    total = len(futures)
                    media_processed_base = (
                        0 if media_type == "movie" else stats.movies_seen)
                    for fut in as_completed(futures):
                        if _cancel_check():
                            ex.shutdown(cancel_futures=True)
                            raise _JobCancelled()
                        result = fut.result()
                        completed += 1
                        # v1.12.124: split log cadence from progress UI
                        # cadence. Pre-fix both fired together at every
                        # 500 items + a 30s wallclock fallback — on a
                        # snapshot run that turns into a ~12% step per
                        # batch, with the bar visibly jumping rather
                        # than ticking. The log line stays at 500-item
                        # milestones (so log files stay terse), but
                        # op_progress now updates every ~300ms so the
                        # bar feels live regardless of items/sec.
                        if (completed == 1 or completed == total
                                or completed % 500 == 0
                                or (time.monotonic() - last_log) > 30.0):
                            log.info(
                                "sync %s progress: %d/%d "
                                "(errors=%d, queued_for_flush=%d)",
                                media_type, completed, total,
                                stats.errors, len(batch),
                            )
                            last_log = time.monotonic()
                        if (completed == 1 or completed == total
                                or (time.monotonic() - last_ui) > 0.3):
                            last_ui = time.monotonic()
                            op_progress.update_progress(
                                db_path, "tdb_sync",
                                stage_current=completed,
                                stage_total=total,
                                processed_total=media_processed_base + completed,
                                error_count=stats.errors,
                            )
                        if result is None:
                            continue
                        if result[0] == "error":
                            stats.errors += 1
                            continue
                        batch.append(result)
                        if len(batch) >= _SYNC_BATCH:
                            _flush_sync_batch(
                                db_path, batch, sync_ts=sync_ts,
                                enqueue_downloads=enqueue_downloads,
                                auto_place_override=auto_place_override,
                                stats=stats,
                            )
                            batch.clear()
                            # v1.11.54: yield the writer lock for ~250ms
                            # with jitter between batches. Pre-fix the 50ms
                            # gap left an 80% lock duty cycle — the
                            # general worker's BEGIN IMMEDIATE timed out
                            # at 30s and the v1.11.51 retry path had to
                            # absorb repeated 'database is locked' for
                            # the entire sync. With ~200ms per batch
                            # transaction + 250ms yield, duty cycle drops
                            # to ~45% and other writers (event flusher,
                            # job claim, log_event) reliably get a
                            # window. Total sync wall-clock cost on a
                            # ~5K-item run: ~25s extra; well worth not
                            # wedging the rest of the app.
                            time.sleep(0.25 + random.uniform(0, 0.05))

                if batch:
                    _flush_sync_batch(
                        db_path, batch, sync_ts=sync_ts,
                        enqueue_downloads=enqueue_downloads,
                        auto_place_override=auto_place_override,
                        stats=stats,
                    )
                    batch.clear()

        # v1.13.1 (Phase C): drop detection. Stamp tdb_dropped_at on
        # themes whose upstream JSON disappeared between syncs. The
        # snapshot/remote paths use the implicit last_seen_sync_at
        # signal (anything not touched this run is implicitly
        # removed); the git path uses the explicit ChangeSet.removed
        # list with a survivors check (one-of-imdb-or-themoviedb).
        # Both honor a 5%-of-catalog cap so a buggy upstream export
        # can't cascade-drop the whole library.
        #
        # v1.13.7: split the trigger so the git-active-changes path
        # actually runs detection. Pre-fix `if not skip_upsert:`
        # gated everything off skip_upsert which is True for ALL
        # git-mode runs (the differential upsert helper does its
        # work above and sets skip_upsert=True to skip the full-tree
        # walk below). Result: _detect_and_stamp_drops_git was dead
        # code; drops were silently never stamped on source=git.
        ran_git_diff = (git_mirror is not None
                        and not git_mirror.is_unchanged())
        ran_full_walk = not skip_upsert
        if ran_git_diff or ran_full_walk:
            try:
                if ran_git_diff:
                    n_dropped = _detect_and_stamp_drops_git(
                        db_path, git_mirror, sync_ts=sync_ts)
                else:
                    media_types_seen: set[str] = set()
                    if stats.movies_seen:
                        media_types_seen.add("movie")
                    if stats.tv_seen:
                        media_types_seen.add("tv")
                    n_dropped = _detect_and_stamp_drops_full_walk(
                        db_path, sync_ts=sync_ts,
                        media_types_seen=media_types_seen)
                if n_dropped:
                    log_event(
                        db_path, level="INFO", component="sync",
                        message=f"Sync run #{run_id}: stamped "
                                f"{n_dropped} item(s) as TDB-dropped",
                        detail={"dropped": n_dropped},
                    )
            except Exception as e:
                log.warning("drop detection failed: %s", e)

        op_progress.update_progress(
            db_path, "tdb_sync",
            stage="resolve",
            stage_label="Resolving theme links",
            stage_current=0, stage_total=0,
            activity="Refreshing plex_items.theme_id",
        )
        # v1.11.26: refresh plex_items.theme_id now that themes has
        # absorbed the new / updated rows from this sync. Keeps the
        # /api/library JOIN's PK lookup column in sync with the latest
        # themes data without waiting for the next plex_enum.
        try:
            from .plex_enum import resolve_theme_ids
            # v1.12.127: route resolve progress through tdb_sync's
            # op_progress row so the ops drawer renders a real %
            # bar during the resolve stage instead of the prior
            # stage_total=0 indeterminate shimmer.
            resolve_theme_ids(db_path, progress_op_id="tdb_sync")
        except Exception as e:
            log.warning("post-sync resolve_theme_ids failed: %s", e)

        # v1.12.60: sweep orphan user_overrides BEFORE the
        # pending_updates sweep below. user_overrides is what
        # makes a row classify as U at download time (worker
        # prefers user_overrides.youtube_url over
        # themes.youtube_url). When the row has no theme presence
        # — no local_files, no placements, no sidecar — and no
        # in-flight job, the override is dangling metadata that
        # silently re-classifies the row as U on the next download.
        # Pre-fix, a sequence like SET URL → PURGE on a pre-v1.12.57
        # build left an orphan override that survived everywhere
        # else and re-applied on the next DOWNLOAD TDB action,
        # confusing users who expected a clean T result. The .57
        # _drop_motif_tracking helper handles the forward path;
        # this sweep handles legacy and edge cases.
        #
        # Same in-flight job guard as the pending_updates sweep —
        # protects a fresh SET URL whose download hasn't started yet
        # (user_overrides exists, local_files doesn't, but there's
        # a queued download job).
        op_progress.update_progress(
            db_path, "tdb_sync",
            stage="prune",
            stage_label="Pruning stale state",
            stage_current=0, stage_total=0,
            activity="Sweeping orphan overrides",
        )
        with get_conn(db_path) as conn:
            stale_overrides = conn.execute(
                """
                DELETE FROM user_overrides
                WHERE NOT EXISTS (
                    SELECT 1 FROM local_files lf
                    WHERE lf.media_type = user_overrides.media_type
                      AND lf.tmdb_id = user_overrides.tmdb_id
                  )
                  AND NOT EXISTS (
                    SELECT 1 FROM placements p
                    WHERE p.media_type = user_overrides.media_type
                      AND p.tmdb_id = user_overrides.tmdb_id
                  )
                  AND NOT EXISTS (
                    SELECT 1 FROM plex_items pi
                    WHERE pi.guid_tmdb = user_overrides.tmdb_id
                      AND pi.media_type = (CASE user_overrides.media_type
                                            WHEN 'tv' THEN 'show'
                                            ELSE user_overrides.media_type END)
                      AND pi.local_theme_file = 1
                  )
                  AND NOT EXISTS (
                    SELECT 1 FROM jobs j
                    WHERE j.media_type = user_overrides.media_type
                      AND j.tmdb_id = user_overrides.tmdb_id
                      AND j.job_type IN ('download', 'place')
                      AND j.status IN ('pending', 'running')
                  )
                """
            ).rowcount or 0
        if stale_overrides:
            log_event(
                db_path, level="INFO", component="sync",
                message=f"Pruned {stale_overrides} orphan user_overrides "
                        f"row{'s' if stale_overrides != 1 else ''} "
                        "(no theme presence, no in-flight jobs)",
                detail={"pruned": stale_overrides},
            )

        # v1.12.49: sweep stale pending_updates rows. A pending_updates
        # row is only meaningful when the user actually has a theme to
        # "update against" (motif local file, manual URL override, or
        # Plex-folder sidecar). Rows can become stale when the user
        # drops/forgets a theme after the upstream URL changed: the
        # pending_updates row stays put, lighting the topbar UPD
        # counter and (pre-v1.12.48) the row's blue TDB ↑ pill, even
        # though there's nothing for ACCEPT UPDATE to swap from.
        # v1.12.48 papered over the row pill in JS; this sweep fixes
        # the root cause so the UPD count and the tdb_pills=update
        # filter both stop counting them too.
        #
        # Mirrors the cross-sync match-detection inverse at line 597:
        # rows are *created* when has_local_files OR has_override OR
        # has_sidecar; rows are *deleted* here when none of those hold.
        # v1.12.52: also keep rows that have a placements entry. The
        # creation criteria don't include placements (placement implies
        # local_files at write time) but a placement row can outlive
        # its local_files row — e.g., the canonical was deleted from
        # the themes dir but the hardlink in the Plex folder survives.
        # The user still has the theme playing in Plex, so the
        # pending_update is still actionable. Defensive: if the four
        # criteria all miss, the row is genuinely orphaned.
        # v1.12.58: dropped the decision IN ('pending','declined')
        # filter that was here to protect in-flight ACCEPT UPDATE
        # downloads. Replaced with a NOT EXISTS jobs check that
        # provides the same protection more precisely. Pre-fix,
        # 'accepted' decisions for rows that had since been PURGEd
        # (or where pre-v1.12.57 PURGE failed to clean up — the
        # whole reason v1.12.57 added _drop_motif_tracking) lingered
        # forever, setting the SQL accepted_update flag and
        # blocking REPLACE TDB / DOWNLOAD TDB in the SOURCE menu
        # for the row's afterlife. With the in-flight guard, any
        # decision is fair game once the row has nothing to update
        # against AND no live download/place jobs.
        with get_conn(db_path) as conn:
            pruned = conn.execute(
                """
                DELETE FROM pending_updates
                WHERE NOT EXISTS (
                    SELECT 1 FROM local_files lf
                    WHERE lf.media_type = pending_updates.media_type
                      AND lf.tmdb_id = pending_updates.tmdb_id
                  )
                  AND NOT EXISTS (
                    SELECT 1 FROM user_overrides uo
                    WHERE uo.media_type = pending_updates.media_type
                      AND uo.tmdb_id = pending_updates.tmdb_id
                  )
                  AND NOT EXISTS (
                    SELECT 1 FROM placements p
                    WHERE p.media_type = pending_updates.media_type
                      AND p.tmdb_id = pending_updates.tmdb_id
                  )
                  AND NOT EXISTS (
                    SELECT 1 FROM plex_items pi
                    WHERE pi.guid_tmdb = pending_updates.tmdb_id
                      AND pi.media_type = (CASE pending_updates.media_type
                                            WHEN 'tv' THEN 'show'
                                            ELSE pending_updates.media_type END)
                      AND pi.local_theme_file = 1
                  )
                  AND NOT EXISTS (
                    SELECT 1 FROM jobs j
                    WHERE j.media_type = pending_updates.media_type
                      AND j.tmdb_id = pending_updates.tmdb_id
                      AND j.job_type IN ('download', 'place')
                      AND j.status IN ('pending', 'running')
                  )
                """
            ).rowcount or 0
        if pruned:
            log_event(
                db_path, level="INFO", component="sync",
                message=f"Pruned {pruned} stale pending_updates row"
                        f"{'s' if pruned != 1 else ''} (no theme to update against)",
                detail={"pruned": pruned},
            )

        # v1.12.53: detect "U-row with override URL == TDB URL" —
        # the row is classified U but the URL is identical to TDB.
        # Without this synthetic pending_update, the user has no
        # blue-↑-pill prompt to reclassify U → T (cross-sync's
        # is_new and url_changed branches don't fire when both the
        # title and the URL are pre-existing). Writing a synthetic
        # pending_update lights up the blue pill so the user can
        # ACCEPT UPDATE — which (in api.py v1.12.53) flips local_files
        # to T eagerly via the url_match path. Idempotent: ON
        # CONFLICT keeps any existing decision the user already
        # made on this row.
        with get_conn(db_path) as conn:
            # v1.12.99: per-section sweep. Pre-fix the synthetic
            # urls_match row was title-global (one row per title)
            # which lit up the blue pill on every section's row even
            # when the override only existed for one. Now each
            # user_overrides row gets its own section-specific
            # synthetic pending_update so KEEP CURRENT / ACCEPT on
            # one section doesn't bleed to siblings.
            synthetic = conn.execute(
                """
                INSERT INTO pending_updates (
                    media_type, tmdb_id, section_id,
                    old_video_id, new_video_id,
                    old_youtube_url, new_youtube_url,
                    upstream_edited_at, detected_at, decision, kind
                )
                SELECT t.media_type, t.tmdb_id, uo.section_id,
                       t.youtube_video_id, t.youtube_video_id,
                       NULL, t.youtube_url,
                       t.youtube_edited_at, ?, 'pending', 'urls_match'
                  FROM themes t
                  JOIN user_overrides uo
                    ON uo.media_type = t.media_type
                   AND uo.tmdb_id = t.tmdb_id
                 WHERE t.youtube_url IS NOT NULL
                   AND t.youtube_url <> ''
                   AND TRIM(uo.youtube_url) = TRIM(t.youtube_url)
                   AND NOT EXISTS (
                     SELECT 1 FROM pending_updates pu
                      WHERE pu.media_type = t.media_type
                        AND pu.tmdb_id = t.tmdb_id
                        AND pu.section_id = uo.section_id
                   )
                """,
                (sync_ts,),
            ).rowcount or 0
        if synthetic:
            log_event(
                db_path, level="INFO", component="sync",
                message=f"Detected {synthetic} U-row"
                        f"{'s' if synthetic != 1 else ''} with override URL "
                        "matching TDB — surfaced as pending updates so the "
                        "user can ACCEPT UPDATE to convert U → T",
                detail={"synthetic": synthetic},
            )

        # v1.13.2 (#1): finalize telemetry. fallback_reason is sourced
        # from op_progress.detail (set by the cascade dispatch); a
        # clean run on the originally-requested transport leaves it
        # NULL. no_changes flips to 1 when either Phase A.5's 304 or
        # Phase B's no-new-commits short-circuit fired.
        finalize_fallback_reason = None
        finalize_no_changes = 0
        try:
            with get_conn(db_path) as conn:
                row = conn.execute(
                    "SELECT detail_json FROM op_progress WHERE op_id = 'tdb_sync'"
                ).fetchone()
            if row is not None and row["detail_json"]:
                _detail = json.loads(row["detail_json"])
                if _detail.get("fallback_active"):
                    finalize_fallback_reason = _detail.get(
                        "fallback_reason") or "unknown"
                if _detail.get("no_changes"):
                    finalize_no_changes = 1
        except Exception as e:
            log.debug("telemetry detail read failed: %s", e)

        with get_conn(db_path) as conn:
            conn.execute(
                """
                UPDATE sync_runs SET
                    finished_at = ?, status = 'success',
                    movies_seen = ?, tv_seen = ?, new_count = ?, updated_count = ?,
                    fallback_reason = ?, no_changes = ?
                WHERE id = ?
                """,
                (now_iso(), stats.movies_seen, stats.tv_seen,
                 stats.new_count, stats.updated_count,
                 finalize_fallback_reason, finalize_no_changes,
                 run_id),
            )

        log_event(
            db_path, level="INFO", component="sync",
            message=f"Sync run #{run_id} finished",
            detail={
                "movies_seen": stats.movies_seen, "tv_seen": stats.tv_seen,
                "new": stats.new_count, "updated": stats.updated_count,
                "errors": stats.errors,
            },
        )
        # v1.12.126 Phase A.5: distinct activity line on the no-change
        # path so the ops drawer's "Last Ops" card reads honestly.
        # v1.13.7: four distinct cases — snapshot 304 (skipped),
        # git unchanged (skipped), git active-changes (differential
        # upsert ran above), and full-tree walk (snapshot/remote).
        # The skip_upsert flag overlaps the first three cases (set
        # whenever git_mirror is not None) so we can't gate on it
        # alone; explicitly disambiguate by transport state.
        snapshot_skipped = (snapshot is not None
                            and snapshot.is_unchanged())
        git_unchanged = (git_mirror is not None
                         and git_mirror.is_unchanged())
        if snapshot_skipped:
            done_msg = "Done — no upstream changes (304)"
        elif git_unchanged:
            done_msg = "Done — no new commits"
        else:
            # Full-tree walk OR git differential. Both populate
            # stats.movies_seen / tv_seen / new_count / updated_count.
            done_msg = (
                f"Done — {stats.movies_seen + stats.tv_seen} items, "
                f"{stats.new_count} new, {stats.updated_count} updated"
            )
        op_progress.update_progress(
            db_path, "tdb_sync",
            activity=done_msg,
        )
        # v1.13.17: stash the counts in detail_json so the drawer's
        # done-headline can render the full breakdown ("Done — 5177
        # items · 0 new · 0 updated") instead of the generic
        # "Done — N items processed". Mirrors what the docker log
        # line already shows + what the sync_runs row records.
        op_progress.set_detail_field(
            db_path, "tdb_sync", "done_summary",
            {
                "movies_seen": stats.movies_seen,
                "tv_seen": stats.tv_seen,
                "new": stats.new_count,
                "updated": stats.updated_count,
                "errors": stats.errors,
            },
        )
        op_progress.finish_progress(db_path, "tdb_sync", status="done")
        return stats

    except Exception as e:
        # v1.11.77: persist partial stats on every termination path so
        # the dashboard 'Last Sync' panel + the TDB-pill gating see
        # how much of the upstream we actually captured before we
        # bailed. Pre-fix the exception handler only wrote the
        # status + error, leaving movies_seen / tv_seen / new_count
        # / updated_count at 0 — the user couldn't tell whether a
        # cancellation hit at item 1 or item 4000.
        #
        # _JobCancelled gets a distinct error string so the dashboard
        # can call out 'cancelled' separately from generic failures
        # without changing the sync_runs.status CHECK enum (which
        # would force a DB wipe in dev mode). Status stays 'failed'
        # for any non-clean exit; the error column distinguishes.
        from .worker import _JobCancelled
        err_text = ("cancelled by user"
                    if isinstance(e, _JobCancelled) else str(e))
        # v1.13.2 (#1): on the failure path read fallback_reason out of
        # op_progress.detail so a run that cascaded but THEN crashed
        # still records what the cascade was. Same shape as the
        # success path.
        fail_fallback_reason = None
        try:
            with get_conn(db_path) as conn:
                row = conn.execute(
                    "SELECT detail_json FROM op_progress WHERE op_id = 'tdb_sync'"
                ).fetchone()
            if row is not None and row["detail_json"]:
                _detail = json.loads(row["detail_json"])
                if _detail.get("fallback_active"):
                    fail_fallback_reason = _detail.get(
                        "fallback_reason") or "unknown"
        except Exception:
            pass
        with get_conn(db_path) as conn:
            conn.execute(
                """UPDATE sync_runs SET
                       finished_at = ?, status = 'failed', error = ?,
                       movies_seen = ?, tv_seen = ?,
                       new_count = ?, updated_count = ?,
                       fallback_reason = ?
                   WHERE id = ?""",
                (now_iso(), err_text,
                 stats.movies_seen, stats.tv_seen,
                 stats.new_count, stats.updated_count,
                 fail_fallback_reason, run_id),
            )
        log_event(
            db_path,
            level="WARNING" if isinstance(e, _JobCancelled) else "ERROR",
            component="sync",
            message=f"Sync run #{run_id} {'cancelled' if isinstance(e, _JobCancelled) else 'failed'}: {err_text}",
            detail={
                "movies_seen": stats.movies_seen, "tv_seen": stats.tv_seen,
                "new": stats.new_count, "updated": stats.updated_count,
                "errors": stats.errors,
            },
        )
        op_progress.finish_progress(
            db_path, "tdb_sync",
            status="cancelled" if isinstance(e, _JobCancelled) else "failed",
            error_message=err_text,
        )
        raise
    finally:
        if snapshot is not None:
            snapshot.release()
        if git_mirror is not None:
            git_mirror.release()
