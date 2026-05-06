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
from dataclasses import dataclass
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
                    title_norm = ?
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
                last_seen_sync_at = ?, title_norm = ?
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


def run_sync(db_path, base_url: str, *,
             auto_place_override: bool | None = None,
             enqueue_downloads: bool = True,
             cancel_check=lambda: False,
             source: str = "remote",
             database_url: str | None = None) -> SyncStats:
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
        run_id = conn.execute(
            "INSERT INTO sync_runs (started_at, status) VALUES (?, 'running')",
            (sync_ts,),
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
    try:
        with _make_client() as client:
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

            # v1.12.126 Phase A.5: skip the per-media-type upsert pipeline
            # entirely when codeload reported 304 (no upstream change).
            # Stats stay zero on this path; the resolve + prune sweeps
            # below still run since they depend on local DB state, not
            # TDB state. is_unchanged is only set for the snapshot path;
            # remote-fallback runs always do the full upsert.
            skip_upsert = (snapshot is not None
                           and snapshot.is_unchanged())
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
            resolve_theme_ids(db_path)
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

        with get_conn(db_path) as conn:
            conn.execute(
                """
                UPDATE sync_runs SET
                    finished_at = ?, status = 'success',
                    movies_seen = ?, tv_seen = ?, new_count = ?, updated_count = ?
                WHERE id = ?
                """,
                (now_iso(), stats.movies_seen, stats.tv_seen,
                 stats.new_count, stats.updated_count, run_id),
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
        # path so the ops drawer's "Last Ops" card reads honestly
        # ("Done — no upstream changes" rather than "Done — 0 items").
        if skip_upsert:
            op_progress.update_progress(
                db_path, "tdb_sync",
                activity="Done — no upstream changes (304)",
            )
        else:
            op_progress.update_progress(
                db_path, "tdb_sync",
                activity=(
                    f"Done — {stats.movies_seen + stats.tv_seen} items, "
                    f"{stats.new_count} new, {stats.updated_count} updated"
                ),
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
        with get_conn(db_path) as conn:
            conn.execute(
                """UPDATE sync_runs SET
                       finished_at = ?, status = 'failed', error = ?,
                       movies_seen = ?, tv_seen = ?,
                       new_count = ?, updated_count = ?
                   WHERE id = ?""",
                (now_iso(), err_text,
                 stats.movies_seen, stats.tv_seen,
                 stats.new_count, stats.updated_count, run_id),
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
