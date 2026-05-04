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


def run_sync(db_path, base_url: str, *,
             auto_place_override: bool | None = None,
             enqueue_downloads: bool = True,
             cancel_check=lambda: False) -> SyncStats:
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

    try:
        with _make_client() as client:
            for media_type, media_path in (("movie", "movies"), ("tv", "tv_shows")):
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
                    record = _fetch_item(
                        client, base_url, media_path,
                        imdb_id=imdb_id, tmdb_id=int(tmdb_id),
                    )
                    if record is None:
                        return ("error", int(tmdb_id))
                    upstream_source = "imdb" if imdb_id else "themoviedb"
                    return (media_type, int(tmdb_id), record, upstream_source)

                with ThreadPoolExecutor(
                    max_workers=_FETCH_WORKERS,
                    thread_name_prefix=f"motif-sync-{media_type}",
                ) as ex:
                    futures = []
                    for entry in index:
                        if cancel_check():
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
                    last_progress = time.monotonic()
                    total = len(futures)
                    for fut in as_completed(futures):
                        if cancel_check():
                            ex.shutdown(cancel_futures=True)
                            raise _JobCancelled()
                        result = fut.result()
                        completed += 1
                        if completed == 1 or completed == total \
                                or completed % 500 == 0 \
                                or (time.monotonic() - last_progress) > 30.0:
                            log.info(
                                "sync %s progress: %d/%d "
                                "(errors=%d, queued_for_flush=%d)",
                                media_type, completed, total,
                                stats.errors, len(batch),
                            )
                            last_progress = time.monotonic()
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
        raise
