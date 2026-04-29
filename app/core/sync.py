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

import logging
import re
import sqlite3
import time
from dataclasses import dataclass
from typing import Iterator

import httpx

from .db import get_conn, transaction
from .events import log_event, now_iso

log = logging.getLogger(__name__)


# ----- HTTP client config -----

_HTTP_TIMEOUT = httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0)
_HTTP_LIMITS = httpx.Limits(max_connections=8, max_keepalive_connections=4)


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
    title = record.get("title") or ""
    original_title = record.get("original_title") or None
    rd = record.get("release_date") or ""
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

    if is_new:
        conn.execute(
            """
            INSERT INTO themes (
                media_type, tmdb_id, imdb_id, title, original_title, year, release_date,
                youtube_url, youtube_video_id, youtube_added_at, youtube_edited_at,
                upstream_source, raw_json, last_seen_sync_at, first_seen_sync_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                media_type, tmdb_id, imdb_id, title, original_title, year, rd or None,
                yt_url, yt_vid, yt_added, yt_edited,
                upstream_source, _safe_json(record), sync_ts, sync_ts,
            ),
        )
    else:
        old_vid = existing["youtube_video_id"]
        url_changed = (yt_vid != old_vid) and yt_vid is not None
        conn.execute(
            """
            UPDATE themes SET
                imdb_id = ?, title = ?, original_title = ?, year = ?, release_date = ?,
                youtube_url = ?, youtube_video_id = ?, youtube_added_at = ?,
                youtube_edited_at = ?, upstream_source = ?, raw_json = ?,
                last_seen_sync_at = ?
            WHERE media_type = ? AND tmdb_id = ?
            """,
            (
                imdb_id, title, original_title, year, rd or None,
                yt_url, yt_vid, yt_added, yt_edited,
                upstream_source, _safe_json(record), sync_ts,
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
) -> None:
    """Add a download job, deduping against any pending/running job for the same item."""
    existing = conn.execute(
        """
        SELECT id FROM jobs
        WHERE job_type = 'download' AND media_type = ? AND tmdb_id = ?
              AND status IN ('pending', 'running')
        """,
        (media_type, tmdb_id),
    ).fetchone()
    if existing:
        return
    conn.execute(
        """
        INSERT INTO jobs (job_type, media_type, tmdb_id, payload, status, created_at, next_run_at)
        VALUES ('download', ?, ?, ?, 'pending', ?, ?)
        """,
        (media_type, tmdb_id, f'{{"reason": "{reason}"}}', now_iso(), now_iso()),
    )


# ----- Public entry point -----


def run_sync(db_path, base_url: str) -> SyncStats:
    """Run a full ThemerrDB sync. Returns stats."""
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

                for entry in index:
                    tmdb_id = entry.get("id")
                    if tmdb_id is None:
                        continue
                    imdb_id = entry.get("imdb_id") or None
                    record = _fetch_item(
                        client, base_url, media_path,
                        imdb_id=imdb_id, tmdb_id=int(tmdb_id),
                    )
                    if record is None:
                        stats.errors += 1
                        continue

                    upstream_source = "imdb" if imdb_id else "themoviedb"

                    with get_conn(db_path) as conn, transaction(conn):
                        is_new, url_changed, old_video_id = _upsert_theme(
                            conn,
                            media_type=media_type,
                            tmdb_id=int(tmdb_id),
                            record=record,
                            upstream_source=upstream_source,
                            sync_ts=sync_ts,
                        )
                        if is_new:
                            stats.new_count += 1
                            _enqueue_download(
                                conn, media_type=media_type, tmdb_id=int(tmdb_id),
                                reason="new",
                            )
                        elif url_changed:
                            stats.updated_count += 1
                            # If we already have a downloaded local file for this
                            # item, write a pending_update row instead of auto-
                            # downloading. The user gets to decide whether to
                            # accept the upstream change.
                            already_have = conn.execute(
                                "SELECT 1 FROM local_files WHERE media_type = ? AND tmdb_id = ?",
                                (media_type, int(tmdb_id)),
                            ).fetchone() is not None

                            if already_have:
                                yt_vid = extract_video_id(record.get("youtube_theme_url"))
                                conn.execute(
                                    """
                                    INSERT INTO pending_updates (
                                        media_type, tmdb_id, old_video_id, new_video_id,
                                        old_youtube_url, new_youtube_url,
                                        upstream_edited_at, detected_at, decision
                                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending')
                                    ON CONFLICT(media_type, tmdb_id) DO UPDATE SET
                                        new_video_id = excluded.new_video_id,
                                        new_youtube_url = excluded.new_youtube_url,
                                        upstream_edited_at = excluded.upstream_edited_at,
                                        detected_at = excluded.detected_at,
                                        decision = CASE
                                            WHEN pending_updates.decision = 'declined'
                                            THEN 'declined'   -- preserve declined state
                                            ELSE 'pending'
                                        END
                                    """,
                                    (
                                        media_type, int(tmdb_id),
                                        old_video_id, yt_vid,
                                        None, record.get("youtube_theme_url"),
                                        record.get("youtube_theme_edited"),
                                        sync_ts,
                                    ),
                                )
                            else:
                                # No local file yet — just enqueue the download
                                _enqueue_download(
                                    conn, media_type=media_type, tmdb_id=int(tmdb_id),
                                    reason="url_changed",
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
        with get_conn(db_path) as conn:
            conn.execute(
                "UPDATE sync_runs SET finished_at = ?, status = 'failed', error = ? WHERE id = ?",
                (now_iso(), str(e), run_id),
            )
        log_event(
            db_path, level="ERROR", component="sync",
            message=f"Sync run #{run_id} failed: {e}",
        )
        raise
