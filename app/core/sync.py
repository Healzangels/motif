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
import re
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

import httpx

from .db import get_conn, transaction
from .downloader import FailureKind, probe_youtube_url
from .events import log_event, now_iso
from .normalize import titles_equal

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
            orphan_candidates = conn.execute(
                """SELECT id, tmdb_id, title FROM themes
                   WHERE upstream_source = 'plex_orphan'
                     AND media_type = ? AND year = ?""",
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
            # The row keeps its `id`; only tmdb_id changes. We must briefly
            # disable FK enforcement because we're updating the parent and
            # children sequentially — there's no atomic "update both" in
            # SQLite, and intermediate states would violate the FK.
            conn.execute("PRAGMA foreign_keys = OFF")
            try:
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
            finally:
                conn.execute("PRAGMA foreign_keys = ON")
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
) -> None:
    """Add a download job, deduping against any pending/running job for the same item.

    `auto_place=None` means "use the global setting"; True/False writes an explicit
    override into the job payload that the worker honors.
    """
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
    payload: dict = {"reason": reason}
    if auto_place is not None:
        payload["auto_place"] = bool(auto_place)
    conn.execute(
        """
        INSERT INTO jobs (job_type, media_type, tmdb_id, payload, status, created_at, next_run_at)
        VALUES ('download', ?, ?, ?, 'pending', ?, ?)
        """,
        (media_type, tmdb_id, json.dumps(payload), now_iso(), now_iso()),
    )


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


def _probe_phase(
    db_path,
    targets: list[tuple[str, int, str]],
    *,
    cookies_file: Path | None,
    cap: int,
    stats: SyncStats,
) -> None:
    """v1.10.43: probe each (mt, tmdb_id, url) tuple via yt-dlp's
    metadata-only path and stamp themes.failure_kind so the UI's
    TDB pill can render red/amber for dead/cookied URLs without
    waiting for an actual download attempt.

    Bounded by `cap` to keep a fresh-install sync from blocking
    on tens of thousands of probes. Targets beyond the cap are
    skipped and pick up on the next sync's new+url_changed set
    over time.
    """
    if not targets:
        return
    probed = 0
    flagged = 0
    cleared = 0
    for media_type, tmdb_id, yt_url in targets:
        if probed >= cap:
            break
        probed += 1
        try:
            failure = probe_youtube_url(yt_url, cookies_file=cookies_file)
        except Exception as e:
            log.debug("probe failed for %s: %s", yt_url, e)
            continue
        with get_conn(db_path) as conn, transaction(conn):
            if failure is None:
                # Probe succeeded — clear any prior failure flag so the
                # row's TDB pill goes back to green if the URL was
                # previously broken and ThemerrDB has now updated to
                # a working one.
                cur = conn.execute(
                    "UPDATE themes SET failure_kind = NULL, "
                    "                  failure_message = NULL, "
                    "                  failure_at = NULL "
                    "WHERE media_type = ? AND tmdb_id = ? "
                    "  AND failure_kind IS NOT NULL",
                    (media_type, tmdb_id),
                )
                cleared += cur.rowcount
            else:
                conn.execute(
                    "UPDATE themes SET failure_kind = ?, "
                    "                  failure_message = ?, "
                    "                  failure_at = ? "
                    "WHERE media_type = ? AND tmdb_id = ?",
                    (failure.value, f"sync probe: {failure.human}",
                     now_iso(), media_type, tmdb_id),
                )
                flagged += 1
    log_event(
        db_path, level="INFO", component="sync",
        message=f"Sync probe: probed {probed} URLs "
                f"({flagged} flagged, {cleared} cleared, "
                f"{max(0, len(targets) - probed)} skipped)",
        detail={"probed": probed, "flagged": flagged,
                "cleared": cleared, "cap": cap,
                "candidates": len(targets)},
    )


def _flush_sync_batch(
    db_path,
    batch: list[tuple[str, int, dict, str]],
    *,
    sync_ts: str,
    enqueue_downloads: bool,
    auto_place_override: bool | None,
    stats: SyncStats,
    probe_targets: list | None = None,
) -> None:
    """Process one batch of fetched ThemerrDB records inside a single txn.

    `batch` is a list of (media_type, tmdb_id, record, upstream_source).
    `probe_targets` (v1.10.43): optional list to which we append
    (media_type, tmdb_id, youtube_url) tuples for items that are
    new or whose YouTube URL changed in this sync. The caller runs
    the actual probe pass after all batches commit so the writer
    lock isn't held during the per-URL HTTP work.
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
            # v1.10.43: collect new + url-changed entries for the
            # post-sync availability probe.
            if (is_new or url_changed) and probe_targets is not None:
                yt_url = record.get("youtube_theme_url")
                if yt_url:
                    probe_targets.append((media_type, tmdb_id, yt_url))
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
                if enqueue_downloads and not plex_supplies:
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
             probe_urls: bool = True,
             probe_cap: int = 100,
             cookies_file: Path | None = None) -> SyncStats:
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

    v1.10.43 — `probe_urls=True` runs a yt-dlp metadata-only probe on
    new + url_changed YouTube URLs after the sync writes commit, so
    dead / cookied / restricted URLs surface as red / amber TDB pills
    before the user tries to download. `probe_cap` bounds how many
    URLs we probe per sync (default 100) — keeps a fresh-install sync
    from spending hours on probes; later syncs catch up incrementally.
    """
    stats = SyncStats()
    sync_ts = now_iso()
    probe_targets: list[tuple[str, int, str]] = []

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

                batch: list[tuple[str, int, dict, str]] = []
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
                    batch.append((media_type, int(tmdb_id), record, upstream_source))
                    if len(batch) >= _SYNC_BATCH:
                        _flush_sync_batch(
                            db_path, batch, sync_ts=sync_ts,
                            enqueue_downloads=enqueue_downloads,
                            auto_place_override=auto_place_override,
                            stats=stats,
                            probe_targets=probe_targets if probe_urls else None,
                        )
                        batch.clear()
                        # Yield the writer lock briefly so concurrent API
                        # writers (log_event etc.) can land between batches.
                        time.sleep(0.05)

                if batch:
                    _flush_sync_batch(
                        db_path, batch, sync_ts=sync_ts,
                        enqueue_downloads=enqueue_downloads,
                        auto_place_override=auto_place_override,
                        stats=stats,
                        probe_targets=probe_targets if probe_urls else None,
                    )
                    batch.clear()

        # v1.10.43: post-sync availability probe. Runs OUTSIDE the
        # writer lock so the per-URL HTTP work doesn't stall API
        # writers. Each probe persists its own failure_kind via a
        # short transaction.
        if probe_urls and probe_targets:
            _probe_phase(db_path, probe_targets, cookies_file=cookies_file,
                         cap=probe_cap, stats=stats)

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
