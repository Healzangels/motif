"""
Plex library enumeration: walk every managed section and upsert plex_items.

Runs as a worker job (job_type='plex_enum'). Triggered automatically after
each ThemerrDB sync, after a /libraries refresh, and on demand from the UI.

Per-row strategy:
- INSERT new rows with first_seen_at = last_seen_at = now
- UPDATE existing rows (matched by rating_key): refresh title/year/guids/
  folder_path/has_theme + bump last_seen_at
- Rows in plex_items whose section_id has been disabled stay in the table
  (history) but aren't included in the unified browse view (the browse
  query inner-joins managed plex_sections).
"""
from __future__ import annotations

import logging
from pathlib import Path

from .db import get_conn, transaction
from .events import log_event, now_iso
from .plex import PlexClient, PlexConfig, PlexLibraryItem
from .sections import list_sections

log = logging.getLogger(__name__)


def run_plex_enum(db_path: Path, plex_cfg: PlexConfig) -> dict:
    """Enumerate every managed Plex section, upsert plex_items. Returns
    a stats dict. Failures on individual sections are logged and skipped
    so a single broken section doesn't kill the whole pass."""
    stats = {"sections": 0, "items_seen": 0, "inserted": 0, "updated": 0, "errors": 0}
    sections = list_sections(db_path)
    managed = [s for s in sections if s["included"]]
    if not managed:
        log.info("plex_enum: no managed sections, nothing to do")
        return stats

    with PlexClient(plex_cfg) as client:
        for s in managed:
            section_id = s["section_id"]
            section_type = s["type"]
            stats["sections"] += 1
            try:
                items = client.enumerate_section_items(
                    section_id=section_id, media_type=section_type,
                )
            except Exception as e:
                log.warning("plex_enum: section %s failed: %s", s["title"], e)
                stats["errors"] += 1
                continue
            stats["items_seen"] += len(items)
            ins, upd = _upsert_items(db_path, items)
            stats["inserted"] += ins
            stats["updated"] += upd
            log.info("plex_enum: section %s — %d items (%d new, %d updated)",
                     s["title"], len(items), ins, upd)

    log_event(db_path, level="INFO", component="plex_enum",
              message=f"Enumerated {stats['sections']} sections, "
                      f"{stats['items_seen']} items "
                      f"({stats['inserted']} new, {stats['updated']} updated)")
    return stats


def _upsert_items(db_path: Path, items: list[PlexLibraryItem]) -> tuple[int, int]:
    """Upsert one batch of items. Returns (inserted_count, updated_count)."""
    inserted = 0
    updated = 0
    now = now_iso()
    with get_conn(db_path) as conn, transaction(conn):
        for it in items:
            existing = conn.execute(
                "SELECT 1 FROM plex_items WHERE rating_key = ?",
                (it.rating_key,),
            ).fetchone()
            if existing:
                conn.execute(
                    """UPDATE plex_items SET
                          section_id = ?, media_type = ?, title = ?, year = ?,
                          guid_imdb = ?, guid_tmdb = ?, guid_tvdb = ?,
                          folder_path = ?, has_theme = ?, last_seen_at = ?
                       WHERE rating_key = ?""",
                    (it.section_id, it.media_type, it.title, it.year,
                     it.guid_imdb, it.guid_tmdb, it.guid_tvdb,
                     it.folder_path, 1 if it.has_theme else 0, now,
                     it.rating_key),
                )
                updated += 1
            else:
                conn.execute(
                    """INSERT INTO plex_items
                       (rating_key, section_id, media_type, title, year,
                        guid_imdb, guid_tmdb, guid_tvdb, folder_path,
                        has_theme, first_seen_at, last_seen_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (it.rating_key, it.section_id, it.media_type, it.title,
                     it.year, it.guid_imdb, it.guid_tmdb, it.guid_tvdb,
                     it.folder_path, 1 if it.has_theme else 0, now, now),
                )
                inserted += 1
    return inserted, updated
