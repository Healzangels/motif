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


def run_plex_enum(db_path: Path, plex_cfg: PlexConfig,
                   *, only_section_id: str | None = None) -> dict:
    """Enumerate Plex sections, upsert plex_items. Returns stats.

    `only_section_id`: scope the enumeration to one section (used by the
    per-section REFRESH button on /settings#plex). Default is every
    managed section.

    Failures on individual sections are logged and skipped so a single
    broken section doesn't kill the whole pass.
    """
    stats = {"sections": 0, "items_seen": 0, "inserted": 0, "updated": 0, "errors": 0}
    sections = list_sections(db_path)
    managed = [s for s in sections if s["included"]]
    if only_section_id:
        managed = [s for s in managed if s["section_id"] == only_section_id]
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

    # v1.10.8: detect Plex folder renames/moves and re-link the canonical
    # theme. plex_items.folder_path now reflects the current Plex-side
    # location; placements.media_folder reflects where motif previously
    # placed the hardlink. When they diverge, the OLD path is stale —
    # update the placement to the new path and enqueue a place job so
    # Plex finds the theme in its current folder.
    relinked = reconcile_placement_paths(db_path)
    stats["relinked"] = relinked

    log_event(db_path, level="INFO", component="plex_enum",
              message=f"Enumerated {stats['sections']} sections, "
                      f"{stats['items_seen']} items "
                      f"({stats['inserted']} new, {stats['updated']} updated"
                      f"{', ' + str(relinked) + ' relinked' if relinked else ''})")
    return stats


def reconcile_placement_paths(db_path: Path) -> int:
    """Find placements whose media_folder no longer matches the current
    plex_items.folder_path for that (media_type, tmdb_id), update the
    placement to the new path, and enqueue a forced place job so the
    canonical theme gets hardlinked into the new folder.

    Returns the number of placements relinked.

    Triggered at the end of every plex_enum run. The cost is one
    indexed JOIN against plex_items + an UPDATE per divergent row,
    so it scales with active placements (typically tens to low
    hundreds), not the full Plex catalog.

    v1.10.39: hardened against UNIQUE-constraint failures. The JOIN
    can produce multiple candidate new_folders per placement (Plex
    sometimes lists one movie under multiple ratingKeys, each with
    its own folder_path). Picking one new_folder via DISTINCT and
    detecting the case where the destination row already exists
    avoids the 'UNIQUE constraint failed' that pre-1.10.39 left
    plex_enum jobs in the failed state.
    """
    enqueued = 0
    with get_conn(db_path) as conn:
        # DISTINCT collapses cases where the same (mt, tmdb, old, new)
        # tuple appears multiple times via different ratingKeys.
        rows = conn.execute(
            """SELECT DISTINCT p.media_type, p.tmdb_id,
                      p.media_folder AS old_folder,
                      pi.folder_path AS new_folder
               FROM placements p
               INNER JOIN plex_items pi
                 ON pi.guid_tmdb = p.tmdb_id
                AND pi.media_type = (CASE p.media_type WHEN 'tv' THEN 'show' ELSE 'movie' END)
               WHERE pi.folder_path IS NOT NULL
                 AND pi.folder_path != ''
                 AND p.media_folder != pi.folder_path"""
        ).fetchall()
        # If a placement already covers one of the candidate new_folders
        # (multi-ratingKey case), don't move that placement — just drop
        # the stale row(s) pointing at folders Plex no longer reports.
        # Build a set of currently-Plex-reported folders per (mt, tmdb).
        plex_paths_by_item: dict[tuple, set[str]] = {}
        for pi in conn.execute(
            """SELECT pi.guid_tmdb AS tmdb_id, pi.media_type AS pi_mt,
                      pi.folder_path
               FROM plex_items pi
               WHERE pi.folder_path IS NOT NULL AND pi.folder_path != ''"""
        ).fetchall():
            mt = "tv" if pi["pi_mt"] == "show" else "movie"
            key = (mt, pi["tmdb_id"])
            plex_paths_by_item.setdefault(key, set()).add(pi["folder_path"])

        for r in rows:
            old_folder = r["old_folder"]
            new_folder = r["new_folder"]
            mt = r["media_type"]
            tmdb_id = r["tmdb_id"]
            current_plex_paths = plex_paths_by_item.get((mt, tmdb_id), set())

            # Skip if the placement's current folder is still in Plex's
            # reported set — Plex just exposes another ratingKey at a
            # different folder, but the existing placement is still
            # valid.
            if old_folder in current_plex_paths:
                continue

            try:
                # Cancel any in-flight place to avoid racing the new one.
                conn.execute(
                    """UPDATE jobs SET status = 'cancelled', finished_at = ?
                       WHERE job_type = 'place' AND media_type = ? AND tmdb_id = ?
                         AND status IN ('pending','running')""",
                    (now_iso(), mt, tmdb_id),
                )
                existing_at_new = conn.execute(
                    "SELECT 1 FROM placements "
                    "WHERE media_type = ? AND tmdb_id = ? AND media_folder = ?",
                    (mt, tmdb_id, new_folder),
                ).fetchone()
                if existing_at_new:
                    # Destination row already exists — UPDATE would
                    # violate the composite UNIQUE. Drop the stale
                    # old_folder row instead; the destination row
                    # already covers it.
                    conn.execute(
                        """DELETE FROM placements
                           WHERE media_type = ? AND tmdb_id = ?
                             AND media_folder = ?""",
                        (mt, tmdb_id, old_folder),
                    )
                else:
                    conn.execute(
                        """UPDATE placements SET media_folder = ?
                           WHERE media_type = ? AND tmdb_id = ? AND media_folder = ?""",
                        (new_folder, mt, tmdb_id, old_folder),
                    )
                conn.execute(
                    """INSERT INTO jobs (job_type, media_type, tmdb_id, payload,
                                         status, created_at, next_run_at)
                       VALUES ('place', ?, ?, '{"force":true,"reason":"folder_relocated"}',
                               'pending', ?, ?)""",
                    (mt, tmdb_id, now_iso(), now_iso()),
                )
                log_event(db_path, level="INFO", component="plex_enum",
                          media_type=mt, tmdb_id=tmdb_id,
                          message="Plex folder moved; relinking theme",
                          detail={"old_folder": old_folder,
                                  "new_folder": new_folder,
                                  "kind": "delete-stale" if existing_at_new
                                          else "rename-in-place"})
                enqueued += 1
            except Exception as e:
                # One bad row shouldn't kill the whole reconcile pass.
                # Log + continue; the next plex_enum will retry.
                log.warning("reconcile_placement_paths: skipping row "
                            "(mt=%s tmdb=%s %s -> %s): %s",
                            mt, tmdb_id, old_folder, new_folder, e)
    return enqueued


# Tunable: how many items per write transaction. Smaller = more lock
# release windows for concurrent API writes; larger = fewer transaction
# round-trips. 200 keeps a 10K-item section under ~50 batches with
# negligible perf cost vs the single-transaction baseline.
_UPSERT_BATCH = 200


def _upsert_items(db_path: Path, items: list[PlexLibraryItem]) -> tuple[int, int]:
    """Upsert one section's items into plex_items. Returns
    (inserted_count, updated_count).

    Two performance properties matter on large libraries (10K+ items):
      - Sidecar stat() (folder_path/theme.mp3 existence) happens BEFORE
        the DB transaction opens — filesystem I/O is sequential but
        doesn't hold a write lock.
      - The DB writes are batched in transactions of `_UPSERT_BATCH`
        rows. Each batch ends with COMMIT, releasing the BEGIN IMMEDIATE
        lock so concurrent API writers (log_event in particular) don't
        stall for the entire enum duration.
    Pre-1.9.6 this was a single 10K-row transaction that soft-locked
    the API for 10+ seconds per enum.
    """
    # Phase 1: stat sidecars outside the transaction. List of (item, sidecar)
    # pairs.
    enriched: list[tuple[PlexLibraryItem, int]] = []
    for it in items:
        sidecar = 0
        if it.folder_path:
            try:
                sidecar = 1 if (Path(it.folder_path) / "theme.mp3").is_file() else 0
            except OSError:
                sidecar = 0
        enriched.append((it, sidecar))

    # Phase 2: batched upserts.
    inserted = 0
    updated = 0
    now = now_iso()
    for batch_start in range(0, len(enriched), _UPSERT_BATCH):
        batch = enriched[batch_start:batch_start + _UPSERT_BATCH]
        with get_conn(db_path) as conn, transaction(conn):
            for it, sidecar in batch:
                # v1.10.32: precompute the normalized title so the library
                # JOIN's title-fallback can match against themes.title_norm
                # without re-running normalize_title at query time.
                try:
                    from .normalize import normalize_title
                    tn = normalize_title(it.title or "")
                except Exception:
                    tn = (it.title or "").lower()
                existing = conn.execute(
                    "SELECT 1 FROM plex_items WHERE rating_key = ?",
                    (it.rating_key,),
                ).fetchone()
                if existing:
                    conn.execute(
                        """UPDATE plex_items SET
                              section_id = ?, media_type = ?, title = ?, year = ?,
                              guid_imdb = ?, guid_tmdb = ?, guid_tvdb = ?,
                              folder_path = ?, has_theme = ?, local_theme_file = ?,
                              title_norm = ?, last_seen_at = ?
                           WHERE rating_key = ?""",
                        (it.section_id, it.media_type, it.title, it.year,
                         it.guid_imdb, it.guid_tmdb, it.guid_tvdb,
                         it.folder_path, 1 if it.has_theme else 0, sidecar,
                         tn, now, it.rating_key),
                    )
                    updated += 1
                else:
                    conn.execute(
                        """INSERT INTO plex_items
                           (rating_key, section_id, media_type, title, year,
                            guid_imdb, guid_tmdb, guid_tvdb, folder_path,
                            has_theme, local_theme_file, title_norm,
                            first_seen_at, last_seen_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (it.rating_key, it.section_id, it.media_type, it.title,
                         it.year, it.guid_imdb, it.guid_tmdb, it.guid_tvdb,
                         it.folder_path, 1 if it.has_theme else 0, sidecar,
                         tn, now, now),
                    )
                    inserted += 1
    # v1.11.26: stamp theme_id once per enum so /api/library's row
    # query becomes a direct PK lookup instead of running a heavy
    # correlated subquery on every page render.
    resolve_theme_ids(db_path)
    return inserted, updated


def resolve_theme_ids(db_path: Path) -> int:
    """Bulk-populate plex_items.theme_id for every row whose match
    against themes can be resolved by tmdb_id, imdb_id, or
    (title_norm + year). Called at the end of plex_enum and sync so
    the cached column stays fresh.

    Returns the number of plex_items rows whose theme_id changed.
    """
    sql = """
        UPDATE plex_items SET theme_id = (
            SELECT t.id FROM themes t
            WHERE t.media_type = (CASE plex_items.media_type
                                       WHEN 'show' THEN 'tv'
                                       ELSE plex_items.media_type END)
              AND (
                (t.tmdb_id = plex_items.guid_tmdb
                 AND plex_items.guid_tmdb IS NOT NULL
                 AND t.upstream_source != 'plex_orphan')
                OR (t.imdb_id = plex_items.guid_imdb
                    AND plex_items.guid_imdb IS NOT NULL
                    AND t.upstream_source = 'plex_orphan')
                OR (t.title_norm = plex_items.title_norm
                    AND t.year = plex_items.year
                    AND t.upstream_source != 'plex_orphan'
                    AND plex_items.title_norm IS NOT NULL
                    AND plex_items.year IS NOT NULL)
              )
            ORDER BY
                CASE WHEN t.upstream_source = 'plex_orphan' THEN 1 ELSE 0 END,
                t.id DESC
            LIMIT 1
        )
    """
    with get_conn(db_path) as conn, transaction(conn):
        cur = conn.execute(sql)
        changed = cur.rowcount
    log.info("resolve_theme_ids: scanned %d plex_items rows", changed)
    return changed
