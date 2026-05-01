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
                   *, only_section_id: str | None = None,
                   cancel_check=lambda: False) -> dict:
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
            # v1.11.36: cooperative cancellation between sections.
            if cancel_check():
                from .worker import _JobCancelled
                raise _JobCancelled()
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
            ins, upd = _upsert_items(db_path, items, cancel_check=cancel_check)
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


# v1.11.61: host→container path-prefix translations to try when Plex
# returns a folder_path that doesn't exist inside motif's container.
# Plex on Unraid (and similar setups) reports the host's mount path,
# e.g. /mnt/user/data/media/anime/Show, while motif's container has
# /data/ bind-mounted to /mnt/user/data on the host. The same file
# is reachable under both paths from different vantage points; we
# try the original first, then each translated form, and use the
# first one that exists.
#
# Order matters — most-specific prefix first so /mnt/user/data/foo
# becomes /data/foo (preferred) rather than /user/data/foo.
_PATH_PREFIX_TRANSLATIONS: tuple[tuple[str, str], ...] = (
    ("/mnt/user/data/", "/data/"),
    ("/mnt/user/", "/"),
    ("/mnt/cache/data/", "/data/"),
    ("/mnt/cache/", "/"),
    ("/mnt/disks/", "/"),
)


# v1.11.62: extension set + helpers shared with the unmanage / forget
# endpoints in api.py so a Plex-folder-perspective sidecar re-stat
# uses the same translation table as plex_enum.
SIDECAR_AUDIO_EXTS = {
    ".mp3", ".m4a", ".m4b", ".flac", ".ogg", ".oga", ".opus",
    ".wav", ".wma", ".aac", ".aif", ".aiff", ".alac",
}


def _candidate_local_paths(folder_path: str):
    """Yield Path candidates for a folder_path returned by Plex,
    starting with the literal value and falling through common
    host→container prefix translations. Caller iterates and uses
    the first one whose .is_dir() returns True."""
    if not folder_path:
        return
    yield Path(folder_path)
    for src, dst in _PATH_PREFIX_TRANSLATIONS:
        if folder_path.startswith(src):
            yield Path(dst + folder_path[len(src):])


def folder_has_theme_sidecar(folder_path: str) -> bool:
    """Public helper: True if any theme.<audio-ext> exists at
    folder_path (or any of its host→container translations).
    Mirrors the inline check used by _upsert_items' Phase 1.

    Convenience wrapper around stat_theme_sidecar() that maps the
    indeterminate (None) result to False — usable when the caller
    doesn't have a previous-known-good value to preserve.
    """
    out = stat_theme_sidecar(folder_path)
    return bool(out)


def stat_theme_sidecar(folder_path: str) -> bool | None:
    """v1.11.67: returns True if a theme.<audio-ext> exists, False
    if we successfully scanned the folder and confirmed no such
    file, or None if EVERY candidate path raised OSError on either
    is_dir() or iterdir() — i.e. we couldn't determine the answer.

    Pre-fix, transient Unraid user-share / NFS hiccups produced a
    silent False return that plex_enum committed to pi.local_theme_file=0,
    which combined with Plex's (correctly cached) has_theme=1 made
    the row render as P (Plex agent) when the file was really there.
    Returning None lets callers preserve the previous-known value
    instead of overwriting truth with a temporary fault.
    """
    if not folder_path:
        return False
    any_reachable = False
    for candidate in _candidate_local_paths(folder_path):
        try:
            if not candidate.is_dir():
                continue
            any_reachable = True
            for entry in candidate.iterdir():
                try:
                    if not entry.is_file():
                        continue
                except OSError:
                    continue
                name = entry.name.lower()
                if not name.startswith("theme."):
                    continue
                if name[len("theme"):] in SIDECAR_AUDIO_EXTS:
                    return True
            return False
        except OSError as e:
            log.debug("stat_theme_sidecar: iterdir(%s) failed: %s",
                      candidate, e)
            continue
    if not any_reachable:
        log.warning("stat_theme_sidecar: no candidate path reachable for "
                    "%r — returning indeterminate (None)", folder_path)
        return None
    # We reached at least one candidate but it had no theme file
    return False


def _upsert_items(db_path: Path, items: list[PlexLibraryItem],
                   *, cancel_check=lambda: False) -> tuple[int, int]:
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
    # pairs. v1.11.62: parallelized + the per-folder check now goes
    # through folder_has_theme_sidecar (host→container path translation
    # + broad extension match). Same helper is reused by api.py's
    # unmanage/forget endpoints so the post-mutation re-stat sees the
    # same files plex_enum does.
    # v1.11.63: per-future timeout + progress logging. Pre-fix one
    # hung folder (dead NFS mount, stalled SMB read, anything where
    # iterdir() never returns) blocked the whole ex.map indefinitely
    # — the user's plex_enum job sat 'running' forever with no further
    # log output. Now: 30s timeout per folder via as_completed; on
    # timeout we mark sidecar=0 (rather than blocking) and log the
    # offending folder so the user can investigate. Periodic progress
    # log every 200 items so a long enum visibly advances.
    import time as _t
    enriched: list[tuple[PlexLibraryItem, int]] = []
    if items:
        from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as _FutTimeout
        # v1.11.67: pre-fetch existing pi.local_theme_file for every
        # rating_key we're about to enum, keyed by rk. When a per-item
        # stat comes back indeterminate (transient Unraid share hiccup,
        # NFS timeout, etc.) we preserve the previous value rather
        # than stomping a known-good 1 with a transient 0. Pre-fix
        # one bad iterdir() drove pi.local_theme_file=0 even though
        # Plex (correctly) reported has_theme=1, leaving the row
        # rendering as P (Plex agent) when the user's local theme.mp3
        # was clearly there.
        existing_local: dict[str, int] = {}
        if items:
            with get_conn(db_path) as conn:
                rks = [it.rating_key for it in items]
                # SQLite IN-clause limit ~999 — chunk just in case.
                for i in range(0, len(rks), 500):
                    chunk = rks[i:i + 500]
                    qmarks = ",".join("?" for _ in chunk)
                    for r in conn.execute(
                        f"SELECT rating_key, local_theme_file "
                        f"FROM plex_items WHERE rating_key IN ({qmarks})",
                        chunk,
                    ).fetchall():
                        existing_local[r["rating_key"]] = int(r["local_theme_file"] or 0)

        # sidecar_results values: 1 / 0 / None (indeterminate — use existing)
        sidecar_results: dict[int, int | None] = {}
        log.info("plex_enum: Phase 1 (sidecar stat) on %d items", len(items))
        phase1_start = _t.monotonic()
        with ThreadPoolExecutor(max_workers=16,
                                thread_name_prefix="motif-sidecar") as ex:
            futures = {
                ex.submit(stat_theme_sidecar, it.folder_path): idx
                for idx, it in enumerate(items)
            }
            done = 0
            for fut in as_completed(futures, timeout=None):
                idx = futures[fut]
                try:
                    res = fut.result(timeout=30)
                    if res is None:
                        sidecar_results[idx] = None  # indeterminate
                    else:
                        sidecar_results[idx] = 1 if res else 0
                except _FutTimeout:
                    log.warning(
                        "plex_enum: sidecar stat timeout for %r — "
                        "preserving existing flag (likely stalled mount)",
                        items[idx].folder_path,
                    )
                    sidecar_results[idx] = None
                except Exception as e:
                    log.warning(
                        "plex_enum: sidecar stat error for %r: %s — "
                        "preserving existing flag",
                        items[idx].folder_path, e,
                    )
                    sidecar_results[idx] = None
                done += 1
                if done % 200 == 0:
                    log.info("plex_enum: Phase 1 progress %d/%d (%.1fs)",
                             done, len(items), _t.monotonic() - phase1_start)
        log.info("plex_enum: Phase 1 done in %.1fs",
                 _t.monotonic() - phase1_start)
        # Resolve indeterminate (None) results against the previously-
        # stored value so a transient stat fault doesn't downgrade a
        # known-good local_theme_file=1 to 0.
        for idx, it in enumerate(items):
            res = sidecar_results.get(idx)
            if res is None:
                # Keep whatever was last in the DB (defaults to 0 on
                # first enum). For a brand-new row we don't have one;
                # 0 is the safe default.
                enriched.append((it, existing_local.get(it.rating_key, 0)))
            else:
                enriched.append((it, res))

    # Phase 2: batched upserts.
    inserted = 0
    updated = 0
    now = now_iso()
    import time as _t
    for batch_start in range(0, len(enriched), _UPSERT_BATCH):
        # v1.11.36: cooperative cancellation between upsert batches.
        if cancel_check():
            from .worker import _JobCancelled
            raise _JobCancelled()
        # v1.11.54: yield ~150ms between batches so the writer lock
        # has gaps for concurrent claim/log_event/event-flusher
        # writes. Pre-fix the back-to-back transactions held the
        # writer at near-100% duty cycle through a 10K-row enum.
        if batch_start > 0:
            _t.sleep(0.15)
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


def resolve_theme_ids(db_path: Path, *, chunk_size: int = 500) -> int:
    """Bulk-populate plex_items.theme_id for every row whose match
    against themes can be resolved by tmdb_id, imdb_id, or
    (title_norm + year). Called at the end of plex_enum and sync so
    the cached column stays fresh.

    v1.11.35: chunked by rating_key in transactions of `chunk_size`
    rows so the writer lock releases between chunks. Pre-fix the
    single bulk UPDATE with the per-row correlated subquery held
    BEGIN IMMEDIATE for 30-60s on a 4K-item plex_items, blocking
    the auth middleware's session-touch UPDATE long enough to fire
    'database is locked' and 500 the user's request mid-sync.

    Returns the total number of plex_items rows whose theme_id was
    rewritten across all chunks.
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
        WHERE rating_key IN (
            SELECT rating_key FROM plex_items
            ORDER BY rating_key
            LIMIT ? OFFSET ?
        )
    """
    import time as _time
    total = 0
    offset = 0
    with get_conn(db_path) as conn:
        # Get the row count up-front so we can stop cleanly.
        row_count = conn.execute(
            "SELECT COUNT(*) FROM plex_items"
        ).fetchone()[0]
    while offset < row_count:
        with get_conn(db_path) as conn, transaction(conn):
            cur = conn.execute(sql, (chunk_size, offset))
            total += cur.rowcount
        offset += chunk_size
        # v1.11.54: yield ~250ms between chunks (was 50ms). Each chunk
        # holds BEGIN IMMEDIATE for ~1-2s on a 15K-row plex_items;
        # 50ms gap kept the writer at 95%+ duty cycle and starved
        # other workers' claim attempts. 250ms keeps the resolve
        # progressing without monopolizing the lock.
        _time.sleep(0.25)
    log.info("resolve_theme_ids: scanned %d plex_items rows (chunk_size=%d)",
             total, chunk_size)
    return total
