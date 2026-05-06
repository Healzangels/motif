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
import threading
from pathlib import Path

from .db import get_conn, transaction
from .events import log_event, now_iso
from .plex import PlexClient, PlexConfig, PlexLibraryItem
from .sections import list_sections
from . import progress as op_progress

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

    # v1.12.106: live progress for the ops side-drawer.
    # v1.12.108: stage_current/total now track CURRENT-SECTION items,
    # not section count. Pre-fix a single-section enum hit
    # stage_current=1/stage_total=1 = 100% the instant the section
    # started, before any items had been processed — bar showed
    # complete throughout the run. Now stage_total starts at 0 (no
    # work known yet); per-section we set it to the section's item
    # count after fetch and bump stage_current as the upsert
    # completes. processed_total in detail_json carries the
    # cumulative items count across sections.
    op_progress.start_progress(
        db_path, op_id="plex_enum", kind="plex_enum",
        stage="enumerate", stage_label="Enumerating Plex sections",
        stage_total=0, processed_est=0,
    )

    def _cancel_check():
        return cancel_check() or op_progress.is_cancelled(db_path, "plex_enum")

    try:
        with PlexClient(plex_cfg) as client:
            for s in managed:
                # v1.11.36: cooperative cancellation between sections.
                if _cancel_check():
                    from .worker import _JobCancelled
                    raise _JobCancelled()
                section_id = s["section_id"]
                section_type = s["type"]
                stats["sections"] += 1
                # Pre-fetch: bar resets to 0 of "unknown" so it
                # doesn't carry the previous section's fill. Label
                # encodes section index for context.
                op_progress.update_progress(
                    db_path, "plex_enum",
                    stage_label=f"Section {stats['sections']}/{len(managed)}: {s['title']}",
                    stage_current=0,
                    stage_total=0,
                    activity=f"Fetching '{s['title']}' from Plex",
                )
                try:
                    items = client.enumerate_section_items(
                        section_id=section_id, media_type=section_type,
                    )
                except Exception as e:
                    log.warning("plex_enum: section %s failed: %s", s["title"], e)
                    stats["errors"] += 1
                    op_progress.update_progress(
                        db_path, "plex_enum",
                        error_count=stats["errors"],
                        activity=f"'{s['title']}' failed: {e}",
                    )
                    continue
                # Now we know the section's item count. Set the bar
                # to "0 of N" while the upsert runs.
                op_progress.update_progress(
                    db_path, "plex_enum",
                    stage_total=len(items),
                    stage_current=0,
                    activity=f"Upserting {len(items)} items from '{s['title']}'",
                )
                stats["items_seen"] += len(items)
                ins, upd = _upsert_items(
                    db_path, items, cancel_check=_cancel_check,
                    # v1.12.18: pass section_id so the resolve pass that
                    # runs at the end of _upsert_items can scope itself
                    # to JUST the rows we just touched. Pre-fix the
                    # resolve always ran against the full plex_items
                    # table (~10K+ rows on a populated install), which
                    # turned a 28-item section refresh into a 70s+ job.
                    section_id=section_id,
                    # v1.12.124: drive op_progress per batch so the
                    # bar reflects in-section progress instead of
                    # jumping 0 → 100% at section boundaries.
                    progress_op_id="plex_enum",
                    progress_total=len(items),
                )
                stats["inserted"] += ins
                stats["updated"] += upd
                log.info("plex_enum: section %s — %d items (%d new, %d updated)",
                         s["title"], len(items), ins, upd)
                # v1.12.112: verify Plex's theme claims for this
                # section's rows. Targets exactly the ambiguous case
                # (has_theme=1 with no fresh verification) — by far
                # the minority of rows in any library. The HEAD probe
                # distinguishes legitimate Plex-served themes
                # (themerr-plex embeds, Plex Pass cloud, sidecars)
                # from stale metadata cache. Steady-state cost is
                # near zero: once verified, the result is cached and
                # only re-tested when Plex's URI changes.
                verified_n = _verify_theme_claims(
                    db_path, client, section_id=section_id,
                    cancel_check=_cancel_check,
                )
                # Section done — fill the bar, bump cumulative.
                op_progress.update_progress(
                    db_path, "plex_enum",
                    stage_current=len(items),
                    stage_total=len(items),
                    processed_total=stats["items_seen"],
                    activity=(f"'{s['title']}': {ins} new, {upd} updated"
                              + (f", {verified_n} themes verified"
                                 if verified_n else "")),
                )

        op_progress.update_progress(
            db_path, "plex_enum",
            stage="reconcile",
            stage_label="Reconciling placement paths",
            activity="Checking for Plex folder renames",
        )
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
        op_progress.update_progress(
            db_path, "plex_enum",
            activity=(
                f"Done — {stats['sections']} sections, "
                f"{stats['items_seen']} items, "
                f"{stats['inserted']} new, {stats['updated']} updated"
                + (f", {relinked} relinked" if relinked else "")
            ),
        )
        op_progress.finish_progress(db_path, "plex_enum", status="done")
        return stats
    except Exception as e:
        from .worker import _JobCancelled
        op_progress.finish_progress(
            db_path, "plex_enum",
            status="cancelled" if isinstance(e, _JobCancelled) else "failed",
            error_message=("cancelled by user"
                           if isinstance(e, _JobCancelled) else str(e)),
        )
        raise


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
        # v1.12.81: include p.section_id in the SELECT and the
        # plex_items join so the relocate-detect is per-section.
        # Pre-fix the enqueue below dropped section_id and the
        # worker rejected the resulting place job with a v1.11.0
        # missing-section_id permanent failure, stuck-failing the
        # job and lighting up the topbar's red FAIL dot.
        rows = conn.execute(
            """SELECT DISTINCT p.media_type, p.tmdb_id, p.section_id,
                      p.media_folder AS old_folder,
                      pi.folder_path AS new_folder
               FROM placements p
               INNER JOIN plex_items pi
                 ON pi.guid_tmdb = p.tmdb_id
                AND pi.media_type = (CASE p.media_type WHEN 'tv' THEN 'show' ELSE 'movie' END)
                AND pi.section_id = p.section_id
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
            section_id = r["section_id"]
            current_plex_paths = plex_paths_by_item.get((mt, tmdb_id), set())

            # Skip if the placement's current folder is still in Plex's
            # reported set — Plex just exposes another ratingKey at a
            # different folder, but the existing placement is still
            # valid.
            if old_folder in current_plex_paths:
                continue

            try:
                # Cancel any in-flight place to avoid racing the new one.
                # v1.12.81: scope cancel + lookups + INSERT to the
                # placement's section_id so a sibling section's place
                # in flight isn't accidentally cancelled, and the
                # follow-on enqueue doesn't fail v1.11.0's
                # section_id requirement.
                conn.execute(
                    """UPDATE jobs SET status = 'cancelled', finished_at = ?
                       WHERE job_type = 'place' AND media_type = ? AND tmdb_id = ?
                         AND section_id = ?
                         AND status IN ('pending','running')""",
                    (now_iso(), mt, tmdb_id, section_id),
                )
                existing_at_new = conn.execute(
                    "SELECT 1 FROM placements "
                    "WHERE media_type = ? AND tmdb_id = ? AND section_id = ? "
                    "  AND media_folder = ?",
                    (mt, tmdb_id, section_id, new_folder),
                ).fetchone()
                if existing_at_new:
                    # Destination row already exists — UPDATE would
                    # violate the composite UNIQUE. Drop the stale
                    # old_folder row instead; the destination row
                    # already covers it.
                    conn.execute(
                        """DELETE FROM placements
                           WHERE media_type = ? AND tmdb_id = ?
                             AND section_id = ?
                             AND media_folder = ?""",
                        (mt, tmdb_id, section_id, old_folder),
                    )
                else:
                    conn.execute(
                        """UPDATE placements SET media_folder = ?
                           WHERE media_type = ? AND tmdb_id = ?
                             AND section_id = ?
                             AND media_folder = ?""",
                        (new_folder, mt, tmdb_id, section_id, old_folder),
                    )
                conn.execute(
                    """INSERT INTO jobs (job_type, media_type, tmdb_id, section_id,
                                         payload, status, created_at, next_run_at)
                       VALUES ('place', ?, ?, ?,
                               '{"force":true,"reason":"folder_relocated"}',
                               'pending', ?, ?)""",
                    (mt, tmdb_id, section_id, now_iso(), now_iso()),
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
                   *, cancel_check=lambda: False,
                   section_id: str | None = None,
                   progress_op_id: str | None = None,
                   progress_total: int | None = None,
                   progress_base: int = 0) -> tuple[int, int]:
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
    # v1.12.124: tick op_progress per batch + on a ~300ms timer so the
    # bar reflects forward progress through the upsert phase. Pre-fix
    # the caller set stage_current=0 before _upsert_items and
    # stage_current=len(items) after; the bar sat at 0 for the whole
    # upsert (5-15s on a 4K-item section) then jumped to 100%.
    last_ui = _t.monotonic()
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
        # Emit progress at the START of each batch — ticks the bar
        # as we work through the section's items rather than at the
        # post-section update point only.
        if progress_op_id and progress_total is not None and (
                batch_start == 0
                or (_t.monotonic() - last_ui) > 0.3):
            last_ui = _t.monotonic()
            op_progress.update_progress(
                db_path, progress_op_id,
                stage_current=batch_start,
                stage_total=progress_total,
                processed_total=progress_base + batch_start,
            )
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
                    "SELECT plex_theme_uri FROM plex_items "
                    "WHERE rating_key = ?",
                    (it.rating_key,),
                ).fetchone()
                if existing:
                    # v1.12.112: detect theme-URI change. Plex's URL
                    # has a trailing version suffix that bumps when
                    # the underlying theme content changes (or when
                    # the metadata refresh produces a new URL). If
                    # the URI changed, prior verification is invalid
                    # — reset verified_* to NULL so the post-upsert
                    # verification pass re-tests the new claim.
                    new_uri = it.plex_theme_uri or None
                    old_uri = existing["plex_theme_uri"] or None
                    if new_uri != old_uri:
                        conn.execute(
                            """UPDATE plex_items SET
                                  section_id = ?, media_type = ?, title = ?, year = ?,
                                  guid_imdb = ?, guid_tmdb = ?, guid_tvdb = ?,
                                  folder_path = ?, has_theme = ?, local_theme_file = ?,
                                  title_norm = ?, last_seen_at = ?,
                                  plex_theme_uri = ?,
                                  plex_theme_verified_at = NULL,
                                  plex_theme_verified_ok = NULL
                               WHERE rating_key = ?""",
                            (it.section_id, it.media_type, it.title, it.year,
                             it.guid_imdb, it.guid_tmdb, it.guid_tvdb,
                             it.folder_path, 1 if it.has_theme else 0, sidecar,
                             tn, now, new_uri, it.rating_key),
                        )
                    else:
                        # URI unchanged — keep prior verification.
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
                            plex_theme_uri,
                            first_seen_at, last_seen_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (it.rating_key, it.section_id, it.media_type, it.title,
                         it.year, it.guid_imdb, it.guid_tmdb, it.guid_tvdb,
                         it.folder_path, 1 if it.has_theme else 0, sidecar,
                         tn, it.plex_theme_uri or None, now, now),
                    )
                    inserted += 1
    # v1.11.26: stamp theme_id once per enum so /api/library's row
    # query becomes a direct PK lookup instead of running a heavy
    # correlated subquery on every page render.
    # v1.12.18: scope to the section we just enumerated when called
    # from the per-section path. Pre-fix this always re-resolved
    # the entire plex_items table (~10K+ rows), turning a 28-item
    # section refresh into a multi-minute job and contending hard
    # with the writer lock the whole time.
    resolve_theme_ids(db_path, section_id=section_id, cancel_check=cancel_check)
    return inserted, updated


def _verify_theme_claims(
    db_path: Path, client: PlexClient,
    *, section_id: str | None = None,
    cancel_check=lambda: False,
) -> int:
    """v1.12.112: HEAD-probe Plex's `theme="..."` claims so motif's
    SRC=P reflects ground truth instead of Plex's metadata cache.

    Targets only ambiguous rows: `has_theme=1` AND `local_theme_file=0`
    (no sidecar — so SRC would land in case 7) AND verification needs
    to run. v1.12.113 broadened "needs verification" to include
    `verified_ok=0` rows older than 7 days (handle transient Plex
    404s). v1.12.115 adds the symmetric TTL on `verified_ok=1` —
    after 30 days, re-verify successful claims too. Closes the
    edge case where Plex's in-memory cache served a just-deleted
    theme during PURGE inline-verify, motif marked verified_ok=1,
    and Plex evicted its cache later but motif kept the stale 1
    forever (until URI changed). 30-day cycle keeps steady-state
    HEAD cost near zero — for a library with 100 P-classified rows,
    ~3–4 verifications per day average.

    Sidecared rows (M/T/U/A) classify earlier in the SRC priority
    chain and don't need this signal.

    Each probe writes either ok=1 (200), ok=0 (404), or leaves
    ok=NULL on transient errors so a network blip doesn't penalize
    the row. Returns the count of rows that received a definitive
    verification (200 or 404) so the caller can surface progress.
    """
    sql = (
        "SELECT rating_key FROM plex_items "
        "WHERE has_theme = 1 "
        "  AND local_theme_file = 0 "
        "  AND ("
        "       plex_theme_verified_ok IS NULL "
        "    OR (plex_theme_verified_ok = 0 "
        "        AND (plex_theme_verified_at IS NULL "
        "             OR plex_theme_verified_at < datetime('now', '-7 days')))"
        "    OR (plex_theme_verified_ok = 1 "
        "        AND (plex_theme_verified_at IS NULL "
        "             OR plex_theme_verified_at < datetime('now', '-30 days')))"
        "  ) "
    )
    params: tuple = ()
    if section_id is not None:
        sql += "  AND section_id = ? "
        params = (section_id,)
    with get_conn(db_path) as conn:
        rows = conn.execute(sql, params).fetchall()
    if not rows:
        return 0
    decided = 0
    # v1.12.116: open a single connection for the whole batch instead
    # of per-row, AND wrap each row's HEAD+UPDATE in its own try/except
    # so a single transient (DB lock timeout, network blip, etc.)
    # doesn't abort verification for the rest of the section.
    # Pre-fix the loop bailed on the first row's exception, plex_enum
    # then marked the whole op 'failed' — a 100-row library lost all
    # verification progress because one row hit a 5s lock.
    with get_conn(db_path) as conn:
        for row in rows:
            if cancel_check():
                from .worker import _JobCancelled
                raise _JobCancelled()
            rk = row["rating_key"]
            try:
                result = client.verify_theme_claim(rk)
                if result is None:
                    # Transient — leave verified_ok NULL so we retry
                    # next enum (or after the v1.12.115 TTL).
                    continue
                conn.execute(
                    "UPDATE plex_items SET plex_theme_verified_at = ?, "
                    "                      plex_theme_verified_ok = ? "
                    "WHERE rating_key = ?",
                    (now_iso(), 1 if result else 0, rk),
                )
                decided += 1
            except Exception as e:
                log.debug("verify_theme_claim row %s failed: %s", rk, e)
                continue
    return decided


def resolve_theme_ids(
    db_path: Path,
    *,
    chunk_size: int = 500,
    section_id: str | None = None,
    cancel_check=lambda: False,
) -> int:
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

    v1.12.14: serialized via _resolve_theme_ids_lock. Pre-fix two
    concurrent callers (sync's post-pass + plex_enum's post-pass on
    the long-job thread, or two plex_enums after a settings change)
    could both enter the loop. SQLite's BEGIN IMMEDIATE serializes
    each chunk transaction at the DB level, but the Python-side
    pagination (offset += chunk_size based on a row_count captured
    before either caller started) didn't coordinate — the second
    caller could re-walk rows the first already updated, doubling
    the wall-clock cost and the writer-lock pressure. The lock
    forces serial execution; the second caller's pass is then
    idempotent (no-op for already-resolved rows + picks up any new
    inserts that arrived while the first ran).

    Returns the total number of plex_items rows whose theme_id was
    rewritten across all chunks.
    """
    with _resolve_theme_ids_lock:
        return _resolve_theme_ids_impl(
            db_path, chunk_size=chunk_size,
            section_id=section_id, cancel_check=cancel_check,
        )


_resolve_theme_ids_lock = threading.Lock()


def _resolve_theme_ids_impl(
    db_path: Path,
    *,
    chunk_size: int = 500,
    section_id: str | None = None,
    cancel_check=lambda: False,
) -> int:
    # v1.12.18: optional section_id scope. When set, restrict the
    # resolve pass to rating_keys in that section — relevant for
    # the per-section refresh path where we know exactly which rows
    # we just touched and don't need to re-walk the whole table.
    scope_clause = ""
    scope_params: tuple = ()
    if section_id is not None:
        scope_clause = "AND section_id = ?"
        scope_params = (section_id,)
    # v1.12.22: split the unified OR'd UPDATE into three single-
    # condition statements. Pre-fix the combined query had:
    #   WHERE (tmdb_id match) OR (imdb_id match) OR (title_norm+year match)
    # SQLite's OR optimization didn't reliably use idx_themes_title_norm
    # on the third branch when paired with the upstream_source filter,
    # so the title-fallback path went full-scan against themes
    # (~10K plex_items rows × ~50K themes rows × full scan = ~30s
    # per 500-row chunk on a populated install). Splitting lets each
    # UPDATE use its own dedicated index cleanly:
    #   1) tmdb_id    → themes PK (media_type, tmdb_id)
    #   2) imdb_id    → idx_themes_imdb
    #   3) title+year → idx_themes_title_norm
    # Each UPDATE only sets theme_id where it's currently NULL or
    # would be improved (orphan → real). Order matters: 1 first
    # (preferred), then 2 (orphan match), then 3 (last-resort title
    # fallback).
    sql_tmdb = f"""
        UPDATE plex_items SET theme_id = (
            SELECT t.id FROM themes t
            WHERE t.media_type = (CASE plex_items.media_type
                                       WHEN 'show' THEN 'tv'
                                       ELSE plex_items.media_type END)
              AND t.tmdb_id = plex_items.guid_tmdb
              AND t.upstream_source != 'plex_orphan'
            ORDER BY t.id DESC
            LIMIT 1
        )
        WHERE plex_items.guid_tmdb IS NOT NULL
          AND rating_key IN (
              SELECT rating_key FROM plex_items
              WHERE 1=1 {scope_clause}
              ORDER BY rating_key
              LIMIT ? OFFSET ?
          )
    """
    sql_imdb = f"""
        UPDATE plex_items SET theme_id = (
            SELECT t.id FROM themes t
            WHERE t.media_type = (CASE plex_items.media_type
                                       WHEN 'show' THEN 'tv'
                                       ELSE plex_items.media_type END)
              AND t.imdb_id = plex_items.guid_imdb
              AND t.upstream_source = 'plex_orphan'
            ORDER BY t.id DESC
            LIMIT 1
        )
        WHERE plex_items.theme_id IS NULL
          AND plex_items.guid_imdb IS NOT NULL
          AND rating_key IN (
              SELECT rating_key FROM plex_items
              WHERE 1=1 {scope_clause}
              ORDER BY rating_key
              LIMIT ? OFFSET ?
          )
    """
    sql_title = f"""
        UPDATE plex_items SET theme_id = (
            SELECT t.id FROM themes t
            WHERE t.media_type = (CASE plex_items.media_type
                                       WHEN 'show' THEN 'tv'
                                       ELSE plex_items.media_type END)
              AND t.title_norm = plex_items.title_norm
              AND t.year = plex_items.year
              AND t.upstream_source != 'plex_orphan'
            ORDER BY t.id DESC
            LIMIT 1
        )
        WHERE plex_items.theme_id IS NULL
          AND plex_items.title_norm IS NOT NULL
          AND plex_items.year IS NOT NULL
          AND rating_key IN (
              SELECT rating_key FROM plex_items
              WHERE 1=1 {scope_clause}
              ORDER BY rating_key
              LIMIT ? OFFSET ?
          )
    """
    import time as _time
    total = 0
    offset = 0
    with get_conn(db_path) as conn:
        count_sql = "SELECT COUNT(*) FROM plex_items"
        if section_id is not None:
            count_sql += " WHERE section_id = ?"
        row_count = conn.execute(count_sql, scope_params).fetchone()[0]
    if row_count == 0:
        return 0
    log.info(
        "resolve_theme_ids: starting on %d plex_items rows%s "
        "(chunk_size=%d)",
        row_count,
        f" (section {section_id})" if section_id else "",
        chunk_size,
    )
    progress_t0 = _time.monotonic()
    while offset < row_count:
        # v1.12.18: cancel-check between chunks so a stuck resolve
        # can be interrupted from the /logs CANCEL button. Also
        # guards against the user kicking off a sync mid-resolve;
        # the new sync's resolve waits on the mutex, but the
        # current one stays interruptible.
        if cancel_check():
            from .worker import _JobCancelled
            log.info("resolve_theme_ids: cancelled at %d/%d rows",
                     offset, row_count)
            raise _JobCancelled()
        # v1.12.22: run all three index-targeted UPDATEs per chunk
        # in one transaction. Most rows match by tmdb_id (PK index)
        # so sql_tmdb does the bulk of the work; sql_imdb covers
        # plex_orphan rows; sql_title is a last-resort fallback for
        # rows missing both GUIDs. Each uses its dedicated index;
        # combined wall-time is ~30x faster than the old OR'd query
        # that fell back to a full themes scan on the title branch.
        chunk_params = scope_params + (chunk_size, offset)
        with get_conn(db_path) as conn, transaction(conn):
            cur = conn.execute(sql_tmdb, chunk_params)
            total += cur.rowcount
            cur = conn.execute(sql_imdb, chunk_params)
            total += cur.rowcount
            cur = conn.execute(sql_title, chunk_params)
            total += cur.rowcount
        offset += chunk_size
        # v1.12.18: progress log every 5s so a stuck resolve is
        # visible in the logs instead of silent. Pre-fix the only
        # log line landed at completion, which made it impossible
        # to tell whether a "running" plex_enum job was making
        # progress or wedged.
        elapsed = _time.monotonic() - progress_t0
        if elapsed > 5.0:
            log.info("resolve_theme_ids: progress %d/%d (%.1fs)",
                     offset, row_count, elapsed)
            progress_t0 = _time.monotonic()
        # v1.11.54: yield ~250ms between chunks (was 50ms). Each chunk
        # holds BEGIN IMMEDIATE for ~1-2s on a 15K-row plex_items;
        # 50ms gap kept the writer at 95%+ duty cycle and starved
        # other workers' claim attempts. 250ms keeps the resolve
        # progressing without monopolizing the lock.
        _time.sleep(0.25)
    log.info("resolve_theme_ids: scanned %d plex_items rows (chunk_size=%d)",
             total, chunk_size)
    return total
