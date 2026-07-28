"""v1.22.49 — imdb→tmdb de-orphan walker.

the user's diagnostic (v1.22.48) found ~547 of 575 plex_orphan themes carry a real
imdb_id that resolves to a real tmdb_id via TMDB — they're real, identifiable
titles motif failed to resolve when it minted their synthetic negative tmdb_id
(no imdb→tmdb resolver exists for movies, only the tvdb→tmdb TVDB bridge). The
INFO card shows "tmdb: orphan" whenever tmdb_id <= 0, so these read as lost rows.

This walker resolves each orphan's imdb → real tmdb via TMDB and RE-KEYS the
synthetic negative tmdb_id to the real one across the theme + its FK'd children
(local_files / placements / pending_updates / user_overrides), mirroring the
battle-tested orphan-promotion re-key in sync._upsert_theme (PRAGMA
defer_foreign_keys so the parent + children move consistently inside one txn).

Deliberate scope (the user: walker-first, alone):
  - SKIP on collision — if a theme already exists at the target (media_type,
    real_tmdb), we never touch it (don't clobber a real theme / risk the
    UNIQUE-merge data-loss class). The diagnostic showed <1% collide.
  - KEEP upstream_source='plex_orphan'. The theme is still a user/adopted manual
    theme, NOT from ThemerrDB — flipping upstream would falsely surface a
    (non-existent) ThemerrDB-url field on the INFO card. Re-keying alone fixes
    "tmdb: orphan" → the real id AND makes the row TDB-matchable, so the next
    sync promotes the ones ThemerrDB actually has (preserving the manual theme
    via the existing override / pending-update guard). The cosmetic plex_orphan
    label for genuinely-TDB-less rows is a deferred display pass.
  - Per-row transaction + try/except so one bad row can't abort the batch;
    idempotent (a re-keyed row now has a positive tmdb_id → no longer selected).
"""
from __future__ import annotations

import logging
import sqlite3
import threading
import time as _time
from dataclasses import dataclass, field
from pathlib import Path

from .db import get_conn, transaction
from .events import log_event

log = logging.getLogger(__name__)

# v0.50.90: single-flight guard for the auto-triggered background resolution
# (boot + TMDB-key save/test). A non-blocking acquire means overlapping triggers
# collapse to one run instead of stacking N concurrent TMDB sweeps.
_RESOLVE_LOCK = threading.Lock()


@dataclass
class DeorphanReport:
    dry_run: bool = True
    scanned: int = 0          # orphan rows considered (negative tmdb + imdb)
    probed: int = 0           # TMDB lookups attempted
    rekeyed: int = 0          # rows re-keyed (or would-re-key in dry_run)
    no_tmdb_match: int = 0     # TMDB lacks the title / disagrees on type
    skipped_collision: int = 0  # a theme already exists at the resolved tmdb
    errors: int = 0           # per-row failures (logged)
    samples: list = field(default_factory=list)


def _resolve_real_tmdb(tmdb_client, imdb_id: str, media_type: str):
    """Return the real tmdb_id for an imdb_id when TMDB agrees on media_type,
    else None. Cached via the client's 7-day tvdb_lookup_cache."""
    hit = tmdb_client.lookup_by_imdb(imdb_id)
    if not hit:
        return None
    cand_tmdb = hit.get("tmdb_id")
    cand_kind = hit.get("kind")
    if not cand_tmdb or not cand_kind:
        return None
    # a 'tv' orphan must resolve to a tv record (and vice-versa) — otherwise
    # we'd re-key into the wrong identity space.
    if (cand_kind == "tv") != (media_type == "tv"):
        return None
    return cand_tmdb


def deorphan_imdb_resolvable(
    db_path: Path,
    tmdb_client,
    *,
    dry_run: bool = True,
    max_rows: int | None = None,
    cancel_check=lambda: False,
) -> DeorphanReport:
    """Walk plex_orphan themes with a synthetic negative tmdb_id + an imdb_id,
    resolve imdb→real tmdb via TMDB, and re-key the synthetic id to the real one
    (skip on collision). Read-only when dry_run=True."""
    rep = DeorphanReport(dry_run=dry_run)
    with get_conn(db_path) as conn:
        orphans = conn.execute(
            "SELECT id, media_type, tmdb_id, imdb_id, title, year FROM themes "
            "WHERE upstream_source = 'plex_orphan' "
            "  AND tmdb_id < 0 "
            "  AND imdb_id IS NOT NULL AND imdb_id != '' "
            "ORDER BY id"
        ).fetchall()

    for o in orphans:
        if cancel_check():
            log.info("deorphan: cancelled at %d scanned", rep.scanned)
            break
        if max_rows is not None and rep.rekeyed >= max_rows:
            break
        rep.scanned += 1
        media_type = o["media_type"]
        old_tmdb = o["tmdb_id"]
        imdb_id = o["imdb_id"]
        rep.probed += 1
        try:
            real_tmdb = _resolve_real_tmdb(tmdb_client, imdb_id, media_type)
        except Exception as e:  # noqa: BLE001
            rep.errors += 1
            log.warning("deorphan: TMDB probe failed for %s (%s): %s",
                        imdb_id, o["title"], e)
            continue
        if real_tmdb is None:
            rep.no_tmdb_match += 1
            continue

        # Collision: a theme already exists at the target identity — never
        # clobber it. (The diagnostic flags these as would-merge; <1%.)
        with get_conn(db_path) as conn:
            clash = conn.execute(
                "SELECT 1 FROM themes WHERE media_type = ? AND tmdb_id = ? "
                "LIMIT 1",
                (media_type, real_tmdb),
            ).fetchone()
        if clash is not None:
            rep.skipped_collision += 1
            if len(rep.samples) < 50:
                rep.samples.append({
                    "imdb": imdb_id, "title": o["title"], "year": o["year"],
                    "media_type": media_type, "old_tmdb": old_tmdb,
                    "real_tmdb": real_tmdb, "action": "skipped_collision",
                })
            continue

        if dry_run:
            rep.rekeyed += 1
            if len(rep.samples) < 50:
                rep.samples.append({
                    "imdb": imdb_id, "title": o["title"], "year": o["year"],
                    "media_type": media_type, "old_tmdb": old_tmdb,
                    "real_tmdb": real_tmdb, "action": "would_rekey"})
            continue

        # Re-key the synthetic id → the real id across the theme + its FK'd
        # children, mirroring sync._upsert_theme's promotion. defer_foreign_keys
        # so the parent + children move consistently before COMMIT validates the
        # FKs. upstream_source is deliberately PRESERVED (still plex_orphan).
        # v1.22.50: record the REAL per-row outcome (sample label was set
        # optimistically pre-txn, so an errored/skipped row mislabeled as
        # "rekeyed"); class-8 retry on `database is locked`; and pre-delete any
        # STALE child rows at the target (FK-invalid leftovers from a prior
        # failed op) — the exact UNIQUE-conflict the sync-promotion already
        # guards against by clearing the target first.
        status = "rekeyed"
        err: str | None = None
        for attempt in range(3):
            try:
                with get_conn(db_path) as conn, transaction(conn):
                    # Re-confirm under the write lock (idempotency / race-safety)
                    # — and catch the intra-batch case where a duplicate orphan
                    # already re-keyed to this target earlier in the run.
                    still = conn.execute(
                        "SELECT 1 FROM themes WHERE id = ? AND tmdb_id = ? "
                        "  AND upstream_source = 'plex_orphan'",
                        (o["id"], old_tmdb)).fetchone()
                    clash2 = conn.execute(
                        "SELECT 1 FROM themes WHERE media_type = ? "
                        "AND tmdb_id = ? LIMIT 1",
                        (media_type, real_tmdb)).fetchone()
                    if clash2 is not None:
                        status = "skipped_collision"
                    elif still is None:
                        status = "skipped_changed"
                    else:
                        conn.execute("PRAGMA defer_foreign_keys = ON")
                        # Clear FK-invalid leftovers at the target so the child
                        # re-key UPDATEs can't UNIQUE-collide (no real theme is
                        # here — clash2 confirmed that — so any child rows are
                        # orphaned junk).
                        for tbl in ("local_files", "placements",
                                    "pending_updates", "user_overrides",
                                    "previous_urls"):
                            conn.execute(
                                f"DELETE FROM {tbl} "
                                f"WHERE media_type = ? AND tmdb_id = ?",
                                (media_type, real_tmdb))
                        conn.execute(
                            "UPDATE themes SET tmdb_id = ? WHERE id = ?",
                            (real_tmdb, o["id"]))
                        for tbl in ("local_files", "placements",
                                    "pending_updates", "user_overrides",
                                    "previous_urls"):
                            conn.execute(
                                f"UPDATE {tbl} SET tmdb_id = ? "
                                f"WHERE media_type = ? AND tmdb_id = ?",
                                (real_tmdb, media_type, old_tmdb))
                        # v0.51.232 (audit): the two NEWER (media_type, tmdb_id) tables
                        # were never added to this loop. They are NOT FK'd, so nothing
                        # cascaded and nothing complained — the rows just kept pointing at
                        # the retired synthetic id. Consequences: an open INBOX row still
                        # renders clickable (the drawer gates on media_type && tmdb_id) and
                        # deep-links to a tmdb that no longer exists, so the v0.51.220
                        # edition-exact click-through lands nowhere; and a dismissed
                        # per-section failure ack stops joining its theme, so the banner
                        # the operator dismissed comes back. Deliberately NOT added to the
                        # pre-delete loop above — that clears FK-invalid junk at the
                        # target, and deleting the target's real notifications/acks would
                        # destroy live inbox rows.
                        for tbl in ("notifications", "section_failure_acks"):
                            conn.execute(
                                f"UPDATE {tbl} SET tmdb_id = ? "
                                f"WHERE media_type = ? AND tmdb_id = ?",
                                (real_tmdb, media_type, old_tmdb))
                        # v0.51.11: re-key in-flight jobs too (mirrors sync.py's
                        # v1.22.87 fix) — a pending download/place enqueued at the
                        # synthetic orphan id (SET URL on an orphan) dies confusingly
                        # when boot's de-orphan walker promotes the id mid-window: the
                        # worker resolves the theme/override at the OLD id (re-keyed
                        # away → empty) and the user's download fails as a mystery job
                        # error. Not FK-constrained, so no target-delete needed.
                        conn.execute(
                            "UPDATE jobs SET tmdb_id = ? WHERE media_type = ? "
                            "AND tmdb_id = ? AND status IN ('pending', 'running')",
                            (real_tmdb, media_type, old_tmdb))
                break
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < 2:
                    _time.sleep(0.4 * (attempt + 1))
                    continue
                status = "error"
                err = str(e)
                break
            except Exception as e:  # noqa: BLE001
                status = "error"
                err = str(e)
                break

        if status == "rekeyed":
            rep.rekeyed += 1
            log.info(
                "deorphan: re-keyed '%s' (%s) imdb=%s tmdb %d → %d",
                o["title"], media_type, imdb_id, old_tmdb, real_tmdb)
        elif status == "skipped_collision":
            rep.skipped_collision += 1
        elif status == "error":
            rep.errors += 1
            log.warning("deorphan: re-key failed for '%s' (%s→%s): %s",
                        o["title"], old_tmdb, real_tmdb, err)
        # status == "skipped_changed": a concurrent writer moved the row out
        # from under us — rare; left silent (re-scanned next run if still apt).

        if len(rep.samples) < 50:
            s = {"imdb": imdb_id, "title": o["title"], "year": o["year"],
                 "media_type": media_type, "old_tmdb": old_tmdb,
                 "real_tmdb": real_tmdb, "action": status}
            if err:
                s["error"] = err[:300]
            rep.samples.append(s)

    if not dry_run and rep.rekeyed:
        log_event(
            db_path, level="INFO", component="deorphan",
            message=(
                f"De-orphan walker re-keyed {rep.rekeyed} imdb-resolvable "
                f"orphan theme(s) to real tmdb_ids "
                f"({rep.skipped_collision} skipped on collision, "
                f"{rep.no_tmdb_match} unresolved, {rep.errors} errors)."),
        )
    return rep


def _has_resolvable_orphans(db_path: Path) -> bool:
    """Cheap indexed check: is there at least one plex_orphan theme with a
    synthetic negative tmdb_id AND an imdb_id? Lets every auto-trigger short-
    circuit to a no-op when there's nothing to resolve."""
    try:
        with get_conn(db_path) as conn:
            return conn.execute(
                "SELECT 1 FROM themes WHERE upstream_source = 'plex_orphan' "
                "  AND tmdb_id < 0 AND imdb_id IS NOT NULL AND imdb_id != '' "
                "LIMIT 1"
            ).fetchone() is not None
    except Exception as e:  # noqa: BLE001
        log.warning("deorphan: orphan-presence check failed: %s", e)
        return False


def resolve_orphans_in_background(
    db_path: Path, *, api_key: str, trigger: str,
) -> bool:
    """v0.50.90: fire-and-forget the imdb→tmdb re-key walker on a daemon
    thread. Called when a valid TMDB key lands (config save / TEST KEY) and
    once per boot when a key is present, so orphans that were minted before a
    key existed self-heal without a manual admin POST.

    Non-destructive: runs ONLY the re-key walker (deorphan_imdb_resolvable),
    never the destructive collision-merge (that stays manual). Single-flight
    via _RESOLVE_LOCK so overlapping triggers don't stack. Returns True if a
    run was actually started, False if it was skipped (no key, no orphans, or
    a run already in flight)."""
    if not api_key:
        return False
    if not _has_resolvable_orphans(db_path):
        return False
    if not _RESOLVE_LOCK.acquire(blocking=False):
        log.info("deorphan: a resolution pass is already running; %s "
                 "trigger skipped", trigger)
        return False

    def _work() -> None:
        try:
            from .tmdb import TMDBClient
            rep = deorphan_imdb_resolvable(
                db_path, TMDBClient(api_key, db_path), dry_run=False)
            log.info(
                "deorphan(%s): re-keyed %d/%d orphan(s) "
                "(unresolved %d, collisions %d, errors %d)",
                trigger, rep.rekeyed, rep.scanned, rep.no_tmdb_match,
                rep.skipped_collision, rep.errors)
        except Exception as e:  # noqa: BLE001
            log.warning("deorphan background resolution failed (%s): %s",
                        trigger, e)
        finally:
            _RESOLVE_LOCK.release()

    t = threading.Thread(target=_work, name="deorphan-resolve", daemon=True)
    try:
        t.start()
    except Exception:
        _RESOLVE_LOCK.release()
        raise
    return True


# v1.24.15 (holistic review): amplifier-sweep abort cap for the destructive
# collision-merge walker (v1.18.10 class). A real run merges a handful of
# duplicate orphans; far more means broken state — abort and let the operator
# review rather than mass-delete.
_MERGE_ABORT_CAP = 250


@dataclass
class MergeReport:
    dry_run: bool = True
    scanned: int = 0
    merged: int = 0
    errors: int = 0
    samples: list = field(default_factory=list)


def merge_orphan_collisions(
    db_path: Path,
    tmdb_client,
    *,
    dry_run: bool = True,
) -> MergeReport:
    """v1.22.54 (the user: 'override wins'): merge each collision the re-key
    walker skipped — an orphan (tmdb<0, imdb set) whose resolved real tmdb
    already has a theme row — INTO that target record, then delete the husk.

    Policy per child table (key = PK minus tmdb_id, so same-section rows
    collide):
      - user_overrides + local_files: ORPHAN WINS on collision (the user's
        deliberate URL choice + its manual theme are authoritative — restores
        the U-row state motif would have had without the identity split; TDB's
        theme stays reachable via the normal pending-update ACCEPT prompt).
      - placements: LATEST placed_at wins on collision (a placement records
        what's physically deployed; the newest write is on-disk reality).
      - pending_updates + previous_urls: TARGET WINS on collision (TDB-side
        state attached to the real identity; orphans hold none in practice).
    Moved rows get theme_id re-pointed to the target theme's pk (the re-key
    walker never needed this — its theme row kept its id; a merge crosses
    rows). plex_items + scan_findings.theme_id re-point before the husk
    delete (both FKs are ON DELETE SET NULL — deleting first would null them).
    Remaining orphan children (collision losers) are deleted explicitly, then
    the husk. One txn per collision (defer FKs + class-8 lock retry)."""
    rep = MergeReport(dry_run=dry_run)
    with get_conn(db_path) as conn:
        orphans = conn.execute(
            "SELECT id, media_type, tmdb_id, imdb_id, title, year FROM themes "
            "WHERE upstream_source = 'plex_orphan' AND tmdb_id < 0 "
            "  AND imdb_id IS NOT NULL AND imdb_id != '' ORDER BY id"
        ).fetchall()

    _EK_TABLES_ORPHAN_WINS = ("user_overrides", "local_files")
    for o in orphans:
        media_type = o["media_type"]
        old_tmdb = o["tmdb_id"]
        try:
            real_tmdb = _resolve_real_tmdb(tmdb_client, o["imdb_id"],
                                           media_type)
        except Exception as e:  # noqa: BLE001
            rep.errors += 1
            log.warning("deorphan-merge: TMDB probe failed for %s: %s",
                        o["imdb_id"], e)
            continue
        if real_tmdb is None:
            continue
        with get_conn(db_path) as conn:
            target = conn.execute(
                "SELECT id FROM themes WHERE media_type = ? AND tmdb_id = ? "
                "LIMIT 1", (media_type, real_tmdb)).fetchone()
        if target is None:
            continue  # re-keyable, not a collision — the re-key walker's job
        rep.scanned += 1
        if dry_run:
            rep.merged += 1
            if len(rep.samples) < 50:
                rep.samples.append({
                    "title": o["title"], "media_type": media_type,
                    "imdb": o["imdb_id"], "orphan_tmdb": old_tmdb,
                    "target_tmdb": real_tmdb, "action": "would_merge"})
            continue

        status = "merged"
        err: str | None = None
        for attempt in range(3):
            try:
                with get_conn(db_path) as conn, transaction(conn):
                    still = conn.execute(
                        "SELECT 1 FROM themes WHERE id = ? AND tmdb_id = ? "
                        "  AND upstream_source = 'plex_orphan'",
                        (o["id"], old_tmdb)).fetchone()
                    tgt = conn.execute(
                        "SELECT id FROM themes WHERE media_type = ? "
                        "AND tmdb_id = ? LIMIT 1",
                        (media_type, real_tmdb)).fetchone()
                    if still is None or tgt is None:
                        status = "skipped_changed"
                        break
                    target_id = tgt["id"]
                    conn.execute("PRAGMA defer_foreign_keys = ON")
                    for tbl in _EK_TABLES_ORPHAN_WINS:
                        conn.execute(
                            f"DELETE FROM {tbl} WHERE media_type = ? "
                            f"AND tmdb_id = ? AND EXISTS ("
                            f"  SELECT 1 FROM {tbl} o WHERE o.media_type = "
                            f"  {tbl}.media_type AND o.tmdb_id = ? "
                            f"  AND o.section_id = {tbl}.section_id "
                            f"  AND o.edition_key = {tbl}.edition_key)",
                            (media_type, real_tmdb, old_tmdb))
                        conn.execute(
                            f"UPDATE {tbl} SET tmdb_id = ?, theme_id = ? "
                            f"WHERE media_type = ? AND tmdb_id = ?",
                            (real_tmdb, target_id, media_type, old_tmdb))
                    # placements: drop target rows beaten by a NEWER orphan
                    # placement at the same slot, then move the free ones.
                    conn.execute(
                        "DELETE FROM placements WHERE media_type = ? "
                        "AND tmdb_id = ? AND EXISTS ("
                        "  SELECT 1 FROM placements o WHERE o.media_type = "
                        "  placements.media_type AND o.tmdb_id = ? "
                        "  AND o.section_id = placements.section_id "
                        "  AND o.media_folder = placements.media_folder "
                        "  AND o.edition_key = placements.edition_key "
                        "  AND o.placed_at > placements.placed_at)",
                        (media_type, real_tmdb, old_tmdb))
                    conn.execute(
                        "UPDATE placements SET tmdb_id = ?, theme_id = ? "
                        "WHERE media_type = ? AND tmdb_id = ? AND NOT EXISTS ("
                        "  SELECT 1 FROM placements t WHERE t.media_type = "
                        "  placements.media_type AND t.tmdb_id = ? "
                        "  AND t.section_id = placements.section_id "
                        "  AND t.media_folder = placements.media_folder "
                        "  AND t.edition_key = placements.edition_key)",
                        (real_tmdb, target_id, media_type, old_tmdb,
                         real_tmdb))
                    # pending_updates (target wins): move only free slots.
                    conn.execute(
                        "UPDATE pending_updates SET tmdb_id = ?, theme_id = ? "
                        "WHERE media_type = ? AND tmdb_id = ? AND NOT EXISTS ("
                        "  SELECT 1 FROM pending_updates t WHERE t.media_type "
                        "  = pending_updates.media_type AND t.tmdb_id = ? "
                        "  AND t.section_id = pending_updates.section_id "
                        "  AND t.edition_key = pending_updates.edition_key)",
                        (real_tmdb, target_id, media_type, old_tmdb,
                         real_tmdb))
                    # previous_urls (target wins; no theme_id/edition_key).
                    conn.execute(
                        "UPDATE previous_urls SET tmdb_id = ? "
                        "WHERE media_type = ? AND tmdb_id = ? AND NOT EXISTS ("
                        "  SELECT 1 FROM previous_urls t WHERE t.media_type = "
                        "  previous_urls.media_type AND t.tmdb_id = ? "
                        "  AND t.section_id = previous_urls.section_id)",
                        (real_tmdb, media_type, old_tmdb, real_tmdb))
                    # v0.51.11: re-key in-flight jobs (mirrors sync.py v1.22.87 +
                    # the resolve-loop above) — a pending download/place enqueued at
                    # the orphan's synthetic id must follow the merge into the real
                    # id, or the worker resolves an empty theme at the old id and the
                    # user's download fails as a mystery job error. No collision guard
                    # needed (jobs has no UNIQUE on media_type/tmdb_id).
                    conn.execute(
                        "UPDATE jobs SET tmdb_id = ? WHERE media_type = ? "
                        "AND tmdb_id = ? AND status IN ('pending', 'running')",
                        (real_tmdb, media_type, old_tmdb))
                    # re-point library + scan rows BEFORE the husk delete
                    # (both FKs are ON DELETE SET NULL).
                    conn.execute(
                        "UPDATE plex_items SET theme_id = ? WHERE theme_id = ?",
                        (target_id, o["id"]))
                    conn.execute(
                        "UPDATE scan_findings SET theme_id = ? "
                        "WHERE theme_id = ?", (target_id, o["id"]))
                    # v0.51.232 (audit): re-point the two non-FK'd (media_type, tmdb_id)
                    # tables at the SURVIVING id before the husk's rows are dropped.
                    # They are not FK'd so the husk DELETE below never touched them and
                    # they were left stranded at a tmdb_id with no theme — a clickable
                    # inbox row deep-linking nowhere, and a dismissed failure ack that
                    # stops matching so the banner reappears. Re-key (not delete): these
                    # are the operator's own notifications and dismissals.
                    for tbl in ("notifications", "section_failure_acks"):
                        conn.execute(
                            f"UPDATE {tbl} SET tmdb_id = ? "
                            f"WHERE media_type = ? AND tmdb_id = ?",
                            (real_tmdb, media_type, old_tmdb))
                    # collision losers left at the old tmdb, then the husk.
                    for tbl in ("local_files", "placements", "pending_updates",
                                "user_overrides", "previous_urls"):
                        conn.execute(
                            f"DELETE FROM {tbl} WHERE media_type = ? "
                            f"AND tmdb_id = ?", (media_type, old_tmdb))
                    conn.execute("DELETE FROM themes WHERE id = ?", (o["id"],))
                break
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < 2:
                    _time.sleep(0.4 * (attempt + 1))
                    continue
                status, err = "error", str(e)
                break
            except Exception as e:  # noqa: BLE001
                status, err = "error", str(e)
                break

        if status == "merged":
            rep.merged += 1
            log.info("deorphan-merge: merged '%s' (%s) tmdb %d → %d",
                     o["title"], media_type, old_tmdb, real_tmdb)
            # v1.24.15 (holistic review): amplifier-sweep guard (v1.18.10
            # class). This walker DELETEs husk theme/local_files/placements/
            # override rows + re-points FKs. Under a broken state that made
            # many orphans resolve to existing targets, it could mass-delete
            # with no fail-safe. A real run merges a handful; abort loudly past
            # a sane absolute cap and let the operator review.
            if rep.merged >= _MERGE_ABORT_CAP:
                log.warning(
                    "deorphan-merge: ABORTING after %d merges (cap=%d) — an "
                    "unexpectedly large collision set may indicate broken "
                    "state; review before re-running",
                    rep.merged, _MERGE_ABORT_CAP)
                break
        elif status == "error":
            rep.errors += 1
            log.warning("deorphan-merge: failed for '%s' (%s→%s): %s",
                        o["title"], old_tmdb, real_tmdb, err)
        if len(rep.samples) < 50:
            s = {"title": o["title"], "media_type": media_type,
                 "imdb": o["imdb_id"], "orphan_tmdb": old_tmdb,
                 "target_tmdb": real_tmdb, "action": status}
            if err:
                s["error"] = err[:300]
            rep.samples.append(s)

    # v1.24.15 (holistic review): breadcrumb UNCONDITIONALLY on a non-dry-run
    # pass (was gated on rep.merged, so a merged-0 destructive pass left no
    # audit entry — invisible that it ran).
    if not dry_run:
        log_event(
            db_path, level="INFO", component="deorphan",
            message=(
                f"Collision-merge consolidated {rep.merged} duplicate orphan "
                f"theme(s) into their real tmdb records "
                f"({rep.errors} errors, {rep.scanned} scanned). "
                f"Policy: user override wins."),
        )
    return rep
