"""v0.51.276 — feature-brief E, first release: ONE reconciliation run.

motif already owns every detector this feature needs — canonical health
(v1.23.37), placement health (v1.23.25), the hourly missing-placement retry
sweep with its mature skip semantics (v1.13.76 → v1.21.44), stale-temp and
stuck-op sweeps, boot deorphan. What it lacked was the brief's unifying
concept: run them NOW, as one action, with a dry-run and a single classified
summary — the difference between "five sweeps fire on five schedules" and "ask
motif how the library actually stands, then let it fix the safe cases".

Scope, deliberately v1:
  * REPAIR = exactly the one class the retry sweep already owns (downloaded,
    canonical present, no placement, no permanent-skip reason, no in-flight
    job). Reused via `_retry_pending_placements(dry_run=...)` — its skip rules
    took eleven tags to mature and are not re-implemented here.
  * REPORT-ONLY = broken canonicals, broken placements, orphaned canonicals
    (no live Plex row via EITHER linkage — guid_tmdb for movies/tv, theme_id
    for collections and synthetic rows; a guid-only join mis-reported healthy
    collections as orphans by 10x on 2026-08-22). The brief's own rules put
    deletion and content-overwrite out of scope for automation.
  * Edition swaps are NOT here — the reaper resolves those inline (v0.51.271
    → .273), and a reconciliation pass cannot see a swap after the fact.
"""
from __future__ import annotations

import logging
from pathlib import Path
from time import monotonic

from .db import get_conn
from .events import log_event
from .plex_enum import verify_canonical_health, verify_placement_health
from .scheduler import _retry_pending_placements

log = logging.getLogger(__name__)

_ORPHAN_SAMPLE_CAP = 10


def run_reconciliation(db_path: Path, themes_dir: Path, *,
                       dry_run: bool = True) -> dict:
    """Verify desired-vs-actual state, repair the safe class, report the rest.

    Returns the summary dict; also writes one events breadcrumb per run (the
    v1.18.5 rule — a run that finds nothing still says so)."""
    t0 = monotonic()
    canonical = verify_canonical_health(db_path, themes_dir)
    placements = verify_placement_health(db_path)
    place_retry = _retry_pending_placements(db_path, dry_run=dry_run)
    with get_conn(db_path) as conn:
        scanned = conn.execute(
            "SELECT COUNT(*) FROM local_files "
            "WHERE file_path IS NOT NULL AND file_path != ''").fetchone()[0]
        broken_canonicals = conn.execute(
            "SELECT COUNT(*) FROM local_files "
            "WHERE canonical_present = 0").fetchone()[0]
        broken_placements = conn.execute(
            "SELECT COUNT(*) FROM placements "
            "WHERE theme_present = 0").fetchone()[0]
        orphan_rows = conn.execute(
            "SELECT lf.media_type, lf.tmdb_id, COALESCE(t.title, '?') AS title "
            "  FROM local_files lf "
            "  LEFT JOIN themes t "
            "    ON t.media_type = lf.media_type AND t.tmdb_id = lf.tmdb_id "
            " WHERE lf.file_path IS NOT NULL AND lf.file_path != '' "
            "   AND NOT EXISTS (SELECT 1 FROM plex_items p "
            "                    WHERE p.section_id = lf.section_id "
            "                      AND (p.guid_tmdb = lf.tmdb_id "
            "                           OR p.theme_id = t.id))"
        ).fetchall()
    orphans = {
        "count": len(orphan_rows),
        "sample": [f"{r['title']} ({r['media_type']}/{r['tmdb_id']})"
                   for r in orphan_rows[:_ORPHAN_SAMPLE_CAP]],
    }
    summary = {
        "dry_run": dry_run,
        "scanned": scanned,
        "canonical": canonical,
        "placements": placements,
        "place_retry": place_retry,
        "broken_canonicals": broken_canonicals,
        "broken_placements": broken_placements,
        "orphans": orphans,
        "duration_s": round(monotonic() - t0, 2),
    }
    verb = "would repair" if dry_run else "repaired (enqueued)"
    log_event(
        db_path, level="INFO", component="reconcile",
        message=(f"Reconciliation{' (dry-run)' if dry_run else ''}: "
                 f"{scanned} canonicals scanned, {verb} "
                 f"{place_retry['candidates'] if dry_run else place_retry['enqueued']}"
                 f" missing placement(s), {broken_canonicals} broken canonical(s), "
                 f"{broken_placements} broken placement(s), "
                 f"{orphans['count']} orphan(s) — report only"),
        detail=summary,
    )
    return summary
