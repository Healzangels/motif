"""v0.51.158: read-only LOUDNESS AUDIT sweep (Phase 0 of the loudness feature).

Walks motif's local-bytes themes, measures each with ffmpeg's EBU R128 loudnorm
analysis (loudness.measure_loudness — read-only, no re-encode, no file written),
and stores the result on the local_files row (schema v72 columns). The report view
(a later tag) reads those stored columns to show the spread + outliers + a
target-preview; this module just fills them in.

**Scope = every local_files row.** A local_files row IS motif's canonical downloaded
/adopted file, so every row has local bytes to measure (T/U/A/M + plex_cloud backups).
P (Plex-served-only) rows have NO local_files row at all → excluded for free, exactly
the plan's scope. No source_kind allowlist needed (it would only risk silently
skipping a legacy NULL-source_kind row that does have bytes).

**sha256-keyed / incremental.** A row whose stored loudness_measured_sha256 already
equals its current file_sha256 was measured at these exact bytes → skipped. Only
never-measured rows + rows whose file changed (re-download / UPLOAD MP3 → new
file_sha256) are (re)measured, so re-running the audit is cheap.

Mirrors orphan_scan.py — a DB-walk module over a leaf primitive, driven by the
in-memory background op in api.py. Measurement is blocking (subprocess) so the
runner fans it out over a ThreadPoolExecutor (each measure is its own ffmpeg
process); the ONLY DB writes are the measurements, done serially on the main thread.
"""
from __future__ import annotations

import concurrent.futures
import logging
from pathlib import Path

from .events import now_iso
from .loudness import measure_loudness

log = logging.getLogger("motif.loudness_audit")

# ffmpeg is a separate process per measure, so a few in flight overlap cleanly
# without GIL contention. Kept modest — a homelab container has few cores and this
# is a background diagnostic, not a race.
_DEFAULT_MAX_WORKERS = 3


def _resolve(themes_dir: Path | None, rel_or_abs: str) -> Path:
    """local_files.file_path is RELATIVE to settings.themes_dir (CLAUDE.md
    file-path convention) — join unless already absolute (legacy rows)."""
    p = Path(rel_or_abs)
    if p.is_absolute() or themes_dir is None:
        return p
    return themes_dir / rel_or_abs


def rows_needing_measure(conn, *, remeasure_all: bool = False) -> list:
    """local_files rows whose loudness needs (re)measuring.

    Requires file_sha256 (the stale-detect key; the v1.19.18 backfill left ~none
    NULL — those few are surfaced as skipped_no_sha, not silently dropped). Skips a
    row already measured at its current bytes unless remeasure_all forces a full
    re-run. Ordered stably so progress + any partial run are deterministic."""
    sha_gate = (
        "" if remeasure_all
        else " AND (loudness_measured_sha256 IS NULL "
             "OR loudness_measured_sha256 != file_sha256)"
    )
    return conn.execute(
        "SELECT media_type, tmdb_id, section_id, edition_key, file_path, file_sha256 "
        "FROM local_files "
        "WHERE file_path IS NOT NULL AND file_path != '' "
        "  AND file_sha256 IS NOT NULL" + sha_gate + " "
        "ORDER BY media_type, tmdb_id, section_id, edition_key"
    ).fetchall()


def audit_counts(conn) -> dict:
    """Universe sizing for the audit summary — total measurable rows, how many
    already carry a current measurement, how many lack a sha (can't stale-detect)."""
    total = conn.execute(
        "SELECT COUNT(*) FROM local_files "
        "WHERE file_path IS NOT NULL AND file_path != '' AND file_sha256 IS NOT NULL"
    ).fetchone()[0]
    current = conn.execute(
        "SELECT COUNT(*) FROM local_files "
        "WHERE file_sha256 IS NOT NULL AND loudness_measured_sha256 = file_sha256"
    ).fetchone()[0]
    no_sha = conn.execute(
        "SELECT COUNT(*) FROM local_files "
        "WHERE file_path IS NOT NULL AND file_path != '' AND file_sha256 IS NULL"
    ).fetchone()[0]
    return {"total": total, "already_current": current, "skipped_no_sha": no_sha}


def record_measurement(conn, row, m: dict, measured_at: str) -> None:
    """Stamp one row's loudness measurement onto its local_files PK. Keyed by the
    full (media_type, tmdb_id, section_id, edition_key) PK so an edition's
    measurement never bleeds onto a sibling edition (v1.21.x edition-scope rule).
    Stamps loudness_measured_sha256 from the row's file_sha256 so a later
    re-download (new sha) marks this measurement stale."""
    conn.execute(
        "UPDATE local_files SET loudness_i = ?, loudness_tp = ?, loudness_lra = ?, "
        "loudness_measured_at = ?, loudness_measured_sha256 = ? "
        "WHERE media_type = ? AND tmdb_id = ? AND section_id = ? AND edition_key = ?",
        (m["loudness_i"], m["true_peak"], m["lra"], measured_at, row["file_sha256"],
         row["media_type"], row["tmdb_id"], row["section_id"], row["edition_key"]),
    )


def run_loudness_audit(
    db_path, themes_dir: Path | None, *,
    progress_cb=None, max_workers: int = _DEFAULT_MAX_WORKERS,
    remeasure_all: bool = False,
) -> dict:
    """Measure every local-bytes row that needs it + store the result. Read-only wrt
    the FILE (measures without writing a byte); the only DB writes are the
    measurements. Returns a summary dict. progress_cb(done, total) fires per row so
    the background op can surface live "X / N measured"."""
    from .db import get_conn

    with get_conn(db_path) as conn:
        rows = rows_needing_measure(conn, remeasure_all=remeasure_all)
        counts = audit_counts(conn)

    total = len(rows)
    if progress_cb is not None:
        try:
            progress_cb(0, total)
        except Exception:  # noqa: BLE001 — cosmetic callback, never abort the sweep
            pass

    measured = 0
    failed = 0
    # Measure in parallel (subprocess, the slow part); write serially on this thread
    # so no sqlite connection is shared across threads (they aren't reusable).
    with get_conn(db_path) as wconn, concurrent.futures.ThreadPoolExecutor(
        max_workers=max_workers
    ) as ex:
        fut_to_row = {
            ex.submit(measure_loudness, str(_resolve(themes_dir, r["file_path"]))): r
            for r in rows
        }
        done = 0
        for fut in concurrent.futures.as_completed(fut_to_row):
            r = fut_to_row[fut]
            done += 1
            try:
                m = fut.result()
            except Exception as e:  # noqa: BLE001 — a bad row must not sink the sweep
                log.warning("loudness audit: measure raised for %s/%s: %s",
                            r["media_type"], r["tmdb_id"], e)
                m = None
            if m is not None:
                record_measurement(wconn, r, m, now_iso())
                wconn.commit()  # per-row so a crash mid-audit keeps prior measurements
                measured += 1
            else:
                failed += 1
            if progress_cb is not None:
                try:
                    progress_cb(done, total)
                except Exception:  # noqa: BLE001
                    pass

    summary = {
        "to_measure": total,
        "measured": measured,
        "failed": failed,
        "total_local_bytes": counts["total"],
        "already_current": counts["already_current"],
        "skipped_no_sha": counts["skipped_no_sha"],
    }
    log.info("loudness audit complete: measured=%d failed=%d of %d needing "
             "(universe=%d, already-current=%d, no-sha=%d)",
             measured, failed, total, counts["total"],
             counts["already_current"], counts["skipped_no_sha"])
    return summary
