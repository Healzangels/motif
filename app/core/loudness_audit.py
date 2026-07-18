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
import math
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


# v0.51.176: Plex 500s on a theme POST over ~10MB (plex.THEME_UPLOAD_CEILING_BYTES,
# known since v1.21.99). Re-upload is one of only two ways to tell Plex a theme's bytes
# changed — and the other (refresh) is measured DEAD — so this ceiling decides whether
# re-upload is a strategy for this library or just for part of it. Sized here from
# file_size rather than designed around a guess.
_UPLOAD_CEILING_BYTES = 10 * 1024 * 1024


def upload_ceiling_counts(conn) -> dict:
    """How much of the library is out of re-upload's reach.

    Every loudness change AFTER Plex has ingested a theme (the ~2,800 backfill, but also
    every undo / re-apply of a download-normalized theme, forever) needs a propagation
    step. Re-upload is proven but ceiling-capped; the operator's first audition target was
    10.5MB and 500'd. So count the cohort before choosing a mechanism.

    Read-only. `unknown_size` is surfaced rather than folded into `under` — a NULL
    file_size is an unknown, not a small file (class-9: don't let a gap read as a pass)."""
    over = conn.execute(
        "SELECT COUNT(*) FROM local_files "
        "WHERE file_size IS NOT NULL AND file_size > ?", (_UPLOAD_CEILING_BYTES,)
    ).fetchone()[0]
    under = conn.execute(
        "SELECT COUNT(*) FROM local_files "
        "WHERE file_size IS NOT NULL AND file_size <= ?", (_UPLOAD_CEILING_BYTES,)
    ).fetchone()[0]
    unknown = conn.execute(
        "SELECT COUNT(*) FROM local_files WHERE file_size IS NULL"
    ).fetchone()[0]
    biggest = conn.execute(
        "SELECT MAX(file_size) FROM local_files WHERE file_size IS NOT NULL"
    ).fetchone()[0]
    return {
        "ceiling_bytes": _UPLOAD_CEILING_BYTES,
        "over_ceiling": over,
        "under_ceiling": under,
        "unknown_size": unknown,
        "largest_bytes": biggest,
    }


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


# the "clearly too loud" margin — a row more than this many dB above the target gets a
# big, audible reduction. Since these are the LOUDEST rows, they're exactly what a
# loudest-first, max_rows-capped bulk run processes, so the count and the op agree.
_OUTLIER_MARGIN_DB = 6.0


def bulk_normalize_counts(conn, *, ceiling_bytes: int, target: float,
                          margin_db: float = _OUTLIER_MARGIN_DB) -> dict:
    """v0.51.197: counts that MATCH _bulk_normalize_run's eligible set (raw,
    hardlink-placed, measurement-current, under the Plex upload ceiling) so the
    // LEVEL OUTLIERS / // LEVEL LIBRARY buttons can be labeled honestly — the report's
    rep.measured / stats.count OVER-count (they include non-placed + already-normalized
    rows), and rep.loudest is capped at 40.

    `eligible` = the full set a // LEVEL LIBRARY run would touch. `outliers` = the subset
    more than margin_db louder than the target — the worst offenders, and (being the
    loudest) exactly the head a max_rows-capped loudest-first run levels first."""
    # v0.51.205 (audit M3): EXISTS, not a JOIN — a row hardlinked into 2+ media folders
    # (multi-location section / a folder-relocation window) has 2+ placement rows sharing
    # (media_type, tmdb_id, section_id, edition_key), and a JOIN would FAN OUT and count the
    # theme N times. EXISTS is the boolean "is it hardlink-placed?" this predicate means.
    base = (
        "FROM local_files lf "
        "WHERE EXISTS (SELECT 1 FROM placements p "
        "   WHERE p.media_type=lf.media_type AND p.tmdb_id=lf.tmdb_id "
        "     AND p.section_id=lf.section_id AND p.edition_key=lf.edition_key "
        "     AND p.placement_kind='hardlink') "
        "  AND lf.loudness_i IS NOT NULL AND lf.loudness_i > -1e30 "
        "  AND lf.norm_state IS NULL "
        "  AND lf.file_sha256 IS NOT NULL "
        "  AND lf.loudness_measured_sha256 = lf.file_sha256 "
        "  AND lf.file_size IS NOT NULL AND lf.file_size <= ?"
    )
    eligible = conn.execute(f"SELECT COUNT(*) {base}", (ceiling_bytes,)).fetchone()[0]
    outliers = conn.execute(
        f"SELECT COUNT(*) {base} AND lf.loudness_i > ?",
        (ceiling_bytes, target + margin_db),
    ).fetchone()[0]
    # v0.51.199: the leveled count powers the // UNDO ALL LEVELING button — NOT gated by
    # the eligible predicate, because bulk-undo reverses every leveled row regardless of
    # placement/ceiling (undo just restores bytes + re-selects the pre-level entry).
    leveled = conn.execute(
        "SELECT COUNT(*) FROM local_files WHERE norm_state = 'normalized'"
    ).fetchone()[0]
    return {"eligible": eligible, "outliers": outliers, "leveled": leveled,
            "outlier_margin_db": margin_db}


def record_measurement(conn, row, m: dict, measured_at: str) -> int:
    """Stamp one row's loudness measurement onto its local_files PK. Keyed by the
    full (media_type, tmdb_id, section_id, edition_key) PK so an edition's
    measurement never bleeds onto a sibling edition (v1.21.x edition-scope rule).
    Stamps loudness_measured_sha256 from the row's file_sha256 so a later
    re-download (new sha) marks this measurement stale.
    v0.51.205 (audit L3): returns rowcount so the caller can tell a real write from a
    zero-row no-op (the row's PK moved between the select and the write)."""
    cur = conn.execute(
        "UPDATE local_files SET loudness_i = ?, loudness_tp = ?, loudness_lra = ?, "
        "loudness_measured_at = ?, loudness_measured_sha256 = ? "
        "WHERE media_type = ? AND tmdb_id = ? AND section_id = ? AND edition_key = ?",
        (m["loudness_i"], m["true_peak"], m["lra"], measured_at, row["file_sha256"],
         row["media_type"], row["tmdb_id"], row["section_id"], row["edition_key"]),
    )
    return cur.rowcount


# comfortable ambient-hover loudness band. Themes play UNDER browsing, so the
# recommendation never goes louder than -18 LUFS (unobtrusive) nor quieter than -23
# (too faint to hear). Within that band it anchors to the library's own median so it's
# data-aware, not a blind constant.
_REC_CEIL = -18.0   # loudest we'd recommend for hover
_REC_FLOOR = -23.0  # quietest


def _recommended_target(median: float | None) -> float | None:
    """A sensible starting target LUFS from the measured distribution: the library
    median clamped into the comfortable hover band [-23, -18] and rounded to 0.5.

    For a loud library (median > -18, e.g. this one at ~-15.9) it lands at -18 — most
    themes attenuate (always lossless-safe) + the clipping ones get pulled back under
    0 dBTP, without boosting the quiet tail into its peak ceiling. The slider still
    lets the operator override; this is only where it starts."""
    if median is None:
        return None
    m = round(median * 2) / 2
    return max(_REC_FLOOR, min(_REC_CEIL, m))


def _percentile(sorted_vals: list[float], frac: float) -> float | None:
    """Nearest-rank percentile of a pre-sorted list (fine for a diagnostic —
    no interpolation). frac in [0,1]. None on empty."""
    if not sorted_vals:
        return None
    idx = int(round(frac * (len(sorted_vals) - 1)))
    return sorted_vals[max(0, min(idx, len(sorted_vals) - 1))]


def build_report(conn, *, outlier_n: int = 40, bin_width: float = 1.0) -> dict:
    """Aggregate the stored loudness measurements for the report view — the read
    side of the audit (writes nothing). Returns everything the page needs from one
    payload: distribution stats, a fixed-bin histogram, the loudest/quietest N rows
    (with title/year + PK for INFO-card deep-links), and a compact [loudness_i,
    true_peak] array for the client-side target-preview slider (exact per-theme
    counts without shipping identity for every row)."""
    counts = audit_counts(conn)
    ceiling = upload_ceiling_counts(conn)
    rows = conn.execute(
        "SELECT loudness_i, loudness_tp FROM local_files WHERE loudness_i IS NOT NULL"
    ).fetchall()
    # v0.51.163: exclude non-finite loudness_i. A silent theme measured as -inf (by an
    # earlier build, before loudness.py's finite guard) would otherwise crash the
    # histogram (math.floor(-inf) → OverflowError) + serialise to invalid JSON. This
    # makes the report robust to any -inf already sitting in the DB.
    vals = [(r["loudness_i"], r["loudness_tp"]) for r in rows if math.isfinite(r["loudness_i"])]
    loudness = sorted(v[0] for v in vals)

    stats = None
    histogram: list[dict] = []
    if loudness:
        lo = loudness[0]
        hi = loudness[-1]
        stats = {
            "count": len(loudness),
            "min": lo, "max": hi,
            "mean": sum(loudness) / len(loudness),
            "median": _percentile(loudness, 0.5),
            "p10": _percentile(loudness, 0.10),
            "p90": _percentile(loudness, 0.90),
        }
        # fixed-width LUFS bins spanning floor(min)..ceil(max). Themes cluster in a
        # narrow band, so a per-run span keeps the histogram legible.
        start = math.floor(lo / bin_width) * bin_width
        end = math.ceil(hi / bin_width) * bin_width
        n_bins = max(1, int(round((end - start) / bin_width)))
        bins = [0] * n_bins
        for v in loudness:
            b = int((v - start) / bin_width)
            bins[max(0, min(b, n_bins - 1))] += 1
        histogram = [
            {"lo": round(start + i * bin_width, 2),
             "hi": round(start + (i + 1) * bin_width, 2), "count": c}
            for i, c in enumerate(bins)
        ]

    def _outliers(order: str) -> list[dict]:
        q = conn.execute(
            "SELECT lf.media_type, lf.tmdb_id, lf.section_id, lf.edition_key, "
            "       lf.loudness_i, lf.loudness_tp, t.title, t.year "
            "FROM local_files lf "
            "LEFT JOIN themes t ON t.media_type = lf.media_type AND t.tmdb_id = lf.tmdb_id "
            "WHERE lf.loudness_i IS NOT NULL "
            "  AND lf.loudness_i > -1e30 AND lf.loudness_i < 1e30 "   # v0.51.163: exclude ±inf
            f"ORDER BY lf.loudness_i {order} LIMIT ?",
            (outlier_n,),
        ).fetchall()
        return [
            {"media_type": r["media_type"], "tmdb_id": r["tmdb_id"],
             "section_id": r["section_id"], "edition_key": r["edition_key"],
             "title": r["title"] or f'{r["media_type"]}/{r["tmdb_id"]}',
             "year": r["year"], "loudness_i": r["loudness_i"],
             # v0.51.163: a -inf peak on an otherwise-finite row → null (valid JSON).
             "true_peak": (r["loudness_tp"]
                           if r["loudness_tp"] is not None and math.isfinite(r["loudness_tp"])
                           else None)}
            for r in q
        ]

    # LUFS is negative; HIGHER (closer to 0) = LOUDER, lower (more negative) = quieter.
    return {
        "measured": len(loudness),
        "total_local_bytes": counts["total"],
        "unmeasured": max(0, counts["total"] - len(loudness)),
        "skipped_no_sha": counts["skipped_no_sha"],
        "stats": stats,
        # v0.51.176: sizes the propagation constraint — re-upload is capped at ~10MB and
        # refresh is measured dead, so this says how much of the library either mechanism
        # can actually reach.
        "upload_ceiling": ceiling,
        "recommended": _recommended_target(stats["median"] if stats else None),
        "histogram": histogram,
        "loudest": _outliers("DESC"),   # highest LUFS first
        "quietest": _outliers("ASC"),   # lowest (most-negative) LUFS first
        # v0.51.163: guard true_peak too — a -inf tp serialises to invalid JSON
        # ("-Infinity") that the browser's JSON.parse rejects, killing the whole render.
        "values": [[v[0], (v[1] if v[1] is not None and math.isfinite(v[1]) else None)]
                   for v in vals],
    }


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
                _rc = record_measurement(wconn, r, m, now_iso())
                wconn.commit()  # per-row so a crash mid-audit keeps prior measurements
                if _rc:
                    measured += 1
                else:
                    # v0.51.205 (audit L3): the UPDATE matched no row — the PK moved between
                    # the select and the write (a promote / re-key mid-sweep). Don't count a
                    # measurement that landed nowhere (class-9 silent no-op); log so it's visible.
                    log.warning("loudness audit: measurement for %s/%s matched no row (PK moved "
                                "mid-sweep) — not counted", r["media_type"], r["tmdb_id"])
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
