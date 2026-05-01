"""
v1.12.0: apply decisions to import_findings rows.

A scanner row sits at status='pending' until either:
  - the user clicks APPROVE (or DECLINE / PURGE) on /import
  - matching.auto_place_unambiguous_imports is True and the worker's
    auto-apply pass picks it up after a scan

Approval semantics by match_kind:
  - auto_place        → hardlink canonical → Plex folder, INSERT
                        local_files+placements, source_kind='import'
  - auto_adopt        → no file movement; the existing Plex sidecar
                        already matches our canonical by content. Just
                        record local_files (pointing at our canonical)
                        + placements (pointing at the existing sidecar).
                        source_kind='import'.
  - conflict_overwrite→ same as auto_place but overwrites the existing
                        sidecar; the user already confirmed in the UI
  - conflict_plex_agent→ same as auto_place; user confirmed they want
                        to override Plex's agent theme
  - multi_section     → loop over target_section_ids (which the user
                        may have pruned); apply auto_place semantics
                        per section
  - orphan / flat_layout → APPROVE is not exposed; only DECLINE / PURGE

DECLINE marks the finding declined and leaves files alone.
PURGE deletes the canonical file (and empty parent folder) and
marks the finding purged.
"""
from __future__ import annotations

import json
import logging
import os
import shutil
from pathlib import Path

from .canonical import canonical_theme_subdir
from .db import get_conn, transaction
from .events import log_event, now_iso

log = logging.getLogger(__name__)


class ImportApplyError(Exception):
    """Failure during import application — caller decides what to do."""


def _resolve_placement_target(folder_path: str) -> Path | None:
    """Use plex_enum's translation table to find the container-side
    folder for a Plex-reported path. Returns None if no candidate
    is reachable.
    """
    from .plex_enum import _candidate_local_paths
    for candidate in _candidate_local_paths(folder_path or ""):
        try:
            if candidate.is_dir():
                return candidate
        except OSError:
            continue
    return None


def _hardlink_or_copy(src: Path, dst: Path) -> str:
    """Hardlink src → dst, fall back to copy on cross-FS. Returns
    'hardlink' or 'copy'. If dst already exists with the same inode,
    no-op and returns 'hardlink'."""
    try:
        if dst.exists():
            try:
                if dst.stat().st_ino == src.stat().st_ino:
                    return "hardlink"
            except OSError:
                pass
            dst.unlink()
        dst.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.link(src, dst)
            return "hardlink"
        except OSError as e:
            if e.errno != 18:  # EXDEV
                raise
            shutil.copy2(src, dst)
            return "copy"
    except OSError as e:
        raise ImportApplyError(f"link/copy failed: {e}") from e


def _ensure_theme_row(conn, *, media_type: str, tmdb_id: int | None,
                     imdb_id: str | None,
                     title: str, year: str) -> tuple[int, int]:
    """Find an existing themes row matching the import, or insert a
    new plex_orphan row. Returns (theme_id, tmdb_id).
    """
    if tmdb_id is not None:
        row = conn.execute(
            "SELECT id, tmdb_id FROM themes "
            "WHERE media_type = ? AND tmdb_id = ?",
            (media_type, tmdb_id),
        ).fetchone()
        if row:
            return row["id"], row["tmdb_id"]
    # Allocate a synthetic-negative tmdb_id orphan row, same scheme
    # adopt._create_orphan_theme uses.
    row = conn.execute(
        "SELECT MIN(tmdb_id) AS lo FROM themes "
        "WHERE media_type = ? AND tmdb_id < 0",
        (media_type,),
    ).fetchone()
    min_tmdb = row["lo"] if row and row["lo"] is not None else 0
    new_tmdb = min(min_tmdb, 0) - 1
    cur = conn.execute(
        """INSERT INTO themes
             (media_type, tmdb_id, imdb_id, title, year,
              upstream_source, last_seen_sync_at, first_seen_sync_at)
           VALUES (?, ?, ?, ?, ?, 'plex_orphan', ?, ?)""",
        (media_type, new_tmdb, imdb_id, title, year,
         now_iso(), now_iso()),
    )
    return cur.lastrowid, new_tmdb


def apply_finding(db_path: Path, settings, *,
                  finding_id: int, decision: str,
                  decided_by: str,
                  target_section_ids: list[str] | None = None,
                  ) -> dict:
    """Apply 'approved' / 'declined' / 'purged' to one finding row.
    Returns an outcome dict. Raises ImportApplyError on failure."""
    if decision not in ("approved", "declined", "purged"):
        raise ImportApplyError(f"unknown decision: {decision}")
    if not settings.is_paths_ready():
        raise ImportApplyError("themes_dir not configured")

    with get_conn(db_path) as conn:
        f = conn.execute(
            "SELECT * FROM import_findings WHERE id = ?",
            (finding_id,),
        ).fetchone()
        if f is None:
            raise ImportApplyError(f"finding {finding_id} not found")
        if f["decision"] != "pending":
            return {"action": "noop",
                    "note": f"already {f['decision']}"}

    themes_dir = settings.themes_dir
    canonical = themes_dir / f["file_path"]

    if decision == "declined":
        with get_conn(db_path) as conn:
            conn.execute(
                """UPDATE import_findings SET decision = 'declined',
                       decision_at = ?, decision_by = ?, note = ?
                   WHERE id = ?""",
                (now_iso(), decided_by, "declined by user", finding_id),
            )
        return {"action": "declined"}

    if decision == "purged":
        try:
            if canonical.is_file():
                canonical.unlink()
            try:
                p = canonical.parent
                if p.is_dir() and not any(p.iterdir()):
                    p.rmdir()
            except OSError:
                pass
        except OSError as e:
            log.warning("import purge: unlink %s failed: %s", canonical, e)
        with get_conn(db_path) as conn:
            conn.execute(
                """UPDATE import_findings SET decision = 'purged',
                       decision_at = ?, decision_by = ?, note = ?
                   WHERE id = ?""",
                (now_iso(), decided_by, "purged from themes_dir", finding_id),
            )
        return {"action": "purged"}

    # decision == 'approved'
    if f["match_kind"] in ("orphan", "flat_layout"):
        raise ImportApplyError(
            f"cannot approve a {f['match_kind']} finding — "
            "decline or purge instead",
        )
    if not canonical.is_file():
        raise ImportApplyError(
            f"canonical missing on disk: {canonical} — re-run scan",
        )

    matches = json.loads(f["match_candidates"] or "[]")
    if not matches:
        raise ImportApplyError("no Plex match candidates recorded")

    requested = target_section_ids or json.loads(
        f["target_section_ids"] or "[]"
    )
    if not requested:
        # Default to every candidate
        requested = [m["section_id"] for m in matches]

    # Apply per requested section
    placed_sections: list[dict] = []
    for sid in requested:
        match = next((m for m in matches if m["section_id"] == sid), None)
        if not match:
            continue  # stale target_section_ids, skip
        placed = _apply_one_section(
            db_path, settings,
            finding=f, canonical=canonical,
            match=match, decided_by=decided_by,
        )
        placed_sections.append(placed)

    note = json.dumps({"placed": placed_sections})
    with get_conn(db_path) as conn:
        conn.execute(
            """UPDATE import_findings SET decision = 'approved',
                   decision_at = ?, decision_by = ?, note = ?
               WHERE id = ?""",
            (now_iso(), decided_by, note, finding_id),
        )

    log_event(db_path, level="INFO", component="import",
              message=f"Import finding #{finding_id} applied "
                      f"({f['match_kind']}) by {decided_by}",
              detail={"finding_id": finding_id,
                      "match_kind": f["match_kind"],
                      "placed": placed_sections})
    return {"action": "approved", "placed": placed_sections}


def _apply_one_section(db_path: Path, settings, *,
                      finding, canonical: Path, match: dict,
                      decided_by: str) -> dict:
    """Place one (item, section) — link canonical → Plex folder, write
    DB rows. Used by both approval and the auto-apply pass."""
    section_id = match["section_id"]
    plex_mt = match.get("media_type") or "movie"
    media_type = "tv" if plex_mt == "show" else "movie"
    folder_path = match.get("folder_path") or ""
    plex_folder = _resolve_placement_target(folder_path)
    if plex_folder is None:
        raise ImportApplyError(
            f"could not resolve container path for {folder_path!r}"
        )

    placement_dst = plex_folder / "theme.mp3"
    placement_kind = _hardlink_or_copy(canonical, placement_dst)

    # Resolve / create themes row, then write per-section local_files +
    # placements with source_kind='import'.
    imdb = match.get("guid_imdb") or None
    tmdb_id = match.get("guid_tmdb")
    if tmdb_id is not None:
        try:
            tmdb_id = int(tmdb_id)
        except (TypeError, ValueError):
            tmdb_id = None
    rel_canonical = str(canonical.relative_to(settings.themes_dir))

    with get_conn(db_path) as conn, transaction(conn):
        theme_id, real_tmdb = _ensure_theme_row(
            conn,
            media_type=media_type,
            tmdb_id=tmdb_id,
            imdb_id=imdb,
            title=finding["parsed_title"] or "Unknown",
            year=finding["parsed_year"] or "",
        )
        conn.execute(
            """INSERT OR REPLACE INTO local_files
                 (media_type, tmdb_id, section_id, theme_id, file_path,
                  file_sha256, file_size, downloaded_at,
                  source_video_id, provenance, source_kind)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (media_type, real_tmdb, section_id, theme_id,
             rel_canonical, finding["file_sha256"], finding["file_size"],
             now_iso(),
             # source_video_id is required NOT NULL; for imports we
             # use the file's own sha as a synthetic identifier so it
             # never collides with a yt video id.
             "import_" + (finding["file_sha256"] or "")[:16],
             "manual", "import"),
        )
        conn.execute(
            """INSERT OR REPLACE INTO placements
                 (media_type, tmdb_id, section_id, theme_id, media_folder,
                  placed_at, placement_kind, plex_refreshed, provenance)
               VALUES (?, ?, ?, ?, ?, ?, ?, 0, 'manual')""",
            (media_type, real_tmdb, section_id, theme_id,
             str(plex_folder), now_iso(), placement_kind),
        )
        # Refresh pi.theme_id for immediate library-render coherency
        # (same trick adopt uses to dodge the pre-v1.11.43 stale-cache
        # issue).
        conn.execute(
            """UPDATE plex_items SET theme_id = ?
               WHERE section_id = ?
                 AND (guid_tmdb = ? OR (guid_tmdb IS NULL AND folder_path = ?))""",
            (theme_id, section_id, real_tmdb, folder_path),
        )

    return {
        "section_id": section_id,
        "media_type": media_type,
        "tmdb_id": real_tmdb,
        "theme_id": theme_id,
        "placement_kind": placement_kind,
        "plex_folder": str(plex_folder),
    }


def apply_unambiguous_findings(db_path: Path, settings, *,
                               decided_by: str = "auto") -> int:
    """Auto-apply every pending auto_place / auto_adopt finding.
    Used by the worker's themes_scan job when
    matching.auto_place_unambiguous_imports is enabled. Returns
    the count of findings applied successfully.
    """
    with get_conn(db_path) as conn:
        rows = conn.execute(
            """SELECT id FROM import_findings
               WHERE decision = 'pending'
                 AND match_kind IN ('auto_place','auto_adopt')""",
        ).fetchall()
    n = 0
    for r in rows:
        try:
            apply_finding(db_path, settings,
                          finding_id=r["id"], decision="approved",
                          decided_by=decided_by)
            n += 1
        except ImportApplyError as e:
            log.warning("auto-apply finding %d failed: %s", r["id"], e)
    return n
