"""
Adoption operations — apply user decisions to scan_findings.

Each scan finding has a `decision` ∈ {pending, adopt, replace, keep_existing}.
The UI surfaces `keep_existing` as "LOCK" — same enum value, clearer label.

  adopt          — hardlink the existing theme.mp3 into motif's themes_dir
                   (or copy if cross-FS), record local_files + placements rows.
                   For orphans, also INSERT a new themes row (with negative
                   tmdb_id if no real ID was resolved).
  replace        — enqueue a download job for this theme's youtube_url, which
                   will overwrite the existing file when placement happens.
                   Only valid for non-orphan findings.
  keep_existing  — (UI label: LOCK) record a user_overrides row marking
                   'manual' provenance, so future placements won't overwrite
                   this theme.

Legacy: pre-v1.6.1 also accepted 'ignore' (a soft "dismiss this finding"
that left no DB side-effects). The CHECK constraint on scan_findings still
permits the value so historical rows stay valid, but the API and UI no
longer expose it — the same effect is implicit by leaving findings in
the 'pending' state.

All operations are recorded as completed by stamping the scan_findings row's
adopt_outcome and adopted_at fields.
"""
from __future__ import annotations

import json
import logging
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .db import get_conn
from .events import log_event, now_iso

log = logging.getLogger(__name__)


class AdoptError(Exception):
    """Failures during adoption — caller decides whether to retry or skip."""


def adopt_finding(db_path: Path, finding_id: int, decision: str,
                  *, decided_by: str, settings) -> dict:
    """Apply a user decision to a scan_finding. Returns the outcome dict
    that gets stored in adopt_outcome and surfaced to the UI."""
    if decision not in ("adopt", "replace", "keep_existing"):
        raise AdoptError(f"unknown decision: {decision}")

    with get_conn(db_path) as conn:
        finding = conn.execute(
            "SELECT * FROM scan_findings WHERE id = ?", (finding_id,)
        ).fetchone()
    if not finding:
        raise AdoptError(f"finding {finding_id} not found")

    # Idempotence: if already adopted, no-op.
    if finding["adopted_at"]:
        return json.loads(finding["adopt_outcome"]) if finding["adopt_outcome"] else {}

    outcome: dict
    try:
        if decision == "adopt":
            outcome = _do_adopt(db_path, finding, settings, decided_by)
        elif decision == "replace":
            outcome = _do_replace(db_path, finding, settings, decided_by)
        elif decision == "keep_existing":
            outcome = _do_keep(db_path, finding, decided_by)
        else:
            raise AdoptError(f"unknown decision: {decision}")
    except Exception as e:
        log.exception("Adopt finding %d failed", finding_id)
        outcome = {"action": "failed", "error": str(e)}

    with get_conn(db_path) as conn:
        conn.execute(
            """UPDATE scan_findings SET
                 decision = ?, decision_at = ?, decision_by = ?,
                 adopt_outcome = ?, adopted_at = ?
               WHERE id = ?""",
            (decision, now_iso(), decided_by,
             json.dumps(outcome), now_iso(), finding_id),
        )

    log_event(db_path, level="INFO", component="adopt",
              media_type=None, tmdb_id=None,
              message=f"Finding {finding_id} resolved: {decision}",
              detail={"finding_id": finding_id, "decision": decision,
                      "outcome": outcome})

    return outcome


def _do_adopt(db_path: Path, finding, settings, decided_by: str) -> dict:
    """Adopt the existing theme.mp3 — hardlink it (or copy if cross-FS) into
    motif's themes_dir and record DB rows."""
    media_type = "movie" if finding["section_type"] == "movie" else "tv"
    finding_kind = finding["finding_kind"]
    theme_id = finding["theme_id"]

    # Resolve target themes_dir for this media type
    target_dir = (settings.movies_themes_dir if media_type == "movie"
                  else settings.tv_themes_dir)
    if not target_dir:
        raise AdoptError("themes_dir not configured")
    target_dir.mkdir(parents=True, exist_ok=True)

    source_path = Path(finding["file_path"])
    if not source_path.is_file():
        raise AdoptError(f"source no longer exists: {source_path}")

    # For orphans, allocate a themes row first
    if finding_kind in ("orphan_resolvable", "orphan_unresolved"):
        theme_id, allocated_tmdb_id = _create_orphan_theme(
            db_path, finding, decided_by,
        )
        media_type = "movie" if finding["section_type"] == "movie" else "tv"
        tmdb_id = allocated_tmdb_id
    else:
        # Look up tmdb_id from existing themes row
        with get_conn(db_path) as conn:
            row = conn.execute(
                "SELECT media_type, tmdb_id FROM themes WHERE id = ?",
                (theme_id,),
            ).fetchone()
            if not row:
                raise AdoptError(f"theme_id {theme_id} not found")
            media_type = row["media_type"]
            tmdb_id = row["tmdb_id"]

    # Decide canonical filename. For orphans without a video_id, use a hash-based
    # name so it's stable. For known themes, use the youtube_video_id if known,
    # otherwise the file's sha256 prefix.
    canonical_name = _canonical_filename(db_path, theme_id, finding["file_sha256"])
    canonical_path = target_dir / canonical_name

    # Place the canonical file. If canonical_path already exists with the same
    # inode as source, nothing to do. Otherwise hardlink (or copy on EXDEV).
    placement_kind = "hardlink"
    try:
        if canonical_path.exists():
            try:
                if canonical_path.stat().st_ino == source_path.stat().st_ino:
                    placement_kind = "hardlink"  # already linked
                else:
                    canonical_path.unlink()
                    os.link(source_path, canonical_path)
            except OSError:
                canonical_path.unlink(missing_ok=True)
                shutil.copy2(source_path, canonical_path)
                placement_kind = "copy"
        else:
            try:
                os.link(source_path, canonical_path)
            except OSError as e:
                if e.errno != 18:  # EXDEV (cross-device)
                    raise
                shutil.copy2(source_path, canonical_path)
                placement_kind = "copy"
    except OSError as e:
        raise AdoptError(f"placement failed: {e}") from e

    # Provenance: hash/exact matches share content with motif's canonical
    # (same sha as what ThemerrDB would produce), so they earn the 'auto'
    # provenance — T badge in the UI. Everything else (orphans, content
    # mismatches) is user-curated and gets 'manual' (M badge).
    provenance = "auto" if finding_kind in ("hash_match", "exact_match") else "manual"

    # Record local_files and placements
    with get_conn(db_path) as conn:
        conn.execute(
            """INSERT OR REPLACE INTO local_files
                 (media_type, tmdb_id, theme_id, file_path, file_sha256,
                  file_size, downloaded_at, source_video_id, provenance)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (media_type, tmdb_id, theme_id,
             str(canonical_path), finding["file_sha256"], finding["file_size"],
             now_iso(), canonical_name.replace(".mp3", ""), provenance),
        )
        conn.execute(
            """INSERT OR REPLACE INTO placements
                 (media_type, tmdb_id, theme_id, media_folder, placed_at,
                  placement_kind, plex_refreshed, provenance)
               VALUES (?, ?, ?, ?, ?, ?, 0, ?)""",
            (media_type, tmdb_id, theme_id,
             finding["media_folder"], now_iso(), placement_kind, provenance),
        )

    return {
        "action": "adopt",
        "media_type": media_type,
        "tmdb_id": tmdb_id,
        "theme_id": theme_id,
        "canonical_path": str(canonical_path),
        "placement_kind": placement_kind,
    }


def _do_replace(db_path: Path, finding, settings, decided_by: str) -> dict:
    """Enqueue a fresh download job for this theme. The existing theme.mp3
    will be overwritten when the worker places the new file."""
    theme_id = finding["theme_id"]
    if theme_id is None:
        raise AdoptError("cannot replace orphan — no upstream record exists")

    with get_conn(db_path) as conn:
        theme = conn.execute(
            "SELECT media_type, tmdb_id, youtube_url FROM themes WHERE id = ?",
            (theme_id,),
        ).fetchone()
        if not theme:
            raise AdoptError(f"theme_id {theme_id} not found")
        if not theme["youtube_url"]:
            raise AdoptError("theme has no youtube_url to download from")

        conn.execute(
            """INSERT INTO jobs (job_type, media_type, tmdb_id, status,
                                 created_at, next_run_at)
               VALUES ('download', ?, ?, 'pending', datetime('now'),
                       datetime('now'))""",
            (theme["media_type"], theme["tmdb_id"]),
        )
    return {
        "action": "replace",
        "theme_id": theme_id,
        "tmdb_id": theme["tmdb_id"],
        "youtube_url": theme["youtube_url"],
        "status": "download enqueued",
    }


def _do_keep(db_path: Path, finding, decided_by: str) -> dict:
    """Mark this theme as a manual override so future placements won't
    overwrite it. The user's existing theme.mp3 stays exactly where it is."""
    theme_id = finding["theme_id"]
    if theme_id is None:
        return {"action": "keep_existing", "note": "orphan kept in place"}

    with get_conn(db_path) as conn:
        theme = conn.execute(
            "SELECT media_type, tmdb_id, youtube_url FROM themes WHERE id = ?",
            (theme_id,),
        ).fetchone()
        if not theme:
            raise AdoptError(f"theme_id {theme_id} not found")
        # An override row keyed on (media_type, tmdb_id) signals manual
        # provenance. We store the existing youtube_url (or empty) so this
        # serves as a "do not auto-replace" marker.
        conn.execute(
            """INSERT OR REPLACE INTO user_overrides
                 (media_type, tmdb_id, theme_id, youtube_url, set_at, set_by, note)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (theme["media_type"], theme["tmdb_id"], theme_id,
             theme["youtube_url"] or "",
             now_iso(), decided_by, "kept existing theme.mp3 from scan"),
        )
    return {
        "action": "keep_existing",
        "theme_id": theme_id,
    }


def _create_orphan_theme(db_path: Path, finding,
                         decided_by: str) -> tuple[int, int]:
    """Insert a new themes row for an orphan finding. Returns (theme_id,
    tmdb_id). For unresolved orphans, allocates a synthetic negative
    tmdb_id starting from -1 going down."""
    metadata = json.loads(finding["resolved_metadata"] or "{}")
    media_type = "movie" if finding["section_type"] == "movie" else "tv"
    title = metadata.get("title") or "Unknown"
    year = metadata.get("year") or ""
    real_tmdb = metadata.get("tmdb_id")
    imdb_id = metadata.get("imdb_id")
    tvdb_id = metadata.get("tvdb_id")

    with get_conn(db_path) as conn:
        if real_tmdb:
            # Check if a row already exists for this real tmdb_id (paranoia
            # — orphan_resolvable said no row, but races happen)
            existing = conn.execute(
                "SELECT id FROM themes WHERE media_type = ? AND tmdb_id = ?",
                (media_type, real_tmdb),
            ).fetchone()
            if existing:
                return existing["id"], real_tmdb
            tmdb_id = real_tmdb
        else:
            # Allocate a new synthetic negative tmdb_id (atomic via single
            # transaction)
            row = conn.execute(
                "SELECT MIN(tmdb_id) AS lo FROM themes WHERE media_type = ? "
                "AND tmdb_id < 0",
                (media_type,),
            ).fetchone()
            min_tmdb = row["lo"] if row and row["lo"] is not None else 0
            tmdb_id = min(min_tmdb, 0) - 1

        cur = conn.execute(
            """INSERT INTO themes
                 (media_type, tmdb_id, tvdb_id, imdb_id, title, year,
                  upstream_source, last_seen_sync_at, first_seen_sync_at)
               VALUES (?, ?, ?, ?, ?, ?, 'plex_orphan', ?, ?)""",
            (media_type, tmdb_id, tvdb_id, imdb_id, title, year,
             now_iso(), now_iso()),
        )
        return cur.lastrowid, tmdb_id


def _canonical_filename(db_path: Path, theme_id: int, file_sha256: str) -> str:
    """Pick a filename for the canonical themes_dir copy.

    Preference: the theme's youtube_video_id if known (consistent with
    auto-downloaded themes). Otherwise: 'orphan_<8-hex>.mp3' from the file's
    sha256."""
    with get_conn(db_path) as conn:
        row = conn.execute(
            "SELECT youtube_video_id FROM themes WHERE id = ?", (theme_id,),
        ).fetchone()
    vid = row and row["youtube_video_id"]
    if vid:
        return f"{vid}.mp3"
    return f"orphan_{file_sha256[:8]}.mp3"
