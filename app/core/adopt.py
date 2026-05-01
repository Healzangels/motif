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

v1.10.9: split out `adopt_folder()` — a finding-less primitive that takes
a folder_path + plex_item context and runs the same adopt logic without
requiring a scan to have produced a scan_findings row. Used by the inline
ADOPT button on /movies, /tv, /anime row actions.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .canonical import canonical_theme_subdir
from .db import get_conn
from .events import log_event, now_iso

log = logging.getLogger(__name__)


class AdoptError(Exception):
    """Failures during adoption — caller decides whether to retry or skip."""


# ---------------------------------------------------------------------------
# v1.10.9: finding-less primitives
#
# adopt_folder / replace_with_themerrdb let the API call adopt and replace
# semantics directly from a Plex row, without first running a scan to
# populate scan_findings. The scan workflow still works as before — these
# primitives are an additional code path for the inline row buttons.
# ---------------------------------------------------------------------------

_SHA_BUFSIZE = 1024 * 1024  # 1 MiB

def _hash_file(path: Path) -> tuple[str, int]:
    """Return (sha256_hex, file_size). Streams to keep memory bounded."""
    h = hashlib.sha256()
    size = 0
    with path.open("rb") as f:
        while True:
            chunk = f.read(_SHA_BUFSIZE)
            if not chunk:
                break
            h.update(chunk)
            size += len(chunk)
    return h.hexdigest(), size


def adopt_folder(
    db_path: Path, *,
    settings,
    folder_path: str,
    media_type: str,
    title: str,
    year: str,
    section_id: str | None = None,
    imdb_id: str | None = None,
    tmdb_id: int | None = None,
    decided_by: str,
) -> dict:
    """Adopt the theme.mp3 sitting at folder_path/theme.mp3 as a motif-managed
    theme. Builds a synthetic finding-shaped dict and reuses _do_adopt so the
    file-placement and DB-write logic is shared with the /scans flow.

    v1.11.0: section_id determines which Plex section the adoption belongs
    to (the staging file lands under that section's themes_subdir, and the
    local_files / placements rows are keyed by it). When the caller doesn't
    supply one, we resolve via plex_items.folder_path → section_id; if that
    lookup fails too, the adopt is rejected so we never write a row keyed
    to the wrong section.
    """
    if media_type not in ("movie", "tv"):
        raise AdoptError(f"unknown media_type: {media_type}")
    sidecar = Path(folder_path) / "theme.mp3"
    if not sidecar.is_file():
        raise AdoptError(f"no theme.mp3 at {folder_path}")
    sha256, size = _hash_file(sidecar)

    if not section_id:
        plex_type = "show" if media_type == "tv" else "movie"
        with get_conn(db_path) as conn:
            row = conn.execute(
                "SELECT section_id FROM plex_items "
                "WHERE folder_path = ? AND media_type = ? "
                "ORDER BY last_seen_at DESC LIMIT 1",
                (str(folder_path), plex_type),
            ).fetchone()
        if row is None:
            raise AdoptError(
                f"no plex_items row for {folder_path} — cannot infer section_id; "
                "re-run plex_enum or pass section_id explicitly"
            )
        section_id = row["section_id"]

    metadata: dict = {"title": title, "year": year}
    if imdb_id:
        metadata["imdb_id"] = imdb_id
    if tmdb_id is not None:
        metadata["tmdb_id"] = int(tmdb_id)

    section_type = "movie" if media_type == "movie" else "show"
    finding_kind = ("orphan_resolvable"
                    if (imdb_id or tmdb_id is not None)
                    else "orphan_unresolved")

    finding = {
        "section_id": section_id,
        "section_type": section_type,
        "finding_kind": finding_kind,
        "theme_id": None,
        "file_path": str(sidecar),
        "file_sha256": sha256,
        "file_size": size,
        "media_folder": str(folder_path),
        "resolved_metadata": json.dumps(metadata),
    }
    outcome = _do_adopt(db_path, finding, settings, decided_by)
    # v1.10.57: if a previous unmanage / forget recorded a URL for
    # this exact file content (sha256 match), restore it. The newly-
    # adopted row promotes from A → U with the original youtube_url
    # back in user_overrides + themes, source_kind='url'.
    restored = _maybe_restore_url_history(
        db_path,
        media_type=outcome.get("media_type"),
        tmdb_id=outcome.get("tmdb_id"),
        sha256=sha256,
        decided_by=decided_by,
    )
    log_event(db_path, level="INFO", component="adopt",
              media_type=outcome.get("media_type"),
              tmdb_id=outcome.get("tmdb_id"),
              message=f"Inline adopt of sidecar at {folder_path}",
              detail={"folder_path": str(folder_path),
                      "sha256": sha256,
                      "kind": finding_kind,
                      "decided_by": decided_by,
                      "restored_from_history": restored})
    if restored:
        outcome["restored_from_history"] = restored
    return outcome


def _maybe_restore_url_history(
    db_path: Path, *,
    media_type: str | None, tmdb_id: int | None,
    sha256: str, decided_by: str,
) -> dict | None:
    """v1.10.57: look up local_files_history by sha256 and, if a prior
    URL-sourced entry matches, restore the youtube_url onto the
    just-adopted row. Promotes the row from A back to U because the
    file content is byte-identical to what was previously
    URL-downloaded.

    Returns a small dict describing what was restored (for logging),
    or None when there's no usable history match.

    Only acts on history rows whose source_kind was 'url' — for
    'upload' the URL was empty anyway, and for 'themerrdb'/'adopt'
    there's no original-URL semantic to preserve. Picks the most
    recent saved_at when multiple matches exist.
    """
    if not sha256 or media_type is None or tmdb_id is None:
        return None
    with get_conn(db_path) as conn:
        hist = conn.execute(
            """SELECT source_kind, source_video_id, youtube_url, saved_at
               FROM local_files_history
               WHERE file_sha256 = ?
                 AND source_kind = 'url'
                 AND youtube_url IS NOT NULL
                 AND youtube_url != ''
               ORDER BY saved_at DESC
               LIMIT 1""",
            (sha256,),
        ).fetchone()
        if hist is None:
            return None
        url = hist["youtube_url"]
        # Promote: write user_overrides + flip local_files.source_kind
        # to 'url' so the row's badge becomes U instead of A.
        conn.execute(
            """INSERT INTO user_overrides
                 (media_type, tmdb_id, youtube_url, set_at, set_by, note)
               VALUES (?, ?, ?, ?, ?, 'restored from local_files_history on adopt')
               ON CONFLICT(media_type, tmdb_id) DO UPDATE SET
                   youtube_url = excluded.youtube_url,
                   set_at = excluded.set_at,
                   set_by = excluded.set_by,
                   note = excluded.note""",
            (media_type, tmdb_id, url, now_iso(), decided_by),
        )
        conn.execute(
            "UPDATE themes SET youtube_url = ? "
            "WHERE media_type = ? AND tmdb_id = ? "
            "  AND (youtube_url IS NULL OR youtube_url = '')",
            (url, media_type, tmdb_id),
        )
        conn.execute(
            "UPDATE local_files SET source_kind = 'url', source_video_id = ? "
            "WHERE media_type = ? AND tmdb_id = ?",
            (hist["source_video_id"] or "", media_type, tmdb_id),
        )
    return {
        "youtube_url": url,
        "source_kind": "url",
        "history_saved_at": hist["saved_at"],
    }


def replace_with_themerrdb(
    db_path: Path, *,
    media_type: str,
    tmdb_id: int,
    decided_by: str,
    section_id: str | None = None,
) -> dict:
    """Sidecar is currently in place but ThemerrDB has the title — fetch
    motif's authoritative download and overwrite. Cancels any in-flight
    download/place jobs for this item, enqueues a fresh download with
    force_place=true so the worker overwrites the sidecar without
    plex_has_theme guarding it.

    v1.11.0: when section_id is provided the replace targets that one
    section (via a single per-section download job). When omitted we
    enqueue per-section jobs for every included section that owns
    this item — same fan-out semantics as a sync-driven download.
    """
    if media_type not in ("movie", "tv"):
        raise AdoptError(f"unknown media_type: {media_type}")
    with get_conn(db_path) as conn:
        theme = conn.execute(
            "SELECT youtube_url, upstream_source FROM themes "
            "WHERE media_type = ? AND tmdb_id = ?",
            (media_type, tmdb_id),
        ).fetchone()
        if theme is None:
            raise AdoptError(f"no theme row for {media_type}/{tmdb_id}")
        if theme["upstream_source"] == "plex_orphan":
            raise AdoptError("no ThemerrDB record to replace from")
        if not theme["youtube_url"]:
            raise AdoptError("ThemerrDB record has no youtube_url")

        # Cancel any in-flight downloads/places for this item across the
        # affected sections — about to enqueue replacements with force_place.
        if section_id:
            conn.execute(
                """UPDATE jobs SET status = 'cancelled', finished_at = ?
                   WHERE job_type IN ('download','place')
                     AND media_type = ? AND tmdb_id = ? AND section_id = ?
                     AND status IN ('pending','running')""",
                (now_iso(), media_type, tmdb_id, section_id),
            )
            target_sections = [section_id]
        else:
            conn.execute(
                """UPDATE jobs SET status = 'cancelled', finished_at = ?
                   WHERE job_type IN ('download','place')
                     AND media_type = ? AND tmdb_id = ?
                     AND status IN ('pending','running')""",
                (now_iso(), media_type, tmdb_id),
            )
            plex_type = "show" if media_type == "tv" else "movie"
            target_sections = [
                r["section_id"] for r in conn.execute(
                    """SELECT DISTINCT pi.section_id FROM plex_items pi
                       JOIN plex_sections ps ON ps.section_id = pi.section_id
                       WHERE pi.guid_tmdb = ? AND pi.media_type = ?
                         AND ps.included = 1""",
                    (tmdb_id, plex_type),
                ).fetchall()
            ]
        if not target_sections:
            raise AdoptError(
                "no managed Plex sections own this item — enable a section in /settings"
            )
        job_ids: list[int] = []
        for sid in target_sections:
            cur = conn.execute(
                """INSERT INTO jobs (job_type, media_type, tmdb_id, section_id,
                                     payload, status, created_at, next_run_at)
                   VALUES ('download', ?, ?, ?, ?, 'pending', ?, ?)""",
                (media_type, tmdb_id, sid,
                 json.dumps({"reason": "replace_with_themerrdb",
                             "force_place": True}),
                 now_iso(), now_iso()),
            )
            job_ids.append(cur.lastrowid)
    log_event(db_path, level="INFO", component="adopt",
              media_type=media_type, tmdb_id=tmdb_id,
              message="Replace-with-ThemerrDB enqueued",
              detail={"job_ids": job_ids, "section_ids": target_sections,
                      "decided_by": decided_by})
    return {"action": "replace_with_themerrdb", "job_ids": job_ids,
            "section_ids": target_sections,
            "media_type": media_type, "tmdb_id": tmdb_id}


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
    motif's per-section staging dir and record DB rows.

    v1.11.0 layout:
        themes_dir/<plex_sections.themes_subdir>/<Title (Year)>/theme.mp3

    The owning section comes from the scan_findings.section_id (the scanner
    walked exactly that section's location_paths to find this sidecar).
    """
    section_id = finding["section_id"]
    if not section_id:
        raise AdoptError("scan finding missing section_id")
    media_type = "movie" if finding["section_type"] == "movie" else "tv"
    finding_kind = finding["finding_kind"]
    theme_id = finding["theme_id"]

    if not settings.is_paths_ready():
        raise AdoptError("themes_dir not configured")
    with get_conn(db_path) as conn:
        sec = conn.execute(
            "SELECT themes_subdir FROM plex_sections WHERE section_id = ?",
            (section_id,),
        ).fetchone()
    if sec is None or not sec["themes_subdir"]:
        raise AdoptError(
            f"section {section_id} has no themes_subdir — re-discover sections in /settings"
        )
    media_root = settings.section_themes_dir_by_subdir(sec["themes_subdir"])
    if not media_root:
        raise AdoptError("themes_dir not configured")

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
        with get_conn(db_path) as conn:
            row = conn.execute(
                "SELECT media_type, tmdb_id FROM themes WHERE id = ?",
                (theme_id,),
            ).fetchone()
            if not row:
                raise AdoptError(f"theme_id {theme_id} not found")
            media_type = row["media_type"]
            tmdb_id = row["tmdb_id"]

    # Resolve title+year for the canonical subdir from the themes row.
    with get_conn(db_path) as conn:
        meta = conn.execute(
            "SELECT title, year FROM themes WHERE id = ?", (theme_id,),
        ).fetchone()
    title = (meta and meta["title"]) or ""
    year = (meta and meta["year"]) or ""
    out_dir = media_root / canonical_theme_subdir(title, year)
    out_dir.mkdir(parents=True, exist_ok=True)
    canonical_path = out_dir / "theme.mp3"

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
    # mismatches) is user-curated and gets 'manual'.
    provenance = "auto" if finding_kind in ("hash_match", "exact_match") else "manual"
    # source_kind drives the T/U/A badge unambiguously (v1.10.12). Hash
    # and exact matches are byte-identical with what ThemerrDB would
    # download, so 'themerrdb'. Everything else (orphan_* findings,
    # content_mismatch) is the user adopting a local file as the source
    # of truth — 'adopt'.
    source_kind = ("themerrdb"
                   if finding_kind in ("hash_match", "exact_match")
                   else "adopt")

    # source_video_id: keep the previous convention (yt id when known,
    # else hash-based) for back-compat with row badge heuristics, but
    # decouple from the on-disk filename which is now always theme.mp3.
    source_vid = _canonical_filename(
        db_path, theme_id, finding["file_sha256"],
    ).replace(".mp3", "")
    # file_path stored relative to themes_dir, matching the worker
    # download path. Pre-1.10.13 stored absolute, which broke the
    # canonical-layout migration on existing installs.
    rel_path = str(canonical_path.relative_to(settings.themes_dir))
    with get_conn(db_path) as conn:
        conn.execute(
            """INSERT OR REPLACE INTO local_files
                 (media_type, tmdb_id, section_id, theme_id, file_path,
                  file_sha256, file_size, downloaded_at, source_video_id,
                  provenance, source_kind)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (media_type, tmdb_id, section_id, theme_id,
             rel_path, finding["file_sha256"], finding["file_size"],
             now_iso(), source_vid, provenance, source_kind),
        )
        conn.execute(
            """INSERT OR REPLACE INTO placements
                 (media_type, tmdb_id, section_id, theme_id, media_folder,
                  placed_at, placement_kind, plex_refreshed, provenance)
               VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)""",
            (media_type, tmdb_id, section_id, theme_id,
             finding["media_folder"], now_iso(), placement_kind, provenance),
        )
        # v1.10.50: implicit ack — adopting a sidecar is the user
        # routing around whatever was broken on the TDB side. Keep
        # failure_kind so the TDB pill stays red but stop alerting.
        # No-op when there's no failure to ack.
        conn.execute(
            "UPDATE themes SET failure_acked_at = ? "
            "WHERE media_type = ? AND tmdb_id = ? AND failure_kind IS NOT NULL",
            (now_iso(), media_type, tmdb_id),
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
