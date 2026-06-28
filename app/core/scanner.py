"""
Plex folder scanner — find existing theme.mp3 files and reconcile against motif's DB.

Triggered on-demand via /coverage's "// SCAN PLEX FOLDERS" button. Walks each
managed Plex section's location_paths, finds `theme.mp3` files in immediate
subfolders, and classifies each finding for user triage:

  exact_match       — file is hardlinked to motif's canonical themes_dir copy.
                      Same inode. Nothing to do.
  hash_match        — same content as a known motif file but separate inode.
                      Adoptable: re-hardlink and now they share an inode.
  content_mismatch  — folder has a theme.mp3 but motif has a DIFFERENT canonical
                      file for this item. User must choose KEEP or REPLACE.
  orphan_resolvable — no themes row matches by title+year, but we have nfo
                      and/or TVDB metadata to resolve to a real ID.
  orphan_unresolved — no themes row, no metadata. Synthetic negative tmdb_id
                      gets allocated when the user adopts.

Performance:
  - SHA256 of a typical 1-3 MB theme.mp3 is well under 100ms.
  - Skip files whose (path, mtime, size) tuple matches a previous scan_findings
    row — copy hash forward without re-reading. So a re-scan only re-hashes
    files that have actually been modified.

Concurrency:
  - The worker thread is single-threaded, so the scan runs as a worker job
    of type='scan'. While it runs, no place/relink jobs run (they share the
    worker). A scan can be cancelled mid-flight by setting status='cancelled'
    on the scan_runs row; the scanner checks every N folders.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .db import get_conn
from .events import log_event, now_iso
from .nfo import find_nfo, parse_nfo
from .normalize import normalize_title, titles_equal
from .placement import parse_folder_name
from .sections import list_sections
from .tmdb import TMDBClient

log = logging.getLogger(__name__)

THEME_FILE_NAME = "theme.mp3"
HASH_BUFFER_SIZE = 1024 * 1024


@dataclass
class ScanContext:
    """Per-scan state shared across helpers."""
    db_path: Path
    scan_run_id: int
    themes_dir: Path
    plus_mode: str
    tmdb: TMDBClient
    cancel_check: callable  # () -> bool, returns True if scan should abort

    def is_cancelled(self) -> bool:
        return self.cancel_check()


def run_scan(db_path: Path, settings, *, initiated_by: str = "system",
             cancel_check=lambda: False) -> int:
    """Top-level entrypoint. Creates a scan_runs row, walks each managed
    section, returns the scan_run_id. Runs synchronously in the worker
    thread.
    """
    if not settings.is_paths_ready():
        raise RuntimeError("themes_dir not configured")

    started = now_iso()
    with get_conn(db_path) as conn:
        cur = conn.execute(
            "INSERT INTO scan_runs (started_at, status, initiated_by) "
            "VALUES (?, 'running', ?)",
            (started, initiated_by),
        )
        scan_run_id = cur.lastrowid

    log_event(db_path, level="INFO", component="scan",
              message=f"Scan started by {initiated_by}",
              detail={"scan_run_id": scan_run_id})

    tmdb = TMDBClient(settings.tmdb_api_key or None, db_path)
    ctx = ScanContext(
        db_path=db_path,
        scan_run_id=scan_run_id,
        themes_dir=settings.themes_dir,
        plus_mode=settings.plus_equiv_mode,
        tmdb=tmdb,
        cancel_check=cancel_check,
    )

    sections_scanned = 0
    folders_walked = 0
    themes_found = 0
    findings_count = 0
    error: Optional[str] = None

    try:
        sections = list_sections(db_path)
        for section in sections:
            if not section.get("included"):
                continue
            if ctx.is_cancelled():
                break

            for path_str in section.get("location_paths") or []:
                if ctx.is_cancelled():
                    break
                path = Path(path_str)
                if not path.exists():
                    log.warning("Section path does not exist: %s", path)
                    continue
                walked, found, fc = _scan_section_path(ctx, section, path)
                folders_walked += walked
                themes_found += found
                findings_count += fc
            sections_scanned += 1

        with get_conn(db_path) as conn:
            conn.execute(
                """UPDATE scan_runs SET
                     finished_at = ?, status = ?,
                     sections_scanned = ?, folders_walked = ?,
                     themes_found = ?, findings_count = ?
                   WHERE id = ?""",
                (now_iso(),
                 "cancelled" if ctx.is_cancelled() else "complete",
                 sections_scanned, folders_walked, themes_found, findings_count,
                 scan_run_id),
            )
    except Exception as e:
        log.exception("Scan %d failed", scan_run_id)
        error = str(e)
        with get_conn(db_path) as conn:
            conn.execute(
                """UPDATE scan_runs SET finished_at = ?, status = 'failed',
                       sections_scanned = ?, folders_walked = ?,
                       themes_found = ?, findings_count = ?, error = ?
                   WHERE id = ?""",
                (now_iso(), sections_scanned, folders_walked,
                 themes_found, findings_count, error, scan_run_id),
            )
        raise
    finally:
        log_event(db_path, level="INFO", component="scan",
                  message=f"Scan {scan_run_id} done — "
                          f"{folders_walked} folders walked, "
                          f"{themes_found} themes found, "
                          f"{findings_count} findings",
                  detail={"scan_run_id": scan_run_id, "error": error})

    return scan_run_id


def _scan_section_path(ctx: ScanContext, section: dict,
                       root: Path) -> tuple[int, int, int]:
    """Walk a single root path, looking for theme.mp3 in immediate subfolders.
    Returns (folders_walked, themes_found, findings_recorded)."""
    folders_walked = 0
    themes_found = 0
    findings = 0

    section_id = section["section_id"]
    section_type = section["type"]   # 'movie' or 'show'

    try:
        entries = sorted(root.iterdir())
    except OSError as e:
        log.warning("Cannot iterate %s: %s", root, e)
        return folders_walked, themes_found, findings

    for media_folder in entries:
        if ctx.is_cancelled():
            break
        if not media_folder.is_dir():
            continue
        folders_walked += 1

        # Cooperative cancellation every 100 folders
        if folders_walked % 100 == 0 and ctx.is_cancelled():
            break

        theme_file = media_folder / THEME_FILE_NAME
        if not theme_file.is_file():
            continue
        themes_found += 1

        try:
            finding_recorded = _classify_and_record(
                ctx, section_id, section_type, media_folder, theme_file,
            )
            if finding_recorded:
                findings += 1
        except OSError as e:
            log.warning("Failed to process %s: %s", theme_file, e)
            continue

    return folders_walked, themes_found, findings


def _classify_and_record(ctx: ScanContext, section_id: str, section_type: str,
                          media_folder: Path, theme_file: Path) -> bool:
    """Hash the file, classify, and write a scan_findings row. Returns True
    if a finding was recorded."""
    try:
        st = theme_file.stat()
    except OSError as e:
        # v1.20.63 (class-9): a theme file present on the walk but
        # un-stattable (perm error, vanished between walk and stat) was
        # dropped from scan findings with no breadcrumb. Mirror the
        # hash-OSError warning just below + the v1.18.97 site at ~445.
        log.warning("scanner: stat() failed on %s — skipping: %s",
                    theme_file, e)
        return False
    file_size = st.st_size
    file_mtime = datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).isoformat()
    file_inode = st.st_ino

    # Hash skip: if a previous scan recorded this exact (path, mtime, size),
    # reuse its hash and finding_kind without re-reading the file.
    file_sha256: Optional[str] = None
    with get_conn(ctx.db_path) as conn:
        prev = conn.execute(
            """SELECT file_sha256 FROM scan_findings
               WHERE file_path = ? AND file_size = ? AND file_mtime = ?
               ORDER BY id DESC LIMIT 1""",
            (str(theme_file), file_size, file_mtime),
        ).fetchone()
        if prev:
            file_sha256 = prev["file_sha256"]

    if not file_sha256:
        try:
            file_sha256 = _sha256_of(theme_file)
        except OSError as e:
            log.warning("Cannot hash %s: %s", theme_file, e)
            return False

    # Now classify. Resolve folder name → media metadata.
    parsed = parse_folder_name(media_folder.name)
    title_norm = normalize_title(parsed.title, ctx.plus_mode)
    media_type = "movie" if section_type == "movie" else "tv"

    finding_kind, theme_id, resolved = _classify(
        ctx, media_type, parsed, title_norm, theme_file, file_inode, file_sha256,
        media_folder,
    )

    # Insert the scan_findings row + flag the plex_items row so the
    # library's SRC badge reflects the freshly-discovered sidecar
    # immediately. Without this update, an anime row with a local
    # theme.mp3 would render as P (Plex agent) until the next plex_enum
    # caught up: pi.local_theme_file=0 + pi.has_theme=1 from Plex's
    # local-media-assets agent picking up the sidecar = the M-vs-P
    # discriminator in app.js falls into the P branch.
    with get_conn(ctx.db_path) as conn:
        conn.execute(
            """INSERT INTO scan_findings (
                scan_run_id, section_id, section_type,
                media_folder, file_path, file_size, file_mtime, file_sha256,
                finding_kind, theme_id, resolved_metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (ctx.scan_run_id, section_id, section_type,
             str(media_folder), str(theme_file), file_size, file_mtime, file_sha256,
             finding_kind, theme_id,
             json.dumps(resolved) if resolved else None),
        )
        # v1.15.92: multi-candidate folder_path UPDATE. Same path-domain
        # mismatch class as v1.15.90/v1.15.91: on Unraid the scanner
        # iterates CONTAINER paths (/data/media/movies/X) while plex_items.
        # folder_path stores Plex's reported HOST path (/mnt/user/movies/X).
        # The pre-v1.15.92 single-candidate WHERE missed the row, so
        # motif's `local_theme_file` flag stayed stale until plex_enum
        # could re-stat the folder — which itself fails indeterminately
        # on Unraid mounts that _candidate_local_paths' translation
        # table doesn't cover.
        #
        # Strategy: try theme_id first (canonical FK; covers most rows
        # after resolve_theme_ids runs); fall back to folder_path
        # matching against EVERY candidate the path could be (the
        # container path the scanner saw + each REVERSE-translated
        # host-path form). The reverse iterates _PATH_PREFIX_TRANSLATIONS
        # but inverts the direction: if the container path starts with
        # dst, prepend src to produce the candidate host path Plex would
        # have reported.
        from .plex_enum import _PATH_PREFIX_TRANSLATIONS
        folder_str = str(media_folder)
        folder_candidates = [folder_str]
        for src, dst in _PATH_PREFIX_TRANSLATIONS:
            if folder_str.startswith(dst):
                folder_candidates.append(
                    src + folder_str[len(dst):]
                )
        # Dedup while preserving order.
        seen = set()
        deduped_candidates = [
            c for c in folder_candidates
            if not (c in seen or seen.add(c))
        ]
        qmarks = ",".join("?" for _ in deduped_candidates)
        # v1.22.22 (edition audit): theme_id is title-wide (themes is keyed by
        # media_type+tmdb_id, identical across editions), so the bare
        # `theme_id = ?` disjunct flipped local_theme_file=1 on EVERY edition's
        # plex_items row in the section when a sidecar was found in just ONE
        # edition's folder — a sibling with no sidecar falsely rendered M until
        # the next plex_enum. Scope the theme_id branch to THIS folder's
        # edition; the folder_path branch is already edition-specific (distinct
        # {edition-X} folders).
        from .editions import edition_key_for_folder
        _scan_edition = edition_key_for_folder(folder_str)
        if theme_id is not None:
            # Natural-key first (most reliable post-resolve_theme_ids).
            cur = conn.execute(
                f"UPDATE plex_items SET local_theme_file = 1 "
                f"WHERE ((theme_id = ? AND edition_key = ?) "
                f"       OR folder_path IN ({qmarks})) "
                f"  AND section_id = ?",
                (theme_id, _scan_edition, *deduped_candidates, section_id),
            )
        else:
            # Orphan / unclassified: theme_id not set, fall back to
            # multi-candidate folder_path. Still better than the
            # pre-fix single-candidate WHERE because we try every
            # plausible host-path form for Unraid setups.
            cur = conn.execute(
                f"UPDATE plex_items SET local_theme_file = 1 "
                f"WHERE folder_path IN ({qmarks}) AND section_id = ?",
                (*deduped_candidates, section_id),
            )
        # v1.15.107: breadcrumb when the multi-candidate UPDATE
        # matched zero rows. Pre-fix this fired silently, leaving
        # operators with an Unraid mount mismatch (host path vs
        # container path divergent in a way we don't yet handle)
        # to debug from outside. The scan-finding stage already
        # succeeded — local_files knows about the theme — but
        # plex_items.local_theme_file stays 0 until the next
        # plex_enum cycle. Logging at debug keeps the default
        # log clean while leaving a trail for `--log-level=debug`
        # operators chasing path-domain mismatches. Sibling fix
        # to v1.15.90/.91/.92/.106 phantom-M class.
        if cur.rowcount == 0:
            log.debug(
                "scanner: multi-candidate plex_items UPDATE matched "
                "0 rows for section=%s candidates=%r theme_id=%s — "
                "Unraid path-domain mismatch?",
                section_id, deduped_candidates, theme_id,
            )
    return True


def _classify(ctx: ScanContext, media_type: str, parsed, title_norm: str,
              theme_file: Path, file_inode: int, file_sha256: str,
              media_folder: Path) -> tuple[str, Optional[int], Optional[dict]]:
    """Return (finding_kind, theme_id_or_none, resolved_metadata_dict_or_none)."""

    with get_conn(ctx.db_path) as conn:
        # Hash-first match: any local_files row with the same sha means
        # motif already manages this exact audio content. Always classify
        # as exact_match regardless of inode — the file is "managed",
        # which is what the user cares about. Inode-mismatch (copy
        # instead of hardlink) is a storage-efficiency issue surfaced via
        # the Dash COPIES card, not something the user needs to act on
        # from /scans. Pre-1.9.5 this returned hash_match for the
        # different-inode case, which made motif-uploaded files re-pester
        # the user as adoptable on every fresh scan.
        hash_row = conn.execute(
            """SELECT lf.media_type, lf.tmdb_id, t.id AS theme_id
               FROM local_files lf
               JOIN themes t
                 ON t.media_type = lf.media_type AND t.tmdb_id = lf.tmdb_id
               WHERE lf.media_type = ? AND lf.file_sha256 = ?
               LIMIT 1""",
            (media_type, file_sha256),
        ).fetchone()
        if hash_row:
            return "exact_match", hash_row["theme_id"], None

        # Look up candidates by year first (cheap filter), then narrow by
        # normalized-title match in Python so we catch things like
        # "(500) Days of Summer" against "500 Days of Summer". Falling back
        # to title-only when the folder lacks a year.
        candidate = None
        candidate_rows: list = []
        if parsed.year:
            # Themes for this year (or null year — sometimes ThemerrDB lacks it)
            candidate_rows = conn.execute(
                """SELECT id, media_type, tmdb_id, title, year, youtube_video_id
                   FROM themes
                   WHERE media_type = ? AND (year = ? OR year IS NULL OR year = '')""",
                (media_type, parsed.year),
            ).fetchall()
        if not candidate_rows:
            # No year → consider every theme of this type
            candidate_rows = conn.execute(
                """SELECT id, media_type, tmdb_id, title, year, youtube_video_id
                   FROM themes WHERE media_type = ?""",
                (media_type,),
            ).fetchall()

        for row in candidate_rows:
            if titles_equal(row["title"] or "", parsed.title, ctx.plus_mode):
                # Prefer rows whose year matches the folder year exactly
                if parsed.year and row["year"] == parsed.year:
                    candidate = row
                    break
                if candidate is None:
                    candidate = row
                # keep scanning in case a year-exact match shows up later

        if candidate:
            theme_id = candidate["id"]
            tmdb_id = candidate["tmdb_id"]
            # v1.23.98 (audit): scope the canonical lookup to the SCANNED folder's
            # edition (media_folder carries the {edition-X} tag), falling back to
            # the '' standard, preferring the exact edition. Pre-fix this
            # fetchone()'d an ARBITRARY edition's local_files row on a multi-
            # edition title — so a sidecar in an edition folder motif does NOT yet
            # track (no own canonical) read another edition's canonical and got
            # classified content_mismatch (a false /scans conflict) instead of
            # hash_match (adoptable as THIS edition's canonical). The v1.22.22 #4
            # read-side twin: that fix edition-scoped the plex_items UPDATE in
            # _classify_and_record; this read was missed.
            from .editions import edition_key_for_folder
            _scan_edition = edition_key_for_folder(str(media_folder))
            local = conn.execute(
                """SELECT file_path, file_sha256 FROM local_files
                   WHERE media_type = ? AND tmdb_id = ?
                     AND edition_key IN (?, '')
                   ORDER BY (edition_key = ?) DESC
                   LIMIT 1""",
                (media_type, tmdb_id, _scan_edition, _scan_edition),
            ).fetchone()

            if local and local["file_path"]:
                # local_files.file_path can be either:
                #   - absolute (legacy adopt.py rows pre-v1.5.6)
                #   - relative to themes_dir (worker downloads + post-v1.5.6
                #     canonical layout migration)
                # Resolve against themes_dir if relative so the inode
                # comparison below actually finds the file.
                local_path = Path(local["file_path"])
                if not local_path.is_absolute():
                    local_path = ctx.themes_dir / local_path
                # Compare hashes if available
                if local["file_sha256"] and local["file_sha256"] == file_sha256:
                    # Same content. Check inode to distinguish exact_match
                    # (already hardlinked) vs hash_match (adoptable).
                    try:
                        local_inode = local_path.stat().st_ino
                        if local_inode == file_inode:
                            return "exact_match", theme_id, None
                    except OSError as e:
                        # v1.18.97: silent-fail audit (class 9). Pre-fix
                        # this OSError swallow fell through to hash_match
                        # classification with no log breadcrumb. If the
                        # canonical is actually hardlinked but we can't
                        # stat it (perm issue, race with rename, missing
                        # file), the caller treats it as adoptable and
                        # may create a duplicate placement. Operator-
                        # blind pre-fix. Fall-through behavior preserved
                        # (still fails to hash_match — safer than
                        # synthesizing exact_match without confirmation);
                        # this just surfaces the failure mode in docker
                        # logs so a real classification gap can be
                        # diagnosed instead of silently accumulating.
                        log.warning(
                            "scanner: stat() failed on canonical %s "
                            "while distinguishing exact_match vs "
                            "hash_match (theme_id=%s media=%s/%s): %s "
                            "— falling back to hash_match. Verify the "
                            "file exists + readable; a persistent "
                            "failure here will cause duplicate-"
                            "placement attempts on the next adopt.",
                            local_path, theme_id, media_type, tmdb_id, e,
                        )
                    return "hash_match", theme_id, None
                else:
                    return "content_mismatch", theme_id, None
            else:
                # Theme record exists but motif has no canonical file yet.
                # Adoptable as the canonical for this item.
                return "hash_match", theme_id, None

        # No themes row matches — orphan path
        return _classify_orphan(ctx, media_type, parsed, theme_file, media_folder)


def _classify_orphan(ctx: ScanContext, media_type: str, parsed,
                     theme_file: Path,
                     media_folder: Path) -> tuple[str, Optional[int], Optional[dict]]:
    """No upstream record for this folder. Try nfo first, then TVDB.
    Returns ('orphan_resolvable' or 'orphan_unresolved', theme_id=None, metadata)."""
    nfo_kind = "movie" if media_type == "movie" else "tv"
    nfo_path = find_nfo(media_folder, nfo_kind)
    metadata: dict = {
        "source": "unresolved",
        "title": parsed.title,
        "year": parsed.year,
        "tmdb_id": None,
        "imdb_id": None,
        "tvdb_id": None,
    }

    if nfo_path:
        nfo_md = parse_nfo(nfo_path, nfo_kind)
        if nfo_md and nfo_md.has_any_id():
            metadata.update({
                "source": "nfo",
                "title": nfo_md.title or parsed.title,
                "year": nfo_md.year or parsed.year,
                "tmdb_id": nfo_md.tmdb_id,
                "imdb_id": nfo_md.imdb_id,
                "tvdb_id": nfo_md.tvdb_id,
            })
            return "orphan_resolvable", None, metadata

    # Fall back to TMDB if API key is configured (free; was TVDB pre-1.9)
    if ctx.tmdb.enabled:
        tmdb_md = (
            ctx.tmdb.search_movie(parsed.title, parsed.year)
            if media_type == "movie"
            else ctx.tmdb.search_show(parsed.title, parsed.year)
        )
        if tmdb_md and tmdb_md.get("tmdb_id"):
            metadata.update({
                "source": "tmdb",
                "title": tmdb_md.get("title") or parsed.title,
                "year": tmdb_md.get("year") or parsed.year,
                "tmdb_id": tmdb_md.get("tmdb_id"),
                "imdb_id": tmdb_md.get("imdb_id"),
                "tvdb_id": None,
            })
            return "orphan_resolvable", None, metadata

    return "orphan_unresolved", None, metadata


def _sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(HASH_BUFFER_SIZE)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()
