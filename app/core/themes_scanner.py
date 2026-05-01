"""
themes_dir manual-import scanner.

Walks <themes_dir>/<section_subdir>/<Title (Year)>/theme.<audio-ext>
and decides what motif should do with each file. The user's mental
model: drop a folder of themes into themes_dir, motif matches each
against Plex and asks for approval (or auto-applies on demand).

This is the inverse of scanner.py:
  - scanner.py walks Plex media folders looking for sidecars to ADOPT
    (file in Plex folder → motif claims it, hardlinks into /themes)
  - this module walks /themes looking for canonicals to PLACE
    (file in /themes → motif matches against Plex, hardlinks into
    Plex folder)

For each found file:
  1. Skip if already in local_files (already managed)
  2. Parse '<Title> (Year)' from the immediate parent folder name
  3. Normalize title; query plex_items for matching (title_norm, year)
  4. Decide match_kind based on Plex state:
       - 0 matches                                → orphan
       - 1+ matches, no Plex theme/sidecar        → auto_place
       - 1+ matches, sidecar matches our content  → auto_adopt
       - 1+ matches, sidecar differs              → conflict_overwrite
       - 1+ matches, Plex agent supplies P-theme  → conflict_plex_agent
       - 2+ matches across sections               → multi_section (still
         auto-placeable to all by default)
  5. Insert/update an import_findings row

A separate flat-layout pass detects files that landed at
<themes_dir>/<Title>/theme.mp3 (no section subdir) and flags them
with match_kind='flat_layout' so the UI can prompt the user to move
them.
"""
from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from .db import get_conn, transaction
from .events import log_event, now_iso
from .normalize import normalize_title, parse_folder_name

log = logging.getLogger(__name__)

# Mirror plex_enum's audio extension match for theme.* files.
_THEME_EXTS = {
    ".mp3", ".m4a", ".m4b", ".flac", ".ogg", ".oga", ".opus",
    ".wav", ".wma", ".aac", ".aif", ".aiff", ".alac",
}

_HASH_BUFSIZE = 1024 * 1024  # 1 MiB


@dataclass
class ImportCandidate:
    """One file the scanner found under themes_dir."""
    rel_path: str        # 'movies/Matilda (1996)/theme.mp3'
    full_path: Path
    section_subdir: str  # 'movies' (or '' when flat)
    title: str           # 'Matilda'
    year: str            # '1996'
    file_size: int
    file_mtime: str
    file_sha256: str


def _hash_file(path: Path) -> tuple[str, int]:
    h = hashlib.sha256()
    size = 0
    with path.open("rb") as f:
        while True:
            chunk = f.read(_HASH_BUFSIZE)
            if not chunk:
                break
            h.update(chunk)
            size += len(chunk)
    return h.hexdigest(), size


def _walk_theme_files(themes_dir: Path) -> Iterable[ImportCandidate]:
    """Yield every theme.<audio-ext> under themes_dir.

    Layout expected: <themes_dir>/<section_subdir>/<Title (Year)>/theme.<ext>
    A flat layout (<themes_dir>/<Title (Year)>/theme.<ext>) is yielded
    too with section_subdir='' so the caller can flag it.
    """
    if not themes_dir.is_dir():
        return
    for section_dir in themes_dir.iterdir():
        try:
            if not section_dir.is_dir():
                continue
        except OSError:
            continue
        section_subdir = section_dir.name
        # Two-level layout: <section_dir>/<Title (Year)>/theme.<ext>
        # Also detect one-level (flat): <section_dir>/theme.<ext>
        # which happens when the user dropped files at the wrong depth.
        for child in _safe_iterdir(section_dir):
            if child.is_file():
                # Flat layout case — the 'section_dir' is actually the
                # Title folder.
                if child.name.lower().startswith("theme.") and \
                        _ext_of(child.name) in _THEME_EXTS:
                    parsed = parse_folder_name(section_dir.name)
                    sha, size = _hash_file(child)
                    yield ImportCandidate(
                        rel_path=str(child.relative_to(themes_dir)),
                        full_path=child,
                        section_subdir="",  # signals flat layout
                        title=parsed.title,
                        year=parsed.year,
                        file_size=size,
                        file_mtime=_mtime_iso(child),
                        file_sha256=sha,
                    )
                continue
            # Two-level case
            try:
                if not child.is_dir():
                    continue
            except OSError:
                continue
            theme = _find_theme_file(child)
            if theme is None:
                continue
            parsed = parse_folder_name(child.name)
            sha, size = _hash_file(theme)
            yield ImportCandidate(
                rel_path=str(theme.relative_to(themes_dir)),
                full_path=theme,
                section_subdir=section_subdir,
                title=parsed.title,
                year=parsed.year,
                file_size=size,
                file_mtime=_mtime_iso(theme),
                file_sha256=sha,
            )


def _safe_iterdir(p: Path):
    try:
        yield from p.iterdir()
    except OSError as e:
        log.warning("themes_scanner: iterdir(%s) failed: %s", p, e)


def _find_theme_file(folder: Path) -> Path | None:
    """First theme.<audio-ext> in folder, or None."""
    try:
        for entry in folder.iterdir():
            try:
                if not entry.is_file():
                    continue
            except OSError:
                continue
            name = entry.name.lower()
            if not name.startswith("theme."):
                continue
            if _ext_of(name) in _THEME_EXTS:
                return entry
    except OSError:
        return None
    return None


def _ext_of(name: str) -> str:
    """'.mp3' from 'theme.mp3', '.tar.gz' style not handled (we only
    care about single-extension theme files)."""
    i = name.rfind(".")
    return name[i:].lower() if i >= 0 else ""


def _mtime_iso(p: Path) -> str:
    from datetime import datetime, timezone
    try:
        ts = p.stat().st_mtime
        return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(timespec="seconds")
    except OSError:
        return now_iso()


def _classify_candidate(conn, cand: ImportCandidate) -> tuple[str, list[dict]]:
    """Decide match_kind + assemble candidates. Returns
    (match_kind, match_candidates_list)."""
    if not cand.section_subdir:
        return "flat_layout", []

    # Already managed? — caller filters this out before calling
    # _classify, so we don't worry about it here.

    if not cand.title:
        return "orphan", []

    title_norm = normalize_title(cand.title)
    rows: list[dict] = []
    if title_norm and cand.year:
        # Match by normalized title + year. Per-section: a single
        # title can land in 4K + standard sections; we collect both.
        # plex_items.section_id is filtered by ps.included so we
        # don't try to push to disabled libraries.
        result = conn.execute(
            """SELECT pi.rating_key, pi.section_id, pi.media_type,
                      pi.folder_path, pi.has_theme, pi.local_theme_file,
                      pi.guid_imdb, pi.guid_tmdb
               FROM plex_items pi
               INNER JOIN plex_sections ps
                 ON ps.section_id = pi.section_id AND ps.included = 1
               WHERE pi.title_norm = ? AND pi.year = ?""",
            (title_norm, cand.year),
        ).fetchall()
        rows = [dict(r) for r in result]

    if not rows:
        return "orphan", []

    # Multi-section disambiguation. Each row in rows is one Plex
    # match; multi means >1 section.
    multi = len({r["section_id"] for r in rows}) > 1

    # Conflict / agent / clean buckets, evaluated against all matches.
    has_local = any(r["local_theme_file"] for r in rows)
    has_plex_agent = any(r["has_theme"] and not r["local_theme_file"] for r in rows)
    no_theme_at_all = not any(r["has_theme"] or r["local_theme_file"] for r in rows)

    if no_theme_at_all:
        return ("multi_section" if multi else "auto_place"), rows

    if has_local:
        # See if any Plex-folder sidecar matches our import by content.
        # If yes → auto_adopt (no file movement, just record DB rows).
        # If no  → conflict_overwrite (their file differs from ours).
        from .plex_enum import _candidate_local_paths
        any_match = False
        any_differ = False
        for r in rows:
            if not r["local_theme_file"]:
                continue
            for folder in _candidate_local_paths(r["folder_path"] or ""):
                p = folder / "theme.mp3"
                try:
                    if not p.is_file():
                        continue
                    sha, _ = _hash_file(p)
                    if sha == cand.file_sha256:
                        any_match = True
                    else:
                        any_differ = True
                except OSError:
                    continue
                break  # first reachable candidate wins
        if any_match and not any_differ:
            return "auto_adopt", rows
        # Mixed or all-differ → user has to confirm overwrite
        return "conflict_overwrite", rows

    if has_plex_agent:
        return "conflict_plex_agent", rows

    # Should be unreachable — we covered all combinations above.
    return ("multi_section" if multi else "auto_place"), rows


def scan_themes_dir(db_path: Path, themes_dir: Path,
                    *, cancel_check=lambda: False) -> dict:
    """Walk themes_dir, classify every theme file we find, write to
    import_findings. Returns a summary dict with per-kind counts.

    Idempotent: on rescan, files already in local_files are skipped
    (already managed); files already in import_findings get their
    decision left alone but match_kind / candidates re-evaluated in
    case Plex state changed.
    """
    log.info("themes_scanner: starting scan of %s", themes_dir)
    summary = {
        "scanned_at": now_iso(),
        "total": 0, "skipped_managed": 0,
        "auto_place": 0, "auto_adopt": 0,
        "conflict_overwrite": 0, "conflict_plex_agent": 0,
        "multi_section": 0, "orphan": 0, "flat_layout": 0,
    }

    # Pre-load the set of (themes_dir-relative) file_paths already in
    # local_files. The scanner skips these — already managed by motif.
    with get_conn(db_path) as conn:
        managed = {
            r["file_path"] for r in conn.execute(
                "SELECT file_path FROM local_files",
            ).fetchall()
        }

    for cand in _walk_theme_files(themes_dir):
        if cancel_check():
            from .worker import _JobCancelled
            raise _JobCancelled()
        summary["total"] += 1
        if cand.rel_path in managed:
            summary["skipped_managed"] += 1
            continue

        with get_conn(db_path) as conn, transaction(conn):
            kind, matches = _classify_candidate(conn, cand)
            summary[kind] += 1
            target_section_ids = [r["section_id"] for r in matches]
            conn.execute(
                """INSERT INTO import_findings
                     (scanned_at, file_path, file_size, file_mtime,
                      file_sha256, parsed_title, parsed_year,
                      parsed_section_subdir, match_kind,
                      match_candidates, target_section_ids)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(file_path) DO UPDATE SET
                     scanned_at = excluded.scanned_at,
                     file_size = excluded.file_size,
                     file_mtime = excluded.file_mtime,
                     file_sha256 = excluded.file_sha256,
                     parsed_title = excluded.parsed_title,
                     parsed_year = excluded.parsed_year,
                     parsed_section_subdir = excluded.parsed_section_subdir,
                     match_kind = excluded.match_kind,
                     match_candidates = excluded.match_candidates,
                     target_section_ids = CASE
                       WHEN import_findings.decision = 'pending'
                       THEN excluded.target_section_ids
                       ELSE import_findings.target_section_ids
                     END
                """,
                (summary["scanned_at"], cand.rel_path, cand.file_size,
                 cand.file_mtime, cand.file_sha256,
                 cand.title, cand.year, cand.section_subdir,
                 kind, json.dumps(matches),
                 json.dumps(target_section_ids)),
            )

    log_event(db_path, level="INFO", component="import",
              message=f"Themes-dir scan: {summary['total']} files "
                      f"({summary['auto_place']} auto-place, "
                      f"{summary['auto_adopt']} auto-adopt, "
                      f"{summary['conflict_overwrite']} conflict, "
                      f"{summary['orphan']} orphan)",
              detail=summary)
    log.info("themes_scanner: scan complete: %s", summary)
    return summary
