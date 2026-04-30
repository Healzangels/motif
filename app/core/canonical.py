"""
Canonical theme-file layout under <themes_dir>/<media_subdir>/<canonical_subdir>/theme.mp3.

The canonical_subdir mirrors Plex's folder name convention: "<title> (<year>)".
Plex is the source of truth for what these names look like, so we use the
same shape. Filesystem-illegal characters in titles are replaced with `_`.
"""
from __future__ import annotations

import logging
import re
from pathlib import Path

from .db import get_conn

log = logging.getLogger(__name__)


# Characters that are problematic on common filesystems (Linux/macOS/Windows
# all together — ext4 allows everything except /, but we want NTFS safety on
# Unraid SMB shares too).
_FS_BAD = set('/\\:*?"<>|')


def sanitize_for_filesystem(s: str) -> str:
    """Replace filesystem-unsafe chars in a single path segment with `_`,
    collapse repeated whitespace, and strip leading/trailing `.` and space
    (Windows + macOS quirks)."""
    if not s:
        return "untitled"
    out = "".join("_" if ch in _FS_BAD else ch for ch in s)
    out = re.sub(r"\s+", " ", out).strip(". ")
    return out or "untitled"


def canonical_theme_subdir(title: str, year: str | None) -> str:
    """The folder name under themes_dir/<movies|tv>/ where this item's
    theme.mp3 lives. Mirrors Plex's "<Title> (<Year>)" convention so the
    staging tree visually matches what Plex sees."""
    safe = sanitize_for_filesystem(title or "untitled")
    if year:
        return f"{safe} ({year})"
    return safe


def relocate_legacy_canonical_files(db_path: Path, themes_dir: Path | None) -> dict:
    """One-shot: walk local_files, move every <vid>.mp3 in flat themes_dir/
    {movies,tv}/ into the per-item subfolder layout, update file_path.

    Hardlinks are preserved across rename (same inode). Idempotent — if
    file_path already matches the new layout, the row is skipped. Missing
    files are logged and skipped (the next sync will re-download).

    Returns a stats dict for logging.
    """
    stats = {"moved": 0, "skipped_uptodate": 0, "missing": 0, "errors": 0}
    if themes_dir is None:
        return stats
    if not themes_dir.is_dir():
        # First-run: no themes_dir yet. Nothing to migrate.
        return stats

    with get_conn(db_path) as conn:
        rows = conn.execute(
            """SELECT lf.media_type, lf.tmdb_id, lf.file_path, t.title, t.year
               FROM local_files lf
               JOIN themes t
                 ON t.media_type = lf.media_type AND t.tmdb_id = lf.tmdb_id"""
        ).fetchall()

        for r in rows:
            media_subdir = "movies" if r["media_type"] == "movie" else "tv"
            new_rel = (Path(media_subdir)
                       / canonical_theme_subdir(r["title"] or "", r["year"])
                       / "theme.mp3")
            old_rel = Path(r["file_path"])
            if old_rel == new_rel:
                stats["skipped_uptodate"] += 1
                continue

            old_abs = themes_dir / old_rel
            new_abs = themes_dir / new_rel
            if not old_abs.is_file():
                # Source missing — could happen if user manually mucked with
                # the staging dir. Best-effort: clear the file_path so a
                # future sync re-downloads.
                log.warning("relocate: source missing, will rely on re-sync: %s", old_abs)
                stats["missing"] += 1
                continue

            try:
                new_abs.parent.mkdir(parents=True, exist_ok=True)
                if new_abs.exists():
                    # Same inode? then we already migrated; just update DB
                    if new_abs.stat().st_ino == old_abs.stat().st_ino:
                        old_abs.unlink()
                    else:
                        # Conflict — leave both, log, skip DB update
                        log.warning("relocate: target exists with different content,"
                                    " leaving alone: %s", new_abs)
                        stats["errors"] += 1
                        continue
                else:
                    old_abs.rename(new_abs)
            except OSError as e:
                log.warning("relocate failed %s -> %s: %s", old_abs, new_abs, e)
                stats["errors"] += 1
                continue

            conn.execute(
                "UPDATE local_files SET file_path = ? "
                "WHERE media_type = ? AND tmdb_id = ?",
                (str(new_rel), r["media_type"], r["tmdb_id"]),
            )
            stats["moved"] += 1

    if stats["moved"] or stats["errors"] or stats["missing"]:
        log.info("Canonical layout relocate: %s", stats)
    return stats
