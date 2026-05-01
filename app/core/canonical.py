"""
Canonical theme-file layout (v1.11.0+):

    <themes_dir>/<plex_sections.themes_subdir>/<Title (Year)>/theme.mp3

The themes_subdir slug is computed from each Plex section's title +
is_4k/is_anime flags at section discovery time and persisted in
plex_sections.themes_subdir so it's stable across renames.

The canonical_subdir mirrors Plex's folder convention: "<title> (<year>)".
Filesystem-illegal characters in titles are replaced with `-`.
"""
from __future__ import annotations

import logging
import re
from pathlib import Path  # noqa: F401  -- re-exported for callers

log = logging.getLogger(__name__)


# Characters that are problematic on common filesystems (Linux/macOS/Windows
# all together — ext4 allows everything except /, but we want NTFS safety on
# Unraid SMB shares too).
_FS_BAD = set('/\\:*?"<>|')


def sanitize_for_filesystem(s: str) -> str:
    """Replace filesystem-unsafe chars in a single path segment with `-`,
    collapse repeated whitespace, and strip leading/trailing `.` and space
    (Windows + macOS quirks).

    v1.10.23: switched the replacement char from `_` to `-` to match
    Plex's own folder convention. Plex Media Server names directories
    like `Mission: Impossible (1996)` as `Mission - Impossible (1996)`,
    so motif's /themes mirror matches that shape.
    """
    if not s:
        return "untitled"
    out = "".join("-" if ch in _FS_BAD else ch for ch in s)
    out = re.sub(r"\s+", " ", out).strip(". ")
    return out or "untitled"


def canonical_theme_subdir(title: str, year: str | None) -> str:
    """The folder name under <themes_dir>/<themes_subdir>/ where this
    item's theme.mp3 lives. Mirrors Plex's "<Title> (<Year>)" convention
    so the staging tree visually matches what Plex sees."""
    safe = sanitize_for_filesystem(title or "untitled")
    if year:
        return f"{safe} ({year})"
    return safe


def compute_section_themes_subdir(
    title: str, *, type_: str, is_anime: bool, is_4k: bool,
    location_paths: list[str] | None = None,
) -> str:
    """v1.11.58: themes subdir mirrors the Plex section's on-disk
    library folder (basename of the first location_path), not the
    section title. The user's mental model is "the themes folder
    should match the library folder" — if Plex stores the library at
    /data/media/4kmovies, themes land at <themes_dir>/4kmovies/.

    Pre-v1.11.58 the slug was derived from the section TITLE — so a
    section named "4K Movies" sitting on a /data/media/4kmovies
    folder produced themes/4k-movies/, which surprised users.

    Examples (location_path → slug):
      - /data/media/4kmovies          → 4kmovies
      - /data/media/movies            → movies
      - /data/media/anime             → anime
      - /data/media/tv-shows          → tv-shows
      - /mnt/disk1/Documentaries      → documentaries
      - /data/media/Movies (4K)       → movies-4k

    Fallbacks (in order):
      1. basename of location_paths[0] when non-empty
      2. slugified title
      3. "movies" (or "tv" for show-type) if everything else is empty

    is_anime / is_4k stay in the signature for caller compatibility
    but no longer affect the slug; the same is true of the title arg
    when location_paths gives us a usable answer.

    Caller (sections._allocate_themes_subdir) handles collisions by
    appending -2 / -3 / ... when two sections compute the same slug.
    """
    del is_anime, is_4k  # unused — see docstring
    if location_paths:
        for p in location_paths:
            if not p:
                continue
            # basename of the path; tolerate trailing slashes
            base = p.rstrip("/").rsplit("/", 1)[-1]
            if not base:
                continue
            slug = _slugify(base)
            if slug:
                return slug
    raw = (title or "").strip()
    if raw:
        slug = _slugify(raw)
        if slug:
            return slug
    return "tv" if type_ == "show" else "movies"


def _slugify(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")


# v1.11.0: relocate_legacy_canonical_files and backfill_hash_match_provenance
# (one-shot startup migrations from the flat <vid>.mp3 staging layout and the
# pre-v1.8.6 hash_match provenance bug) are gone — v1.11.0 ships as a
# fresh-start release; the v17 → v18 migration's hard-stop blocks loading
# old DBs, so there's no legacy data to fix up.
