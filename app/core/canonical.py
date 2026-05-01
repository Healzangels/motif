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
) -> str:
    """v1.11.46: filesystem-safe slug derived directly from the Plex
    section title, regardless of is_anime / is_4k flags. The user
    names their section "4kmovies" or "Flex" or "Documentaries"; the
    themes subdir mirrors that exactly (lowercased, with runs of
    non-alphanumeric characters collapsed to a single hyphen).

    Pre-v1.11.46 the slug was either category-fixed ("4kmovies",
    "anime", ...) or title-with-suffix ("movies-4k", "anime-anime"),
    both of which surprised users whose Plex section names already
    encoded the category. The flags now only drive the tab partition
    (Movies / TV / Anime) and the standard / 4K toggle within a tab —
    they no longer touch the storage layout.

    is_anime / is_4k stay in the signature so callers don't need to
    change, but they're unused in the slug derivation.

    Examples:
      - "Movies"          → "movies"
      - "4K Movies"       → "4k-movies"
      - "Movies4K"        → "movies4k"
      - "4kmovies"        → "4kmovies"
      - "TV Shows"        → "tv-shows"
      - "Flex"            → "flex"
      - "Documentaries"   → "documentaries"
      - "Anime"           → "anime"
      - empty title       → "movies" (or "tv" for show-type)

    Caller (sections._allocate_themes_subdir) is responsible for
    collision handling — if two sections compute the same subdir, the
    second one gets a -2 / -3 / ... suffix until unique.
    """
    del is_anime, is_4k  # unused — see docstring
    raw = (title or "").strip().lower() or ("tv" if type_ == "show" else "movies")
    slug = re.sub(r"[^a-z0-9]+", "-", raw).strip("-") or (
        "tv" if type_ == "show" else "movies"
    )
    return slug


# v1.11.0: relocate_legacy_canonical_files and backfill_hash_match_provenance
# (one-shot startup migrations from the flat <vid>.mp3 staging layout and the
# pre-v1.8.6 hash_match provenance bug) are gone — v1.11.0 ships as a
# fresh-start release; the v17 → v18 migration's hard-stop blocks loading
# old DBs, so there's no legacy data to fix up.
