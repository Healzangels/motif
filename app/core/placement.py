"""
Placement engine.

Given a downloaded theme file in the local library, find the corresponding
movie/show folder in the Plex library and place a hardlink (or copy on
cross-filesystem) named theme.mp3 next to the media file.

Mirrors the matching logic in merge-themes.sh:
- Index target folders once at startup
- Try exact folder name match
- Try (title, year, edition) match
- Try (title, edition) with unique year fallback
- Skip on edition mismatch when STRICT_EDITION_MATCH is set

Then:
- Probe Plex for an existing theme; skip if present
- Hardlink (preferred) or copy
- Trigger Plex analyze/refresh
"""
from __future__ import annotations

import logging
import os
import shutil
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

from .normalize import (
    PlusMode, editions_equal, normalize_edition, normalize_title,
    parse_folder_name, titles_equal,
)
from .plex import PlexClient

log = logging.getLogger(__name__)


@dataclass
class FolderEntry:
    path: Path
    title: str
    year: str
    edition_raw: str

    @property
    def title_norm(self) -> str:
        return normalize_title(self.title)

    @property
    def edition_norm(self) -> str:
        return normalize_edition(self.edition_raw)


@dataclass
@dataclass
class FolderIndex:
    """Indexes Plex media folders by various keys for fast matching.
    Can be built from a single root or from multiple roots (when a media
    type has multiple Plex sections, e.g. Movies + 4K Movies + Anime Movies)."""
    by_full_name: dict[str, FolderEntry] = field(default_factory=dict)
    by_title_year_edition: dict[tuple, FolderEntry] = field(default_factory=dict)
    by_title_edition: dict[tuple, list[FolderEntry]] = field(default_factory=lambda: defaultdict(list))

    @classmethod
    def build(cls, root: Path, plus_mode: PlusMode = "separator") -> "FolderIndex":
        idx = cls()
        idx._index_root(root, plus_mode)
        log.info(
            "Indexed %s: %d folders, %d (title,year,edition) keys",
            root, len(idx.by_full_name), len(idx.by_title_year_edition),
        )
        return idx

    @classmethod
    def build_multi(cls, roots: list[Path], plus_mode: PlusMode = "separator") -> "FolderIndex":
        """Build a unified index across multiple root paths. Used when a media
        type has multiple Plex sections (e.g. one server might have Movies
        and 4K Movies as separate sections, both type=movie). On full-name
        collisions across roots, the earlier root wins (and a warning is
        logged) — collisions are vanishingly rare in practice (different
        people don't usually have identically-named folders across separate
        libraries) but possible."""
        idx = cls()
        for root in roots:
            before_count = len(idx.by_full_name)
            idx._index_root(root, plus_mode)
            log.info(
                "Indexed %s: +%d folders (total %d)",
                root, len(idx.by_full_name) - before_count, len(idx.by_full_name),
            )
        return idx

    def _index_root(self, root: Path, plus_mode: PlusMode) -> None:
        if not root.exists():
            log.warning("Media root does not exist: %s", root)
            return
        for p in root.iterdir():
            if not p.is_dir():
                continue
            parsed = parse_folder_name(p.name)
            entry = FolderEntry(
                path=p, title=parsed.title, year=parsed.year,
                edition_raw=parsed.editions_raw,
            )
            if p.name in self.by_full_name:
                # Collision: keep the existing one and log; the new one is
                # ignored. Concrete example: a "Test (2020)" in both Movies
                # and 4K Movies — both will get themed when sync runs because
                # the Plex API resolves both ratingKeys; this index just
                # decides where the theme.mp3 sidecar goes when downloaded
                # via this code path.
                log.debug(
                    "Folder name collision across roots: %s already at %s, "
                    "ignoring duplicate at %s", p.name, self.by_full_name[p.name].path, p,
                )
                continue
            self.by_full_name[p.name] = entry
            tn = normalize_title(parsed.title, plus_mode)
            ed = normalize_edition(parsed.editions_raw)
            if parsed.year:
                self.by_title_year_edition[(tn, parsed.year, ed)] = entry
            self.by_title_edition[(tn, ed)].append(entry)


class MatchResult:
    EXACT = "exact"
    TITLE_YEAR_EDITION = "title_year_edition"
    TITLE_EDITION_UNIQUE = "title_edition_unique"
    AMBIGUOUS = "ambiguous"
    NONE = "none"


@dataclass
class FindResult:
    kind: str
    folder: Path | None = None
    reason: str | None = None


def find_target_folder(
    index: FolderIndex,
    *,
    title: str,
    year: str,
    edition_raw: str = "",
    strict_edition: bool = True,
    plus_mode: PlusMode = "separator",
) -> FindResult:
    """Replicates the destination-resolution chain in merge-themes.sh."""
    # We don't have an "exact name" to match because our source isn't a Plex
    # folder name — it's a ThemerrDB title. Skip the by_full_name lookup.

    title_norm = normalize_title(title, plus_mode)
    edition_norm = normalize_edition(edition_raw)

    # 1. (title, year, edition)
    if year:
        key = (title_norm, year, edition_norm)
        hit = index.by_title_year_edition.get(key)
        if hit:
            return FindResult(MatchResult.TITLE_YEAR_EDITION, hit.path)

    # 2. (title, edition) — only if exactly one candidate exists
    candidates = index.by_title_edition.get((title_norm, edition_norm), [])
    # If we have a year but it didn't match in step 1, restrict to entries that
    # don't have a conflicting year
    if year and candidates:
        candidates = [c for c in candidates if not c.year or c.year == year]
    if len(candidates) == 1:
        return FindResult(MatchResult.TITLE_EDITION_UNIQUE, candidates[0].path)
    if len(candidates) > 1:
        return FindResult(
            MatchResult.AMBIGUOUS, None,
            reason=f"{len(candidates)} ambiguous yearless matches",
        )

    # 3. Edition mismatch — refuse to fall through to a no-edition folder
    if strict_edition and edition_norm:
        return FindResult(
            MatchResult.NONE, None,
            reason=f"no folder with edition '{edition_raw}'",
        )

    return FindResult(MatchResult.NONE, None, reason="no matching folder")


def find_existing_theme_file(folder: Path) -> Path | None:
    """Case-insensitive lookup for theme.mp3 / Theme.mp3 / THEME.MP3."""
    if not folder.is_dir():
        return None
    for f in folder.iterdir():
        if f.is_file() and f.name.lower() == "theme.mp3":
            return f
    return None


def _can_hardlink(src: Path, dst_dir: Path) -> bool:
    """Hardlinks only work within the same filesystem."""
    try:
        return src.stat().st_dev == dst_dir.stat().st_dev
    except OSError:
        return False


def _safe_link_or_copy(src: Path, dst: Path) -> str:
    """Hardlink when possible, copy otherwise. Atomic via temp+rename."""
    dst_tmp = dst.with_suffix(dst.suffix + ".motif-tmp")
    if dst_tmp.exists():
        dst_tmp.unlink()
    kind: str
    if _can_hardlink(src, dst.parent):
        try:
            os.link(src, dst_tmp)
            kind = "hardlink"
        except OSError as e:
            log.warning("Hardlink failed (%s), falling back to copy", e)
            shutil.copy2(src, dst_tmp)
            kind = "copy"
    else:
        shutil.copy2(src, dst_tmp)
        kind = "copy"
    os.replace(dst_tmp, dst)
    return kind


@dataclass
class PlacementOutcome:
    placed: bool
    kind: str | None  # 'hardlink', 'copy', or None when not placed
    reason: str
    target_folder: Path | None = None
    plex_rating_key: str | None = None
    plex_refreshed: bool = False


def place_theme(
    *,
    media_type: str,
    title: str,
    year: str,
    edition_raw: str,
    source_file: Path,
    index: FolderIndex,
    plex: PlexClient | None,
    strict_edition: bool = True,
    plus_mode: PlusMode = "separator",
    skip_if_plex_has_theme: bool = True,
    analyze_after: bool = True,
) -> PlacementOutcome:
    """
    Place `source_file` as theme.mp3 in the matched media folder.
    Skip if the destination already has a theme (case-insensitive) or if
    Plex already exposes one.
    """
    # 1. Find target folder
    find = find_target_folder(
        index, title=title, year=year, edition_raw=edition_raw,
        strict_edition=strict_edition, plus_mode=plus_mode,
    )
    if find.folder is None:
        return PlacementOutcome(False, None, find.reason or "no_match")

    target = find.folder

    # 2. Skip if Plex already has a theme
    rk: str | None = None
    if plex and plex.cfg.enabled and skip_if_plex_has_theme:
        rk = plex.resolve_rating_key(
            media_type=media_type, title=title, year=year, edition_raw=edition_raw,
        )
        if rk and plex.item_has_theme(rk):
            return PlacementOutcome(
                False, None, "plex_has_theme",
                target_folder=target, plex_rating_key=rk,
            )

    # 3. Skip if a theme file is already in the folder
    existing = find_existing_theme_file(target)
    if existing:
        return PlacementOutcome(
            False, None, f"existing_theme:{existing.name}",
            target_folder=target, plex_rating_key=rk,
        )

    # 4. Place
    dst = target / "theme.mp3"
    try:
        kind = _safe_link_or_copy(source_file, dst)
    except OSError as e:
        return PlacementOutcome(False, None, f"placement_error:{e}", target_folder=target)

    refreshed = False
    if plex and plex.cfg.enabled and analyze_after:
        # Re-resolve in case we didn't earlier
        if rk is None:
            rk = plex.resolve_rating_key(
                media_type=media_type, title=title, year=year, edition_raw=edition_raw,
            )
        if rk:
            refreshed = plex.refresh(rk)

    return PlacementOutcome(
        True, kind, "placed",
        target_folder=target, plex_rating_key=rk, plex_refreshed=refreshed,
    )
