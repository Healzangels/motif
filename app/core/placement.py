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
    PlusMode, normalize_edition, normalize_title, parse_folder_name,
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
        # v1.15.117: tolerate OSError on the pre-clean unlink. Pre-
        # fix a stale .motif-tmp left behind by a prior crashed run
        # (permission held / lock / filesystem hiccup) would raise
        # PermissionError here and propagate out of place_theme as
        # a worker-job failure. The subsequent os.link/copy + os.
        # replace below would overwrite the temp anyway, so the
        # unlink is best-effort; an unlink failure that leaves the
        # temp file in place just means os.replace handles the
        # overwrite. Log at WARNING so the operator sees the
        # underlying stale state but the job survives.
        try:
            dst_tmp.unlink()
        except OSError as e:
            log.warning(
                "place: stale .motif-tmp at %s could not be removed "
                "(%s) — proceeding; os.replace will overwrite",
                dst_tmp, e,
            )
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
    cached_rk: str | None = None,
    cached_has_theme: bool = False,
    cached_folder_path: str | None = None,
    strict_edition: bool = True,
    plus_mode: PlusMode = "separator",
    skip_if_plex_has_theme: bool = True,
    force_overwrite: bool = False,
    analyze_after: bool = True,
) -> PlacementOutcome:
    """
    Place `source_file` as theme.mp3 in the matched media folder.
    Skip if the destination already has a theme (case-insensitive) or if
    Plex already exposes one.

    `force_overwrite=True` bypasses BOTH skip conditions — used when the
    user explicitly approves placement from /pending knowing it will
    overwrite an existing sidecar.

    v1.11.68: when `cached_folder_path` is supplied (the worker pulls
    plex_items.folder_path alongside rating_key), use it as the
    target folder directly — skip FolderIndex title+year+edition
    matching entirely. This dodges the strict_edition false-negative
    where a Plex folder carries a {edition-4K} or {edition-Theatrical}
    tag but the theme row has no edition recorded (typical for
    SET URL / UPLOAD MP3 on orphan rows). The pre-fix path made
    placement reach for FolderIndex with edition_raw="" and reject
    every {edition-X} folder, leaving the download stuck in /pending
    forever with reason='no_match'.
    """
    # 1. Find target folder
    find: FindResult | None = None
    if cached_folder_path:
        # Path translation already happens for stat/iterdir via
        # plex_enum._candidate_local_paths; mirror it here so the
        # placement target resolves whether Plex reports
        # /data/media/... (container view) or /mnt/user/data/...
        # (Unraid host view).
        from .plex_enum import _candidate_local_paths
        for cand in _candidate_local_paths(cached_folder_path):
            if cand.is_dir():
                find = FindResult(MatchResult.EXACT, cand,
                                  reason="cached_folder_path")
                break
    if find is None:
        find = find_target_folder(
            index, title=title, year=year, edition_raw=edition_raw,
            strict_edition=strict_edition, plus_mode=plus_mode,
        )
    if find.folder is None:
        # v1.11.68: emit the enum-key reason ('no_match') so /pending's
        # last_place_attempt_reason switch matches. The human-readable
        # 'no matching folder' string used pre-fix made the /pending
        # render fall through to its generic 'No place job queued —
        # click APPROVE' message instead of the specific 'No Plex
        # folder matched this title' branch.
        return PlacementOutcome(False, None, "no_match")

    target = find.folder

    # 2. Skip if Plex already has a theme (unless force_overwrite).
    # Reads from cached_has_theme — set by the caller from plex_items.
    rk: str | None = cached_rk
    if skip_if_plex_has_theme and not force_overwrite and cached_has_theme:
        return PlacementOutcome(
            False, None, "plex_has_theme",
            target_folder=target, plex_rating_key=rk,
        )

    # 3. Skip if a theme file is already in the folder (unless force_overwrite,
    # in which case we unlink it before linking the new one)
    existing = find_existing_theme_file(target)
    if existing and not force_overwrite:
        # v0.51.11: but NOT if `existing` is motif's OWN placement — the same inode
        # as source_file (a hardlink we already laid down). If a prior place landed
        # the file but its placements-row write then failed (lock-ladder exhaustion,
        # worker kill mid-transaction), the retry would otherwise classify motif's own
        # theme.mp3 as a foreign M sidecar (existing_theme skip → no placements row →
        # wrong SRC, sweep-excluded forever). samefile() (dev+inode) distinguishes our
        # hardlink from a genuinely foreign sidecar; on our own file fall through to
        # re-place (idempotent) so the placement row gets (re)written. Mirrors the
        # inode-identity check already in scanner.py / adopt.py / worker.py:1722. NOTE:
        # only catches hardlink placements — a cross-FS COPY is a distinct inode and
        # still reads as foreign, same limitation those sibling sites carry.
        try:
            is_own = existing.samefile(source_file)
        except OSError:
            is_own = False
        if not is_own:
            return PlacementOutcome(
                False, None, f"existing_theme:{existing.name}",
                target_folder=target, plex_rating_key=rk,
            )

    # 4. Place
    # v1.22.40 (audit): on force_overwrite, DON'T pre-unlink the existing theme.
    # _safe_link_or_copy writes a temp then os.replace's it over theme.mp3 —
    # atomic — so a same-named existing file is overwritten with no window.
    # Pre-fix existing.unlink() ran BEFORE the place; a place I/O error
    # (cross-FS / disk-full / source gone) then left the folder with NO theme at
    # all (old destroyed, new never landed). Place atomically first, THEN remove
    # any differently-named leftover (e.g. "Theme.mp3" beside the new theme.mp3).
    dst = target / "theme.mp3"
    try:
        kind = _safe_link_or_copy(source_file, dst)
    except OSError as e:
        return PlacementOutcome(False, None, f"placement_error:{e}", target_folder=target)
    if force_overwrite and existing and existing.name != dst.name and existing.exists():
        # v1.22.40 (audit): on a case-insensitive FS, "Theme.mp3" and
        # "theme.mp3" are the SAME inode — os.replace above already overwrote it,
        # so unlinking `existing` here would delete the file we just placed.
        # samefile() (dev+inode) catches that; only remove a genuinely distinct
        # leftover (case-sensitive FS where both names physically coexist).
        try:
            distinct = not existing.samefile(dst)
        except OSError:
            distinct = True
        if distinct:
            try:
                existing.unlink()
            except OSError as e:
                log.warning("place: could not remove prior theme %s: %s", existing, e)

    refreshed = False
    if plex and plex.cfg.enabled and analyze_after and rk:
        # The ONE remaining Plex call: tell the agent to re-scan so the
        # newly-placed theme.mp3 gets picked up. v1.11.24 schedules
        # additional retries at +10s/+30s/+90s from worker._do_place
        # so we get multiple chances without hammering the API.
        refreshed = plex.refresh(rk)

    return PlacementOutcome(
        True, kind, "placed",
        target_folder=target, plex_rating_key=rk, plex_refreshed=refreshed,
    )
