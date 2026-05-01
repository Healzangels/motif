"""
Plex API client.

Mirrors the helper functions in merge-themes.sh:
- Theme presence probe with HEAD->GET fallback
- Strict (year+title) and relaxed (title-only) candidate search
- Edition-aware ratingKey resolution
- Analyze with refresh fallback

All requests use a short timeout and a stable Client-Identifier.
"""
from __future__ import annotations

import logging
import re
import socket
from dataclasses import dataclass
from typing import Iterable
from urllib.parse import quote
from xml.etree import ElementTree as ET

import httpx

from .normalize import editions_equal, titles_equal, PlusMode

log = logging.getLogger(__name__)


def _safe_int(s: str | None) -> int | None:
    if s is None:
        return None
    try:
        return int(s)
    except (TypeError, ValueError):
        return None


def _extract_guids(el) -> dict[str, str]:
    """Plex returns <Guid id="imdb://tt..."/>, <Guid id="tmdb://12345"/>, etc.
    Returns {agent: id_value}."""
    out: dict[str, str] = {}
    for g in el.iter("Guid"):
        gid = g.get("id", "")
        if "://" not in gid:
            continue
        agent, val = gid.split("://", 1)
        out[agent.lower()] = val
    return out


def _extract_folder_path(el) -> str:
    """For movies: parent dir of first Part.file. For shows: first
    Location.path. Empty string when neither is available."""
    for part in el.iter("Part"):
        f = part.get("file", "")
        if f:
            i = f.rfind("/")
            return f[:i] if i > 0 else f
    for loc in el.iter("Location"):
        p = loc.get("path", "")
        if p:
            return p
    return ""


@dataclass
class PlexSection:
    section_id: str
    uuid: str
    title: str
    type: str  # 'movie' or 'show'
    agent: str
    language: str
    location_paths: list[str]


@dataclass
class PlexCandidate:
    rating_key: str
    title: str
    year: str
    edition_title: str
    has_theme: bool
    section_id: str | None = None  # which section this candidate came from


@dataclass
class PlexLibraryItem:
    """A full record for a Plex library item — what we cache in plex_items."""
    rating_key: str
    section_id: str
    media_type: str  # 'movie' or 'show'
    title: str
    year: str  # "" if missing
    guid_imdb: str | None  # tt-id or None
    guid_tmdb: int | None
    guid_tvdb: int | None
    folder_path: str  # absolute path of the item's media folder
    has_theme: bool  # what Plex itself says


@dataclass
class PlexConfig:
    url: str
    token: str
    movie_section: str
    tv_section: str
    enabled: bool = True
    timeout: float = 10.0


class PlexClient:
    def __init__(self, cfg: PlexConfig, plus_mode: PlusMode = "separator"):
        self.cfg = cfg
        self._plus = plus_mode
        client_id = f"motif-{socket.gethostname()}"
        self._headers = {
            "X-Plex-Token": cfg.token,
            "X-Plex-Product": "Motif",
            "X-Plex-Version": "1.0",
            "X-Plex-Client-Identifier": client_id,
            "Accept": "application/xml",
        }
        self._client = httpx.Client(
            base_url=cfg.url.rstrip("/"),
            timeout=cfg.timeout,
            headers=self._headers,
            verify=False,  # most homelab Plex servers use self-signed
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ---- Internal HTTP ----

    def _get(self, path: str, params: dict | None = None,
             timeout: float | None = None) -> httpx.Response | None:
        # v1.11.21: per-call timeout override + warning-level error log.
        # Pre-fix the HTTP error was logged at debug, so timeouts on
        # /library/sections/{id}/all (which can take 30-60s on a large
        # section) surfaced as a silent 'no response' in the worker log.
        try:
            kwargs: dict = {"params": params}
            if timeout is not None:
                kwargs["timeout"] = timeout
            return self._client.get(path, **kwargs)
        except httpx.HTTPError as e:
            log.warning("Plex GET %s failed: %s: %s",
                        path, type(e).__name__, e)
            return None

    def _put(self, path: str, params: dict | None = None) -> int | None:
        try:
            r = self._client.put(path, params=params)
            return r.status_code
        except httpx.HTTPError as e:
            log.debug("Plex PUT failed: %s", e)
            return None

    def _head_or_get_status(self, path: str) -> int | None:
        try:
            r = self._client.head(path)
            if r.status_code in (200, 401, 403):
                return r.status_code
            r = self._client.get(path)
            return r.status_code
        except httpx.HTTPError as e:
            log.debug("Plex HEAD/GET failed: %s", e)
            return None

    # ---- Search / candidates ----

    def _build_title_variants(self, title: str) -> Iterable[str]:
        """Match the Bash script's variant generator."""
        seen: set[str] = set()
        v = [
            title,
            title.replace(" - ", ": "),
            title.replace(": ", " - "),
            title.replace("&", "and"),
            title.replace("and", "&"),
            re.sub(r"[^a-zA-Z0-9+ ]+", " ", title).strip(),
        ]
        for s in v:
            s = re.sub(r"\s+", " ", s).strip()
            if s and s not in seen:
                seen.add(s)
                yield s

    def _section_for(self, media_type: str) -> str:
        return self.cfg.movie_section if media_type == "movie" else self.cfg.tv_section

    # ---- Section discovery ----

    def discover_sections(self) -> list[PlexSection]:
        """Enumerate all library sections on the Plex server.

        Returns sections of type 'movie' and 'show'. Other types (music,
        photos) are ignored — motif has nothing to do with them.
        """
        r = self._get("/library/sections")
        if r is None or r.status_code != 200:
            return []
        try:
            root = ET.fromstring(r.text)
        except ET.ParseError:
            return []
        out: list[PlexSection] = []
        for el in root.iter("Directory"):
            stype = el.get("type", "")
            if stype not in ("movie", "show"):
                continue
            section_id = el.get("key", "")
            if not section_id:
                continue
            locations = [
                loc.get("path", "") for loc in el.iter("Location")
                if loc.get("path")
            ]
            out.append(PlexSection(
                section_id=section_id,
                uuid=el.get("uuid", ""),
                title=el.get("title", ""),
                type=stype,
                agent=el.get("agent", ""),
                language=el.get("language", ""),
                location_paths=locations,
            ))
        return out

    def _parse_candidates(self, xml_text: str) -> list[PlexCandidate]:
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError:
            return []
        out: list[PlexCandidate] = []
        # Movies appear as <Video>, shows as <Directory type="show">
        for el in list(root):
            if el.tag not in ("Video", "Directory"):
                continue
            rk = el.get("ratingKey")
            if not rk:
                continue
            out.append(PlexCandidate(
                rating_key=rk,
                title=el.get("title", ""),
                year=el.get("year", ""),
                edition_title=el.get("editionTitle", ""),
                has_theme=el.get("theme") is not None,
            ))
        return out

    def query_strict(
        self, *, media_type: str, title: str, year: str,
        section_ids: list[str] | None = None,
    ) -> list[PlexCandidate]:
        """Search for items by exact title+year. If section_ids is provided,
        searches across all of them. Otherwise falls back to the configured
        single section ID."""
        sections = section_ids or [self._section_for(media_type)]
        type_id = "1" if media_type == "movie" else "2"
        out: list[PlexCandidate] = []
        for section in sections:
            for variant in self._build_title_variants(title):
                r = self._get(
                    f"/library/sections/{section}/all",
                    params={"type": type_id, "title": variant, "year": year},
                )
                if r is None or r.status_code != 200:
                    continue
                if "<Video " not in r.text and "<Directory " not in r.text:
                    continue
                cands = self._parse_candidates(r.text)
                for c in cands:
                    c.section_id = section
                if cands:
                    out.extend(cands)
                    break  # found in this section's title-variant search; move to next section
            if not any(c.section_id == section for c in out):
                # Fallback to /search per-section
                r = self._get(
                    f"/library/sections/{section}/search",
                    params={"type": type_id, "year": year, "query": title},
                )
                if r is not None and r.status_code == 200:
                    cands = self._parse_candidates(r.text)
                    for c in cands:
                        c.section_id = section
                    out.extend(cands)
        return out

    def query_relaxed(
        self, *, media_type: str, title: str,
        section_ids: list[str] | None = None,
    ) -> list[PlexCandidate]:
        sections = section_ids or [self._section_for(media_type)]
        type_id = "1" if media_type == "movie" else "2"
        out: list[PlexCandidate] = []
        for section in sections:
            r = self._get(
                f"/library/sections/{section}/search",
                params={"type": type_id, "query": title},
            )
            cands_for_section: list[PlexCandidate] = []
            if r is not None and r.status_code == 200:
                cands_for_section = self._parse_candidates(r.text)
            if not cands_for_section:
                for variant in self._build_title_variants(title):
                    r = self._get(
                        f"/library/sections/{section}/all",
                        params={"type": type_id, "title": variant},
                    )
                    if r is not None and r.status_code == 200:
                        cands_for_section = self._parse_candidates(r.text)
                        if cands_for_section:
                            break
            for c in cands_for_section:
                c.section_id = section
            out.extend(cands_for_section)
        return out

    # ---- Theme presence ----

    def item_has_theme(self, rating_key: str) -> bool:
        # Probe /metadata/<rk>/theme
        status = self._head_or_get_status(f"/library/metadata/{rating_key}/theme")
        if status == 200:
            return True
        # Fallback: parse the metadata XML for a theme="..." attr
        r = self._get(f"/library/metadata/{rating_key}")
        if r is not None and r.status_code == 200:
            return ' theme="' in r.text
        return False

    # ---- Resolve ratingKey ----

    def resolve_rating_key(
        self, *, media_type: str, title: str, year: str, edition_raw: str = "",
        section_ids: list[str] | None = None,
    ) -> str | None:
        edition_active = bool(edition_raw and edition_raw.strip())
        first_rk: str | None = None

        if year:
            cands = self.query_strict(
                media_type=media_type, title=title, year=year,
                section_ids=section_ids,
            )
            for c in cands:
                if not titles_equal(title, c.title, self._plus):
                    continue
                if edition_active and not editions_equal(edition_raw, c.edition_title):
                    continue
                if c.has_theme:
                    return c.rating_key
                if first_rk is None:
                    first_rk = c.rating_key
            if first_rk:
                return first_rk

        # Relaxed
        cands = self.query_relaxed(
            media_type=media_type, title=title, section_ids=section_ids,
        )
        best_rk: str | None = None
        for c in cands:
            if not titles_equal(title, c.title, self._plus):
                continue
            if year and c.year and c.year != year:
                continue
            if edition_active and not editions_equal(edition_raw, c.edition_title):
                continue
            if year and c.year == year and c.has_theme:
                return c.rating_key
            if best_rk is None:
                best_rk = c.rating_key
        return best_rk

    def has_theme(
        self, *, media_type: str, title: str, year: str, edition_raw: str = "",
        section_ids: list[str] | None = None,
    ) -> bool:
        rk = self.resolve_rating_key(
            media_type=media_type, title=title, year=year, edition_raw=edition_raw,
            section_ids=section_ids,
        )
        if not rk:
            return False
        return self.item_has_theme(rk)

    # ---- Refresh / analyze ----

    def refresh(self, rating_key: str) -> bool:
        """Tell Plex to re-run the metadata agent on this item.

        This is the correct primitive for picking up newly-added local assets
        like theme.mp3 — provided the item's library agent has Local Media
        Assets enabled in its source priority.

        Per Plex docs, refresh metadata is what should be invoked when:
          'Added local media assets, such as artwork, theme music,
           subtitle files, etc.'

        We try refresh?force=1 first; if for some reason that returns non-2xx
        we fall back to analyze (which on most Plex builds also has the side
        effect of picking up sidecar assets).
        """
        code = self._put(f"/library/metadata/{rating_key}/refresh",
                         params={"force": "1"})
        if code in (200, 204):
            return True
        # Fallback: analyze (legacy behavior of merge-themes.sh)
        code = self._put(f"/library/metadata/{rating_key}/analyze")
        return code in (200, 204)

    # Backward-compatible alias for code that still calls .analyze()
    def analyze(self, rating_key: str) -> bool:
        return self.refresh(rating_key)

    # ---- List a section ----

    def list_section(
        self, *, media_type: str | None = None, section_id: str | None = None,
    ) -> list[PlexCandidate]:
        """List all items in a Plex section. Pass section_id directly, or
        pass media_type to use the configured fallback section."""
        if section_id is None:
            if media_type is None:
                return []
            section_id = self._section_for(media_type)
        # Determine type_id from media_type if given, else infer (default movie)
        type_id = "1" if (media_type == "movie" or media_type is None) else "2"
        if media_type is None:
            # We don't know what type this section is — try movie type first,
            # callers should pass media_type when they care about the right value.
            type_id = "1"
        r = self._get(f"/library/sections/{section_id}/all", params={"type": type_id})
        if r is None or r.status_code != 200:
            return []
        cands = self._parse_candidates(r.text)
        for c in cands:
            c.section_id = section_id
        return cands

    def enumerate_section_items(
        self, *, section_id: str, media_type: str,
    ) -> list[PlexLibraryItem]:
        """Full enumeration of one library section. Returns one PlexLibraryItem
        per content row, with GUIDs and folder paths extracted.

        For movies, folder_path is the parent dir of the first Part.file.
        For shows, folder_path is the show-level Location/path.

        v1.11.19: two-shot enumeration to handle Plex's inconsistent
        behavior between section variants:
          1. First try with explicit type=1 (movie) or type=2 (show).
             This is the canonical call for typed sections and works
             on every standard install.
          2. If the typed call returns 0 children, fall back to /all
             with no type filter. Some custom-agent variants ('TV
             Shows by Original Air Date', etc.) returned 0 for
             type=2 even when the section had thousands of items;
             v1.10.x lived with this, v1.11.10 'fixed' it by always
             dropping the filter, which then broke standard show
             sections (those return EPISODES instead of shows for an
             unfiltered call). Trying typed-first then unfiltered
             covers both populations.
        """
        type_id = "1" if media_type == "movie" else "2"
        url = f"/library/sections/{section_id}/all"
        items, root = self._fetch_section_items(
            url, section_id, media_type,
            params={"type": type_id, "includeGuids": "1"},
            label=f"type={type_id}",
        )
        if items:
            return items
        # Fallback: drop the type filter. Logged so we can tell which
        # path served each section over time.
        log.info(
            "enumerate_section_items: section %s typed call returned 0; "
            "retrying without type filter",
            section_id,
        )
        items, _ = self._fetch_section_items(
            url, section_id, media_type,
            params={"includeGuids": "1"},
            label="no-type",
        )
        return items

    def _fetch_section_items(
        self, url: str, section_id: str, media_type: str,
        *, params: dict[str, str], label: str,
    ) -> tuple[list[PlexLibraryItem], "ET.Element | None"]:
        # v1.11.21: paginated fetch with a generous per-call timeout.
        # Pre-fix the unpaginated /all on a large section (10K+ shows
        # with locations + GUIDs) blew past the client-wide 10s timeout
        # and the worker logged 'no response'. Plex pages by passing
        # X-Plex-Container-Start / X-Plex-Container-Size as either
        # query params or headers — query params keep the call URL
        # self-describing in the docker log.
        page_size = 500
        per_call_timeout = 120.0
        out: list[PlexLibraryItem] = []
        skipped_no_rk = 0
        tag_counts: dict[str, int] = {}
        merged_root: "ET.Element | None" = None
        total_size: int | None = None
        offset = 0
        while True:
            page_params = dict(params)
            page_params["X-Plex-Container-Start"] = str(offset)
            page_params["X-Plex-Container-Size"] = str(page_size)
            r = self._get(url, params=page_params, timeout=per_call_timeout)
            if r is None or r.status_code != 200:
                log.warning(
                    "enumerate_section_items[%s]: section %s GET %s "
                    "(start=%d, size=%d) returned %s (%s)",
                    label, section_id, url, offset, page_size,
                    r.status_code if r is not None else "no response",
                    "no response" if r is None else (r.text[:200] if r.text else ""),
                )
                return [], None
            try:
                root = ET.fromstring(r.text)
            except ET.ParseError as e:
                log.warning(
                    "enumerate_section_items[%s]: section %s XML parse failed "
                    "at offset %d: %s",
                    label, section_id, offset, e,
                )
                return [], None
            if merged_root is None:
                merged_root = root
            container_size = int(root.get("size", "0") or 0)
            container_total = root.get("totalSize")
            if total_size is None and container_total is not None:
                try:
                    total_size = int(container_total)
                except ValueError:
                    total_size = None
            log.info(
                "enumerate_section_items[%s]: section %s (type=%s) "
                "page start=%d size=%d totalSize=%s",
                label, section_id, media_type, offset, container_size,
                container_total,
            )
            page_children = list(root)
            for el in page_children:
                tag_counts[el.tag] = tag_counts.get(el.tag, 0) + 1
                if el.tag not in ("Video", "Directory"):
                    continue
                rk = el.get("ratingKey")
                if not rk:
                    skipped_no_rk += 1
                    continue
                guids = _extract_guids(el)
                folder = _extract_folder_path(el)
                out.append(PlexLibraryItem(
                    rating_key=rk,
                    section_id=section_id,
                    media_type=media_type,
                    title=el.get("title", ""),
                    year=el.get("year", "") or "",
                    guid_imdb=guids.get("imdb"),
                    guid_tmdb=_safe_int(guids.get("tmdb")),
                    guid_tvdb=_safe_int(guids.get("tvdb")),
                    folder_path=folder,
                    has_theme=el.get("theme") is not None,
                ))
            if container_size < page_size:
                break  # last page
            if total_size is not None and (offset + container_size) >= total_size:
                break  # caught up to totalSize
            if not page_children:
                break  # defensive: empty page protects against infinite loop
            offset += container_size
        if not out:
            log.warning(
                "enumerate_section_items[%s]: section %s returned 0 usable items "
                "(media_type=%s, total children seen=%d, tag_counts=%s, "
                "skipped_no_rk=%d, total_size=%s)",
                label, section_id, media_type,
                sum(tag_counts.values()), tag_counts, skipped_no_rk, total_size,
            )
        return out, merged_root

    def get_item_paths(self, rating_key: str) -> list[str]:
        """Return all file paths for an item (for finding the media folder)."""
        r = self._get(f"/library/metadata/{rating_key}")
        if r is None or r.status_code != 200:
            return []
        try:
            root = ET.fromstring(r.text)
        except ET.ParseError:
            return []
        paths: list[str] = []
        for part in root.iter("Part"):
            f = part.get("file")
            if f:
                paths.append(f)
        # For TV shows, /metadata/<rk> returns the show; we need a Location
        for loc in root.iter("Location"):
            p = loc.get("path")
            if p:
                paths.append(p)
        return paths
