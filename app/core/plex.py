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

    def _get(self, path: str, params: dict | None = None) -> httpx.Response | None:
        try:
            return self._client.get(path, params=params)
        except httpx.HTTPError as e:
            log.debug("Plex GET failed: %s", e)
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
