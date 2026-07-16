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

import json
import logging
import re
import socket
from dataclasses import dataclass
from typing import Iterable
from urllib.parse import quote

import httpx

from .normalize import editions_equal, titles_equal, PlusMode

log = logging.getLogger(__name__)


def _safe_int(s) -> int | None:
    if s is None:
        return None
    try:
        return int(s)
    except (TypeError, ValueError):
        return None


def _extract_guids(item: dict) -> dict[str, str]:
    """v1.14.78: JSON-flavored. Plex JSON serves GUIDs as a `Guid`
    array on each item: `[{"id": "imdb://tt..."}, {"id": "tmdb://12345"}]`.
    Returns {agent: id_value}."""
    out: dict[str, str] = {}
    for g in (item.get("Guid") or []):
        gid = g.get("id", "")
        if "://" not in gid:
            continue
        agent, val = gid.split("://", 1)
        out[agent.lower()] = val
    return out


def _extract_folder_path(item: dict) -> str:
    """v1.14.78: JSON-flavored. For movies: parent dir of the first
    Media[].Part[].file. For shows: first Location[].path. Empty
    string when neither is available."""
    for media in (item.get("Media") or []):
        for part in (media.get("Part") or []):
            f = part.get("file", "")
            if f:
                i = f.rfind("/")
                return f[:i] if i > 0 else f
    for loc in (item.get("Location") or []):
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
    # v1.14.74: Plex's "content actually changed" watermark.
    # Captured at the start of every plex_enum so the next run
    # can compare and skip when nothing's moved. Plex bumps this
    # on adds/removes/edits but NOT on no-op rescans, so it's a
    # correct proxy for "is there anything new to fetch."
    # String to match how Plex serves it (epoch-ish int as text);
    # empty string when the field isn't present in the response.
    content_changed_at: str = ""


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
    # v1.12.112: Plex's theme URL verbatim — empty string when no theme
    # is claimed. Captured so plex_enum can detect URI changes between
    # enums (the trailing version suffix bumps when the underlying
    # theme content changes) and so the row can be HEAD-probed via
    # /library/metadata/{rating_key}/theme to verify Plex actually
    # serves content. The HEAD primitive lives in PlexClient.item_has_theme.
    plex_theme_uri: str = ""
    # v0.50.17: Plex's editionTitle metadata field verbatim ('' when unset).
    # DISPLAY-ONLY — plex_enum persists it for the library ED column when the
    # folder has no {edition-X} tag. Never folded into edition_key (scoping
    # stays folder-based), so a metadata-only edition can't drive placement.
    plex_edition_title: str = ""


@dataclass
class PlexConfig:
    url: str
    token: str
    movie_section: str
    tv_section: str
    enabled: bool = True
    timeout: float = 10.0


class PlexParseError(Exception):
    """v1.15.34: raised by PlexClient methods when a Plex HTTP call
    succeeded at the transport level but the response body couldn't
    be parsed (bad status code, malformed JSON, missing fields).

    Pre-fix the affected methods returned `[]` / `{}` on parse
    failures, indistinguishable from a legitimately empty Plex
    response. The plex_enum caller treated `[]` as "section is
    empty" → stamped `last_enum_content_changed_at` → next sync
    short-circuited the gate, so the section silently lost items
    until the user noticed coverage drift.

    Raising lets the caller's existing `try/except` count the
    failure as a section error + skip the post-success
    bookkeeping (last_enum_content_changed_at stamping)."""


# v0.51.175: Plex 500s on a theme POST above ~10MB. This was known and guarded at THREE
# separate sites (worker._PLEX_THEME_UPLOAD_CEILING_MB, orphan_scan._UPLOAD_CEILING_BYTES,
# and set_active_theme_via_reupload's inline copy — v1.21.99, after the operator's Watchmen
# re-upload 500'd and LPS "looked like it did nothing"). v0.51.174's loudness push was a
# FOURTH upload path written without the guard, so it fired the doomed POST and surfaced a
# raw 500. One constant, enforced at the chokepoint below, so every caller inherits it.
THEME_UPLOAD_CEILING_BYTES = 10 * 1024 * 1024


class PlexClient:
    def __init__(self, cfg: PlexConfig, plus_mode: PlusMode = "separator"):
        self.cfg = cfg
        self._plus = plus_mode
        client_id = f"motif-{socket.gethostname()}"
        # v1.14.78: switched Accept from XML → JSON (perf #3 from
        # the v1.14.71 research note). Plex's JSON encoder is
        # smaller per item and json.loads is C-implemented vs
        # ET.fromstring's partly-Python parse — ~10-25% faster
        # parse on the bulk-listing hot path. Plex has supported
        # JSON output on every endpoint since well before motif's
        # minimum supported version.
        self._headers = {
            "X-Plex-Token": cfg.token,
            "X-Plex-Product": "Motif",
            "X-Plex-Version": "1.0",
            "X-Plex-Client-Identifier": client_id,
            "Accept": "application/json",
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

    @staticmethod
    def _rk_path(rating_key: str, suffix: str = "") -> str:
        """v1.12.115: build /library/metadata/{rk}{suffix} with the
        rating_key URL-encoded. Plex's rating_keys are integers in
        practice, but defense-in-depth — if a stray '/' or '..' ever
        landed in the value, raw f-string interpolation would
        re-target the request to a different endpoint. quote(safe='')
        also encodes ':' and other reserved chars, so a malformed
        value 404s cleanly instead of silently mis-routing.
        """
        from urllib.parse import quote
        return f"/library/metadata/{quote(str(rating_key), safe='')}{suffix}"

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
            # v1.15.107: bumped log.debug → log.warning. _put backs
            # `refresh()` and `analyze()` — the calls that tell Plex
            # to re-scan a folder after motif's PURGE/DEL/UNMANAGE.
            # Pre-fix, when those PUTs failed (Plex offline, slow
            # response, auth) motif silently logged at DEBUG; the
            # user's "PURGE didn't make my row drop to '—'" mystery
            # had no breadcrumb in the default-INFO log. The
            # post-destructive-action refresh is the only path that
            # clears Plex's stale theme cache without waiting for
            # the next plex_enum cycle, so failures here are not
            # noise — they're the reason phantom-P sticks around.
            log.warning("Plex PUT %s failed: %s: %s",
                        path, type(e).__name__, e)
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

    def get_server_info(self) -> dict | None:
        """v1.24.21: lightweight connection probe for the settings TEST
        CONNECTION button. GET / returns the server identity (friendlyName,
        version, machineIdentifier). Returns None on any HTTP/parse failure
        (unreachable, or a bad token → 401) so the caller reports a clean
        'could not reach Plex' instead of a stack trace — mirrors
        discover_sections' defensive shape."""
        r = self._get("/")
        if r is None or r.status_code != 200:
            log.warning(
                "get_server_info: GET / returned %s (check Plex URL/token)",
                r.status_code if r is not None else "no response",
            )
            return None
        try:
            data = json.loads(r.text)
        except (ValueError, json.JSONDecodeError) as e:
            log.warning("get_server_info: JSON parse failed (%s)", e)
            return None
        c = data.get("MediaContainer", {}) or {}
        return {
            "friendly_name": c.get("friendlyName") or "",
            "version": c.get("version") or "",
            "machine_identifier": c.get("machineIdentifier") or "",
            "platform": c.get("platform") or "",
        }

    def discover_sections(self) -> list[PlexSection]:
        """Enumerate all library sections on the Plex server.

        Returns sections of type 'movie' and 'show'. Other types (music,
        photos) are ignored — motif has nothing to do with them.
        """
        r = self._get("/library/sections")
        if r is None or r.status_code != 200:
            # v1.15.36: log discovery HTTP failures so first-run
            # users can diagnose "no sections detected" reports.
            # Pre-fix the silent `[]` return looked the same as
            # a legit empty Plex install.
            log.warning(
                "discover_sections: GET /library/sections returned "
                "%s — treating as no sections (check Plex URL/token)",
                r.status_code if r is not None else "no response",
            )
            return []
        try:
            data = json.loads(r.text)
        except (ValueError, json.JSONDecodeError) as e:
            # v1.15.36: log JSON parse failures. Pre-fix silent
            # `[]` return made a corrupted Plex response look
            # like an empty install.
            log.warning(
                "discover_sections: JSON parse failed (%s) — "
                "treating as no sections",
                e,
            )
            return []
        container = data.get("MediaContainer", {}) or {}
        out: list[PlexSection] = []
        for el in (container.get("Directory") or []):
            stype = el.get("type", "")
            if stype not in ("movie", "show"):
                continue
            # v1.14.78: Plex JSON serves the section id as `key`
            # (string in JSON, like the XML attribute).
            section_id = str(el.get("key", "") or "")
            if not section_id:
                continue
            locations = [
                str(loc.get("path", "")) for loc in (el.get("Location") or [])
                if loc.get("path")
            ]
            out.append(PlexSection(
                section_id=section_id,
                uuid=str(el.get("uuid", "") or ""),
                title=str(el.get("title", "") or ""),
                type=stype,
                agent=str(el.get("agent", "") or ""),
                language=str(el.get("language", "") or ""),
                location_paths=locations,
                # v1.14.74: Plex serves contentChangedAt as a
                # top-level attribute. v1.14.78: in JSON it's
                # an int (Unix epoch); we stringify to keep the
                # storage shape stable with the v46 schema's
                # TEXT column. "" when absent so Plex builds
                # that don't surface the field still degrade
                # gracefully (the gate treats "" as "unknown" →
                # always run full enum).
                content_changed_at=(
                    str(el["contentChangedAt"])
                    if "contentChangedAt" in el
                    else ""
                ),
            ))
        return out

    def _parse_candidates(self, body_text: str) -> list[PlexCandidate]:
        """v1.14.78: JSON parse. Plex JSON serves both movies and
        shows under MediaContainer.Metadata as a unified array;
        the per-item `type` field distinguishes them. The XML
        version had two separate element tags (<Video> vs
        <Directory>) — JSON unifies them which is actually
        cleaner here since we don't care about the distinction."""
        try:
            data = json.loads(body_text)
        except (ValueError, json.JSONDecodeError) as e:
            # v1.17.1 (class 9): pre-fix this swallow returned []
            # silently — a Plex response that becomes malformed
            # (server-side bug, HTML error page, mid-stream
            # truncation) was indistinguishable from a legitimate
            # empty section. log.warning surfaces it in default
            # docker logs so the operator can correlate empty
            # enum results with Plex-side flakiness.
            log.warning(
                "Plex enum JSON parse failed (treating as empty "
                "section): %s. First 200 chars: %r",
                e, body_text[:200],
            )
            return []
        container = data.get("MediaContainer", {}) or {}
        items = container.get("Metadata") or []
        out: list[PlexCandidate] = []
        for el in items:
            rk = el.get("ratingKey")
            if rk is None:
                continue
            # JSON serves year as int; stringify to keep the
            # downstream PlexCandidate.year contract (str) stable.
            year_val = el.get("year", "")
            year_str = str(year_val) if year_val not in (None, "") else ""
            out.append(PlexCandidate(
                rating_key=str(rk),
                title=str(el.get("title", "") or ""),
                year=year_str,
                edition_title=str(el.get("editionTitle", "") or ""),
                # In JSON `theme` is a string (the URI) when present
                # and absent (key missing) when not. has_theme = key
                # is present.
                has_theme=("theme" in el),
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
                # v1.14.78: pre-check used XML markers
                # (<Video / <Directory) — for the JSON path
                # _parse_candidates handles empty bodies (returns
                # []) so we can drop the early-bail. Cheap when
                # nothing matches; the parse cost on a small
                # title-search response is negligible.
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
        status = self._head_or_get_status(self._rk_path(rating_key, "/theme"))
        if status == 200:
            return True
        # Fallback: parse the metadata response for a theme="..." attr.
        # v1.15.36: the v1.14.78 XML→JSON migration silently broke
        # this fallback — JSON serializes the attr as `"theme":"..."`
        # (no leading space, with colon) so the original ` theme="`
        # substring check never matched, making the fallback a
        # silent always-False return path. Now check both shapes
        # so the v1.14.78 migration's coverage gap is closed.
        r = self._get(self._rk_path(rating_key))
        if r is not None and r.status_code == 200:
            return ' theme="' in r.text or '"theme":"' in r.text
        return False

    def verify_theme_claim(self, rating_key: str) -> bool | None:
        """v1.12.112: source-of-truth check for `theme="..."` claims.

        Plex's /library/metadata/{rk}/theme endpoint serves the actual
        theme bytes. A HEAD against it tells us whether Plex really
        delivers content for the theme it advertises in metadata —
        distinguishing legitimate themes (themerr-plex embeds, Plex
        Pass cloud themes, sidecars) from stale metadata cache pointing
        at a file motif (or someone else) deleted.

        Returns True for 200 (verified live), False for 404 (verified
        stale), None for transient errors (network, timeout, 5xx) so
        the caller can leave verified_ok=NULL and try again next enum
        rather than penalizing the row for a network blip.
        """
        status = self._head_or_get_status(self._rk_path(rating_key, "/theme"))
        if status == 200:
            return True
        if status == 404:
            return False
        return None

    def fetch_theme_prefix(self, rating_key: str, n_bytes: int = 2048) -> bytes | None:
        """v1.13.40: Range-GET the first n_bytes of the theme stream.

        Used by the REPROBE PLEX THEMES backfill to disambiguate
        "Plex serves motif's sidecar" from "Plex serves an independent
        theme (cloud / themerr-plex embed)" without touching files.
        Compare the returned bytes to the same prefix of the local
        sidecar at local_files.file_path: matching → sidecar is
        what Plex serves; differing → +P composite.

        Returns the bytes on 200/206, None on any error. Plex serves
        themes as-is (no transcoding) so byte-equality is exact.
        """
        try:
            r = self._client.get(
                self._rk_path(rating_key, "/theme"),
                headers={"Range": f"bytes=0-{n_bytes - 1}"},
                timeout=15.0,
            )
            if r.status_code in (200, 206):
                return r.content[:n_bytes]
            return None
        except httpx.HTTPError as e:
            log.debug("Plex theme range-GET rk=%s failed: %s", rating_key, e)
            return None

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
        code = self._put(self._rk_path(rating_key, "/refresh"),
                         params={"force": "1"})
        if code in (200, 204):
            return True
        # Fallback: analyze (legacy behavior of merge-themes.sh)
        code = self._put(self._rk_path(rating_key, "/analyze"))
        return code in (200, 204)

    def enumerate_section_items(
        self, *, section_id: str, media_type: str,
        progress_callback=None,
        fallback_progress_callback=None,
    ) -> list[PlexLibraryItem]:
        """Full enumeration of one library section. Returns one PlexLibraryItem
        per content row, with GUIDs and folder paths extracted.

        For movies, folder_path is the parent dir of the first Part.file.
        For shows, folder_path is the show-level Location/path.

        Plex pages results via X-Plex-Container-Start / X-Plex-Container-Size;
        we walk pages of 500 to keep each fetch comfortably under the
        per-call timeout. The explicit type=1 (movie) / type=2 (show)
        filter ensures the response is at the section's primary level
        (movies / shows) instead of the leaf level (episodes).

        v1.12.128: optional progress_callback(received, total) fires
        after each page is appended so callers can drive a UI bar
        during the fetch. `total` is None on the first call (we don't
        learn it until Plex's first response carries totalSize),
        then concrete on subsequent calls. Exceptions inside the
        callback are caught — progress emission must never wedge
        the enum.

        v1.13.21 (was v1.13.20): optional fallback_progress_callback
        (done, total) fires every 25 items during the per-show
        /library/metadata fetch fallback (the slow-path that runs when
        the bulk listing missed Location elements). Pre-fix the bar
        sat at the bulk-list's 100% for the entire ~30s fallback,
        looking stuck. Same exception-swallow contract as the bulk
        progress_callback.
        """
        type_id = "1" if media_type == "movie" else "2"
        url = f"/library/sections/{section_id}/all"
        page_size = 500
        per_call_timeout = 120.0
        out: list[PlexLibraryItem] = []
        skipped_no_rk = 0
        tag_counts: dict[str, int] = {}
        total_size: int | None = None
        offset = 0
        while True:
            params = {
                "type": type_id,
                "includeGuids": "1",
                # v1.11.82: shows have no Part children at the section
                # level — their on-disk path is on a <Location> child of
                # the Directory element. Plex omits those by default;
                # without this flag motif stores folder_path='' for
                # every show, which kills sidecar (M) detection across
                # the entire TV/anime library.
                "includeLocations": "1",
                "X-Plex-Container-Start": str(offset),
                "X-Plex-Container-Size": str(page_size),
                # v1.14.75: trim per-item child elements motif
                # never reads. Default Plex response carries
                # ~80 lines per item (Genre/Writer/Director/
                # Country/Role/Producer/Similar/Collection/
                # Field/Label/Mood/Style/Image/Review tag lists)
                # — for a 4126-item TV section that's megabytes
                # of XML per page that we parse and immediately
                # discard. Excluded names from the Plex API
                # "Response Customization" docs; these are
                # metadata tag children of the item element, NOT
                # the item itself. Crucially we KEEP `Media`
                # because <Part> (which carries the file path
                # for movie folder_path extraction) is nested
                # inside <Media>; excluding Media would drop
                # Part too and break movie folder discovery.
                # Caveat: per the spec this is "treated as a
                # request, not a guarantee" — Plex builds that
                # ignore it just return the full payload, no
                # behavior break. Verified motif-side via the
                # _extract_guids + _extract_folder_path
                # callsites: neither reads any of the excluded
                # element types.
                "excludeElements": (
                    "Genre,Writer,Director,Country,Role,Producer,"
                    "Similar,Collection,Field,Label,Mood,Style,"
                    "Image,Review"
                ),
            }
            r = self._get(url, params=params, timeout=per_call_timeout)
            if r is None or r.status_code != 200:
                # v1.15.34: raise PlexParseError instead of returning
                # []. The pre-fix silent-empty path made the caller
                # treat a network/HTTP failure as "section is empty",
                # which then stamped last_enum_content_changed_at +
                # short-circuited future enums until coverage drift
                # was visible. plex_enum's existing try/except now
                # counts this as a section error + skips the
                # post-success bookkeeping.
                msg = (
                    f"enumerate_section_items: section {section_id} "
                    f"GET {url} (start={offset}, size={page_size}) "
                    f"returned "
                    f"{r.status_code if r is not None else 'no response'} "
                    f"({'no response' if r is None else (r.text[:200] if r.text else '')})"
                )
                log.warning(msg)
                raise PlexParseError(msg)
            # v1.14.78: JSON parse. Plex JSON's MediaContainer
            # carries `size` and `totalSize` as ints (not str
            # like the XML attributes), and the per-item array
            # is `Metadata` (unifying the XML's <Video> /
            # <Directory> split — the per-item `type` field
            # tells movies from shows). The downstream
            # PlexLibraryItem shape is unchanged.
            try:
                data = json.loads(r.text)
            except (ValueError, json.JSONDecodeError) as e:
                # v1.15.34: raise on JSON parse failure — see the
                # HTTP-failure branch above for the full rationale.
                msg = (
                    f"enumerate_section_items: section {section_id} "
                    f"JSON parse failed at offset {offset}: {e}"
                )
                log.warning(msg)
                raise PlexParseError(msg) from e
            container = data.get("MediaContainer", {}) or {}
            container_size = int(container.get("size", 0) or 0)
            container_total = container.get("totalSize")
            if total_size is None and container_total is not None:
                try:
                    total_size = int(container_total)
                except (TypeError, ValueError):
                    total_size = None
            log.info(
                "enumerate_section_items: section %s (type=%s) "
                "page start=%d size=%d totalSize=%s",
                section_id, media_type, offset, container_size, container_total,
            )
            page_children = container.get("Metadata") or []
            for el in page_children:
                # v1.14.78: tag_counts kept as a debug aid; in JSON
                # everything's under Metadata so the bucket is
                # always 'Metadata' for matched items.
                tag_counts["Metadata"] = tag_counts.get("Metadata", 0) + 1
                rk = el.get("ratingKey")
                if rk is None:
                    skipped_no_rk += 1
                    continue
                guids = _extract_guids(el)
                folder = _extract_folder_path(el)
                year_val = el.get("year", "")
                year_str = (
                    str(year_val) if year_val not in (None, "") else ""
                )
                theme_val = el.get("theme")
                out.append(PlexLibraryItem(
                    rating_key=str(rk),
                    section_id=section_id,
                    media_type=media_type,
                    title=str(el.get("title", "") or ""),
                    year=year_str,
                    guid_imdb=guids.get("imdb"),
                    guid_tmdb=_safe_int(guids.get("tmdb")),
                    guid_tvdb=_safe_int(guids.get("tvdb")),
                    folder_path=folder,
                    has_theme=theme_val is not None,
                    plex_theme_uri=str(theme_val) if theme_val else "",
                    plex_edition_title=str(el.get("editionTitle", "") or ""),
                ))
            # v1.12.128: emit per-page progress so the ops drawer's
            # bar can tick during the fetch instead of sitting at the
            # post-fetch stage_total=0 indeterminate-shimmer state.
            if progress_callback is not None:
                try:
                    progress_callback(len(out), total_size)
                except Exception as e:
                    log.debug("enumerate progress callback failed: %s", e)
            # v1.22.29 (audit): total_size is the AUTHORITATIVE page-count
            # signal. Pre-fix the `container_size < page_size` break fired
            # FIRST, so a short page mid-section (Plex's X-Plex-Container-Size
            # is advisory — a loaded server can return fewer items than
            # requested before totalSize is reached) truncated the section, and
            # the v1.18.89 reaper then treated the truncated set as authoritative
            # and DELETED the "missing" plex_items rows (moderate truncation
            # slips under its 20% abort guard) — silent data-loss. Fall back to
            # the short-page heuristic ONLY when totalSize is unavailable.
            if total_size is not None:
                if (offset + container_size) >= total_size:
                    break  # caught up to the authoritative totalSize
                if not page_children:
                    # v1.23.64 (audit): totalSize says more items remain but Plex
                    # returned an EMPTY Metadata page mid-section (a transient
                    # under load / mid-rescan). Pre-fix this fell through to the
                    # `if not page_children: break` below and exited with `out`
                    # TRUNCATED + NO error, so the v1.18.89 reaper treated the
                    # short set as authoritative and DELETED the unseen rows (the
                    # v1.22.29 short-page data-loss class through the empty-page
                    # door). Raise so plex_enum's try/except counts a section
                    # error + skips the reaper, instead of reaping live rows.
                    msg = (
                        f"enumerate_section_items: section {section_id} returned "
                        f"an empty page at offset {offset} while totalSize="
                        f"{total_size} indicates more items remain — refusing to "
                        f"report a truncated enumeration as complete"
                    )
                    log.warning(msg)
                    raise PlexParseError(msg)
            elif container_size < page_size:
                break  # no totalSize to trust → a short page is the last page
            if not page_children:
                break  # defensive: empty page protects against infinite loop
            if container_size <= 0:
                # v1.22.85: no-progress guard — a malformed page (totalSize
                # present, size missing/0, NON-empty Metadata) slipped every
                # break above and offset never advanced: the same request
                # looped forever, `out` growing until OOM (the v1.22.29
                # reorder removed the short-page break that incidentally
                # caught this).
                log.warning(
                    "enumerate pagination: page advertised size=0 with %d "
                    "items at offset %d — aborting walk to avoid an "
                    "infinite loop", len(page_children), offset)
                break
            offset += container_size
        if not out:
            log.warning(
                "enumerate_section_items: section %s returned 0 usable items "
                "(media_type=%s, total children seen=%d, tag_counts=%s, "
                "skipped_no_rk=%d, total_size=%s)",
                section_id, media_type,
                sum(tag_counts.values()), tag_counts, skipped_no_rk, total_size,
            )

        # v1.11.83: per-item folder_path fallback for shows.
        # Some Plex builds ignore includeLocations=1 on the section
        # listing and return Directory elements without their <Location>
        # children — folder_path comes back '' for the entire library,
        # which kills sidecar (M) detection. Catch any item still
        # missing folder_path and re-hit /library/metadata/<rk> to
        # recover the path. We skip movies because their
        # <Video>/<Media>/<Part> branch in _extract_folder_path always
        # works on the bulk listing.
        # v1.14.76: switched from per-item ThreadPool (16 workers,
        # one HTTP call per missing rk) to BULK metadata fetch
        # (`/library/metadata/{rk1,rk2,…}` with 50 ids per call,
        # 4 concurrent batches). Plex's API accepts comma-
        # separated rating_keys and returns one element per id
        # in a single MediaContainer — so 4126 items drop from
        # ~258 batched HTTP calls (16-thread pool, 4126 reqs)
        # to ~83 batched HTTP calls (50 ids × ~83 batches),
        # then further to ~21 wall-clock batches with 4-way
        # concurrency. the user's v1.14.74 logs showed the
        # fallback firing on every show section (his Plex
        # ignores includeLocations=1) — 4126 items took ~68s
        # under the per-item pool; bulk should drop that to
        # single-digit seconds.
        if media_type == "show":
            missing = [i for i, it in enumerate(out) if not it.folder_path]
            if missing:
                log.info(
                    "enumerate_section_items: section %s — %d show(s) "
                    "missing folder_path on bulk listing, falling back "
                    "to BULK /library/metadata fetch",
                    section_id, len(missing),
                )
                fallback_total = len(missing)
                missing_rks = [out[i].rating_key for i in missing]

                def _on_batch(filled_so_far: int) -> None:
                    if fallback_progress_callback is None:
                        return
                    try:
                        fallback_progress_callback(
                            filled_so_far, fallback_total)
                    except Exception as e:  # noqa: BLE001
                        log.debug(
                            "fallback progress callback failed: %s", e)

                rk_to_path = self.get_item_paths_bulk(
                    missing_rks, on_batch_complete=_on_batch)
                for idx in missing:
                    p = rk_to_path.get(out[idx].rating_key, "")
                    if p:
                        out[idx] = PlexLibraryItem(
                            **{**out[idx].__dict__, "folder_path": p}
                        )
                # Final emit so the bar definitely lands at 100%
                # even if the last batch's resolved-count was a
                # short partial.
                if fallback_progress_callback is not None:
                    try:
                        fallback_progress_callback(
                            fallback_total, fallback_total)
                    except Exception as e:  # noqa: BLE001
                        log.debug(
                            "fallback progress callback failed: %s", e)
                still_missing = sum(1 for it in out if not it.folder_path)
                log.info(
                    "enumerate_section_items: section %s — per-item "
                    "fallback recovered %d/%d, still missing %d",
                    section_id, len(missing) - still_missing,
                    len(missing), still_missing,
                )
        return out

    # v1.18.0: Plex Collections enumeration + theme upload/delete.
    # Collections live at `/library/sections/{id}/collections`
    # (parallel to `/library/sections/{id}/all` for movies/shows).
    # They share the metadata-id namespace with movies/shows — each
    # collection has its own `ratingKey` — so the same
    # `/library/metadata/{rk}/themes` POST endpoint that
    # themerr-plex used for movies/shows works for collections too
    # (verified via the v1.17.x audit of themerr-plex's source +
    # the Plex OpenAPI). No on-disk sidecar concept applies since
    # collections have no media folder; the audio bytes live in
    # Plex's own metadata bundle.

    def enumerate_collections_for_section(
        self, *, section_id: str,
    ) -> list["PlexLibraryItem"]:
        """v1.18.0: page through every collection in a Plex section.

        Returns one PlexLibraryItem per collection with
        media_type='collection', folder_path='' (collections have
        no folder), and guid_tmdb populated from the collection's
        Guid children (TMDB collection IDs — the ID space
        ThemerrDB tracks via `movie_collections/themoviedb/`).

        Reuses PlexLibraryItem (vs a separate PlexCollection
        dataclass) so plex_enum's upsert path is unified — the
        downstream plex_items.media_type CHECK was widened in
        schema v55 to accept 'collection'.

        Plex paginates collections the same way as the main
        /all listing (X-Plex-Container-Start / -Size); typical
        installs have << 500 collections so a single page often
        suffices. We loop anyway for correctness.

        Errors raise PlexParseError — same contract as
        enumerate_section_items so the plex_enum caller's
        existing try/except routes through identically.
        """
        url = f"/library/sections/{section_id}/collections"
        page_size = 500
        per_call_timeout = 60.0
        out: list[PlexLibraryItem] = []
        skipped_no_rk = 0
        total_size: int | None = None
        offset = 0
        while True:
            params = {
                "includeGuids": "1",
                "X-Plex-Container-Start": str(offset),
                "X-Plex-Container-Size": str(page_size),
                # Same payload trim as enumerate_section_items —
                # collections also carry the verbose tag-child
                # tree (Genre / Director / Label / etc.) that
                # motif never reads. Keep `Media` for parity with
                # the main listing's pattern even though
                # collections don't typically have <Part> elements.
                "excludeElements": (
                    "Genre,Writer,Director,Country,Role,Producer,"
                    "Similar,Field,Label,Mood,Style,Image,Review"
                ),
            }
            r = self._get(url, params=params, timeout=per_call_timeout)
            if r is None or r.status_code != 200:
                msg = (
                    f"enumerate_collections_for_section: section "
                    f"{section_id} GET {url} (start={offset}, "
                    f"size={page_size}) returned "
                    f"{r.status_code if r is not None else 'no response'} "
                    f"({'no response' if r is None else (r.text[:200] if r.text else '')})"
                )
                log.warning(msg)
                raise PlexParseError(msg)
            try:
                data = json.loads(r.text)
            except (ValueError, json.JSONDecodeError) as e:
                msg = (
                    f"enumerate_collections_for_section: section "
                    f"{section_id} JSON parse failed at offset "
                    f"{offset}: {e}"
                )
                log.warning(msg)
                raise PlexParseError(msg) from e
            container = data.get("MediaContainer", {}) or {}
            container_size = int(container.get("size", 0) or 0)
            container_total = container.get("totalSize")
            if total_size is None and container_total is not None:
                try:
                    total_size = int(container_total)
                except (TypeError, ValueError):
                    total_size = None
            log.info(
                "enumerate_collections_for_section: section %s "
                "page start=%d size=%d totalSize=%s",
                section_id, offset, container_size, container_total,
            )
            page_children = container.get("Metadata") or []
            for el in page_children:
                rk = el.get("ratingKey")
                if rk is None:
                    skipped_no_rk += 1
                    continue
                guids = _extract_guids(el)
                # For collections, Plex serves the TMDB collection
                # ID under `tmdb://` in the Guid children. The
                # ThemerrDB `movie_collections/themoviedb/{id}.json`
                # path uses exactly this ID space — direct match.
                guid_tmdb = _safe_int(guids.get("tmdb"))
                theme_val = el.get("theme")
                # year — collections don't have a release date,
                # but Plex sometimes synthesizes one from the
                # earliest member's release. Capture if present
                # for display in motif's library row; harmless if
                # empty.
                year_val = el.get("year", "")
                year_str = (
                    str(year_val) if year_val not in (None, "") else ""
                )
                out.append(PlexLibraryItem(
                    rating_key=str(rk),
                    section_id=section_id,
                    media_type="collection",
                    title=str(el.get("title", "") or ""),
                    year=year_str,
                    guid_imdb=None,        # not present on collections
                    guid_tmdb=guid_tmdb,
                    guid_tvdb=None,        # not present on collections
                    folder_path="",        # collections have no folder
                    has_theme=theme_val is not None,
                    plex_theme_uri=str(theme_val) if theme_val else "",
                ))
            # v1.22.29 (audit): total_size authoritative (see
            # enumerate_section_items) — a short page must not truncate the walk.
            if total_size is not None:
                if (offset + container_size) >= total_size:
                    break
                if not page_children:
                    # v1.23.95 (audit): totalSize says more collections remain
                    # but Plex returned an EMPTY page mid-walk → refuse to report
                    # a truncated enumeration as complete. Pre-fix this fell to
                    # the bare `if not page_children: break` below and exited with
                    # `out` TRUNCATED + NO error, so the v1.18.89 reaper deleted
                    # the unseen live collection rows. v1.23.64 added this guard
                    # to enumerate_section_items; the collections twin was missed.
                    msg = (
                        f"enumerate_collections_for_section: section {section_id} "
                        f"returned an empty page at offset {offset} while "
                        f"totalSize={total_size} indicates more collections "
                        f"remain — refusing to report a truncated enumeration as "
                        f"complete"
                    )
                    log.warning(msg)
                    raise PlexParseError(msg)
            elif container_size < page_size:
                break
            if not page_children:
                break  # defensive infinite-loop guard
            if container_size <= 0:
                # v1.22.85: no-progress guard (see enumerate_section_items).
                log.warning(
                    "collections pagination: page advertised size=0 with %d "
                    "items at offset %d — aborting walk", 
                    len(page_children), offset)
                break
            offset += container_size
        if not out and skipped_no_rk > 0:
            log.warning(
                "enumerate_collections_for_section: section %s — "
                "all %d items skipped (no ratingKey)",
                section_id, skipped_no_rk,
            )
        return out

    def upload_collection_theme(
        self, *, rating_key: str, audio_bytes: bytes,
        content_type: str = "audio/mpeg",
    ) -> "tuple[bool, int | None, str]":
        """v1.18.0: POST audio bytes to
        `/library/metadata/{rating_key}/themes` to attach a theme
        to a Plex collection (or any metadata item). Used by the
        worker's place adapter for media_type='collection' rows.

        v1.18.68: return shape is `(ok, status_code, body_snippet)`
        — pre-fix bare bool dropped the HTTP status + response body
        snippet on the floor, so failed uploads stamped the job
        with the unhelpful generic "Plex collection upload failed
        for rk=X" note. the user's repro: a 27MB file rejected with
        HTTP 500 left him guessing whether it was a Plex bug, a
        network glitch, or a size cap. Now the worker can format
        a note including the HTTP code + body size for actionable
        triage. status_code is None when both attempts raised
        before returning a response (network error / timeout).

        Errors are logged (not raised) — same fire-and-forget
        contract as other PlexClient mutating methods.

        v1.18.9: tries multipart/form-data first (matches the
        upload format python-plexapi's Collection.uploadTheme()
        uses for local files), falls back to raw-body POST if
        multipart returns a 4xx. Some Plex builds reject raw-
        body uploads with 400 because they can't parse the body
        without a multipart wrapper; older builds accept raw
        body. Trying multipart first matches the Plex web UI's
        upload behavior + python-plexapi's documented path.

        Verbose logging at every step so the operator can see
        the failure shape directly without enabling debug
        logging. Each upload attempt logs HTTP method + URL +
        body size + response status + truncated response body
        + total wall-clock duration."""
        import time as _t
        url = f"/library/metadata/{rating_key}/themes"
        body_size = len(audio_bytes)
        # v0.51.175: refuse a POST Plex will 500 on. Callers that already pre-check
        # (worker place, orphan_scan, set_active_theme_via_reupload) never reach this —
        # it's the backstop that stops the next new upload path repeating v0.51.174.
        if body_size > THEME_UPLOAD_CEILING_BYTES:
            log.warning(
                "upload_collection_theme: rk=%s SKIPPED — %d bytes is over Plex's ~%dMB "
                "theme-upload ceiling; Plex answers these with HTTP 500",
                rating_key, body_size, THEME_UPLOAD_CEILING_BYTES // (1024 * 1024),
            )
            return (False, None,
                    f"over_ceiling: {body_size} bytes exceeds Plex's "
                    f"~{THEME_UPLOAD_CEILING_BYTES // (1024 * 1024)}MB theme-upload "
                    f"ceiling (Plex answers these with HTTP 500)")
        log.info(
            "upload_collection_theme: rk=%s starting upload "
            "(url=%s, body=%d bytes, content_type=%s)",
            rating_key, url, body_size, content_type,
        )

        # Attempt 1: multipart POST. python-plexapi's
        # Collection.uploadTheme() uses this shape via
        # `_server.query(key, method=POST, files={'file': fp})`.
        # Strip Content-Type from headers — httpx generates a
        # multipart/form-data boundary automatically and a stale
        # Content-Type would override it.
        headers_multipart = dict(self._headers)
        headers_multipart.pop("Accept", None)
        headers_multipart.pop("Content-Type", None)
        t0 = _t.monotonic()
        try:
            r = self._client.post(
                url,
                files={"file": ("theme.mp3", audio_bytes, content_type)},
                headers=headers_multipart,
                timeout=60.0,
            )
            elapsed = _t.monotonic() - t0
        except Exception as e:  # noqa: BLE001
            elapsed = _t.monotonic() - t0
            log.warning(
                "upload_collection_theme: rk=%s multipart "
                "exception after %.1fs: %s",
                rating_key, elapsed, e,
            )
            r = None

        if r is not None:
            ok = 200 <= r.status_code < 300
            r_body_snip = r.text[:200] if r.text else "(empty)"
            log.info(
                "upload_collection_theme: rk=%s multipart "
                "HTTP %s in %.1fs (body=%s)",
                rating_key, r.status_code, elapsed, r_body_snip,
            )
            if ok:
                return True, r.status_code, r_body_snip

        # Attempt 2: raw-body POST fallback. Mirrors the original
        # v1.18.0 implementation; some Plex builds want this
        # shape (no multipart wrapper, just bytes with a
        # Content-Type header).
        log.info(
            "upload_collection_theme: rk=%s multipart failed — "
            "falling back to raw-body POST",
            rating_key,
        )
        headers_raw = dict(self._headers)
        headers_raw["Content-Type"] = content_type
        headers_raw.pop("Accept", None)
        t0 = _t.monotonic()
        try:
            r2 = self._client.post(
                url, content=audio_bytes,
                headers=headers_raw, timeout=60.0,
            )
            elapsed = _t.monotonic() - t0
        except Exception as e:  # noqa: BLE001
            elapsed = _t.monotonic() - t0
            log.warning(
                "upload_collection_theme: rk=%s raw-body "
                "exception after %.1fs: %s",
                rating_key, elapsed, e,
            )
            return False, None, f"exception: {type(e).__name__}: {e}"

        ok2 = 200 <= r2.status_code < 300
        r2_body_snip = r2.text[:200] if r2.text else "(empty)"
        log.info(
            "upload_collection_theme: rk=%s raw-body HTTP %s "
            "in %.1fs (body=%s)",
            rating_key, r2.status_code, elapsed, r2_body_snip,
        )
        return ok2, r2.status_code, r2_body_snip

    def get_field_locks(self, *, rating_key: str) -> dict:
        """v0.51.178: READ which metadata fields Plex has locked on this item.

        Exists because v0.51.177 asserted the theme field's lock was not what stopped a
        refresh from re-reading a changed sidecar — and based that on an unlock PUT
        returning 200. A 200 is not evidence the flag moved; it is the same
        status-code-as-proof mistake this whole arc keeps making. So read the flag.

        `theme_locked` is a TRISTATE: None means "could not read", which must never be
        allowed to read as "unlocked" (class-9). Never raises."""
        try:
            r = self._client.get(self._rk_path(rating_key, ""),
                                 headers=self._headers, timeout=30.0)
        except Exception as e:  # noqa: BLE001
            log.warning("get_field_locks: rk=%s transport error: %s", rating_key, e)
            return {"ok": False, "http_status": None, "error": f"transport: {e!r}",
                    "locked_fields": None, "theme_locked": None}
        try:
            body = r.json()
        except (ValueError, json.JSONDecodeError):
            return {"ok": False, "http_status": r.status_code,
                    "error": "non-JSON body", "locked_fields": None,
                    "theme_locked": None}
        meta = ((body.get("MediaContainer") or {}).get("Metadata") or []) \
            if isinstance(body, dict) else []
        if not meta:
            return {"ok": False, "http_status": r.status_code,
                    "error": "no Metadata for this rating_key",
                    "locked_fields": None, "theme_locked": None}
        locked = [f.get("name") for f in (meta[0].get("Field") or []) if f.get("locked")]
        return {"ok": 200 <= r.status_code < 300, "http_status": r.status_code,
                "error": None, "locked_fields": locked,
                "theme_locked": "theme" in locked}

    def set_theme_field_lock(self, *, rating_key: str, locked: bool,
                             shape: str = "metadata", section_id: str | None = None,
                             plex_type: int | None = None) -> int | None:
        """v0.51.177 / v0.51.178: lock/unlock the item's `theme` field.

        Needed because delete_collection_theme's own docstring records that Plex's DELETE
        on singular /theme "will also lock the field". A LOCKED field is precisely what
        stops an agent from writing it — so a locked theme could be why v0.51.173 measured
        a refresh failing to re-read a changed sidecar. The upload path never noticed
        because a POST to plural /themes overrides the lock (v1.18.33).

        v0.51.178: `shape`. v0.51.177 only ever tried the metadata-endpoint form and
        trusted its 200, but Plex's field-lock API conventionally goes through the SECTION
        endpoint with type= + id=, so the metadata form may be a silent no-op. Which one
        actually moves the flag is an empirical question — callers MUST verify with
        get_field_locks. The status code proves nothing.

        Returns the HTTP status (None on transport error). Best-effort, never raises."""
        val = "1" if locked else "0"
        if shape == "section":
            if section_id is None or plex_type is None:
                log.warning("set_theme_field_lock: section shape needs section_id + "
                            "plex_type; got %r/%r", section_id, plex_type)
                return None
            from urllib.parse import quote
            return self._put(f"/library/sections/{quote(str(section_id), safe='')}/all",
                             params={"type": plex_type, "id": rating_key,
                                     "theme.locked": val})
        return self._put(self._rk_path(rating_key, ""), params={"theme.locked": val})

    def delete_collection_theme(self, *, rating_key: str) -> bool:
        """v1.18.0 / v1.18.36: clear the currently-serving theme
        association via DELETE on the SINGULAR
        `/library/metadata/{rating_key}/theme` endpoint.

        v1.18.36 fixed the URL — pre-fix this hit PLURAL `/themes`
        which Plex's router has no DELETE handler for (returned
        404 every time). Latent bug since v1.18.0: every
        collection UNPLACE/PURGE silently no-op'd against Plex.
        v1.18.33's probe confirmed singular works (HTTP 200,
        entries in /themes survive, only active selection
        cleared). v1.18.31-35 series probe data + python-plexapi's
        deleteTheme attestation guided the URL choice.

        Side effect per Plex's OpenAPI: "This operation will also
        lock the field." But motif's subsequent POST to plural
        /themes (the upload path) auto-overrides the lock and
        re-selects, per v1.18.33 probe.

        Same return contract as upload_collection_theme."""
        url = f"/library/metadata/{rating_key}/theme"
        try:
            r = self._client.delete(url, headers=self._headers, timeout=30.0)
        except Exception as e:  # noqa: BLE001
            log.warning(
                "delete_collection_theme: rk=%s exception: %s",
                rating_key, e,
            )
            return False
        ok = 200 <= r.status_code < 300
        if not ok:
            log.warning(
                "delete_collection_theme: rk=%s HTTP %s — %s",
                rating_key, r.status_code,
                r.text[:200] if r.text else "(no body)",
            )
        return ok

    # v1.18.23: generalized aliases for theme upload/delete. The
    # `/library/metadata/{rk}/themes` endpoint works for movies,
    # shows, AND collections — themerr-plex used it for movies/
    # shows before motif's v1.18.0 added the collection-specific
    # name. The collection-specific function names stay for back-
    # compat with the v1.18.0+ call sites; the generalized aliases
    # let v1.18.23+ callers (movie/TV API push) skip the
    # collection-specific naming.
    def upload_theme(
        self, *, rating_key: str, audio_bytes: bytes,
        content_type: str = "audio/mpeg",
    ) -> "tuple[bool, int | None, str]":
        """v1.18.23: alias for upload_collection_theme. Delegates
        unchanged — the HTTP endpoint is universal across media
        types so no logic differs.

        v1.18.68: return shape follows upload_collection_theme —
        `(ok, status_code, body_snippet)`."""
        return self.upload_collection_theme(
            rating_key=rating_key,
            audio_bytes=audio_bytes,
            content_type=content_type,
        )

    def delete_theme(self, *, rating_key: str) -> bool:
        """v1.18.23: alias for delete_collection_theme."""
        return self.delete_collection_theme(rating_key=rating_key)

    def try_targeted_delete_theme(
        self, *, item_rating_key: str, theme_rating_key: str,
        shape: str = "query",
    ) -> dict:
        """v1.18.32: characterise Plex's targeted-DELETE shape for
        a specific theme entry on an item. Read mostly — actually
        attempts a DELETE, so the caller must be sure they want
        the entry gone. Used by the probe endpoint to figure out
        which URL shape Plex accepts before v1.18.33+ wires the
        targeted-delete into the real `delete_theme` path.

        Four shape candidates we try, in order of likeliness
        (singular_delete is the python-plexapi-attested shape
        added in v1.18.33):

          - `singular_delete` — DELETE /library/metadata/{rk}/theme
                                (SINGULAR. python-plexapi's
                                 `deleteTheme()` uses this URL.
                                 theme_rk is ignored — endpoint
                                 deletes the CURRENTLY-SERVING
                                 theme association, leaves the
                                 /themes collection intact.)
          - `query`        — DELETE /library/metadata/{rk}/themes?url=<encoded>
                             (plural; v1.18.32 confirmed 404)
          - `subpath`      — DELETE /library/metadata/{rk}/themes/<encoded>
                             (plural; v1.18.32 confirmed 404)
          - `put_unselect` — PUT /library/metadata/{rk}/themes?url=<encoded>&unset=1
                             (plural; v1.18.32 confirmed 404)

        Returns the same debug-shaped dict as get_themes plus the
        attempted URL + method so the operator can paste a clear
        trace into a follow-up discussion.
        """
        from urllib.parse import quote
        item_path_plural = self._rk_path(item_rating_key, "/themes")
        item_path_singular = self._rk_path(item_rating_key, "/theme")
        encoded_theme = quote(theme_rating_key, safe="")
        method = "DELETE"
        if shape == "singular_delete":
            url = item_path_singular
        elif shape == "query":
            url = f"{item_path_plural}?url={encoded_theme}"
        elif shape == "subpath":
            url = f"{item_path_plural}/{encoded_theme}"
        elif shape == "put_unselect":
            url = f"{item_path_plural}?url={encoded_theme}&unset=1"
            method = "PUT"
        else:
            return {
                "ok": False, "http_status": None,
                "error": f"unknown shape: {shape!r}",
                "attempted_url": None, "attempted_method": None,
                "body": None,
            }
        try:
            if method == "DELETE":
                r = self._client.delete(
                    url, headers=self._headers, timeout=30.0,
                )
            else:
                r = self._client.put(
                    url, headers=self._headers, timeout=30.0,
                )
        except Exception as e:  # noqa: BLE001
            return {
                "ok": False, "http_status": None,
                "error": f"transport: {e!r}",
                "attempted_url": url,
                "attempted_method": method,
                "body": None,
            }
        body: dict | str
        try:
            body = r.json()
        except (ValueError, json.JSONDecodeError):
            body = r.text[:4000] if r.text else ""
        return {
            "ok": 200 <= r.status_code < 300,
            "http_status": r.status_code,
            "error": None,
            "attempted_url": url,
            "attempted_method": method,
            "body": body,
        }

    def set_active_theme_via_reupload(
        self, *, rating_key: str, theme_rating_key: str,
    ) -> bool:
        """v1.18.36: re-select an existing theme entry as Plex's
        active serving theme. The only viable "select" primitive
        — Plex's HTTP API has no direct way to mark an existing
        theme entry as `selected: true` (POST/PUT singular `?url=`
        confirmed broken in v1.18.34: POST 404, PUT 500).

        The trick: Plex content-dedupes uploads by SHA-1. We GET
        the existing entry's audio bytes from /file?url=<rk>,
        POST them back via /themes (motif's normal upload path),
        and Plex re-uses the existing entry (same hash) and
        marks it `selected: true`. Confirmed on v1.18.35 probe
        against 12 Monkeys (rk=124233): 2.37MB fetch + upload,
        size stayed at 1, entry kept selected.

        Bandwidth: ~1MB round-trip per call. Acceptable for
        manual user actions (LPS, SWITCH).

        Returns True on full success, False if either step fails.
        Diagnostics live in the v1.18.35 probe-reupload-theme
        endpoint if a debug trace is needed.
        """
        out = self._fetch_and_reupload_theme(
            item_rating_key=rating_key,
            theme_rating_key=theme_rating_key,
        )
        if not out["ok"]:
            # v1.21.99: over_ceiling is an EXPECTED skip, not a failure —
            # the >10MB fallback can't be re-selected via re-upload; on a
            # sidecar LPS Plex's own theme serves via auto-fallback. Log it
            # at INFO so the operator sees "working as intended", not a 500.
            if out.get("step_failed") == "over_ceiling":
                _mb = round(out.get("fetch", {}).get("bytes", 0) / 1048576, 1)
                log.info(
                    "set_active_theme_via_reupload: rk=%s theme_rk=%s "
                    "skipped re-select — fallback theme %sMB is over Plex's "
                    "~10MB upload ceiling; Plex's own theme serves via "
                    "auto-fallback", rating_key, theme_rating_key, _mb,
                )
            else:
                log.warning(
                    "set_active_theme_via_reupload: rk=%s theme_rk=%s "
                    "step_failed=%s",
                    rating_key, theme_rating_key, out.get("step_failed"),
                )
        return out["ok"]

    def try_reupload_existing_theme(
        self, *, item_rating_key: str, theme_rating_key: str,
    ) -> dict:
        """v1.18.35 probe wrapper, retained for diagnostic use by
        the /api/admin/probe-reupload-theme endpoint. Delegates
        to the shared `_fetch_and_reupload_theme` helper that
        v1.18.36's production `set_active_theme_via_reupload`
        also uses — single code path, no drift risk."""
        return self._fetch_and_reupload_theme(
            item_rating_key=item_rating_key,
            theme_rating_key=theme_rating_key,
        )

    def _fetch_and_reupload_theme(
        self, *, item_rating_key: str, theme_rating_key: str,
    ) -> dict:
        """v1.18.35: the re-upload trick. Plex content-dedupes
        uploads (same SHA-1 → same entry, marked selected). So:

          1. GET the audio bytes of an existing theme entry from
             /library/metadata/{rk}/file?url=<theme-uri>
          2. POST those bytes back to /library/metadata/{rk}/themes
             (motif's normal upload endpoint)
          3. Plex sees the same hash → re-uses the existing entry
             → marks it `selected: true`

        Verified prereqs:
          - v1.18.33: DELETE singular clears the active selection.
          - v1.18.34: POST/PUT `?url=...` does NOT select existing
            entries (POST 404, PUT 500). No native select API.
          - Earlier probes: Plex content-dedupes uploads (M*A*S*H
            re-pushed 3x but only 1 entry stayed in /themes with
            the same hash each time).

        This is the only viable path for LET PLEX SERVE to leave
        a row serving Plex's pre-motif theme without user
        intervention. Bandwidth: ~1MB round-trip per LPS click.

        Returns rich debug dict: fetch result + upload result.
        Never raises — both phases land in the return dict so
        the probe endpoint can surface a clear trace.
        """
        from urllib.parse import quote
        # Step 1: fetch existing audio bytes.
        file_url = (
            f"{self._rk_path(item_rating_key, '/file')}"
            f"?url={quote(theme_rating_key, safe='')}"
        )
        try:
            r = self._client.get(
                file_url, headers=self._headers, timeout=60.0,
            )
        except Exception as e:  # noqa: BLE001
            return {
                "ok": False,
                "step_failed": "fetch",
                "fetch": {
                    "http_status": None,
                    "error": f"transport: {e!r}",
                    "url": file_url, "bytes": 0,
                },
                "upload": None,
            }
        if not (200 <= r.status_code < 300):
            return {
                "ok": False,
                "step_failed": "fetch",
                "fetch": {
                    "http_status": r.status_code,
                    "error": None,
                    "url": file_url,
                    "bytes": 0,
                    "body_preview": (
                        r.text[:300] if r.text else "(empty)"
                    ),
                },
                "upload": None,
            }
        audio_bytes = r.content
        # v1.21.99: the re-upload POST hits Plex's ~10MB theme-upload
        # ceiling exactly like the place path's plex_upload does (worker.
        # _PLEX_THEME_UPLOAD_CEILING_MB → HTTP 500). A >10MB fallback theme
        # CAN'T be re-selected via this trick. For LET PLEX SERVE on a
        # SIDECAR row that's fine: removing the theme.mp3 already lets Plex's
        # Local Media Assets fall back to its own (cloud / agent) theme — the
        # selected /themes entry — so no re-select is needed. Skip the doomed
        # 15MB POST (the user's Watchmen Theatrical Cut: rk=417795 re-upload
        # 500'd, restore "failed", LPS looked like it did nothing) and report
        # it distinctly so the caller logs "Plex's own theme serves" instead
        # of a scary upload 500.
        # v0.51.175: one constant now (was an inline 4th copy of the same 10MB).
        if len(audio_bytes) > THEME_UPLOAD_CEILING_BYTES:
            return {
                "ok": False,
                "step_failed": "over_ceiling",
                "fetch": {
                    "http_status": r.status_code, "error": None,
                    "url": file_url, "bytes": len(audio_bytes),
                    "body_preview": None,
                },
                "upload": None,
            }
        # Step 2: re-upload via existing upload_collection_theme
        # (universal across media types per v1.18.23).
        # v1.18.68: upload_collection_theme now returns a tuple
        # (ok, status_code, body_snippet) — destructure rather than
        # treating the result as bool (a non-empty tuple is always
        # truthy, so the pre-fix `upload_ok = self.upload_...()`
        # would have evaluated True even on a failed upload).
        upload_ok, upload_status, upload_body = (
            self.upload_collection_theme(
                rating_key=item_rating_key,
                audio_bytes=audio_bytes,
            )
        )
        return {
            "ok": upload_ok,
            "step_failed": None if upload_ok else "upload",
            "fetch": {
                "http_status": r.status_code,
                "error": None,
                "url": file_url,
                "bytes": len(audio_bytes),
                "body_preview": None,
            },
            "upload": {
                "ok": upload_ok,
                "http_status": upload_status,
                "body_preview": upload_body,
                "url": (
                    f"{self._rk_path(item_rating_key, '/themes')} "
                    "(multipart POST)"
                ),
            },
        }

    def try_set_active_theme(
        self, *, item_rating_key: str, theme_rating_key: str,
        method: str = "post",
    ) -> dict:
        """v1.18.34: probe whether POST/PUT on the singular
        `/library/metadata/{rk}/theme?url=<theme_rk>` endpoint
        selects a specific theme entry as the row's active theme.

        OpenAPI summary on this path: "Set an item's artwork,
        theme, etc". python-plexapi's setTheme() raises
        NotImplementedError — they never wrapped this, but the
        underlying HTTP API is documented in Plex's spec.

        Method options (both should work per spec; we test both
        to see which Plex actually honours):
          - 'post' → POST /library/metadata/{rk}/theme?url=...
          - 'put'  → PUT  /library/metadata/{rk}/theme?url=...

        If this works, v1.18.35's LET PLEX SERVE flow becomes:
        DELETE singular (clear motif's selection) → POST select
        on first non-motif entry. No metadata refresh needed,
        no unlock-then-refresh dance, no user intervention.

        Read mostly; only affects which entry is `selected:
        true`. No DELETE of entries.
        """
        from urllib.parse import quote
        url = (
            f"{self._rk_path(item_rating_key, '/theme')}"
            f"?url={quote(theme_rating_key, safe='')}"
        )
        if method == "post":
            http_method = "POST"
            send = self._client.post
        elif method == "put":
            http_method = "PUT"
            send = self._client.put
        else:
            return {
                "ok": False, "http_status": None,
                "error": f"unknown method: {method!r}",
                "attempted_url": None, "attempted_method": None,
                "body": None,
            }
        try:
            r = send(url, headers=self._headers, timeout=30.0)
        except Exception as e:  # noqa: BLE001
            return {
                "ok": False, "http_status": None,
                "error": f"transport: {e!r}",
                "attempted_url": url,
                "attempted_method": http_method,
                "body": None,
            }
        body: dict | str
        try:
            body = r.json()
        except (ValueError, json.JSONDecodeError):
            body = r.text[:4000] if r.text else ""
        return {
            "ok": 200 <= r.status_code < 300,
            "http_status": r.status_code,
            "error": None,
            "attempted_url": url,
            "attempted_method": http_method,
            "body": body,
        }

    def get_themes(self, *, rating_key: str) -> dict:
        """v1.18.31: read-only probe of `/library/metadata/{rk}/themes`.
        Returns a debug-shaped dict so the admin probe endpoint can
        dump it directly to the operator. Never raises — every
        failure path lands in the return dict so the caller can
        keep iterating across rks.

        Diagnostic-only: motif's own placement paths don't call
        this. The probe exists to characterise what's in Plex's
        theme store for a given item — specifically, whether a
        defunct themerr-plex's uploaded themes still occupy the
        Uploads/themes directory. Knowing the count + provider
        of existing theme entries is the prereq for deciding
        whether LET PLEX SERVE's collection-wide DELETE is safe
        on +P rows.
        """
        path = self._rk_path(rating_key, "/themes")
        try:
            r = self._client.get(path, headers=self._headers, timeout=30.0)
        except Exception as e:  # noqa: BLE001
            return {
                "ok": False,
                "http_status": None,
                "error": f"transport: {e!r}",
                "body": None,
            }
        body: dict | str
        try:
            body = r.json()
        except (ValueError, json.JSONDecodeError):
            # Plex sometimes returns XML even when JSON requested if
            # the endpoint quirks; surface the raw text so the
            # operator sees the shape.
            body = r.text[:4000] if r.text else ""
        return {
            "ok": 200 <= r.status_code < 300,
            "http_status": r.status_code,
            "error": None,
            "body": body,
        }

    def fetch_theme_bytes(self, *, item_rating_key: str, entry_uri: str) -> dict:
        """v0.51.171: the FULL audio bytes Plex is serving for a theme entry — no Range
        (probe_theme_entry_bytes caps at 4KB; measuring loudness needs the whole file).

        This exists to answer "is Plex actually playing the normalized theme?" by
        MEASURING what Plex serves rather than judging by ear. motif places a theme as a
        hardlinked sidecar, but Plex's Local Media Assets agent INGESTS it into its own
        metadata store at scan time (hence the metadata://themes/<sha1> entries) — so
        mutating the sidecar's bytes does NOT change what Plex plays until a refresh
        re-runs the agent. That assumption was never verified; this measures it.

        Full-buffering matches cloud_theme_backup (themes are ~1MB, ≤~9MB per the v1.18.85
        probe). Never raises. Returns {ok, http_status, bytes, error?}."""
        from urllib.parse import quote
        url = (f"{self._rk_path(item_rating_key, '/file')}"
               f"?url={quote(entry_uri, safe='')}")
        try:
            r = self._client.get(url, headers=self._headers, timeout=60.0)
        except Exception as e:  # noqa: BLE001 — diagnostic, never break the caller
            return {"ok": False, "http_status": None, "bytes": None,
                    "error": f"transport: {e!r}"}
        if not (200 <= r.status_code < 300):
            return {"ok": False, "http_status": r.status_code, "bytes": None,
                    "error": f"HTTP {r.status_code}"}
        return {"ok": True, "http_status": r.status_code, "bytes": r.content,
                "error": None}

    def probe_theme_entry_bytes(
        self, *, item_rating_key: str, entry_uri: str,
        range_bytes: int = 4096,
    ) -> dict:
        """v1.18.85: characterise the byte response for a single
        theme entry. Used by the cloud-themes-investigation probe
        to know whether `/library/metadata/{rk}/file?url=<entry>`
        actually returns audio bytes for Plex Pass cloud themes
        (vs themerr-plex embeds which we already know work). The
        v1.18.35 probe already confirmed bytes-return for
        upload:// + metadata:// entries; this method generalises
        the read so the diagnostic UI can compare ALL entries
        side-by-side without invoking the re-upload path.

        Captures:
          - HTTP status + ALL response headers (as a dict)
          - content_type / content_length (parsed from headers)
          - body_preview_hex (first range_bytes of bytes, hex-
            encoded with spaces for readability) — enough to see
            if it's MP3 magic (49 44 33 ...), HTML, JSON, or
            unexpected
          - body_length_received (actual bytes count regardless
            of advertised content-length)

        Range-GET so a misconfigured entry can't accidentally
        stream a huge file. Never raises — every failure path
        lands in the return dict (mirror v1.18.31's get_themes).
        """
        from urllib.parse import quote
        file_url = (
            f"{self._rk_path(item_rating_key, '/file')}"
            f"?url={quote(entry_uri, safe='')}"
        )
        # Range guard so we don't accidentally pull a giant file
        # if Plex returns something unexpected. Plex honors Range
        # on this endpoint (v1.18.36 production already relies on
        # the full GET; partial GET is a safer probe).
        headers = dict(self._headers)
        if range_bytes > 0:
            headers["Range"] = f"bytes=0-{range_bytes - 1}"
        try:
            r = self._client.get(file_url, headers=headers, timeout=30.0)
        except Exception as e:  # noqa: BLE001
            return {
                "ok": False,
                "url": file_url,
                "http_status": None,
                "error": f"transport: {e!r}",
                "headers": {},
                "content_type": None,
                "content_length": None,
                "body_length_received": 0,
                "body_preview_hex": None,
            }
        body = r.content or b""
        # Hex preview with spaces every 2 chars for readability.
        # Cap at range_bytes so we never emit more than requested.
        preview = body[:range_bytes]
        hex_preview = " ".join(f"{b:02x}" for b in preview)
        return {
            "ok": 200 <= r.status_code < 300,
            "url": file_url,
            "http_status": r.status_code,
            "error": None,
            "headers": dict(r.headers),
            "content_type": r.headers.get("content-type"),
            "content_length": r.headers.get("content-length"),
            "body_length_received": len(body),
            "body_preview_hex": hex_preview,
        }

    def get_item_paths(self, rating_key: str) -> list[str]:
        """Return all file paths for an item (for finding the media folder)."""
        r = self._get(self._rk_path(rating_key))
        if r is None:
            # _get already logged the transport failure at WARNING.
            return []
        if r.status_code != 200:
            # v1.22.20 (audit class-9): a non-200 (404 / 500 / auth) returned
            # [] silently, so the placement pipeline lost the media folder with
            # no breadcrumb — the SAME class the v1.17.9 fix closed on the
            # malformed-JSON branch just below. _get only logs transport errors
            # (None), never HTTP error STATUSES, so this was the last silent
            # []-return in the folder lookup.
            log.warning(
                "get_item_paths: rk=%s returned HTTP %s — no media folder",
                rating_key, r.status_code,
            )
            return []
        try:
            data = json.loads(r.text)
        except (ValueError, json.JSONDecodeError) as e:
            # v1.17.9: log on malformed Plex JSON. Twin sites in this
            # file (lines 288/346/744/990) already log + return []; the
            # silent `[]` here meant the placement pipeline lost the
            # media folder with no breadcrumb, falling through to
            # "no candidate paths" and skipping the item. Hygiene
            # audit (class 9).
            log.warning(
                "get_item_paths: malformed JSON for rk=%s: %s",
                rating_key, e,
            )
            return []
        # v1.14.78: Plex JSON puts both single-rk and multi-rk
        # /library/metadata responses under MediaContainer.Metadata
        # (one item per requested id). Movies have Media[].Part[];
        # shows have Location[]. Walk both per item.
        container = data.get("MediaContainer", {}) or {}
        items = container.get("Metadata") or []
        paths: list[str] = []
        for item in items:
            for media in (item.get("Media") or []):
                for part in (media.get("Part") or []):
                    f = part.get("file")
                    if f:
                        paths.append(f)
            # For TV shows, /metadata/<rk> returns the show; we
            # need a Location.
            for loc in (item.get("Location") or []):
                p = loc.get("path")
                if p:
                    paths.append(p)
        return paths

    def get_item_paths_bulk(
        self, rating_keys: list[str], *,
        batch_size: int = 50,
        # v0.51.102: 4 → 8. The docstring below notes Plex tolerates ≥16
        # concurrent /library/metadata GETs; with the v0.51.102 excludeElements
        # trim each batch's payload is ~5-10× smaller, so 8 stays well within
        # the server's headroom while halving the fallback's wall-clock.
        max_concurrent_batches: int = 8,
        on_batch_complete=None,
    ) -> dict[str, str]:
        """v1.14.76: bulk variant of get_item_paths. Plex's
        /library/metadata accepts a comma-separated list of
        rating_keys and returns one <Directory>/<Video> per id
        in a single MediaContainer. Cuts request count by ~50x
        vs. the per-rk fallback when Plex doesn't honor
        includeLocations=1 on the section listing (the slow-
        path that fired on every show section in the user's
        v1.14.74 logs — 4126 + 1232 + 2 items took ~91s of
        sequential per-item GETs).

        Returns {rating_key: first_path}. Missing rks (Plex
        returned no Location/Part for them) are omitted from
        the dict; callers should treat absence as "no path".

        The on_batch_complete callback fires after each batch
        completes with the cumulative number of rks resolved
        so far — wires the existing 'falling back, X of Y'
        progress bar without per-item ticks.

        Implementation notes:
        - We bypass `_rk_path` because that helper URL-encodes
          its input via quote(safe='') which would percent-
          encode the commas separating rating_keys; Plex's
          parser expects literal commas. Hand-builds the URL
          with quote(safe=',') instead.
        - Batches default to 50 ids per call; chosen to keep
          URL length comfortably under common 8KB server
          limits (50 × ~6-digit rk + commas ≈ 350 bytes).
        - max_concurrent_batches=8 (v0.51.102, was 4) — Plex
          tolerates at least 16 concurrent /library/metadata
          GETs (proven by v1.13.90's per-item fallback); with
          the v0.51.102 excludeElements trim each batch's payload
          is ~5-10× smaller, so 8 stays well within the server's
          headroom while halving the fallback's wall-clock.
        """
        from urllib.parse import quote
        if not rating_keys:
            return {}
        import threading
        # v1.22.72: per-worker-thread client for the threaded branch —
        # 4 workers shared self._client's httpx pool, the one site
        # violating the class-12 one-client-per-thread doctrine (api.py
        # pools everywhere else). Serial path never sets .client → self.
        _bulk_tl = threading.local()

        def _fetch_batch(batch: list[str]) -> dict[str, str]:
            ids = quote(",".join(batch), safe=",")
            path = f"/library/metadata/{ids}"
            _cli = getattr(_bulk_tl, "client", None) or self
            # v0.51.102: trim the tag tree the same way the bulk /all
            # listing does — this fallback reads ONLY Location.path (show
            # folders) with a Part.file fallback, so dropping Genre/Role/
            # Writer/… shrinks each 50-item batch ~5-10× (it fires on every
            # enum on builds that ignore includeLocations=1 at section level).
            # Location/Media/Part are NOT in the exclude set, so folder
            # discovery is unaffected; per the Plex spec it's advisory, so
            # builds that ignore it just return the full payload — no break.
            # Same per-call timeout as the bulk-listing fetch since payloads
            # can be comparable on big batches.
            r = _cli._get(path, params={
                "excludeElements": (
                    "Genre,Writer,Director,Country,Role,Producer,"
                    "Similar,Collection,Field,Label,Mood,Style,"
                    "Image,Review"
                ),
            }, timeout=120.0)
            if r is None or r.status_code != 200:
                # v1.15.35: include the first/last batch rks in the
                # warning so the operator can correlate "this row's
                # folder_path is empty" reports with a specific
                # batch failure (was just `batch of N`, no way to
                # narrow). Per-batch isolation preserved (return
                # empty dict — caller's other batches still
                # succeed).
                log.warning(
                    "get_item_paths_bulk: batch of %d (rks "
                    "%s..%s) returned %s — those items will "
                    "have empty folder_path until the next refresh",
                    len(batch), batch[0], batch[-1],
                    r.status_code if r is not None else "no response",
                )
                return {}
            try:
                data = json.loads(r.text)
            except (ValueError, json.JSONDecodeError) as e:
                log.warning(
                    "get_item_paths_bulk: JSON parse failed for "
                    "batch of %d (rks %s..%s): %s — those items "
                    "will have empty folder_path until next refresh",
                    len(batch), batch[0], batch[-1], e)
                return {}
            # v1.14.78: JSON puts each requested rk under
            # MediaContainer.Metadata (the per-item shape that
            # XML had under <Video>/<Directory>). Walk per-item
            # to keep the per-rk mapping intact — never iter()
            # across the whole container or Parts/Locations
            # from different items would mix.
            container = data.get("MediaContainer", {}) or {}
            items = container.get("Metadata") or []
            out: dict[str, str] = {}
            for item in items:
                rk_raw = item.get("ratingKey")
                if rk_raw is None:
                    continue
                rk = str(rk_raw)
                # Prefer Location (the show-folder path the
                # caller wants) and fall back to Media[].Part[].file
                # (which would be a movie file path; the caller
                # treats both uniformly).
                for loc in (item.get("Location") or []):
                    p = loc.get("path", "")
                    if p:
                        out[rk] = p
                        break
                if rk in out:
                    continue
                for media in (item.get("Media") or []):
                    matched = False
                    for part in (media.get("Part") or []):
                        f = part.get("file", "")
                        if f:
                            out[rk] = f
                            matched = True
                            break
                    if matched:
                        break
            return out

        batches = [
            rating_keys[i:i + batch_size]
            for i in range(0, len(rating_keys), batch_size)
        ]

        out: dict[str, str] = {}
        if max_concurrent_batches <= 1 or len(batches) == 1:
            for batch in batches:
                out.update(_fetch_batch(batch))
                if on_batch_complete is not None:
                    try:
                        on_batch_complete(len(out))
                    except Exception as e:  # noqa: BLE001
                        log.debug(
                            "bulk batch callback failed: %s", e)
        else:
            from concurrent.futures import ThreadPoolExecutor
            # v1.22.72: spawn one PlexClient per worker via the pool
            # initializer; close them all when the pool drains.
            _spawned: list[PlexClient] = []
            _sp_lock = threading.Lock()

            def _init_worker():
                c = PlexClient(self.cfg, self._plus)
                _bulk_tl.client = c
                with _sp_lock:
                    _spawned.append(c)

            try:
                with ThreadPoolExecutor(
                        max_workers=max_concurrent_batches,
                        initializer=_init_worker) as ex:
                    for batch_result in ex.map(_fetch_batch, batches):
                        out.update(batch_result)
                        if on_batch_complete is not None:
                            try:
                                on_batch_complete(len(out))
                            except Exception as e:  # noqa: BLE001
                                log.debug(
                                    "bulk batch callback failed: %s", e)
            finally:
                for _c in _spawned:
                    try:
                        _c.close()
                    except Exception:  # noqa: BLE001
                        pass
        return out
