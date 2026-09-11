"""AnimeThemes.moe source — ID bridge + API client + resolver (v0.51.314).

Design: docs/specs/ANIMETHEMES_SPEC.md. Pure over its inputs so the
tools/animethemes_eval.py harness and the (later) endpoints share it.
"""
from __future__ import annotations

import difflib
import json
import logging
import re
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

import httpx

log = logging.getLogger("motif.animethemes")

API_BASE = "https://api.animethemes.moe"
BRIDGE_URL = ("https://raw.githubusercontent.com/Fribb/anime-lists/master/"
              "anime-list-mini.json")
BRIDGE_FILENAME = "anime-list-mini.json"
BRIDGE_TTL_S = 7 * 86400
# v0.51.314: the API accepts comma lists; 50 ids/request measured OK (422 came from a deep include, not the list).
BATCH = 50
# v0.51.314: documented limit is 90/min — pace a third under it.
MIN_INTERVAL_S = 1.0
CACHE_TTL_S = 24 * 3600
MAX_RETRY_AFTER_S = 60.0
_HTTP_TIMEOUT = httpx.Timeout(connect=10.0, read=20.0, write=10.0, pool=5.0)

# v0.51.314: /resource refuses deep includes → two steps; list filters must be SCOPED (filter[anime][id]) or the include comes back empty.
_STEP2_FIELDS: tuple[tuple[str, str], ...] = (
    # v0.51.318: + the song and its artists — "OP1 — Tank! · The Seatbelts" is what
    # the operator recognises; a bare slug reads like an episode theme.
    ("include", "animethemes.animethemeentries.videos.audio,animethemes.song.artists"),
    ("fields[anime]", "id,name,slug,year,season"),
    ("fields[animetheme]", "type,sequence,slug"),
    ("fields[animethemeentry]", "version,nsfw,spoiler"),
    ("fields[video]", "basename,nc,source,resolution"),
    ("fields[audio]", "link,size"),
    ("fields[song]", "title"),
    ("fields[artist]", "name"),
    ("page[size]", "100"),
)


class AnimeThemesError(RuntimeError):
    """Non-2xx from the API after retries — never cached (the tmdb.py v1.22.43 lesson)."""

    def __init__(self, status: int, url: str):
        super().__init__(f"animethemes API {status} for {url}")
        self.status = status


class BridgeUnavailable(RuntimeError):
    """No usable bridge file: nothing cached and the fetch failed. Reported, never silent."""


def _user_agent() -> str:
    from .. import __version__ as motif_version
    return f"motif/{motif_version} (+https://github.com/Healzangels/motif)"


# ── bridge (Fribb/anime-lists) ─────────────────────────────────


@dataclass(frozen=True)
class BridgeEntry:
    anidb: int
    season: int | None
    kind: str
    mal: int | None = None


@dataclass
class Bridge:
    by_tvdb: dict[int, list[BridgeEntry]] = field(default_factory=dict)
    by_tmdb_tv: dict[int, list[BridgeEntry]] = field(default_factory=dict)
    # v0.51.328: TMDB *movie* ids are their own key space (the file carries
    # them as `themoviedb_id: {"movie": [128]}` — a list); a movie row must
    # never be looked up in the tv index, or a numerically equal tv id would
    # match the wrong show. 1,363 entries in the 2026-09 file.
    by_tmdb_movie: dict[int, list[BridgeEntry]] = field(default_factory=dict)
    entries: int = 0

    @classmethod
    def from_json(cls, data: Iterable[Mapping[str, Any]]) -> "Bridge":
        b = cls()
        for e in data:
            anidb = e.get("anidb_id")
            if not anidb:
                continue
            b.entries += 1
            # v0.51.317 (live check): 70 TV entries carry only season.tmdb (Bleach's
            # 2004 series) — without the fallback they sorted LAST and the resolver
            # fell through to season 17.
            sd = e.get("season") if isinstance(e.get("season"), dict) else {}
            season = sd.get("tvdb") if "tvdb" in sd else sd.get("tmdb")
            ent = BridgeEntry(anidb=int(anidb), season=season, kind=str(e.get("type") or ""),
                              mal=e.get("mal_id"))
            if e.get("tvdb_id"):
                b.by_tvdb.setdefault(int(e["tvdb_id"]), []).append(ent)
            tm = e.get("themoviedb_id")
            if isinstance(tm, dict) and tm.get("tv"):
                b.by_tmdb_tv.setdefault(int(tm["tv"]), []).append(ent)
            if isinstance(tm, dict) and tm.get("movie"):
                ids = tm["movie"] if isinstance(tm["movie"], list) else [tm["movie"]]
                for mid in ids:
                    if mid:
                        b.by_tmdb_movie.setdefault(int(mid), []).append(ent)
        return b

    def entries_for(self, guid_tvdb: int | None, guid_tmdb: int | None,
                    media_type: str | None = None) -> tuple[str | None, list[BridgeEntry]]:
        # v0.51.314: TVDB first (carries the season split); TMDB tv id as the fallback key.
        # v0.51.328: a Plex movie row's guid_tmdb is a MOVIE id — route it to the
        # movie index and never the tv one (shows keep the tv path; a show never
        # consults the movie index).
        if media_type == "movie":
            if guid_tmdb and guid_tmdb in self.by_tmdb_movie:
                return "tmdb", _season_order(self.by_tmdb_movie[guid_tmdb])
            return None, []
        if guid_tvdb and guid_tvdb in self.by_tvdb:
            return "tvdb", _season_order(self.by_tvdb[guid_tvdb])
        if guid_tmdb and guid_tmdb in self.by_tmdb_tv:
            return "tmdb", _season_order(self.by_tmdb_tv[guid_tmdb])
        return None, []


def _season_order(ents: list[BridgeEntry]) -> list[BridgeEntry]:
    # season 1 first, then ascending seasons, specials (0/None) last; TV before
    # MOVIE/OVA/SPECIAL within a season (v0.51.317)
    return sorted(ents, key=lambda e: (0 if e.season == 1 else 1,
                                       e.season if e.season else 10_000,
                                       0 if e.kind == "TV" else 1, e.anidb))


_BRIDGE_LOCK = threading.Lock()
_BRIDGE_CACHE: dict[str, tuple[float, Bridge]] = {}
_BRIDGE_STALE_WARNED = False


def load_bridge(cache_dir: Path, *, client: httpx.Client | None = None,
                now: Callable[[], float] = time.time, ttl_s: float = BRIDGE_TTL_S) -> Bridge:
    """Parsed bridge from cache_dir/anime-list-mini.json, refreshed weekly (ETag). Stale beats nothing."""
    global _BRIDGE_STALE_WARNED
    path = cache_dir / BRIDGE_FILENAME
    etag_path = cache_dir / (BRIDGE_FILENAME + ".etag")
    fresh = path.exists() and (now() - path.stat().st_mtime) < ttl_s
    if not fresh:
        try:
            _refresh_bridge(path, etag_path, client)
        except Exception as e:  # noqa: BLE001 — class 9: breadcrumb + fallback
            if path.exists():
                if not _BRIDGE_STALE_WARNED:
                    log.warning("animethemes: bridge refresh failed (%s); using the stale cache at %s", e, path)
                    _BRIDGE_STALE_WARNED = True
            else:
                log.warning("animethemes: bridge unavailable — no cache at %s and the fetch failed: %s", path, e)
                raise BridgeUnavailable(str(e)) from e
    key = str(path)
    with _BRIDGE_LOCK:
        mtime = path.stat().st_mtime
        hit = _BRIDGE_CACHE.get(key)
        if hit and hit[0] == mtime:
            return hit[1]
        with path.open("rb") as fh:
            bridge = Bridge.from_json(json.load(fh))
        _BRIDGE_CACHE[key] = (mtime, bridge)
        log.info("animethemes: bridge loaded — %d entries, %d tvdb keys, %d tmdb tv keys, %d tmdb movie keys",
                 bridge.entries, len(bridge.by_tvdb), len(bridge.by_tmdb_tv), len(bridge.by_tmdb_movie))
        return bridge


def refresh_bridge(cache_dir: Path, *, client: httpx.Client | None = None) -> str:
    """v0.51.327 (tag 5): the scheduled refresh. 'skipped' when the operator has
    never used ANIME THEMES (no cache file → no fetch, spec §3.7), else the
    ETag-conditional fetch: 'unchanged' on a 304 (mtime touched, so load_bridge's
    TTL restarts), 'refreshed' on a new payload. Errors raise — the job logs them."""
    path = cache_dir / BRIDGE_FILENAME
    if not path.exists():
        return "skipped"
    return _refresh_bridge(path, cache_dir / (BRIDGE_FILENAME + ".etag"), client)


def _refresh_bridge(path: Path, etag_path: Path, client: httpx.Client | None) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    headers = {"User-Agent": _user_agent()}
    if path.exists() and etag_path.exists():
        headers["If-None-Match"] = etag_path.read_text().strip()
    own = client is None
    c = client or httpx.Client(timeout=_HTTP_TIMEOUT, follow_redirects=True)
    try:
        r = c.get(BRIDGE_URL, headers=headers)
        if r.status_code == 304 and path.exists():
            path.touch()
            return "unchanged"
        if r.status_code != 200:
            raise AnimeThemesError(r.status_code, BRIDGE_URL)
        data = r.json()
        if not isinstance(data, list) or len(data) < 1000:
            raise ValueError(f"bridge payload looks wrong ({type(data).__name__}, {len(data) if isinstance(data, list) else 0} entries)")
        tmp = path.with_suffix(".tmp")
        tmp.write_bytes(r.content)
        tmp.replace(path)
        etag = r.headers.get("etag")
        if etag:
            etag_path.write_text(etag)
        return "refreshed"
    finally:
        if own:
            c.close()


# ── API client ─────────────────────────────────────────────────


@dataclass(frozen=True)
class Audio:
    link: str
    size: int | None
    version: int | None
    source: str | None
    nc: bool
    nsfw: bool


@dataclass(frozen=True)
class Theme:
    slug: str
    type: str
    sequence: int | None
    audio: tuple[Audio, ...]
    song: str | None = None                  # v0.51.318
    artists: tuple[str, ...] = ()            # v0.51.318


@dataclass(frozen=True)
class AnimeInfo:
    anime_id: int
    name: str
    year: int | None
    season: str | None
    slug: str


def _parse_themes(anime: Mapping[str, Any]) -> tuple[Theme, ...]:
    out: list[Theme] = []
    for t in anime.get("animethemes") or []:
        auds: list[Audio] = []
        for e in t.get("animethemeentries") or []:
            for v in e.get("videos") or []:
                au = v.get("audio") or {}
                if au.get("link"):
                    auds.append(Audio(link=au["link"], size=au.get("size"), version=e.get("version"),
                                      source=v.get("source"), nc=bool(v.get("nc")), nsfw=bool(e.get("nsfw"))))
        song = t.get("song") or {}
        out.append(Theme(slug=str(t.get("slug") or ""), type=str(t.get("type") or ""),
                         sequence=t.get("sequence"), audio=tuple(auds),
                         song=(str(song.get("title")) if song.get("title") else None),
                         artists=tuple(str(a.get("name")) for a in (song.get("artists") or []) if a.get("name"))))
    return tuple(out)


def _info(anime: Mapping[str, Any]) -> AnimeInfo:
    return AnimeInfo(anime_id=int(anime["id"]), name=str(anime.get("name") or ""),
                     year=anime.get("year"), season=anime.get("season"), slug=str(anime.get("slug") or ""))


class AnimeThemesClient:
    """Paced, batched, two-step lookups with a 24h in-memory cache. Errors raise; nothing bad is cached."""

    def __init__(self, *, client: httpx.Client | None = None, min_interval_s: float = MIN_INTERVAL_S,
                 sleep: Callable[[float], None] = time.sleep,
                 monotonic: Callable[[], float] = time.monotonic,
                 now: Callable[[], float] = time.time):
        self._own = client is None
        self._client = client or httpx.Client(base_url=API_BASE, timeout=_HTTP_TIMEOUT,
                                              headers={"User-Agent": _user_agent(), "Accept": "application/json"})
        self._min_interval = min_interval_s
        self._sleep = sleep
        self._monotonic = monotonic
        self._now = now
        self._lock = threading.RLock()
        self._last_call = 0.0
        self._anime_by_anidb: dict[int, tuple[float, list[AnimeInfo]]] = {}
        self._themes_by_anime: dict[int, tuple[float, tuple[Theme, ...]]] = {}
        self.requests = 0

    def close(self) -> None:
        if self._own:
            self._client.close()

    def __enter__(self) -> "AnimeThemesClient":
        return self

    def __exit__(self, *a: Any) -> None:
        self.close()

    def _pace(self) -> None:
        with self._lock:
            wait = self._min_interval - (self._monotonic() - self._last_call)
            if wait > 0:
                self._sleep(wait)
            self._last_call = self._monotonic()

    def _get(self, path: str, params: list[tuple[str, str]]) -> dict[str, Any]:
        url = f"{API_BASE}{path}"
        for attempt in range(3):
            self._pace()
            self.requests += 1
            r = self._client.get(url, params=params)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429 and attempt < 2:
                # v0.51.314: honour Retry-After (capped) — the limiter is per minute, a burst must not become a ban
                try:
                    ra = min(float(r.headers.get("retry-after") or 5.0), MAX_RETRY_AFTER_S)
                except ValueError:
                    ra = 5.0
                log.warning("animethemes: 429 on %s — sleeping %.0fs (attempt %d)", path, ra, attempt + 1)
                self._sleep(ra)
                continue
            log.warning("animethemes: HTTP %s on %s", r.status_code, path)
            raise AnimeThemesError(r.status_code, url)
        raise AnimeThemesError(429, url)

    # step 1: aniDB id -> anime
    def lookup_anidb(self, anidb_ids: Iterable[int]) -> dict[int, list[AnimeInfo]]:
        ids = sorted({int(i) for i in anidb_ids})
        out: dict[int, list[AnimeInfo]] = {}
        missing: list[int] = []
        t = self._now()
        for i in ids:
            hit = self._anime_by_anidb.get(i)
            if hit and hit[0] > t:
                out[i] = hit[1]
            else:
                missing.append(i)
        for k in range(0, len(missing), BATCH):
            chunk = missing[k:k + BATCH]
            d = self._get("/resource", [
                ("filter[site]", "aniDB"),
                ("filter[external_id]", ",".join(map(str, chunk))),
                ("include", "anime"),
                ("fields[resource]", "external_id"),
                ("fields[anime]", "id,name,year,season,slug"),
                ("page[size]", "100"),
            ])
            found: dict[int, list[AnimeInfo]] = {}
            for res in d.get("resources") or []:
                try:
                    ext = int(res.get("external_id"))
                except (TypeError, ValueError):
                    continue
                found.setdefault(ext, []).extend(_info(a) for a in (res.get("anime") or []) if a.get("id") is not None)
            exp = self._now() + CACHE_TTL_S
            for i in chunk:
                # v0.51.314: a 200 with no resource IS an answer ("not on AnimeThemes") — cache the empty list
                self._anime_by_anidb[i] = (exp, found.get(i, []))
                out[i] = found.get(i, [])
        return out

    # step 2: anime id -> themes with audio
    def themes_for(self, anime_ids: Iterable[int]) -> dict[int, tuple[Theme, ...]]:
        ids = sorted({int(i) for i in anime_ids})
        out: dict[int, tuple[Theme, ...]] = {}
        missing: list[int] = []
        t = self._now()
        for i in ids:
            hit = self._themes_by_anime.get(i)
            if hit and hit[0] > t:
                out[i] = hit[1]
            else:
                missing.append(i)
        for k in range(0, len(missing), BATCH):
            chunk = missing[k:k + BATCH]
            d = self._get("/anime", [("filter[anime][id]", ",".join(map(str, chunk))), *_STEP2_FIELDS])
            found = {int(a["id"]): _parse_themes(a) for a in (d.get("anime") or []) if a.get("id") is not None}
            exp = self._now() + CACHE_TTL_S
            for i in chunk:
                self._themes_by_anime[i] = (exp, found.get(i, ()))
                out[i] = found.get(i, ())
        return out

    def search(self, name: str, *, limit: int = 5) -> list[tuple[AnimeInfo, tuple[Theme, ...]]]:
        d = self._get("/anime", [("q", name), ("page[size]", str(limit)), *_STEP2_FIELDS[:-1]])
        return [(_info(a), _parse_themes(a)) for a in (d.get("anime") or []) if a.get("id") is not None]


# ── resolver ───────────────────────────────────────────────────


@dataclass(frozen=True)
class SeasonMatch:
    season: int | None
    anidb: int | None
    info: AnimeInfo
    themes: tuple[Theme, ...]


@dataclass
class Resolution:
    confidence: str | None          # "clean" | "glance" | "name" | None
    via: str | None                 # "tvdb" | "tmdb" | "name" | None
    reason: str
    seasons: list[SeasonMatch] = field(default_factory=list)
    default: tuple[SeasonMatch, Theme, Audio] | None = None

    @property
    def has_op(self) -> bool:
        return any(t.type == "OP" and t.audio for s in self.seasons for t in s.themes)

    @property
    def has_audio(self) -> bool:
        return any(t.audio for s in self.seasons for t in s.themes)


_SOURCE_RANK = {"BD": 0, "DVD": 1, "WEB": 2, "VHS": 3, "LD": 4, "RAW": 5}


def pick_default(season: SeasonMatch) -> tuple[Theme, Audio] | None:
    """OP with the lowest sequence, first version, best source. ED only when no OP has audio."""
    def key(t: Theme) -> tuple[int, int]:
        return (0 if t.type == "OP" else 1, t.sequence or 1)
    for t in sorted([t for t in season.themes if t.audio], key=key):
        au = sorted(t.audio, key=lambda a: (a.version or 1, _SOURCE_RANK.get(a.source or "", 9), -(a.size or 0)))[0]
        return t, au
    return None


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (s or "").lower()).strip()


def name_similarity(a: str, b: str) -> float:
    return difflib.SequenceMatcher(None, _norm(a), _norm(b)).ratio()


def _year_ok(plex_year: Any, at_year: Any) -> bool:
    try:
        return abs(int(plex_year) - int(at_year)) <= 1
    except (TypeError, ValueError):
        return True  # unknown on either side → not evidence against


def resolve(row: Mapping[str, Any], bridge: Bridge, client: AnimeThemesClient, *,
            name_search: bool = True) -> Resolution:
    """One row → Resolution. Row needs title, year, guid_tvdb, guid_tmdb."""
    via, ents = bridge.entries_for(row.get("guid_tvdb"), row.get("guid_tmdb"), row.get("media_type"))
    seasons: list[SeasonMatch] = []
    if ents:
        anime = client.lookup_anidb([e.anidb for e in ents])
        ids = [a.anime_id for e in ents for a in anime.get(e.anidb, [])]
        themes = client.themes_for(ids) if ids else {}
        for e in ents:
            for a in anime.get(e.anidb, []):
                th = themes.get(a.anime_id, ())
                if any(t.audio for t in th):
                    seasons.append(SeasonMatch(season=e.season, anidb=e.anidb, info=a, themes=th))
                    break  # one AnimeThemes anime per bridge entry
    if seasons:
        chosen = seasons[0]
        picked = pick_default(chosen)
        # v0.51.328: a film's bridge entry carries no season (there is none);
        # for a Plex movie row that entry IS the clean match. Shows keep the
        # season-1 rule.
        season_ok = chosen.season == 1 or (row.get("media_type") == "movie" and chosen.season is None)
        if season_ok and _year_ok(row.get("year"), chosen.info.year):
            conf, reason = "clean", ("film entry, year agrees" if chosen.season is None
                                     else "season-1 entry, year agrees")
        elif not season_ok:
            conf, reason = "glance", f"season-1 entry absent on AnimeThemes; fell through to season {chosen.season}"
        else:
            conf, reason = "glance", f"year differs: Plex {row.get('year')} vs AnimeThemes {chosen.info.year}"
        return Resolution(conf, via, reason, seasons, (chosen, *picked) if picked else None)
    if ents:
        reason = "bridged but no AnimeThemes entry with audio"
    else:
        reason = "no bridge entry for this row's guids"
    if name_search and row.get("title"):
        best: tuple[float, AnimeInfo, tuple[Theme, ...]] | None = None
        for info, th in client.search(str(row["title"])):
            s = name_similarity(info.name, str(row["title"]))
            if s >= 0.6 and _year_ok(row.get("year"), info.year) and any(t.audio for t in th):
                if best is None or s > best[0]:
                    best = (s, info, th)
        if best:
            sm = SeasonMatch(season=None, anidb=None, info=best[1], themes=best[2])
            picked = pick_default(sm)
            # v0.51.314: NAME resolutions always need a human — the caller must never auto-apply them
            return Resolution("name", "name", f"name search (similarity {best[0]:.2f}); {reason}", [sm],
                              (sm, *picked) if picked else None)
    return Resolution(None, via, reason)


def prefetch(rows: Iterable[Mapping[str, Any]], bridge: Bridge, client: AnimeThemesClient) -> None:
    """Warm the client caches for many rows in ~2 batched passes (the harness + the sweep use this)."""
    anidb: set[int] = set()
    for r in rows:
        _, ents = bridge.entries_for(r.get("guid_tvdb"), r.get("guid_tmdb"), r.get("media_type"))
        anidb.update(e.anidb for e in ents)
    anime = client.lookup_anidb(anidb)
    client.themes_for(a.anime_id for lst in anime.values() for a in lst)


# ── endpoint helpers (v0.51.317: the picker dialog) ─────────────


# ── Phase 2: the sweep (spec §3.6, v0.51.325) ─────────────────────────────


def sweep_row_to_json(row: Mapping[str, Any], res: "Resolution") -> dict[str, Any]:
    """One review-list row. `group` is the operator's bucket: 'ready' (CLEAN with an
    audio default — bulk-appliable), 'review' (GLANCE / NAME — the picker, never the
    bulk path, spec §3.7) or 'unresolved' (no AnimeThemes entry with audio)."""
    d = resolution_to_json(res, title=row.get("title"), year=row.get("year"))
    default = d["default"]
    if res.confidence == "clean" and default:
        group = "ready"
    elif d["seasons"]:
        group = "review"
    else:
        group = "unresolved"
    anidb = None
    if default is not None:
        anidb = d["seasons"][default["season_index"]]["anidb"]
        _, theme, _ = res.default
        default = {**default, "type": theme.type, "sequence": theme.sequence}  # the picker's label inputs
    first = d["seasons"][0] if d["seasons"] else None
    return {
        "rating_key": str(row.get("rating_key") or ""),
        "title": row.get("title"), "year": row.get("year"),
        "media_type": row.get("media_type"), "section_id": row.get("section_id"),
        "section_title": row.get("section_title"), "guid_tmdb": row.get("guid_tmdb"),
        "plex_has_theme": 1 if row.get("has_theme") else 0,
        "confidence": res.confidence, "via": res.via, "reason": res.reason, "group": group,
        "name": default["name"] if default else (first["name"] if first else None),
        "at_year": default["year"] if default else (first["year"] if first else None),
        "anidb": anidb, "seasons": len(d["seasons"]),
        "default": default,
    }


def sweep(rows: Iterable[Mapping[str, Any]], bridge: Bridge, client: AnimeThemesClient, *,
          progress_cb: Callable[[int, int], None] | None = None,
          cancel_check: Callable[[], bool] | None = None) -> list[dict[str, Any]]:
    """Resolve many rows for the review list. One prefetch warms the client
    (~2 batched passes for a whole library), then the per-row loop is cache-only,
    so a cancel between rows costs nothing. No name search: an unbridged row is
    listed as unresolved and the picker (which does search) is one click away —
    the sweep's promise is a handful of API calls, not one per row."""
    rows = list(rows)
    total = len(rows)
    prefetch(rows, bridge, client)
    out: list[dict[str, Any]] = []
    for i, r in enumerate(rows):
        if cancel_check is not None and cancel_check():
            break
        res = resolve(r, bridge, client, name_search=False)
        out.append(sweep_row_to_json(r, res))
        if progress_cb is not None:
            progress_cb(i + 1, total)
    return out


def resolution_to_json(res: "Resolution", *, title: str | None = None, year: Any = None) -> dict[str, Any]:
    """The wire shape the picker renders. Seasons keep resolver order; `default`
    points INTO that list so the UI never re-derives the OP1 preference."""
    seasons = []
    for sm in res.seasons:
        seasons.append({
            "season": sm.season, "anidb": sm.anidb, "anime_id": sm.info.anime_id,
            "name": sm.info.name, "year": sm.info.year, "slug": sm.info.slug,
            "themes": [{
                "slug": t.slug, "type": t.type, "sequence": t.sequence,
                "song": t.song, "artists": list(t.artists),  # v0.51.318
                # v0.51.318: best audio FIRST (version 1, BD over WEB) — the picker shows one row per theme
                "audio": [{"link": a.link, "size": a.size, "version": a.version, "source": a.source,
                           "nc": a.nc, "nsfw": a.nsfw}
                          for a in sorted(t.audio, key=lambda a: (a.version or 1, _SOURCE_RANK.get(a.source or "", 9), -(a.size or 0)))],
            } for t in sm.themes if t.audio],
        })
    default = None
    if res.default:
        sm, theme, audio = res.default
        default = {"season_index": res.seasons.index(sm), "theme": theme.slug, "link": audio.link,
                   "size": audio.size, "name": sm.info.name, "year": sm.info.year,
                   "song": theme.song, "artists": list(theme.artists)}  # v0.51.318
    return {"title": title, "year": year, "confidence": res.confidence, "via": res.via,
            "reason": res.reason, "seasons": seasons, "default": default}


PREVIEW_MAX_BYTES = 40 * 1024 * 1024
_PREVIEW_LOCK = threading.Lock()


class PreviewBusy(RuntimeError):
    """One preview download at a time per process — a second click waits for the first."""


def download_preview_audio(link: str, dest: Path, *, client: httpx.Client | None = None,
                           max_bytes: int = PREVIEW_MAX_BYTES) -> int:
    """Stream an AnimeThemes audio link to `dest` (bytes written). Refuses non-AnimeThemes
    links (the SSRF allowlist is downloader's; this is the belt to its braces) and files
    over `max_bytes` — the picker previews TV-size openings, not full albums."""
    from .downloader import is_fetchable_theme_url
    from .sync import url_source
    if url_source(link) != "animethemes" or not is_fetchable_theme_url(link):
        raise ValueError("not an AnimeThemes audio link")
    if not _PREVIEW_LOCK.acquire(blocking=False):
        raise PreviewBusy("a preview is already downloading")
    own = client is None
    c = client or httpx.Client(timeout=_HTTP_TIMEOUT, follow_redirects=True,
                               headers={"User-Agent": _user_agent()})
    try:
        dest.parent.mkdir(parents=True, exist_ok=True)
        written = 0
        with c.stream("GET", link) as r:
            if r.status_code != 200:
                raise AnimeThemesError(r.status_code, link)
            try:
                declared = int(r.headers.get("content-length") or 0)
            except ValueError:
                declared = 0
            if declared > max_bytes:
                raise ValueError(f"audio is {declared / 1e6:.0f} MB — too large to preview")
            tmp = dest.with_suffix(dest.suffix + ".part")
            with tmp.open("wb") as fh:
                for chunk in r.iter_bytes():
                    written += len(chunk)
                    if written > max_bytes:
                        fh.close(); tmp.unlink(missing_ok=True)
                        raise ValueError(f"audio exceeded {max_bytes / 1e6:.0f} MB while downloading")
                    fh.write(chunk)
            if written == 0:
                tmp.unlink(missing_ok=True)
                raise AnimeThemesError(200, link + " (empty body)")
            tmp.replace(dest)
        return written
    finally:
        if own:
            c.close()
        _PREVIEW_LOCK.release()
