"""
TMDB API client — orphan resolution fallback when nfo files are absent.

motif uses TMDB to look up movies and TV shows by title+year and pull
canonical TMDB IDs and IMDB ids for orphan adoption. The free TMDB v3
API only requires a key (no JWT exchange like TVDB v4), so the surface
is much simpler.

The user supplies the API key via /settings (or MOTIF_TMDB_API_KEY env).
Without a key, this module is a no-op and orphans without nfo files get
a synthetic negative tmdb_id and remain in 'orphan_unresolved' state.

Auth model:
  api_key passed as ?api_key=... query param on every request.

Rate limiting: TMDB allows ~50 req/sec on the free tier. We rate-limit
to 5 req/sec ourselves and cache responses in tvdb_lookup_cache (legacy
table name; the schema is just a generic key/value cache) for 7 days.

Endpoints used:
  - GET /3/search/movie?query=&year=
  - GET /3/search/tv?query=&first_air_date_year=
  - GET /3/find/{imdb_id}?external_source=imdb_id

Returns normalized {tmdb_id, imdb_id, title, year} dicts so the adopt
module doesn't need to care which source the metadata came from.
"""
from __future__ import annotations

import json
import logging
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import httpx

from .db import get_conn

log = logging.getLogger(__name__)

TMDB_BASE = "https://api.themoviedb.org/3"
CACHE_TTL_DAYS = 7
RATE_LIMIT_INTERVAL = 0.2   # seconds between calls (5 req/sec ceiling)


class TMDBError(Exception):
    """Base for TMDB-related failures (auth, rate limit, parse, etc)."""


class TMDBAuthError(TMDBError):
    """API key was rejected."""


class TMDBClient:
    """Per-process TMDB client. Rate-limits, caches.

    Construct with the user's API key (None disables all lookups). The
    client is thread-safe — `_lock` guards the rate-limit timestamp."""

    def __init__(self, api_key: Optional[str], db_path: Path,
                 timeout: float = 10.0):
        self.api_key = api_key
        self.db_path = db_path
        self.timeout = timeout
        self._last_call_at: float = 0.0
        self._lock = threading.RLock()

    @property
    def enabled(self) -> bool:
        return bool(self.api_key)

    # ---------------- public API ----------------

    def search_movie(self, title: str, year: Optional[str] = None) -> Optional[dict]:
        if not self.enabled:
            return None
        return self._cached_or_fetch(
            self._cache_key("tmdb:movie", title, year),
            lambda: self._search("movie", title, year),
        )

    def search_show(self, title: str, year: Optional[str] = None) -> Optional[dict]:
        if not self.enabled:
            return None
        return self._cached_or_fetch(
            self._cache_key("tmdb:tv", title, year),
            lambda: self._search("tv", title, year),
        )

    def lookup_by_imdb(self, imdb_id: str) -> Optional[dict]:
        if not self.enabled or not imdb_id:
            return None
        return self._cached_or_fetch(
            f"tmdb:imdb:{imdb_id}",
            lambda: self._lookup_by_imdb(imdb_id),
        )

    def test_credentials(self) -> tuple[bool, str]:
        """Probe the API key by hitting /authentication. Returns (ok, msg).
        Used by /api/tmdb/test."""
        if not self.api_key:
            return False, "no API key configured"
        try:
            self._rate_limit()
            r = httpx.get(
                f"{TMDB_BASE}/authentication",
                params={"api_key": self.api_key},
                timeout=self.timeout,
            )
        except httpx.HTTPError as e:
            return False, f"connection error: {e}"
        if r.status_code == 401:
            return False, "HTTP 401 — invalid API key"
        if r.status_code != 200:
            return False, f"HTTP {r.status_code}"
        return True, "credentials valid"

    # ---------------- internal: cache ----------------

    @staticmethod
    def _cache_key(kind: str, title: str, year: Optional[str]) -> str:
        title_key = title.strip().lower()
        year_key = (year or "").strip()
        return f"{kind}:{title_key}:{year_key}"

    def _cached_or_fetch(self, key: str, fetcher) -> Optional[dict]:
        with get_conn(self.db_path) as conn:
            row = conn.execute(
                "SELECT response_json, expires_at FROM tvdb_lookup_cache WHERE cache_key = ?",
                (key,),
            ).fetchone()
            if row:
                exp_str = row["expires_at"]
                try:
                    expires = datetime.fromisoformat(exp_str)
                    if expires.tzinfo is not None:
                        expires = expires.replace(tzinfo=None)
                except (TypeError, ValueError):
                    expires = None
                if expires and expires > datetime.utcnow():
                    return json.loads(row["response_json"]) or None

        try:
            result = fetcher()
        except (TMDBError, httpx.HTTPError) as e:
            log.warning("TMDB lookup failed for %s: %s", key, e)
            return None

        cached_value = json.dumps(result) if result is not None else "null"
        expires_naive = datetime.utcnow() + timedelta(days=CACHE_TTL_DAYS)
        with get_conn(self.db_path) as conn:
            conn.execute(
                """INSERT OR REPLACE INTO tvdb_lookup_cache
                   (cache_key, response_json, fetched_at, expires_at)
                   VALUES (?, ?, ?, ?)""",
                (key, cached_value,
                 datetime.utcnow().isoformat(timespec="seconds"),
                 expires_naive.isoformat(timespec="seconds")),
            )
        return result

    # ---------------- internal: HTTP ----------------

    def _rate_limit(self) -> None:
        with self._lock:
            now = time.monotonic()
            wait = RATE_LIMIT_INTERVAL - (now - self._last_call_at)
            if wait > 0:
                time.sleep(wait)
            self._last_call_at = time.monotonic()

    def _search(self, kind: str, title: str, year: Optional[str]) -> Optional[dict]:
        """kind is 'movie' or 'tv'. Returns the first matching result."""
        params: dict[str, Any] = {
            "api_key": self.api_key,
            "query": title,
            "include_adult": "false",
            "language": "en-US",
        }
        if year:
            # TMDB uses different param names per kind for year filtering.
            params["year" if kind == "movie" else "first_air_date_year"] = year
        self._rate_limit()
        try:
            r = httpx.get(
                f"{TMDB_BASE}/search/{kind}",
                params=params, timeout=self.timeout,
            )
        except httpx.HTTPError as e:
            raise TMDBError(f"search failed: {e}") from e

        if r.status_code == 401:
            raise TMDBAuthError("HTTP 401 — check your TMDB API key")
        if r.status_code != 200:
            raise TMDBError(f"search returned HTTP {r.status_code}")

        results = r.json().get("results") or []
        if not results:
            return None
        # TMDB orders by popularity/relevance. Take the first.
        first = results[0]
        # /search doesn't return imdb_id; if the caller cares, follow up
        # with an external_ids fetch. For orphan adoption we mostly need
        # the tmdb_id, so skip the extra round-trip by default.
        return self._normalize(first, kind)

    def _lookup_by_imdb(self, imdb_id: str) -> Optional[dict]:
        """TMDB's /find endpoint resolves an IMDB id to whichever movie/TV
        record matches, plus a few other external sources."""
        self._rate_limit()
        try:
            r = httpx.get(
                f"{TMDB_BASE}/find/{imdb_id}",
                params={"api_key": self.api_key, "external_source": "imdb_id"},
                timeout=self.timeout,
            )
        except httpx.HTTPError as e:
            raise TMDBError(f"find request failed: {e}") from e

        if r.status_code == 401:
            raise TMDBAuthError("HTTP 401 — check your TMDB API key")
        if r.status_code != 200:
            return None

        data = r.json()
        for kind, key in (("movie", "movie_results"), ("tv", "tv_results")):
            arr = data.get(key) or []
            if arr:
                out = self._normalize(arr[0], kind)
                out["imdb_id"] = imdb_id  # we know it from input
                return out
        return None

    @staticmethod
    def _normalize(raw: dict, kind: str) -> dict:
        """Extract canonical fields out of a TMDB v3 search/find result."""
        # Movie results carry `title` + `release_date`; TV uses `name` +
        # `first_air_date`. Year is parsed from whichever date field has it.
        title = raw.get("title") or raw.get("name") or ""
        date_str = raw.get("release_date") or raw.get("first_air_date") or ""
        year = date_str[:4] if date_str else None
        return {
            "tvdb_id": None,                      # TMDB doesn't expose tvdb id
            "tmdb_id": _safe_int(raw.get("id")),
            "imdb_id": None,                      # not in /search payload
            "title": title,
            "year": year,
            "kind": kind,
        }


def _safe_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None
