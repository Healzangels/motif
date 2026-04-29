"""
TVDB API client (v4) — orphan resolution fallback when nfo files are absent.

motif uses TVDB to look up movies and TV shows by title+year and pull canonical
IDs and metadata. The API requires a paid subscription; the API key is
user-supplied via /settings (or MOTIF_TVDB_API_KEY env). Without a key, this
module is a no-op — orphans without nfo files get a synthetic negative
tmdb_id and remain in 'orphan_unresolved' state.

Auth model:
  POST /v4/login {apikey} → {data: {token}} (JWT, valid 30 days)
  Subsequent calls include `Authorization: Bearer <jwt>` header.

Rate limiting: TVDB allows ~50 req/sec for paid keys. We rate-limit at
2 req/sec ourselves to stay well under, and cache responses in
tvdb_lookup_cache for 7 days. Cache is hit-first on every lookup.

Endpoints used:
  - POST /v4/login              — get JWT
  - GET  /v4/search?query=&type=&year=
  - GET  /v4/series/{id}/extended
  - GET  /v4/movies/{id}/extended

Returns normalized {tvdb_id, tmdb_id, imdb_id, title, year} dicts so the
adopt module doesn't need to care which source the metadata came from.
"""
from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import httpx

from .db import get_conn

log = logging.getLogger(__name__)

TVDB_BASE = "https://api4.thetvdb.com/v4"
CACHE_TTL_DAYS = 7
RATE_LIMIT_INTERVAL = 0.5   # seconds between calls (2 req/sec ceiling)


class TVDBError(Exception):
    """Base for TVDB-related failures (auth, rate limit, parse, etc)."""


class TVDBAuthError(TVDBError):
    """Login failed or JWT was rejected."""


class TVDBClient:
    """Per-process TVDB client. Holds the JWT, rate-limits, caches.

    Construct with the user's API key (None disables all lookups). The
    client is thread-safe — `_lock` guards the JWT and last-call-time."""

    def __init__(self, api_key: Optional[str], db_path: Path,
                 timeout: float = 10.0):
        self.api_key = api_key
        self.db_path = db_path
        self.timeout = timeout
        self._jwt: Optional[str] = None
        self._jwt_expires_at: Optional[datetime] = None
        self._last_call_at: float = 0.0
        self._lock = threading.RLock()

    @property
    def enabled(self) -> bool:
        return bool(self.api_key)

    # ---------------- public API ----------------

    def search_movie(self, title: str, year: Optional[str] = None) -> Optional[dict]:
        """Search for a movie by title+year. Returns a normalized dict or None
        if no result or lookups are disabled."""
        if not self.enabled:
            return None
        return self._cached_or_fetch(
            self._cache_key("movie", title, year),
            lambda: self._search("movie", title, year),
        )

    def search_show(self, title: str, year: Optional[str] = None) -> Optional[dict]:
        if not self.enabled:
            return None
        return self._cached_or_fetch(
            self._cache_key("series", title, year),
            lambda: self._search("series", title, year),
        )

    def lookup_by_imdb(self, imdb_id: str) -> Optional[dict]:
        """Resolve by IMDB id (more reliable than fuzzy title match)."""
        if not self.enabled or not imdb_id:
            return None
        return self._cached_or_fetch(
            f"imdb:{imdb_id}",
            lambda: self._search_remote_id(imdb_id),
        )

    def test_credentials(self) -> tuple[bool, str]:
        """Try to log in. Returns (ok, msg). Used by /api/tvdb/test."""
        if not self.api_key:
            return False, "no API key configured"
        try:
            self._login()
            return True, "credentials valid"
        except TVDBAuthError as e:
            return False, f"auth failed: {e}"
        except (httpx.HTTPError, TVDBError) as e:
            return False, f"connection error: {e}"

    # ---------------- internal: cache ----------------

    @staticmethod
    def _cache_key(kind: str, title: str, year: Optional[str]) -> str:
        # Keep keys deterministic and stable across runs
        title_key = title.strip().lower()
        year_key = (year or "").strip()
        return f"{kind}:{title_key}:{year_key}"

    def _cached_or_fetch(self, key: str, fetcher) -> Optional[dict]:
        # Cache hit?
        with get_conn(self.db_path) as conn:
            row = conn.execute(
                "SELECT response_json, expires_at FROM tvdb_lookup_cache WHERE cache_key = ?",
                (key,),
            ).fetchone()
            if row:
                # SQLite's datetime('now') and our isoformat() writes both produce
                # naive UTC strings (no tz suffix). Parse and compare against a
                # naive utcnow() for consistency. If the column got a tz-aware
                # value somehow (older row), strip the tz before comparing.
                exp_str = row["expires_at"]
                try:
                    expires = datetime.fromisoformat(exp_str)
                    if expires.tzinfo is not None:
                        expires = expires.replace(tzinfo=None)
                except (TypeError, ValueError):
                    expires = None
                if expires and expires > datetime.utcnow():
                    return json.loads(row["response_json"]) or None

        # Miss: fetch live
        try:
            result = fetcher()
        except (TVDBError, httpx.HTTPError) as e:
            log.warning("TVDB lookup failed for %s: %s", key, e)
            return None

        # Cache the result (even None, to avoid hammering on misses).
        # Store naive UTC isoformat to match SQLite's datetime('now') format.
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

    def _ensure_logged_in(self) -> str:
        with self._lock:
            if self._jwt and self._jwt_expires_at and self._jwt_expires_at > datetime.now(timezone.utc):
                return self._jwt
            self._login()
            return self._jwt  # type: ignore[return-value]

    def _login(self) -> None:
        if not self.api_key:
            raise TVDBAuthError("no API key configured")
        self._rate_limit()
        try:
            r = httpx.post(
                f"{TVDB_BASE}/login",
                json={"apikey": self.api_key},
                timeout=self.timeout,
            )
        except httpx.HTTPError as e:
            raise TVDBError(f"login request failed: {e}") from e

        if r.status_code in (401, 403):
            raise TVDBAuthError(f"HTTP {r.status_code} — check your API key")
        if r.status_code != 200:
            raise TVDBError(f"login returned HTTP {r.status_code}")

        try:
            data = r.json()
        except json.JSONDecodeError as e:
            raise TVDBError(f"login returned non-JSON: {e}") from e

        token = (data.get("data") or {}).get("token")
        if not token:
            raise TVDBAuthError("login response missing token")

        with self._lock:
            self._jwt = token
            # JWT is valid for 30 days; use 28 to be safe
            self._jwt_expires_at = datetime.now(timezone.utc) + timedelta(days=28)
            log.info("TVDB login successful, JWT valid until %s", self._jwt_expires_at)

    def _search(self, kind: str, title: str, year: Optional[str]) -> Optional[dict]:
        """kind is 'movie' or 'series'. Returns the first matching result."""
        token = self._ensure_logged_in()
        params: dict[str, Any] = {"query": title, "type": kind}
        if year:
            params["year"] = year
        self._rate_limit()
        try:
            r = httpx.get(
                f"{TVDB_BASE}/search",
                params=params,
                headers={"Authorization": f"Bearer {token}"},
                timeout=self.timeout,
            )
        except httpx.HTTPError as e:
            raise TVDBError(f"search failed: {e}") from e

        if r.status_code == 401:
            # JWT expired? Force refresh once.
            with self._lock:
                self._jwt = None
            token = self._ensure_logged_in()
            self._rate_limit()
            r = httpx.get(
                f"{TVDB_BASE}/search",
                params=params,
                headers={"Authorization": f"Bearer {token}"},
                timeout=self.timeout,
            )

        if r.status_code != 200:
            raise TVDBError(f"search returned HTTP {r.status_code}")

        data = r.json().get("data") or []
        if not data:
            return None
        # Take the first result; TVDB orders by relevance.
        return self._normalize(data[0], kind)

    def _search_remote_id(self, imdb_id: str) -> Optional[dict]:
        """TVDB has /search/remoteid/{id} which can resolve by IMDB."""
        token = self._ensure_logged_in()
        self._rate_limit()
        try:
            r = httpx.get(
                f"{TVDB_BASE}/search/remoteid/{imdb_id}",
                headers={"Authorization": f"Bearer {token}"},
                timeout=self.timeout,
            )
        except httpx.HTTPError as e:
            raise TVDBError(f"remoteid lookup failed: {e}") from e

        if r.status_code != 200:
            return None
        data = r.json().get("data") or []
        if not data:
            return None
        # Each entry has movie/series sub-objects
        first = data[0]
        for kind in ("movie", "series"):
            obj = first.get(kind)
            if obj:
                return self._normalize(obj, kind)
        return None

    @staticmethod
    def _normalize(raw: dict, kind: str) -> dict:
        """Pull whatever IDs we can find out of a TVDB result."""
        # TVDB v4 search result fields vary slightly between movies and series.
        # Common ones: id, name, year, imdb_id, image_url. extended endpoint
        # has more, but we usually don't need it.
        result = {
            "tvdb_id": _safe_int(raw.get("tvdb_id") or raw.get("id")),
            "tmdb_id": None,
            "imdb_id": raw.get("imdb_id") or raw.get("imdbId"),
            "title": raw.get("name") or raw.get("title"),
            "year": str(raw.get("year")) if raw.get("year") else None,
            "kind": kind,
        }
        # Some results expose remote_ids list
        for ri in raw.get("remote_ids") or []:
            src = (ri.get("sourceName") or "").strip().lower()
            val = ri.get("id")
            if src in ("imdb",) and val and not result["imdb_id"]:
                result["imdb_id"] = val
            elif src in ("themoviedb", "tmdb") and val:
                result["tmdb_id"] = _safe_int(val)
        return result


def _safe_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None
