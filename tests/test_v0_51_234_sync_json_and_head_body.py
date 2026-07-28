"""v0.51.234 — three audit-wave-7 fixes, verified behaviorally.

1. _fetch_item's `except httpx.HTTPError` did not catch json.JSONDecodeError
   (a ValueError), so a 200 carrying a non-JSON body escaped the handler,
   propagated out of _do_fetch, and hit the un-try'd fut.result() in the
   remote-tier loop — one bad item aborted the whole sync.
2. _head_or_get_status's GET fallback read the full body (a theme MP3) to
   obtain a status code.
3. idx_sync_runs_started existed only in the v36->v37 migration, so fresh
   installs never created it.
"""
from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

import httpx

from app.core.sync import _fetch_item


# ── 1. a non-JSON 200 must not escape _fetch_item ────────────────────────

def _client_returning(bodies: dict[str, tuple[int, bytes]]) -> httpx.Client:
    def handler(request: httpx.Request) -> httpx.Response:
        status, body = bodies.get(str(request.url), (404, b""))
        return httpx.Response(status, content=body)
    return httpx.Client(transport=httpx.MockTransport(handler))


def test_non_json_body_does_not_raise_out_of_fetch_item():
    """A CDN/proxy error page served with status 200. Pre-fix this raised
    JSONDecodeError straight through the httpx-only handler."""
    base = "https://tdb.example/db"
    url = f"{base}/movies/themoviedb/603.json"
    with _client_returning({url: (200, b"<html>502 Bad Gateway</html>")}) as c:
        got = _fetch_item(c, base, "movies", imdb_id=None, tmdb_id=603)
    # returns None (the caller's existing "record is None" error path), never raises
    assert got is None


def test_bad_imdb_body_still_falls_through_to_themoviedb():
    """Raising past `continue` abandoned the candidate loop, so the perfectly
    good themoviedb record was never tried."""
    base = "https://tdb.example/db"
    bad = f"{base}/movies/imdb/tt0468569.json"
    good = f"{base}/movies/themoviedb/155.json"
    with _client_returning({
        bad: (200, b"not json at all"),
        good: (200, b'{"youtube_theme_url": "https://youtu.be/abc"}'),
    }) as c:
        got = _fetch_item(c, base, "movies", imdb_id="tt0468569", tmdb_id=155)
    assert got == {"youtube_theme_url": "https://youtu.be/abc"}


def test_valid_json_still_returned_unchanged():
    base = "https://tdb.example/db"
    url = f"{base}/movies/themoviedb/603.json"
    with _client_returning({url: (200, b'{"ok": true}')}) as c:
        assert _fetch_item(c, base, "movies", imdb_id=None, tmdb_id=603) == {"ok": True}


# ── 2. the GET fallback must not download the body ───────────────────────

class _CountingStream(httpx.SyncByteStream):
    """Yields the payload in chunks, counting how many bytes were actually pulled.
    A plain client.get() drains this; client.stream() without .read() never does."""

    def __init__(self, payload: bytes, chunk: int = 64 * 1024):
        self._payload, self._chunk = payload, chunk
        self.bytes_pulled = 0

    def __iter__(self):
        for i in range(0, len(self._payload), self._chunk):
            part = self._payload[i:i + self._chunk]
            self.bytes_pulled += len(part)
            yield part

    def close(self) -> None:
        pass


def test_head_fallback_get_does_not_read_the_theme_body():
    """HEAD 405 -> GET fallback. The status must still be returned, but the
    multi-MB theme payload must never be pulled into memory."""
    from app.core.plex import PlexClient

    body = _CountingStream(b"\xff\xfb" + b"\x00" * (2 * 1024 * 1024))

    class _T(httpx.BaseTransport):
        def handle_request(self, request):
            if request.method == "HEAD":
                return httpx.Response(405)
            return httpx.Response(200, stream=body)

    client = PlexClient.__new__(PlexClient)   # bypass __init__/network
    client._client = httpx.Client(transport=_T(), base_url="http://plex.test:32400")
    try:
        status = client._head_or_get_status("/library/metadata/1/theme")
    finally:
        client._client.close()

    assert status == 200, "the GET fallback must still report the real status"
    assert body.bytes_pulled == 0, (
        f"the theme body must not be downloaded to read a status code "
        f"({body.bytes_pulled} bytes pulled)")


def test_head_200_short_circuits_without_any_get():
    from app.core.plex import PlexClient

    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        return httpx.Response(200)

    client = PlexClient.__new__(PlexClient)
    client._client = httpx.Client(transport=httpx.MockTransport(handler),
                                  base_url="http://plex.test:32400")
    try:
        assert client._head_or_get_status("/library/metadata/1/theme") == 200
    finally:
        client._client.close()
    assert calls == ["HEAD"], "a 200 HEAD must not be followed by a GET"


# ── 3. fresh installs get the sync_runs index ────────────────────────────

def test_fresh_install_has_idx_sync_runs_started():
    from app.core.db import init_db

    d = Path(tempfile.mkdtemp()) / "fresh.db"
    init_db(d)
    conn = sqlite3.connect(d)
    try:
        names = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='sync_runs'")}
    finally:
        conn.close()
    assert "idx_sync_runs_started" in names, (
        "created only in _migrate_v36_to_v37, so fresh installs diverged from "
        "migrated ones — the class _migrate_v9_to_v10 documents")


def test_sparkline_ordering_uses_the_index_not_a_scan():
    from app.core.db import init_db

    d = Path(tempfile.mkdtemp()) / "fresh2.db"
    init_db(d)
    conn = sqlite3.connect(d)
    try:
        plan = " ".join(str(r[-1]) for r in conn.execute(
            "EXPLAIN QUERY PLAN "
            "SELECT * FROM sync_runs ORDER BY started_at DESC LIMIT 20"))
    finally:
        conn.close()
    assert "idx_sync_runs_started" in plan, plan
    # "SCAN x USING INDEX y" is the index-ordered walk (no sort). The
    # regression signal is a temp b-tree, which is what an absent index forces.
    assert "TEMP B-TREE" not in plan.upper(), plan
