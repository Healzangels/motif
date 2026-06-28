"""v1.22.29 (audit) — enum/sync data-integrity: short-page truncation + 200-with-bad-body.

Two HIGH silent-data-loss findings from the full-codebase audit:

(plex.py) enumerate_section_items / enumerate_collections_for_section broke
pagination on `container_size < page_size` BEFORE the authoritative total_size
check, so a short page mid-section (Plex's X-Plex-Container-Size is advisory)
truncated the section → the v1.18.89 reaper deleted the "missing" plex_items
rows. Now total_size is the primary signal; the short-page heuristic only
applies when total_size is unavailable.

(sync.py) _fetch_index (git/remote) + the snapshot index counted ONLY
httpx/OS/parse errors as a failed page. A page that 200s with `[]` (CDN
stale-empty) or a non-list body passed silently → index_incomplete=False → the
full-walk drop sweep ran against a partial catalog and mis-stamped live themes
as TDB-dropped (the v1.21.38 class, through the 200-with-bad-body door). Now a
non-list/empty body counts as a failed page (errs SAFE — suppresses that run's
drop sweep).
"""
from __future__ import annotations

from pathlib import Path

import httpx

from app.core.sync import _fetch_index

REPO = Path(__file__).resolve().parent.parent
PLEX_PY = (REPO / "app" / "core" / "plex.py").read_text()
SYNC_PY = (REPO / "app" / "core" / "sync.py").read_text()


class _Resp:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class _FakeClient:
    """Maps a URL → payload; .get returns a _Resp (or raises for a 5xx URL)."""
    def __init__(self, by_url):
        self._by_url = by_url

    def get(self, url, **kw):
        if url not in self._by_url:
            raise httpx.HTTPError(f"404 {url}")
        return _Resp(self._by_url[url])


BASE = "https://tdb.test"
MP = "movies/themoviedb"


def _client(pages, page_bodies):
    by = {f"{BASE}/{MP}/pages.json": {"pages": pages}}
    for n, body in page_bodies.items():
        by[f"{BASE}/{MP}/all_page_{n}.json"] = body
    return _FakeClient(by)


# ── sync.py _fetch_index: bad-body counts as a failed page ────


def test_empty_list_page_counts_as_failed():
    c = _client(2, {1: [{"id": 1}], 2: []})  # page 2 is stale-empty
    items, failed = _fetch_index(c, BASE, MP)
    assert failed == 1, "an empty-list page must be counted as failed (partial index)"
    assert items == [{"id": 1}], "only the good page's items merge"


def test_dict_body_page_counts_as_failed_not_corrupting_items():
    c = _client(1, {1: {"oops": "not a list"}})
    items, failed = _fetch_index(c, BASE, MP)
    assert failed == 1, "a non-list body must count as failed"
    assert items == [], "extend(dict) must NOT corrupt items with the dict's keys"


def test_healthy_pages_have_zero_failed():
    c = _client(2, {1: [{"id": 1}], 2: [{"id": 2}]})
    items, failed = _fetch_index(c, BASE, MP)
    assert failed == 0 and items == [{"id": 1}, {"id": 2}]


def test_http_error_page_still_counts_as_failed():
    # page 2 URL absent → _FakeClient raises httpx.HTTPError → failed.
    c = _client(2, {1: [{"id": 1}]})
    items, failed = _fetch_index(c, BASE, MP)
    assert failed == 1 and items == [{"id": 1}]


# ── source pins: plex pagination reorder + snapshot guard ─────


def test_plex_pagination_total_size_is_authoritative_both_sites():
    # Both enumerate loops must check total_size FIRST, gating the short-page
    # break on `total_size is None`.
    assert PLEX_PY.count("if total_size is not None:\n                if (offset + container_size) >= total_size:") == 2, (
        "v1.22.29: both plex pagination loops must make total_size authoritative")
    assert PLEX_PY.count("elif container_size < page_size:") == 2, (
        "the short-page break must be the FALLBACK (total_size unavailable)")


def test_snapshot_index_validates_body():
    i = SYNC_PY.index("snapshot page %s unreadable")
    block = SYNC_PY[i - 400:i]
    assert "not isinstance(body, list) or not body" in block, (
        "v1.22.29: the snapshot index must reject a non-list/empty page body")


def test_fetch_index_validates_body_source_pin():
    i = SYNC_PY.index("def _fetch_index(")
    # v1.24.14 widened 2600→3400: the catalog-level bad-body / zero-page guard
    # added lines before the per-page validation this pins.
    block = SYNC_PY[i:i + 3400]
    assert "not isinstance(body, list) or not body" in block
    assert "except (httpx.HTTPError, ValueError)" in block


def test_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
