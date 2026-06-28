"""v1.23.96 (audit) — move two unguarded blocking ops off the asyncio event loop.

From the silent-failure sweep (event-loop-block class, bug-class-12):

(1) GET /api/coverage/plex ran 4 queries — each carrying a correlated EXISTS over
    placements PER plex_items row → seconds on a large library — DIRECTLY in the
    async handler body. It's dashboard-POLLED, so it froze the event loop on every
    poll (the v1.22.69 class: api_public_stats was offloaded for the identical
    correlated-subquery shape; this sibling was missed). Now the DB work runs via
    run_in_threadpool; to_item()/dict-build stays in the async body (cheap
    pure-Python over already-fetched rows).

(2) the admin DELETE-orphan-sidecar handler ran sp.unlink() (a /data-mount FS op)
    directly in the async body — a slow mount blocked the loop. Now offloaded.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()


def test_coverage_plex_offloads_queries_to_threadpool():
    fn = API_PY.index('async def api_coverage_plex(')
    body = API_PY[fn:fn + 8000]
    # the 4 row queries are inside a nested _run() called via run_in_threadpool.
    assert "def _run():" in body
    assert "await run_in_threadpool(_run)" in body
    # the queries moved into _run (it returns the 4 row-lists).
    assert "return movies_rows, tv_rows, anime_rows, collections_rows" in body
    # the conn/query work is no longer at the top level of the async body.
    run_i = body.index("def _run():")
    assert body.index("with get_conn(db) as conn:") > run_i, (
        "the get_conn block must be inside _run(), not the async body")


def test_delete_orphan_sidecar_unlink_offloaded():
    fn = API_PY.index('async def api_admin_delete_orphan_sidecar(')
    body = API_PY[fn:fn + 4000]
    assert "await run_in_threadpool(sp.unlink)" in body
    # the bare synchronous unlink is gone.
    assert "\n            sp.unlink()\n" not in body


def test_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
