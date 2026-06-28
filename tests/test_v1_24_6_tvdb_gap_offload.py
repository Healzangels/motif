"""v1.24.6 — api_admin_tvdb_gap runs its ~13 sync conn.execute off the event loop.

The /api/admin/diagnostics/tvdb-gap handler ran all its bucket COUNT(*)s + sample
queries directly in the async body — a class-12 event-loop block (sqlite isn't in
the standing AST lint's tracked set, so it slipped the guard). Now the whole body is
a nested def offloaded via run_in_threadpool, mirroring api_coverage_plex (v1.23.96)
+ the v1.24.1 stats disk offload. Behavior is byte-identical (the v1.15.143 behavioral
test still passes); this pins the offload so a refactor can't re-inline it. Deferred
from the v1.24.0 rollover.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()


def _tvdb_gap_body() -> str:
    i = API_PY.index("async def api_admin_tvdb_gap(")
    return API_PY[i:API_PY.index("\n    # v1.22.48", i)]


def test_tvdb_gap_offloaded_to_threadpool():
    body = _tvdb_gap_body()
    assert "await run_in_threadpool(_run)" in body
    # the DB work lives in the nested def, not the async body.
    drun = body.index("def _run():")
    getconn = body.index("with get_conn(db) as conn:")
    assert drun < getconn, "the get_conn block must sit inside _run (off the loop)"
    # auth stays in the async body, before the offload.
    assert body.index("_require_admin(request)") < drun
