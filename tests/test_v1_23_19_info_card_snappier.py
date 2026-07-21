"""v1.23.19 — snappier INFO cards.

the user: "sometimes I notice a bit of a delay when I open an info card."
Diagnosis: api_item runs ~39 indexed reads directly on the async event
loop, so opening a card while a sync/enum/download is busy made it wait
behind that blocking work — and the card itself blocked the loop.

Two changes:
1. api_item's pure-DB, await-free body is offloaded via
   run_in_threadpool (class-12) so it no longer waits behind — or
   contributes to — event-loop contention. Behaviour is identical
   (verified by the existing behavioural test
   test_v1_22_19_info_card_noop_suppression which GETs /api/items
   through the offloaded path).
2. Frontend prefetch: hovering a row's ⓘ kicks off the api_item GET and
   caches the promise for 6s, so the click reuses an in-flight (or
   finished) request and the card opens instantly.
"""
from __future__ import annotations

import ast
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def test_api_item_offloaded_to_threadpool():
    i = API_PY.index("async def api_item(")
    # the next route after api_item bounds the search window.
    nxt = API_PY.index('@app.post("/api/items/{media_type}/{tmdb_id}/unplace")', i)
    body = API_PY[i:nxt]
    assert "def _build():" in body, "api_item body must be wrapped in a sync inner fn"
    assert "return await run_in_threadpool(_build)" in body, (
        "api_item must offload its DB work off the event loop"
    )
    # the queries must live INSIDE the nested fn (not the async direct body).
    build_i = body.index("def _build():")
    assert body.index("with get_conn(db) as conn:") > build_i


def test_api_py_still_parses():
    ast.parse(API_PY)


def test_prefetch_helpers_and_hover_wired():
    assert "function prefetchInfo(" in APP_JS
    assert "function _infoFetch(" in APP_JS
    assert "const _infoPrefetch = new Map();" in APP_JS
    # openInfoDialog reuses the prefetch instead of a bare GET.
    # v0.51.218: pin the INTENT (openInfoDialog goes through the prefetch helper) rather
    # than a verbatim argument list — the arity grew when the card learned to scope by
    # edition, and a literal match failed on a change that preserved this invariant
    # exactly. The trailing args are checked by that tag's own test.
    assert "_infoFetch(_infoUrl(mediaType, tmdbId, sectionId, ratingKey" in APP_JS
    # the ⓘ-hover listener kicks the prefetch.
    assert "addEventListener('mouseover'" in APP_JS
    assert 'button[data-act="info"]' in APP_JS
