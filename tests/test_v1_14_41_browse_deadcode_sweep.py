"""v1.14.41 — dead-code sweep: /api/items LIST + browse.html chain.

Audit api M1 finding: `/api/items` GET endpoint had a 4-query
N+1 in its row loop (up to 800 queries per 200-row page). The
endpoint was the data source for the legacy "Browse" page
(`browse.html`) which got replaced by /movies, /tv, /anime
(library page using `/api/library`). The Browse template was
no longer routed; its endpoint stayed orphaned.

Per the audit's lighter fix recommendation: deprecate the
endpoint + its caller rather than rewrite the SQL.

## What got deleted

Server:
  • `@app.get("/api/items")` (api.py — 95 lines)
  • `app/web/templates/browse.html` (entire template)

Frontend:
  • `browseState` const (app.js)
  • `loadItems()` function (app.js)
  • `bindBrowse()` function (app.js)
  • `bindBrowse()` call from page-init block

Tests:
  • `test_html_pattern_attr_accepts_soundcloud` browse.html
    assertion (only the library.html assertion still relevant)

## What got upgraded (small UX win)

The 4 standalone `loadItems().catch(()=>{})` calls inside still-
live functions (override-save, deleteOrphan, acceptUpdate,
declineUpdate) were silently failing on the library page (their
target `#items-body` only existed in browse.html). Replaced
with `loadLibrary().catch(()=>{})` so the library page actually
refreshes immediately after these actions instead of waiting for
the 30s auto-poll.

## Distinct from the off-limits item

The off-limits memory note refers to the SINGLE-item endpoint
`/api/items/{mt}/{tmdb_id}` (INFO card data source, sync→async
refactor risk). v1.14.41 only touches the LIST endpoint
`/api/items` — different endpoint, different concerns. The
single-item endpoint is unchanged.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── Server: /api/items GET endpoint deleted ─────────────────


def test_api_items_list_endpoint_removed():
    """The `@app.get("/api/items")` route registration must be
    gone — only the single-item route remains. Pin via the
    decorator string match."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    # The exact list-endpoint decorator must NOT exist anymore.
    assert '@app.get("/api/items")\n' not in src, (
        "GET /api/items LIST endpoint survived the v1.14.41 "
        "deletion. The N+1 in its row loop is supposed to be "
        "fixed by removing the endpoint entirely."
    )
    # The single-item endpoint MUST stay (off-limits territory).
    assert '@app.get("/api/items/{media_type}/{tmdb_id}")' in src


def test_api_items_list_def_removed():
    """The `async def api_items(` function definition (sibling
    of `api_item` for the single-row variant) must also be gone.
    Pin with the underscore-suffixed name."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    assert "async def api_items(" not in src
    # Single-item def stays.
    assert "async def api_item(" in src


def test_v1_14_41_archaeology_marker_in_api():
    """The v1.14.41 marker comment captures WHY the endpoint
    went — load-bearing in case someone wonders why there's no
    LIST endpoint."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    assert "v1.14.41: removed `@app.get(\"/api/items\")` LIST endpoint" in src


# ── Frontend: dead JS removed ────────────────────────────────


def test_browse_state_const_removed():
    """`browseState` was the dead browse-page state container.
    The const + every browseState.* reference must be gone."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # The const definition.
    assert "const browseState = {" not in js
    # Any property access (browseState.X) must also be gone.
    assert "browseState." not in js


def test_load_items_function_removed():
    """`loadItems()` rendered the dead `#items-body`. The
    function def + every call site must be gone."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # Function definition.
    assert "async function loadItems()" not in js
    assert "function loadItems()" not in js
    # Call sites — `loadItems(`. Allow the substring in a marker
    # comment that explains the deletion.
    # Strip lines that are pure comments (start with optional ws + //).
    code_lines = [
        ln for ln in js.split("\n")
        if not ln.lstrip().startswith("//")
    ]
    code_only = "\n".join(code_lines)
    assert "loadItems(" not in code_only, (
        "loadItems() call survived in non-comment code — "
        "should have been replaced with loadLibrary() (live UX) "
        "or removed entirely."
    )


def test_bind_browse_function_removed():
    """`bindBrowse()` returned early on every live page (no
    `#search-input`); it was a guaranteed no-op. Function +
    invocation must be gone."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # Function def.
    assert "function bindBrowse()" not in js
    # Invocation — same comment exclusion as loadItems.
    code_lines = [
        ln for ln in js.split("\n")
        if not ln.lstrip().startswith("//")
    ]
    code_only = "\n".join(code_lines)
    assert "bindBrowse(" not in code_only


# ── browse.html template gone ───────────────────────────────


def test_browse_html_template_deleted():
    """The `browse.html` template file must be gone — no route
    served it and `bindBrowse()` is the only thing that would
    have wired it up."""
    template = REPO / "app" / "web" / "templates" / "browse.html"
    assert not template.exists(), (
        f"browse.html survives at {template} — should have been "
        "deleted in the v1.14.41 sweep."
    )


# ── UX upgrade: loadLibrary() in still-live action handlers ─


def test_loadlibrary_replaces_loaditems_in_live_handlers():
    """Four standalone `loadItems().catch(()=>{})` calls inside
    still-live functions (override-save, deleteOrphan,
    acceptUpdate, declineUpdate) were silently failing — replaced
    with `loadLibrary().catch(()=>{})` so the library page
    actually refreshes immediately after these actions.

    Pin with a count: there should be at least 4 `loadLibrary()
    .catch(()=>{})` invocations in code. (More may exist in
    other paths — check the lower bound.)"""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # Pattern matches both `loadLibrary().catch(()=>{})` and the
    # arrow-callback variant `setTimeout(() => loadLibrary()
    # .catch(()=>{}), N)`. Count occurrences.
    n = js.count("loadLibrary().catch(()=>{})")
    assert n >= 4, (
        f"Expected ≥4 loadLibrary().catch() calls (the v1.14.41 "
        f"replacements for loadItems), found {n}. If lower, the "
        "UX upgrade regressed."
    )


# ── Single-item endpoint stays (off-limits territory) ───────


def test_single_item_endpoint_unchanged():
    """The single-item GET `/api/items/{mt}/{tmdb_id}` is the
    INFO card's data source and was specifically OFF-LIMITS
    per the user's prior memory note. Pin its existence so a
    future sweep doesn't accidentally take it out with the
    list endpoint."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    assert '@app.get("/api/items/{media_type}/{tmdb_id}")' in src
    assert "async def api_item(" in src
