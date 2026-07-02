"""v0.51.12 — round-4 audit Batch B: the v1.23.71 client-tab-switch cluster.

The in-place (pushState) library tab switcher skipped three things the old
full navigation did implicitly:

#14/#18: the 30s background poll + v1.22.36 stuck-row reconciler are armed once
  at DOMContentLoaded from the LANDING pathname, and the gate omitted
  '/collections' — so /collections never got either, and a session that landed
  there and client-switched to /movies ran without them for its whole life.
#15: the filterbar is server-rendered per tab (SRC A/M, LINK HL/C, ED row are
  gated on tab != 'collections') and is NOT swapped by switchLibraryTab (its
  pill handlers are bound once at init — an innerHTML swap would drop them,
  the v0.50.96 lost-listener class). Crossing the collections boundary now
  falls back to a full navigation; movies/tv/anime (identical filterbar)
  keep the fast path.
#16: libraryState.selected/selectedRows survived the switch, leaving the bulk
  bar armed with the previous tab's off-screen rows.

JS source pins (the project's convention for app.js behavior; quickjs parse +
the full-nav semantics are the behavioral layer).
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
LIBRARY_HTML = (REPO / "app" / "web" / "templates" / "library.html").read_text()


def _fn(name: str) -> str:
    i = APP_JS.index(f"function {name}(")
    # slice to the next top-level function declaration — coarse but stable.
    j = APP_JS.index("\n  function ", i + 10)
    return APP_JS[i:j]


# ── #14/#18: poll + reconciler arm on /collections too ──────────────────

def test_arming_gate_includes_collections():
    m = re.search(
        r"if \(path === '/movies' \|\| path === '/tv' \|\| path === '/anime'"
        r"\s*\|\| path === '/collections'\)", APP_JS)
    assert m, "the 30s poll + stuck-row reconciler must arm on /collections too"
    # both intervals still live inside that gate: the block must contain the
    # reconciler marker AND the 30s tick.
    block = APP_JS[m.start():m.start() + 3000]
    assert "v1.22.36: stuck-row reconciler" in block
    assert "30000" in block and "6000" in block


# ── #15: collections-boundary switches fall back to full navigation ─────

def test_switch_crossing_collections_boundary_full_navs():
    fn = _fn("switchLibraryTab")
    i_guard = fn.index("(tab === 'collections') !== (_curTabEl.value === 'collections')")
    i_fetch = fn.index("await fetch(")
    assert i_guard < i_fetch, (
        "the boundary check must run BEFORE the in-place fetch/swap")
    assert "window.location.href = href; return;" in fn[i_guard - 200:i_guard + 200]


def test_filterbar_tab_gates_are_collections_only():
    """The fast path's soundness rests on movies/tv/anime rendering an
    IDENTICAL filterbar — every tab gate in library.html must test against
    'collections' only. A future movies-vs-tv gate would silently break the
    in-place switch again."""
    for m in re.finditer(r"{%-? if[^%]*\btab\s*[!=]=", LIBRARY_HTML):
        cond = LIBRARY_HTML[m.start():LIBRARY_HTML.index("%}", m.start())]
        assert "collections" in cond, (
            f"non-collections tab gate found: {cond!r} — the v0.51.12 "
            "boundary-fallback assumption no longer holds")


# ── #16: selection cleared on tab switch ────────────────────────────────

def test_hydrate_clears_selection():
    fn = _fn("hydrateLibraryStateForTab")
    assert "libraryState.selected.clear();" in fn
    assert "libraryState.selectedRows.clear();" in fn
    assert "updateLibrarySelectionUi();" in fn, (
        "the bulk bar must hide immediately, not wait for the fetch")


def test_pill_filters_still_persist_across_switch():
    # counter-guard: the intentional v1.23.71 persistence (filters, q, sort)
    # must survive — only SELECTION resets.
    fn = _fn("hydrateLibraryStateForTab")
    assert "pill filters (SRC/TDB/DL/PL/LINK/ATTN), q + sort are intentionally KEPT" in fn
    assert "srcFilter.clear()" not in fn
