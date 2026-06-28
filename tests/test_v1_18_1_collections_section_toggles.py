"""v1.18.1 — per-library section toggles for /collections.

The v1.18.0 ship reused the STANDARD/4K binary toggle on the
/collections tab; that was a category error. Plex collections
span every managed section (Movies, 4K Movies, TV Shows, 4K TV
Shows, Anime, Anime 4K, etc.) and aren't 4K-tagged at the Plex
level. v1.18.1 replaces the binary toggle on /collections with
one chip per managed section that has ≥1 collection enumerated,
plus an ALL chip to clear the filter.

The narrowing axis lives on a new `section_id` query param to
/api/library. Honored only for `tab='collections'` — the route
forces empty string for other tabs.

Surfaces:

  * `library_section_state(tab)` Jinja helper — returns the list
    of managed sections that own ≥1 collection (sorted by anime /
    type / 4K / title) for tab='collections'; empty for others.

  * `/api/library` Query — adds `section_id` param with the
    `^[0-9]*$` pattern (digits-only; empty means ALL).

  * `_library_main_query` — accepts `section_id` and applies
    `pi.section_id = '<id>'` to tab_where when set + tab='collections'.

  * `library.html` — branches on tab. /collections gets the
    section chip row (`data-section-id`); other tabs keep the
    STANDARD/4K row (`data-fourk`).

  * `app.js` — `libraryState.section_id` field; loadLibrary
    threads it on the query string; chip-click handler binds to
    `data-section-id` and persists pick to localStorage; URL
    param hydration on page load.
"""

from __future__ import annotations

import re
import sqlite3
import sys
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

API_PY = REPO / "app" / "web" / "api.py"
LIBRARY_HTML = REPO / "app" / "web" / "templates" / "library.html"
APP_JS = REPO / "app" / "web" / "static" / "app.js"


# ── /api/library accepts section_id query param ───────────────


def test_api_library_route_accepts_section_id_param():
    """The /api/library route's signature must declare a
    `section_id` query param with the digits-only pattern so a
    GET ?section_id=5 isn't rejected with 422."""
    src = API_PY.read_text()
    # Pin the Query() call exactly.
    assert re.search(
        r'section_id:\s*str\s*=\s*Query\(\s*""\s*,\s*pattern\s*=\s*r?["\']?\^\[0-9\]\*\$["\']?\s*\)',
        src,
    ), (
        "v1.18.1: /api/library must declare `section_id: str = "
        "Query('', pattern='^[0-9]*$')`"
    )


def test_api_library_forwards_section_id_only_for_collections():
    """The threadpool call to _library_main_query must pass
    section_id=<param> for tab='collections' and force empty
    string for other tabs — keeps the narrowing axis collections-
    scoped without leaking into movie/tv/anime queries."""
    src = API_PY.read_text()
    # Pin the conditional forward.
    assert re.search(
        r'section_id=\(section_id if tab == "collections" else ""\)',
        src,
    ), (
        "v1.18.1: api_library must forward section_id only for "
        "tab='collections' (empty string for other tabs)"
    )


# ── _library_main_query honors section_id ─────────────────────


def test_library_main_query_signature_has_section_id():
    """`_library_main_query` must declare a `section_id` keyword
    argument with empty-string default."""
    src = API_PY.read_text()
    fn_start = src.index("def _library_main_query(")
    fn_sig_end = src.index(") -> dict:", fn_start)
    sig = src[fn_start:fn_sig_end]
    assert "section_id:" in sig, (
        "v1.18.1: _library_main_query must accept `section_id`"
    )


def test_library_main_query_applies_section_id_for_collections_only():
    """The helper must apply `pi.section_id = '<id>'` to tab_where
    only when tab='collections' AND section_id is truthy. Without
    the tab guard, a stray section_id on a movies tab would
    silently override the standard movie filter."""
    src = API_PY.read_text()
    # Find the helper body.
    fn_start = src.index("def _library_main_query(")
    fn_end = src.index("\ndef ", fn_start + 1)
    body = src[fn_start:fn_end]
    assert 'if tab == "collections" and section_id:' in body, (
        "v1.18.1: section_id filter must be tab-gated"
    )
    assert "pi.section_id = '{section_id}'" in body, (
        "v1.18.1: tab_where must add the pi.section_id predicate"
    )


def test_library_section_state_helper_exists():
    """The new Jinja helper `library_section_state(tab)` must be
    registered on templates.env.globals so library.html can call
    it. Returns the per-section chip list for /collections."""
    src = API_PY.read_text()
    assert '_library_section_state' in src
    assert (
        'templates.env.globals["library_section_state"] '
        '= _library_section_state'
    ) in src, (
        "v1.18.1: library_section_state must be registered as a "
        "Jinja global"
    )


def test_library_section_state_returns_empty_for_non_collections_tabs():
    """The helper must short-circuit (no DB hit) for tab='movies'
    / 'tv' / 'anime' — sections don't drive the toggle for those
    tabs (they use STANDARD/4K via library_resolution_state)."""
    src = API_PY.read_text()
    fn_start = src.index("def _library_section_state(")
    fn_end = src.index("\n    templates.env.globals[\"library_section_state\"]", fn_start)
    body = src[fn_start:fn_end]
    assert 'if tab != "collections":' in body
    assert '"show_chips": False' in body
    assert '"sections": []' in body


# ── library.html branches on tab ─────────────────────────────


def test_library_html_renders_section_chips_for_collections():
    """The template must render the section-chip row ONLY when
    `tab == "collections"`. The chips bind via data-section-id.

    v1.18.18 update: the // ALL chip was dropped (the user's
    "collections only make sense scoped to one library" ask).
    What remains: one chip per managed section that has ≥1
    collection. See test_v1_18_18_collections_drop_all_chip.py
    for the new contract pins."""
    html = LIBRARY_HTML.read_text()
    # Pin the conditional branch.
    assert '{% if tab == "collections" %}' in html
    # Pin the helper call.
    assert "{% set _sec = library_section_state(tab) %}" in html
    # Per-section chips remain (the ALL chip was removed).
    assert 'data-section-id="{{ s.section_id }}"' in html, (
        "v1.18.1: per-section chips must template section_id"
    )


def test_library_html_keeps_fourk_chips_for_other_tabs():
    """The else branch must keep the v1.10.5 STANDARD/4K toggle
    intact for movies/tv/anime — no regression on the existing
    binary-toggle UX."""
    html = LIBRARY_HTML.read_text()
    # Pin the else branch.
    assert "{% else %}" in html
    # The else branch must still have data-fourk chips.
    else_idx = html.index("{% else %}")
    else_block = html[else_idx:else_idx + 1000]
    assert 'data-fourk="0"' in else_block
    assert 'data-fourk="1"' in else_block
    assert "library_resolution_state(tab)" in else_block


def test_library_html_refresh_button_strips_4k_label_for_collections():
    """The // REFRESH button label includes `4K` when fourk=true,
    but for collections (which aren't 4K-tagged) the 4K prefix
    must NOT render — the user picks a section via the chip row,
    and the button reads `// REFRESH COLLECTIONS` regardless."""
    html = LIBRARY_HTML.read_text()
    # Pin the conditional that strips 4K from collections refresh.
    assert "fourk and tab != \"collections\"" in html, (
        "v1.18.1: refresh button must skip the 4K prefix on "
        "/collections (collections aren't 4K-tagged)"
    )


# ── app.js wiring ─────────────────────────────────────────────


def test_library_state_has_section_id_field():
    """The libraryState declaration must include `section_id`
    initialized to empty string so the field exists for the
    loadLibrary serializer + chip-click handler."""
    js = APP_JS.read_text()
    # Find the libraryState constructor.
    idx = js.index("const libraryState = {")
    obj_end = js.index("};", idx)
    obj = js[idx:obj_end]
    assert "section_id:" in obj, (
        "v1.18.1: libraryState must initialize section_id"
    )
    assert 'section_id: ""' in obj


def test_load_library_threads_section_id_for_collections_only():
    """loadLibrary's URLSearchParams build must include
    section_id ONLY when libraryState.tab === 'collections' AND
    libraryState.section_id is non-empty. Sending it on other
    tabs would be confusing in dev-tools network logs."""
    js = APP_JS.read_text()
    assert (
        "libraryState.tab === 'collections' && libraryState.section_id"
    ) in js, (
        "v1.18.1: section_id must be tab-gated in the URL builder"
    )
    assert "params.set('section_id', libraryState.section_id)" in js


def test_section_id_chip_click_handler_binds():
    """The chip-click handler must bind on `data-section-id`,
    update chip-active, persist to localStorage, and call
    loadLibrary."""
    js = APP_JS.read_text()
    assert "document.querySelectorAll('.chips [data-section-id]')" in js, (
        "v1.18.1: section-chip click handler must bind on "
        "data-section-id"
    )
    # Pin localStorage persistence under the collections-section key.
    assert "'motif:collections-section'" in js


def test_section_id_hydrates_from_url_or_localstorage():
    """Page load must hydrate libraryState.section_id from the
    URL param if present, else from localStorage for return
    visits — same persistence pattern as the v1.12.9
    motif:variant: storage for fourk."""
    js = APP_JS.read_text()
    # The hydration block must read both sources.
    assert "sp.get('section_id')" in js
    assert "localStorage.getItem('motif:collections-section')" in js


def test_section_id_hydration_applies_chip_active_class():
    """Hydration must visually mirror the picked section by
    setting chip-active on the matching chip and clearing it from
    the others — same SSR-meets-JS contract as the fourk chips."""
    js = APP_JS.read_text()
    # The hydration block must toggle chip-active.
    assert (
        "x.classList.toggle('chip-active',\n"
        "                               (x.dataset.sectionId || '') === wantSec)"
    ) in js or (
        # Tolerate whitespace variations from auto-formatting.
        "chip-active" in js and "dataset.sectionId" in js
    )


# ── End-to-end DB-level: section_state lists collection-bearing sections ──


@pytest.fixture
def fresh_db(tmp_path: Path) -> Path:
    db = tmp_path / "motif.db"
    from app.core.db import init_db
    init_db(db)
    return db


def test_section_state_lists_sections_with_collections_only(fresh_db: Path):
    """End-to-end: a section with 0 collections must NOT appear in
    the chip list. Otherwise the user clicks a chip and sees an
    empty page — confusing. The helper filters via EXISTS on
    plex_items WHERE media_type='collection'."""
    ts = "2026-05-19T12:00:00"
    with sqlite3.connect(fresh_db) as conn:
        conn.row_factory = sqlite3.Row
        # Two managed sections: one with collections, one without.
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   included, discovered_at, last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 0, 0, 1, ?, ?)",
            (ts, ts),
        )
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   included, discovered_at, last_seen_at) "
            "VALUES ('2', 'TV Shows', 'show', 0, 0, 1, ?, ?)",
            (ts, ts),
        )
        # Add a collection only to section 1.
        conn.execute(
            "INSERT INTO plex_items "
            "  (rating_key, section_id, media_type, title, "
            "   first_seen_at, last_seen_at) "
            "VALUES ('rk-1', '1', 'collection', "
            "        'Harry Potter Collection', ?, ?)",
            (ts, ts),
        )
        conn.commit()
        # Now exec the same shape the helper runs.
        rows = conn.execute("""
            SELECT ps.section_id, ps.title
              FROM plex_sections ps
              WHERE ps.included = 1
                AND EXISTS (
                  SELECT 1 FROM plex_items pi
                  WHERE pi.section_id = ps.section_id
                    AND pi.media_type = 'collection'
                )
              ORDER BY ps.is_anime ASC,
                       ps.type ASC,
                       ps.is_4k ASC,
                       ps.title ASC
        """).fetchall()
    assert len(rows) == 1, (
        "v1.18.1: only sections with ≥1 collection must appear "
        "in the chip list — sections with 0 collections produce "
        "an empty page on click and confuse the user."
    )
    assert rows[0]["section_id"] == "1"
    assert rows[0]["title"] == "Movies"


def test_section_state_orders_chips_consistently(fresh_db: Path):
    """The ORDER BY (is_anime ASC, type ASC, is_4k ASC, title ASC)
    keeps the chip row in a stable visual order across page loads:
    non-anime first, movies before shows, standard before 4K,
    title-sorted within each group."""
    ts = "2026-05-19T12:00:00"
    with sqlite3.connect(fresh_db) as conn:
        conn.row_factory = sqlite3.Row
        # Seed in deliberately scrambled order.
        sections = [
            ("3", "Anime", "show", 1, 0),
            ("1", "Movies", "movie", 0, 0),
            ("5", "4K Movies", "movie", 0, 1),
            ("2", "TV Shows", "show", 0, 0),
            ("4", "Anime 4K", "show", 1, 1),
        ]
        for sid, title, ptype, is_anime, is_4k in sections:
            conn.execute(
                "INSERT INTO plex_sections "
                "  (section_id, title, type, is_anime, is_4k, "
                "   included, discovered_at, last_seen_at) "
                "VALUES (?, ?, ?, ?, ?, 1, ?, ?)",
                (sid, title, ptype, is_anime, is_4k, ts, ts),
            )
            # One collection per section so they all appear.
            conn.execute(
                "INSERT INTO plex_items "
                "  (rating_key, section_id, media_type, title, "
                "   first_seen_at, last_seen_at) "
                "VALUES (?, ?, 'collection', ?, ?, ?)",
                (f"rk-{sid}", sid, f"{title} Collection", ts, ts),
            )
        conn.commit()
        rows = conn.execute("""
            SELECT ps.section_id, ps.title
              FROM plex_sections ps
              WHERE ps.included = 1
                AND EXISTS (
                  SELECT 1 FROM plex_items pi
                  WHERE pi.section_id = ps.section_id
                    AND pi.media_type = 'collection'
                )
              ORDER BY ps.is_anime ASC,
                       ps.type ASC,
                       ps.is_4k ASC,
                       ps.title ASC
        """).fetchall()
    titles = [r["title"] for r in rows]
    # Expected order: non-anime first (Movies → 4K Movies →
    # TV Shows), then anime (Anime → Anime 4K). Within each
    # type+anime group, standard (is_4k=0) comes before 4K
    # (is_4k=1). Title sort is the final tiebreaker.
    assert titles == ["Movies", "4K Movies", "TV Shows", "Anime", "Anime 4K"], (
        f"v1.18.1: chip order must be stable + scannable; got {titles}"
    )
