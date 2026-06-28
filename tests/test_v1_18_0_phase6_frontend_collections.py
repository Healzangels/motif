"""v1.18.0 Phase 6 — Frontend wiring for /collections.

Four surfaces in this phase:

  1. `/collections` route — new TemplateResponse parallel to
     `/movies`, `/tv`, `/anime`, all four backed by library.html.
     The route passes `tab='collections'` + `fourk=False`
     (collections aren't 4K-tagged).

  2. `base.html` nav — `<a href="/collections" data-nav="collections">
     COLLECTIONS</a>` rendered alongside MOVIES / TV SHOWS / ANIME.
     No nav_tab_availability gate yet — collections live within
     parent sections, so any included Plex section makes
     collections eligible.

  3. `app.js` `computeSrcLetter` — extends the `placed` predicate
     from `!!it.media_folder` to ALSO include
     `it.placement_kind === 'plex_upload'`. Without this, motif-
     uploaded collection themes had `media_folder=''` (empty
     string = falsy) and fell through to '–'.

  4. `app.js` `rowMt` dispatch for the TDB-coverage pill gate —
     adds a `collection` branch when `theme_media_type='collection'`
     or `plex_media_type='collection'`, with a parallel
     `window.__motif_themes_have.collection` bucket sourced from
     `stats.collections.tdb_total`.

This file is a source-grep pin set — each test reads the
relevant file and asserts the wiring is intact. JS behavior tests
(simulating the DOM + clicking) live in tests/test_app_js_*
files; this file gates the contract.
"""

from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"
BASE_HTML = REPO / "app" / "web" / "templates" / "base.html"
APP_JS = REPO / "app" / "web" / "static" / "app.js"


# ── Route registration ────────────────────────────────────────


def test_collections_route_registered():
    """`@app.get("/collections")` must exist with a TemplateResponse
    rendering library.html — the same template the other tabs use,
    parameterized by tab='collections'."""
    src = API_PY.read_text()
    assert '@app.get("/collections"' in src, (
        "v1.18.0: /collections route must be registered"
    )
    # Pin the handler signature + template arg.
    assert "collections_page" in src
    assert '"tab": "collections"' in src, (
        "v1.18.0: /collections handler must pass tab='collections' "
        "to library.html"
    )


def test_collections_route_uses_library_template():
    """Reuses library.html (vs spinning up a new template) —
    keeps the markup parallel to movies/tv/anime so a shared CSS
    pass + a single layout audit covers all four tabs."""
    src = API_PY.read_text()
    # The route block must reference library.html.
    idx = src.index('@app.get("/collections"')
    handler_block = src[idx:idx + 600]
    assert '"library.html"' in handler_block


def test_collections_route_no_fourk():
    """The route must pass `fourk=False` — collections aren't
    4K-tagged at the Plex level, and the /api/library SQL skips
    the is_4k filter for them. Surfacing a 4K toggle in the
    library.html toolbar would be misleading."""
    src = API_PY.read_text()
    idx = src.index('@app.get("/collections"')
    handler_block = src[idx:idx + 1000]
    assert '"fourk": False' in handler_block, (
        "v1.18.0: /collections handler must pass fourk=False — "
        "collections aren't 4K-tagged at the Plex level."
    )


# ── Nav link ──────────────────────────────────────────────────


def test_nav_has_collections_link():
    """base.html top-nav must include a /collections link parallel
    to MOVIES / TV SHOWS / ANIME."""
    html = BASE_HTML.read_text()
    assert 'href="/collections"' in html, (
        "v1.18.0: base.html nav must include a /collections link"
    )
    assert 'data-nav="collections"' in html, (
        "v1.18.0: the /collections nav link must have "
        "data-nav='collections' for the active-tab highlighter"
    )
    # Pin the visible label.
    assert ">COLLECTIONS<" in html


# ── JS: computeSrcLetter recognizes plex_upload ──────────────


def test_compute_src_letter_treats_plex_upload_as_placed():
    """The `placed` predicate in computeSrcLetter must accept
    `placement_kind === 'plex_upload'` so collection rows (which
    have media_folder='' — falsy) classify as T/U based on
    source_kind instead of falling through to P / sidecar / –."""
    js = APP_JS.read_text()
    fn_idx = js.index("function computeSrcLetter(it)")
    fn_end = js.index("\n  function ", fn_idx + 1)
    fn_body = js[fn_idx:fn_end]
    assert "placement_kind === 'plex_upload'" in fn_body, (
        "v1.18.0: computeSrcLetter must recognize "
        "placement_kind='plex_upload' as a placed state — "
        "without this, motif-uploaded collection themes "
        "(media_folder='' which is falsy) classify as '–'."
    )


# ── JS: rowMt + themes_have collections bucket ───────────────


def test_row_mt_dispatch_includes_collection():
    """The rowMt dispatch in renderLibraryRow's TDB-coverage gate
    must map theme_media_type='collection' /
    plex_media_type='collection' to a 'collection' bucket. Without
    this, collection rows fall through to 'movie' and silently
    consume the movies-side TDB-coverage signal."""
    js = APP_JS.read_text()
    # The dispatch must include a 'collection' return.
    assert "rowMt = 'collection'" in js, (
        "v1.18.0: rowMt dispatch must return 'collection' for "
        "collection rows (theme_media_type or plex_media_type)"
    )
    # And it must check both shapes.
    assert "theme_media_type === 'collection'" in js
    assert "plex_media_type === 'collection'" in js


def test_themes_have_has_collection_bucket():
    """window.__motif_themes_have must carry a `collection`
    bucket sourced from stats.collections.tdb_total so the TDB
    pill gate reads truthfully when collections sync ran but
    movies/tv haven't."""
    js = APP_JS.read_text()
    # The construction must include the collection key.
    assert "collection: coll_tdb > 0" in js, (
        "v1.18.0: __motif_themes_have must include a "
        "'collection' bucket"
    )
    # And it must source from stats.collections.tdb_total
    # (defensive — Phase 7 wires the actual stat shape).
    assert "stats.collections" in js


def test_themes_have_any_includes_collection_count():
    """The aggregate `any` flag must include collections in its
    sum so the cross-tab haveTdb gate fires when ONLY collections
    have synced (e.g., during a partial migration window)."""
    js = APP_JS.read_text()
    assert "(movies_tdb + tv_tdb + coll_tdb) > 0" in js, (
        "v1.18.0: themes_have.any aggregate must include "
        "collections in its sum"
    )
