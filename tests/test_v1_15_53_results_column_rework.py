"""v1.15.53 — Results table column rework: add ED column, reorder,
drop [Movies] section label.

the user: "I think I would like to reorder and add a filter column
to our current Results sections. I would like it to be Title,
Edition, Year, TDB, SRC, DL, PL, Link, IMDB, Actions. Each besides
actions should be sortable. Also for TDB right now we're
mentioning the library such as Movies, Anime, TV Shows etc let's
drop that so we're just showing the TDB Pill. Edition can also be
shortened to ED to match the existing filter."

## Changes

1. Column order: TITLE / ED / YEAR / TDB / SRC / DL / PL / LINK /
   IMDB / ACTIONS (was TITLE / TDB / YEAR / SRC / DL / PL / LINK /
   IMDB / ACTIONS).
2. New ED column — promoted from inline title-cell pill. Sortable
   via data-sort="edition"; SQL extracts {edition-...} substring
   from pi.folder_path.
3. TDB cell drops the `[Movies]`/`[Anime]`/`[TV Shows]` section
   label — the active tab nav already identifies the library.
4. All columns except ACTIONS are sortable (consistent with
   pre-v1.15.53 behavior).
5. col-tdb width tightened 180px → 90px (no longer holds the
   section label).

Static-text guards consistent with v1.13.67 column-promotion
test patterns.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
LIB_HTML = REPO / "app" / "web" / "templates" / "library.html"
APP_JS = REPO / "app" / "web" / "static" / "app.js"
APP_CSS = REPO / "app" / "web" / "static" / "app.css"
API_PY = REPO / "app" / "web" / "api.py"


# ── 1. Template header order ─────────────────────────────────


EXPECTED_HEADER_ORDER = [
    'data-sort="title"',
    'data-sort="edition"',
    'data-sort="year"',
    'data-sort="tdb"',
    'data-sort="src"',
    'data-sort="dl"',
    'data-sort="pl"',
    'data-sort="link"',
    'data-sort="imdb"',
]


def test_library_table_header_order_matches_v1_15_53_spec():
    """The <thead> column order must be TITLE / ED / YEAR / TDB /
    SRC / DL / PL / LINK / IMDB / ACTIONS. Header offsets must
    appear in the expected order in the source."""
    html = LIB_HTML.read_text()
    table_anchor = html.index('id="library-table"')
    thead_anchor = html.index('<thead>', table_anchor)
    thead_end = html.index('</thead>', thead_anchor)
    thead_block = html[thead_anchor:thead_end]
    offsets = []
    for marker in EXPECTED_HEADER_ORDER:
        idx = thead_block.find(marker)
        assert idx != -1, (
            f"v1.15.53: missing header {marker!r} in <thead>"
        )
        offsets.append((marker, idx))
    # Verify monotonically increasing offsets (= source order).
    for prev, curr in zip(offsets, offsets[1:]):
        assert prev[1] < curr[1], (
            f"v1.15.53: column order wrong — {prev[0]!r} (offset "
            f"{prev[1]}) must come BEFORE {curr[0]!r} (offset "
            f"{curr[1]})"
        )
    # ACTIONS header is non-sortable; must come LAST.
    actions_idx = thead_block.index('col-actions')
    assert actions_idx > offsets[-1][1], (
        "v1.15.53: ACTIONS column must come after IMDB (final)"
    )


def test_ed_column_header_uses_short_label():
    """the user: 'Edition can also be shortened to ED to match the
    existing filter.' The column header label must be 'ED', not
    'EDITION'."""
    html = LIB_HTML.read_text()
    ed_anchor = html.index('data-sort="edition"')
    block = html[ed_anchor:ed_anchor + 300]
    # v1.23.6: th-label wrapper retired (inline carat layout) — the
    # label is bare text inside .th-stack now.
    assert '<span class="th-stack">ED<span' in block, (
        "v1.15.53: ED column header label must be 'ED' (short form)"
    )
    assert 'EDITION' not in block, (
        "v1.15.53: header must not say 'EDITION' (use 'ED' to match "
        "the ED filter row)"
    )


def test_library_loading_row_colspan_is_11():
    """Adding the ED column means the loading-state placeholder
    row's colspan must bump from 10 → 11. Without it the loading
    message renders at the wrong width during page load."""
    html = LIB_HTML.read_text()
    assert '<td colspan="11"' in html, (
        "v1.15.53: loading row colspan must be 11 (10 + new ED col)"
    )
    assert '<td colspan="10" class="muted center">loading' not in html, (
        "v1.15.53: stale colspan=10 must be updated"
    )


# ── 2. JS row template uses new TD order ─────────────────────


def test_js_row_template_uses_new_td_order():
    """renderLibraryRow's <tr> template must emit TDs in the new
    order: state / title / edition / year / tdb / src / dl / pl /
    link / imdb / actions. Pin the col-edition position
    immediately after col-title (col without class) and before
    col-year."""
    js = APP_JS.read_text()
    fn_anchor = js.index("function renderLibraryRow(it)")
    fn_end = js.index("\n  function ", fn_anchor + 1)
    fn_body = js[fn_anchor:fn_end]
    # Find the <tr> template literal block specifically (avoids
    # matching markers elsewhere in the function body — there's
    # ~1200 lines of helper code before the return statement).
    tr_anchor = fn_body.rindex("<tr${rowExtra}")
    tr_end = fn_body.index("</tr>", tr_anchor)
    tr_block = fn_body[tr_anchor:tr_end]
    # Unique markers in expected order (one per TD).
    td_markers = [
        'data-lib-select',                # select checkbox
        '<span class="title-cell-name">',  # title text
        '<td class="col-edition">',
        '<td class="col-year">',
        '<td class="col-tdb">',
        '${srcCell}',
        '${htmlEscape(dlTip)}',
        '${htmlEscape(plTip)}',
        '${linkCell}',
        '<td class="col-imdb">',
        '<td class="col-actions">',
    ]
    offsets = []
    for marker in td_markers:
        idx = tr_block.find(marker)
        assert idx != -1, (
            f"v1.15.53: missing TD marker {marker!r} in <tr> template"
        )
        offsets.append((marker, idx))
    for prev, curr in zip(offsets, offsets[1:]):
        assert prev[1] < curr[1], (
            f"v1.15.53: row TD order wrong — {prev[0]!r} must come "
            f"before {curr[0]!r}"
        )


def test_js_tdb_cell_drops_section_label():
    """TDB cell must contain just the tdbAvailLabel — not
    sectionLabel. the user: 'for TDB right now we're mentioning
    the library such as Movies, Anime, TV Shows etc let's drop
    that so we're just showing the TDB Pill.'"""
    js = APP_JS.read_text()
    fn_anchor = js.index("function renderLibraryRow(it)")
    fn_end = js.index("\n  function ", fn_anchor + 1)
    fn_body = js[fn_anchor:fn_end]
    # The TDB cell must NOT include ${sectionLabel}.
    tdb_anchor = fn_body.index('<td class="col-tdb">')
    cell_end = fn_body.index('</td>', tdb_anchor)
    cell = fn_body[tdb_anchor:cell_end]
    assert "sectionLabel" not in cell, (
        "v1.15.53: TDB cell must NOT include ${sectionLabel} — "
        "drop the [Movies]/[Anime] tag per the user's ask"
    )
    assert "tdbAvailLabel" in cell, (
        "v1.15.53: TDB cell must still render tdbAvailLabel"
    )


def test_js_edition_cell_carries_edition_label():
    """The new col-edition cell must render ${editionLabel}
    (the parsed {edition-...} pill from the row's folder_path).
    Without this the column is always empty."""
    js = APP_JS.read_text()
    fn_anchor = js.index("function renderLibraryRow(it)")
    fn_end = js.index("\n  function ", fn_anchor + 1)
    fn_body = js[fn_anchor:fn_end]
    # Anchor on the actual <tr> template, not the rationale comment
    # block above it (which also references col-edition by name).
    tr_anchor = fn_body.rindex("<tr${rowExtra}")
    tr_end = fn_body.index("</tr>", tr_anchor)
    tr_block = fn_body[tr_anchor:tr_end]
    edition_td_anchor = tr_block.index('<td class="col-edition">')
    cell_end = tr_block.index('</td>', edition_td_anchor)
    cell = tr_block[edition_td_anchor:cell_end]
    assert "${editionLabel}" in cell, (
        "v1.15.53: col-edition cell must render ${editionLabel}"
    )


def test_js_title_cell_no_longer_inlines_edition():
    """Edition was promoted out of the title cell — the title
    cell must no longer carry ${editionLabel} (would cause it to
    render twice)."""
    js = APP_JS.read_text()
    fn_anchor = js.index("function renderLibraryRow(it)")
    fn_end = js.index("\n  function ", fn_anchor + 1)
    fn_body = js[fn_anchor:fn_end]
    title_cell_anchor = fn_body.index('<div class="title-cell"')
    title_cell_end = fn_body.index('</div>', title_cell_anchor)
    title_cell = fn_body[title_cell_anchor:title_cell_end]
    assert "${editionLabel}" not in title_cell, (
        "v1.15.53: title cell must NOT inline ${editionLabel} — "
        "it's promoted to its own col-edition td. Duplicate "
        "rendering = visual bug"
    )


def test_js_not_in_plex_row_has_same_column_count():
    """renderLibraryRowNotInPlex must emit exactly the same TD
    count + same column order as renderLibraryRow. Mismatched
    column counts = table renders askew when a not-in-plex row
    appears mid-list."""
    js = APP_JS.read_text()
    fn_anchor = js.index("function renderLibraryRowNotInPlex(it)")
    fn_end = js.index("\n  function ", fn_anchor + 1)
    fn_body = js[fn_anchor:fn_end]
    # Find the <tr> template specifically.
    tr_anchor = fn_body.rindex('<tr class="row-not-in-plex">')
    tr_end = fn_body.index("</tr>", tr_anchor)
    tr_block = fn_body[tr_anchor:tr_end]
    # Unique markers in expected order.
    nip_markers = [
        'data-lib-select',
        '<span class="title-cell-name muted">',
        '<td class="col-edition">',
        '<td class="col-year">',
        '<td class="col-tdb">',
        'link-badge-themerrdb-only',  # SRC T badge
        'state-pill" title="not applicable',  # DL/PL pills
        'link-glyph-none',  # LINK cell
        '<td class="col-imdb">',
        '<td class="col-actions">',
    ]
    offsets = []
    for marker in nip_markers:
        idx = tr_block.find(marker)
        assert idx != -1, (
            f"v1.15.53: not-in-plex row missing TD {marker!r}"
        )
        offsets.append(idx)
    for prev, curr in zip(offsets, offsets[1:]):
        assert prev < curr, (
            "v1.15.53: not-in-plex row TD order doesn't match main row"
        )


# ── 3. Server-side sort key ─────────────────────────────────


def test_sort_key_edition_registered():
    """The 'edition' sort key must be registered in
    _LIBRARY_SORTS_MAIN so the SQL ORDER BY can resolve it.
    Without registration, sort=edition silently falls back to
    title (the .get() default)."""
    src = API_PY.read_text()
    sorts_anchor = src.index("_LIBRARY_SORTS_MAIN = {")
    sorts_end = src.index("}\n", sorts_anchor)
    sorts_block = src[sorts_anchor:sorts_end]
    assert '"edition":' in sorts_block, (
        "v1.15.53: _LIBRARY_SORTS_MAIN must include 'edition' key — "
        "without it sort=edition falls back to title"
    )
    # The expression must extract from pi.folder_path.
    edition_anchor = sorts_block.index('"edition":')
    edition_end = sorts_block.index("),", edition_anchor)
    edition_expr = sorts_block[edition_anchor:edition_end]
    assert "pi.folder_path" in edition_expr, (
        "v1.15.53: edition sort must read from pi.folder_path "
        "(where the {edition-...} tag lives)"
    )
    assert "'{edition-'" in edition_expr, (
        "v1.15.53: edition sort must look for the literal "
        "'{edition-' tag prefix"
    )


def test_api_library_sort_regex_accepts_edition():
    """The FastAPI Query() pattern validator must include 'edition'
    in the allowed sort values, otherwise FastAPI 422s the
    request before it ever reaches the SQL layer."""
    src = API_PY.read_text()
    pattern_line = "pattern=\"^(title|year|tdb|src|dl|pl|link|imdb|attention|edition)$\""
    assert pattern_line in src, (
        "v1.15.53: /api/library sort regex must include 'edition' "
        "in the allowed values — pre-fix sort=edition returned 422"
    )


# ── 4. CSS for new column + tightened TDB col ───────────────


def test_col_edition_css_defined():
    """A .col-edition rule must exist with a sensible width so
    the new column renders consistently. Without it the column
    auto-sizes to content + jitter on every render."""
    css = APP_CSS.read_text()
    assert ".col-edition {" in css, (
        "v1.15.53: .col-edition CSS rule required for consistent "
        "column width"
    )


def test_col_tdb_width_tightened_after_section_label_drop():
    """With the section label gone, col-tdb only carries the pill.
    Width must be tightened from 180px → 90px to reclaim the
    horizontal space (the whole reorder was partly motivated by
    layout breathing room)."""
    css = APP_CSS.read_text()
    # Find the .col-tdb rule (not .col-tdb in a comment).
    anchor = css.index(".col-tdb {")
    block = css[anchor:anchor + 200]
    assert "width: 90px" in block, (
        "v1.15.53: .col-tdb width must be tightened to 90px now "
        "that the [Movies]/[Anime] section label is gone"
    )
