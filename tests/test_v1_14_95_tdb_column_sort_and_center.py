"""v1.14.95 — TDB column sort accepts `tdb` + chip centered.

the user: "can we make it so the TDB column is centered over
the info below it and make it so the information is centered
as right now the different tdb tags misalign it"

Plus a 422 visible in the screenshots the user shared:

    422: {"detail":[{"type":"string_pattern_mismatch",
                     "loc":["query","sort"],
                     "msg":"String should match pattern
                            '^(title|year|src|dl|pl|link|imdb|attention)$'",
                     "input":"tdb", ...}]}

Clicking the TDB column header sends `?sort=tdb` to /api/library
and FastAPI 422s before the sort even reaches the SQL layer.

## Root cause

Mirror-principle leak between the route's regex pattern and the
SQL dispatch table. v1.13.67 added the dedicated TDB column
with a sort caret AND a SQL ORDER BY branch in
`_LIBRARY_SORTS_MAIN["tdb"]` (priority CASE: pending update →
tracked-ok → cookies → dead → dropped → no-TDB). But the
regex pattern at api.py was never widened — `tdb` got dropped
on the way in, the user saw a JSON validation error pasted into
the page where the result rows would have been.

Centering: `.col-tdb` was `text-align: left` + 180px wide. The
chip pill width varies across states (TDB / TDB ↑ / TDB ⚠ /
TDB ✗ / TDB ◌ / NO TDB) so left-aligning anchors the chip at
column-left while the chip+section tag pair reads as
unbalanced relative to the column header.

## Fix

1. Widen the regex pattern at api.py to include `tdb`.
2. Switch `.col-tdb` CSS from `text-align: left` to
   `text-align: center`. Both header and data inherit the
   alignment via the `.col-tdb` class on the `<th>` and `<td>`.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"
APP_CSS = REPO / "app" / "web" / "static" / "app.css"
LIBRARY_HTML = REPO / "app" / "web" / "templates" / "library.html"


# ── Bug 1: sort=tdb regex acceptance ───────────────────────────


def test_sort_regex_includes_tdb():
    """The /api/library sort Query pattern must include 'tdb' so
    a TDB-header click doesn't 422 before reaching the SQL."""
    src = API_PY.read_text()
    # Find the sort Query line (there's only one in the file).
    sort_line = next(
        line for line in src.splitlines()
        if "sort: str = Query" in line and "pattern=" in line
    )
    assert "|tdb|" in sort_line or "(tdb|" in sort_line or "|tdb)" in sort_line, (
        f"sort regex must accept 'tdb'; got: {sort_line}"
    )


def test_sort_regex_keeps_existing_values():
    """Adding 'tdb' must not have dropped any of the existing
    sort axes — the regex is the only enum gate."""
    src = API_PY.read_text()
    sort_line = next(
        line for line in src.splitlines()
        if "sort: str = Query" in line and "pattern=" in line
    )
    for axis in ("title", "year", "src", "dl", "pl", "link",
                 "imdb", "attention"):
        assert axis in sort_line, (
            f"sort regex must keep '{axis}'; got: {sort_line}"
        )


def test_sql_dispatch_table_already_has_tdb():
    """_LIBRARY_SORTS_MAIN['tdb'] has been there since v1.13.67 —
    pin it so the regex widening + SQL stay in sync.

    If a future cleanup removes the SQL branch, this test fails
    to flag that the regex would now silently fall through to
    the default sort on every TDB-header click."""
    src = API_PY.read_text()
    # Anchor on the dispatch table.
    anchor = src.index("_LIBRARY_SORTS_MAIN = {")
    block = src[anchor:anchor + 4000]
    assert '"tdb": (' in block, (
        "_LIBRARY_SORTS_MAIN must define a 'tdb' entry — the SQL "
        "ORDER BY snippet for TDB sort lives there"
    )


# ── Bug 2: TDB column centering ────────────────────────────────


def test_col_tdb_css_uses_center_alignment():
    """The .col-tdb CSS rule must use text-align:center so the
    chip+section-tag pair sits centered under the TDB header
    instead of left-anchored (which made the variable-width
    chips read as misaligned)."""
    src = APP_CSS.read_text()
    # Anchor on the .col-tdb rule.
    anchor = src.index(".col-tdb {")
    rule = src[anchor:anchor + 250]
    assert "text-align: center" in rule, (
        f".col-tdb must center-align; got rule: {rule!r}"
    )
    assert "text-align: left" not in rule, (
        "Pre-v1.14.95 left-align must be gone"
    )


def test_col_tdb_keeps_ellipsis_overflow():
    """text-align:center must not have broken the ellipsis
    behavior — the cell still needs to truncate cleanly if its
    contents ever overflow. overflow:hidden + white-space:nowrap
    drive the ellipsis; alignment is orthogonal.

    v1.15.53: width tightened 180px → 90px after the
    [Movies]/[Anime] section label was dropped from the cell.
    The ellipsis bones (nowrap + overflow:hidden + ellipsis)
    must still be present regardless of width."""
    src = APP_CSS.read_text()
    anchor = src.index(".col-tdb {")
    rule = src[anchor:anchor + 250]
    assert "white-space: nowrap" in rule
    assert "overflow: hidden" in rule
    assert "text-overflow: ellipsis" in rule
    assert "width: 90px" in rule


def test_col_tdb_th_carries_data_sort_tdb():
    """The TDB <th> in library.html carries data-sort="tdb" — pin
    it so a future cleanup doesn't drop the attribute (the
    handler reads dataset.sort to build the API URL)."""
    html = LIBRARY_HTML.read_text()
    assert 'data-sort="tdb"' in html, (
        "TDB <th> must carry data-sort='tdb' so clicks send "
        "?sort=tdb to /api/library"
    )


def test_v1_14_95_marker_explains_the_regex_widening():
    """A v1.14.95 marker on the sort Query line explains the
    mirror-principle leak so a future tightening refactor
    doesn't drop tdb again."""
    src = API_PY.read_text()
    # The marker should be on / near the sort Query line.
    sort_idx = src.index("sort: str = Query")
    block = src[max(0, sort_idx - 800):sort_idx + 200]
    assert "v1.14.95" in block


def test_v1_14_95_marker_explains_the_centering():
    """A v1.14.95 marker on the .col-tdb rule explains WHY the
    centering matters (variable chip widths).

    v1.15.53: widened the search window 600 → 1200 since the
    v1.15.53 width-change comment pushed v1.14.95's rationale
    comment further from the rule. v1.14.95 marker is still
    present; just lives a bit further up."""
    src = APP_CSS.read_text()
    anchor = src.index(".col-tdb {")
    block = src[max(0, anchor - 1200):anchor]
    assert "v1.14.95" in block
