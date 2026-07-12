"""v0.50.48 — mobile foundation pass.

The UI is desktop-first with only 4 narrow, single-purpose @media blocks and no
phone tier, so a ~390px viewport overflowed: the fixed-width results table was
clipped (ACTIONS unreachable), the 64px hero title spilled, and several min-widths
(refresh button, search, dash-pair cols) exceeded the viewport. This pins the
foundation: a horizontal-scroll wrapper on the table, a clamped hero title, and one
consolidated phone breakpoint.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
LIB = (REPO / "app" / "web" / "templates" / "library.html").read_text()


def test_results_table_has_horizontal_scroll_wrapper():
    # the wrapper opens before the table and the table is inside it
    i_wrap = LIB.index('<div class="table-scroll">')
    i_table = LIB.index('<table class="table" id="library-table">')
    assert i_wrap < i_table
    # the wrapper closes after the table
    i_close_table = LIB.index("</table>", i_table)
    i_close_wrap = LIB.index("</div>", i_close_table)
    assert i_close_table < i_close_wrap
    # and the CSS gives it its own scroll context — v0.50.58: SCOPED to the
    # <=1080 breakpoint (where the table actually overflows), not a base rule
    # (a base rule clipped the desktop row dropdowns).
    assert ".table-scroll { overflow-x: auto;" in CSS
    i_media = CSS.index("@media (max-width: 1080px)")
    assert CSS.index(".table-scroll { overflow-x: auto;") > i_media, (
        "v0.50.58: .table-scroll overflow must live in the <=1080 block, not as a "
        "base rule (base rule = dual-axis clip box that clipped row menus on desktop)"
    )


def test_hero_title_clamps_so_it_scales_on_narrow_viewports():
    m = re.search(r"\.title\s*\{[^}]*\}", CSS)
    assert m and "font-size: clamp(34px, 12vw, var(--t-huge));" in m.group(0)
    # the bare un-clamped --t-huge font-size is gone from .title
    assert "font-size: var(--t-huge);" not in m.group(0)


def test_phone_breakpoint_exists_with_the_key_overrides():
    m = re.search(r"@media \(max-width: 600px\) \{.*?\n\}", CSS, re.S)
    assert m, "no consolidated phone breakpoint"
    block = m.group(0)
    # nav scrolls as a single compact row on a phone (v0.50.49: + flex-wrap:nowrap
    # to override the base flex-wrap; v0.51.3: + grid-area:nav — the nav is now its
    # own full-width second row, see test_v0_51_3_mobile_nav_row)
    assert ".nav { grid-area: nav; flex-wrap: nowrap; overflow-x: auto;" in block
    # the 260px refresh button + 240px search no longer reserve their min-width
    assert "#library-refresh-btn { min-width: 0;" in block
    assert ".library-search-wrap { min-width: 0;" in block
    # v0.51.136: the .dash-pair-col min-width:0 floor-drop + stack+swipe MOVED out
    # of this ≤600 block into @media (max-width: 1200px) — the tables spilled their
    # cards through ~1130px, not just on phones. Guarded now by test_v0_51_136 +
    # test_v0_51_24 (which read the ≤1200 block).
