"""v1.18.44 — bulk-bar overflow clip fix (CSS width constraint).

the user's screenshot on v1.18.43: 6 bulk buttons (SELECT ALL
FILTERED + CLEAR + DOWNLOAD & REPLACE FROM TDB (108) + PUSH TO
PLEX (2) + PROBE TDB SELECTED (108) + EXPORT CSV (111)) spilled
past the right border of the bulk-action bar. No // MORE ▾
overflow menu appeared. The v1.17.18 overflow logic should have
moved the rightmost buttons into the // MORE dropdown — but
didn't.

## Root cause

`#library-bulk-bar` had no explicit width constraint. With
`flex-wrap: nowrap` + `flex-shrink: 0` on children, the bar
grew horizontally to fit its content (children's natural sum).
That made `bar.scrollWidth === bar.clientWidth` — both equal
the natural content width — so `_layoutBulkBar`'s overflow
check (`scrollWidth > clientWidth`) never tripped and no
buttons got moved to the overflow panel.

The bar respected its parent's width visually (the green border
sat at the parent's right edge) but the buttons-with-flex-
shrink-0 rendered past the bar's box via `overflow: visible`.
The discrepancy: the bar's BOX was being grown to fit children,
not constrained by the parent.

## Fix

CSS: add `max-width: 100%` + `min-width: 0` to
`#library-bulk-bar`. `max-width` forces the bar to respect its
parent. `min-width: 0` defeats the default `min-width: auto`
on flex items so the bar can actually shrink below content
width when constrained. With both, scrollWidth > clientWidth
becomes true when the children overflow, and _layoutBulkBar
fires correctly.

JS: add a `requestAnimationFrame` follow-up call to
_layoutBulkBar after the synchronous one. Guards against rare
cases where the synchronous read measures stale layout state
(e.g., when multiple state writes batch into one paint).
"""

from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
CSS = REPO / "app" / "web" / "static" / "app.css"
JS = REPO / "app" / "web" / "static" / "app.js"


def test_bulk_bar_has_max_width_constraint():
    """#library-bulk-bar must declare max-width: 100% so it
    respects its parent's width instead of expanding to fit
    content. Without this the v1.17.18 overflow detection
    never trips on installs where the bar's parent doesn't
    aggressively constrain width."""
    css = CSS.read_text()
    block_start = css.index("#library-bulk-bar {")
    # Block ends at the first standalone closing brace.
    block = css[block_start:block_start + 2500]
    assert "max-width: 100%" in block, (
        "v1.18.44: #library-bulk-bar must have max-width: 100% "
        "so the bar's clientWidth doesn't grow to match its "
        "content (which makes scrollWidth === clientWidth and "
        "breaks the overflow check)"
    )


def test_bulk_bar_has_min_width_zero():
    """#library-bulk-bar must declare min-width: 0 to defeat
    the default min-width: auto on flex items. Without this,
    even with max-width set, a flex item won't shrink below
    its content's intrinsic min-width."""
    css = CSS.read_text()
    block_start = css.index("#library-bulk-bar {")
    block = css[block_start:block_start + 2500]
    assert "min-width: 0" in block, (
        "v1.18.44: #library-bulk-bar must have min-width: 0 "
        "so flex sizing can actually shrink the bar below "
        "content width"
    )


def test_layout_bulk_bar_schedules_raf_follow_up():
    """updateLibrarySelectionUi must schedule a second
    _layoutBulkBar call on the next animation frame. Guards
    against stale layout measurements when multiple text
    changes batch into one paint."""
    js = JS.read_text()
    # The synchronous call site is unchanged; the rAF wrapper
    # is added immediately after.
    fn_idx = js.index("_layoutBulkBar();")
    block = js[fn_idx:fn_idx + 1000]
    assert "requestAnimationFrame(_layoutBulkBar)" in block, (
        "v1.18.44: a rAF follow-up call must be scheduled "
        "after the synchronous _layoutBulkBar so layout-state "
        "races can self-correct on the next paint"
    )
