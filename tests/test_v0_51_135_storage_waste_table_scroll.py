"""v0.51.135 — CSS-audit T3: dashboard // STORAGE WASTE table swipes on mobile.

The dashboard's STORAGE WASTE table (5 cols: TITLE YEAR DESTINATION SIZE ACT)
had no scroll wrapper. Its DESTINATION cell is a long container path (the copies
renderer in app.js), so the table's min-content sat ~486px and pushed the
per-row // RELINK button off-screen right under body{overflow-x:hidden} below
~500px — the whole page also spilled horizontally.

Fix: wrap the table in the shared `.table-scroll` (overflow-x:auto ≤1080px — the
#library-table swipe pattern). Harness-proven at a 375px layout viewport: page
horizontal overflow gone (documentElement scrollWidth 375) and // RELINK
reachable-after-swipe true. The // RE-LINK ALL bulk button lives in the block
header (outside the table) so it stays visible regardless.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DASH = (REPO / "app" / "web" / "templates" / "dashboard.html").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def test_storage_waste_table_wrapped_in_table_scroll():
    # locate the STORAGE WASTE block, then its table must sit inside .table-scroll.
    i = DASH.index("// STORAGE WASTE")
    block = DASH[i:i + 1200]
    tbl = block.index('<tbody id="copies-body">')
    head = block[:tbl]
    assert 'class="table-scroll"' in head, (
        "the STORAGE WASTE table needs the shared .table-scroll swipe context "
        "so its long DESTINATION path doesn't clip // RELINK on mobile"
    )


def test_relink_all_button_stays_in_header_not_the_scroll_box():
    # the bulk // RE-LINK ALL button must remain in the block-head (always
    # visible), NOT inside the swipe box — the per-row // RELINK is the swipeable
    # one, but the bulk action must never be gated behind a horizontal scroll.
    i = DASH.index("// STORAGE WASTE")
    header = DASH[i:i + 400]
    assert 'id="relink-all-btn"' in header
    assert 'class="table-scroll"' not in header  # header precedes the wrapper


def test_table_scroll_swipe_context_still_defined():
    assert ".table-scroll { overflow-x: auto; -webkit-overflow-scrolling: touch; }" in APP_CSS
