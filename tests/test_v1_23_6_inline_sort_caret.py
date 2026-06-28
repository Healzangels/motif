"""v1.23.6 — sort carat inline, right of the header label.

the user: "our sort carrots I designed to be below the different
titles. I no longer want this and would like a more standard carrot
to the right of the text on the same row so we can remove the extra
white space below the headers ... uniform across the board."

The v1.12.32 stacked layout (block .th-label over a block 12px
.sort-indicator) reserved a caret line under EVERY header. Now the
.th-stack is an inline-flex row — "LABEL ▲" on one line — with a
fixed-width caret slot so centered headers don't shift sideways
when the caret appears/disappears on sort changes. All four library
tabs (movies/tv/anime/collections) share library.html, so one
change covers the board.
"""
from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
LIB_HTML = (REPO / "app" / "web" / "templates" / "library.html").read_text()
HTML_CODE = re.sub(r"\{#.*?#\}", "", LIB_HTML, flags=re.S)


def test_th_stack_is_inline_flex_row():
    i = APP_CSS.index(".table thead th .th-stack {")
    block = APP_CSS[i:APP_CSS.index("}", i)]
    assert "display: inline-flex;" in block
    assert "align-items: center;" in block


def test_sort_indicator_is_out_of_flow():
    # v1.23.13: the v1.23.6 in-flow 10px slot centered "LABEL ▲" as
    # a unit, pushing the label ~5px off the column axis (the user:
    # "the carrot is pushing it over to the left"). The caret is now
    # absolute off the label's right edge — the label centers alone
    # and still never shifts on sort changes.
    i = APP_CSS.index(".table th .sort-indicator {")
    block = APP_CSS[i:APP_CSS.index("}", i)]
    assert "position: absolute;" in block
    assert "left: calc(100% + var(--gap-1));" in block
    assert "width: 10px;" not in block, (
        "the in-flow slot is the off-center bug — the caret must "
        "not reserve layout width"
    )
    assert "height: 12px;" not in block, (
        "the stacked layout's reserved caret line is the whitespace "
        "the user asked to remove"
    )
    assert "margin: 2px 0 0 0;" not in block


def test_actions_header_lost_its_vestigial_indicator():
    i = HTML_CODE.index('class="col-actions"')
    th = HTML_CODE[i:HTML_CODE.index("</th>", i)]
    assert "sort-indicator" not in th, (
        "the ACTIONS indicator only height-matched the old stacked "
        "layout; inline it would ghost-offset the centered label"
    )
    # sortable headers still carry their indicator span.
    assert HTML_CODE.count('<span class="sort-indicator"></span>') >= 8


def test_th_label_wrapper_retired():
    # the .th-label span lost its CSS rule with the inline layout
    # (the v1.15.111 hygiene guard flagged it) — bare header text is
    # an anonymous flex item and the column-gap still applies.
    assert "th-label" not in HTML_CODE
    assert ".th-label" not in (REPO / "app" / "web" / "static"
                               / "app.css").read_text()
