"""v1.22.96 — even vertical distribution of the filter drawer columns.

the user on the v1.22.95 drawer: "the right column looks a bit
squished since it has 4 items vs the other columns 3 anyway to make
this look a bit more even". Both columns stretch to the same grid
height, but rows stacked from the top — the 3-row theme-axis column
left a void under ATTN while the 4-row file-axis column read dense.

Fix: .pill-filter-col distributes rows with justify-content:
space-between (first/last rows align across columns; the 3-row
column breathes wider) + the legacy 1px single-column stack margin
is zeroed inside the drawer so it can't offset the alignment.
Degrades cleanly where heights match (collections' 3v3, the <900px
single-column mode).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def test_columns_distribute_rows_evenly():
    i = APP_CSS.index(".pill-filter-col {")
    block = APP_CSS[i:APP_CSS.index("}", i)]
    assert "justify-content: space-between;" in block
    assert "gap: var(--gap-1);" in block


def test_drawer_rows_drop_legacy_stack_margin():
    i = APP_CSS.index(".pill-filter-drawer .pill-filter-row {")
    block = APP_CSS[i:APP_CSS.index("}", i)]
    assert "margin-top: 0;" in block
