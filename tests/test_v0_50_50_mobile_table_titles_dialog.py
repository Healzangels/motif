"""v0.50.50 — mobile fixes: visible titles, pager wrap, full-screen dialogs.

At 480px the user hit:
  1. Row TITLES were blank on every row (and on any window < ~1440px). Cause: the
     results table is table-layout:fixed and .col-title is the only column with no
     declared width — when the fixed columns summed past the available width, the
     title column was squeezed to 0. Fix: a table min-width so it keeps its full
     width (scrolling in .table-scroll) and col-title always has room; desktop is
     unchanged (width:100% wins at >=1440px so col-title still absorbs the slack).
  2. The // RESULTS header's pager spilled the next/last buttons off the edge → the
     header + pager now flex-wrap on a phone.
  3. The row INFO card (and the other .dlg modals) now go full-screen on a phone.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def test_table_min_width_keeps_the_title_column_from_collapsing():
    # v0.50.53/54: the min-width lives in a max-width media block, not the base rule
    # (as a base rule it clipped ACTIONS on narrow desktop windows).
    assert "#library-table { table-layout: fixed; }" in CSS
    block = re.search(r"@media \(max-width: 1080px\) \{.*?\n\}", CSS, re.S).group(0)
    assert "#library-table { min-width: 1080px; }" in block
    # the base rule must NOT carry a min-width (the regression)
    base = CSS[CSS.index("#library-table { table-layout: fixed; }"):]
    assert "min-width" not in base.split("\n")[0]


def _phone_block() -> str:
    m = re.search(r"@media \(max-width: 600px\) \{.*?\n\}", CSS, re.S)
    assert m, "no phone breakpoint"
    return m.group(0)


def test_results_header_and_pager_wrap_on_phone():
    block = _phone_block()
    assert ".block-head { flex-wrap: wrap;" in block
    # v0.51.38: the action group + pager still wrap on a phone, and now also
    # centre (width:100% + justify-content:center) so the // RESULTS controls
    # read as a centred stack — see test_v0_51_38_pager_centering.
    assert ".block-head-actions { flex-wrap: wrap; width: 100%; justify-content: center; }" in block
    assert ".pager { flex-wrap: wrap; justify-content: center; }" in block


def test_dialogs_go_fullscreen_on_phone():
    block = _phone_block()
    # find the .dlg override inside the phone block
    i = block.index(".dlg {")
    rule = block[i:block.index("}", i) + 1]
    assert "width: 100vw; max-width: none;" in rule
    assert "height: 100dvh;" in rule
    assert "max-height: none;" in rule
    assert ".dlg-body { padding: var(--gap-4); }" in block
