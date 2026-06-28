"""v1.24.78 — follow-up audit fixes for the dashboard customize system.

After v1.24.77, a read-only audit of the customize/reorder machinery found two
more issues of the same families:

1. The GRID analogue of the v1.24.77 dash-pair bug. The stat-card sections
   (top-stats, plex-coverage, operations, activity, storage) carry
   data-dash-section on the .grid element and keep display:grid while editing, so
   the injected .dash-section-controls bar landed in the first grid CELL and
   shoved the cards into a ragged row (the user's PLEX LIBRARY control box wedged
   to the left of the cards). Fix: span the bar across all columns.

2. saveLayout() nulled SAVE_TIMER but never cleared it, so exitCustomize's
   immediate flush during an armed 300ms debounce fired a redundant duplicate
   PUT. Fix: clearTimeout at the top of saveLayout.

The sibling-walk audit + persistence round-trip audit found no other live bugs.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
JS = (REPO / "app" / "web" / "static" / "dashboard-customize.js").read_text()


def test_grid_section_controls_span_all_columns():
    anchor = "body.dash-customize-mode #dash-sections > .grid > .dash-section-controls"
    assert anchor in CSS
    block = CSS[CSS.index(anchor):CSS.index(anchor) + 130]
    assert "grid-column: 1 / -1" in block


def test_save_layout_clears_pending_debounce():
    fn = JS[JS.index("async function saveLayout()"):JS.index("async function saveLayout()") + 300]
    assert "if (SAVE_TIMER) clearTimeout(SAVE_TIMER);" in fn
