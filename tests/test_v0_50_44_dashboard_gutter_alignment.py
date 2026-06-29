"""v0.50.44 — dashboard column gutters align across every row.

The dashboard stacks rows of cards inside #dash-sections: the 4-card PLEX LIBRARY
row + the 2-card ADDED row are .grid.grid-stats (gutter = --gap-4, from .grid),
while PER-SECTION COVERAGE | GENERAL STATISTICS is a .dash-pair (flex). The pair
used --gap-6, so its 2-column split sat 4px off the column boundaries of the rows
it stacks between — the dashboard "centering looks off" (the user). Both surfaces
must share the grid gutter so every row's center boundary lands at the same x.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def _rule(selector: str) -> str:
    m = re.search(re.escape(selector) + r"\s*\{", CSS)
    assert m, f"{selector} rule not found"
    return CSS[m.start():CSS.index("}", m.start()) + 1]


def test_grid_default_gutter_is_gap_4():
    # the baseline every grid-stats row inherits
    assert "gap: var(--gap-4)" in _rule(".grid")


def test_dash_pair_gutter_matches_the_grid_gutter():
    rule = _rule(".dash-pair")
    assert "gap: var(--gap-4)" in rule, (
        "dash-pair gutter must match the grid rows (--gap-4) so the 2-column "
        "split aligns with the rows stacked above/below it")
    assert "gap: var(--gap-6)" not in rule, (
        "the --gap-6 gutter is the v0.50.44 misalignment — don't reintroduce it")


def test_dash_pair_col_basis_subtracts_the_same_gutter():
    rule = _rule(".dash-pair-col")
    assert "calc(50% - (var(--gap-4) / 2))" in rule
    assert "var(--gap-6)" not in rule
