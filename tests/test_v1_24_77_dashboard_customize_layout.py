"""v1.24.77 — dashboard customize-mode layout fixes + GENERAL STATISTICS fill.

the user (reorder-mode screenshot):
  1. the // LIBRARY COLORS panel detaches from the PLEX cards when sections are
     reordered (it's a standalone sibling that doesn't move with the section);
  2. the STATISTICS .dash-pair renders as "a single large row and a small row"
     in customize mode (the injected control bar is a flex sibling of the two
     columns);
  3. (separately) the GENERAL STATISTICS table leaves bottom whitespace in the
     equal-height pair — its rows should grow to fill.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
JS = (REPO / "app" / "web" / "static" / "dashboard-customize.js").read_text()


# ── #2: control bar gets its own row in the customize-mode dash-pair ─────────


def test_customize_dash_pair_controls_full_width():
    anchor = "body.dash-customize-mode #dash-sections > .dash-pair > .dash-section-controls"
    assert anchor in CSS
    block = CSS[CSS.index(anchor):CSS.index(anchor) + 140]
    assert "flex: 0 0 100%" in block


# ── #3: columns grow their table to fill the equal-height card ──────────────


def test_dash_pair_col_grows_table_to_fill():
    block = CSS[CSS.index(".dash-pair-col {"):CSS.index(".dash-pair-col {") + 650]
    assert "flex-direction: column" in block
    assert "min-height: 0" in block
    assert ".dash-pair-col > .table { flex: 1 1 auto; }" in CSS


# ── #1: color panel re-pins to the plex section on every reorder ────────────


def test_color_panel_repositions_to_plex_on_reorder():
    assert "function repositionColorPanel()" in JS
    fn = JS[JS.index("function repositionColorPanel()"):][:500]
    # v0.51.121: the PLEX (colored) cards moved into the top-stats section, so
    # the // LIBRARY COLORS panel re-pins onto top-stats now (not plex-coverage).
    assert '[data-dash-section="top-stats"]' in fn
    assert "insertBefore(panel, plex)" in fn
    # invoked from the post-reorder chokepoint (rebuildLayoutFromDOM).
    assert "repositionColorPanel();  // v1.24.77: keep colors pinned" in JS
    assert "function rebuildLayoutFromDOM()" in JS


def test_section_arrow_move_skips_the_color_panel():
    # the panel pinned before plex must not dead-button the section ▲/▼ arrows:
    # onArrowMove walks past non-[data-dash-section] siblings (code-review catch).
    fn = JS[JS.index("function onArrowMove("):JS.index("function onArrowMove(") + 1000]
    assert "adjacentSection" in fn
    assert "while (n && !n.matches('[data-dash-section]'))" in fn
    # the old single-step guard that skipped the move is gone.
    assert "prev && prev.matches('[data-dash-section]')" not in JS
