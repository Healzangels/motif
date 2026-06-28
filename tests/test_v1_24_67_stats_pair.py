"""v1.24.67 — PER-SECTION COVERAGE + GENERAL STATISTICS as a 2-up pair.

the user: "make // PER-SECTION COVERAGE and // GENERAL STATISTICS a double wide
instead of 1 row each." Both are compact tables that waste horizontal space at
full width, so they now sit side by side in a .dash-pair flex row. The wrapper
is the single customize unit (keeps the section-coverage layout key so an
existing saved layout positions the pair); each inner .block keeps its id for JS
reveal and flexes to fill the row if its sibling is hidden.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DASH_HTML = (REPO / "app" / "web" / "templates" / "dashboard.html").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def test_pair_wrapper_is_the_customize_unit():
    idx = DASH_HTML.index('<div class="dash-pair"')
    head = DASH_HTML[idx:idx + 130]
    assert 'data-dash-section="section-coverage"' in head  # keeps the layout key
    assert 'data-dash-label="STATISTICS"' in head


def test_both_tables_are_pair_columns_inside_the_wrapper():
    open_idx = DASH_HTML.index('<div class="dash-pair"')
    close_idx = DASH_HTML.index("/.dash-pair", open_idx)
    block = DASH_HTML[open_idx:close_idx]
    # both inner sections live inside the wrapper, as .dash-pair-col .block.
    assert 'class="block dash-pair-col" id="section-coverage-block"' in block
    assert 'class="block dash-pair-col" id="general-stats-block"' in block
    # both tables present.
    assert 'id="section-coverage-table"' in block
    assert 'id="general-stats-table"' in block


def test_only_one_general_stats_block_remains():
    # the original standalone GENERAL STATISTICS section was removed (no dup id).
    assert DASH_HTML.count('id="general-stats-block"') == 1
    # the inner sections no longer carry their own data-dash-section (the wrapper
    # owns it) — only one section-coverage data-dash-section in the doc.
    assert DASH_HTML.count('data-dash-section="general-stats"') == 0
    assert DASH_HTML.count('data-dash-section="section-coverage"') == 1


def test_css_lays_out_the_pair_side_by_side():
    pair = APP_CSS[APP_CSS.index(".dash-pair {"):APP_CSS.index(".dash-pair {") + 200]
    assert "display: flex" in pair
    assert "flex-wrap: wrap" in pair
    col = APP_CSS[APP_CSS.index(".dash-pair-col {"):APP_CSS.index(".dash-pair-col {") + 200]
    assert "flex: 1 1 calc(50%" in col  # two columns; grows to fill if sibling hidden


def test_customize_mode_keeps_pair_flex():
    assert "body.dash-customize-mode #dash-sections > .dash-pair { display: flex !important; }" in APP_CSS
    # the override must come AFTER the generic display:block reveal to win.
    generic = APP_CSS.index("body.dash-customize-mode #dash-sections > [data-dash-section] {")
    pair = APP_CSS.index("body.dash-customize-mode #dash-sections > .dash-pair {")
    assert pair > generic
