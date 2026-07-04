"""v0.51.45 — SYNC PERFORMANCE + DOWNLOAD ACTIVITY as a 2-up pair.

the user: "can we make // DOWNLOAD ACTIVITY and // SYNC PERFORMANCE instead of
full width of the dashboard but half and next to one another." Both are short
insight charts that waste horizontal space full-width, so they now sit side by
side in a .dash-pair flex row — mirroring the v1.24.66 STATISTICS pair. The
wrapper is the single customize unit (reuses the insight-syncs layout key); each
inner .block keeps its id for the JS chart render + its own SSR display gate.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DASH_HTML = (REPO / "app" / "web" / "templates" / "dashboard.html").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def _insight_pair() -> str:
    key = DASH_HTML.index('data-dash-section="insight-syncs"')
    open_idx = DASH_HTML.rindex('<div class="dash-pair"', 0, key + 40)
    close_idx = DASH_HTML.index("/.dash-pair (SYNC PERFORMANCE", open_idx)
    return DASH_HTML[open_idx:close_idx]


def test_insight_pair_wrapper_is_the_customize_unit():
    pair = _insight_pair()
    head = pair[:180]
    assert 'data-dash-section="insight-syncs"' in head   # keeps a layout key
    assert 'data-dash-label="SYNC &amp; DOWNLOADS"' in head


def test_both_charts_are_pair_columns_inside_the_wrapper():
    pair = _insight_pair()
    assert 'class="block dash-pair-col insight-row" id="insight-syncs-block"' in pair
    assert 'class="block dash-pair-col insight-row" id="insight-downloads-block"' in pair
    # the chart mount points the JS renders into stay (by id).
    assert 'id="insight-syncs-body"' in pair
    assert 'id="insight-downloads-body"' in pair
    # each inner block keeps its SSR display gate (visible-then-empty flash guard).
    assert "_ssr_dash.has_insight_syncs" in pair
    assert "_ssr_dash.has_insight_downloads" in pair


def test_inner_blocks_no_longer_own_a_data_dash_section():
    # the wrapper owns the customize key now — the old per-card keys are gone.
    assert DASH_HTML.count('data-dash-section="insight-downloads"') == 0
    assert DASH_HTML.count('data-dash-section="insight-syncs"') == 1
    # and they're no longer standalone full-width insight-row .blocks.
    assert 'class="block insight-row" id="insight-syncs-block"' not in DASH_HTML
    assert 'class="block insight-row" id="insight-downloads-block"' not in DASH_HTML


def test_dash_pair_css_lays_the_pair_side_by_side():
    # reuses the existing .dash-pair / .dash-pair-col layout (no new CSS needed).
    pair = APP_CSS[APP_CSS.index(".dash-pair {"):APP_CSS.index(".dash-pair {") + 200]
    assert "display: flex" in pair and "flex-wrap: wrap" in pair
    col = APP_CSS[APP_CSS.index(".dash-pair-col {"):APP_CSS.index(".dash-pair-col {") + 200]
    assert "flex: 1 1 calc(50%" in col
    # customize mode keeps every #dash-sections .dash-pair a flex row (covers this
    # new pair too — generic selector).
    assert "body.dash-customize-mode #dash-sections > .dash-pair { display: flex !important; }" in APP_CSS
