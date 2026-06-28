"""v1.24.68 — SOURCE BREAKDOWN as 3 separate cards + reference-style selectors.

the user: "I would like them broken up into their own little block instead of one
large bar and have the same sort of selectors as seen here" (the reference's
TRAILER COVERAGE: 3 donut cards, each with a "Local Trailer  4271 (29.0%)"
legend). So each donut is its own .source-pie-col card (donut beside its legend),
the outer section is borderless (just a label + the cards), and each legend row
is dot · letter · name … count (pct%).
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def _rule(sel):
    i = APP_CSS.index(sel)
    return APP_CSS[i:APP_CSS.index("}", i)]


# ── each donut is its own card ───────────────────────────────────────────────


def test_source_pie_col_is_a_card_with_donut_beside_legend():
    col = _rule(".source-pie-col {")
    assert "border: 1px solid var(--line)" in col
    assert "background: var(--bg-elev)" in col
    assert "border-radius" in col
    # grid: label spans the top, donut beside legend below.
    assert '"chart legend"' in col


def test_outer_section_is_borderless():
    block = _rule("#source-breakdown-block {")
    assert "border: 0" in block
    assert "background: transparent" in block
    head = _rule("#source-breakdown-block > .block-head {")
    assert "border-bottom: 0" in head


def test_cards_wrap_responsively():
    row = _rule(".source-pie-row {")
    assert "repeat(auto-fit, minmax(320px, 1fr))" in row


# ── reference-style selectors (1 per row, count + (pct%)) ────────────────────


def test_legend_is_one_item_per_row():
    leg = _rule(".source-breakdown-legend {")
    assert "flex-direction: column" in leg  # was a 2-col grid


def test_legend_vals_group_pushed_right():
    vals = _rule(".source-legend-vals {")
    assert "margin-left: auto" in vals


def test_js_renders_count_and_bracketed_pct():
    idx = APP_JS.index("function _renderSourcePie(rows, opts)")
    body = APP_JS[idx:APP_JS.index("\n  function ", idx + 1)]
    assert "source-legend-vals" in body
    assert "'(' + pct + '%)'" in body          # bracketed percentage
    assert "(n / visibleTotal) * 100).toFixed(1)" in body  # 1-decimal like the ref
