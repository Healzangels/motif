"""v0.50.79 — reorder the SOURCE BREAKDOWN pie charts (◀ ▶, like PLEX LIBRARY cards).

the user: "lets make it so you can reorder the pie charts as seen in the plex library
items." Each donut card gets ◀ ▶ buttons that swap it with the adjacent VISIBLE chart;
the order is persisted per-browser (motif:dash:src-charts-order) and applied by physically
reordering the .source-pie-col elements so DOM order == visual order (keeping the
is-last-visible / data-visible arena logic correct).
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
DASH = (REPO / "app" / "web" / "templates" / "dashboard.html").read_text()


# ── Template + CSS ──

def test_every_card_has_left_right_move_buttons():
    for did in ("total", "movies", "tv", "anime"):
        assert f'class="source-pie-move" data-donut="{did}" data-dir="-1"' in DASH
        assert f'class="source-pie-move" data-donut="{did}" data-dir="1"' in DASH
    # ◀ ▶ glyphs present, grouped with // HIDE in the control cluster.
    assert "◀" in DASH and "▶" in DASH
    assert DASH.count('class="source-pie-col-ctrls"') == 4


def test_move_and_ctrls_styled():
    assert ".source-pie-col-ctrls {" in CSS
    assert ".source-pie-move {" in CSS
    assert ".source-pie-move:hover" in CSS


# ── JS wiring ──

def test_order_state_persisted_and_applied():
    assert "_CHART_ORDER_KEY = 'motif:dash:src-charts-order'" in JS
    assert "let _chartOrder = _loadChartOrder();" in JS
    assert "function _applyChartOrder() {" in JS
    # applied once at load so a reordered layout paints correctly pre-first-render.
    assert "if (document.querySelector('#source-breakdown-block .source-pie-row')) _applyChartOrder();" in JS


def test_render_iterates_display_order():
    idx = JS.index("function renderAllSourcePies(")
    body = JS[idx:JS.index("\n  function ", idx + 1)]
    assert "for (const d of _orderedDonuts())" in body


def test_move_button_click_is_wired():
    idx = JS.index("const moveBtn = ev.target.closest('.source-pie-move');")
    body = JS[idx:idx + 400]
    assert "const dir = parseInt(moveBtn.dataset.dir, 10);" in body
    assert "if (id && (dir === 1 || dir === -1)) _moveChart(id, dir);" in body


# ── Behavioral: _reorderIds swap logic ──

def _fn_src(start_marker, end_marker) -> str:
    start = JS.index(start_marker)
    end = JS.index(end_marker, start)
    return JS[start:end]


def _run(js_body, preamble=""):
    quickjs = pytest.importorskip("quickjs")
    ctx = quickjs.Context()
    return json.loads(ctx.eval(preamble + "\n" + js_body))


REORDER = None  # lazy


def _reorder_src():
    return _fn_src("  function _reorderIds(order, visibleIds, id, dir) {",
                   "  function _moveChart(id, dir) {")


def test_move_right_swaps_with_next_visible():
    out = _run(
        "var order = ['total','movies','tv','anime'];\n"
        "JSON.stringify(_reorderIds(order, order.slice(), 'total', 1));",
        _reorder_src(),
    )
    assert out == ["movies", "total", "tv", "anime"]


def test_move_left_at_edge_is_a_noop():
    out = _run(
        "var order = ['total','movies','tv','anime'];\n"
        "JSON.stringify(_reorderIds(order, order.slice(), 'total', -1));",
        _reorder_src(),
    )
    assert out == ["total", "movies", "tv", "anime"]


def test_move_skips_a_hidden_chart():
    # movies hidden → total's right neighbor is tv; they swap, movies keeps its slot.
    out = _run(
        "var order = ['total','movies','tv','anime'];\n"
        "var visible = ['total','tv','anime'];\n"
        "JSON.stringify(_reorderIds(order, visible, 'total', 1));",
        _reorder_src(),
    )
    assert out == ["tv", "movies", "total", "anime"]


# ── Behavioral: _loadChartOrder sanitization ──

def _load_src():
    return _fn_src("  function _loadChartOrder() {", "  function _persistChartOrder() {")


def _run_load(stored):
    preamble = (
        "var _CHART_ORDER_KEY = 'k';\n"
        "var _CHART_IDS = ['total','movies','tv','anime'];\n"
        f"var _stored = {json.dumps(stored)};\n"
        "var localStorage = { getItem: function(k){ return _stored; } };\n"
        + _load_src()
    )
    return _run("JSON.stringify(_loadChartOrder());", preamble)


def test_load_honors_saved_order_and_appends_missing():
    out = _run_load(json.dumps(["anime", "tv"]))
    assert out == ["anime", "tv", "total", "movies"]


def test_load_drops_unknown_and_dedups():
    out = _run_load(json.dumps(["foo", "tv", "tv", "total"]))
    assert out == ["tv", "total", "movies", "anime"]


def test_load_defaults_when_empty_or_corrupt():
    assert _run_load(None) == ["total", "movies", "tv", "anime"]
    assert _run_load("not json{") == ["total", "movies", "tv", "anime"]
