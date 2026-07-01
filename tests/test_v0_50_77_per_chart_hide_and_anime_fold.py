"""v0.50.77 — per-chart // HIDE button + anime-folds-into-TV when anime is hidden.

the user: "each of the pie charts should have its own hide button so you can hide
just one or two or three graphs which determines the dynamic size of the remaining
graphs. Also ... if the anime pie chart was hidden then the information would be
added into the TV chart ... but if anime is showing then each just shows its given
library."

- Each .source-pie-col carries a // HIDE button (data-donut) in its label row; the
  clicked chart drops out and the survivors resize (data-visible grid). The last
  visible chart can't be hidden. Hidden charts get a restore chip in the header.
- The TV chart's scope is dynamic: when the ANIME chart isn't shown (user-hidden or
  no anime library) anime folds INTO TV (scope show||is_anime) and the card relabels
  "// TV + ANIME"; when anime IS shown, TV excludes anime (show && !is_anime).
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
DASH = (REPO / "app" / "web" / "templates" / "dashboard.html").read_text()


# ── 1. Per-chart hide button (template + CSS) ──

def test_every_card_has_a_hide_button():
    for did in ("total", "movies", "tv", "anime"):
        assert f'class="source-pie-hide" data-donut="{did}"' in DASH
    # the name is its own span so JS can relabel TV without clobbering the button.
    assert DASH.count('class="source-pie-col-name"') == 4
    # header restore strip for bringing hidden charts back.
    assert 'id="source-pie-restore"' in DASH


def test_hide_button_and_restore_chip_styled():
    i = CSS.index(".source-pie-col-label {")
    label = CSS[i:CSS.index("}", i)]
    assert "justify-content: space-between" in label  # name left, HIDE right
    assert ".source-pie-hide {" in CSS
    assert ".source-pie-restore-chip {" in CSS


# ── 2. Per-chart hide state + wiring (JS) ──

def test_chart_hidden_set_persisted():
    assert "_CHART_HIDDEN_KEY = 'motif:dash:src-charts-hidden'" in JS
    assert "let _chartHidden = _loadHiddenSet(_CHART_HIDDEN_KEY);" in JS


def test_hide_click_guards_the_last_chart_and_persists():
    idx = JS.index("const hideBtn = ev.target.closest('.source-pie-hide');")
    body = JS[idx:idx + 900]
    assert "const showChip = ev.target.closest('.source-pie-restore-chip');" in body
    # never hide the final visible chart.
    assert "if (visibleNow.length <= 1) return;" in body
    assert "_chartHidden.add(id);" in body
    assert "_chartHidden.delete(id);" in body
    assert "_persistHiddenSet(_CHART_HIDDEN_KEY, _chartHidden);" in body


def test_render_honors_user_hide():
    idx = JS.index("function renderAllSourcePies(")
    body = JS[idx:JS.index("\n  function ", idx + 1)]
    assert "const show = _donutHasData(d, rows) && !_chartHidden.has(d.id);" in body
    # v0.50.78: the fold is silent — no "+ ANIME" relabel on any card.
    assert "'// TV + ANIME'" not in JS
    # safety: a fully-hidden arena keeps TOTAL.
    assert "_chartHidden.delete('total');" in body
    assert "_renderChartRestore(rows);" in body


# ── 3. Anime folds back into its NATIVE chart (behavioral) ──

def _fold_src() -> str:
    start = JS.index("  function _animeShown(rows) {")
    end = JS.index("  function _donutHasData(d, rows) {")
    return JS[start:end]


def _run(js_body):
    quickjs = pytest.importorskip("quickjs")
    ctx = quickjs.Context()
    # MOVIES/TV carry the static `&& !is_anime` scope _scopeFor reuses when anime shows.
    harness = (
        "var _chartHidden = new Set();\n"
        + _fold_src() + "\n"
        + "var MOVIES = { id: 'movies', scopeFn: function(r){"
        " return r.media_type === 'movie' && !r.is_anime; } };\n"
        "var TV = { id: 'tv', scopeFn: function(r){"
        " return r.media_type === 'show' && !r.is_anime; } };\n"
        "function sumScope(d, rows){ return rows.filter(_scopeFor(d, rows))"
        ".reduce(function(a,r){ return a + (r.count||0); }, 0); }\n"
        + js_body + "\n"
    )
    return json.loads(ctx.eval(harness))


ROWS = (
    "var rows = ["
    "{media_type:'show', is_anime:0, count:10},"   # plain TV show
    "{media_type:'show', is_anime:1, count:5},"     # anime show
    "{media_type:'movie', is_anime:1, count:2},"    # anime movie
    "{media_type:'movie', is_anime:0, count:3}];\n"  # plain movie
)


def test_each_chart_is_its_own_library_when_anime_shown():
    # anime not hidden + anime rows exist → anime is its own chart → Movies/TV exclude it.
    out = _run(ROWS + "JSON.stringify([_animeShown(rows), sumScope(MOVIES, rows), sumScope(TV, rows)]);")
    assert out == [True, 3, 10]


def test_anime_folds_back_by_type_when_hidden():
    # hide the anime chart → anime MOVIES rejoin Movies, anime SHOWS rejoin TV.
    out = _run(ROWS + "_chartHidden.add('anime');"
               "JSON.stringify([_animeShown(rows), sumScope(MOVIES, rows), sumScope(TV, rows)]);")
    assert out == [False, 3 + 2, 10 + 5]


def test_anime_not_shown_when_no_anime_library():
    # no is_anime rows → _animeShown false → each type just holds its own (nothing to fold).
    out = _run(
        "var rows = [{media_type:'show', is_anime:0, count:7},"
        "{media_type:'movie', is_anime:0, count:4}];\n"
        "JSON.stringify([_animeShown(rows), sumScope(MOVIES, rows), sumScope(TV, rows)]);"
    )
    assert out == [False, 4, 7]
