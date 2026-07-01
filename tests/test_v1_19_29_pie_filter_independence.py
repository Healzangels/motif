"""v1.19.29 → v1.24.59 — source-donut filter independence.

v1.19.29 made the (then two) source donuts filter-independent. v1.24.55 briefly
SHARED one hidden-set across the 3-up row (Total / Movies / TV). v1.24.59 reverts
to independent toggles (the user: "make the toggle independent of one another") —
each donut owns its own hidden-set + localStorage key, and clicking a legend
letter hides it on ONLY that donut. Movies/TV are seeded once from the legacy
shared key so the deploy doesn't visually jump, then they diverge.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def test_three_independent_hidden_sets():
    # v1.24.59: one Set per donut; the shared _mainPieHidden is gone.
    assert "let _totalPieHidden" in APP_JS
    assert "let _moviesPieHidden" in APP_JS
    assert "let _tvPieHidden" in APP_JS
    assert "_mainPieHidden" not in APP_JS, (
        "v1.24.59: the shared Set was replaced by three independent ones")
    assert "_collPieHidden" not in APP_JS


def test_each_donut_has_its_own_hide_key():
    assert "_SOURCE_PIE_HIDE_KEY = 'motif:dash:src-hide'" in APP_JS  # Total (legacy)
    assert "_MOVIES_PIE_HIDE_KEY = 'motif:dash:src-hide-movies'" in APP_JS
    assert "_TV_PIE_HIDE_KEY = 'motif:dash:src-hide-tv'" in APP_JS
    assert "_COLL_PIE_HIDE_KEY" not in APP_JS


def test_movies_tv_seed_from_legacy_key_once():
    # First run clones the legacy shared state so a previously-hidden wedge
    # doesn't re-appear on deploy; then the keys diverge.
    assert "_seedIndependentSet(_MOVIES_PIE_HIDE_KEY, _SOURCE_PIE_HIDE_KEY)" in APP_JS
    assert "_seedIndependentSet(_TV_PIE_HIDE_KEY, _SOURCE_PIE_HIDE_KEY)" in APP_JS


def test_each_donut_spec_routes_its_own_set():
    # v0.50.74: the 4 donuts (Total/Movies/TV/Anime) live in _SOURCE_DONUTS, each
    # carrying its OWN hiddenSet; _renderOneDonut forwards d.hiddenSet. Independence
    # preserved, just table-driven instead of 3 hand-written wrappers.
    for hidden in ("_totalPieHidden", "_moviesPieHidden", "_tvPieHidden", "_animePieHidden"):
        assert f"hiddenSet: {hidden}" in APP_JS, f"a donut must route {hidden}"
    assert "hiddenSet: d.hiddenSet," in APP_JS


def test_render_helper_reads_hiddenset_from_opts():
    fn_idx = APP_JS.index("function _renderSourcePie(rows, opts)")
    fn_end = APP_JS.index("\n  function ", fn_idx + 1)
    fn = APP_JS[fn_idx:fn_end]
    assert "hiddenSet" in fn and "hiddenSet.has" in fn


def test_legend_click_toggles_only_the_clicked_donut():
    # v1.24.59: discriminate by the enclosing .source-pie-col id, toggle that
    # donut's Set, re-render ONLY it (not renderAllSourcePies).
    handler_idx = APP_JS.index(
        "document.addEventListener('click', (ev) => {\n"
        "    const btn = ev.target.closest('.source-legend-item');")
    handler = APP_JS[handler_idx:APP_JS.index("\n  });", handler_idx) + 5]
    assert "closest('.source-pie-col')" in handler
    # v0.50.74: discriminate the clicked donut by its col id → its _SOURCE_DONUTS
    # spec, then re-render ONLY it via _renderOneDonut.
    assert "source-pie-(\\w+)-col" in handler
    assert "_SOURCE_DONUTS.find(" in handler
    assert "_renderOneDonut(d," in handler
    # the whole-row re-render is gone — only the clicked donut re-renders.
    assert "renderAllSourcePies(" not in handler
