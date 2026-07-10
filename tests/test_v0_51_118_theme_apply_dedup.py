"""v0.51.118 — one shared theme-apply path (no pre-paint / picker mirror-drift).

Pre-v0.51.118 two places applied a theme bundle with different loops: the
base.html pre-paint IIFE (for-in setProperty, no clear) and app.js
bindThemePicker (build an allTokens set → removeProperty each → setProperty the
chosen). Same job, two writers — a later change to how a theme applies could
land in only one, so a reload and an in-app switch could diverge (mirror-drift,
the codebase's #1 regression class).

v0.51.118 extracts ONE window.MOTIF_APPLY_THEME(name) in base.html (defined
before the pre-paint, so both the pre-paint IIFE and bindThemePicker call it):
clear every preset's tokens, then apply the chosen bundle (fallout / unknown =
clear only). The clear is a no-op on the fresh pre-paint load.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
BASE = (REPO / "app" / "web" / "templates" / "base.html").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


# ── source: one applier, both callers use it, no duplicated apply loop ──


def test_single_applier_defined_in_base():
    assert BASE.count("window.MOTIF_APPLY_THEME = function") == 1


def test_prepaint_and_picker_both_call_the_shared_applier():
    # pre-paint (base.html) applies the saved key through it.
    assert "window.MOTIF_APPLY_THEME(localStorage.getItem('motif:theme'))" in BASE
    # the picker (app.js) applies the selected value through it.
    assert "window.MOTIF_APPLY_THEME(v)" in APP_JS


def test_picker_has_no_duplicated_apply_loop():
    # bindThemePicker must delegate — no allTokens set, no inline
    # removeProperty/setProperty apply loop (that logic lives only in the
    # shared applier now).
    i = APP_JS.index("function bindThemePicker()")
    body = APP_JS[i:APP_JS.index("\n  function ", i + 1)]
    assert "window.MOTIF_APPLY_THEME(v)" in body
    assert "allTokens" not in body
    assert "removeProperty" not in body
    assert "setProperty" not in body


# ── behavioral: the shared applier clears-then-applies ──


def _apply_fn() -> str:
    start = BASE.index("window.MOTIF_APPLY_THEME = function")
    end = BASE.index("(function () {", start)
    return BASE[start:end]


def _run(body: str) -> dict:
    quickjs = pytest.importorskip("quickjs")
    preamble = (
        "var _set = {};\n"
        "var _removeCount = 0;\n"
        "var _r = { style: {\n"
        "  setProperty: function(k, v){ _set[k] = v; },\n"
        "  removeProperty: function(k){ _removeCount++; delete _set[k]; }\n"
        "} };\n"
        "var document = { documentElement: _r };\n"
        "var window = { MOTIF_THEMES: {\n"
        "  plex: { '--bg': '#1d1d1f', '--accent': '#e5a00d' },\n"
        "  dracula: { '--bg': '#282a36', '--accent': '#bd93f9' }\n"
        "} };\n"
        + _apply_fn()
    )
    ctx = quickjs.Context()
    return json.loads(ctx.eval(preamble + "\n" + body))


def test_applies_the_chosen_bundle():
    out = _run("window.MOTIF_APPLY_THEME('plex'); JSON.stringify(_set);")
    assert out == {"--bg": "#1d1d1f", "--accent": "#e5a00d"}


def test_switching_clears_the_prior_theme_tokens():
    # plex → dracula must leave ONLY dracula's tokens (no stale plex values).
    out = _run("window.MOTIF_APPLY_THEME('plex');\n"
               "window.MOTIF_APPLY_THEME('dracula');\n"
               "JSON.stringify(_set);")
    assert out == {"--bg": "#282a36", "--accent": "#bd93f9"}


def test_clear_is_deduped_across_presets():
    # v0.51.119: the two mock presets share the same 2 keys (--bg, --accent), so
    # ONE apply must call removeProperty exactly twice (deduped), not 4× (once per
    # preset per shared key). Guards the seen-set that made the clear spotless.
    out = _run("window.MOTIF_APPLY_THEME('plex'); JSON.stringify(_removeCount);")
    assert out == 2


def test_fallout_or_unknown_name_clears_to_defaults():
    # an unknown / fallout name clears every preset token and sets nothing —
    # the CSS :root defaults take over.
    out = _run("window.MOTIF_APPLY_THEME('plex');\n"
               "window.MOTIF_APPLY_THEME('fallout');\n"
               "JSON.stringify(_set);")
    assert out == {}
