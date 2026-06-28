"""v1.24.65 — customizable per-library accent colors.

the user: make the PLEX dashboard card colors (movies/tv/anime/collections)
customizable via the customize-dashboard button, with a reset-to-defaults
button; and colour each active nav-tab underline to match its library's color
(anime pink, collections red, …).

Architecture: 4 CSS vars (--dash-{movies,tv,anime,collections}-color, default
= the tokens) drive BOTH the PLEX cards' left bar AND the active nav-tab
underline. A base.html head script applies localStorage 'motif:dashColors'
overrides pre-paint (the nav is on every page). The customize-mode // LIBRARY
COLORS panel edits + persists them; // RESET COLORS clears the overrides.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
BASE_HTML = (REPO / "app" / "web" / "templates" / "base.html").read_text()
CUSTOMIZE_JS = (REPO / "app" / "web" / "static" / "dashboard-customize.js").read_text()

VARS = {
    "movies": "--dash-movies-color",
    "tv": "--dash-tv-color",
    "anime": "--dash-anime-color",
    "collections": "--dash-collections-color",
}


# ── CSS: vars default to tokens, cards + nav reference them ───────────────────


def test_root_defines_dash_color_vars_defaulting_to_tokens():
    root = APP_CSS[APP_CSS.index(":root {"):APP_CSS.index("--dash-collections-color") + 60]
    assert "--dash-movies-color: var(--amber)" in root
    assert "--dash-tv-color: var(--blue)" in root
    assert "--dash-anime-color: var(--magenta)" in root
    assert "--dash-collections-color: var(--red)" in root


def test_plex_cards_use_the_vars():
    assert ".stat-plex-primary { border-left-color: var(--dash-movies-color)" in APP_CSS
    assert ".stat-plex-tv     { border-left-color: var(--dash-tv-color)" in APP_CSS
    assert ".stat-plex-anime  { border-left-color: var(--dash-anime-color)" in APP_CSS
    assert ".stat-plex-collections { border-left-color: var(--dash-collections-color)" in APP_CSS


def test_active_nav_underline_per_tab():
    for nav, var in VARS.items():
        assert (f'.nav a[data-nav="{nav}"].active' in APP_CSS
                and f"border-bottom-color: var({var})" in APP_CSS), nav


def test_color_panel_css_present():
    assert ".dash-color-panel {" in APP_CSS
    assert '.dash-color-item input[type="color"]' in APP_CSS
    assert ".dash-color-reset" in APP_CSS


# ── base.html head: pre-paint apply with hex validation ──────────────────────


def test_head_script_applies_saved_colors():
    assert "motif:dashColors" in BASE_HTML
    for var in VARS.values():
        assert var in BASE_HTML
    # values are hex-validated before being applied.
    assert "/^#[0-9a-fA-F]{6}$/.test" in BASE_HTML


# ── customize JS: config, panel, edit, reset, wiring ─────────────────────────


def test_dash_colors_config_defaults_match_tokens():
    # the picker defaults / reset targets must equal the :root token hexes.
    for key, hexv in (("movies", "#ffb84a"), ("tv", "#6d8fff"),
                      ("anime", "#ff7ad6"), ("collections", "#ff6b6b")):
        assert f"key: '{key}'" in CUSTOMIZE_JS
        assert f"def: '{hexv}'" in CUSTOMIZE_JS


def test_panel_inject_remove_and_handlers():
    for fn in ("function injectColorPanel()", "function removeColorPanel()",
               "function onColorInput(ev)", "function onColorReset()"):
        assert fn in CUSTOMIZE_JS
    assert "dash-color-panel" in CUSTOMIZE_JS
    assert "// LIBRARY COLORS" in CUSTOMIZE_JS
    assert "// RESET COLORS" in CUSTOMIZE_JS


def test_reset_clears_override_and_storage():
    body = CUSTOMIZE_JS[CUSTOMIZE_JS.index("function onColorReset()"):]
    body = body[:body.index("\n  }") + 4]
    assert "removeProperty(c.cssVar)" in body  # fall back to :root default
    assert "removeItem(DASH_COLORS_KEY)" in body
    assert "inp.value = c.def" in body


def test_input_validates_hex_and_persists():
    body = CUSTOMIZE_JS[CUSTOMIZE_JS.index("function onColorInput(ev)"):]
    body = body[:body.index("\n  }") + 4]
    assert "isHex(inp.value)" in body
    assert "setProperty(cfg.cssVar, inp.value)" in body
    assert "saveDashColors(saved)" in body


def test_panel_wired_into_customize_lifecycle():
    enter = CUSTOMIZE_JS[CUSTOMIZE_JS.index("function enterCustomize()"):]
    enter = enter[:enter.index("\n  function ")]
    assert "injectColorPanel()" in enter
    exit_ = CUSTOMIZE_JS[CUSTOMIZE_JS.index("function exitCustomize()"):]
    exit_ = exit_[:exit_.index("\n  function ")]
    assert "removeColorPanel()" in exit_
    # colors are also (re)applied on init.
    init = CUSTOMIZE_JS[CUSTOMIZE_JS.index("async function init()"):]
    init = init[:init.index("\n  function ", 1)] if "\n  function " in init[1:] else init[:400]
    assert "applyDashColors()" in init


def test_hex_validation_helper_present():
    assert "function isHex(v)" in CUSTOMIZE_JS
    assert "/^#[0-9a-fA-F]{6}$/.test" in CUSTOMIZE_JS
