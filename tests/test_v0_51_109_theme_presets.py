"""v0.51.109 — theme presets Tag 2: the // THEME picker + Fallout/Plex/Dracula/Nord.

A preset overrides the CANVAS (--bg/--fg/--line families) + the v0.51.108 --accent
family, applied pre-paint from localStorage 'motif:theme'. window.MOTIF_THEMES
(base.html) is the single source of truth; the // THEME picker in the customize
panel reads it. Fallout = no override (the :root/green defaults).

The load-bearing guard: a preset must NEVER touch a SEMANTIC token — the SRC/LINK
pill + chip colors (src-t/ok/amber/violet/blue/magenta/red/green-pale/…) stay
fixed in every theme, per the user's "don't change the pill or chip colors".
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
BASE = (REPO / "app" / "web" / "templates" / "base.html").read_text()
CUST = (REPO / "app" / "web" / "static" / "dashboard-customize.js").read_text()
APP = (REPO / "app" / "web" / "static" / "app.css").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
SETTINGS = (REPO / "app" / "web" / "templates" / "settings.html").read_text()

# the MOTIF_THEMES object literal in base.html
THEMES_BLK = BASE[BASE.index("window.MOTIF_THEMES = {"):
                  BASE.index("};", BASE.index("window.MOTIF_THEMES = {")) + 2]

# every preset bundle in window.MOTIF_THEMES (Fallout = no entry = the defaults).
PRESETS = ("plex", "dracula", "nord", "gruvbox", "tokyonight", "synthwave", "mono")
# every token a preset is ALLOWED to set — canvas + accent only.
CANVAS_ACCENT = [
    "--bg", "--bg-elev", "--bg-elev-2", "--bg-rgb", "--line", "--line-bright",
    "--line-rgb", "--fg", "--fg-dim", "--fg-mute", "--accent", "--accent-bright",
    "--accent-deep", "--accent-rgb",
]
# tokens a preset must NEVER set (semantic pill/chip identity + health).
FORBIDDEN = [
    "--src-t", "--ok", "--amber", "--violet", "--blue", "--magenta", "--red",
    "--orange", "--lemon", "--cyan", "--green-pale", "--green:", "--green-bright",
    "--dash-movies", "--dash-tv", "--dash-anime", "--dash-collections",
]


def _preset(name: str) -> str:
    i = THEMES_BLK.index(name + ": {")
    return THEMES_BLK[i:THEMES_BLK.index("}", i)]


# ── the preset bundles ───────────────────────────────────────


def test_all_presets_defined():
    for name in PRESETS:
        assert name + ": {" in THEMES_BLK, f"{name} preset missing"


def test_each_preset_sets_the_full_canvas_and_accent():
    for name in PRESETS:
        blk = _preset(name)
        for tok in CANVAS_ACCENT:
            assert f"'{tok}':" in blk, f"{name} is missing {tok}"


def test_presets_never_touch_a_semantic_token():
    # the whole point: pills/chips keep their meaning across themes.
    for bad in FORBIDDEN:
        assert bad not in THEMES_BLK, (
            f"a preset sets {bad!r} — that would retint a pill/chip; presets "
            "may only override the canvas + --accent family")


def test_dracula_and_nord_use_their_signature_hexes():
    assert "'--bg': '#282a36'" in _preset("dracula")
    assert "'--accent': '#bd93f9'" in _preset("dracula")
    assert "'--bg': '#2e3440'" in _preset("nord")
    assert "'--accent': '#88c0d0'" in _preset("nord")
    assert "'--accent': '#e5a00d'" in _preset("plex")  # Plex gold


def test_plex_canvas_is_neutral_charcoal_not_warm_brown():
    # v0.51.110: the user's real Plex is a NEUTRAL near-black + gold, not the
    # warm brown of v0.51.109. bg channels stay near-equal (neutral), not R>>B.
    blk = _preset("plex")
    assert "'--bg': '#1d1d1f'" in blk
    assert "'--bg': '#16140f'" not in blk  # the old warm-brown value is gone


def test_tag3_presets_signature_hexes_and_label():
    # v0.51.112: the four added presets carry their recognisable signatures.
    assert "'--bg': '#282828'" in _preset("gruvbox")        # Gruvbox dark0
    assert "'--accent': '#fe8019'" in _preset("gruvbox")    # Gruvbox orange
    assert "'--bg': '#1a1b26'" in _preset("tokyonight")     # Tokyo Night bg
    assert "'--accent': '#7aa2f7'" in _preset("tokyonight")  # Tokyo blue
    assert "'--accent': '#ff5fd2'" in _preset("synthwave")  # neon pink
    assert "'--bg': '#0e0e0e'" in _preset("mono")           # near-pure black
    assert "'--accent': '#d0d0d0'" in _preset("mono")       # grayscale
    # tokyonight gets a two-word display label in the picker.
    assert "tokyonight: 'TOKYO NIGHT'" in APP_JS


# ── pre-paint (base.html) ────────────────────────────────────


def test_prepaint_applies_saved_theme():
    # reads the key, applies the bundle onto documentElement before app.css.
    assert "localStorage.getItem('motif:theme')" in BASE
    i = BASE.index("window.MOTIF_THEMES[localStorage.getItem('motif:theme')]")
    tail = BASE[i:i + 400]
    assert "setProperty(k, b[k])" in tail
    # runs BEFORE the deferred app.css/app.js (the head scripts are inline).
    assert BASE.index("window.MOTIF_THEMES") < BASE.index('src="/static/app.js')


# ── picker lives in Settings → VISUALS (v0.51.110) ───────────


def test_theme_picker_in_settings_visuals():
    # the <select> shell lives in the VISUALS settings panel.
    assert 'id="theme-select"' in SETTINGS
    vis = SETTINGS[SETTINGS.index('data-panel="visuals"'):]
    vis = vis[:vis.index("</section>")]
    assert 'id="theme-select"' in vis, "// THEME select must be in the VISUALS panel"
    # app.js populates + wires it from the shared bundle.
    assert "function bindThemePicker()" in APP_JS
    assert "bindThemePicker();" in APP_JS  # called on boot
    assert "window.MOTIF_THEMES" in APP_JS
    assert "localStorage.getItem('motif:theme')" in APP_JS
    assert "removeItem('motif:theme')" in APP_JS  # fallout clears it


def test_app_js_reads_shared_bundle_no_duplicate_hexes():
    # single source of truth — the picker reads window.MOTIF_THEMES; the preset
    # hexes live ONLY in base.html, not duplicated in app.js.
    blk = APP_JS[APP_JS.index("function bindThemePicker()"):]
    blk = blk[:blk.index("\n  }\n") + 4]
    assert "#282a36" not in blk and "#bd93f9" not in blk and "#e5a00d" not in blk


def test_theme_picker_removed_from_dashboard_customize():
    # relocated to Settings — the dashboard customize panel no longer owns it.
    assert "motif:theme" not in CUST
    assert "dash-theme-select" not in CUST
    assert "applyTheme" not in CUST


def test_theme_select_has_styling():
    assert ".theme-select {" in APP
