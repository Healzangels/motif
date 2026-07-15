"""v0.51.155 — display / UI scaling.

The user (2560×1440 monitor): motif renders physically small at 100% browser zoom;
manually setting Chrome to 125% "feels normal" AND de-pixelates the posters. motif's
whole size scale is px (--t-*, --gap-*), so it doesn't scale with the display; and the
poster sharpness is an image-rasterization effect that ONLY browser-zoom-style scaling
fixes (a rem refactor would scale text but not images).

Fix: a root `zoom` (behaves like browser zoom — scales text + layout + re-rasterizes
poster bitmaps). An AUTO ladder (app.css media queries) bumps it up on large native
panels (2560px+) and stays 1× on laptops / 1080p / OS-scaled displays. A MANUAL
override in Settings → VISUALS (Auto / 100 / 110 / 125 / 150 %) sets an inline
html.zoom that wins over the ladder, persisted to localStorage 'motif:uiScale' and
applied pre-paint. Verified in a browser harness that `zoom` keeps sticky/fixed/vw
correct and doesn't overflow.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
BASE_HTML = (REPO / "app" / "web" / "templates" / "base.html").read_text()
SETTINGS_HTML = (REPO / "app" / "web" / "templates" / "settings.html").read_text()


def test_css_auto_zoom_ladder():
    # --ui-zoom default lives in the :root token block; the html rule applies it;
    # the @media overrides bump it on large panels. (Folded into the existing
    # :root/html rules — no duplicate top-level selectors, per test_v1_15_116.)
    assert "--ui-zoom: 1;" in APP_CSS
    assert "zoom: var(--ui-zoom);" in APP_CSS
    assert "@media (min-width: 2400px) { :root { --ui-zoom: 1.25; } }" in APP_CSS
    assert "@media (min-width: 3600px) { :root { --ui-zoom: 1.5; } }" in APP_CSS


def test_base_html_prepaint_scale_apply():
    # a shared apply fn (mirrors MOTIF_APPLY_THEME) + a pre-paint invocation.
    assert "window.MOTIF_APPLY_UI_SCALE = function" in BASE_HTML
    assert "r.style.zoom = String(pct / 100);" in BASE_HTML
    assert "r.style.removeProperty('zoom');" in BASE_HTML  # 'auto' → ladder
    assert "localStorage.getItem('motif:uiScale')" in BASE_HTML


def test_settings_ui_scale_picker():
    assert 'id="ui-scale-select"' in SETTINGS_HTML
    for opt in ('value="auto"', 'value="100"', 'value="110"',
                'value="125"', 'value="150"'):
        assert opt in SETTINGS_HTML, f"missing UI SCALE option {opt}"
    assert "UI SCALE" in SETTINGS_HTML


def test_appjs_binds_ui_scale():
    assert "function bindUiScale()" in APP_JS
    assert "bindUiScale();" in APP_JS  # invoked at init
    assert "localStorage.setItem(KEY, v)" in APP_JS
    assert "window.MOTIF_APPLY_UI_SCALE(v)" in APP_JS
    # keyed to the same localStorage slot as the pre-paint.
    assert "'motif:uiScale'" in APP_JS
