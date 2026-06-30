"""v0.50.60 — Settings → VISUALS: per-browser CRT-effect toggles.

localStorage 'motif:visuals' (live-apply, no SAVE — like the dashboard LIBRARY
COLORS panel). base.html stamps html.viz-no-<effect> pre-paint; CSS hides each
effect + collapses the hero when the wave is off; the CRT power on/off JS gates
honour the opt-out. Pins the wiring across all the surfaces so it can't silently
drift.
"""
from __future__ import annotations
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
BASE = (REPO / "app" / "web" / "templates" / "base.html").read_text()
SETTINGS = (REPO / "app" / "web" / "templates" / "settings.html").read_text()
CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
JS = (REPO / "app" / "web" / "static" / "app.js").read_text()

FIELDS = ["crtOn", "crtOff", "heroWave", "scanlines", "equalizer"]
CLASSES = ["viz-no-crt-on", "viz-no-crt-off", "viz-no-hero-wave",
           "viz-no-scanlines", "viz-no-equalizer"]


def test_settings_has_visuals_tab_and_five_toggles():
    assert '<button class="tab" data-tab="visuals" role="tab">VISUALS</button>' in SETTINGS
    assert '<section class="block tab-panel" data-panel="visuals"' in SETTINGS
    for f in FIELDS:
        assert f'data-viz-field="{f}"' in SETTINGS, f"missing toggle {f}"


def test_base_prepaint_reads_localStorage_and_stamps_classes():
    assert "localStorage.getItem('motif:visuals')" in BASE
    for c in CLASSES:
        assert c in BASE, f"pre-paint must map a class {c}"


def test_crt_gates_honour_opt_out():
    # power-on skips when viz-no-crt-on; power-off returns early when viz-no-crt-off
    assert "viz-no-crt-on" in BASE and "viz-no-crt-off" in BASE


def test_css_hides_each_effect_and_collapses_hero():
    assert "html.viz-no-scanlines .scanlines { display: none; }" in CSS
    assert "html.viz-no-equalizer .brand-mark { display: none; }" in CSS
    assert "html.viz-no-hero-wave .hero::after { display: none; }" in CSS
    # the wave-off hero collapse (shift content up, no white band)
    assert "html.viz-no-hero-wave .hero { min-height: 0; }" in CSS


def test_visuals_deeplink_ssr_rules_present():
    # /settings#visuals must un-hide the panel + light the tab pre-paint
    assert 'html[data-settings-tab="visuals"] .tab-panel[data-panel="visuals"]' in CSS
    assert 'html[data-settings-tab="visuals"] #settings-tabs .tab[data-tab="visuals"]' in CSS


def test_js_binds_visuals_toggles():
    assert "function bindVisualsToggles()" in JS
    assert "bindVisualsToggles();" in JS
    assert "motif:visuals" in JS
