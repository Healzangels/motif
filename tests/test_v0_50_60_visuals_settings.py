"""v0.50.60 — Settings → VISUALS: per-browser CRT-effect toggles.

localStorage 'motif:visuals' (live-apply, no SAVE — like the dashboard LIBRARY
COLORS panel). base.html stamps html.viz-no-<effect> pre-paint; CSS hides each
effect + collapses the hero when the wave is off; the CRT power on/off JS gates
honour the opt-out. Pins the wiring across all the surfaces so it can't silently
drift.
"""
from __future__ import annotations
import re
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
    assert "html.viz-no-hero-wave .hero { min-height: 130px; }" in CSS


def test_visuals_deeplink_ssr_rules_present():
    # /settings#visuals must un-hide the panel + light the tab pre-paint
    assert 'html[data-settings-tab="visuals"] .tab-panel[data-panel="visuals"]' in CSS
    assert 'html[data-settings-tab="visuals"] #settings-tabs .tab[data-tab="visuals"]' in CSS


def test_js_binds_visuals_toggles():
    assert "function bindVisualsToggles()" in JS
    assert "bindVisualsToggles();" in JS
    assert "motif:visuals" in JS


def _viz_map(text):
    # field -> viz-no-* class, as written in a `key: 'viz-no-...'` object literal
    return dict(re.findall(r"(\w+):\s*'(viz-no-[a-z-]+)'", text))


def test_viz_field_class_maps_agree_across_all_sites():
    """v0.50.63 (code-review): the field->class map is hand-duplicated in base.html
    (pre-paint), app.js (bindVisualsToggles CLS), and settings.html (data-viz-field).
    Pin that they AGREE, not just that each exists — a rename in one copy would
    otherwise leave a class stamped that nothing toggles back (phantom-guard, v1.18.81)."""
    base_map = _viz_map(BASE)   # base.html pre-paint `m`
    js_map = _viz_map(JS)   # app.js bindVisualsToggles `CLS`
    assert len(base_map) == 5, base_map
    assert base_map == js_map, f"base.html {base_map} != app.js {js_map}"
    fields = set(re.findall(r'data-viz-field="(\w+)"', SETTINGS))
    assert fields == set(base_map.keys()), f"settings data-viz-field {fields} != map keys"
