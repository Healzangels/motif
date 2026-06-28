"""v0.50.33 — the applied saved-filter row stays highlighted as "selected".

The user: "when a filter is selected it should remain highlighted to indicate it's
selected." v0.50.31 had removed the persistent applied-row paint (the cyan-text
version was indistinguishable from :hover, so it read as a stray hover on open).
v0.50.33 restores it but makes the SELECTED state visually distinct from hover —
a static background tint + a left accent bar — so the genuinely-applied preset
reads as selected, not a hover. JS still toggles .is-active off the live filter
state, so only the truly-applied preset lights up.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def _rule(selector: str) -> str:
    i = APP_CSS.index(selector + " {")
    return APP_CSS[i:APP_CSS.index("}", i)]


def test_selected_row_has_distinct_persistent_highlight():
    block = _rule(".library-presets-popup-list .library-presets-popup-apply.is-active")
    # cyan text + a distinct selected treatment (bg tint + left accent bar) — NOT
    # just the cyan text that hover already uses.
    assert "color: var(--cyan)" in block
    assert "background: rgba(var(--cyan-rgb), 0.10)" in block
    assert "box-shadow: inset 2px 0 0 var(--cyan)" in block


def test_hover_stays_text_only_so_it_differs_from_selected():
    block = _rule(".library-presets-popup-list .library-presets-popup-apply:hover")
    # hover is the text colour only (v0.50.22 — no bg flash); the bg/accent belongs
    # to .is-active alone, so selected and hover stay visually distinct.
    assert "color: var(--cyan)" in block
    assert "background:" not in block


def test_js_toggles_is_active_off_live_match():
    # The highlight is driven by _buildPresetQueryString() matching, so only the
    # genuinely-applied preset gets .is-active (never on hover, never a phantom).
    assert "classList.toggle('is-active'" in APP_JS
    assert "f.query_json === here" in APP_JS
