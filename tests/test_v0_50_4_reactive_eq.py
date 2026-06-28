"""v0.50.4 — reactive brand-mark EQ (design-flavor pass 2/4).

the user: the v0.50.2 synth bars "feel way too fast" at rest; the current rate
should be the SYNCING speed and idle should be very slow.

So the brand-mark EQ now has two tempos:
  - idle: slow ambient drift (2.4-4.4s per band) — the default;
  - syncing: the lively 0.7-1.5s rate + brighter amber, applied via
    `.brand-mark.is-active`, which app.js toggles off the existing
    `anyMutatingOpActive` topbar signal (so it tracks real op activity).

The keyframes (brand-eq-1..5) and infinite iteration are unchanged — only the
tempo/color differ — so the v1.15.134 reduced-motion clamp still rests every
band at full height in both states.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def test_idle_drift_is_slow():
    """The base (idle) tempo was slowed from the v0.50.2 lively rate."""
    i = APP_CSS.index(".brand-mark .brand-bar {")
    block = APP_CSS[i:APP_CSS.index("}", i)]
    # band 1 idle is now multiple seconds, not sub-second.
    assert "animation: brand-eq-1 2.8s" in block


def test_is_active_is_the_lively_syncing_state():
    assert ".brand-mark.is-active .brand-bar { background: var(--amber-bright); }" in APP_CSS
    # the lively rate (the old default) returns as the syncing override.
    assert ".brand-mark.is-active .brand-bar:nth-child(1) { animation-duration: 0.85s; }" in APP_CSS
    assert ".brand-mark.is-active .brand-bar:nth-child(3) { animation-duration: 0.7s; }" in APP_CSS


def test_js_toggles_is_active_off_activity_signal():
    """app.js drives .is-active from the existing anyMutatingOpActive signal."""
    assert "classList.toggle('is-active', anyMutatingOpActive)" in APP_JS
    # guarded so the chrome-less login page (no .brand-mark) is a no-op.
    assert "document.querySelector('.brand-mark')" in APP_JS
