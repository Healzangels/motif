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


def test_brand_mark_reactivity_retired_v0_50_67():
    """v0.50.67: the brand EQ's activity reactivity was RETIRED — it stays a calm
    ambient drift; the activity signal now drives the hero wave instead (the user
    — "make that effect impact the hero bar"). The old `.brand-mark.is-active`
    tempo/colour override rule no longer exists (test_v0_50_67 owns the hero side)."""
    assert ".brand-mark.is-active .brand-bar {" not in APP_CSS


def test_js_drives_activity_signal_into_the_hero_not_the_brand_mark():
    """v0.50.67: app.js toggles motif-busy on <html> to drive the hero wave (CSS);
    it no longer toggles .is-active on the brand-mark. v0.50.68: the toggle keys off
    heroBusy (anyMutatingOpActive unioned with the click-time optimistic signal)."""
    # v0.50.73: motif-busy is driven through _rampWaveLevel (hero wave), not a
    # direct toggle; the brand-mark still gets no .is-active.
    assert "_rampWaveLevel(heroBusy ? _busyLevel : 0);" in APP_JS
    assert "_brandMark.classList.toggle('is-active'" not in APP_JS
