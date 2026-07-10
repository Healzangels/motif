"""v0.50.2 — topbar brand-mark becomes a 5-band synth spectrum analyzer.

the user: "can we make the bars look more like a synth and make them move in a
more synth like manner."

The v1.24.95 brand-mark was 3 bars all animated by ONE keyframe (brand-eq),
phase-shifted — a uniform VU-meter sweep. v0.50.2 widens it to FIVE bars, each
driven by its OWN irregular multi-peak keyframe (brand-eq-1..5) at its own
duration, so the bands never sync up — the independent bounce of a graphic-EQ /
spectrum analyzer.

Invariants preserved from v1.24.95 (test_v1_24_95 + test_v1_15_134):
  - amber (brand accent), infinite animation (reduced-motion-clamp-covered),
  - no animation-fill-mode:forwards, so reduced-motion rests at full-height bars.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
BASE = (REPO / "app" / "web" / "templates" / "base.html").read_text()


def test_brand_mark_has_five_bars():
    assert BASE.count('class="brand-bar"') == 5


def test_five_independent_keyframes_exist():
    """Each band has its own keyframe (the source of the non-uniform motion)."""
    for n in range(1, 6):
        assert f"@keyframes brand-eq-{n}" in APP_CSS, f"missing brand-eq-{n}"
    # the old single uniform keyframe is gone (replaced, not kept alongside).
    assert "@keyframes brand-eq " not in APP_CSS
    assert "@keyframes brand-eq{" not in APP_CSS


def test_bands_run_at_different_durations():
    """Distinct per-band durations are what stop the bars syncing up — the
    'synth' feel. At least three different duration values across the bands."""
    durations = set()
    for n in range(1, 6):
        anchor = f"animation: brand-eq-{n} "
        i = APP_CSS.index(anchor) + len(anchor)
        durations.add(APP_CSS[i:APP_CSS.index(" ", i)])
    assert len(durations) >= 3, f"bands too uniform: {durations}"


def test_brand_bars_follow_accent_and_infinite():
    """Brand accent + reduced-motion coverage preserved."""
    i = APP_CSS.index(".brand-mark .brand-bar {")
    block = APP_CSS[i:APP_CSS.index("}", i)]
    # v0.51.113: the brand bars follow the THEME accent (was --amber).
    assert "var(--accent)" in block
    assert "infinite" in block
    # full-height base (no fill-mode:forwards) → reduced-motion rests full bars.
    assert "forwards" not in block
