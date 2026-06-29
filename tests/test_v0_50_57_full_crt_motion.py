"""v0.50.57 — full CRT motion: the prefers-reduced-motion gating was removed.

The user runs motif on Windows, which commonly reports prefers-reduced-motion:
reduce. The v1.15.134 universal CSS clamp then froze the brand-mark equalizer (and
every other looping animation) to a static frame — "the wave on the top not doing
anything." The user chose to drop the accessibility behavior ENTIRELY (full CRT
vibe for everyone), so all three reduced-motion gates were removed:
  - the universal @media (prefers-reduced-motion: reduce) clamp in app.css
  - the CRT power-off flourish skip in base.html
  - the dashboard count-up skip in dashboard.html

This guards the decision so a future "respect reduced-motion" patch doesn't
silently re-freeze the equalizer on the user's machine.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
BASE = (REPO / "app" / "web" / "templates" / "base.html").read_text()
DASH = (REPO / "app" / "web" / "templates" / "dashboard.html").read_text()


def test_no_universal_reduced_motion_clamp_in_css():
    # the @media block that clamped every animation to a single frame is gone.
    assert "@media (prefers-reduced-motion: reduce)" not in APP_CSS


def test_power_off_flourish_not_reduced_motion_gated():
    assert "prefers-reduced-motion" not in BASE


def test_dashboard_countup_not_reduced_motion_gated():
    assert "prefers-reduced-motion" not in DASH


def test_brand_equalizer_still_animates_infinitely():
    # the motion the user wants kept — the 5-band equalizer loops forever.
    assert "animation: brand-eq-1 2.8s ease-in-out infinite;" in APP_CSS
