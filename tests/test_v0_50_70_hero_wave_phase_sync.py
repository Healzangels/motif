"""v0.50.70 — hero wave phase-synced to the wall clock (no reset-on-navigation).

the user: dashboard / logs / settings "reset the hero wave while the others don't,
so it feels like it flickers when changing sections." Every nav is a full-page
`<a href>` load, so the CSS `hero-wave` animation restarts at phase 0 each time;
the flicker is that reset. Fix: base.html sets a NEGATIVE animation-delay derived
from Date.now() BEFORE first paint, so each wave layer resumes exactly where a
continuously-running one would be — identical phase across every page, no jump.
Both layers (9s + 14s) sync independently so their cross-weave stays continuous.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
BASE = (REPO / "app" / "web" / "templates" / "base.html").read_text()


def _rule(sel: str) -> str:
    i = APP_CSS.index(sel)
    return APP_CSS[i:APP_CSS.index("}", i) + 1]


def test_base_sets_wall_clock_delay_vars_pre_paint():
    """Both delay vars are set from Date.now() on documentElement. The block must
    live in <head> BEFORE the deferred app.js so it runs before the hero paints
    (no phase-0 flash)."""
    assert "r.setProperty('--hero-wave-delay', '-' + (n % 9000) + 'ms')" in BASE
    assert "r.setProperty('--hero-wave-delay-2', '-' + (n % 14000) + 'ms')" in BASE
    assert "var n = Date.now()" in BASE
    # ordering: the sync script precedes the deferred app.js load.
    assert BASE.index("--hero-wave-delay") < BASE.index('src="/static/app.js')


def test_both_wave_layers_consume_the_synced_delay():
    after = _rule(".hero::after {")
    before = _rule(".hero::before {")
    # each layer reads its OWN var (periods differ: 9s vs 14s), with a 0s fallback
    # so a browser without the var (or JS disabled) still animates from phase 0.
    assert "animation-delay: var(--hero-wave-delay, 0s)" in after
    assert "animation-delay: var(--hero-wave-delay-2, 0s)" in before
    # the delay must come AFTER the animation shorthand (which resets delay to 0).
    assert after.index("animation:") < after.index("animation-delay:")
    assert before.index("animation:") < before.index("animation-delay:")


def test_delay_period_matches_each_layers_duration():
    """The modulo must match the layer's animation-duration or the phase won't be
    continuous: ::after is 9s → n % 9000; ::before is 14s → n % 14000."""
    after = _rule(".hero::after {")
    before = _rule(".hero::before {")
    assert "animation: hero-wave 9s linear infinite" in after
    assert "(n % 9000)" in BASE
    assert "animation: hero-wave-2 14s linear infinite" in before
    assert "(n % 14000)" in BASE
