"""v0.50.70 / v0.50.76 — hero wave phase-synced to the wall clock (no reset on nav).

the user: dashboard / logs / settings "reset the hero wave while the others don't,
so it feels like it flickers when changing sections." Every nav is a full-page
`<a href>` load, so the wave restarts at phase 0 each time; the flicker is that
reset. Fix: base.html seeds the wave's SCROLL POSITION from Date.now() BEFORE first
paint, so each layer resumes exactly where a continuously-running one would be —
identical phase across every page, no jump.

v0.50.76 moved the wave to a continuous-velocity rAF model (app.js), so the phase
is now a --hero-wave-x/2-x px offset (seeded here) that app.js's loop advances,
rather than a CSS animation-delay. The seed uses the same idle speeds app.js uses
(240px/9s, 320px/14s) so the hand-off is seamless.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
BASE = (REPO / "app" / "web" / "templates" / "base.html").read_text()


def _rule(sel: str) -> str:
    i = APP_CSS.index(sel)
    return APP_CSS[i:APP_CSS.index("}", i) + 1]


def test_base_seeds_wall_clock_phase_pre_paint():
    """Both phase vars are seeded from Date.now() on documentElement. The block must
    live in <head> BEFORE the deferred app.js so it runs before the hero paints (no
    phase-0 flash)."""
    assert "r.setProperty('--hero-wave-x', '-' + ((t * (240 / 9)) % 240).toFixed(2) + 'px')" in BASE
    assert "r.setProperty('--hero-wave2-x', ((t * (320 / 14)) % 320).toFixed(2) + 'px')" in BASE
    assert "var t = Date.now() / 1000" in BASE
    # ordering: the seed script precedes the deferred app.js load.
    assert BASE.index("--hero-wave-x") < BASE.index('src="/static/app.js')


def test_both_wave_layers_consume_the_seeded_phase():
    after = _rule(".hero::after {")
    before = _rule(".hero::before {")
    # each layer reads its OWN phase var, with a 0px fallback so a browser without the
    # var (or JS disabled) still renders the wave statically at phase 0.
    assert "mask-position: var(--hero-wave-x, 0px) center" in after
    assert "mask-position: var(--hero-wave2-x, 0px) center" in before


def test_app_js_reseeds_phase_from_the_same_clock_speeds():
    """app.js's rAF loop seeds its own phase from Date.now() at the SAME idle speeds,
    so it continues from base.html's pre-paint value with no jump on hand-off."""
    assert "_hero.phase = (t * _HERO_SPEED_IDLE) % _HERO_TILE;" in APP_JS
    assert "_hero.phase2 = (t * _HERO_SPEED_IDLE2) % _HERO_TILE2;" in APP_JS
    assert "const _HERO_SPEED_IDLE = _HERO_TILE / 9;" in APP_JS
    assert "const _HERO_SPEED_IDLE2 = _HERO_TILE2 / 14;" in APP_JS
    # the old animation-delay phase-sync is gone (no CSS animation to delay).
    assert "hero-wave-delay" not in APP_CSS
    assert "hero-wave-delay" not in BASE
