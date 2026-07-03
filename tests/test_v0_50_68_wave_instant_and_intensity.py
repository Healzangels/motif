"""v0.50.68 / v0.50.76 — hero-wave: instant start + queue-scaled intensity.

Two follow-ups from the user on the v0.50.67 reactive hero wave:

1. INSTANT START: the wave reacted ~1.1s late — it waited for refreshTopbarStatus
   to see the enqueued job past the /api/stats 1s cache. Now ops.js kicks the wave
   the instant of the click (in setOptimisticPlaceholder), and refreshTopbarStatus
   unions hasOptimistic() so it can't drop it back off in the gap before the real op
   lands.

2. INTENSITY BY QUEUE DEPTH: stacking work (tdb sync + plex refresh + downloads…)
   escalates the wave. Score = distinct active op-kinds + total queued per-row jobs.

v0.50.76 rebuilt the wave on a CONTINUOUS-velocity model (the user — "like a gas
pedal ... not suddenly at the new speed"). The old discrete data-busy-level CSS
steps snapped animation-duration; now one rAF loop eases a 0..1 energy toward a
target derived from the score, and both speed (phase velocity) and intensity
(opacity/scaleY/brightness) scale continuously off that energy.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
OPS_JS = (REPO / "app" / "web" / "static" / "ops.js").read_text()


def _rule(css: str, sel: str) -> str:
    i = css.index(sel)
    return css[i:css.index("}", i) + 1]


# ── 1. Instant start (ops.js kicks + hasOptimistic; app.js unions it) ──

def test_optimistic_click_kicks_the_wave_immediately():
    """setOptimisticPlaceholder (fired on the click, before any poll) bumps the wave
    energy target so it starts accelerating with zero latency (v0.50.76)."""
    fn = OPS_JS[OPS_JS.index("function setOptimisticPlaceholder("):]
    fn = fn[:fn.index("\n  }") + 1]
    assert "window.__motifHeroWaveBump()" in fn


def test_bump_raises_the_target_to_the_optimistic_floor():
    assert "window.__motifHeroWaveBump = function ()" in APP_JS
    assert "if (_hero.target < _HERO_OPT_FLOOR) _setHeroWaveTarget(_HERO_OPT_FLOOR);" in APP_JS


def test_ops_exposes_has_optimistic():
    # v0.51.25: gained an optional `kind` filter (bare call still any-kind).
    assert "function hasOptimistic(kind)" in OPS_JS
    assert "_optimisticOp.expiresAt > Date.now()" in OPS_JS
    # exported so app.js can union it into the busy calc.
    assert re.search(r"window\.motifOps\s*=\s*\{[^}]*\bhasOptimistic\b", OPS_JS, re.S)


def test_app_unions_optimistic_so_busy_isnt_dropped_in_the_gap():
    """refreshTopbarStatus keeps the wave target up while a click-time optimistic op
    is live, so the ~1.1s /api/stats gap doesn't brake the wave back off."""
    assert "window.motifOps.hasOptimistic()" in APP_JS
    assert "const heroBusy = anyMutatingOpActive || _optimisticBusy;" in APP_JS
    # v0.50.76: the busy state is a continuous energy target the rAF loop eases into.
    assert "_setHeroWaveTarget(_busyEnergy);" in APP_JS


# ── 2. Intensity scales with queue depth, saturating at full ──

def test_busy_score_counts_op_kinds_plus_queued_jobs():
    # distinct active op-kinds + the per-row job SUM (pending+running).
    assert ("const _busyScore = (themerrdbBusy ? 1 : 0) + (plexEnumBusy ? 1 : 0)\n"
            "        + (opProgressRunning ? 1 : 0) + perJobSum;" in APP_JS)
    assert "const perJobSum = (" in APP_JS
    assert "const perJobBusy = perJobSum > 0;" in APP_JS


def test_busy_energy_is_continuous_floored_and_saturated():
    # v0.50.76: score → a 0..1 energy target. Any busy floors at _HERO_OPT_FLOOR so
    # one job still reads; heavy stacking saturates at 1 (min) so it can't get frantic.
    assert ("const _busyEnergy = heroBusy\n"
            "        ? Math.min(1, Math.max(_HERO_OPT_FLOOR, 0.28 + (_busyScore - 1) * 0.145))\n"
            "        : 0;" in APP_JS)
    assert "const _HERO_OPT_FLOOR = 0.3;" in APP_JS


def _energy_mapping(score, hero_busy=True):
    """mirror the JS _busyEnergy formula so we can assert its shape."""
    if not hero_busy:
        return 0.0
    return min(1.0, max(0.3, 0.28 + (score - 1) * 0.145))


def test_energy_mapping_rises_monotonically_then_caps():
    vals = [_energy_mapping(s) for s in range(1, 9)]
    # non-decreasing across the score range
    assert all(b >= a for a, b in zip(vals, vals[1:]))
    # a single job is already clearly busy (>= the floor)
    assert vals[0] >= 0.3
    # heavy stacking saturates at exactly 1 (capped — no runaway)
    assert vals[-1] == 1.0
    assert _energy_mapping(6) == 1.0
    # idle → zero
    assert _energy_mapping(0, hero_busy=False) == 0.0


def test_css_intensity_scales_continuously_off_energy_and_stays_in_band():
    """opacity (brightness), scaleY (height) + brightness all rise with energy off ONE
    variable — no discrete levels. At full energy (1) the wave is brightest + tallest;
    scaleY tops out inside the 38px reserved band."""
    a = _rule(APP_CSS, ".hero::after {")
    # linear in energy: base + coeff * var
    assert "opacity: calc(0.18 + 0.37 * var(--hero-wave-energy, 0))" in a
    assert "transform: scaleY(calc(1 + 0.32 * var(--hero-wave-energy, 0)))" in a
    assert "filter: brightness(calc(1 + 0.55 * var(--hero-wave-energy, 0)))" in a
    # full-energy scaleY (1 + 0.32) stays within the clearance the 38px band affords.
    assert 1 + 0.32 <= 1.35
    # the old discrete-level rules + per-level duration swaps are gone.
    assert '[data-busy-level="' not in APP_CSS
    assert "animation-duration" not in _rule(APP_CSS, ".hero::after {")


def test_speed_scales_off_the_same_energy_in_js():
    # the phase velocity interpolates idle→full by energy — so speed ramps with the
    # same continuous signal as the visuals (never a discrete duration swap).
    assert "const sp = _HERO_SPEED_IDLE + (_HERO_SPEED_FULL - _HERO_SPEED_IDLE) * e;" in APP_JS
    assert "const sp2 = _HERO_SPEED_IDLE2 + (_HERO_SPEED_FULL2 - _HERO_SPEED_IDLE2) * e;" in APP_JS
