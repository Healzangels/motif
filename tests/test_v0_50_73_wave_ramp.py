"""v0.50.73 / v0.50.76 — hero-wave intensity + speed ramp smoothly (gas pedal).

the user (v0.50.73): "when the intensity of the hero wave increases as more activity
occurs [it] can naturally increase ... right now it jumps or spikes." (v0.50.76): the
discrete-level ramp STILL jolted — each data-busy-level step hard-swapped the
un-animatable animation-duration, re-timing the keyframe. "I want ... like pressing
the gas pedal it smoothly starts increasing in speed, not suddenly at the new speed
... same when a job completes it's like gently pressing the brake."

Fix: a continuous-velocity rAF loop (_heroWaveTick). It advances a MONOTONIC phase
(a speed change alters the velocity, never the position — so it can't jump) while
easing an energy scalar toward a target with exponential smoothing. energy drives
both the phase velocity and the CSS intensity vars. This file drives the loop in
quickjs with synthetic frame timestamps + a mock DOM.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


# ── Static wiring ──

def test_loop_advances_a_monotonic_phase_and_eases_energy():
    assert "function _heroWaveTick(now) {" in APP_JS
    # position advances by velocity*dt (monotonic — never re-timed like a keyframe).
    assert "h.phase = (h.phase + sp * dt) % _HERO_TILE;" in APP_JS
    # energy chases its target exponentially (the gas/brake ease).
    assert "const k = 1 - Math.exp(-dt / _HERO_TAU);" in APP_JS
    assert "h.energy += (h.target - h.energy) * k;" in APP_JS
    # dt is clamped so a background-tab gap can't fling the phase / snap the energy.
    assert "if (dt > 0.05) dt = 0.05;" in APP_JS


# ── Behavioral: run _heroWaveTick in quickjs with synthetic frames + mock DOM ──

def _hero_src() -> str:
    start = APP_JS.index("  const _HERO_TILE = 240, _HERO_TILE2 = 320;")
    end = APP_JS.index("  function _startHeroWave() {", start)
    return APP_JS[start:end]


def _run(js_body):
    quickjs = pytest.importorskip("quickjs")
    ctx = quickjs.Context()
    # mock DOM (records CSS vars) + a non-recursing requestAnimationFrame so we can
    # drive frames manually with chosen timestamps.
    harness = (
        "var _vars = {};\n"
        "var document = { documentElement: {\n"
        "  style: { setProperty: function(k, v){ _vars[k] = v; } },\n"
        "  classList: { contains: function(){ return false; } }\n"
        "} };\n"
        "function requestAnimationFrame(){ return 1; }\n"
        + _hero_src() + "\n"
        + js_body + "\n"
    )
    return json.loads(ctx.eval(harness))


def test_gas_energy_eases_up_not_snaps():
    # target 1 from idle: energy climbs GRADUALLY (first frame is a tiny step, not a
    # jump to full) and keeps rising toward 1.
    out = _run(
        "_setHeroWaveTarget(1);\n"
        "var out = [], t = 0;\n"
        "for (var i = 0; i < 80; i++) { t += 16; _heroWaveTick(t); out.push(_hero.energy); }\n"
        "JSON.stringify(out);"
    )
    assert out[0] < 0.1                       # first frame nudges, doesn't snap
    assert all(b >= a for a, b in zip(out, out[1:]))   # monotonic up
    assert out[-1] > 0.8                      # ~settled toward full after ~1.3s


def test_position_never_jumps_when_the_target_flips():
    # advance-per-frame stays small + strictly forward across an idle→full target flip
    # (the whole point: a speed change moves the velocity, not the position). And the
    # advance GROWS after the flip — the wave accelerated (gas pedal).
    adv = _run(
        "var adv = [], t = 16;\n"
        "_setHeroWaveTarget(0);\n"
        "_heroWaveTick(t);\n"                # prime: first tick has dt 0 (last unset)
        "var prev = _hero.phase;\n"
        "for (var i = 0; i < 30; i++) { t += 16; _heroWaveTick(t);\n"
        "  adv.push((_hero.phase - prev + 240) % 240); prev = _hero.phase; }\n"
        "_setHeroWaveTarget(1);\n"
        "for (var j = 0; j < 60; j++) { t += 16; _heroWaveTick(t);\n"
        "  adv.push((_hero.phase - prev + 240) % 240); prev = _hero.phase; }\n"
        "JSON.stringify(adv);"
    )
    assert all(0 < d <= 5 for d in adv)       # forward + no teleport, every frame
    assert adv[-1] > adv[0]                    # accelerated after the flip


def test_brake_energy_eases_back_down_to_idle():
    data = _run(
        "_setHeroWaveTarget(1);\n"
        "var t = 0; for (var i = 0; i < 120; i++) { t += 16; _heroWaveTick(t); }\n"
        "var up = _hero.energy;\n"
        "_setHeroWaveTarget(0);\n"
        "var out = []; for (var j = 0; j < 500; j++) { t += 16; _heroWaveTick(t); out.push(_hero.energy); }\n"
        "JSON.stringify([up].concat(out));"
    )
    up, out = data[0], data[1:]
    assert up > 0.9                            # reached ~full
    assert all(b <= a for a, b in zip(out, out[1:]))   # monotonic down (coast)
    assert out[-1] == 0                        # settles exactly at idle (~8s of brake)


def test_background_tab_gap_is_clamped_not_flung():
    # a huge timestamp jump (tab was hidden) must NOT snap energy to target or fling
    # the phase across many tiles — dt clamps to 0.05s.
    e0, e1, adv = _run(
        "_setHeroWaveTarget(1);\n"
        "_heroWaveTick(0); var p0 = _hero.phase, e0 = _hero.energy;\n"
        "_heroWaveTick(100000); var adv = (_hero.phase - p0 + 240) % 240;\n"
        "JSON.stringify([e0, _hero.energy, adv]);"
    )
    assert e0 == 0                             # first frame has dt 0 (last unset)
    assert e1 < 0.1                            # 100s gap eased ONE clamped step, not to 1
    assert adv <= 5                            # phase advanced ≤ one clamped frame
