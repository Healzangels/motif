"""v0.50.73 — hero-wave intensity ramps naturally instead of spiking.

the user: "when the intensity of the hero wave increases as more activity occurs
[it] can naturally increase to the higher intensity — right now it jumps or spikes
to the higher level and it's a bit jarring when kicking off a few jobs from the
dashboard." Fix: refreshTopbarStatus sets a TARGET level; _rampWaveLevel steps the
DISPLAYED level toward it one at a time (~380ms/step), so idle→L4 swells over ~1.5s
instead of snapping. Nav-restore still snaps (init from the DOM, no ramp on load).
"""
from __future__ import annotations

from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


# ── Static wiring ──

def test_busy_block_ramps_toward_target_not_snaps():
    # the direct snap (set/removeAttribute in the busy block) is replaced by a ramp.
    assert "_rampWaveLevel(heroBusy ? _busyLevel : 0);" in APP_JS
    # the old snap lines must be gone from the busy block.
    assert "if (heroBusy) _root.setAttribute('data-busy-level', String(_busyLevel));" not in APP_JS


def test_ramp_inits_displayed_from_dom_so_nav_restore_doesnt_ramp():
    assert "let _waveDisplayed = (parseInt(" in APP_JS
    assert "document.documentElement.getAttribute('data-busy-level'), 10) || 0);" in APP_JS


def test_ramp_steps_one_level_at_a_time_on_a_timer():
    assert "function _rampWaveLevel(target) {" in APP_JS
    assert "_waveDisplayed += target > _waveDisplayed ? 1 : -1;" in APP_JS
    assert "_waveRampTimer = setTimeout(step, 380);" in APP_JS


# ── Behavioral: run _rampWaveLevel in quickjs with synchronous timers ──

def _ramp_src() -> str:
    start = APP_JS.index("let _waveDisplayed = (parseInt(")
    end = APP_JS.index("\n  // v1.12.118:", start)
    return APP_JS[start:end]


def _run_ramp(target_calls, ret="_seq.join(',')"):
    quickjs = pytest.importorskip("quickjs")
    ctx = quickjs.Context()
    # Mock DOM (records data-busy-level history) + synchronous setTimeout so the
    # ramp runs to completion in one call, and we can read the visited sequence.
    harness = (
        "var _seq = [];\n"
        "var _attr = null; var _cls = false;\n"
        "var document = { documentElement: {\n"
        "  getAttribute: function(){ return _attr; },\n"
        "  setAttribute: function(_k,v){ _attr = v; _seq.push('L'+v); },\n"
        "  removeAttribute: function(){ _attr = null; _seq.push('idle'); },\n"
        "  classList: { add: function(){ _cls = true; }, remove: function(){ _cls = false; } }\n"
        "} };\n"
        "function setTimeout(fn){ fn(); return 1; }\n"
        "function clearTimeout(){}\n"
        + _ramp_src() + "\n"
        + target_calls + "\n"
        + ret + ";"
    )
    return ctx.eval(harness)


def test_idle_to_l4_visits_every_intermediate_level():
    # from idle (0), ramping to 4 must pass through 1,2,3,4 — a build-up, not a jump.
    seq = _run_ramp("_rampWaveLevel(4);")
    assert seq == "L1,L2,L3,L4"


def test_ramp_down_steps_through_levels_to_idle():
    # start at L4 (DOM attr '4'), ramp to 0 → 3,2,1,idle (natural wind-down).
    seq = _run_ramp("_waveDisplayed = 4; _rampWaveLevel(0);")
    assert seq == "L3,L2,L1,idle"


def test_no_ramp_when_already_at_target():
    seq = _run_ramp("_waveDisplayed = 2; _rampWaveLevel(2);")
    assert seq == ""  # no attribute writes when displayed already == target


def test_idle_reconcile_clears_stuck_busy_class_when_already_at_zero():
    # v0.50.75 (code-review): ops.js can add 'motif-busy' out-of-band (optimistic
    # click) WITHOUT bumping _waveDisplayed. If that op then fizzles, refreshTopbar
    # calls _rampWaveLevel(0) while _waveDisplayed is already 0 — the terminal
    # branch must still authoritatively clear the class + attr, or the wave stays
    # busy forever. (Pre-fix the early-return skipped the clear.)
    cls = _run_ramp("_cls = true; _waveDisplayed = 0; _rampWaveLevel(0);", ret="_cls")
    assert cls is False  # stuck busy class was removed
    seq = _run_ramp("_cls = true; _waveDisplayed = 0; _rampWaveLevel(0);")
    assert seq == "idle"  # removeAttribute fired even though displayed already == 0
