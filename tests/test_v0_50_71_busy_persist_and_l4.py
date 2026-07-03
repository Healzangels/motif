"""v0.50.71 / v0.50.76 — hero-wave busy state survives navigation + full saturation.

Follow-ups from the user on the reactive hero wave:

1. NO idle→busy FLIP ON NAV: starting a refresh on /tv then navigating to
   /dashboard briefly painted the SLOW (idle) wave, then the poll flipped it fast
   — a visible "change in how the wave looked." Fix: app.js persists the busy state
   to sessionStorage (per-tab); base.html restores it PRE-PAINT so the page loads
   already-busy. The poll then keeps it accurate and clears it (self-corrects if the
   op finished mid-nav).

2. FULL SATURATION for heavy stacked activity ("increase the wave richness ... as a
   sync AND refresh is going on"). v0.50.76's continuous model saturates the energy
   at 1 when the score is heavy (6+), the ceiling — it can't get frantic past that.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
BASE = (REPO / "app" / "web" / "templates" / "base.html").read_text()


# ── 1. Persist on write, restore pre-paint ──

def test_app_persists_busy_energy_to_session_storage():
    # v0.50.76: persists the continuous ENERGY (0..1), not a discrete level.
    assert "sessionStorage.setItem('motif:busy', _busyEnergy.toFixed(3))" in APP_JS
    # cleared when idle so a finished op doesn't leave a stale busy hint.
    assert "sessionStorage.removeItem('motif:busy')" in APP_JS


def test_base_restores_busy_energy_pre_paint():
    """The restore must run in <head> BEFORE the deferred app.js and seed the energy
    var so the wave paints at the RIGHT intensity, not idle."""
    assert "parseFloat(sessionStorage.getItem('motif:busy'))" in BASE
    assert "r.setProperty('--hero-wave-energy', b.toFixed(3))" in BASE
    assert BASE.index("getItem('motif:busy')") < BASE.index('src="/static/app.js')
    # the old discrete class/attr restore is gone.
    assert "data-busy-level" not in BASE
    assert "motif-busy" not in BASE


def test_app_start_seeds_energy_from_the_restore_so_it_doesnt_ramp_from_idle():
    # a page loaded mid-op snaps to the restored energy (no idle→ramp-up flicker),
    # then eases to the live target.
    assert "restored = parseFloat(sessionStorage.getItem('motif:busy')) || 0;" in APP_JS
    assert "if (restored > 0) { _hero.energy = restored; _hero.target = restored; }" in APP_JS


# ── 2. Saturation at full energy ──

def test_energy_saturates_at_one():
    # heavy stacking (score 6+) hits Math.min(1, ...) — the ceiling.
    assert "Math.min(1, Math.max(_HERO_OPT_FLOOR, 0.28 + (_busyScore - 1) * 0.22))" in APP_JS
