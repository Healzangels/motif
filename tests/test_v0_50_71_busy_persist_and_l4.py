"""v0.50.71 — hero-wave busy state survives navigation + a richer level 4.

Two follow-ups from the user on the reactive hero wave:

1. NO idle→busy FLIP ON NAV: starting a refresh on /tv then navigating to
   /dashboard briefly painted the SLOW (idle) wave, then the poll flipped it fast
   — a visible "change in how the wave looked." Fix: app.js persists the busy
   level to sessionStorage (per-tab); base.html restores motif-busy +
   data-busy-level PRE-PAINT so the page loads already-busy. The poll then keeps
   it accurate and clears it (self-corrects if the op finished mid-nav).

2. RICHER LEVEL 4: "increase the wave richness another level as a sync AND refresh
   is going on and so forth." A 4th tier (score >= 6) whose richness comes mostly
   from the SECOND wave layer swelling, so it reads fuller — not just faster.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
BASE = (REPO / "app" / "web" / "templates" / "base.html").read_text()


# ── 1. Persist on write, restore pre-paint ──

def test_app_persists_busy_level_to_session_storage():
    assert "sessionStorage.setItem('motif:busy', String(_busyLevel))" in APP_JS
    # cleared when idle so a finished op doesn't leave a stale busy hint.
    assert "sessionStorage.removeItem('motif:busy')" in APP_JS


def test_base_restores_busy_pre_paint():
    """The restore must run in <head> BEFORE the deferred app.js, and stamp both
    the class and the level so the wave paints at the RIGHT intensity, not idle."""
    assert "sessionStorage.getItem('motif:busy')" in BASE
    assert "e.classList.add('motif-busy')" in BASE
    assert "e.setAttribute('data-busy-level', b)" in BASE
    assert BASE.index("getItem('motif:busy')") < BASE.index('src="/static/app.js')


# ── 2. Level 4 wiring ──

def test_level_4_threshold_and_cap():
    assert ("const _busyLevel = _busyScore >= 6 ? 4\n"
            "        : _busyScore >= 4 ? 3 : _busyScore >= 2 ? 2 : 1;" in APP_JS)
