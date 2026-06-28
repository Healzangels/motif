"""v0.50.7 — CRT power-on flicker (flavor pass 4/4, CRT half).

A quick, gentle "screen turns on" sweep played ONCE per tab session. base.html
renders a full-viewport .crt-power-on overlay + a tiny inline script that adds
.playing only when sessionStorage hasn't seen it — so it fires on the first
load, not on every full-page nav. The prefers-reduced-motion clamp shortens it
to near-instant.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
BASE = (REPO / "app" / "web" / "templates" / "base.html").read_text()


def test_overlay_and_session_gate_in_base():
    assert 'id="crt-power-on"' in BASE
    # session-gated so it doesn't re-fire on every full-page nav.
    assert "sessionStorage.getItem('motif:crt-power-on')" in BASE
    assert "sessionStorage.setItem('motif:crt-power-on'" in BASE
    assert "classList.add('playing')" in BASE


def test_css_overlay_is_inert_and_one_shot():
    i = APP_CSS.index(".crt-power-on {")
    block = APP_CSS[i:APP_CSS.index("}", i)]
    # rests invisible + never blocks clicks.
    assert "opacity: 0" in block
    assert "pointer-events: none" in block
    # the animation only plays under .playing (the session-gated class).
    assert "@keyframes crt-power-on" in APP_CSS
    j = APP_CSS.index(".crt-power-on.playing {")
    pblock = APP_CSS[j:APP_CSS.index("}", j)]
    assert "animation: crt-power-on" in pblock
    # one-shot — no infinite, no fill-mode:forwards (rests at the natural 0).
    assert "infinite" not in pblock
    assert "forwards" not in pblock
