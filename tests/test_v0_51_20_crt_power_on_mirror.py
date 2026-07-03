"""v0.51.20 — CRT power-on is the exact time-reverse of power-off.

the user, iterating on the reveal across v0.51.10-19: the shutters felt
"too much like curtains ... something being pulled across"; the v0.51.19
tube-stretch felt like "a conveyor belt" AND left the page scrolled
mid-way (the squash shrank the scrollable region, so Chrome's scroll
anchoring chased the growing content off the top). The power-OFF, by
contrast, is "perfect" — and the reason is that NOTHING TRAVELS: a black
veil fades in while a bright beam blooms then collapses to a dot. Pure
luminance, zero page motion.

So power-on now mirrors power-off, reversed:
  - the veil (`.crt-power-on` black background) HOLDS then fades AWAY to
    reveal the untouched page — the reverse of power-off's veil (fade in,
    then hold);
  - the beam (`.crt-on-line`) plays power-off's line keyframes backwards:
    a collapsed dot (scaleX 0) streaks out horizontally into a thin
    bright line, blooms tall as the picture floods in, then fades.

Because the page never transforms, there is no scroll jank and no
"travel" — the same visual language as the power-off.

Verified live (preview instance, real login trigger): per-frame samples
confirm the veil holds opacity 1 then fades to 0, the beam scaleX
interpolates 0→1 over the streak, scaleY blooms to ~2.6, and the page's
own transform stays `none` throughout (scroll position untouched).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
BASE = (REPO / "app" / "web" / "templates" / "base.html").read_text()


# ── the geometric approaches are gone (no travel, no scroll jank) ──


def test_no_page_transform_mechanisms():
    # the v0.50.98/v0.51.10-18 shutters and the v0.51.19 tube-stretch
    # (the scroll-jank + "conveyor" source) are all removed.
    assert "@keyframes crt-power-on-shutter-top" not in CSS
    assert "@keyframes crt-power-on-shutter-bot" not in CSS
    assert "@keyframes crt-tube-unfold" not in CSS
    assert "#crt-tube" not in CSS
    assert "#crt-tube" not in BASE, "the tube wrapper must be gone from base.html"
    assert ".crt-power-on::before" not in CSS and ".crt-power-on::after" not in CSS


# ── the veil: hold black, then reveal (reverse of power-off) ──


def test_veil_holds_black_then_reveals():
    cont = CSS[CSS.index(".crt-power-on {"):]
    cont = cont[:cont.index("}")]
    assert "background: var(--bg)" in cont, "the container IS the black veil"
    assert "opacity: 0" in cont, "inert at rest — page visible"
    assert "pointer-events: none" in cont
    veil = CSS[CSS.index("@keyframes crt-power-on-veil"):]
    veil = veil[:veil.index("}", veil.index("100%"))]
    # reverse of power-off's bg (0 -> 1, then hold): hold at 1, then -> 0.
    assert "0% { opacity: 1; }" in veil
    assert "45% { opacity: 1; }" in veil
    assert "100% { opacity: 0;" in veil


def test_power_off_veil_is_the_forward_shape():
    # the mirror is only meaningful if power-off still fades IN then holds —
    # pin it so a future power-off edit that breaks the symmetry is caught.
    off = CSS[CSS.index("@keyframes crt-power-off-bg"):]
    off = off[:off.index("}", off.index("100%"))]
    assert "0% { opacity: 0; }" in off
    assert "100% { opacity: 1;" in off


# ── the beam: power-off's line, reversed ──────────────────────


def test_beam_is_power_off_line_reversed():
    on = CSS[CSS.index("@keyframes crt-power-on-line"):]
    on = on[:on.index("}", on.index("100%"))]
    # starts as a collapsed dot (the END state of power-off's line) and
    # streaks out; power-off ENDS at scaleX(0) — power-on STARTS there.
    assert "0%   { opacity: 0;    transform: translateY(-50%) scaleX(0) scaleY(0.6); }" in on
    # blooms tall mid-way (power-off's early bloom, mirrored).
    assert "scaleY(2.6)" in on
    off = CSS[CSS.index("@keyframes crt-power-off-line"):]
    off = off[:off.index("}", off.index("100%"))]
    assert "scaleX(0)" in off, "power-off line collapses to a dot (scaleX 0) — the shape power-on reverses"


def test_beam_element_and_glow():
    assert 'class="crt-on-line"' in BASE, "base.html renders the beam span"
    line = CSS[CSS.index(".crt-on-line {"):]
    line = line[:line.index("}")]
    assert "background: var(--fg)" in line  # bright scanline core
    assert "box-shadow" in line and "green-bright" in line  # phosphor glow


# ── still inert + one-shot (unchanged contract) ───────────────


def test_power_on_inert_and_one_shot():
    j = CSS.index(".crt-power-on.playing {")
    pblock = CSS[j:CSS.index("}", j)]
    assert "animation: crt-power-on-veil" in pblock
    assert "ease-out" in pblock  # time-reverse of power-off's ease-in
    assert "infinite" not in pblock and "forwards" not in pblock
