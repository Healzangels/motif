"""v0.50.28 — flowing soundwave across the hero's lower band.

the user: "I think it would be cool to add a wave style music animation across the
width" in the empty space the v0.50.25 hero min-height opened up. A .hero::after
sine wave: SVG sine path as a CSS mask, var(--green) as the masked colour, scrolled
via mask-position; behind the text (z-index:-1).
"""
from pathlib import Path

APP_CSS = (Path(__file__).resolve().parent.parent / "app" / "web" / "static" / "app.css").read_text()


def _rule(sel):
    i = APP_CSS.index(sel)
    return APP_CSS[i:APP_CSS.index("}", i)]


def test_hero_is_a_positioned_stacking_context():
    h = _rule(".hero { margin-bottom")
    assert "position: relative" in h
    assert "isolation: isolate" in h


def test_wave_uses_token_colour_masked_by_an_svg_wave():
    a = _rule(".hero::after {")
    # colour is the token, not a hardcoded hex.
    assert "background: var(--green)" in a
    # the wave shape is an SVG sine path used as a mask.
    assert "mask: url(\"data:image/svg+xml," in a
    assert "Q30 6 60 24 T120 24" in a  # the sine path
    # behind the title/subtitle, decorative, inert.
    assert "z-index: -1" in a
    assert "pointer-events: none" in a


def test_wave_scroll_is_driven_by_the_rAF_phase_var():
    # v0.50.76: the scroll is no longer a CSS @keyframes animation (its speed snapped
    # between discrete busy levels). app.js's rAF loop drives a monotonic phase into
    # --hero-wave-x so speed changes never jump the position.
    a = _rule(".hero::after {")
    assert "mask-position: var(--hero-wave-x" in a
    assert "@keyframes hero-wave {" not in APP_CSS
    assert "animation: hero-wave" not in APP_CSS
