"""v1.22.98 — center the saved-presets ☆ glyph in its square.

the user: "the star for the saved filter presets isn't centered in
its square." The <summary> bookmark rendered the 18px glyph on the
text baseline inside uneven 4/8px padding, drifting low-left of the
border box. Now an inline-flex 26x26 square (tracking the 4px-pad +
t-tiny toolbar buttons' rendered height) with both axes centered.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def test_bookmark_glyph_is_flex_centered():
    i = APP_CSS.index(".library-presets-bookmark {")
    block = APP_CSS[i:APP_CSS.index("}", i)]
    assert "display: inline-flex;" in block
    assert "align-items: center;" in block
    assert "justify-content: center;" in block
    assert "width: 26px;" in block
    assert "height: 26px;" in block
    assert "padding: 0;" in block
    # v1.23.14: the flex box still centres its content, but the
    # content is now an SVG star (not a ☆/★ glyph whose ink sat off
    # its advance box). See test_v1_23_14_preset_star_svg for the
    # deterministic-centring guarantee.
