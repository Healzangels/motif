"""v1.23.14 — saved-presets star is an inline SVG, centred for real.

the user flagged the star as off-centre twice (v1.22.98 fixed the box;
this fixes the glyph). The ☆/★ text glyph's ink sits ~1.2px low +
~0.5px left of its advance box (font-dependent side bearings), so
flex-centring the line box still left it visibly off. The fix
replaces the glyph with an inline 5-point SVG star whose bounding
box is centred in the viewBox (y-origin shifted -0.95 to cancel the
star's top-heavy bbox) — measured ink centre 0,0 vs the 26x26 box.

Fill is CSS-driven off the .has-active class (the ☆→★ semantic),
NOT a textContent swap — a swap would wipe the SVG. The JS keeps
toggling .has-active; the two `bookmark.textContent = …` lines are
gone.
"""
from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
LIB_HTML = (REPO / "app" / "web" / "templates" / "library.html").read_text()
# Strip jinja {# #} comments — they don't render (the explanatory
# comment names ☆/★, which would false-trip the glyph check below).
LIB_CODE = re.sub(r"\{#.*?#\}", "", LIB_HTML, flags=re.S)
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def test_summary_holds_an_svg_star_not_a_glyph():
    i = LIB_CODE.index("library-presets-bookmark")
    summary = LIB_CODE[i:LIB_CODE.index("</summary>", i)]
    assert 'class="library-presets-star"' in summary
    assert 'viewBox="0 -0.95 24 24"' in summary, (
        "the -0.95 y-origin centres the star's top-heavy bounding "
        "box in the square"
    )
    assert "<path" in summary
    # the bare text glyph must be gone from the rendered summary.
    assert "☆" not in summary and "★" not in summary


def test_star_svg_is_block_sized():
    i = APP_CSS.index(".library-presets-star {")
    block = APP_CSS[i:APP_CSS.index("}", i)]
    assert "display: block;" in block
    assert "width: 15px;" in block
    assert "height: 15px;" in block


def test_fill_is_class_driven_outline_default():
    # outline by default (fill: none + stroke).
    i = APP_CSS.index(".library-presets-star path {")
    pblock = APP_CSS[i:APP_CSS.index("}", i)]
    assert "fill: none;" in pblock
    assert "stroke: currentColor;" in pblock
    # filled only when the menu carries .has-active.
    assert (".library-presets-menu.has-active .library-presets-star path {"
            in APP_CSS)
    j = APP_CSS.index(
        ".library-presets-menu.has-active .library-presets-star path {")
    ablock = APP_CSS[j:APP_CSS.index("}", j)]
    assert "fill: currentColor;" in ablock


def test_js_no_longer_swaps_glyph_textcontent():
    """A textContent swap would wipe the inline SVG — the fill is
    CSS-driven off .has-active now, which the JS still toggles."""
    assert "bookmark.textContent = '★'" not in APP_JS
    assert "bookmark.textContent = '☆'" not in APP_JS
    # the class toggle that drives the fill must remain.
    assert "menu.classList.add('has-active')" in APP_JS
    assert "menu.classList.remove('has-active')" in APP_JS
