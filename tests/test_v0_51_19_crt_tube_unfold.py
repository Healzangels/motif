"""v0.51.19 — CRT power-on: the PICTURE unfolds, not just the mask.

the user on the v0.51.10-18 reveal: "it still feels a bit too much like
curtains ... we have the left and right white space so you can really
feel like it's something being pulled across." The page behind the
shutters was fully-formed and static, so the moving edge read as a
drape being drawn — worst over the empty side margins, where the edge
was the only thing in motion.

A real CRT's vertical deflection ramps up: the image starts as a
compressed line and STRETCHES open from it. #crt-tube (a style-free
base.html wrapper around all visible chrome) squashes to a sliver at
the fold-line, holds through the centre-line beat, then expands on the
SAME duration + bezier as the shutters — the phosphor glow now rides
the edge of a growing picture instead of sliding across a finished
one. A brightness/saturation bloom starts white-hot and settles as the
raster opens.

Verified live (preview instance, real login trigger): computed
transform sampled per-frame — scaleY 0.004 through the ~100ms hold,
0.10 at 139ms, 0.88 at 398ms, 1.0 at 622ms; brightness 2.6 → 1.0 on
the same curve; transform/filter both `none` at rest (wrapper fully
inert); dashboard layout pixel-identical at rest.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
BASE_HTML = (REPO / "app" / "web" / "templates" / "base.html").read_text()


# ── the wrapper: present once, wraps all visible chrome ───────


def test_tube_wrapper_present_exactly_once():
    assert BASE_HTML.count('<div id="crt-tube">') == 1


def test_tube_wraps_chrome_but_not_the_overlays():
    i_overlay = BASE_HTML.index('id="crt-power-on"')
    i_off = BASE_HTML.index('id="crt-power-off"')
    i_tube = BASE_HTML.index('<div id="crt-tube">')
    i_topbar = BASE_HTML.index('<header class="topbar">')
    # overlays (fixed, must NOT squash with the page) come BEFORE the
    # wrapper opens; the topbar is inside it.
    assert i_overlay < i_tube and i_off < i_tube, (
        "v0.51.19: the fixed CRT overlays must stay OUTSIDE #crt-tube — "
        "inside, the shutters would squash with the picture")
    assert i_tube < i_topbar
    # the wrapper closes after the footer, before </body>.
    i_footer = BASE_HTML.index("</footer>")
    i_close = BASE_HTML.index("</div>", i_footer)
    assert i_close < BASE_HTML.index("</body>")


# ── the unfold: gated, synchronized, inert at rest ────────────


def test_tube_animation_gated_on_playing():
    assert "body:has(> .crt-power-on.playing) #crt-tube {" in CSS, (
        "v0.51.19: the unfold must be :has-gated on .playing so the "
        "wrapper is inert at rest and the VISUALS opt-out is honoured")
    # NO bare base rule — the wrapper carries zero styles at rest.
    assert "\n#crt-tube {" not in CSS


def test_tube_unfold_squashes_holds_and_stretches():
    k = CSS[CSS.index("@keyframes crt-tube-unfold"):]
    k = k[:k.index("}", k.index("100%"))]
    # squashed to a sliver through the centre-line beat (same 16% hold
    # as the shutters), then stretches to full.
    assert k.count("scaleY(0.004)") == 2, "hold at 0% AND 16%"
    assert "scaleY(1)" in k
    # phosphor bloom: white-hot at the line, settled by the end.
    assert "brightness(2.6)" in k
    assert "brightness(1)" in k


def test_tube_and_shutters_share_timing():
    """The picture edge must track the shutter edge — same duration +
    bezier, or the glow detaches from the growing image."""
    tube_rule = CSS[CSS.index("body:has(> .crt-power-on.playing) #crt-tube {"):]
    tube_rule = tube_rule[:tube_rule.index("}")]
    assert "crt-tube-unfold 0.62s cubic-bezier(0.32, 0.62, 0.4, 1)" in tube_rule
    assert ("crt-power-on-shutter-top 0.62s "
            "cubic-bezier(0.32, 0.62, 0.4, 1)") in CSS
    # origin = the fold-line (viewport centre on the top-of-page plays
    # this fires on).
    assert "transform-origin: 50% 50vh;" in tube_rule


def test_shutters_and_line_survive():
    """The unfold rides UNDER the existing v0.51.10 shutters + fold-line
    — they carry the glow sweep and mask the squashed page's edges."""
    assert ".crt-power-on.playing::before" in CSS
    assert "@keyframes crt-power-on-line" in CSS
