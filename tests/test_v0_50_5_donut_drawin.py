"""v0.50.5 — dashboard SOURCE BREAKDOWN donut draw-in (flavor pass 3/4, donut half).

On a pie's FIRST render the slices sweep + scale + fade into place. app.js
_renderSourcePie adds .pie-drawin to the persistent slices <g> exactly once —
gated on _pieState[lastKeyKey] still being its initial '' — so the 1s/30s poll
re-renders and legend toggles (which rebuild the inner slice circles) don't
restart the one-shot animation.

The count-up half was split to a later tag: the dashboard stat numbers are
SSR-baked, so an on-load count-up needs special handling to avoid a brief
value-then-reset flash.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def test_pie_drawin_keyframe_and_rule_exist():
    assert "@keyframes pie-draw-in" in APP_CSS
    i = APP_CSS.index(".pie-drawin {")
    block = APP_CSS[i:APP_CSS.index("}", i)]
    assert "animation: pie-draw-in" in block
    # pivots on the donut hub, not the SVG corner.
    assert "transform-origin: center" in block
    # no fill-mode:forwards → reduced-motion clamp rests at the natural state.
    assert "forwards" not in block


def test_js_adds_drawin_only_on_first_render():
    """The class is added on the persistent slices group, gated on the pie's
    initial '' state — so it fires once, not on every poll/legend re-render."""
    assert "slicesEl.classList.add('pie-drawin')" in APP_JS
    # the first-render gate references the per-pie hash-skip state.
    i = APP_JS.index("slicesEl.classList.add('pie-drawin')")
    line_start = APP_JS.rfind("\n", 0, i)
    line = APP_JS[line_start:i]
    assert "_pieState[lastKeyKey] === ''" in line
