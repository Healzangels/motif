"""v0.50.3 — tactile micro-interactions (design-flavor pass 1/4).

the user wanted small "fun flavor" polish. This tag:
  - modal <dialog>s EASE IN (fade + slight rise/scale) via @starting-style,
    instead of popping in;
  - filter/toggle .chip:hover gains a 1px lift + faint glow.

The .btn:active press already existed (translateY(1px), v-earlier) and is kept.

All three are brief, user-initiated TRANSITIONS — not infinite animations — so
the v1.15.134 prefers-reduced-motion policy (which keeps transitions and only
clamps infinite animations) leaves them intact by design. CSS-only.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def test_dialog_eases_in_with_starting_style():
    i = APP_CSS.index(".dlg {")
    block = APP_CSS[i:APP_CSS.index("}", i)]
    # a transition on opacity + transform drives the ease-in.
    assert "transition:" in block
    assert "opacity" in block and "transform" in block
    # @starting-style supplies the entry frame for showModal() (graceful degrade
    # to instant on engines without it).
    assert "@starting-style" in APP_CSS
    assert ".dlg[open]" in APP_CSS


def test_dialog_backdrop_fades_in():
    i = APP_CSS.index(".dlg::backdrop {")
    block = APP_CSS[i:APP_CSS.index("}", i)]
    assert "transition:" in block and "opacity" in block
    assert ".dlg[open]::backdrop" in APP_CSS


def test_chip_hover_lifts():
    # v0.50.91: the @media (hover: none) touch block adds an earlier
    # `.chip:hover { transform: none }` override, so target the BASE rule
    # (last occurrence) which still carries the desktop lift.
    i = APP_CSS.rfind(".chip:hover {")
    block = APP_CSS[i:APP_CSS.index("}", i)]
    assert "transform: translateY(-1px)" in block
    # the pre-existing color/border hover affordance is preserved.
    assert "color: var(--fg)" in block


def test_button_press_feedback_still_present():
    """The .btn:active press predates this tag; assert it wasn't lost."""
    assert ".btn:active { transform: translateY(1px); }" in APP_CSS
