"""v1.20.46 — button interaction-state + keyboard-a11y fixes (CSS audit).

Two defects the 2026-05-30 CSS/button audit found and verified:

1. Disabled danger/promote re-light on hover. `.btn:disabled` and
   `.btn-danger:hover` are both specificity (0,2,0); CSS breaks the tie
   by SOURCE ORDER. Pre-fix `.btn:disabled` was declared ABOVE
   `.btn-danger:hover` and the four `.btn-promote-*:hover` rules, so a
   disabled REMOVE / PURGE / PROMOTE button (clearUrlOverride disables
   every row-menu button) re-lit its red/violet/etc. background + glow
   on hover. Fix: move `.btn:disabled` BELOW every tone `:hover` so it
   wins the tie uniformly.

2. No keyboard focus ring on ops controls. app.css's universal
   `:focus { outline: none }` strips the ring app-wide; the
   `:focus-visible` allow-list restored it only for app.css primitives
   and omitted every ops.css interactive element. Keyboard users
   tabbing onto the topbar pills / drawer close / op-card cancel saw no
   focus. Fix: add a `:focus-visible` block for `.op-pill`,
   `.ops-drawer-close`, `.op-card-cancel` in ops.css.

Source-order pins (CSS cascade is position-dependent, so byte offsets
are the contract here).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
OPS_CSS = (REPO / "app" / "web" / "static" / "ops.css").read_text()


# ── Bug 1: :disabled must follow every tone :hover ───────────


def test_disabled_declared_after_all_tone_hovers():
    """:disabled wins the (0,2,0) tie only if it is the LAST of the
    contending rules in source order. Assert it appears AFTER the
    danger + every promote tone :hover."""
    disabled = APP_CSS.index(".btn:disabled")
    for hover in (
        ".btn-danger:hover",
        ".btn-promote-ub:hover",
        ".btn-promote-pb:hover",
        ".btn-promote-tb:hover",
        ".btn-promote-ab:hover",
    ):
        assert APP_CSS.index(hover) < disabled, (
            f"{hover} must be declared BEFORE .btn:disabled so the "
            f"disabled rule wins the source-order tiebreak (else a "
            f"disabled button re-lights on hover)"
        )


def test_disabled_block_intact():
    """The moved block keeps its neutralizing declarations."""
    idx = APP_CSS.index(".btn:disabled, .btn[disabled] {")
    block = APP_CSS[idx:idx + 160]
    assert "box-shadow: none" in block
    assert "background: transparent" in block
    assert "cursor: not-allowed" in block


def test_disabled_appears_exactly_once():
    """Guard against a future edit re-adding a second :disabled block
    above the hovers (which would resurrect the bug via source order)."""
    assert APP_CSS.count(".btn:disabled, .btn[disabled] {") == 1


# ── Bug 2: ops focus-visible ring ────────────────────────────


def test_ops_focus_visible_block_present():
    """The tabbable ops controls must restore a keyboard focus ring
    that app.css's universal :focus{outline:none} stripped."""
    assert ".op-pill:focus-visible" in OPS_CSS
    assert ".ops-drawer-close:focus-visible" in OPS_CSS
    assert ".op-card-cancel:focus-visible" in OPS_CSS
    anchor = OPS_CSS.index(".op-pill:focus-visible")
    block = OPS_CSS[anchor:anchor + 220]
    assert "outline: 2px solid var(--cyan)" in block
    assert "outline-offset: 2px" in block


def test_v1_20_46_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
