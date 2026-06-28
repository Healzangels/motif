"""v1.15.134 — prefers-reduced-motion respect.

Motif has ~12 infinite pulse / blink / breathe animations across:

  - `.brand-mark` VU-meter equalizer pulse (topbar logo, amber; v1.24.95)
  - `.record-spinner` + `.login-ripple` (v1.24.95 record/music motion)
  - `.dot-pulse` (live indicators)
  - `.fail-pulse` (failure-state pulses on title-glyph + state-pill)
  - `.dry-run-pulse` (dry-run banner pulse)
  - `.state-pill-pulse` (state-pill activity pulse)
  - `.op-pill-fail-pulse` (topbar FAIL pill)
  - `.op-pill-live-pulse` (topbar OPS-active pill)
  - `.op-card-breathe` (op-card running-state breathing)
  - + various others in ops.css

For users with motion sensitivity (vestibular triggers, ADHD
focus impact, motion-induced headaches), persistent looping
animations are an accessibility issue WCAG calls out
specifically. The `prefers-reduced-motion: reduce` media query
lets motion-sensitive users opt out at the OS level
(macOS System Settings → Accessibility → Display → Reduce motion;
Windows Settings → Ease of Access → Display → Show animations).

## Fix

Universal `@media (prefers-reduced-motion: reduce)` rule kills
infinite animations by clamping `animation-iteration-count: 1`
and `animation-duration: 0.01ms`. Effectively disables every
pulse / blink / breathe.

The rule does NOT touch `transition:` properties — finite
hover/focus color shifts stay intact since they're brief and
user-initiated (the v1.15.110 motion tokens already keep these
under 200ms). `scroll-behavior: auto` override disables
smooth-scroll for users who find it disorienting.

## Tests
"""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = REPO / "app" / "web" / "static" / "app.css"


def _strip_comments(src: str) -> str:
    return re.sub(r"/\*.*?\*/", "", src, flags=re.DOTALL)


def test_prefers_reduced_motion_block_exists():
    """The `@media (prefers-reduced-motion: reduce)` block must
    exist in app.css. Without it, users with motion sensitivity
    have no way to opt out of the topbar/pill/banner pulses."""
    src = _strip_comments(APP_CSS.read_text())
    assert "@media (prefers-reduced-motion: reduce)" in src, (
        "v1.15.134: prefers-reduced-motion media query missing. "
        "Motif's 12+ infinite-animation surfaces are an "
        "accessibility issue for motion-sensitive users without "
        "this opt-out."
    )


def test_reduced_motion_kills_infinite_animations():
    """The rule must clamp animation-iteration-count to 1 and
    duration to a near-zero value. This is the standard pattern
    — duration:0.01ms + iteration-count:1 ensures the animation
    runs effectively a single frame and stops."""
    src = _strip_comments(APP_CSS.read_text())
    block_idx = src.index("@media (prefers-reduced-motion: reduce)")
    block_end = src.index("}", src.index("}", block_idx) + 1)
    block = src[block_idx:block_end + 1]
    assert "animation-duration: 0.01ms" in block
    assert "animation-iteration-count: 1" in block


def test_reduced_motion_does_not_kill_transitions():
    """Finite transitions (hover color shifts, etc.) stay intact
    since they're brief + user-initiated. The rule should ONLY
    touch animation properties, not transition-duration."""
    src = _strip_comments(APP_CSS.read_text())
    block_idx = src.index("@media (prefers-reduced-motion: reduce)")
    block_end = src.index("}", src.index("}", block_idx) + 1)
    block = src[block_idx:block_end + 1]
    # The rule must NOT contain a transition-duration override.
    assert "transition-duration" not in block, (
        "v1.15.134: the rule must not clamp transition-duration. "
        "Hover-state color shifts are brief + user-initiated; "
        "killing them removes a useful feedback channel without "
        "addressing the motion-sensitivity concern."
    )


def test_reduced_motion_disables_smooth_scroll():
    """For users who find smooth-scrolling disorienting (a
    documented vestibular trigger), set scroll-behavior: auto
    so the page jumps instead of animating to the target."""
    src = _strip_comments(APP_CSS.read_text())
    block_idx = src.index("@media (prefers-reduced-motion: reduce)")
    block_end = src.index("}", src.index("}", block_idx) + 1)
    block = src[block_idx:block_end + 1]
    assert "scroll-behavior: auto" in block


def test_reduced_motion_applies_universally():
    """The rule must use a universal selector (*) so it covers
    every animated element without an allowlist. Adding new
    pulses in the future shouldn't require touching this rule."""
    src = _strip_comments(APP_CSS.read_text())
    block_idx = src.index("@media (prefers-reduced-motion: reduce)")
    block_end = src.index("}", src.index("}", block_idx) + 1)
    block = src[block_idx:block_end + 1]
    # Universal selector + pseudo-elements ensures coverage.
    assert "*, *::before, *::after" in block or "* {" in block
