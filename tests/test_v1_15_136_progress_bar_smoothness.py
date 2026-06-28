"""v1.15.136 — ops drawer progress-bar transition tune.

the user on v1.15.135:

> in the drawer the progress bars progress feels a bit jumpy and
> not smooth, especially when a refresh is running and another
> queued up action is in place the status bar that fills while
> waiting I like how it looks good but it's movement feels a bit
> jumpy and not as smooth and clean as it could.

## Root cause

The op-mini-bar-fill + op-card-bar-fill both used:

    transition: width 0.6s cubic-bezier(0.4, 0, 0.2, 1);

But the /api/progress poll cadence is **1s** when ops are active
(ops.js:1400 — `(running || pending) ? 1000 : 10000`).

The mismatch produced a visible stutter:

  - t=0: bar at 30%
  - poll at t=1s: returns 45%, transition starts
  - t=1.6s: 0.6s transition completes, bar settles at 45%
  - t=1.6s-2.0s: bar sits motionless
  - poll at t=2s: returns 60%, transition starts
  - …

PLUS the cubic-bezier(0.4, 0, 0.2, 1) Material-standard curve
DECELERATES at the end of each segment, so successive polls
showed an accelerate→decelerate→accelerate pattern. Visible
"jumpy" rhythm.

## Fix

Tuned both bar transitions to:

    transition: width 1s linear;

Effects:
  - Duration matches poll cadence: when one transition ends, the
    next poll's transition starts immediately from the current
    visual value (CSS handles mid-transition interrupt cleanly).
    No "settle then jump."
  - Linear easing: constant velocity. The eye reads it as
    "continuous forward motion" rather than per-poll segments.

## Other bars NOT changed

  - `.bar-fill` (dashboard THEMERRDB MOVIES / TV stat bars):
    set once on page load, not poll-driven. The 0.6s ease
    one-time fill animation reads cleanly.
  - `.coverage-bar-seg`: ditto, `var(--motion-slow)` for the
    one-time render.

Only the OPS DRAWER bars are poll-driven and need the cadence
match.

## Tests
"""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
OPS_CSS = REPO / "app" / "web" / "static" / "ops.css"
APP_CSS = REPO / "app" / "web" / "static" / "app.css"


def _strip_comments(src: str) -> str:
    return re.sub(r"/\*.*?\*/", "", src, flags=re.DOTALL)


def test_op_mini_bar_fill_uses_linear_1s():
    """topbar mini-bar transition must match the 1s poll cadence
    with linear easing for continuous motion."""
    src = _strip_comments(OPS_CSS.read_text())
    rule_idx = src.index(".op-mini .op-mini-bar-fill {")
    block = src[rule_idx:rule_idx + 800]
    assert "transition: width 1s linear" in block, (
        "v1.15.136: .op-mini-bar-fill transition must be "
        "`width 1s linear` so the bar advances at constant "
        "velocity in sync with the 1s /api/progress poll cadence."
    )


def test_op_card_bar_fill_uses_linear_1s():
    """drawer per-op card bar must match the same cadence."""
    src = _strip_comments(OPS_CSS.read_text())
    rule_idx = src.index(".op-card-bar-fill {")
    block = src[rule_idx:rule_idx + 400]
    assert "transition: width 1s linear" in block, (
        "v1.15.136: .op-card-bar-fill must move in lockstep with "
        "the topbar mini-bar — both feed from the same poll."
    )


def test_old_cubic_bezier_transitions_gone():
    """The pre-fix 0.6s cubic-bezier(0.4, 0, 0.2, 1) decelerating
    curve must be gone from the ops bars — it created the
    accelerate→decelerate→accelerate pattern the user saw as
    'jumpy'."""
    src = _strip_comments(OPS_CSS.read_text())
    # Both bar-fill rules together.
    for rule in (".op-mini .op-mini-bar-fill {", ".op-card-bar-fill {"):
        idx = src.index(rule)
        block = src[idx:idx + 800]
        assert "0.6s cubic-bezier" not in block, (
            f"v1.15.136: pre-fix 0.6s cubic-bezier transition "
            f"survives in {rule!r}. Should be `1s linear` to "
            "match poll cadence."
        )


def test_dashboard_bar_fill_unchanged():
    """The dashboard .bar-fill (THEMERRDB MOVIES / TV stat bars)
    is set once on page load, not poll-driven. Its 0.6s ease
    one-time fill stays — the fix is scoped to the ops bars."""
    src = _strip_comments(APP_CSS.read_text())
    rule_idx = src.index(".bar-fill {")
    block = src[rule_idx:rule_idx + 400]
    assert "transition: width 0.6s ease" in block, (
        "v1.15.136: dashboard .bar-fill should keep its one-time "
        "fill animation — the cadence-match fix is scoped to the "
        "poll-driven ops bars only."
    )
