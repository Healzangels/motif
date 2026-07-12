"""v0.51.24 — mobile card overflow: dash-pair tables + SOURCE BREAKDOWN labels.

the user (mobile screenshot, ~549px): "the per section coverage and the general
statistics in mobile view are spilling out of bounds of their sections, can we
also do an audit to make sure no other sections in mobile view are doing this".

Root cause (measured live at 375px):
  * .dash-pair is a flex 2-up (v1.24.66); on mobile the two .dash-pair-col cards
    kept a calc(50%) basis and sat 2-up at ~152px each, and each 6-7-col compact
    table (476-599px of nowrap cells) spilled hundreds of px past its card —
    dropping min-width alone (the old v0.50.48 rule) never actually stacked them
    because a flex item won't shrink below its content's min-content. Fix: plain
    block flow on a phone (one full-width card per row) + the card as its own
    overflow-x scroll context so the wide table scrolls inside it.
  * the audit also caught SOURCE BREAKDOWN: the v0.51.6 30px touch bump on the
    per-donut ◀ ▶ / // HIDE controls made the label's control cluster ~122px,
    which overflowed the ~150px 2-up donut column by ~25px. Fix: let the label
    wrap the controls under the name.

The SOURCE BREAKDOWN label fix is scoped to the @media (max-width: 600px) phone
tier. The dash-pair stack+swipe was later WIDENED (v0.51.136) to
@media (max-width: 1200px) — the same tables spilled their cards all the way
through ~1130px, not only on phones (≤600 ⊂ ≤1200, so the phone behaviour this
test guards is unchanged). Desktop (≥1201px) keeps the v1.24.66/69/77 flex 2-up
equal-height layout.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def _mobile_block() -> str:
    """The @media (max-width: 600px) { ... } block body (brace-matched)."""
    i = APP_CSS.index("@media (max-width: 600px) {")
    j = i + APP_CSS[i:].index("{")
    depth = 0
    for k in range(j, len(APP_CSS)):
        c = APP_CSS[k]
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return APP_CSS[j : k + 1]
    raise AssertionError("unterminated @media (max-width: 600px) block")


MOBILE = _mobile_block()


def _tablet_block() -> str:
    """The @media (max-width: 1200px) { ... } block body — where v0.51.136 moved
    the dash-pair stack+swipe (still applies at phone width: ≤600 ⊂ ≤1200)."""
    i = APP_CSS.index("@media (max-width: 1200px) {")
    j = i + APP_CSS[i:].index("{")
    depth = 0
    for k in range(j, len(APP_CSS)):
        c = APP_CSS[k]
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return APP_CSS[j : k + 1]
    raise AssertionError("unterminated @media (max-width: 1200px) block")


TABLET = _tablet_block()


def test_dash_pair_stacks_to_block_on_mobile():
    # v0.51.136: the stack lives in the ≤1200 block. v0.51.140: scoped to the
    # tables pair (.dash-pair-tables); the insight pair stacks at ≤600 instead.
    assert re.search(r"\.dash-pair-tables\s*\{[^}]*display:\s*block", TABLET), (
        "v0.51.24/136/140: the STATISTICS tables pair must drop to block flow "
        "(stack) for narrow viewports (≤1200px, which includes the phone tier)")


def test_dash_pair_col_is_a_horizontal_scroll_context_on_mobile():
    # v0.51.136: the col becomes its own swipe context in the ≤1200 block.
    bodies = re.findall(r"\.dash-pair-col\s*\{([^}]*)\}", TABLET)
    assert any("display: block" in b and "overflow-x: auto" in b for b in bodies), (
        "v0.51.24/136: a .dash-pair-col rule must set display:block (not "
        "flex, so it respects width) AND overflow-x:auto (its own scroll "
        "context so the wide table scrolls inside the card instead of spilling)")


def test_dash_pair_table_keeps_a_readable_min_width_floor():
    assert re.search(r"\.dash-pair-col\s*>\s*\.table\s*\{[^}]*min-width:\s*480px",
                     TABLET), (
        "v0.51.24/136: the compact table needs a min-width floor so its columns "
        "stay readable and it scrolls rather than crushing")


def test_source_pie_label_wraps_controls_on_mobile():
    assert re.search(r"\.source-pie-col-label\s*\{[^}]*flex-wrap:\s*wrap",
                     MOBILE), (
        "v0.51.24: the SOURCE BREAKDOWN donut label must wrap so the 30px touch "
        "controls drop below the name instead of overflowing the 2-up column")


def test_desktop_base_dash_pair_stays_flex_2up():
    # the fix must NOT touch the base (desktop) rule — equal-height 2-up (v1.24.77).
    base = APP_CSS[: APP_CSS.index("@media (max-width: 600px) {")]
    assert re.search(r"\.dash-pair\s*\{[^}]*display:\s*flex", base), (
        "the base .dash-pair must remain flex for the desktop 2-up layout")
    assert ".dash-pair-col > .table { flex: 1 1 auto; }" in base, (
        "the desktop equal-height rule (v1.24.77) must be untouched")
