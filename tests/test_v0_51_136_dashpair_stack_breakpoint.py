"""v0.51.136 — CSS-audit T2: dashboard STATISTICS 2-up stops spilling 601-1200px.

The STATISTICS .dash-pair (PER-SECTION COVERAGE | GENERAL STATISTICS) sits 2-up
on desktop, but each 6-7-col compact table's min-content is ~532px. A flex item
won't shrink below its content's min-content, so below ~1132px the flex cols
(min-width:340) couldn't give each table its width and the wider COVERAGE table
spilled ~115px past its card — hidden by body{overflow-x:hidden}. The v0.51.24
stack fix only covered ≤600px, leaving the whole 601-1130px band broken.

Fix: the dash-pair stack+swipe rules MOVED from the ≤600 block into a new
@media (max-width: 1200px) block (≤600 ⊂ ≤1200, so phone behaviour is unchanged).
Harness-proven: 900px & 1080px went from 2-up-with-spill to clean stack (no
page overflow); 1250px keeps the 2-up equal-height layout; 375px still stacks.
Breakpoint 1200px = the measured ~1132px 2-up-fit threshold + margin for longer
Plex library names, and preserves 2-up for standard desktop widths (≥1280).
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def _media_block(css: str, opener: str) -> str:
    i = css.index(opener)
    depth = 0
    j = i
    while True:
        if css[j] == "{":
            depth += 1
        elif css[j] == "}":
            depth -= 1
            if depth == 0:
                return css[i:j + 1]
        j += 1


TABLET = _media_block(APP_CSS, "@media (max-width: 1200px) {")
PHONE = _media_block(APP_CSS, "@media (max-width: 600px) {")


def test_dashpair_stack_lives_in_the_1200_block():
    # the stack + per-card swipe now triggers at ≤1200px, not ≤600px.
    assert ".dash-pair { display: block; }" in TABLET
    assert "overflow-x: auto" in TABLET and ".dash-pair-col" in TABLET
    assert ".dash-pair-col > .table { min-width: 480px; }" in TABLET


def test_dashpair_stack_removed_from_the_600_block():
    # guard against the mirror-drift class: the stack rules must live in ONE place
    # (≤1200), not be duplicated back into the ≤600 block.
    assert ".dash-pair { display: block; }" not in PHONE
    assert ".dash-pair-col > .table { min-width: 480px; }" not in PHONE


def test_desktop_2up_layout_preserved():
    # the base .dash-pair keeps flex (the v1.24.66/69/77 equal-height 2-up) for
    # ≥1201px — the ≤1200 override only kicks in below the breakpoint.
    base = APP_CSS[APP_CSS.index(".dash-pair {\n"):]
    base = base[:base.index("}")]
    assert "display: flex;" in base
