"""v0.51.140 — scope the ≤1200 dash-pair stack to the tables pair only.

v0.51.136 stacked BOTH dashboard .dash-pair blocks at ≤1200px. But only the
STATISTICS pair (PER-SECTION COVERAGE | GENERAL STATISTICS) has the table-
overflow that motivated it; the SYNC & DOWNLOADS insight-chart pair renders
width:100% SVGs with no overflow, and v0.51.45 deliberately put those two short
sparklines 2-up. So the ≤1200 stack was over-broad — it restacked the charts at
601-1200px too.

Fix: mark the STATISTICS wrapper `.dash-pair-tables`, scope the ≤1200 stack to
it, and give the insight pair (`.dash-pair:not(.dash-pair-tables)`) its 2-up
back down to 600px (it stacks at the phone tier like every other card).
Harness-verified: at 900px the tables stack while the insight charts stay 2-up;
at 375px both stack.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
DASH = (REPO / "app" / "web" / "templates" / "dashboard.html").read_text()


def _media_block(opener: str) -> str:
    i = APP_CSS.index(opener)
    depth, j = 0, i + APP_CSS[i:].index("{")
    for k in range(j, len(APP_CSS)):
        if APP_CSS[k] == "{":
            depth += 1
        elif APP_CSS[k] == "}":
            depth -= 1
            if depth == 0:
                return APP_CSS[j:k + 1]
    raise AssertionError(f"unterminated {opener}")


TABLET = _media_block("@media (max-width: 1200px) {")
PHONE = _media_block("@media (max-width: 600px) {")


def test_statistics_wrapper_carries_the_tables_class():
    # the ≤1200 scope hinges on this class being on the STATISTICS .dash-pair.
    i = DASH.index('data-dash-section="section-coverage"')
    tag = DASH[DASH.rfind("<div", 0, i):i]
    assert "dash-pair-tables" in tag, "STATISTICS wrapper must carry .dash-pair-tables"


def test_1200_stack_is_scoped_to_tables():
    assert ".dash-pair-tables { display: block; }" in TABLET
    # and NOT the bare all-pairs form that would re-catch the insight charts.
    assert ".dash-pair { display: block; }" not in TABLET


def test_insight_pair_stacks_only_at_the_phone_tier():
    # the non-tables pair (insight charts) gets its stack in the ≤600 block.
    assert ".dash-pair:not(.dash-pair-tables) { display: block; }" in PHONE
    assert ".dash-pair:not(.dash-pair-tables) > .dash-pair-col" in PHONE


def test_base_dash_pair_stays_flex_for_both_pairs():
    base = APP_CSS[APP_CSS.index(".dash-pair {\n"):]
    base = base[:base.index("}")]
    assert "display: flex;" in base
