"""v1.21.1 — // ACK / × CANCEL align under the ACTION header on wide panes.

The LOGS // JOBS grid renders the column header (.jobs-grid-header) as a
SIBLING of the vertical scroll container (.jobs-scroll-y), with the data
rows INSIDE it. Both use the same 7-col grid with a flexible
minmax(280px,1fr) NOTE column and a fixed 130px right-anchored ACTION
column. When the failed-jobs list overflows 700px, the body's custom 10px
::-webkit-scrollbar steals width from the data rows, so their 1fr NOTE
column ends up 10px narrower than the header's → the right-anchored ACTION
track (and the v1.20.45-centered // ACK button under it) sits 10px left of
the ACTION label. The 1fr only flexes wide enough to expose the gap on a
large monitor (the user 2026-06-01).

Fix: reserve the 10px scrollbar gutter on the body ALWAYS
(scrollbar-gutter: stable, so width is constant scrolled-or-not) and
reserve the matching 10px on the header's right padding, so both column
areas end at the same x.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def _rule(selector: str) -> str:
    i = CSS.index(selector + " {")
    return CSS[i:CSS.index("}", i)]


def test_body_reserves_scrollbar_gutter():
    rule = _rule(".jobs-scroll-y")
    assert "scrollbar-gutter: stable;" in rule, (
        "the jobs body must reserve the 10px scrollbar gutter ALWAYS so its "
        "width is constant whether or not the list overflows"
    )


def test_header_reserves_matching_gutter():
    """v1.21.3 (corrects v1.21.1): the header reserves the SAME gutter as
    the body via scrollbar-gutter:stable (auto-matches the real scrollbar
    width), NOT a hardcoded padding-right. The v1.21.1 `padding-right:
    calc(18px+10px)` assumed a 10px scrollbar — wrong for overlay (0px)
    and wider classic scrollbars. Verified delta=0 via browser preview."""
    rule = _rule(".jobs-grid-header")
    assert "scrollbar-gutter: stable;" in rule, (
        "the header must reserve the same gutter as the body (auto-matched)"
    )
    assert "overflow-x: hidden;" in rule, (
        "overflow-x:hidden keeps the header from spawning its own h-scroll "
        "(parent .jobs-scroll-x owns horizontal)"
    )
    # The wrong hardcoded hack must be gone.
    assert "padding-right: calc(18px + 10px);" not in rule, (
        "the hardcoded +10px hack was width-specific and wrong — removed"
    )


def test_v1_20_45_action_centering_still_present():
    """The v1.20.45 ACTION-column centering must stay — it centers the
    label + button within the (now-aligned) 130px track."""
    assert ".jobs-grid-header > span:last-child," in CSS
    i = CSS.index(".jobs-grid-header > span:last-child,")
    assert "text-align: center;" in CSS[i:i + 200]


def test_v1_21_1_version():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
