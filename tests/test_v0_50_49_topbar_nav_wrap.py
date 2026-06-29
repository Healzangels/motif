"""v0.50.49 — topbar nav no longer spills the status cluster / floats the tabs.

Two bugs the user hit at ~927px:
  1. The IDLE / // HELP / logout status cluster spilled off the clipped right edge.
     Cause: the topbar is a grid (auto | 1fr | auto = brand | nav | status); the
     nav's 7-tab min-content forced the grid wider than the viewport, pushing the
     status column off. Fix: .nav flex-wraps within its column + min-width:0 so it
     can shrink to its 1fr allocation instead of forcing overflow.
  2. "TV SHOWS" wrapped to two lines, which (flex align-items:stretch) top-aligned
     the single-line tabs so they floated up out of line. Fix: .nav a white-space:
     nowrap — every tab is one line; the nav wraps as a GROUP instead.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def test_nav_wraps_as_a_group_and_can_shrink():
    m = re.search(r"\n\.nav \{[^}]*\}", CSS)
    assert m, ".nav base rule not found"
    rule = m.group(0)
    assert "flex-wrap: wrap;" in rule
    assert "min-width: 0;" in rule


def test_nav_tabs_never_wrap_individually():
    m = re.search(r"\.nav a \{.*?\}", CSS, re.S)
    assert m and "white-space: nowrap;" in m.group(0)
