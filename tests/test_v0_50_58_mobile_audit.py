"""v0.50.58 — mobile-arc (v0.50.48..54) silent-bug audit follow-ups.

1. .table-scroll overflow scoped to <=1080 (the dual-axis clip box that clipped
   the desktop row dropdowns is gone above 1080) — pinned in test_v0_50_48.
2. The mobile nav + settings-tabs scroll strips (overflow-x:auto, pinned left)
   hid the active tab off-screen-right. Both now scrollIntoView the active item
   (no-op on desktop). These pin those calls.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def test_settings_tabs_scroll_active_into_view():
    assert "active.scrollIntoView({ block: 'nearest', inline: 'nearest' })" in APP_JS


def test_nav_active_scrolls_into_view():
    assert "a.scrollIntoView({ block: 'nearest', inline: 'nearest' })" in APP_JS


def test_table_scroll_overflow_not_a_base_rule():
    # the overflow lives only inside the <=1080 media block; no base .table-scroll
    # rule sets overflow-x (that base rule clipped desktop row menus).
    base_region = CSS[:CSS.index("@media (max-width: 1080px)")]
    assert ".table-scroll { overflow-x: auto" not in base_region
