"""v0.51.153 — the INBOX pill gets its own topbar grid column.

The user: a running-job op-mini pill inflates the .topbar-status cluster and could
push the INBOX pill out of view. Fix: move the INBOX pill OUT of .topbar-status into
its own topbar grid column (before the cluster), so op-mini's growth can't reach it.

Desktop: the topbar grid goes 3→4 columns (brand · nav(1fr) · INBOX · status), gap
trimmed gap-7→gap-5 so the extra column doesn't squeeze the nav into wrapping. Mobile
(≤600px): INBOX joins row 1 via its own named area ("brand inbox status" / "nav nav
nav"). Verified in a topbar harness at desktop width (INBOX always visible, no
horizontal overflow, with op-mini running).
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
BASE_HTML = (REPO / "app" / "web" / "templates" / "base.html").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def test_inbox_pill_is_outside_and_before_topbar_status():
    # the pill still exists …
    assert 'id="topbar-inbox-badge"' in BASE_HTML
    i_pill = BASE_HTML.index('id="topbar-inbox-badge"')
    i_status = BASE_HTML.index('id="topbar-status"')
    # … and now precedes the status cluster in source order (its own grid child).
    assert i_pill < i_status
    # the pill's <button> opens before the status <div> opens — i.e. it is NOT
    # nested inside .topbar-status.
    pill_tag_open = BASE_HTML.rfind("<button", 0, i_pill)
    status_div_open = BASE_HTML.rfind("<div", 0, i_status)
    assert pill_tag_open < status_div_open


def test_desktop_topbar_has_four_columns():
    rule = APP_CSS[APP_CSS.index(".topbar {"):]
    head = rule[:rule.index("}")]
    assert "grid-template-columns: auto 1fr auto auto;" in head
    # gap trimmed so the 4th column doesn't wrap the nav.
    assert "gap: var(--gap-5);" in head


def test_mobile_topbar_grid_places_inbox():
    # the ≤600px block gives INBOX its own named area on row 1.
    assert 'grid-template-areas: "brand inbox status" "nav nav nav";' in APP_CSS
    assert "#topbar-inbox-badge { grid-area: inbox; }" in APP_CSS
    assert "grid-template-columns: minmax(0, auto) auto 1fr;" in APP_CSS
