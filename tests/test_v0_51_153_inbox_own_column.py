"""v0.51.153 → v0.51.161 — where the INBOX pill lives in the topbar.

v0.51.153 gave INBOX its own topbar grid column (out of .topbar-status) so a
running-job op-mini couldn't push it out. v0.51.161 (the user's ask) moved it BACK
into .topbar-status, positioned between the IDLE status pill and // HELP — still
safe from op-mini push-out because op-mini renders to INBOX's LEFT (before IDLE) on
desktop and drops to a bottom strip on mobile, so it grows away from INBOX.

Desktop: the topbar grid is 3 columns again (brand · nav(1fr) · status). Mobile
(≤600px): row 1 is just brand · status ("brand status" / "nav nav").
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
BASE_HTML = (REPO / "app" / "web" / "templates" / "base.html").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def test_inbox_pill_is_inside_status_between_idle_and_help():
    # v0.51.161: the pill now lives INSIDE .topbar-status …
    assert 'id="topbar-inbox-badge"' in BASE_HTML
    i_status = BASE_HTML.index('id="topbar-status"')
    i_pill = BASE_HTML.index('id="topbar-inbox-badge"')
    i_help = BASE_HTML.index('id="help-toggle"')
    i_idle = BASE_HTML.index('id="op-status-idle"')
    # … after the status div opens, and ordered IDLE → INBOX → // HELP.
    assert i_status < i_pill < i_help
    assert i_idle < i_pill


def test_desktop_topbar_has_three_columns():
    rule = APP_CSS[APP_CSS.index(".topbar {"):]
    head = rule[:rule.index("}")]
    assert "grid-template-columns: auto 1fr auto;" in head
    # gap stays trimmed so the status cluster doesn't wrap the nav.
    assert "gap: var(--gap-5);" in head


def test_mobile_topbar_grid_has_no_inbox_area():
    # the ≤600px block is back to brand · status on row 1 (INBOX rides the cluster).
    assert 'grid-template-areas: "brand status" "nav nav";' in APP_CSS
    assert "grid-template-columns: minmax(0, auto) 1fr;" in APP_CSS
    # the standalone inbox grid-area is gone.
    assert "grid-area: inbox" not in APP_CSS
    assert '"brand inbox status"' not in APP_CSS
