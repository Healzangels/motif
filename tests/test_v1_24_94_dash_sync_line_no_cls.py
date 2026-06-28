"""v1.24.94 — dashboard sync line no longer shifts the page on async load.

The "Next sync … · Last run … · no changes" line (#dash-sync-line) is populated
async from /api/stats. It was display:none until then, so revealing it pushed the
whole dashboard down — a jarring jump on navigation (the user: "it shifts all the
dashboards down which looks weird"). Fix: start it visibility:hidden with a
reserved min-height (one line), so the row holds its space from first paint; JS
flips it visible once filled — no layout shift.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DASH = (REPO / "app" / "web" / "templates" / "dashboard.html").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def test_sync_line_uses_visibility_not_display_none():
    i = DASH.index('id="dash-sync-line"')
    tag = DASH[i - 80:i + 80]
    assert 'visibility:hidden' in tag, "sync line must reserve space (visibility), not display:none"
    assert 'display:none' not in tag, "display:none re-introduces the layout shift"


def test_sync_line_reserves_min_height():
    _s = APP_CSS.index(".dash-sync-line {")
    block = APP_CSS[_s:APP_CSS.index("}", _s) + 1]
    assert "min-height" in block, "the reserved row needs a min-height or it collapses to 0"


def test_js_reveals_via_visibility():
    fn = APP_JS[APP_JS.index("function renderDashSyncLine("):]
    fn = fn[:fn.index("\n  }") + 4]
    assert "line.style.visibility = 'visible'" in fn
    assert "line.style.display = ''" not in fn
