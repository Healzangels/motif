"""v1.23.48 — help-feature fixes from the user's deploy screenshot.

Three regressions from v1.23.44/45:
  1. the // HELP toggle was a 4th child of the 3-column topbar grid
     (brand | nav | status), so .topbar-status (IDLE + logout) wrapped onto a
     new row on the LEFT. Fix: the toggle lives INSIDE .topbar-status.
  2. .help-mode-bar used `display: flex` unconditionally, which overrode the
     `hidden` attribute — the bar showed regardless of help state and the × could
     never hide it. Fix: gate on body.help-mode (covered in v1.23.44 test).
  3. the inline LEGEND was a full-width bar even collapsed ("takes up an entire
     row"). Fix: collapsed = a compact inline pill.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
BASE_HTML = (REPO / "app" / "web" / "templates" / "base.html").read_text()
LIBRARY_HTML = (REPO / "app" / "web" / "templates" / "library.html").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def test_help_toggle_lives_inside_topbar_status():
    """The toggle must be a child of .topbar-status so it doesn't push the
    status cluster (IDLE + logout) onto a wrapped grid row."""
    si = BASE_HTML.index('class="topbar-status"')
    se = BASE_HTML.index("</div>", si)
    cluster = BASE_HTML[si:se]
    assert 'id="help-toggle"' in cluster, "// HELP must sit inside .topbar-status"
    # and NOT between </nav> and the status div (the old 4th-grid-child spot).
    nav_end = BASE_HTML.index("</nav>")
    gap = BASE_HTML[nav_end:si]
    assert 'id="help-toggle"' not in gap


def test_help_mode_bar_no_longer_uses_hidden_attribute():
    """The `hidden` attribute is gone (it was dead under display:flex); the bar
    is purely body.help-mode-gated now."""
    i = BASE_HTML.index('id="help-mode-bar"')
    assert "hidden" not in BASE_HTML[i:i + 60]
    assert "body.help-mode .help-mode-bar { display: flex; }" in APP_CSS
    # applyHelpMode no longer pokes bar.hidden (the class is the source of truth).
    j = APP_JS.index("function applyHelpMode(on)")
    assert "bar.hidden" not in APP_JS[j:j + 320]


def test_legend_collapsed_is_a_compact_pill():
    """The legend toggle is a compact inline chip in the results header (next to
    NEEDS WORK); the decode panel drops below only when open. v1.23.53: the
    toggle carries the .chip class (border/padding/transparent come from there),
    so the pill rule is slim and no longer self-gates with display:none."""
    pill = APP_CSS[APP_CSS.index(".library-legend-pill {"):]
    pill = pill[:pill.index("}")]
    assert "display: none" not in pill, "no self-gate — reuses .chip, always shown"
    assert 'class="chip library-legend-pill"' in LIBRARY_HTML, "toggle reuses .chip"
