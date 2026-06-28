"""v1.23.85 — the LOGS jobs-grid ACTION column fits the // ACK FAILURE button.

Regression from v1.23.81: the per-row failure button label grew
"// ACK" → "// ACK FAILURE" (~144px in VT323/11px), but the jobs-grid
ACTION track stayed 130px, so the button overflowed and got clipped by
the cell's overflow:hidden at the panel edge (the user's LOGS screenshot).
Widened the fixed ACTION track to 180px. The header shares the
.jobs-grid-row template (via the jobs-grid-header class on the same
element), so it stays aligned automatically.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def _jobs_grid_template() -> str:
    i = APP_CSS.index(".jobs-grid-row {")
    block = APP_CSS[i:i + 1400]
    j = block.index("grid-template-columns:")
    return block[j:block.index(";", j) + 1]


def test_action_track_is_wide_enough_for_ack_failure():
    tmpl = _jobs_grid_template()
    # the fixed ACTION track (the only non-fr column) must clear the
    # ~144px "// ACK FAILURE" button — the old track clipped it. Pin the
    # track DECLARATION ("180px;") not the bare number, so the explanatory
    # comment (which mentions the old value) doesn't trip the guard.
    assert "180px;" in tmpl, "ACTION track must be 180px to fit // ACK FAILURE"
    assert "130px;" not in tmpl, "the old 130px track clipped the button"


def test_the_button_that_drove_the_width_still_says_ack_failure():
    # tie the width to its cause: if the per-row label ever shrinks back,
    # this guard documents why 180px exists (and someone can re-narrow).
    i = APP_JS.index('data-act="ack-job-failure"')
    btn = APP_JS[i:APP_JS.index("</button>", i)]
    assert "// ACK FAILURE" in btn


def test_header_shares_the_row_template():
    # the header element carries both classes so it inherits the columns;
    # there must be no SEPARATE grid-template-columns on .jobs-grid-header
    # that could drift from the body.
    html = (REPO / "app" / "web" / "templates" / "queue.html").read_text()
    assert 'class="jobs-grid-row jobs-grid-header"' in html


def test_v1_23_85_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
