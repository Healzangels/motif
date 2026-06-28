"""v1.20.31 — bulk-bar overflow detection uses getBoundingClientRect.

the user (on select-all): "selecting all is making selections spill outside
of borders, after a little bit it fixes itself." The bulk-action buttons
painted past the bar's right border, then snapped into the // MORE ▾ menu
once a later layout pass ran.

Root cause: _layoutBulkBar detected overflow with
`bar.scrollWidth <= bar.clientWidth`. But v1.17.21 set the bar to
`overflow: visible` (so the dropdown panel can escape downward), and on
an overflow:visible flex row a horizontally-overflowing child doesn't
reliably grow scrollWidth — so the check intermittently reported "fits"
while buttons spilled, and only a later rAF / ResizeObserver pass caught
it. v1.18.44's max-width/min-width + rAF reduced but didn't eliminate it.

Fix: measure the rightmost visible child's painted edge against the bar's
inner content edge via getBoundingClientRect (accurate regardless of the
overflow property; the read forces a synchronous reflow so it's never
stale). Both the early-return check and the per-button loop check now use
this helper.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def test_overflow_helper_defined():
    assert "function _barHasOverflow(" in JS


def test_overflow_helper_uses_bounding_rect_not_scrollwidth():
    """The helper measures painted edges, not scrollWidth — the whole
    point is to be reliable on an overflow:visible flex row."""
    anchor = JS.index("function _barHasOverflow(")
    body = JS[anchor:anchor + 700]
    assert "getBoundingClientRect()" in body
    assert "scrollWidth" not in body, (
        "v1.20.31: the reliable helper must NOT fall back to scrollWidth"
    )
    # skips hidden children (they don't paint, can't overflow).
    assert "display === 'none'" in body


def test_layout_uses_helper_for_both_checks():
    """_layoutBulkBar's early-return AND per-button loop must both use
    the new helper, and the old scrollWidth comparison must be gone."""
    anchor = JS.index("function _layoutBulkBar()")
    body = JS[anchor:anchor + 2200]
    assert body.count("_barHasOverflow(bar)") >= 2, (
        "both the early-return and the loop must use the helper"
    )
    assert "bar.scrollWidth <= bar.clientWidth" not in body, (
        "v1.20.31: the unreliable scrollWidth check must be removed"
    )


def test_raf_backstop_preserved():
    """The v1.18.44 rAF follow-up stays as a backstop — the helper makes
    the synchronous pass reliable, but a post-paint pass is cheap
    insurance and must not regress."""
    assert "requestAnimationFrame(_layoutBulkBar)" in JS


def test_v1_20_31_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
