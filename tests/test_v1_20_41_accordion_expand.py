"""v1.20.41 — drawer cards expand as an in-place accordion.

The end of the drawer-expand saga. After widen-left, focus-mode, and a
floating flyout, the user: "I want the actual card to expand like in my
picture." The geometric reality: a right-pinned fixed-width panel has no
room to grow a card LEFTWARD without widening the whole panel (which read
as "everything expands"). So — confirmed with the user — the card expands
DOWNWARD instead: clicking a card grows IT in place, the run log renders
inside it below its own content, the cards below shift down, and nothing
resizes sideways or floats.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
OPS_JS = (REPO / "app" / "web" / "static" / "ops.js").read_text()
OPS_CSS = (REPO / "app" / "web" / "static" / "ops.css").read_text()
BASE_HTML = (REPO / "app" / "web" / "templates" / "base.html").read_text()


# ── no floating panel / no widening anywhere ─────────────────


def test_no_flyout_remnants():
    assert "ops-run-flyout" not in OPS_JS
    assert "ops-run-flyout" not in OPS_CSS
    assert "ops-run-flyout" not in BASE_HTML
    assert "updateRunFlyout" not in OPS_JS


def test_no_panel_widening():
    assert "ops-drawer-wide" not in OPS_CSS
    assert "ops-drawer-wide" not in OPS_JS
    assert "width: 720px" not in OPS_CSS


# ── run log renders inside the card, below its content ───────


def test_run_log_renders_in_card():
    assert ('isExpanded ? `<div class="op-card-detail">'
            '${renderExpandedDetail(op)}</div>') in OPS_JS


def test_detail_is_a_stacked_section_below_content():
    """.op-card-detail is the card's accordion section — a top rule +
    top margin separate it from the card content above."""
    anchor = OPS_CSS.index(".op-card-detail {")
    block = OPS_CSS[anchor:anchor + 160]
    assert "margin-top:" in block
    assert "border-top:" in block
    # not a side column / floating panel.
    assert "position: fixed" not in block
    assert "border-right" not in block


def test_events_rerender_the_card_not_a_flyout():
    """When the expanded op's events land, the card re-renders (the run
    log is part of the card now)."""
    anchor = OPS_JS.index("state.expandedEvents[opId] = data.events")
    block = OPS_JS[anchor:anchor + 450]
    assert "renderDrawerBody(state.ops)" in block


# ── multi-expand: independent per-card unfurl (v1.21.28) ──────


def test_multi_expand_independent():
    # v1.21.28: multiple cards unfurl at once — a Set tracks every open
    # card, and toggling one no longer collapses the others.
    assert "state.expandedOpIds.has(op.op_id)" in OPS_JS
    tog = OPS_JS.index("function toggleExpand(")
    body = OPS_JS[tog:tog + 600]
    assert "state.expandedOpIds.add(opId)" in body
    assert "state.expandedOpIds.delete(opId)" in body
    # still re-renders the body; no flyout call.
    assert "renderDrawerBody(state.ops)" in body


def test_v1_20_41_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
