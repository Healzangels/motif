"""v1.24.33 — RP rows get a distinct, sortable 'needs re-push' attention glyph.

the user: a stale-plex_upload (RP) row showed "the yellow ! at the start of the
row but no way to sort by it. it looks similar to an ATN icon so is a bit
confusing." Root cause: an RP row reads as downloaded-but-unplaced (v1.24.28
nulls its placement), so it tripped the generic amber `!` "awaiting placement"
title glyph — visually identical to an ATTN signal, RP-undistinguished.

Fix: a dedicated orange ⟳ title glyph (matching the RP LINK badge) checked
BEFORE awaitingApproval, that LINKS to the link_pills=rp filter — one click
surfaces every row needing a re-push, so they're both unambiguous AND findable.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def test_repush_glyph_branch_exists():
    assert "it.needs_repush" in APP_JS
    i = APP_JS.index("} else if (it.needs_repush) {")
    branch = APP_JS[i:i + 700]
    assert "title-glyph-repush" in branch
    assert "link_pills=rp" in branch, "the glyph must link to the RP filter"
    assert "⟳" in branch


def test_repush_glyph_wins_over_generic_await():
    # The needs_repush branch must precede the awaitingApproval branch so an RP
    # row gets the distinct ⟳, not the ambiguous amber ! it shared before.
    rp_idx = APP_JS.index("} else if (it.needs_repush) {")
    await_idx = APP_JS.index("} else if (awaitingApproval) {")
    assert rp_idx < await_idx


def test_repush_glyph_css_is_distinct_orange():
    assert ".title-glyph-repush" in APP_CSS
    i = APP_CSS.index(".title-glyph-repush")
    rule = APP_CSS[i:i + 160]
    assert "var(--orange)" in rule, "RP glyph is orange (matches the RP badge)"
