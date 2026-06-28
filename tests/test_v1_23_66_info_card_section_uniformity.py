"""v1.23.66 — INFO-card section dividers share one spacing rhythm.

The card stacks .dlg-section (plain metadata sections) alongside three special
panels — .diff-section (THEMERRDB MATCH), .recovery-section (TRY THIS NEXT),
.history-section (HISTORY). The three special panels hardcoded an 18px/14px
divider + 10px head margin (pre-token drift the v1.15.114 migration skipped
because the values match no gap token), so they sat ~2px tighter than the
.dlg-section peers in the same card. This pins all three to the canonical
.dlg-section tokens so the rhythm can't drift apart again.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def _rule(selector: str) -> str:
    # return the declaration block for an exact `selector {` rule, with CSS
    # comments stripped (the breadcrumbs mention the old px values).
    m = re.search(re.escape(selector) + r"\s*\{([^}]*)\}", CSS)
    assert m, f"rule {selector} not found in app.css"
    return re.sub(r"/\*.*?\*/", "", m.group(1), flags=re.DOTALL)


def test_canonical_dlg_section_uses_gap_tokens():
    # the reference the others must match.
    block = _rule(".dlg-section")
    assert "margin-top: var(--gap-5)" in block
    assert "padding-top: var(--gap-4)" in block


def test_info_card_special_sections_match_canonical_divider():
    for sel in (".diff-section", ".recovery-section", ".history-section"):
        block = _rule(sel)
        assert "margin-top: var(--gap-5)" in block, (
            f"v1.23.66: {sel} divider must match .dlg-section (--gap-5)"
        )
        assert "padding-top: var(--gap-4)" in block, (
            f"v1.23.66: {sel} divider must match .dlg-section (--gap-4)"
        )
        # the old pre-token drift must not creep back.
        assert "18px" not in block and "14px" not in block, (
            f"v1.23.66: {sel} must not reintroduce the raw 18px/14px divider"
        )


def test_info_card_section_heads_match_canonical_header_margin():
    for sel in (".diff-section-head", ".recovery-section-head"):
        block = _rule(sel)
        assert "margin-bottom: var(--gap-2)" in block, (
            f"v1.23.66: {sel} bottom margin must match .dlg-section h4 (--gap-2)"
        )
        assert "10px" not in block, (
            f"v1.23.66: {sel} must not reintroduce the raw 10px head margin"
        )
