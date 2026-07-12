"""v0.51.137 — CSS-audit T4: library filter-drawer pill dimension drifts.

Two desktop drifts left over from the v1.12.48 "shrink filter pills 22->20px for
uniform row rhythm" pass:

  (1) LINK row 2px taller than the others. v1.12.48 added
      `.pill-filter-row .link-glyph { height: 20px }` to its uniform group, but
      the older v1.12.25 rule ALSO set `.pill-filter-row .link-glyph { height:
      22px }` — same selector, equal specificity, LATER in source → the stale
      22px won. Dropped so the 20px group governs.

  (2) Round DL/PL filter dots rendered as 22w×20h ovals. v1.12.48 shrank pill
      HEIGHT via the group but never the `.state-pill-btn` base WIDTH (still
      22px). Base shrunk 22->20 so the dots are 20×20 circles. state-pill-btn
      only ever renders inside .pill-filter-row (grep-verified), so 20px here is
      the real rendered size.

Harness-proven at a 1400px desktop viewport: LINK row 22->20px (matches DL/PL);
DL/PL dots oval->circle. The ≤600px phone tier (30px touch pills, a separate
higher-specificity rule) is unchanged.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
# strip CSS comments first — inline `# vX.Y.Z:` markers here mention the old
# values (and even a literal `{ height: 20px }`), which would defeat naive
# substring / brace-matching checks below.
APP_CSS = re.sub(r"/\*.*?\*/", "", (REPO / "app" / "web" / "static" / "app.css").read_text(), flags=re.S)


def _rule_body(selector_line: str) -> str:
    """Body of the first CSS rule whose opening line is `selector_line {`
    (comment-free CSS, so the first `}` is the real rule close)."""
    i = APP_CSS.index(selector_line + " {")
    return APP_CSS[i:APP_CSS.index("}", i)]


def test_state_pill_btn_base_is_a_20px_circle():
    body = _rule_body(".state-pill-btn")
    assert "width: 20px;" in body, "the round filter dot base must be 20px wide (was 22 → oval)"
    assert "height: 20px;" in body
    assert "width: 22px;" not in body and "height: 22px;" not in body
    assert "border-radius: 50%;" in body  # still a circle


def test_link_glyph_filter_rule_no_longer_pins_a_stale_height():
    # the v1.12.25 `.pill-filter-row .link-glyph` rule must NOT set height —
    # otherwise it re-wins over the v1.12.48 20px group and the LINK row grows.
    body = _rule_body(".pill-filter-row .link-glyph")
    assert "height:" not in body, (
        "v0.51.137: the .pill-filter-row .link-glyph rule must not set height — "
        "the v1.12.48 uniform 20px group owns it"
    )


def test_uniform_20px_group_still_governs_the_filter_rows():
    # the v1.12.48 group (the height authority for all six filter rows) intact.
    assert re.search(
        r"\.pill-filter-row \.link-glyph,\s*\.pill-filter-row \.ed-pill-btn \{[^}]*height: 20px;",
        APP_CSS,
    ), "the v1.12.48 uniform height:20px group must still cover .link-glyph"
