"""v0.51.7 — settings action rows left-align their buttons after wrap on mobile.

Mobile audit finding B2 (P2): the settings .control-row action groups (DRY-RUN,
TVDB BRIDGE, REPROBE, PROBE, ORPHAN SCAN) use .control-actions { margin-left: auto
} to dock the buttons RIGHT on desktop. On a phone the flex-wrap:wrap row drops the
buttons to their own line — but margin-left:auto kept them right-anchored there,
visually detached from the label they belong to (desktop reads label-left /
buttons-right on one line; mobile should read label then buttons-under-it).

Fix: in the ≤600px block, .control-row .control-actions { margin-left: 0 } so the
buttons left-align under their label. The .control-row prefix (0,2,0) is REQUIRED —
the base .control-actions { margin-left: auto } (0,1,0) is defined LATER in the file
than the ≤600px block, so an equal-specificity override there would lose on source
order (the classic mobile-override-before-base-rule trap). Verified in-browser at
375px: marginLeft 0, actions x == row x (left-aligned), wrapped to a 2nd line.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def _mobile_block(css: str) -> str:
    i = css.index("@media (max-width: 600px) {")
    depth = 0
    j = i
    while True:
        if css[j] == "{":
            depth += 1
        elif css[j] == "}":
            depth -= 1
            if depth == 0:
                return css[i:j + 1]
        j += 1


MOBILE = _mobile_block(APP_CSS)


def test_control_actions_left_aligned_on_mobile():
    # v0.51.39: + flex-wrap so the one 2-button row (DRY-RUN) wraps rather than
    # forcing a page-wide overflow; still margin-left:0 (LEFT-aligned under label,
    # not centred) — see test_v0_51_39_mobile_button_overflow.
    assert ".control-row .control-actions { margin-left: 0; flex-wrap: wrap; }" in MOBILE


def test_override_outspecifies_the_base_rule():
    # the base right-anchor is defined AFTER the mobile block, so the override must
    # carry the .control-row prefix to win on specificity (not source order). Anchor
    # the base rule on its column-0 form (the mobile override is indented + prefixed).
    base_idx = APP_CSS.index("\n.control-actions {")
    mobile_idx = APP_CSS.index("@media (max-width: 600px) {")
    assert base_idx > mobile_idx, (
        "base .control-actions rule is expected AFTER the mobile block — if it "
        "moves before, a bare .control-actions override would suffice"
    )
    assert "margin-left: auto;" in APP_CSS[base_idx:base_idx + 120]


def test_desktop_keeps_right_anchor():
    # base rule (column-0, outside the mobile block) still right-docks on desktop.
    base = APP_CSS[APP_CSS.index("\n.control-actions {"):]
    base = base[:base.index("}")]
    assert "margin-left: auto;" in base
