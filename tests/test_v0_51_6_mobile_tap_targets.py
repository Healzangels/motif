"""v0.51.6 — mobile tap targets bumped to ~30px (audit A6).

Several controls sat below the 24px WCAG-AA target / a comfortable thumb: the
topbar `?` help + `⏻` logout glyphs (~22px), the filter-drawer DL/PL/LINK pills
(20px, deliberately dense to fit six rows on desktop), and the SOURCE BREAKDOWN
per-donut ◀ ▶ / // HIDE controls (~10-14px, customize mode). All bumped to 30px
in the ≤600px phone block ONLY — the desktop layouts stay dense.

Verified in-browser at 375px: help-toggle 30×30, logout 30h, filter link-glyph +
state-pill 30h, no page overflow.
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


def test_topbar_glyph_controls_bumped():
    assert ".help-toggle, .topbar-logout { min-height: 30px; min-width: 30px; }" in MOBILE


def test_filter_drawer_pills_bumped():
    # the drawer-scoped override lifts every pill type to 30px (they share a row so
    # a uniform bump keeps them aligned); scoped to .pill-filter-drawer so inline
    # rows keep their density.
    assert ".pill-filter-drawer .pill-filter-row .link-glyph,\n" in MOBILE
    assert ".pill-filter-drawer .pill-filter-row .state-pill-btn," in MOBILE
    assert ".ed-pill-btn { height: 30px; }" in MOBILE


def test_source_pie_controls_bumped():
    assert ".source-pie-move, .source-pie-hide { min-height: 30px; min-width: 30px; }" in MOBILE


def test_desktop_densities_unchanged():
    # the base rules keep their tuned tight sizes — the bump is phone-only. Both the
    # help-toggle 22px and the filter-pill 20px base heights live before the ≤600px
    # block and must survive there.
    # v0.51.18 (audit #32): extract each RULE BODY before asserting. The old bare
    # substring asserts matched four unrelated rules ("height: 22px;" also lives on
    # .state-pill-btn / .ed-pill-btn / .link-glyph; "height: 20px;" substring-matched
    # .pill-filter-spacer's min-height) — a phantom guard (v1.18.81 class): dropping
    # either pinned density shipped green. Same scoped-extraction shape as
    # test_v1_23_76_help_question_glyph.
    pre = APP_CSS[:APP_CSS.index("@media (max-width: 600px) {")]
    help_rule = pre[pre.index(".help-toggle {"):]
    help_rule = help_rule[:help_rule.index("}")]
    assert "height: 22px;" in help_rule, (
        "base 22px .help-toggle height stays on desktop")
    # the v1.12.25 shared filter-pill density rule — anchor on its selector list.
    pill_anchor = pre.index(".pill-filter-row .pill-filter-clear,")
    pill_rule = pre[pill_anchor:]
    pill_rule = pill_rule[:pill_rule.index("}")]
    assert "height: 20px;" in pill_rule, (
        "base 20px filter-pill density stays on desktop")
