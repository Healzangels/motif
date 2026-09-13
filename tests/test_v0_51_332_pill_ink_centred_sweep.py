"""v0.51.332: pill ink centred — the whole family.

The operator: "audit all our pills and guards etc to make sure they're
all properly centered". Measured in the browser (ink vs box, both axes,
every page plus the INFO card and the glossary): every TRACKED family
carried the v0.51.331 defect — letter-spacing is laid after the last
glyph too, so a centred text box sits its ink half a tracking unit left
of centre. The fix is one rule per family: right padding = left padding
− letter-spacing. This test parses each rule and asserts that INVARIANT
(left is read from the rule, not pinned), so a retuned tracking that
forgets the padding, or a padding put back symmetric, fails here.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from _slice_helpers import slice_between

REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
OPS_CSS = (REPO / "app" / "web" / "static" / "ops.css").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def _block(css: str, selector: str) -> str:
    return slice_between(css, f"\n{selector} {{", "\n}")


def _decl(block: str, prop: str) -> str | None:
    m = re.search(rf"^\s*{re.escape(prop)}:\s*([^;]+);", block, re.M)
    return m.group(1).strip() if m else None


def _padding4(block: str) -> list[str]:
    val = _decl(block, "padding")
    assert val, "padding not declared"
    parts = re.split(r"\s+(?![^()]*\))", val)
    assert len(parts) == 4, f"expected a 4-value padding, got {val!r}"
    return parts


# (selector, letter-spacing the rule relies on when it does not declare one)
TRACKED = [
    (".btn", None),
    (".btn-tiny", "0.15em"),          # inherits .btn's tracking
    (".row-info-btn", "0.15em"),      # a .btn.btn-tiny
    (".chip", None),
    ('.chips[aria-label="section"] .chip', "0.15em"),  # a .chip
    (".tab", None),
    (".tdb-pill", None),
    (".loudness-pill", None),
    (".attn-pill", None),
    (".pill-filter-clear", None),
    (".pill-filter-row .link-glyph", None),
    (".ed-pill-btn", None),
    (".info-scope-chip", None),
    (".library-clear-all-btn", None),
    (".tier-badge", None),
    (".pill", None),
    (".lib-flag-pill", None),
    (".edition-pill", None),
    (".form-env-badge", None),
    (".sync-hist-status", None),
    (".dash-section-toggle", None),
    (".dash-card-toggle", None),
    (".library-filter-toggle", None),
]


# glyph-only labels whose glyph sits off the box centre: vertical padding
# moved, never grown — (top, bottom). The Ⓘ button is block-flow, so weight
# on TOP moves its glyph down; flex pills take weight on the BOTTOM to rise.
GLYPH_NUDGED = {".row-info-btn": ("5px", "3px")}


@pytest.mark.parametrize("selector,inherited", TRACKED, ids=[t[0] for t in TRACKED])
def test_right_padding_gives_the_trailing_tracking_back(selector, inherited):
    block = _block(APP_CSS, selector)
    ls = _decl(block, "letter-spacing")
    if inherited is None:
        assert ls, f"{selector} must declare the letter-spacing its padding compensates"
    else:
        assert ls is None, f"{selector} now declares its own tracking — retune the table"
        ls = inherited
    top, right, bottom, left = _padding4(block)
    if selector in GLYPH_NUDGED:
        assert (top, bottom) == GLYPH_NUDGED[selector], (selector, top, bottom)
    else:
        assert top == bottom, (selector, top, bottom)
    assert right == f"calc({left} - {ls})", (selector, right, left, ls)


def test_topbar_inbox_label_compensates_on_itself():
    block = _block(OPS_CSS, ".op-pill .op-pill-label")
    ls = _decl(block, "letter-spacing")
    assert ls and _decl(block, "margin-right") == f"-{ls}", block


def _side_paddings(block: str) -> tuple[str | None, str | None]:
    left, right = _decl(block, "padding-left"), _decl(block, "padding-right")
    if left is None and right is None:
        parts = re.split(r"\s+(?![^()]*\))", _decl(block, "padding") or "")
        right = parts[1] if len(parts) > 1 else parts[0]
        left = parts[3] if len(parts) == 4 else right
    return left, right


# lone-glyph labels keep symmetric padding, so the tracking itself goes (the .topbar-logout idiom)
LONE_GLYPH = [".topbar-logout", ".loud-stepper .btn-tiny"]


@pytest.mark.parametrize("selector", LONE_GLYPH)
def test_lone_glyph_buttons_have_no_tracking(selector):
    block = _block(APP_CSS, selector)
    # v0.51.339: a one-line rule has no "\n}" of its own — the slice ran into later rules and read their letter-spacing
    assert block.count("{") == 1 and "}" not in block, f"{selector} must be its own multi-line rule:\n{block[:400]}"
    left, right = _side_paddings(block)
    assert left and left == right, (selector, left, right)
    assert _decl(block, "letter-spacing") == "0", block


def test_loud_stepper_labels_are_lone_glyphs():
    labels = re.findall(r'data-act="loud-step"[^>]*>([^<]*)</button>', APP_JS)
    assert len(labels) == 2 and all(len(label.strip()) == 1 for label in labels), labels


def test_state_dot_buttons_are_lifted_without_growing():
    assert _decl(_block(APP_CSS, ".state-pill-btn"), "padding") == "0 0 1px"
    line = next(l for l in APP_CSS.splitlines() if l.startswith(".state-pill-btn-square {"))
    assert "padding-bottom: 2px;" in line, line


def test_attention_symbols_are_lifted_without_growing():
    block = _block(APP_CSS, ".attn-pill-broken,\n.attn-pill-repush,\n.attn-pill-fail")
    assert "padding-top: 0" in block and "padding-bottom: 2px" in block, block  # bottom weight lifts flex content
    assert APP_CSS.index("\n.attn-pill {") < APP_CSS.index("\n.attn-pill-broken,\n.attn-pill-repush,\n.attn-pill-fail {"), "the nudge must follow the base rule to win at equal specificity"


def test_v0_51_332_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "0.51.332: pill ink centred, the whole family" in init_py
