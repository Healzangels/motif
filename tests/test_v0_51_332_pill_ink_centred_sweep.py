"""v0.51.332: pill ink centred — the whole family.

The operator: "audit all our pills and guards etc to make sure they're
all properly centered". Measured in the browser (ink vs box, both axes,
every page plus the INFO card and the glossary): every TRACKED family
carried the v0.51.331 defect — letter-spacing is laid after the last
glyph too, so a centred text box sits its ink half a tracking unit left
of centre. The fix is one rule per family: right padding = left padding
− letter-spacing. Since v0.51.343 each family declares one --track that
its letter-spacing and its right-padding calc both read, and this test
asserts that MECHANISM (left is read from the rule, not pinned), so a
tracking or padding retyped as a literal, or a padding put back
symmetric, fails here.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from _slice_helpers import slice_between

REPO = Path(__file__).resolve().parent.parent
STATIC = REPO / "app" / "web" / "static"
APP_CSS = (STATIC / "app.css").read_text()
OPS_CSS = (STATIC / "ops.css").read_text()
APP_JS = (STATIC / "app.js").read_text()

TRACK = "var(--track)"
# v0.51.343: split a shorthand on top-level spaces only; calc(12px - var(--track)) keeps its own
_TOP_SPACE = re.compile(r"\s+(?![^()]*(?:\([^()]*\)[^()]*)*\))")


def _block(css: str, selector: str) -> str:
    return slice_between(css, f"\n{selector} {{", "\n}")


def _decl(block: str, prop: str) -> str | None:
    m = re.search(rf"^\s*{re.escape(prop)}:\s*([^;]+);", block, re.M)
    return m.group(1).strip() if m else None


def _padding4(block: str) -> list[str]:
    val = _decl(block, "padding")
    assert val, "padding not declared"
    parts = _TOP_SPACE.split(val)
    assert len(parts) == 4, f"expected a 4-value padding, got {val!r}"
    return parts


# families that declare their own --track
TRACKED = [
    ".btn",
    ".chip",
    ".tab",
    ".tdb-pill",
    ".loudness-pill",
    ".attn-pill",
    ".pill-filter-clear",
    ".pill-filter-row .link-glyph",
    ".ed-pill-btn",
    ".info-scope-chip",
    ".library-clear-all-btn",
    ".tier-badge",
    ".pill",
    ".lib-flag-pill",
    ".edition-pill",
    ".form-env-badge",
    ".sync-hist-status",
    ".dash-section-toggle",
    ".dash-card-toggle",
    ".library-filter-toggle",
    ".link-badge",
]

# families whose every element also matches the owner rule, whose --track they read
INHERITED = {
    ".btn-tiny": ".btn",
    ".row-info-btn": ".btn",               # a .btn.btn-tiny
    '.chips[aria-label="section"] .chip': ".chip",
}


# glyph-only labels whose glyph sits off the box centre: vertical padding
# moved, never grown — (top, bottom). The Ⓘ button is block-flow, so weight
# on TOP moves its glyph down; flex pills take weight on the BOTTOM to rise.
GLYPH_NUDGED = {".row-info-btn": ("5px", "3px")}


def _assert_right_gives_the_track_back(selector: str, block: str) -> None:
    top, right, bottom, left = _padding4(block)
    if selector in GLYPH_NUDGED:
        assert (top, bottom) == GLYPH_NUDGED[selector], (selector, top, bottom)
    else:
        assert top == bottom, (selector, top, bottom)
    assert right == f"calc({left} - {TRACK})", (selector, right, left)


@pytest.mark.parametrize("selector", TRACKED)
def test_tracking_and_right_padding_read_one_track(selector):
    block = _block(APP_CSS, selector)
    assert re.fullmatch(r"\d*\.?\d+(em|px)", _decl(block, "--track") or ""), f"{selector} must declare its --track"
    assert _decl(block, "letter-spacing") == TRACK, f"{selector}'s letter-spacing must read --track, not a retyped value"
    _assert_right_gives_the_track_back(selector, block)


@pytest.mark.parametrize("selector,owner", INHERITED.items(), ids=list(INHERITED))
def test_inherited_tracking_reads_the_owner_track(selector, owner):
    block = _block(APP_CSS, selector)
    assert _decl(block, "letter-spacing") is None and _decl(block, "--track") is None, f"{selector} now tracks on its own — move it to TRACKED"
    owner_block = _block(APP_CSS, owner)
    assert _decl(owner_block, "--track") and _decl(owner_block, "letter-spacing") == TRACK, owner
    _assert_right_gives_the_track_back(selector, block)


def _rules(css: str):
    body = re.sub(r"/\*.*?\*/", "", css, flags=re.S)
    for m in re.finditer(r"([^{}]+)\{([^{}]*)\}", body):
        yield m.group(1), m.group(2)


def test_every_compensated_padding_is_a_listed_family():
    # v0.51.343: a new pill that subtracts its tracking from its padding lands in the tables above, and so under the mechanism
    found = set()
    for selectors, body in _rules(APP_CSS):
        for prop in ("padding", "padding-right"):
            val = _decl(body, prop)
            if val and re.search(r"calc\([^;]* - ", val):
                found.add(selectors.split(",")[-1].strip())
    expected = set(TRACKED) | set(INHERITED)
    assert found == expected, sorted(found ^ expected)


def test_inheriting_elements_carry_the_owner_class():
    # v0.51.343: a .btn-tiny without .btn would read no --track, and its right padding would fall to 0
    files = sorted((REPO / "app" / "web" / "templates").glob("*.html")) + sorted(STATIC.glob("*.js")) + sorted(STATIC.glob("lib/*.js"))
    seen = 0
    for f in files:
        for m in re.finditer(r"\bclass=(\"|')(.*?)\1", f.read_text()):
            tokens = set(re.sub(r"\$\{[^}]*\}|\{\{.*?\}\}|\{%.*?%\}", " ", m.group(2)).split())
            if tokens & {"btn-tiny", "row-info-btn"}:
                seen += 1
                assert "btn" in tokens, (f.name, m.group(0))
    assert seen >= 20, seen


def test_topbar_inbox_label_compensates_on_itself():
    block = _block(OPS_CSS, ".op-pill .op-pill-label")
    assert _decl(block, "--track") and _decl(block, "letter-spacing") == TRACK, block
    assert _decl(block, "margin-right") == f"calc(-1 * {TRACK})", block


def _side_paddings(block: str) -> tuple[str | None, str | None]:
    left, right = _decl(block, "padding-left"), _decl(block, "padding-right")
    if left is None and right is None:
        parts = _TOP_SPACE.split(_decl(block, "padding") or "")
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
    # v0.51.343: the invariant is one glyph per label, however many steps there are (was len == 2)
    assert labels and all(len(label.strip()) == 1 for label in labels), labels


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
