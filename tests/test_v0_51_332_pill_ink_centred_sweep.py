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
    # v0.51.343: the per-kind LINK glyphs (row cell, glossary, filter chips) read one --track too
    ".link-glyph-hardlink",
    ".link-glyph-copy",
    ".link-glyph-mismatch",
    ".link-glyph-pu",
    ".link-glyph-repush",
    ".link-glyph-bk",
    ".link-glyph-b",
    ".link-glyph-tb",
    ".link-glyph-ab",
]

# families whose every element also matches the owner rule, whose --track they read
INHERITED = {
    ".btn-tiny": ".btn",
    ".row-info-btn": ".btn",               # a .btn.btn-tiny
    '.chips[aria-label="section"] .chip': ".chip",
    '.chips[aria-label="resolution"] .chip': ".chip",  # v0.51.344: shares the section chips' rule; last-selector keying hid it (PB-C01)
}


# glyph-only labels whose glyph sits off the box centre: vertical padding
# moved, never grown — (top, bottom). The Ⓘ button is block-flow, so weight
# on TOP moves its glyph down; flex pills take weight on the BOTTOM to rise.
GLYPH_NUDGED = {".row-info-btn": ("5px", "3px")}

_SELECTOR_COMMA = re.compile(r",(?![^()]*\))")
_COMBINATOR = re.compile(r"\s*[>+~]\s*|\s+")


def _all_rules(css: str):
    # v0.51.344: every leaf rule of a sheet, @media ones too, as (each selector of its list, body, nested) — PB-083 / PB-C01
    body = re.sub(r"/\*.*?\*/", "", css, flags=re.S)
    heads, start = [], 0
    for i, ch in enumerate(body):
        if ch == "{":
            heads.append((body[start:i].rsplit(";", 1)[-1], i + 1))
            start = i + 1
        elif ch == "}":
            head, opened = heads.pop()
            if "{" not in body[opened:i]:
                selectors = tuple(" ".join(s.split()) for s in _SELECTOR_COMMA.split(head) if s.strip())
                yield selectors, body[opened:i], bool(heads)
            start = i + 1


def _rule_for(css: str, selector: str) -> str:
    # v0.51.344: a family's own top-level rule — its one-selector rule, else the first list that holds it (PB-C01)
    top = [(selectors, body) for selectors, body, nested in _all_rules(css) if not nested]
    body = next((b for s, b in top if s == (selector,)), None) or next((b for s, b in top if selector in s), None)
    assert body is not None, f"no top-level rule for {selector}"
    return body


def _decl_all(body: str, prop: str) -> list[str]:
    return [v.strip() for v in re.findall(rf"(?:^|[;\s]){re.escape(prop)}\s*:\s*([^;]+)", body)]


def _names(selector: str, family: str) -> bool:
    # v0.51.344: the selector's subject carries the family's classes, inside the family's context (`.pill-filter-row …`)
    *context, subject = _COMBINATOR.split(family)
    last = _COMBINATOR.split(selector)[-1]

    def has(text: str, part: str) -> bool:
        return re.search(re.escape(part) + r"(?![\w-])", text) is not None

    return all(has(last, c) for c in re.findall(r"\.[\w-]+", subject)) and all(has(selector, c) for c in context)


def _assert_right_gives_the_track_back(selector: str, block: str) -> None:
    top, right, bottom, left = _padding4(block)
    if selector in GLYPH_NUDGED:
        assert (top, bottom) == GLYPH_NUDGED[selector], (selector, top, bottom)
    else:
        assert top == bottom, (selector, top, bottom)
    assert right == f"calc({left} - {TRACK})", (selector, right, left)


@pytest.mark.parametrize("selector", TRACKED)
def test_tracking_and_right_padding_read_one_track(selector):
    block = _rule_for(APP_CSS, selector)
    assert re.fullmatch(r"\d*\.?\d+(em|px)", _decl(block, "--track") or ""), f"{selector} must declare its --track"
    # v0.51.344: exactly one — a second letter-spacing later in the block wins the cascade over the give-back (PB-083)
    assert _decl_all(block, "letter-spacing") == [TRACK], f"{selector}'s letter-spacing must read --track once, not a retyped value"
    _assert_right_gives_the_track_back(selector, block)


@pytest.mark.parametrize("selector,owner", INHERITED.items(), ids=list(INHERITED))
def test_inherited_tracking_reads_the_owner_track(selector, owner):
    block = _rule_for(APP_CSS, selector)
    assert _decl(block, "letter-spacing") is None and _decl(block, "--track") is None, f"{selector} now tracks on its own — move it to TRACKED"
    owner_block = _rule_for(APP_CSS, owner)
    assert _decl(owner_block, "--track") and _decl(owner_block, "letter-spacing") == TRACK, owner
    _assert_right_gives_the_track_back(selector, block)


def _rules(css: str):
    body = re.sub(r"/\*.*?\*/", "", css, flags=re.S)
    for m in re.finditer(r"([^{}]+)\{([^{}]*)\}", body):
        yield m.group(1), m.group(2)


_GIVE_BACK = re.compile(r"calc\([^;]*-\s*(?:var\(--track\)|\d*\.?\d+em)\s*\)")


def _padding_values(body: str, *props: str) -> list[str]:
    # v0.51.344: every declaration of the props, a trailing !important dropped — `8px !important` is still the left 8px
    return [re.sub(r"\s*!\s*important$", "", v, flags=re.I) for prop in props for v in _decl_all(body, prop)]


def _gives_back(body: str) -> bool:
    # v0.51.344: a track calc, or any right side that is calc(<the rule's own left> - x) — the v0.51.332 px form too (PB-083)
    rights = _padding_values(body, "padding-right", "padding-inline-end")
    shorthands = _padding_values(body, "padding")
    if any(_GIVE_BACK.search(v) for v in rights + shorthands):
        return True
    lefts = set(_padding_values(body, "padding-left", "padding-inline-start"))
    # v0.51.344: a lone right calc with no left and no shorthand in its rule subtracts from a left set elsewhere (never the F4 shorthand)
    if not lefts and not shorthands and any(re.fullmatch(r"calc\(.+\s-\s.+\)", v) for v in rights):
        return True
    for parts in (_TOP_SPACE.split(v) for v in shorthands):
        rights += parts[1:2]
        lefts.update(parts[3:4] or parts[1:2])
    # v0.51.344: the subtrahend is any text — var(--ls) and var(--track-sm) carry parentheses of their own
    return any(re.fullmatch(rf"calc\(\s*{re.escape(left)}\s*-\s*.+\)", right) for left in lefts for right in rights)


def test_the_give_back_reader_knows_every_form():
    for body in ("padding: 3px calc(8px - 0.5px) 3px 8px;", "padding-left: 8px; padding-right: calc(8px - 0.5px);",
                 "padding: 3px 8px; padding-right: calc(8px - 0.5px);", "padding: 2px calc(8px - var(--track)) 2px 8px;",
                 "padding-inline-end: calc(10px - 0.1em);",
                 # v0.51.344: any custom property, !important on either side, and a lone right calc whose left lives in another rule
                 "--ls: 0.5px; letter-spacing: var(--ls); padding: 3px calc(8px - var(--ls)) 3px 8px;",
                 "letter-spacing: var(--track-sm); padding-left: 8px; padding-right: calc(8px - var(--track-sm));",
                 "padding-left: 8px !important; padding-right: calc(8px - 0.5px) !important;",
                 "letter-spacing: 0.5px; padding-right: calc(12px - 0.5px);", "padding-inline-end: calc(12px - 0.5px) !important;"):
        assert _gives_back(body), body
    for body in ("padding: calc(var(--gap-3) - 1px) 12px;", "padding: 4px calc(var(--gap-3) - 1px) 4px 12px;", "padding: 3px 8px;",
                 "padding-left: 12px; padding-right: calc(8px - 1px);",
                 # v0.51.344: the lone-right arm reads only a rule with no left and no shorthand, and only a subtraction
                 "padding: 4px 12px; padding-right: calc(var(--gap-3) - 1px);", "padding-inline-start: 12px; padding-inline-end: calc(8px - 1px);",
                 "padding-left: 12px !important; padding-right: calc(8px - 1px) !important;", "padding-right: calc(8px + 1px);"):
        assert not _gives_back(body), body


def test_every_compensated_padding_is_a_listed_family():
    # v0.51.343: a new pill that subtracts its tracking from its padding lands in the tables above, and so under the mechanism
    # v0.51.344: both sheets, every selector of a list, every declaration; only a padding _gives_back reads as a give-back counts (PB-083 / PB-C01)
    found = set()
    for css in (APP_CSS, OPS_CSS):
        for selectors, body, _nested in _all_rules(css):
            if _gives_back(body):
                found.update(selectors)
    expected = set(TRACKED) | set(INHERITED)
    assert found == expected, sorted(found ^ expected)


_OWNED = {"btn-tiny", "row-info-btn"}
_LITERAL = re.compile(r"(['\"`])((?:\\.|(?!\1)[^\\\n])*)\1")
_CLASS_ONLY = re.compile(r"\s*[A-Za-z_][\w-]*(?:\s+[A-Za-z_][\w-]*)*\s*")
_CLASSLIST_CALL = re.compile(r"\bclassList\.(add|toggle|replace|contains|remove)\(([^()\n]*)\)")
_WHOLE_LINE_COMMENT = re.compile(r"\s*(?://|/\*|\*|\{#|<!--)")


def _class_tokens(text: str) -> set[str]:
    return set(re.sub(r"\$\{[^}]*\}|\{\{.*?\}\}|\{%.*?%\}", " ", text).split())


def _owner_class_uses(source: str) -> list[set[str]]:
    # v0.51.344: class= attributes, classList writes, and class-list-only literals (className =, a `btn btn-tiny${tone}` builder) — PB-083
    uses = [_class_tokens(m.group(2)) for m in re.finditer(r"\bclass=(\"|')(.*?)\1", source)]
    for line in source.splitlines():
        if _WHOLE_LINE_COMMENT.match(line):
            continue
        calls = list(_CLASSLIST_CALL.finditer(line))
        for call in calls:
            literals = [lit.group(2) for lit in _LITERAL.finditer(call.group(2))]
            if call.group(1) in ("add", "toggle", "replace"):
                uses.append(set().union(*map(_class_tokens, literals[1:] if call.group(1) == "replace" else literals)))
        for lit in _LITERAL.finditer(line):
            content = re.sub(r"\$\{[^}]*\}", " ", lit.group(2))
            if not any(call.start() <= lit.start() < call.end() for call in calls) and _CLASS_ONLY.fullmatch(content):
                uses.append(set(content.split()))
    return [tokens for tokens in uses if tokens & _OWNED]


def test_the_owner_class_scan_reads_every_way_a_class_is_set():
    for bare in ("b.className = 'btn-tiny';", "b.classList.add('btn-tiny');", "b.classList.toggle('row-info-btn', on);",
                 "const cls = `btn-tiny${tone}`;", '<a class="btn-tiny">'):
        assert [t for t in _owner_class_uses(bare) if "btn" not in t], bare
    for fine in ("b.classList.add('btn', 'btn-tiny');", "b.classList.contains('btn-tiny')", "b.classList.replace('btn-tiny', 'chip')",
                 "b.closest('.btn-tiny')", "  // a bare 'btn-tiny' reads no --track", "const cls = `btn btn-tiny${tone}`;"):
        assert not [t for t in _owner_class_uses(fine) if "btn" not in t], fine


def test_inheriting_elements_carry_the_owner_class():
    # v0.51.343: a .btn-tiny without .btn would read no --track, and its right padding would fall to 0
    files = sorted((REPO / "app" / "web" / "templates").glob("*.html")) + sorted(STATIC.glob("*.js")) + sorted(STATIC.glob("lib/*.js"))
    seen = 0
    for f in files:
        for tokens in _owner_class_uses(f.read_text()):
            seen += 1
            assert "btn" in tokens, (f.name, sorted(tokens))
    assert seen >= 20, seen


def test_the_rule_reader_sees_media_rules_and_every_selector_of_a_list():
    css = "@media (max-width: 600px) {\n  .a,\n  .b:is(.c, .d) { padding: 0; }\n}\n/* x */\n.e { color: red; }"
    assert [(s, nested) for s, _, nested in _all_rules(css)] == [((".a", ".b:is(.c, .d)"), True), ((".e",), False)]
    assert _names(".chips .chip:hover", ".chip") and _names(".pill-filter-row .link-glyph.on", ".pill-filter-row .link-glyph")
    assert not _names(".chip-row", ".chip") and not _names(".chip .count", ".chip") and not _names(".row .link-glyph", ".pill-filter-row .link-glyph")


def test_no_rule_retypes_a_tracked_familys_letter_spacing():
    # v0.51.344: a later rule, an @media one or a second declaration that retypes a family's tracking outlives its give-back (PB-083)
    checked, offenders = 0, []
    for sheet, css in (("app.css", APP_CSS), ("ops.css", OPS_CSS)):
        for selectors, body, _nested in _all_rules(css):
            values = _decl_all(body, "letter-spacing")
            for selector in selectors if values else ():
                lone = any(_names(selector, f) for f in LONE_GLYPH)
                if not (lone or any(_names(selector, f) for f in (*TRACKED, *INHERITED))):
                    continue
                checked += 1
                offenders += [(sheet, selector, v) for v in values if v != ("0" if lone else TRACK)]
    assert checked >= len(TRACKED), checked
    assert not offenders, f"a tracked family's letter-spacing reads its --track (a lone glyph's is 0): {offenders}"


def test_no_rule_retypes_a_tracked_familys_side_padding():
    # v0.51.344: the same for padding — a later or @media padding on a family must still give its track back (PB-083)
    offenders = []
    for sheet, css in (("app.css", APP_CSS), ("ops.css", OPS_CSS)):
        for selectors, body, _nested in _all_rules(css):
            paddings = _decl_all(body, "padding")
            sides = re.findall(r"(?:^|[;\s])(padding-(?:left|right|inline(?:-start|-end)?))\s*:", body)
            for selector in selectors if (paddings or sides) else ():
                if "::" in selector or any(_names(selector, f) for f in LONE_GLYPH):
                    continue
                if not any(_names(selector, f) for f in (*TRACKED, *INHERITED)):
                    continue
                offenders += [(sheet, selector, side) for side in sides]
                for value in paddings:
                    parts = _TOP_SPACE.split(value)
                    if len(parts) != 4 or parts[1] != f"calc({parts[3]} - {TRACK})":
                        offenders.append((sheet, selector, value))
    assert not offenders, f"right padding = left − the family's --track, in every rule that pads it: {offenders}"


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
