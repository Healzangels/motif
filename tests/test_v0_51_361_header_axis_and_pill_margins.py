"""v0.51.361 — a header on its column's axis, and no pill with a one-sided margin.

Two regressions of one kind: something moved, and the value that compensated for where it
used to be stayed behind.

(1) `.tdb-pill` / `.edition-pill` / `.loudness-pill` / `.attn-pill` each carried
    `margin-left: 6px` from when a pill trailed the title INLINE. v1.15.53 moved TDB and ED
    into their own CENTRED `<td>`s, the filter rows went to a flex gap in v1.13.0, and the
    glossary puts chips on a shared rail — in every one of those an asymmetric margin puts
    the box 3px right of the axis. Measured: all 50 TDB pills at 491 against a column and
    header at 488; an edition pill at 688 against 685; the LEVELED/RAW/OUTLIERS chips at 553
    on a rail the other chips sit at 550. v1.23.68 had already zeroed it for the glossary's
    TDB chip alone, which is the tell.

(2) v1.12.28 centred every `.table thead th` and exempted the library's TITLE for a reason it
    wrote down — a centred label over a wide column floats far from its data — but wired the
    exemption to that table alone. Worse, `.table thead th` is (0-1-2) and a bare column class
    is (0-1-0), so a column class sets its CELLS' alignment and silently not its header's.
    Measured header-label-to-data before: LOCATIONS 268px, LIBRARY 86px, SECTION 76px,
    ROLE 69px, SECTION 32px. `.col-name` (two classes, so it wins) now marks a left-aligned
    text column and puts both sides on one axis.

A browser resolves the cascade, so the guard below resolves it too: a small specificity
engine over app.css answers "what does `text-align` compute to for this <th>?".
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
DASH = (REPO / "app" / "web" / "templates" / "dashboard.html").read_text()
SETTINGS = (REPO / "app" / "web" / "templates" / "settings.html").read_text()

# the pills that render as the only thing in a centred cell or on a centred rail
CENTRED_PILLS = (".tdb-pill", ".edition-pill", ".loudness-pill", ".attn-pill")
# trailing badges that DO sit after inline text — a left margin is their job
INLINE_BADGES = (".brand-update", ".tdb-unavailable-badge", ".nav-attn")


def _rule_body(selector: str) -> str:
    m = re.search(rf"^{re.escape(selector)} \{{(.*?)^\}}", CSS, re.S | re.M)
    assert m, f"no rule for {selector}"
    return m.group(1)


def test_no_centred_pill_carries_a_one_sided_margin():
    for sel in CENTRED_PILLS:
        body = _rule_body(sel)
        left = re.search(r"(?:^|;|\*/)\s*margin-left:\s*([^;]+);", body, re.M)
        right = re.search(r"(?:^|;|\*/)\s*margin-right:\s*([^;]+);", body, re.M)
        lv = left.group(1).strip() if left else "0"
        rv = right.group(1).strip() if right else "0"
        assert lv == rv, (
            f"v0.51.361: {sel} renders centred (its own <td>, a flex-gap filter row, the "
            f"glossary rail); margin-left {lv} vs margin-right {rv} puts it off that axis"
        )


def test_the_inline_badges_keep_theirs():
    """The same declaration is correct where a badge really does trail text — so the test
    above is about the context, not about the property."""
    for sel in INLINE_BADGES:
        assert "margin-left" in _rule_body(sel), f"{sel} trails inline text and wants its gap"


# ── a small cascade resolver, so the guard answers the browser's question ──

_RULE_RE = re.compile(r"(?m)^([^@{}/][^{}]*?)\{([^{}]*)\}")
_SIMPLE_RE = re.compile(r"^[a-z0-9.\- ]+$")


def _rules():
    """Top-level rules only: (selector, body, order). Rules inside @media are skipped, and
    `_resolve` asserts none of them could have matched what we ask about."""
    out, depth, i = [], 0, 0
    src = re.sub(r"/\*.*?\*/", "", CSS, flags=re.S)
    for m in re.finditer(r"[{}]", src):
        if m.group() == "{":
            if depth == 0:
                sel_start = i
                sel = src[sel_start:m.start()].strip()
                body_start = m.end()
            depth += 1
        else:
            depth -= 1
            if depth == 0:
                body = src[body_start:m.start()]
                if not sel.startswith("@"):
                    out.append((sel, body, len(out)))
                i = m.end()
            elif depth < 0:
                depth = 0
    return out


def _specificity(sel: str):
    ids = sel.count("#")
    classes = sel.count(".") + sel.count("[") + len(re.findall(r":(?!:)(?!not)", sel))
    elements = len(re.findall(r"(?:^|[\s>+~])([a-z][a-z0-9]*)", sel))
    return (ids, classes, elements)


def _matches(sel: str, element, ancestors):
    """Descendant combinators and tag/class compounds only; anything else returns None."""
    if not _SIMPLE_RE.match(sel):
        return None
    parts = sel.split()
    chain = list(ancestors) + [element]

    def compound_ok(part, node):
        tag = re.match(r"^[a-z0-9]+", part)
        classes = set(re.findall(r"\.([a-z0-9\-]+)", part))
        if tag and tag.group() != node[0]:
            return False
        return classes <= set(node[1])

    ci = len(chain) - 1
    if not compound_ok(parts[-1], chain[ci]):
        return False
    ci -= 1
    for part in reversed(parts[:-1]):
        while ci >= 0 and not compound_ok(part, chain[ci]):
            ci -= 1
        if ci < 0:
            return False
        ci -= 1
    return True


def _resolve(prop, element, ancestors):
    best, best_key = None, None
    for sel_list, body, order in _rules():
        decl = re.search(rf"(?:^|;)\s*{prop}:\s*([^;]+)", body)
        if not decl:
            continue
        for sel in (s.strip() for s in sel_list.split(",")):
            hit = _matches(sel, element, ancestors)
            if hit is None:
                # unparsed: it must not be able to speak about these classes at all
                assert not (set(element[1]) & set(re.findall(r"\.([a-z0-9\-]+)", sel))), (
                    f"v0.51.361: the resolver cannot read {sel!r}, and it names one of "
                    f"{element[1]} — teach it that shape before trusting this test"
                )
                continue
            if not hit:
                continue
            key = (_specificity(sel), order)
            if best_key is None or key > best_key:
                best, best_key = decl.group(1).strip(), key
    return best


TABLE = ("table", {"table"})
THEAD = ("thead", set())

# (what we are asking about, expected)
AXIS_CASES = (
    (("th", {"col-name"}), [TABLE, THEAD], "left", "the dashboard tables' first column"),
    (("th", {"lib-locations", "col-name"}), [TABLE, THEAD], "left", "settings LOCATIONS, 642px"),
    (("th", {"lib-col-section", "col-name"}), [TABLE, THEAD], "left", "settings SECTION"),
    (("th", {"lib-col-role", "col-name"}), [TABLE, THEAD], "left", "settings ROLE"),
    (("td", {"col-name"}), [TABLE], "left", "the cells under them"),
    # controls: the resolver must not simply answer 'left'
    (("th", {"col-tdb"}), [TABLE, THEAD], "center", "a narrow centred column, unchanged"),
    (("th", set()), [TABLE, THEAD], "center", "v1.12.28's default, unchanged"),
)


def test_each_header_resolves_onto_its_column_axis():
    for element, ancestors, want, why in AXIS_CASES:
        got = _resolve("text-align", element, ancestors)
        assert got == want, (
            f"v0.51.361: <{element[0]} class=\"{' '.join(sorted(element[1]))}\"> ({why}) "
            f"resolves text-align: {got}, want {want} — `.table thead th` (0-1-2) outranks a "
            f"bare column class, so a header can silently keep center"
        )


# ── both sides carry the class, or the column is only half-fixed ──

COLUMN_SITES = (
    (DASH, '<th class="col-name">SECTION</th>', 1, "dashboard PER-SECTION COVERAGE header"),
    (DASH, '<th class="col-name">LIBRARY</th>', 1, "dashboard GENERAL STATISTICS header"),
    (APP_JS, '<td class="col-name" title="${htmlEscape(sectionTip)}"', 1, "its section-name cell"),
    (APP_JS, '<td class="gs-lib col-name">', 1, "its library-name cell"),
    (SETTINGS, '<th class="lib-col-section col-name">', 2, "settings SECTION header x2"),
    (SETTINGS, '<th class="lib-col-role col-name">', 2, "settings ROLE header x2"),
    (SETTINGS, '<th class="lib-locations col-name">', 2, "settings LOCATIONS header x2"),
    (APP_JS, '<td class="lib-col-section col-name">', 1, "settings SECTION cell"),
    (APP_JS, '<td class="lib-col-role col-name">', 1, "settings ROLE cell"),
    (APP_JS, '<td class="lib-locations col-name mono small"', 1, "settings LOCATIONS cell"),
)


def test_every_left_text_column_marks_both_sides():
    for src, needle, n, why in COLUMN_SITES:
        assert src.count(needle) == n, (
            f"v0.51.361: {why} — expected {n}x {needle!r}, found {src.count(needle)}; a column "
            f"marked on one side only puts its header back off the data's axis"
        )
