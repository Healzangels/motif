"""v0.51.338: library UI follow-ups to the v0.51.328..337 review.

A. A `hidden` element whose class gets an author `display` is VISIBLE: the
   author declaration outranks the UA `[hidden] { display: none }`. The NOW
   PLAYING strip (`.now-playing { display: inline-flex }`) shipped that way and
   showed "now playing · 0:00 / –:––" on every library load. The guard below is
   generic — every hidden element in every template against every stylesheet
   base.html links — so the next one fails the gate instead of production.

B. A SRC letter the page does not offer (AT off /anime, A / M on /collections)
   stayed in libraryState.srcFilter across an in-place tab switch, a deep link
   or a sessionStorage restore: 0 rows, a FILTERS badge of 1, and the only lit
   chip hidden. The rule is lib/src-filter.js — the live module base.html loads
   before app.js, pinned under node by tests/js/test_src_filter.js — and
   loadLibrary() prunes through it before it saves, badges or requests, while
   SRC ALL fills through it.
"""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from html.parser import HTMLParser
from pathlib import Path

import jinja2
import pytest

from _slice_helpers import slice_between

REPO = Path(__file__).resolve().parent.parent
STATIC = REPO / "app" / "web" / "static"
TEMPLATES = REPO / "app" / "web" / "templates"
APP_JS = (STATIC / "app.js").read_text()
BASE_HTML = (TEMPLATES / "base.html").read_text()
LIBRARY_HTML = (TEMPLATES / "library.html").read_text()
LIB_PATH = STATIC / "lib" / "src-filter.js"
LIB_JS = LIB_PATH.read_text()
HARNESS = REPO / "tests" / "js" / "test_src_filter.js"
_NODE = shutil.which("node")

if os.environ.get("MOTIF_REQUIRE_NODE") and not _NODE:
    raise RuntimeError("MOTIF_REQUIRE_NODE=1 but node is not on PATH — the src-filter harness would silently not run")


# ══ A. hidden elements an author display would still show ═══════════

_JINJA_COMMENT = re.compile(r"\{#.*?#\}", re.S)
_JINJA_STMT = re.compile(r"\{%.*?%\}", re.S)
_JINJA_EXPR = re.compile(r"\{\{.*?\}\}", re.S)
_INTERACTION = re.compile(r":(hover|focus|focus-visible|focus-within|active)\b")
_PSEUDO_ELEMENT = re.compile(r"::|:(before|after|first-line|first-letter)\b")


class _TagCollector(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.tags = []

    def handle_starttag(self, tag, attrs):
        self.tags.append((tag, attrs, self.getpos()[0]))


def _blank_keeping_lines(pattern, text, fill=""):
    return pattern.sub(lambda m: fill + "\n" * m.group(0).count("\n"), text)


def _hidden_elements(html):
    """(tag, attrs, line) for every start tag carrying a `hidden` attribute —
    unconditional or inside `{% if %}` — with the Jinja neutralised so the
    HTML parser sees the markup a page can render."""
    text = _blank_keeping_lines(_JINJA_COMMENT, html)
    text = _blank_keeping_lines(_JINJA_STMT, text, " ")
    text = _blank_keeping_lines(_JINJA_EXPR, text, "__jinja__")
    collector = _TagCollector()
    collector.feed(text)
    collector.close()
    out = []
    for tag, attrs, line in collector.tags:
        if any(name == "hidden" for name, _ in attrs):
            merged = {}
            for name, value in attrs:
                merged.setdefault(name, value if value is not None else "")
            merged["class"] = (merged.get("class") or "").replace("__jinja__", " ").strip()
            out.append((tag, merged, line))
    return out


def _split_top(s, seps):
    """Split on any of `seps` outside (), [] and quotes; drop empty parts."""
    parts, cur, depth, quote = [], [], 0, None
    for ch in s:
        if quote:
            cur.append(ch)
            if ch == quote:
                quote = None
            continue
        if ch in "\"'":
            quote = ch
        elif ch in "([":
            depth += 1
        elif ch in ")]":
            depth -= 1
        elif depth == 0 and ch in seps:
            parts.append("".join(cur))
            cur = []
            continue
        cur.append(ch)
    parts.append("".join(cur))
    return [p.strip() for p in parts if p.strip()]


def _outside_parens(s):
    out, depth = [], 0
    for ch in s:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif depth == 0:
            out.append(ch)
    return "".join(out)


def _css_rules(css, media=None):
    """(prelude, body, media) for every style rule in source order; @media and
    @supports bodies recurse with their condition carried along."""
    out, i, n = [], 0, len(css)
    while True:
        b = css.find("{", i)
        if b < 0:
            return out
        depth, k, quote = 0, b, None
        while k < n:
            ch = css[k]
            if quote:
                quote = None if ch == quote else quote
            elif ch in "\"'":
                quote = ch
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    break
            k += 1
        prelude = css[i:b].split(";")[-1].strip()
        body = css[b + 1:k]
        if prelude.startswith(("@media", "@supports", "@container", "@layer")):
            out.extend(_css_rules(body, prelude if media is None else media + " / " + prelude))
        elif prelude and not prelude.startswith("@"):
            out.append((prelude, body, media))
        i = k + 1


def _display_of(body):
    """(value, important) of the display declaration that wins inside one rule body."""
    best = None
    for decl in body.split(";"):
        name, sep, value = decl.partition(":")
        if not sep or name.strip().lower() != "display":
            continue
        value = value.strip().lower()
        important = value.endswith("!important")
        if important:
            value = value.rsplit("!", 1)[0].strip()
        if best is None or important or not best[1]:
            best = (value, important)
    return best


def _specificity(selector):
    ids = classes = types = 0
    i, s = 0, selector
    while i < len(s):
        ch = s[i]
        if ch in "#.":
            m = re.match(r"[#.][\w-]+", s[i:])
            if m:
                if ch == "#":
                    ids += 1
                else:
                    classes += 1
                i += len(m.group(0))
                continue
        if ch == "[":
            classes += 1
            i = s.index("]", i) + 1
            continue
        if s.startswith("::", i):
            m = re.match(r"::[\w-]+", s[i:])
            types += 1
            i += len(m.group(0))
            continue
        if ch == ":":
            m = re.match(r":([\w-]+)", s[i:])
            i += len(m.group(0))
            if i < len(s) and s[i] == "(":
                depth, j = 0, i
                while True:
                    depth += {"(": 1, ")": -1}.get(s[j], 0)
                    if depth == 0:
                        break
                    j += 1
                arg, i = s[i + 1:j], j + 1
                if m.group(1) == "where":
                    continue
                if m.group(1) in ("not", "is", "has", "matches"):
                    best = max((_specificity(a) for a in _split_top(arg, ",")), default=(0, 0, 0))
                    ids, classes, types = ids + best[0], classes + best[1], types + best[2]
                    continue
            classes += 1
            continue
        if ch.isalpha() and (i == 0 or s[i - 1] in " >+~\t\n"):
            m = re.match(r"[a-zA-Z][\w-]*", s[i:])
            types += 1
            i += len(m.group(0))
            continue
        i += 1
    return (ids, classes, types)


def _compound_matches(compound, tag, attrs):
    """Does this subject compound select the element as the template renders it?
    Ancestors are assumed to match; interaction states and pseudo-elements never
    do (a hidden element cannot be hovered, and ::before is not its own box)."""
    flat = _outside_parens(compound)
    if _PSEUDO_ELEMENT.search(flat) or _INTERACTION.search(flat):
        return False
    bare = re.sub(r"\[[^\]]*\]", "", flat)
    need_classes = re.findall(r"\.([\w-]+)", bare)
    need_ids = re.findall(r"#([\w-]+)", bare)
    if not need_classes and not need_ids:
        return False  # class / id keyed rules only
    classes = set((attrs.get("class") or "").split())
    if any(c not in classes for c in need_classes) or any(x != attrs.get("id") for x in need_ids):
        return False
    m = re.match(r"[a-zA-Z][\w-]*", bare)
    if m and m.group(0).lower() != tag:
        return False
    for name, op, value in re.findall(r"\[\s*([\w-]+)\s*(?:([~|^$*]?=)\s*[\"']?([^\"'\]]*)[\"']?)?\s*\]", flat):
        name = name.lower()
        if name == "hidden":
            continue
        if name not in attrs or (op == "=" and attrs[name] != value):
            return False
    return True


def _beats(hide, show):
    if hide["media"] is not None and hide["media"] != show["media"]:
        return False
    if hide["important"] != show["important"]:
        return hide["important"]
    return (_specificity(hide["selector"]), hide["order"]) > (_specificity(show["selector"]), show["order"])


def _hidden_display_leaks(templates, stylesheets):
    """Every hidden element an author display would still show on load.

    templates: {name: html}; stylesheets: [(name, css)] in cascade order.
    Returns (leaks, stats)."""
    rules = []
    for sheet, css in stylesheets:
        for prelude, body, media in _css_rules(re.sub(r"/\*.*?\*/", " ", css, flags=re.S)):
            display = _display_of(body)
            if display is None:
                continue
            for selector in _split_top(prelude, ","):
                rules.append({"sheet": sheet, "selector": selector, "value": display[0],
                              "important": display[1], "media": media, "order": len(rules)})
    leaks, scanned, covered = [], 0, 0
    for name, html in templates.items():
        for tag, attrs, line in _hidden_elements(html):
            if not attrs.get("class") and not attrs.get("id"):
                continue
            scanned += 1
            shows, hides = [], []
            for rule in rules:
                subject = _split_top(rule["selector"], " \t\n>+~")[-1]
                if not _compound_matches(subject, tag, attrs):
                    continue
                if re.search(r"\[\s*hidden\s*\]", _outside_parens(subject)):
                    if rule["value"] == "none":
                        hides.append(rule)
                elif re.search(r":not\(\s*\[\s*hidden\s*\]\s*\)", subject):
                    continue
                elif rule["value"] != "none":
                    shows.append(rule)
            style = re.sub(r"\s+", "", attrs.get("style") or "").lower()
            if re.search(r"(?:^|;)display:none", style):
                shows = [s for s in shows if s["important"]]
            losing = [f"{s['sheet']} `{s['selector']} {{ display: {s['value']} }}`"
                      for s in shows if not any(_beats(h, s) for h in hides)]
            if re.search(r"(?:^|;)display:(?!none)", style) and not any(h["important"] for h in hides):
                losing.append(f"inline style=\"{attrs['style']}\"")
            if losing:
                who = "#" + attrs["id"] if attrs.get("id") else "." + ".".join(sorted(attrs["class"].split()))
                leaks.append({"key": f"{name} {who}", "line": line,
                              "text": f"{name}:{line} <{tag} {who}> shown by " + "; ".join(losing)})
            elif shows:
                covered += 1
    return leaks, {"scanned": scanned, "covered": covered}


# Hidden elements a display rule reaches that are NOT visible on load (a parent
# stays hidden, JS sets style before paint, ...), each with a one-line reason.
# Empty at v0.51.338: the one hit at HEAD, the NOW PLAYING strip, really showed.
_ALLOWED: dict[str, str] = {}


def _templates():
    return {p.relative_to(TEMPLATES).as_posix(): p.read_text() for p in sorted(TEMPLATES.rglob("*.html"))}


def _stylesheets():
    hrefs = re.findall(r'<link rel="stylesheet" href="/static/([\w./-]+\.css)\?', BASE_HTML)
    assert "app.css" in hrefs, hrefs
    return [(h, (STATIC / h).read_text()) for h in hrefs]


def test_no_hidden_element_is_shown_by_an_author_display():
    leaks, stats = _hidden_display_leaks(_templates(), _stylesheets())
    assert stats["scanned"] >= 10 and stats["covered"] >= 1, f"the scan went blind: {stats}"
    unexplained = [leak["text"] for leak in leaks if leak["key"] not in _ALLOWED]
    assert not unexplained, (
        "a display rule outranks the UA [hidden] rule, so these render while hidden — "
        "add a `.X[hidden] { display: none; }` companion next to the rule:\n  " + "\n  ".join(unexplained))
    stale = sorted(set(_ALLOWED) - {leak["key"] for leak in leaks})
    assert not stale, f"allowlisted but no longer a leak — drop them: {stale}"


def _leaks(html, css):
    return _hidden_display_leaks({"t.html": html}, [("s.css", css)])[0]


def test_checker_sees_the_leak_and_its_cures():
    el = '<div class="strip" id="s" hidden></div>'
    assert len(_leaks(el, ".strip { display: inline-flex; }")) == 1
    assert _leaks(el, ".strip { display: inline-flex; }\n.strip[hidden] { display: none; }") == []
    assert _leaks(el, "#s[hidden] { display: none; }\n.strip { display: flex; }") == [], "an id companion outranks a class rule"
    assert _leaks(el, ".strip:not([hidden]) { display: flex; }") == []
    assert _leaks(el, ".strip::before { display: block; }") == []
    assert _leaks(el, ".strip:hover { display: flex; }") == []
    assert _leaks(el, ".strip { display: none; }") == []
    assert _leaks(el, ".strip.is-open { display: flex; }") == [], "a state class the markup does not carry"
    assert _leaks('<div class="strip"></div>', ".strip { display: flex; }") == []
    assert _leaks('<div class="strip" hidden style="display:none"></div>', ".strip { display: flex; }") == []


def test_checker_refuses_a_companion_that_loses():
    el = '<div class="strip" hidden></div>'
    assert len(_leaks(el, ".strip[hidden] { display: none; }\n.row .strip.strip { display: flex; }")) == 1, "higher specificity"
    assert len(_leaks(el, ".strip[hidden] { display: none; }\n.strip[class] { display: flex; }")) == 1, "equal specificity, later wins"
    assert len(_leaks(el, "@media (max-width: 600px) { .strip[hidden] { display: none; } }\n.strip { display: flex; }")) == 1, "a phone-only companion"
    assert len(_leaks(el, ".strip { display: flex !important; }\n.strip[hidden] { display: none; }")) == 1
    assert _leaks(el, ".strip { display: flex !important; }\n.strip[hidden] { display: none !important; }") == []
    assert len(_leaks('<div class="strip" hidden style="display: flex"></div>', "")) == 1, "inline display beats the UA rule too"
    assert len(_hidden_display_leaks({"t.html": el}, [("a.css", ".strip[hidden] { display: none; }"), ("b.css", ".strip[class] { display: flex; }")])[0]) == 1, "a later sheet wins a tie"


def test_checker_reads_jinja_conditional_hidden_and_multiline_tags():
    html = ('<span class="live"{% if view != "events" %} hidden{% endif %}>x</span>\n'
            '<a class="pill {{ tone }}"\n   href="/x"\n   {% if not n %}hidden{% endif %}>n</a>\n'
            '{# <div class="gone" hidden></div> #}')
    leaks = _leaks(html, ".live { display: flex; } .pill { display: inline-flex; } .gone { display: block; }")
    assert sorted((leak["key"], leak["line"]) for leak in leaks) == [("t.html .live", 1), ("t.html .pill", 2)]


# ══ B. the SRC filter only holds letters the page shows ══════════════

@pytest.mark.skipif(not _NODE, reason="node not installed")
def test_src_filter_harness_passes():
    r = subprocess.run([_NODE, "--test", str(HARNESS)], capture_output=True, text=True, timeout=120, cwd=REPO)
    assert r.returncode == 0, f"JS harness failed:\n{r.stdout[-3000:]}\n{r.stderr[-1500:]}"
    m = re.search(r"# pass (\d+)", r.stdout)
    assert m and int(m.group(1)) >= 12, r.stdout[-800:]
    assert "# fail 0" in r.stdout


def test_the_module_the_harness_tests_is_the_module_the_page_loads():
    lib = BASE_HTML.index('<script defer src="/static/lib/src-filter.js?v={{ motif_version }}">')
    assert lib < BASE_HTML.index('<script defer src="/static/app.js?v={{ motif_version }}">')
    assert 'require("../../app/web/static/lib/src-filter.js")' in HARNESS.read_text()
    assert "root.motifSrcFilter = factory()" in LIB_JS


def _src_row_chips(tab):
    """The SRC row of library.html rendered for `tab`: [{letter, tabOnly, shown}]."""
    row = slice_between(LIBRARY_HTML, '<div class="pill-filter-row" aria-label="SRC pill filter">', "\n  </div>")
    collector = _TagCollector()
    collector.feed(jinja2.Environment(autoescape=True).from_string(row).render(tab=tab))
    chips = []
    for _tag, attrs, _line in collector.tags:
        d = dict(attrs)
        if d.get("data-src-filter"):
            style = re.sub(r"\s+", "", d.get("style") or "").lower()
            chips.append({"letter": d["data-src-filter"], "tabOnly": d.get("data-tab-only") or "",
                          "shown": "display:none" not in style})
    return chips


def _deep_link_src_tokens():
    m = re.search(r"\{ param: 'src_pills',.*?values: new Set\(\[([^\]]*)\]\)", APP_JS, re.S)
    assert m, "the ?src_pills= restore allowlist"
    return re.findall(r"'([^']+)'", m.group(1))


def _all_letters():
    m = re.search(r"const allLetters = \[([^\]]*)\];", APP_JS)
    assert m, "the SRC ALL allowlist"
    return re.findall(r"'([^']+)'", m.group(1))


def _node_keep(cases):
    script = ("const { keepOfferedLetters } = require(process.argv[1]);"
              "const cases = JSON.parse(require('fs').readFileSync(0, 'utf8'));"
              "process.stdout.write(JSON.stringify(cases.map((c) => keepOfferedLetters(c.letters, c.chips, c.tab))));")
    r = subprocess.run([_NODE, "-e", script, str(LIB_PATH)], input=json.dumps(cases),
                       capture_output=True, text=True, timeout=60, cwd=REPO)
    assert r.returncode == 0, r.stderr[-1500:]
    return json.loads(r.stdout)


@pytest.mark.skipif(not _NODE, reason="node not installed")
def test_the_live_rule_keeps_exactly_the_letters_each_rendered_tab_shows():
    """Render the real SRC row per tab and run the live rule over it: a letter
    survives iff its chip is on that page AND shown there (the SSR style and
    data-tab-only agree), so nothing can narrow the table from a hidden chip."""
    tabs = ["movies", "tv", "anime", "collections"]
    rendered = {tab: _src_row_chips(tab) for tab in tabs}
    letters = sorted({c["letter"] for chips in rendered.values() for c in chips}
                     | set(_deep_link_src_tokens()) | set(_all_letters()) | {"ZZ"})
    cases = [{"letters": letters, "tab": tab,
              "chips": [{"letter": c["letter"], "tabOnly": c["tabOnly"]} for c in rendered[tab]]} for tab in tabs]
    kept = dict(zip(tabs, _node_keep(cases)))
    for tab in tabs:
        shown = {c["letter"] for c in rendered[tab] if c["shown"]}
        assert set(kept[tab]) == shown, (tab, kept[tab], sorted(shown))
    assert len({frozenset(v) for v in kept.values()}) > 1, f"no tab drops anything another keeps: {kept}"


def test_every_restorable_src_letter_is_a_shown_chip_on_some_shared_tab():
    """The prune makes a missing chip destructive (its letter is dropped), so
    every token ?src_pills= restores and every ALL letter must be a shown chip on
    at least one of the shared movies/tv/anime pages — which render one chip set."""
    shared = {tab: _src_row_chips(tab) for tab in ("movies", "tv", "anime")}
    chip_sets = {frozenset(c["letter"] for c in chips) for chips in shared.values()}
    assert len(chip_sets) == 1, "movies/tv/anime share one page and must render the same SRC chips"
    shown_somewhere = {c["letter"] for chips in shared.values() for c in chips if c["shown"]}
    tokens = set(_deep_link_src_tokens()) | set(_all_letters())
    assert tokens and tokens <= shown_somewhere, sorted(tokens - shown_somewhere)


def test_load_library_prunes_before_it_saves_badges_or_requests():
    body = slice_between(APP_JS, "async function loadLibrary() {", "\n  }\n")
    prune = body.index("_pruneSrcFilterToOfferedChips();")
    assert body.index("libraryState.tab = tabEl.value;") < prune, "prunes against the tab being loaded"
    assert prune < body.index("_saveLibraryFilterState();"), "sessionStorage never keeps a hidden letter"
    assert prune < body.index("updateFilterDrawerUi();"), "the FILTERS badge counts the pruned set"
    assert prune < body.index("params.set('src_pills',"), "the request never ships a hidden letter"


def test_prune_and_src_all_route_through_the_lib():
    fn = slice_between(APP_JS, "function _pruneSrcFilterToOfferedChips() {", "\n  }\n")
    assert "window.motifSrcFilter" in fn
    assert ".keepOfferedLetters(libraryState.srcFilter, _srcFilterChips(), libraryState.tab)" in fn
    assert "libraryState.srcFilter.delete(" in fn and "classList.remove('src-key-btn-active')" in fn
    chips = slice_between(APP_JS, "function _srcFilterChips() {", "\n  }\n")
    assert "querySelectorAll('[data-src-filter]')" in chips and "el.dataset.tabOnly" in chips
    all_branch = slice_between(APP_JS, "if (b.dataset.srcFilterAll) {", "} else {")
    assert "_srcFilterChips()" in all_branch
    assert ".keepOfferedLetters(letters, chips, libraryState.tab)" in all_branch
    assert "libraryState.srcFilter.add(v)" in all_branch
