"""v0.51.340 — the INFO card lines up.

The operator: "can we audit and edit the info card to make things align and more
professional looking the misaligned playback for examples looks bad". Measured at the
720px drawer: the state badge led each play row, so the players started at four
different x's (232.5 / 239 / 258.5 / 265); play labels sat 11.2px above the player
centre and button-row labels 2.8px above the button centre (top-aligned <dt> text); a
fold title's // started 18.8px right of a group <h4>'s (the in-flow caret).

These pin the invariants, not the pixels (the merger re-measures geometry):
  a. a play row badges its <dt> and its <dd> opens on the <audio>;
  b. every control-first value in the card builders labels its <dt> .info-ctl-label;
  c. the label rule and .info-audio share one player-height token, and the button-line
     token is composed from the .btn-tiny primitive's own metrics;
  d. the fold caret hangs off the title into the gutter, on the title's first line (a
     phone-width summary wraps; a summary-centred caret fell between its lines).

v0.51.341 (reviewer residuals): b. classifies a value by its FIRST element — a button,
an a.btn, an audio or a .loud-ctl-row — not an allow-list of literal heads; the
.history-section scope check reaches app.js itself; and the LOUDNESS row label is one
button line tall, so its <dt> centres on line 1 when the stepper wraps under it at
375px/360px (measured 3.3px low before).
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from _slice_helpers import slice_between

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()

_SEAM = re.compile(r"`\s*\+\s*`")
_PLAY_DT = '<dt class="info-ctl-label info-ctl-label-play">'


def _joined(src: str) -> str:
    return _SEAM.sub("", src)


def _rule(selector: str) -> str:
    return slice_between(APP_CSS, f"\n{selector} {{", "}")


def _block(selector: str) -> str:
    return slice_between(APP_CSS, f"\n{selector} {{", "\n}")


def _decl(block: str, prop: str) -> str | None:
    m = re.search(rf"(?:^|[;{{]|\s){re.escape(prop)}:\s*([^;]+);", block)
    return m.group(1).strip() if m else None


def _root_token(name: str) -> str:
    root = _block(":root")
    found = re.findall(rf"^\s*{re.escape(name)}:\s*([^;]+);", root, re.M)
    assert len(found) == 1, f"{name} must be defined exactly once in :root"
    return found[0].strip()


def _px(value: str) -> float:
    value = value.strip()
    m = re.fullmatch(r"var\((--[\w-]+)\)", value)
    if m:
        return _px(_root_token(m.group(1)))
    m = re.fullmatch(r"(\d+(?:\.\d+)?)px", value)
    assert m, f"not a px length: {value!r}"
    return float(m.group(1))


# ── a. play rows: the badge rides the label column ────────────────────────────

_PLAY_ROW = re.compile(
    re.escape(_PLAY_DT) + r"(?P<label>[a-z ]+)"
    r'<span class="tier-badge (?P<tone>[^"]+)" title="[^"]+">(?P<state>[^<]+)</span></dt>'
    r'<dd class="info-play-row">(?P<dd>.*?)</dd>', re.S)

_PLAY_BUILDERS = (
    ("bare card", "function renderBareInfoCard(", "\n  function _bindPlexThemePlayer(body) {", "plex serves"),
    ("full card, plex", "const plexThemeBlock = ", "      : '';", "plex serves"),
    ("full card, motif", "const audioBlock = lf", "      : '';", "motif file"),
)


@pytest.mark.parametrize("name,start,end,label", _PLAY_BUILDERS, ids=[b[0] for b in _PLAY_BUILDERS])
def test_play_row_badges_the_label_and_opens_the_value_on_the_player(name, start, end, label):
    src = _joined(slice_between(APP_JS, start, end))
    rows = list(_PLAY_ROW.finditer(src))
    assert len(rows) == 1, f"{name}: one play row, badge inside its <dt>"
    row = rows[0]
    assert row.group("label").rstrip() == label, name  # v0.51.340: the trailing separator is its own guard below
    assert row.group("dd").startswith("<audio "), (
        f"{name}: the <dd> must open on the player, so every player starts at the value edge")
    assert "tier-badge" not in row.group("dd"), f"{name}: no badge left inside the value row"


def test_no_play_row_leads_with_a_badge_anywhere():
    assert 'class="info-play-row"><span class="tier-badge' not in _joined(APP_JS), (
        "a badge ahead of the player is what put each player at a different x")


def _render_bare(row_js: str) -> str:
    quickjs = pytest.importorskip("quickjs")
    src = slice_between(APP_JS, "function renderBareInfoCard(", "\n  function _bindPlexThemePlayer(body) {")
    harness = (
        # v0.51.343: the bare card's Plex player URL comes from lib/quick-play.js, which base.html loads first
        (REPO / "app" / "web" / "static" / "lib" / "quick-play.js").read_text()
        + "\nvar window = {motifQuickPlay: motifQuickPlay};\n"
        "var htmlEscape = function(s){return String(s===undefined||s===null?'':s)"
        ".replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')"
        ".replace(/\"/g,'&quot;').replace(/'/g,'&#39;');};\n"
        + src + f"\nrenderBareInfoCard({row_js});"
    )
    return quickjs.Context().eval(harness)


def test_bare_card_renders_badge_under_label_and_player_first():
    out = _render_bare("{plex_title:'Bleach',year:2004,plex_media_type:'show',"
                       "section_id:'3',rating_key:'778',plex_has_theme:1}")
    assert re.search(
        re.escape(_PLAY_DT) + r'plex serves <span class="tier-badge tier-badge-serving" title="[^"]+">SERVING</span></dt>'
        r'<dd class="info-play-row"><audio ', out), out


_DEAD_PLAYER_HARNESS = r"""
var removed = [];
var dtClassesRemoved = [];
function node(name, extra) {
  var o = {remove: function () { removed.push(name); }};
  for (var k in extra) o[k] = extra[k];
  return o;
}
var badge = node('badge', {});
var meta = node('meta', {textContent: ''});
var dt = node('dt', {
  classList: {remove: function () { for (var i = 0; i < arguments.length; i++) dtClassesRemoved.push(arguments[i]); }},
  querySelector: function (sel) { return sel === '.tier-badge' ? badge : null; }
});
var dd = node('dd', {
  previousElementSibling: dt,
  querySelector: function (sel) { return sel === '.info-probe-meta' ? meta : null; }
});
var handlers = {};
var audio = node('audio', {parentElement: dd, addEventListener: function (t, fn) { handlers[t] = fn; }});
var body = {querySelector: function (sel) { return sel === 'audio[data-plex-theme]' ? audio : null; }};
"""


def test_a_dead_player_clears_the_badge_from_its_label():
    quickjs = pytest.importorskip("quickjs")
    fn = slice_between(APP_JS, "function _bindPlexThemePlayer(body) {", "\n  }") + "\n  }"
    out = quickjs.Context().eval(
        _DEAD_PLAYER_HARNESS + fn
        + "\n_bindPlexThemePlayer(body); handlers.error();"
        + "\nJSON.stringify({removed: removed, dt: dtClassesRemoved, meta: meta.textContent});")
    got = json.loads(out)
    assert got["removed"] == ["badge", "audio"], (
        "the badge is found through the row's <dt> and goes before the player")
    assert {"info-ctl-label", "info-ctl-label-play"} <= set(got["dt"]), (
        "with the player gone the value is a text line, so the label top-aligns again")
    assert "did not play" in got["meta"]


# ── b. every control-first value labels its <dt> ──────────────────────────────

# one-line labels (a "<dt>" in a JS comment never closes on its own line).
_DT_DD = re.compile(r"<dt(?P<attrs>[^>\n]*)>(?P<label>(?:(?!</dt>|<dt)[^\n])*?)</dt>\s*<dd[^>]*>", re.S)
# v0.51.341: a value's FIRST element, read where its <dd> opens — a tag (optionally the first tag of a
# nested `${xs.map((x) => \`…` literal), or a `${ref}` to a builder const resolved to its literal's first tag.
_ATTRS = r"(?:[^>\"']|\"[^\"]*\"|'[^']*')*"
_HEAD = re.compile(rf"\s*(?:\$\{{(?P<ref>\w+)\}}|(?:\$\{{[^`}}]*`\s*)?<(?P<tag>[a-zA-Z][\w-]*)(?P<attrs>{_ATTRS})>)")


def _classes(attrs: str) -> set[str]:
    m = re.search(r'\bclass="([^"]*)"', attrs)
    return set(m.group(1).split()) if m else set()


def _first_element(src: str, pos: int) -> tuple[str, set[str]] | None:
    """(tag, classes) of the first element at `pos`, or None for a text value."""
    h = _HEAD.match(src, pos)
    if not h:
        return None
    if h.group("ref"):
        d = re.search(rf"\b(?:const|let) {h.group('ref')} = [^`;]*`\s*<([a-zA-Z][\w-]*)({_ATTRS})>", src)
        return (d.group(1), _classes(d.group(2))) if d else None
    return h.group("tag"), _classes(h.group("attrs"))


def _is_control(el: tuple[str, set[str]] | None) -> bool:
    if el is None:
        return False
    tag, classes = el
    return tag in ("button", "audio") or (tag == "a" and "btn" in classes) or "loud-ctl-row" in classes


def _card_region() -> str:
    return _joined(slice_between(APP_JS, "function renderBareInfoCard(", "function closeInfoDialog() {"))


def _card_rows() -> list:
    region = _card_region()
    rows = []
    for m in _DT_DD.finditer(region):
        label = re.split(r"<|\$\{", m.group("label"), maxsplit=1)[0].strip()
        rows.append((label, _first_element(region, m.end()), _classes(m.group("attrs"))))
    return rows


def test_the_row_scan_sees_every_term_in_the_card():
    assert len(_card_rows()) == _card_region().count("</dt>"), (
        "a term the scan skips is a control row nobody checks")


@pytest.mark.parametrize("snippet,control", [
    ('<dt>x</dt><dd><a class="btn btn-tiny" href="#">// GO</a></dd>', True),
    ('<dt>x</dt><dd>\n  <button class="notif-x" type="button">', True),
    ('<dt>x</dt><dd class="info-play-row"><audio controls class="info-audio">', True),
    ('<dt>x</dt><dd class="loud-controls">\n  <div class="loud-ctl-row">', True),
    ('<dt>x</dt><dd>${xs.map((e) => `\n  <button class="btn btn-tiny">', True),
    ('<dt>x</dt><dd>${goBtn}</dd> const goBtn = ok\n  ? `<a class="btn btn-info" href="#">`', True),
    ('<dt>x</dt><dd><a class="info-source-link" href="#">', False),
    ('<dt>x</dt><dd><span class="muted small">', False),
    ('<dt>x</dt><dd>${htmlEscape(value)}</dd>', False),
    ('<dt>x</dt><dd>${linkHtml}</dd> const linkHtml = linkOrDash(url);', False),
])
def test_the_control_classifier_reads_the_first_element_of_any_value(snippet, control):
    m = _DT_DD.search(snippet)
    assert m, snippet
    assert _is_control(_first_element(snippet, m.end())) is control, snippet


def test_every_control_first_value_centres_its_label_on_the_control_line():
    found = []
    for label, el, classes in _card_rows():
        if _is_control(el):
            found.append(label)
            assert "info-ctl-label" in classes, f"<dt>{label}</dt> sits beside a control line (<{el[0]}>)"
            assert ("info-ctl-label-play" in classes) == (el[0] == "audio"), (
                f"<dt>{label}</dt>: the player height for a player, the button line otherwise")
        else:
            assert "info-ctl-label" not in classes, (
                f"<dt>{label}</dt> heads a text/media value and stays top-aligned")
    assert found.count("plex serves") == 2 and found.count("action") == 2, found
    assert {"motif file", "actions", "measure", "measured"} <= set(found), found


def test_the_source_video_thumbnail_stays_top_aligned():
    thumbs = [classes for label, _, classes in _card_rows() if label == "source video"]
    assert len(thumbs) == 2 and not any(thumbs), "tall media keeps its label at the top"


# ── c. one player height, one button line, one centring rule ─────────────────


def test_the_player_and_its_label_share_one_height_token():
    audio = _rule(".info-audio")
    play_label = _rule(".dlg-grid dt.info-ctl-label-play")
    assert _decl(audio, "height") == "var(--info-player-h)"
    assert _decl(play_label, "min-height") == "var(--info-player-h)"
    _root_token("--info-player-h")


def test_control_label_centres_on_the_first_line_not_the_whole_value():
    r = _rule(".dlg-grid dt.info-ctl-label")
    assert _decl(r, "align-self") == "start", "a wrapped second value line must not drag the label down"
    assert _decl(r, "display") == "flex" and _decl(r, "flex-direction") == "column"
    assert _decl(r, "justify-content") == "center"
    assert _decl(r, "min-height") == "var(--btn-tiny-h)"


def test_button_line_token_is_composed_from_the_btn_tiny_primitive():
    h = _root_token("--btn-tiny-h")
    for part in ("var(--t-tiny)", "var(--btn-tiny-lh)", "var(--btn-tiny-pad-y)"):
        assert part in h, f"--btn-tiny-h must read {part}"
    tiny = _block(".btn-tiny")
    assert _decl(tiny, "line-height") == "var(--btn-tiny-lh)"
    pad = re.split(r"\s+(?![^()]*(?:\([^()]*\)[^()]*)*\))", _decl(tiny, "padding"))  # v0.51.343: the right calc nests var(--track)
    assert pad[0] == pad[2] == "var(--btn-tiny-pad-y)", pad
    border = re.search(r"^\s*border:\s*(\d+)px solid", _block(".btn"), re.M)
    assert border and f"+ {2 * int(border.group(1))}px" in h, "the .btn border, both edges"


def test_label_and_badge_stack_fits_inside_the_player_line():
    line = _px(_decl(_rule(".dlg-grid"), "font-size")) * float(_decl(_block("body"), "line-height"))
    gap = _px(_decl(_rule(".dlg-grid dt.info-ctl-label-play"), "gap"))
    badge_rule = _block(".tier-badge")
    pad_top = _px(re.split(r"\s+(?![^()]*(?:\([^()]*\)[^()]*)*\))", _decl(badge_rule, "padding"))[0])
    border = max(_px(re.search(r"border:\s*(\d+px) solid", _rule(f".tier-badge-{s}")).group(1))
                 for s in ("serving", "standing", "placed", "unplaced"))
    badge = _px(_decl(badge_rule, "font-size")) * float(_decl(badge_rule, "line-height")) + 2 * pad_top + 2 * border
    assert line + gap + badge <= _px(_root_token("--info-player-h")), (
        "a taller stack would grow the row past the player line and un-centre it")


def test_the_dead_in_row_badge_rule_is_gone():
    assert ".info-play-row > .tier-badge" not in APP_CSS


def test_the_loudness_row_label_is_the_button_line_its_dt_centres_on():
    """v0.51.341: at 375px/360px the first .loud-ctl-row wraps — 'target' alone on line 1
    (a 16.5px t-tiny line box), the stepper on line 2 — so the <dt>, centred on --btn-tiny-h
    (23.2px), sat (23.2 - 16.5) / 2 = 3.3px low. A label one button line tall makes line 1 that
    line whether or not the stepper wraps; unwrapped, line 1 was already the stepper's 23.2px."""
    label = _rule(".loud-ctl-label")
    assert _decl(label, "min-height") == _decl(_rule(".dlg-grid dt.info-ctl-label"), "min-height") == "var(--btn-tiny-h)", (
        "the label and the <dt> must share the one button-line token")
    assert _decl(label, "display") in ("flex", "inline-flex") and _decl(label, "align-items") == "center", (
        "the label text centres inside its button-line box")
    assert _decl(_rule(".loud-ctl-row"), "align-items") == "center", "items on one line share one centre"


def test_every_loudness_row_opens_on_a_button_line():
    """Line 1 of a .loud-ctl-row holds its first item at least, so every row must open on a
    .btn-tiny (exactly --btn-tiny-h) or the button-line-tall .loud-ctl-label — then its <dt>
    lines up with line 1 whatever wraps."""
    heads = [_first_element(APP_JS, m.end()) for m in re.finditer(r'<div class="loud-ctl-row">', APP_JS)]
    assert len(heads) >= 3, "the which-cut picker, the leveled row and the raw rows"
    for el in heads:
        assert el is not None, "a .loud-ctl-row opening on bare text has a line shorter than its label's"
        tag, classes = el
        assert (tag == "button" and "btn-tiny" in classes) or "loud-ctl-label" in classes, (
            f"a .loud-ctl-row opens on <{tag} class={sorted(classes)}> — its first line may be shorter "
            "than the button line the <dt> centres on")


# ── d. the fold caret hangs off the title into the gutter ─────────────────────


def test_fold_caret_sits_outside_the_title_text_start():
    assert _decl(_block(".history-section-title"), "position") == "relative", "the caret hangs off the title"
    caret = _block(".history-section-title::before")
    assert _decl(caret, "position") == "absolute", "out of the flex flow — it no longer pushes the title"
    assert _decl(caret, "right") == "100%", "its right edge ends where the title's // begins"
    assert _decl(caret, "left") is None
    opened = _block(".history-section[open] .history-section-title::before")
    assert "rotate(90deg)" in opened, "the open/closed rotation survives"


def test_fold_caret_rides_the_title_first_line_not_the_summary_centre():
    # a phone-width summary wraps to two lines; a caret centred on the summary fell between them
    assert ".history-section > summary::before" not in APP_CSS
    title = _block(".history-section-title")
    caret = _block(".history-section-title::before")
    assert _decl(caret, "top") == "0", "the title box's top is its first line box's top"
    assert _decl(caret, "line-height") is None and _decl(caret, "margin-top") is None, (
        "the caret inherits the title's line height, so its box is exactly the title's first line")
    assert _decl(caret, "font-size") == _decl(title, "font-size"), (
        "same size on the same line height puts the caret on the title's first baseline")


def test_fold_title_never_breaks_its_slashes_from_its_name():
    assert _decl(_block(".history-section-title"), "white-space") == "nowrap", (
        "a narrow summary wraps its note; '//' never orphans onto a line of its own")


def test_every_fold_title_opens_its_summary():
    starts = [m.start() for m in re.finditer(re.escape('<span class="history-section-title">'), APP_JS)]
    assert starts, "the fold titles moved — re-anchor"
    for i in starts:
        assert APP_JS[:i].rstrip().endswith("<summary>"), (
            "the caret hangs left of every .history-section-title: outside a <summary> it fakes a disclosure, "
            "after a sibling it lands on that sibling instead of the gutter")


def test_the_gutter_holds_the_caret():
    caret = _block(".history-section-title::before")
    assert _decl(caret, "letter-spacing") == "0", "the title's tracking would trail the glyph and widen its gap"
    need = _px(_decl(caret, "margin-right")) + _px(_decl(caret, "font-size"))
    pad = _px(_decl(_rule(".dlg.dlg-drawer-left #info-dlg-body"), "padding"))
    assert need < pad, "the caret must stay visible inside the card body's padding"


def test_history_sections_render_only_in_the_info_card():
    web = REPO / "app" / "web"
    others = [p for p in web.rglob("*") if p.suffix in (".html", ".js", ".css")
              and p.name not in ("app.js", "app.css")]
    assert others and not [p.name for p in others if "history-section" in p.read_text()], (
        "the gutter caret is scoped by the base rule because only the INFO card uses it")
    # v0.51.341: app.js itself — every emitter (a class value, not a `.history-section` selector) sits in the card.
    start = APP_JS.index("async function openInfoDialog(")
    end = APP_JS.index("function closeInfoDialog() {", start)
    emitters = [m.start() for m in re.finditer(r"(?<!\.)\bhistory-section\b", APP_JS)
                if not APP_JS[APP_JS.rindex("\n", 0, m.start()) + 1:m.start()].lstrip().startswith("//")]
    assert len(emitters) >= 3, "the folds, PROVENANCE and HISTORY emit it — re-anchor"
    outside = [APP_JS.count("\n", 0, i) + 1 for i in emitters if not start <= i < end]
    assert not outside, (
        f"app.js line(s) {outside} emit .history-section outside openInfoDialog…closeInfoDialog — "
        "its caret hangs into a gutter only the INFO card body pads")


def test_play_label_text_and_its_badge_read_as_two_words():
    """The badge lives in the <dt> now: without whitespace between them, a screen
    reader and a copy read 'plex servesSERVING' (the dt is a flex column, so the
    space is invisible)."""
    joined = _joined(APP_JS)
    starts = [m.end() for m in re.finditer(re.escape(_PLAY_DT), joined)]
    assert len(starts) >= 3, "the bare card, the full card's plex serves and motif file builders"
    for i in starts:
        label = joined[i:joined.index('<span class="tier-badge', i)]
        assert label.strip() and label.endswith(" "), f"no separator after {label!r}"
    out = _render_bare("{plex_title:'Bleach',year:2004,plex_media_type:'show',"
                       "section_id:'3',rating_key:'778',plex_has_theme:1}")
    dt = out[out.index(_PLAY_DT):out.index("</dt>", out.index(_PLAY_DT))]
    assert re.sub(r"<[^>]+>", "", dt) == "plex serves SERVING"
