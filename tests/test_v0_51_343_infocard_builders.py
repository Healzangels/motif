"""v0.51.343: the INFO card's JS, cleaned up (tag 3, infocard-js).

1. The card's four players built the row ▶'s two URLs by hand. They route through
   lib/quick-play.js fileSrc / plexSrc now; the real call sites render under node with
   the query each asked for, and no app.js line spells either path.
2. libraryState.quickPlay stored a src nothing read. computeQuickPlay's collapsed ladder
   is pinned over an exhaustive grid in tests/js/test_quick_play.js.
3. A card with no row loaded (a deep link, a row off this page) hid the Plex player
   whenever motif held a file; the payload decides now, by computeQuickPlay's rule.
4. The STANDING BY tooltip and the backup strip said Plex serves whether or not it did;
   they key on the headline's reading now.
"""
from __future__ import annotations

import html
import json
import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

from _slice_helpers import blank_js_comments, slice_between

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
QUICK_PLAY = REPO / "app" / "web" / "static" / "lib" / "quick-play.js"
_NODE = shutil.which("node")

if os.environ.get("MOTIF_REQUIRE_NODE") and not _NODE:
    raise RuntimeError("MOTIF_REQUIRE_NODE=1 but node is not on PATH — the INFO card harness would silently not run")

needs_node = pytest.mark.skipif(not _NODE, reason="node not installed")

_HELPER = ("  function _plexBackupState(data) {", "\n  }\n")
_SRC_LETTER = ("  function computeSrcLetter(it) {", "\n  }\n")
_AUDIO_BLOCK = ("const audioBlock = lf", "      : '';")
_PREVIEW = ("const _previewSrc = ", ";\n")
_LOUD_CONTROLS = ('controls = `<dt class="info-ctl-label">action</dt><dd class="loud-controls">\n', "</dd>`;")
_PLEX_BLOCK = ("const _plexRk = ", "const _onDiskRows = ")
_BARE = ("function renderBareInfoCard(", "\n  function _bindPlexThemePlayer(body) {")
_PLACEHOLDER = ("const recoveryPlaceholder = recoverySectionId", "      : '';")
_STRIP = ("const overrideIntent = (data.override && data.override.intent) || null;", "    const _noteParts = [];")
_HELD = ("    function _heldWord(sk) {", "\n    }")
_HEADLINE = ("    function _derivePlaybackSourceLabel() {", "\n    }")
_TOGGLE = ("function quickPlayToggle(btn) {", "\n  }\n")
_SLOT = ("const _qp = (window.motifQuickPlay", "const titleTooltip = ")
_UNDEF = "__undefined__"


def _cut(anchors: tuple[str, str], keep_end: bool = False) -> str:
    s = slice_between(APP_JS, *anchors)
    return s + anchors[1] if keep_end else s


_PRELUDE = r"""
const vm = require('vm');
const payload = JSON.parse(require('fs').readFileSync(0, 'utf8'));
const lib = require(payload.qp);
const window = { motifQuickPlay: lib };
const htmlEscape = (s) => String(s === undefined || s === null ? '' : s)
  .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');
const U = (v) => (v === '__undefined__' ? undefined : v);
const run = (code, ctx) => vm.runInNewContext(code, Object.assign({ window, htmlEscape }, ctx));
"""


def _node(script: str, payload: dict):
    payload = {"qp": str(QUICK_PLAY), **payload}
    r = subprocess.run([_NODE, "-e", _PRELUDE + script], input=json.dumps(payload),
                       capture_output=True, text=True, timeout=120, cwd=REPO)
    assert r.returncode == 0, r.stderr[-2000:]
    return json.loads(r.stdout)


# ── 1. every player URL comes from the shared builders ─────────────────────────

_FILE_SITES = r"""
const out = payload.cases.map((c) => {
  const [mt, tmdb, sectionId, ratingKey] = c.map(U);
  const card = run(`${payload.helper}\n${payload.audio}\naudioBlock;`, {
    lf: { section_id: '3', edition_key: '', file_sha256: 'sha' }, t: { media_type: mt, tmdb_id: tmdb },
    sectionId, ratingKey, placements: [], lfIsBackupOnly: false, data: {} });
  const preview = run(`${payload.preview};\n_previewSrc;`, { lf: { media_type: mt, tmdb_id: tmdb }, sectionId, ratingKey });
  return { card, preview };
});
process.stdout.write(JSON.stringify(out));
"""

# (media_type, tmdb_id, sectionId, ratingKey) as the card passes them → the URL the items endpoint gets
_FILE_CASES = [
    (("tv", 777, "3", "1001"), "/api/items/tv/777/theme.mp3?section_id=3&rating_key=1001"),
    (("movie", 120, "1", _UNDEF), "/api/items/movie/120/theme.mp3?section_id=1"),
    (("movie", 120, "", "222"), "/api/items/movie/120/theme.mp3?rating_key=222"),
    (("collection", -12, _UNDEF, _UNDEF), "/api/items/collection/-12/theme.mp3"),
    (("tv", 777, None, 0), "/api/items/tv/777/theme.mp3"),
    (("tv", "7 7", "a b", "rk&1"), "/api/items/tv/7%207/theme.mp3?section_id=a%20b&rating_key=rk%261"),
    (("movie", 5, 4, 9001), "/api/items/movie/5/theme.mp3?section_id=4&rating_key=9001"),
]


@needs_node
def test_motif_file_players_serve_the_query_each_site_asks_for():
    out = _node(_FILE_SITES, {"helper": _cut(_HELPER, keep_end=True), "audio": _cut(_AUDIO_BLOCK, keep_end=True),
                              "preview": _cut(_PREVIEW), "cases": [c for c, _ in _FILE_CASES]})
    for (case, want), got in zip(_FILE_CASES, out):
        m = re.search(r'<audio controls preload="auto" src="([^"]*)"', got["card"])
        assert m and html.unescape(m.group(1)) == want, (case, got["card"][:300])
        assert got["preview"] == want, (case, got["preview"])


def _code_lines():
    # v0.51.344: comments blanked (strings, templates, regexes kept) — a trailing // note naming an endpoint is not a URL
    return list(enumerate(blank_js_comments(APP_JS).splitlines(), 1))


def test_no_app_js_line_builds_a_player_url_by_hand():
    """Behaviour is driven above and in test_v0_51_341_ui_residuals_2.py; this keeps a fifth
    hand-built copy of either endpoint from appearing beside them."""
    hand = [(n, line.strip()) for n, line in _code_lines()
            if re.search(r"/api/plex/theme/|\}/theme\.mp3|/api/items/[^\n]*theme\.mp3", line)]
    assert hand == [], f"build player URLs with window.motifQuickPlay.fileSrc / plexSrc: {hand}"
    calls = [m.start() for m in re.finditer(r"motifQuickPlay\.(?:fileSrc|plexSrc)\(", blank_js_comments(APP_JS))]
    spans = []
    for start, end in (_AUDIO_BLOCK, _PREVIEW, _PLEX_BLOCK, _BARE):
        i = APP_JS.index(start)
        spans.append((i, APP_JS.index(end, i)))
    assert calls and all(any(a <= i < b for a, b in spans) for i in calls), (
        "a builder call outside the players these tests render — add its site to the harness")
    assert all(any(a <= i < b for i in calls) for a, b in spans), "each player routes through the lib"


def test_the_comment_blanker_drops_comments_and_keeps_code():
    src = ("const a = '// kept'; // gone /api/plex/theme/1.mp3\n"
           "/* gone\n   gone /api/items/x/theme.mp3 */ const b = `t // kept ${f(`/* kept */`)} ${'x'}`;\n"
           "const r = s.replace(/'/g, '&#39;'); // gone\n"
           "const d = a / b; // gone\n")
    out = blank_js_comments(src)
    assert len(out) == len(src) and out.count("\n") == src.count("\n"), "positions and line numbers hold"
    assert "gone" not in out and "/api/" not in out, out
    assert out.count("kept") == 3 and "replace(/'/g, '&#39;');" in out and "a / b;" in out, out
    blanked = blank_js_comments(APP_JS)
    assert len(blanked) == len(APP_JS) and blanked.count("\n") == APP_JS.count("\n")


@needs_node
def test_blanked_app_js_is_still_the_same_program():
    r = subprocess.run([_NODE, "-e", "new (require('vm').Script)(require('fs').readFileSync(0, 'utf8'))"],
                       input=blank_js_comments(APP_JS), capture_output=True, text=True, timeout=60)
    assert r.returncode == 0, f"blanking reached into code, so app.js no longer parses: {r.stderr[-800:]}"


# ── 1b. no lib, no player — the card still renders ─────────────────────────────

_NO_LIB_HARNESS = r"""
const lf = { section_id: '3', edition_key: '', file_sha256: 'sha', media_type: 'tv', tmdb_id: 777,
             file_path: 'tv/x/theme.mp3', last_place_attempt_reason: null, source_kind: 'themerrdb' };
const cardCtx = { t: { media_type: 'tv', tmdb_id: 777 }, sectionId: '3', ratingKey: '1001', placements: [],
  lfIsBackupOnly: false, data: { plex_has_theme: 1, plex_theme_verified_ok: 1, plex_rating_key: '1001' },
  libraryState: { items: [] } };
const out = {};
for (const [name, w] of [['lib', window], ['none', {}]]) {
  const ctx = (extra) => Object.assign({ window: w }, cardCtx, extra);
  const safe = (fn) => { try { return fn(); } catch (e) { return `THREW ${e.name}: ${e.message}`; } };
  out[name] = {
    bare: safe(() => run(`${payload.bare}\nrenderBareInfoCard(row);`,
                         { window: w, row: { plex_title: 'X', rating_key: '1001', plex_has_theme: 1 } })),
    plex: safe(() => run(`${payload.block}\nplexThemeBlock;`, ctx({ lf: null }))),
    audio: safe(() => run(`${payload.helper}\n${payload.audio}\naudioBlock;`, ctx({ lf }))),
    loudness: safe(() => run(`${payload.preview};\nlet controls;\n${payload.controls}\ncontrols;`, ctx({ lf }))),
  };
}
process.stdout.write(JSON.stringify(out));
"""


@needs_node
def test_a_card_without_lib_quick_play_renders_every_block_minus_its_players():
    out = _node(_NO_LIB_HARNESS, {
        "bare": _cut(_BARE), "block": _cut(_PLEX_BLOCK), "helper": _cut(_HELPER, keep_end=True),
        "audio": _cut(_AUDIO_BLOCK, keep_end=True), "preview": _cut(_PREVIEW),
        "controls": _cut(_LOUD_CONTROLS, keep_end=True)})
    lib, none = out["lib"], out["none"]
    for name, got in {**{f"lib {k}": v for k, v in lib.items()}, **{f"none {k}": v for k, v in none.items()}}.items():
        assert not got.startswith("THREW"), f"v0.51.344: {name} — {got}"
    # with the lib every site plays, so the checks below are not vacuous
    assert all("<audio" in lib[k] for k in lib), lib
    assert 'data-act="loud-preview"' in lib["loudness"]
    # v0.51.344: without it no site emits an <audio> (an empty src fetches the page URL), and the rest stays
    assert all("<audio" not in none[k] for k in none), none
    assert none["plex"] == "", none["plex"]
    assert "// plex metadata" in none["bare"], none["bare"]
    assert 'data-act="edit-audio"' in none["audio"] and "tier-badge" in none["audio"], none["audio"]
    assert 'data-act="loud-preview"' not in none["loudness"], "no audition button without its player"
    assert 'data-act="loud-normalize"' in none["loudness"], none["loudness"]


# ── 2. the row's play state holds what its readers read ────────────────────────

_TOGGLE_HARNESS = r"""
const audio = { src: '', paused: true, currentTime: 3,
  pause() { this.paused = true; }, play() { this.paused = false; return Promise.resolve(); } };
const libraryState = { quickPlay: null };
const noop = () => {};
const it = { theme_media_type: 'movie', theme_tmdb: 120, section_id: '1', rating_key: '9001', plex_title: 'Heat & Co',
  media_folder: '/m/Heat', file_path: 'movie/heat/theme.mp3' };
const slot = run(`${payload.slot}\nquickPlaySlot;`, { it, libraryState, selKey: 'movie|120|1' });
const dataset = {};
const unescape = (s) => s.replace(/&quot;/g, '"').replace(/&#39;/g, "'").replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&amp;/g, '&');
for (const m of slot.matchAll(/\sdata-([\w-]+)="([^"]*)"/g)) dataset[m[1].replace(/-([a-z])/g, (_, ch) => ch.toUpperCase())] = unescape(m[2]);
const btn = { dataset };
run(`${payload.toggle}\nquickPlayToggle(btn);`, { btn, libraryState, _quickPlayAudio: () => audio,
  _pauseOtherAudio: noop, _quickPlayNote: noop, _paintQuickPlay: noop, _paintNowPlaying: noop });
process.stdout.write(JSON.stringify({ state: libraryState.quickPlay, audioSrc: audio.src, dataset }));
"""


@needs_node
def test_quick_play_state_stores_exactly_the_fields_its_readers_read():
    got = _node(_TOGGLE_HARNESS, {"toggle": _cut(_TOGGLE, keep_end=True), "slot": _cut(_SLOT)})
    assert got["dataset"].get("src") and got["audioSrc"] == got["dataset"]["src"], "the player plays the row button's URL"
    code = blank_js_comments(APP_JS)
    helpers = ("function _quickPlayAudio()", "function renderLibraryRow(it) {")
    lo = code.index(helpers[0])
    region = slice_between(code, *helpers)
    # v0.51.344: readers file-wide, plus the aliases the play-state helpers bind (was two slices and a literal field set)
    reads = set(re.findall(r"libraryState\.quickPlay\.(\w+)", code))
    aliases = list(re.finditer(r"\b(?:const|let|var)\s+(\w+)\s*=\s*libraryState\.quickPlay\s*;", code))
    assert all(lo <= a.start() < lo + len(region) for a in aliases), (
        "an alias of libraryState.quickPlay outside the play-state helpers — its reads go unseen here")
    for name in {a.group(1) for a in aliases}:
        reads |= set(re.findall(rf"\b{re.escape(name)}\.(\w+)", region))
    assert reads, "the play state has readers"
    assert set(got["state"]) == reads, f"v0.51.343: the state carries exactly what is read — {got['state']} vs {reads}"
    assert got["state"] == {k: got["dataset"][k] for k in reads}, (got["state"], got["dataset"])


# ── 3. the card's Plex player follows the row ▶'s rule ─────────────────────────

_GATE_HARNESS = r"""
const player = (h) => { const m = h.match(/<audio[^>]*\bsrc="([^"]*)"[^>]*data-plex-theme="1"/); return m ? m[1] : null; };
const kind = (r) => { const q = lib.computeQuickPlay(r); return q ? q.kind : null; };
const card = (c, ctx) => player(run(`${payload.helper}\n${payload.letter}\n${payload.block}\nplexThemeBlock;`, Object.assign({
  t: { media_type: 'tv', tmdb_id: 777 }, sectionId: '3' }, ctx, {
  lfIsBackupOnly: !!ctx.lf && ctx.lf.last_place_attempt_reason === 'backup_only' })));
const out = payload.cases.map((c) => {
  const plays = kind(c.row);
  const other = Object.assign({}, c.row, { rating_key: '9', file_path: null, plex_has_theme: 1, plex_theme_verified_ok: 1 });
  // the loaded row must win over a payload that says the opposite
  const contrary = plays === 'plex' ? { plex_has_theme: 0, plex_rating_key: '1001' }
    : { plex_has_theme: 1, plex_theme_verified_ok: 1, plex_rating_key: '1001' };
  return {
    plays, plexSrc: lib.plexSrc({ rating_key: '1001' }),
    deepLink: card(c, { ratingKey: undefined, lf: c.lf, placements: c.placements, data: c.data, libraryState: { items: [other] } }),
    loaded: card(c, { ratingKey: '1001', lf: null, placements: [], data: contrary, libraryState: { items: [other, c.row] } }),
    bare: player(run(`${payload.bare}\nrenderBareInfoCard(row);`, { row: Object.assign({ plex_title: 'X' }, c.row) })),
  };
});
process.stdout.write(JSON.stringify(out));
"""

_PLACED = {"no": [], "sidecar": [{"media_folder": "/tv/x", "placement_kind": "hardlink"}],
           "plex_upload": [{"media_folder": "", "placement_kind": "plex_upload"}]}


def _gate_cells():
    cells = []
    for has in (None, 0, 1):
        for ok in (None, 0, 1, 2):
            for m_sidecar in (0, 1):
                cells.append(("none", "no", None, has, ok, 1, m_sidecar))
                cells += [("file", placed, reason, has, ok, present, m_sidecar)
                          for placed in _PLACED for reason in ("backup_only", None) for present in (1, 0)]
    return cells


def _gate_case(cell):
    on_disk, placed, reason, has, ok, present, m_sidecar = cell
    lf = None if on_disk == "none" else {
        "source_kind": "themerrdb", "file_path": "tv/x/theme.mp3", "last_place_attempt_reason": reason,
        "canonical_present": present, "section_id": "3", "edition_key": "", "file_sha256": "sha"}
    placements = _PLACED[placed]
    # the same state as /api/library's row carries it: the placement join, the live canonical stat, the sidecar flag
    row = {"theme_media_type": "tv", "theme_tmdb": 777, "section_id": "3", "rating_key": "1001",
           "media_folder": placements[0]["media_folder"] if placements else None,
           "placement_kind": placements[0]["placement_kind"] if placements else None,
           "file_path": lf["file_path"] if lf else None, "canonical_missing": bool(lf) and present == 0,
           "plex_has_theme": has, "plex_theme_verified_ok": ok, "plex_local_theme": m_sidecar,
           "last_place_attempt_reason": reason if lf else None, "source_kind": "themerrdb" if lf else None}
    return {"lf": lf, "placements": placements, "row": row,
            "data": {"plex_has_theme": has, "plex_theme_verified_ok": ok, "plex_rating_key": "1001"}}


@needs_node
def test_every_card_path_shows_the_plex_player_exactly_when_the_row_would_play_plex():
    cells = _gate_cells()
    out = _node(_GATE_HARNESS, {"helper": _cut(_HELPER, keep_end=True), "letter": _cut(_SRC_LETTER, keep_end=True),
                                "block": _cut(_PLEX_BLOCK), "bare": _cut(_BARE),
                                "cases": [_gate_case(c) for c in cells]})
    paths = ("deepLink", "loaded", "bare")
    for path in paths:
        shown = [got[path] is not None for got in out]
        assert any(shown) and not all(shown), f"{path}: the grid reaches both sides of the rule"
    for cell, got in zip(cells, out):
        # v0.51.344: one rule — a deep link, a loaded row and the bare card all agree with the row ▶
        for path in paths:
            assert (got[path] is not None) == (got["plays"] == "plex"), (path, cell, got)
            if got[path] is not None:
                assert got[path] == got["plexSrc"], (path, cell, got)


# ── 4. the tooltip and the strip say what the headline says ────────────────────

_WORDS_HARNESS = r"""
const out = payload.cells.map(([has, ok, sk, synthetic, stamp]) => {
  const data = { plex_has_theme: has, plex_theme_verified_ok: ok, resolved: true,
                 override: { intent: 'backup', source_kind: sk, synthetic } };
  const lf = { source_kind: sk, source_video_id: 'dQw4w9WgXcQ', last_place_attempt_reason: 'backup_only',
               section_id: '3', edition_key: '', file_sha256: 'sha', file_path: 'tv/x/theme.mp3' };
  const ctx = { data, lf, _ambiguousCut: false, placements: [], lfIsBackupOnly: true,
                t: { media_type: 'tv', tmdb_id: 777 }, sectionId: '3', ratingKey: '1001' };
  const headline = run(`${payload.helper}\n${payload.headline}\n_derivePlaybackSourceLabel();`, ctx);
  const audio = run(`${payload.helper}\n${payload.audio}\naudioBlock;`, ctx);
  const placeholder = run(`${payload.helper}\n${payload.placeholder}\nrecoveryPlaceholder;`,
                          Object.assign({ recoverySectionId: 'recovery-section' }, ctx));
  const stamped = placeholder.match(/data-plex-state="([^"]*)"/);
  const strip = run(`${payload.strip}\n({ title: sectionTitleText, caption: intentFlipCaption, btns: intentFlipBtnsHtml });`, {
    data, section: { dataset: { plexState: stamp && stamped ? stamped[1] : undefined } },
    mediaType: 'tv', tmdbId: 777, ackedOnly: false });
  const q = lib.computeQuickPlay({ theme_media_type: 'tv', theme_tmdb: 777, section_id: '3', rating_key: '1001',
    media_folder: null, placement_kind: null, file_path: lf.file_path, canonical_missing: false,
    plex_has_theme: has, plex_theme_verified_ok: ok, last_place_attempt_reason: 'backup_only' });
  return Object.assign({ headline, audio, plays: q ? q.kind : null }, strip);
});
process.stdout.write(JSON.stringify(out));
"""


@needs_node
def test_standing_by_tooltip_and_backup_strip_say_plex_serves_exactly_when_the_headline_does():
    cells = [(has, ok, sk, synthetic, True) for has in (None, 0, 1) for ok in (None, 0, 1)
             for sk in ("url", "themerrdb", "adopt", "plex_cloud") for synthetic in (False, True)]
    cells += [(1, None, "url", False, False), (0, None, "themerrdb", True, False)]  # a strip with no data-plex-state
    out = _node(_WORDS_HARNESS, {
        "cells": cells, "helper": _cut(_HELPER, keep_end=True),
        "headline": _cut(_HELD) + "\n    }\n" + _cut(_HEADLINE) + "\n    }",
        "audio": _cut(_AUDIO_BLOCK, keep_end=True), "placeholder": _cut(_PLACEHOLDER, keep_end=True),
        "strip": _cut(_STRIP)})
    seen = set()
    for cell, got in zip(cells, out):
        has, ok, sk, synthetic, stamp = cell
        promote = re.search(r'data-act="promote-to-active"[^>]*\btitle="([^"]*)"', got["btns"])
        if not stamp:
            assert promote, ("v0.51.344: a strip with no data-plex-state keeps PROMOTE", cell)
            continue
        badge = re.search(r'<span class="tier-badge tier-badge-standing" title="([^"]*)">STANDING BY</span>', got["audio"])
        assert badge, ("STANDING BY keeps its class and label", cell, got["audio"][:400])
        tip, title, caption, head = html.unescape(badge.group(1)), got["title"], got["caption"], got["headline"]
        serves = head.endswith(" on disk as backup · Plex serves its own theme")
        absent = head.endswith(" on disk as backup · this item is not in Plex")
        seen.add("serves" if serves else "absent" if absent else "silent")
        assert serves == (got["plays"] == "plex"), ("the headline's Plex reading is the row ▶'s", cell, head)
        assert title.startswith("✓ BACKUP READY — "), (cell, title)
        assert ("keeps serving its own theme" in tip) == serves, (cell, tip)
        assert ("DEFERRING TO PLEX" in title) == serves, (cell, title)
        assert ("over Plex's theme" in caption) == serves, (cell, caption)
        # v0.51.344 (operator decision b): PROMOTE only where there is a Plex item to deploy into
        assert bool(promote) == (not absent), (cell, got["btns"])
        promote_tip = html.unescape(promote.group(1)) if promote else ""
        cloud = synthetic and sk == "plex_cloud"
        if promote and cloud:
            assert "re-upload trick" in promote_tip, (cell, promote_tip)
        elif promote:
            assert ("over Plex's theme" in promote_tip) == serves, (cell, promote_tip)
            assert ("its downloaded copy" if synthetic else "your URL") in promote_tip, (cell, promote_tip)
        if absent:
            assert "not in Plex" in tip and "NOT IN PLEX" in title and "not in Plex" in caption, (cell, tip, title, caption)
            assert "PROMOTE" not in tip and "PROMOTE" not in caption, (cell, tip, caption)
        elif not serves:
            # v0.51.344: has_theme 0 or a verify 404 — one phrase on every surface; nothing records that Plex once served
            surfaces = {"headline": head, "tooltip": tip, "title": title, "caption": caption}
            if not cloud:
                surfaces["promote tip"] = promote_tip
            for name, text in surfaces.items():
                assert "plex serves no theme" in text.casefold(), (cell, name, text)
                assert "no longer" not in text.casefold(), (cell, name, text)
            assert "PROMOTE TO ACTIVE" in tip and "PROMOTE" in caption, (cell, tip, caption)
    assert seen == {"serves", "absent", "silent"}, seen
