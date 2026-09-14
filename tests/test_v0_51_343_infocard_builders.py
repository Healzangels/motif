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

from _slice_helpers import slice_between

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
QUICK_PLAY = REPO / "app" / "web" / "static" / "lib" / "quick-play.js"
_NODE = shutil.which("node")

if os.environ.get("MOTIF_REQUIRE_NODE") and not _NODE:
    raise RuntimeError("MOTIF_REQUIRE_NODE=1 but node is not on PATH — the INFO card harness would silently not run")

needs_node = pytest.mark.skipif(not _NODE, reason="node not installed")

_HELPER = ("  function _plexBackupState(data) {", "\n  }\n")
_AUDIO_BLOCK = ("const audioBlock = lf", "      : '';")
_PREVIEW = ("const _previewSrc = ", ";\n")
_PLEX_BLOCK = ("const _plexRk = ", "const _onDiskRows = ")
_BARE = ("function renderBareInfoCard(", "\n  function _bindPlexThemePlayer(body) {")
_PLACEHOLDER = ("const recoveryPlaceholder = recoverySectionId", "      : '';")
_STRIP = ("const overrideIntent = (data.override && data.override.intent) || null;", "    const _noteParts = [];")
_HELD = ("    function _heldWord(sk) {", "\n    }")
_HEADLINE = ("    function _derivePlaybackSourceLabel() {", "\n    }")
_TOGGLE = ("function quickPlayToggle(btn) {", "\n  }\n")
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
    return [(n, line) for n, line in enumerate(APP_JS.splitlines(), 1) if not line.lstrip().startswith("//")]


def test_no_app_js_line_builds_a_player_url_by_hand():
    """Behaviour is driven above and in test_v0_51_341_ui_residuals_2.py; this keeps a fifth
    hand-built copy of either endpoint from appearing beside them."""
    hand = [(n, line.strip()) for n, line in _code_lines()
            if re.search(r"/api/plex/theme/|\}/theme\.mp3|/api/items/[^\n]*theme\.mp3", line)]
    assert hand == [], f"build player URLs with window.motifQuickPlay.fileSrc / plexSrc: {hand}"
    calls = [m.start() for m in re.finditer(r"motifQuickPlay\.(?:fileSrc|plexSrc)\(", APP_JS)]
    spans = []
    for start, end in (_AUDIO_BLOCK, _PREVIEW, _PLEX_BLOCK, _BARE):
        i = APP_JS.index(start)
        spans.append((i, APP_JS.index(end, i)))
    assert calls and all(any(a <= i < b for a, b in spans) for i in calls), (
        "a builder call outside the players these tests render — add its site to the harness")
    assert all(any(a <= i < b for i in calls) for a, b in spans), "each player routes through the lib"


# ── 2. the row's play state holds what its readers read ────────────────────────

_TOGGLE_HARNESS = r"""
const audio = { src: '', paused: true, currentTime: 3,
  pause() { this.paused = true; }, play() { this.paused = false; return Promise.resolve(); } };
const libraryState = { quickPlay: null };
const noop = () => {};
const btn = { dataset: { key: 'movie|120|1', src: '/api/items/movie/120/theme.mp3?section_id=1', kind: 'file', title: 'Heat' } };
run(`${payload.toggle}\nquickPlayToggle(btn);`, { btn, libraryState, _quickPlayAudio: () => audio,
  _pauseOtherAudio: noop, _quickPlayNote: noop, _paintQuickPlay: noop, _paintNowPlaying: noop });
process.stdout.write(JSON.stringify({ state: libraryState.quickPlay, audioSrc: audio.src }));
"""


@needs_node
def test_quick_play_state_stores_exactly_the_fields_its_readers_read():
    got = _node(_TOGGLE_HARNESS, {"toggle": _cut(_TOGGLE, keep_end=True)})
    assert got["audioSrc"] == "/api/items/movie/120/theme.mp3?section_id=1", "the player still plays the button's URL"
    region = (slice_between(APP_JS, "function _quickPlayAudio()", "function renderLibraryRow(it) {")
              + slice_between(APP_JS, "const _qp = (window.motifQuickPlay", "const quickPlaySlot = "))
    reads = set(re.findall(r"libraryState\.quickPlay\.(\w+)", region)) | set(re.findall(r"\bon\.(\w+)", region))
    assert reads == {"key", "kind", "title"}, reads
    assert set(got["state"]) == reads, f"v0.51.343: the state carries no field nothing reads — {got['state']}"
    assert got["state"] == {"key": "movie|120|1", "kind": "file", "title": "Heat"}


# ── 3. a card with no row loaded decides the Plex player from its payload ──────

_GATE_HARNESS = r"""
const out = payload.cases.map((c) => {
  const lf = c.lf;
  const html = run(`${payload.helper}\n${payload.block}\nplexThemeBlock;`, {
    ratingKey: U(c.ratingKey), lf, lfIsBackupOnly: !!lf && lf.last_place_attempt_reason === 'backup_only',
    data: c.data, libraryState: { items: c.rows }, computeSrcLetter: (it) => it.letter });
  const row = { theme_media_type: 'tv', theme_tmdb: 777, section_id: '3', rating_key: c.data.plex_rating_key,
    media_folder: c.placed ? '/x' : null, placement_kind: null, file_path: lf ? 'tv/x/theme.mp3' : null,
    canonical_missing: 0, plex_has_theme: c.data.plex_has_theme, plex_theme_verified_ok: c.data.plex_theme_verified_ok,
    last_place_attempt_reason: lf ? lf.last_place_attempt_reason : null };
  const qp = lib.computeQuickPlay(row);
  const m = html.match(/<audio[^>]*\bsrc="([^"]*)"[^>]*data-plex-theme="1"/);
  return { player: m ? m[1] : null, plays: qp ? qp.kind : null };
});
process.stdout.write(JSON.stringify(out));
"""

_FILES = {
    "backup": ({"source_kind": "themerrdb", "last_place_attempt_reason": "backup_only"}, False),
    "unplaced": ({"source_kind": "url", "last_place_attempt_reason": None}, False),
    "placed": ({"source_kind": "themerrdb", "last_place_attempt_reason": None}, True),
}


def _gate(cases):
    return _node(_GATE_HARNESS, {"helper": _cut(_HELPER, keep_end=True), "block": _cut(_PLEX_BLOCK), "cases": cases})


@needs_node
def test_a_deep_linked_card_shows_the_plex_player_exactly_when_the_row_would_play_plex():
    cells = [(name, has, ok) for name in _FILES for has in (None, 0, 1) for ok in (None, 0, 1)]
    cases = [{"lf": _FILES[name][0], "placed": _FILES[name][1], "ratingKey": _UNDEF, "rows": [],
              "data": {"plex_has_theme": has, "plex_theme_verified_ok": ok, "plex_rating_key": "1001"}}
             for name, has, ok in cells]
    shown = set()
    for cell, got in zip(cells, _gate(cases)):
        assert (got["player"] is not None) == (got["plays"] == "plex"), (cell, got)
        if got["player"] is not None:
            assert got["player"] == "/api/plex/theme/1001.mp3", (cell, got)
            shown.add(cell)
    # v0.51.343: the fix — a standing-by backup beside a serving Plex, opened with no row on the page
    assert shown == {("backup", 1, None), ("backup", 1, 1)}, shown


@needs_node
def test_a_loaded_row_still_decides_by_its_src_letter():
    data = {"plex_has_theme": 1, "plex_theme_verified_ok": None, "plex_rating_key": "1001"}
    p_row, t_row, other_page = _gate([
        {"lf": _FILES["unplaced"][0], "ratingKey": "1001", "rows": [{"rating_key": "1001", "letter": "P"}], "data": data},
        {"lf": _FILES["backup"][0], "ratingKey": "1001", "rows": [{"rating_key": "1001", "letter": "T"}], "data": data},
        {"lf": _FILES["backup"][0], "ratingKey": "1001", "rows": [{"rating_key": "9", "letter": "T"}], "data": data},
    ])
    assert p_row["player"] == "/api/plex/theme/1001.mp3", "SRC=P beside motif's file shows Plex's player"
    assert t_row["player"] is None, "a loaded non-P row keeps it hidden"
    assert other_page["player"] == "/api/plex/theme/1001.mp3", "a row off this page falls to the payload rule"
    no_file, = _gate([{"lf": None, "ratingKey": _UNDEF, "rows": [], "data": data}])
    assert no_file["player"] == "/api/plex/theme/1001.mp3", "nothing on disk: Plex's player, as before"


# ── 4. the tooltip and the strip say what the headline says ────────────────────

_WORDS_HARNESS = r"""
const out = payload.cells.map(([has, ok]) => {
  const data = { plex_has_theme: has, plex_theme_verified_ok: ok, resolved: true,
                 override: { intent: 'backup', source_kind: 'themerrdb' } };
  const lf = { source_kind: 'themerrdb', source_video_id: 'dQw4w9WgXcQ', last_place_attempt_reason: 'backup_only',
               section_id: '3', edition_key: '', file_sha256: 'sha' };
  const ctx = { data, lf, _ambiguousCut: false, placements: [], lfIsBackupOnly: true,
                t: { media_type: 'tv', tmdb_id: 777 }, sectionId: '3', ratingKey: '1001' };
  const headline = run(`${payload.helper}\n${payload.headline}\n_derivePlaybackSourceLabel();`, ctx);
  const audio = run(`${payload.helper}\n${payload.audio}\naudioBlock;`, ctx);
  const placeholder = run(`${payload.helper}\n${payload.placeholder}\nrecoveryPlaceholder;`,
                          Object.assign({ recoverySectionId: 'recovery-section' }, ctx));
  const stamped = placeholder.match(/data-plex-state="([^"]*)"/);
  const strip = run(`${payload.strip}\n({ title: sectionTitleText, caption: intentFlipCaption });`, {
    data, section: { dataset: { plexState: stamped ? stamped[1] : undefined } },
    mediaType: 'tv', tmdbId: 777, ackedOnly: false });
  return { headline, audio, title: strip.title, caption: strip.caption };
});
process.stdout.write(JSON.stringify(out));
"""


@needs_node
def test_standing_by_tooltip_and_backup_strip_say_plex_serves_exactly_when_the_headline_does():
    cells = [(has, ok) for has in (None, 0, 1) for ok in (None, 0, 1)]
    out = _node(_WORDS_HARNESS, {
        "cells": cells, "helper": _cut(_HELPER, keep_end=True),
        "headline": _cut(_HELD) + "\n    }\n" + _cut(_HEADLINE) + "\n    }",
        "audio": _cut(_AUDIO_BLOCK, keep_end=True), "placeholder": _cut(_PLACEHOLDER, keep_end=True),
        "strip": _cut(_STRIP)})
    serving = set()
    for cell, got in zip(cells, out):
        badge = re.search(r'<span class="tier-badge tier-badge-standing" title="([^"]*)">STANDING BY</span>', got["audio"])
        assert badge, ("STANDING BY keeps its class and label", cell, got["audio"][:400])
        tip, title, caption, head = html.unescape(badge.group(1)), got["title"], got["caption"], got["headline"]
        serves = head.endswith(" on disk as backup · Plex serves its own theme")
        absent = head.endswith(" on disk as backup · this item is not in Plex")
        assert title.startswith("✓ BACKUP READY — "), (cell, title)
        assert ("keeps serving its own theme" in tip) == serves, (cell, tip)
        assert ("DEFERRING TO PLEX" in title) == serves, (cell, title)
        assert ("over Plex's theme" in caption) == serves, (cell, caption)
        if serves:
            serving.add(cell)
        elif absent:
            assert "not in Plex" in tip and "NOT IN PLEX" in title and "not in Plex" in caption, (cell, tip, title, caption)
            assert "PROMOTE" not in tip and "PROMOTE" not in caption, (cell, tip, caption)
        else:
            assert "Plex no longer serves a theme" in head, (cell, head)
            assert "no longer serves a theme" in tip and "PROMOTE TO ACTIVE" in tip, (cell, tip)
            assert "NO LONGER SERVES" in title, (cell, title)
            assert "no longer serves a theme" in caption and "PROMOTE" in caption, (cell, caption)
    assert serving == {(1, None), (1, 1)}, serving
