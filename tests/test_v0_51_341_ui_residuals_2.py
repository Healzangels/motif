"""v0.51.341 — residual INFO card findings from the .338-.340 reviews.

1. EDIT AUDIO read its trim range from `body.querySelector('.info-audio')` — the
   card's FIRST player. When the AUDIO group renders both rows ("plex serves"
   above "motif file", the SRC=P backup case) that is Plex's player, so the
   editor opened with Plex's theme length instead of motif's file.
2. Every /api/plex/theme/ builder interpolated the rating key unchecked, so an
   empty key built a keyless player URL and a non-numeric one a player the proxy
   400s. The proxy and lib/quick-play.js's rkOk both accept digits only.

Both run the card's own source under node: the AUDIO rows are rendered from the
builders, parsed into a small DOM, and the real click handler reads it.
"""
from __future__ import annotations

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

_BARE = ("function renderBareInfoCard(", "\n  function _bindPlexThemePlayer(body) {")
_HELPER = ("  function _plexBackupState(data) {", "\n  }\n")  # v0.51.343: the Plex block and the badge read it
_PLEX_BLOCK = ("const _plexRk = ", "const _onDiskRows = ")
_AUDIO_BLOCK = ("const audioBlock = lf", "      : '';")
_AUDIO_ROWS = ("const _audioRows = ", "`;")
_EDIT_HANDLER = ("body.querySelector('button[data-act=\"edit-audio\"]')?.addEventListener('click', (ev) => {", "\n    });")

# a small DOM: enough HTML parsing for the card's own markup, and the compound selectors it queries with
_DOM = r"""
const VOID = new Set(['img', 'br', 'input', 'source', 'hr', 'meta', 'link']);
function el(tag, attrs, parent) {
  const n = { tag, attrs, parent, children: [], listeners: {} };
  n.classList = { contains: (c) => (attrs.class || '').split(/\s+/).includes(c) };
  n.hasAttribute = (a) => a in attrs;
  n.dataset = new Proxy({}, { get: (_, k) => attrs['data-' + String(k).replace(/[A-Z]/g, (c) => '-' + c.toLowerCase())] });
  n.addEventListener = (type, fn) => { n.listeners[type] = fn; };
  n.querySelector = (sel) => { const all = []; walk(n, all); return all.find((d) => matches(d, sel)) || null; };
  n.querySelectorAll = (sel) => { const all = []; walk(n, all); return all.filter((d) => matches(d, sel)); };
  n.closest = (sel) => { for (let c = n; c && c.tag; c = c.parent) if (matches(c, sel)) return c; return null; };
  return n;
}
function walk(n, out) { for (const c of n.children) { out.push(c); walk(c, out); } }
function matches(n, sel) {
  const m = /^([a-z][\w-]*)?((?:\.[\w-]+)*)((?:\[[\w-]+(?:="[^"]*")?\])*)$/.exec(sel.trim());
  if (!m) throw new Error('selector outside the harness grammar: ' + sel);
  if (m[1] && n.tag !== m[1]) return false;
  for (const c of (m[2].match(/\.[\w-]+/g) || [])) if (!n.classList.contains(c.slice(1))) return false;
  for (const a of (m[3].match(/\[[^\]]+\]/g) || [])) {
    const [, k, v] = /^\[([\w-]+)(?:="([^"]*)")?\]$/.exec(a);
    if (!(k in n.attrs) || (v !== undefined && n.attrs[k] !== v)) return false;
  }
  return true;
}
function parse(html) {
  const root = el('#root', {}, null);
  let cur = root;
  const re = /<\/([a-zA-Z][\w-]*)\s*>|<([a-zA-Z][\w-]*)((?:\s+[\w:-]+(?:\s*=\s*"[^"]*")?)*)\s*\/?>/g;
  let m;
  while ((m = re.exec(html))) {
    if (m[1]) { while (cur !== root && cur.tag !== m[1].toLowerCase()) cur = cur.parent; if (cur !== root) cur = cur.parent; continue; }
    const attrs = {};
    for (const a of m[3].matchAll(/([\w:-]+)(?:\s*=\s*"([^"]*)")?/g)) attrs[a[1]] = a[2] === undefined ? '' : a[2];
    const n = el(m[2].toLowerCase(), attrs, cur);
    cur.children.push(n);
    if (!VOID.has(n.tag)) cur = n;
  }
  return root;
}
"""

_EDIT_AUDIO_HARNESS = _DOM + r"""
const vm = require('vm');
const { src, handler, cases, qp } = JSON.parse(require('fs').readFileSync(0, 'utf8'));
const htmlEscape = (s) => String(s === undefined || s === null ? '' : s)
  .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
const out = cases.map((c) => {
  const rows = vm.runInNewContext(src + '\n_audioRows;', {
    htmlEscape, window: { motifQuickPlay: require(qp) }, _ambiguousCut: false, ratingKey: c.rk, sectionId: '3', lfIsBackupOnly: true,
    lf: { section_id: '3', edition_key: '', file_sha256: 'sha-motif', source_kind: 'themerrdb',
          last_place_attempt_reason: 'backup_only' },
    t: { media_type: 'tv', tmdb_id: 777 }, placements: [],
    data: { plex_has_theme: c.plexHasTheme, plex_rating_key: '' },
    // v0.51.344: the loaded row is what the row ▶'s rule reads — a standing-by backup beside c.plexHasTheme
    libraryState: { items: [{ rating_key: c.rk, theme_media_type: 'tv', theme_tmdb: 777, file_path: 'tv/x/theme.mp3',
                              last_place_attempt_reason: 'backup_only', plex_has_theme: c.plexHasTheme }] },
  });
  const body = parse('<dl class="dlg-grid">' + rows + '</dl>');
  const players = body.querySelectorAll('audio.info-audio');
  for (const a of players) a.duration = a.hasAttribute('data-plex-theme') ? c.plexSeconds : c.motifSeconds;
  let opened = null;
  vm.runInNewContext(handler, { body, openEditAudioDialog: (ctx) => { opened = ctx; } });
  const btn = body.querySelector('button[data-act="edit-audio"]');
  btn.listeners.click({ currentTarget: btn, preventDefault() {}, stopPropagation() {} });
  return { players: players.map((a) => (a.hasAttribute('data-plex-theme') ? 'plex' : 'motif')), opened };
});
process.stdout.write(JSON.stringify(out));
"""


def _node(script: str, payload: dict) -> list:
    r = subprocess.run([_NODE, "-e", script], input=json.dumps(payload),
                       capture_output=True, text=True, timeout=60, cwd=REPO)
    assert r.returncode == 0, r.stderr[-1500:]
    return json.loads(r.stdout)


def _cut(anchors: tuple[str, str], keep_end: bool = False) -> str:
    s = slice_between(APP_JS, *anchors)
    return s + anchors[1] if keep_end else s


# ── 1. EDIT AUDIO opens on motif's own file length ────────────────────────────


@needs_node
def test_edit_audio_reads_the_duration_of_its_own_rows_player():
    src = "\n".join((_cut(_HELPER, keep_end=True), _cut(_PLEX_BLOCK), _cut(_AUDIO_BLOCK, keep_end=True),
                     _cut(_AUDIO_ROWS, keep_end=True)))
    handler = _cut(_EDIT_HANDLER, keep_end=True)
    cases = [
        {"rk": "1001", "plexHasTheme": 1, "plexSeconds": 31.5, "motifSeconds": 95.25},
        {"rk": "1001", "plexHasTheme": 0, "plexSeconds": 31.5, "motifSeconds": 88.0},
    ]
    both, motif_only = _node(_EDIT_AUDIO_HARNESS, {"src": src, "handler": handler, "cases": cases,
                                                   "qp": str(QUICK_PLAY)})
    assert both["players"] == ["plex", "motif"], (
        f"the case under test: Plex's player renders first in the AUDIO group — {both['players']}")
    assert both["opened"]["sha"] == "sha-motif" and both["opened"]["id"] == "777", both["opened"]
    assert both["opened"]["duration"] == 95.25, (
        "v0.51.341: EDIT AUDIO trims motif's file — it must read the player in its own row, "
        f"not the card's first (Plex's) player: got {both['opened']['duration']}")
    assert motif_only["players"] == ["motif"] and motif_only["opened"]["duration"] == 88.0, motif_only


# ── 2. no Plex player without a digits-only rating key ────────────────────────

_PLEX_HARNESS = r"""
const vm = require('vm');
const { bare, block, qp, cases } = JSON.parse(require('fs').readFileSync(0, 'utf8'));
const { computeQuickPlay } = require(qp);
const window = { motifQuickPlay: require(qp) };  // v0.51.343: the card's builders route through lib/quick-play.js
const htmlEscape = (s) => String(s === undefined || s === null ? '' : s);
const out = cases.map((rk) => {
  const bareHtml = vm.runInNewContext(bare + '\nrenderBareInfoCard(row);', {
    htmlEscape, window, row: { plex_title: 'X', plex_media_type: 'show', rating_key: rk, plex_has_theme: 1 } });
  const full = (ratingKey, plexRk) => vm.runInNewContext(block + '\nplexThemeBlock;', {
    htmlEscape, window, lfIsBackupOnly: false, ratingKey, lf: null, data: { plex_has_theme: 1, plex_rating_key: plexRk },
    t: { media_type: 'show', tmdb_id: 1 }, placements: [], libraryState: { items: [] } });  // v0.51.344: the card's in-scope theme + placements feed the row-shaped gate
  const quick = computeQuickPlay({ plex_has_theme: 1, plex_theme_verified_ok: 1, rating_key: rk });
  return { rk, bare: bareHtml, fullArg: full(rk, ''), fullPayload: full(undefined, rk),
           quick: quick ? quick.src : null };
});
process.stdout.write(JSON.stringify(out));
"""

_BAD_KEYS = ["", None, "abc", "12a", " 12", "undefined"]
_GOOD_KEYS = ["778", 778]


def _plex_players(html: str) -> list[str]:
    return re.findall(r'<audio[^>]*\bsrc="(/api/plex/theme/[^"]*)"[^>]*data-plex-theme="1"', html)


@needs_node
def test_every_plex_theme_builder_renders_a_player_only_for_a_digits_key():
    runs = _node(_PLEX_HARNESS, {"bare": _cut(_BARE), "block": _cut(_HELPER, keep_end=True) + _cut(_PLEX_BLOCK),
                                 "qp": str(QUICK_PLAY),
                                 "cases": _BAD_KEYS + _GOOD_KEYS})
    for run in runs:
        rk = run["rk"]
        got = {"bare card": _plex_players(run["bare"]), "full card (rk arg)": _plex_players(run["fullArg"]),
               "full card (payload rk)": _plex_players(run["fullPayload"])}
        if rk in _GOOD_KEYS:
            want = [f"/api/plex/theme/{rk}.mp3"]
            assert all(v == want for v in got.values()), (rk, got)
            assert run["quick"] == want[0], "the row quick-play plays the same proxy URL"
        else:
            assert not any(got.values()), (
                f"v0.51.341: rating key {rk!r} is not all digits — the proxy 400s it (an empty key built a "
                f"keyless URL), so no Plex player may render: {got}")
            assert "/api/plex/theme/" not in run["bare"] + run["fullArg"] + run["fullPayload"]
            assert run["quick"] is None, "quick-play's rkOk rule — the builders agree with it"


def test_the_matrix_covers_every_plex_theme_builder():
    """A new Plex player must join the matrix above: every plexSrc call sits inside a builder
    it renders, app.js spells no /api/plex/theme/ URL by hand, and only the lib builds one."""
    spans = []
    for anchors in (_BARE, _PLEX_BLOCK):
        start = APP_JS.index(anchors[0])
        spans.append((start, APP_JS.index(anchors[1], start)))
    # v0.51.343: the URL is lib/quick-play.js plexSrc's now, so the matrix follows the calls, not the literal
    calls = [m.start() for m in re.finditer(r"motifQuickPlay\.plexSrc\(", APP_JS)]
    assert calls and all(any(a <= i < b for a, b in spans) for i in calls), (
        "a plexSrc call outside the bare card and the full card's plexThemeBlock — "
        "gate it on a digits-only rating key and add it to the matrix")
    assert all(any(a <= i < b for i in calls) for a, b in spans), "each rendered builder routes through plexSrc"
    hand = [m.start() for m in re.finditer(r"/api/plex/theme/", APP_JS)
            if not APP_JS[APP_JS.rindex("\n", 0, m.start()) + 1:m.start()].lstrip().startswith("//")]
    assert hand == [], "an /api/plex/theme/ URL spelled by hand in app.js — build it with motifQuickPlay.plexSrc"
    web = REPO / "app" / "web"
    others = sorted(p.relative_to(web).as_posix() for p in web.rglob("*")
                    if p.suffix in (".js", ".html") and p.name != "app.js" and "/api/plex/theme/" in p.read_text())
    assert others == ["static/lib/quick-play.js"], others
