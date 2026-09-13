"""v0.51.339: UI residuals from the v0.51.328..337 review, tag 2.

1. The FILTERS drawer auto-opened on /movies?src_pills=AT (or a stored AT)
   with nothing lit: bindLibrary's open decision counted srcFilter before the
   first loadLibrary pruned the off-tab letter. The drawer block now prunes
   first, against the tab the page is showing. Driven under node: the real
   bindLibrary block, the real prune + lib/src-filter.js, the rendered SRC row.
2. The INFO headline said "Plex serves its own theme" for every backup_only
   row, reading neither plex_has_theme nor plex_theme_verified_ok — while the
   row's ▶ (lib/quick-play.js) and the card's Plex player use that test. Driven
   under node against computeQuickPlay; api_item now ships the verify stamp.
3. The stepper glyphs are pinned in tests/test_v0_51_332_pill_ink_centred_sweep.py.
4. The provenance suffix named AnimeThemes by a case-sensitive substring; it
   now asks sync.url_source, the classifier enrich_item uses.
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
APP_JS = (STATIC / "app.js").read_text()
LIBRARY_HTML = (REPO / "app" / "web" / "templates" / "library.html").read_text()
SRC_FILTER_LIB = STATIC / "lib" / "src-filter.js"
QUICK_PLAY_LIB = STATIC / "lib" / "quick-play.js"
AUTH = {"X-Authentik-Username": "testadmin"}
TS = "2026-01-01T00:00:00+00:00"  # a stored stamp only; nothing here reads a now-relative window
_NODE = shutil.which("node")

if os.environ.get("MOTIF_REQUIRE_NODE") and not _NODE:
    raise RuntimeError("MOTIF_REQUIRE_NODE=1 but node is not on PATH — the drawer and headline checks would silently not run")

needs_node = pytest.mark.skipif(not _NODE, reason="node not installed")


def _node(script: str, payload, tmp_path: Path, *argv: Path):
    harness = tmp_path / "harness.js"
    harness.write_text(script)
    r = subprocess.run([_NODE, str(harness), *map(str, argv)], input=json.dumps(payload),
                       capture_output=True, text=True, timeout=60, cwd=REPO)
    assert r.returncode == 0, r.stderr[-2000:]
    return json.loads(r.stdout)


# ══ 1. the drawer opens only on a filter the page shows ═══════════════

TABS = ("movies", "tv", "anime", "collections")
DRAWER_BLOCK_START = "document.getElementById('library-filter-toggle')\n      ?.addEventListener('click'"


class _TagCollector(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.tags = []

    def handle_starttag(self, tag, attrs):
        self.tags.append(dict(attrs))


def _src_row_chips(tab):
    """The SRC row of library.html rendered for `tab`: [{letter, tabOnly, shown}]."""
    row = slice_between(LIBRARY_HTML, '<div class="pill-filter-row" aria-label="SRC pill filter">', "\n  </div>")
    collector = _TagCollector()
    collector.feed(jinja2.Environment(autoescape=True).from_string(row).render(tab=tab))
    chips = []
    for attrs in collector.tags:
        if attrs.get("data-src-filter"):
            style = re.sub(r"\s+", "", attrs.get("style") or "").lower()
            chips.append({"letter": attrs["data-src-filter"], "tabOnly": attrs.get("data-tab-only") or "",
                          "shown": "display:none" not in style})
    return chips


def _deep_link_src_tokens():
    m = re.search(r"\{ param: 'src_pills',.*?values: new Set\(\[([^\]]*)\]\)", APP_JS, re.S)
    assert m, "the ?src_pills= restore allowlist"
    return re.findall(r"'([^']+)'", m.group(1))


def _drawer_code():
    fns = "".join(slice_between(APP_JS, start, "\n  }\n") + "\n  }\n" for start in (
        "function _srcFilterChips() {",
        "function _pruneSrcFilterToOfferedChips() {",
        "function _activeAxisFilterCount() {",
        "function _setFilterDrawerOpen(open) {",
        "function updateFilterDrawerUi() {",
    ))
    block = slice_between(APP_JS, DRAWER_BLOCK_START, "bindLibraryToolbarChips();")
    return ("const libraryState = { tab: null, srcFilter: new Set(LETTERS), tdbPills: new Set(),"
            " attnPills: new Set(), dlPills: new Set(), plPills: new Set(), linkPills: new Set(), edPills: new Set() };\n"
            f"{fns}\n(function () {{\n  const tabEl = {{ value: TAB }};\n{block}\n}})();\n"
            "Array.from(libraryState.srcFilter);")


_DRAWER_HARNESS = r"""
const fs = require("fs");
const vm = require("vm");
const lib = require(process.argv[2]);
const { code, cases } = JSON.parse(fs.readFileSync(0, "utf8"));
const out = cases.map((c) => {
  const store = Object.create(null);
  if (c.storedOpen) store.motifFilterDrawerOpen = "1";
  const drawer = { hidden: true };
  const toggle = { classList: { toggle() {} }, setAttribute() {}, addEventListener() {} };
  const badge = { hidden: true, textContent: "" };
  const byId = { "library-filter-drawer": drawer, "library-filter-toggle": toggle, "library-filter-count": badge };
  const chips = c.chips.map((ch) => {
    const el = { dataset: { srcFilter: ch.letter }, lit: c.letters.includes(ch.letter) };
    if (ch.tabOnly) el.dataset.tabOnly = ch.tabOnly;
    el.classList = { remove(cls) { if (cls === "src-key-btn-active") el.lit = false; } };
    return el;
  });
  const kept = vm.runInNewContext(code, {
    window: { motifSrcFilter: lib },
    document: {
      getElementById: (id) => byId[id] || null,
      querySelectorAll: (sel) => (sel === "[data-src-filter]" ? chips : []),
    },
    localStorage: {
      getItem: (k) => (k in store ? store[k] : null),
      setItem: (k, v) => { store[k] = String(v); },
    },
    LETTERS: c.letters,
    TAB: c.tab,
  });
  return {
    open: !drawer.hidden,
    kept: Array.from(kept),
    lit: chips.filter((el) => el.lit).map((el) => el.dataset.srcFilter),
    badge: badge.hidden ? "" : badge.textContent,
  };
});
process.stdout.write(JSON.stringify(out));
"""


def _run_drawer(cases, tmp_path):
    return _node(_DRAWER_HARNESS, {"code": _drawer_code(), "cases": cases}, tmp_path, SRC_FILTER_LIB)


def _chips_for(tab):
    return [{"letter": c["letter"], "tabOnly": c["tabOnly"]} for c in _src_row_chips(tab)]


def test_the_driven_block_is_binds_drawer_decision_after_hydration():
    assert APP_JS.count(DRAWER_BLOCK_START) == 1
    bind = APP_JS.index("function bindLibrary() {")
    assert (APP_JS.index("if (!hasFilterParam) _hydrateLibraryFromStorage();", bind)
            < APP_JS.index(DRAWER_BLOCK_START, bind)), "the open decision reads the URL / storage restore"
    assert "if (_drawerWasOpen || _activeAxisFilterCount() > 0)" in _drawer_code()


@needs_node
def test_a_restored_src_letter_opens_the_drawer_only_where_its_chip_shows(tmp_path):
    """Every ?src_pills= token on every tab, as the restore leaves it (lit, in
    srcFilter): the drawer opens iff that tab shows the chip, and whatever the
    filter keeps is exactly what is lit and counted — never an open drawer with
    nothing lit, never a hidden letter narrowing a closed one."""
    cases, expect = [], []
    for tab in TABS:
        chips = _src_row_chips(tab)
        shown = {c["letter"] for c in chips if c["shown"]}
        for letter in _deep_link_src_tokens():
            cases.append({"tab": tab, "letters": [letter], "chips": _chips_for(tab)})
            expect.append((tab, letter, letter in shown))
    assert {offered for _, _, offered in expect} == {True, False}, "the matrix must hold offered and unoffered letters"
    assert ("movies", "AT", False) in expect and ("anime", "AT", True) in expect
    for (tab, letter, offered), got in zip(expect, _run_drawer(cases, tmp_path)):
        assert got["open"] == offered, (tab, letter, got)
        assert got["kept"] == ([letter] if offered else []), (tab, letter, got)
        assert got["lit"] == got["kept"], (tab, letter, got)
        assert got["badge"] == ("1" if offered else ""), (tab, letter, got)


@needs_node
def test_the_drawer_keeps_its_other_reasons_to_open(tmp_path):
    movies = _chips_for("movies")
    left_open, closed, mixed = _run_drawer([
        {"tab": "movies", "letters": [], "chips": movies, "storedOpen": True},
        {"tab": "movies", "letters": [], "chips": movies},
        {"tab": "movies", "letters": ["AT", "T"], "chips": movies},
    ], tmp_path)
    assert left_open["open"] and not closed["open"]
    assert mixed == {"open": True, "kept": ["T"], "lit": ["T"], "badge": "1"}, mixed


# ══ 2. the INFO headline says what the row's ▶ plays ═════════════════

_HEADLINE_HARNESS = r"""
const fs = require("fs");
const vm = require("vm");
const { computeQuickPlay } = require(process.argv[2]);
const { fns, cases } = JSON.parse(fs.readFileSync(0, "utf8"));
const out = cases.map((c) => ({
  label: vm.runInNewContext(`${fns}\n_derivePlaybackSourceLabel();`,
                            { _ambiguousCut: false, lf: c.lf, data: c.data, placements: [] }),
  play: computeQuickPlay(c.row),
}));
process.stdout.write(JSON.stringify(out));
"""


def _headline_fns():
    held = slice_between(APP_JS, "    function _heldWord(sk) {", "\n    }") + "\n    }"
    fn = slice_between(APP_JS, "    function _derivePlaybackSourceLabel() {", "\n    }") + "\n    }"
    return f"{held}\n{fn}"


@needs_node
@pytest.mark.parametrize("source_kind", ["themerrdb", "url", "plex_cloud", "adopt"])
def test_backup_headline_says_plex_serves_exactly_when_the_row_plays_plex(tmp_path, source_kind):
    """A backup_only row across every (plex_has_theme, plex_theme_verified_ok)
    cell the payload can carry: the headline names Plex as serving iff the row's
    ▶ plays what Plex serves; otherwise it says Plex no longer serves and names
    the one action that deploys motif's copy."""
    cells = [(has, ok) for has in (None, 0, 1) for ok in (None, 0, 1)]
    cases = [{
        "lf": {"source_kind": source_kind, "source_video_id": "vid", "last_place_attempt_reason": "backup_only"},
        "data": {"plex_has_theme": has, "plex_theme_verified_ok": ok},
        "row": {"theme_media_type": "tv", "theme_tmdb": 777, "section_id": "3", "rating_key": "1001",
                "media_folder": None, "placement_kind": None, "file_path": "tv/x/theme.mp3",
                "canonical_missing": 0, "plex_has_theme": has, "plex_theme_verified_ok": ok,
                "last_place_attempt_reason": "backup_only"},
    } for has, ok in cells]
    out = _node(_HEADLINE_HARNESS, {"fns": _headline_fns(), "cases": cases}, tmp_path, QUICK_PLAY_LIB)
    plays = {}
    for (has, ok), got in zip(cells, out):
        label, kind = got["label"], got["play"]["kind"]
        plays[(has, ok)] = kind
        assert " on disk as backup · " in label, label
        if kind == "plex":
            assert label.endswith(" on disk as backup · Plex serves its own theme"), ((has, ok), label)
            assert "PROMOTE" not in label, label
        else:
            assert "Plex serves its own theme" not in label, ((has, ok), label)
            assert "Plex no longer serves a theme" in label and "PROMOTE TO ACTIVE" in label, ((has, ok), label)
    # the verdict's sticky cells: has_theme dropped to 0, and a verify 404 under has_theme 1
    assert plays[(0, None)] == "file" and plays[(1, 0)] == "file"
    assert plays[(1, None)] == "plex" and plays[(1, 1)] == "plex"


def _make_app(tmp_path, monkeypatch):
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    from fastapi.testclient import TestClient
    (tmp_path / "data").mkdir(exist_ok=True)
    (tmp_path / "motif.yaml").write_text("paths: {}\n")
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    return TestClient(create_app(s)), s


def _seed(db, items):
    from app.core.db import get_conn, transaction
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k, themes_subdir, included, "
            "                           discovered_at, last_seen_at) VALUES ('3', 'S', 'show', 0, 0, 's3', 1, ?, ?)",
            (TS, TS))
        tid = conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, upstream_source, last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('tv', 339001, 'X', 'imdb', ?, ?)", (TS, TS)).lastrowid
        for rk, edition, has, ok in items:
            conn.execute(
                "INSERT INTO plex_items (rating_key, section_id, media_type, title, guid_tmdb, theme_id, folder_path, "
                "                        edition_key, has_theme, plex_theme_verified_ok, first_seen_at, last_seen_at) "
                "VALUES (?, '3', 'show', 'X', '339001', ?, '/x', ?, ?, ?, ?, ?)",
                (rk, tid, edition, has, ok, TS, TS))


@pytest.mark.parametrize("items, query, expected", [
    ([("9101", "", 1, 0)], "?section_id=3&rating_key=9101", 0),
    ([("9101", "", 1, None)], "?section_id=3&rating_key=9101", None),
    ([("9101", "", 1, 1)], "?section_id=3&rating_key=9101", 1),
    ([("9101", "", 1, 0)], "?section_id=3", 0),
    ([("9101", "", 1, 0)], "", 0),
    ([("9101", "", 1, 0), ("9102", "ext", 0, None)], "?section_id=3", 0),
    ([("9101", "", 1, 0), ("9102", "ext", 1, 1)], "?section_id=3", 1),
    ([("9101", "", 0, None)], "?section_id=3", None),
], ids=["rk-404", "rk-unverified", "rk-verified", "section-404", "global-404",
        "silent-sibling-lends-nothing", "one-cut-serves", "nothing-claims"])
def test_api_item_ships_the_verify_stamp_the_headline_reads(tmp_path, monkeypatch, items, query, expected):
    c, s = _make_app(tmp_path, monkeypatch)
    _seed(s.db_path, items)
    r = c.get(f"/api/items/tv/339001{query}", headers=AUTH)
    assert r.status_code == 200, r.text
    body = r.json()
    assert "plex_theme_verified_ok" in body
    assert body["plex_has_theme"] == max(has for _, _, has, _ in items)
    assert body["plex_theme_verified_ok"] == expected, body["plex_theme_verified_ok"]


# ══ 4. the provenance suffix asks the classifier ═════════════════════

@pytest.mark.parametrize("url", [
    "https://a.animethemes.moe/Bleach-OP1.ogg",
    "HTTPS://A.ANIMETHEMES.MOE/Bleach-OP1.ogg",
    "https://A.AnimeThemes.moe/Bleach-OP1.ogg?ref=youtube.com",
    "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
    "https://example.com/mirror/a.animethemes.moe/Bleach-OP1.ogg",
])
def test_provenance_suffix_names_animethemes_exactly_when_url_source_does(url):
    from app.core.notify_content import _format_provenance_line
    from app.core.sync import url_source
    line = _format_provenance_line({"provenance": "animethemes", "theme_url": url})
    assert line.endswith(" · AnimeThemes") == (url_source(url) == "animethemes"), (url, line)


def test_an_uppercase_animethemes_host_reads_animethemes():
    from app.core.notify_content import _format_provenance_line
    line = _format_provenance_line({"provenance": "animethemes", "theme_url": "HTTPS://A.ANIMETHEMES.MOE/Bleach-OP1.ogg"})
    assert line.endswith(" · AnimeThemes"), line
