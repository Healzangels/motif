"""v0.51.333: row quick-play (feature E), tag 1.

Spec: docs/specs/ROW_QUICK_PLAY_SPEC.md. The rule (WHAT a row plays) is
lib/quick-play.js — the live module base.html loads before app.js — and
is pinned behaviourally by tests/js/test_quick_play.js under node. This
file runs that harness inside the gate (the v0.51.293 wrapper shape) and
pins the wiring around it: the shared player, the slot in the renderer,
the click path that bypasses the job lock, exclusivity both ways, and the
CSS that makes the slot a proper title glyph.
"""
from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

from _slice_helpers import slice_between

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
BASE_HTML = (REPO / "app" / "web" / "templates" / "base.html").read_text()
LIBRARY_HTML = (REPO / "app" / "web" / "templates" / "library.html").read_text()
LIB_JS = (REPO / "app" / "web" / "static" / "lib" / "quick-play.js").read_text()
HARNESS = REPO / "tests" / "js" / "test_quick_play.js"
_NODE = shutil.which("node")

if os.environ.get("MOTIF_REQUIRE_NODE") and not _NODE:
    raise RuntimeError("MOTIF_REQUIRE_NODE=1 but node is not on PATH — the quick-play harness would silently not run")


# ── the rule, behaviourally ──────────────────────────────────────────

@pytest.mark.skipif(not _NODE, reason="node not installed")
def test_quick_play_harness_passes():
    r = subprocess.run([_NODE, "--test", str(HARNESS)], capture_output=True, text=True, timeout=120, cwd=REPO)
    assert r.returncode == 0, f"JS harness failed:\n{r.stdout[-3000:]}\n{r.stderr[-1500:]}"
    m = re.search(r"# pass (\d+)", r.stdout)
    assert m and int(m.group(1)) >= 14, r.stdout[-800:]
    assert "# fail 0" in r.stdout


def test_the_module_the_harness_tests_is_the_module_the_page_loads():
    """The whole point of a lib module over a hand-mirrored copy: base.html
    loads lib/quick-play.js BEFORE app.js (defer keeps document order), and
    the harness requires that same path."""
    lib = BASE_HTML.index('src="/static/lib/quick-play.js?v={{ motif_version }}"')
    app = BASE_HTML.index('src="/static/app.js?v={{ motif_version }}"')
    assert lib < app
    assert 'require("../../app/web/static/lib/quick-play.js")' in HARNESS.read_text()
    assert "root.motifQuickPlay = factory()" in LIB_JS
    assert "window.motifQuickPlay.computeQuickPlay(it)" in APP_JS


# ── the wiring ───────────────────────────────────────────────────────

def test_shared_player_and_note_live_on_the_library_page():
    m = re.search(r'<audio id="row-quick-play"([^>]*)>', LIBRARY_HTML)
    assert m, "the shared player"
    attrs = m.group(1)
    assert 'preload="none"' in attrs and "controls" not in attrs and "hidden" in attrs
    assert 'id="quick-play-note" hidden' in LIBRARY_HTML


def _renderer() -> str:
    return slice_between(APP_JS, "function renderLibraryRow(it) {", "function renderLibraryRowNotInPlex(it) {")


def test_renderer_emits_the_slot_first_in_the_title_cell():
    r = _renderer()
    assert 'data-act="quick-play"' in r
    assert "libraryState.quickPlay.key === selKey" in r, "the playing row survives a re-render"
    assert 'class="row-play row-play-none"' in r, "rows with nothing to play still hold the column"
    cell = slice_between(r, '<div class="title-cell" title="${htmlEscape(titleTooltip)}">', "</div>")
    assert cell.index("${quickPlaySlot}") < cell.index("${titleGlyphs.join('')}"), "the slot leads"
    for attr in ('data-key="', 'data-src="', 'data-kind="', 'data-tip="', 'data-title="', 'aria-pressed="'):
        assert attr in r, attr


def test_tdb_only_browse_rows_hold_the_slot_too():
    r = slice_between(APP_JS, "function renderLibraryRowNotInPlex(it) {", "\n  }\n")
    assert 'class="row-play row-play-none"' in r


def test_click_path_bypasses_the_job_lock():
    """A listen is not an operation: handled before the lock / prefetch
    invalidation / poll boost, and it returns."""
    h = slice_between(APP_JS, "document.getElementById('library-body')?.addEventListener('click', async (e) => {", "const act = btn.dataset.act;")
    assert "if (btn.dataset.act === 'quick-play') { quickPlayToggle(btn); return; }" in h
    assert "bindQuickPlay();" in APP_JS


def test_player_binding_is_exclusive_both_ways_and_ignores_the_stale_pause():
    b = slice_between(APP_JS, "function bindQuickPlay() {", "\n  }\n")
    assert "document.addEventListener('play'," in b and "}, true);" in b, "capture-phase: any other <audio> starting pauses the row"
    assert "if (audio.paused) clear();" in b, "the src swap queues a stale pause; play() has already flipped paused=false"
    assert "audio.addEventListener('ended', clear);" in b
    assert "Plex reports a theme but it did not play" in b, "the v0.51.322 wording"
    t = slice_between(APP_JS, "function quickPlayToggle(btn) {", "\n  }\n")
    assert "_pauseOtherAudio(audio);" in t, "starting a row pauses every other player"
    assert "audio.currentTime = 0;" in t, "Stop rewinds"


def test_slot_css_is_a_reserved_untracked_title_glyph():
    assert APP_CSS.index("\n.title-glyph {") < APP_CSS.index("\n.row-play {"), "extends the primitive, so it must follow it"
    blk = slice_between(APP_CSS, "\n.row-play {", "\n}")
    assert "flex: 0 0 18px;" in blk and "width: 18px;" in blk and "letter-spacing: 0;" in blk and "padding: 0;" in blk
    on = next(l for l in APP_CSS.splitlines() if l.startswith(".row-play-on {"))
    assert "var(--green)" in on and "var(--green-rgb)" in on
    assert ".row-play-none {" in APP_CSS
    mobile = slice_between(APP_CSS, "  .help-toggle, .topbar-logout { min-height: 30px; min-width: 30px; }", "\n")
    assert ".row-play { flex-basis: 30px; width: 30px; min-height: 30px; }" in mobile or ".row-play { flex-basis: 30px; width: 30px; min-height: 30px; }" in APP_CSS


def test_state_field_declared():
    st = slice_between(APP_JS, "const libraryState = {", "\n  };")
    assert "quickPlay: null," in st


def test_v0_51_333_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "0.51.333: row quick-play, tag 1" in init_py
