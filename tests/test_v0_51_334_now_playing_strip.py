"""v0.51.334: row quick-play tag 2 — the NOW PLAYING strip (spec § 6).

While a row plays, the results header's action cluster shows the title, a
clock and the row's ■. The clock formatter is lib/quick-play.js
formatClock, pinned under node by tests/js/test_quick_play.js (run by the
v0.51.333 wrapper); this file pins the strip's markup, its bindings and
its CSS.
"""
from __future__ import annotations

import re
from pathlib import Path

from _slice_helpers import slice_between

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
LIBRARY_HTML = (REPO / "app" / "web" / "templates" / "library.html").read_text()
LIB_JS = (REPO / "app" / "web" / "static" / "lib" / "quick-play.js").read_text()
HARNESS = (REPO / "tests" / "js" / "test_quick_play.js").read_text()


def test_strip_markup_hidden_until_a_row_plays():
    strip = slice_between(LIBRARY_HTML, '<div class="now-playing" id="now-playing" hidden>', "\n      </div>")
    assert 'id="now-playing-title"' in strip and 'id="now-playing-time"' in strip
    assert 'data-act="quick-play-stop"' in strip and 'aria-label="Stop"' in strip
    assert 'class="title-glyph row-play row-play-on"' in strip, "the ■ is the row's own glyph"
    # first in the action cluster, left of // LEGEND
    assert LIBRARY_HTML.index('id="now-playing"') < LIBRARY_HTML.index('id="library-legend-toggle"')


def test_clock_formatter_lives_in_the_lib_and_is_node_tested():
    assert "function formatClock(seconds)" in LIB_JS and "formatClock: formatClock," in LIB_JS
    assert "formatClock(72.6)" in HARNESS and 'assert.equal(formatClock(NaN), dash)' in HARNESS


def test_bindings_drive_the_clock_stop_and_scroll():
    b = slice_between(APP_JS, "function bindQuickPlay() {", "\n  }\n")
    assert "audio.addEventListener('timeupdate', _paintNowPlayingClock);" in b
    assert "audio.addEventListener('durationchange', _paintNowPlayingClock);" in b
    assert "_paintNowPlaying(); };" in b, "clear() hides the strip"
    assert "[data-act=\"quick-play-stop\"]')?.addEventListener('click'" in b
    assert "scrollIntoView({ block: 'center'" in b and "CSS.escape(on.key)" in b


def test_paint_hides_when_nothing_plays_and_shows_the_title():
    p = slice_between(APP_JS, "function _paintNowPlaying() {", "\n  }\n")
    assert "el.hidden = !on;" in p and "t.textContent = on.title || '';" in p
    t = slice_between(APP_JS, "function quickPlayToggle(btn) {", "\n  }\n")
    assert "title: btn.dataset.title || ''" in t, "the state carries the title the strip shows"
    assert "_paintNowPlaying();" in t
    c = slice_between(APP_JS, "function _paintNowPlayingClock() {", "\n  }\n")
    assert "window.motifQuickPlay.formatClock" in c
    assert "${fmt(audio.currentTime)} / ${fmt(audio.duration)}" in c


def test_strip_css_is_a_composite_pill_with_a_tabular_clock():
    blk = slice_between(APP_CSS, "\n.now-playing {", "\n}")
    assert "border: 1px dashed var(--line-bright);" in blk and "display: inline-flex;" in blk
    assert re.search(r"^\s*padding: 3px 10px;", blk, re.M), "symmetric: its last run is the untracked ■"
    assert "font-variant-numeric: tabular-nums;" in next(l for l in APP_CSS.splitlines() if l.startswith(".now-playing-time {"))
    title = slice_between(APP_CSS, "\n.now-playing-title {", "\n}")
    assert "text-overflow: ellipsis;" in title and "max-width: 220px;" in title
    assert ".now-playing { max-width: 100%; } .now-playing-title { max-width: 140px; } .now-playing-label { display: none; }" in APP_CSS, "the phone block: the label yields its room to the title"


def test_v0_51_334_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "0.51.334: row quick-play, tag 2" in init_py
