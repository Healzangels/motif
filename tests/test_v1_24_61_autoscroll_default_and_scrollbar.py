"""v1.24.61 — carousel auto-scroll: default-on (versioned key) + hidden scrollbar.

the user: "autoscroll is not on by default" (his prefs carried an explicit '0'
persisted during the v1.24.54-56 broken-scroll phase, which beat the v1.24.57
default-ON) and "get rid of the horizontal scroll bar when autoscroll is on".

Fix: version the localStorage key (motif:recentAutoScroll → ...2) so the stale
'0' is ignored; toggle a .recent-strip-autoscroll class that hides the scrollbar
while the strip drives itself (overflow-x stays auto so scrollLeft still works).
"""
from __future__ import annotations

from pathlib import Path

from _slice_helpers import slice_to_next

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def _setup_body():
    idx = APP_JS.index("function _setupCarouselAutoScroll()")
    # up to the next top-level function definition (the whole setup body).
    return APP_JS[idx:APP_JS.index("\n  function ", idx + 1)]


def test_localstorage_key_is_versioned():
    body = _setup_body()
    assert "motif:recentAutoScroll2" in body, (
        "key must be bumped so a stale '0' from the broken-scroll phase is ignored")


def test_default_on_logic_retained():
    body = _setup_body()
    assert "stored === null ? true : stored === '1'" in body


def test_scrollbar_hidden_class_toggled_by_checkbox():
    body = _setup_body()
    assert "recent-strip-autoscroll" in body
    assert "classList.toggle('recent-strip-autoscroll', cb.checked)" in body
    # applied on initial setup AND on change.
    assert body.count("applyScrollbarVis()") >= 2


def test_css_hides_scrollbar_on_autoscroll():
    assert ".recent-strip-autoscroll { scrollbar-width: none; }" in APP_CSS
    assert ".recent-strip-autoscroll::-webkit-scrollbar { display: none; }" in APP_CSS


def test_strip_keeps_overflow_auto_for_programmatic_scroll():
    # The scrollbar is hidden visually, but overflow-x:auto must remain so
    # scrollLeft still drives the strip.
    block = APP_CSS[APP_CSS.index(".recent-strip {"):APP_CSS.index(".recent-card {")]
    assert "overflow-x: auto" in block


def test_autoscroll_pauses_while_a_dialog_is_open():
    # v1.24.64: the tick bails while a modal dialog (the INFO card) is open so
    # the strip doesn't drift behind it.
    body = _setup_body()
    # v0.51.286 (code-review): was a scan to the FIRST '}' after tick's brace —
    # the rAF rewrite's braced guard silently shrank that scope to the guard
    # line, and any future brace above it re-scopes again. Anchor to the next
    # sibling function instead (v0.51.285: rAF tick takes the frame timestamp).
    tick = slice_to_next(body, "function tick(ts)", "function start()")
    assert "document.querySelector('dialog[open]')" in tick
    # v1.24.82: hover-pause is read live via :hover (was a `paused` flag that
    # could stick true if a 30s poll re-rendered the strip mid-hover), and the
    # tick also bails when the tab is hidden (no idle-tab scroll churn).
    assert "strip.matches(':hover')" in tick
    assert "document.hidden" in tick
