"""v1.24.82 — dashboard LOW-severity polish (from the dashboard audit).

1. Carousel autoscroll: the tick now bails on document.hidden (no idle-tab scroll
   churn) and reads hover live via :hover, replacing a `paused` flag that could
   stick true if a 30s poll re-rendered the strip mid-hover (autoscroll silently
   dead until the next enter/leave).
2. PER-SECTION COVERAGE THEMED / UNTHEMED cells are now click-through filters
   (status=has_theme / untracked — the library's own THEMED/UNTHEMED chip
   tokens), mirroring the failures + pending cells.

The SERVICES-panel re-paint was deliberately left as-is: its latency number
legitimately changes every poll, so a hash guard would never skip (online) or
show stale latency. Documented at _serviceCard in app.js.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def test_carousel_tick_bails_on_hidden_and_hover():
    idx = APP_JS.index("function tick(ts)")  # v0.51.285: rAF tick takes the frame timestamp
    tick = APP_JS[idx:APP_JS.index("}", APP_JS.index("{", idx))]
    assert "document.hidden" in tick
    assert "strip.matches(':hover')" in tick


def test_carousel_dropped_the_stuck_prone_paused_flag():
    start = APP_JS.index("function _setupCarouselAutoScroll()")
    fn = APP_JS[start:APP_JS.index("\n  function ", start + 10)]
    assert "let paused" not in fn
    assert "addEventListener('mouseenter'" not in fn
    assert "addEventListener('mouseleave'" not in fn


def test_section_coverage_themed_unthemed_clickable():
    assert "const themedHref = `${href}&status=has_theme`;" in APP_JS
    assert "const unthemedHref = `${href}&status=untracked`;" in APP_JS
    assert 'data-href="${htmlEscape(themedHref)}"' in APP_JS
    assert 'data-href="${htmlEscape(unthemedHref)}"' in APP_JS
