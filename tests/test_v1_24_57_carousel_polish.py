"""v1.24.57 — RECENTLY ADDED carousel polish.

the user's asks on the carousel:
  1. Show a media-type icon (movie vs tv) next to the year.
  2. Align the title + date rows across cards (1-line and 2-line titles pushed
     the date to different heights).
  3. Auto-scroll enabled by default, AND fix auto-scroll that "wasn't working"
     whether the box was checked or not.

The scroll bug: `.recent-strip` had `scroll-snap-type: x proximity`, which
re-snapped every 1px auto-scroll increment back to offset 0 — so the strip
never visibly moved even with the toggle on. Removing scroll-snap fixes it.

These pin the client-side surfaces (JS/CSS/HTML source).
"""
from __future__ import annotations
from _slice_helpers import slice_to_next

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
DASH_HTML = (REPO / "app" / "web" / "templates" / "dashboard.html").read_text()


# ── 1. media-type glyph ──────────────────────────────────────────────────────


def test_type_icon_maps_movie_tv_collection():
    # v1.24.63: the ▶/▭/▦ glyphs became Feather-style inline SVG line icons.
    idx = APP_JS.index("function _recentTypeIcon(mt)")
    fn = APP_JS[idx:idx + 900]
    assert "movie:" in fn and "tv:" in fn and "collection:" in fn
    assert 'stroke="currentColor"' in fn  # icons inherit the meta color
    assert "<svg viewBox=\"0 0 24 24\"" in fn
    # unknown media types → no icon (empty string).
    assert "if (!inner) return ''" in fn


def test_carousel_meta_renders_the_icon():
    # The meta line builds the icon span (innerHTML SVG) ahead of the year/date.
    body = slice_to_next(APP_JS, "async function loadRecentlyAdded()",
                        "\n  function ", "\n  async function ")
    assert "_recentTypeIcon(it.media_type)" in body
    assert "recent-type" in body
    assert "g.innerHTML = icon" in body


def test_css_styles_the_type_icon():
    assert ".recent-type {" in APP_CSS
    assert ".recent-type svg {" in APP_CSS


# ── 2. title / date alignment ────────────────────────────────────────────────


def test_title_is_single_line_ellipsis():
    # v1.24.58 superseded the v1.24.57 2-line reserve: the title is now a single
    # ellipsised line (the user: "make title 1 line"), which also aligns the meta.
    idx = APP_CSS.index(".recent-title {")
    block = APP_CSS[idx:APP_CSS.index("}", idx)]
    assert "white-space: nowrap" in block
    assert "text-overflow: ellipsis" in block
    assert "min-height:" not in block  # the 2-line reserve declaration is gone


# ── 3. auto-scroll default-on + the scroll-snap fix ──────────────────────────


def test_autoscroll_defaults_on_when_unset():
    idx = APP_JS.index("function _setupCarouselAutoScroll()")
    body = APP_JS[idx:idx + 900]
    # Unset (first visit) → checked; only an explicit '0' disables it.
    assert "stored === null ? true : stored === '1'" in body


def test_checkbox_checked_by_default_in_template():
    idx = DASH_HTML.index('id="recent-autoscroll"')
    assert "checked" in DASH_HTML[idx:idx + 40]


def test_scroll_snap_removed_from_strip():
    # The snap-revert bug: scroll-snap fought the 1px auto-scroll increments.
    block = APP_CSS[APP_CSS.index(".recent-strip {"):APP_CSS.index(".recent-card {")]
    assert "scroll-snap-type: x" not in block  # the active declaration is gone
    # and the per-card snap-align is gone too.
    cidx = APP_CSS.index(".recent-card {")
    assert "scroll-snap-align" not in APP_CSS[cidx:cidx + 400]
