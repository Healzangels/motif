"""v1.24.66 — dashboard stat-card icons + carousel scrollbar visibility.

the user: update the dashboard stat-card glyphs (MOVIES/TV/ANIME/COLLECTIONS,
both the THEMED and PLEX rows) to the new Feather-style SVG icons that match the
carousel; and when carousel auto-scroll is OFF, make the horizontal scrollbar
visible (so manual scroll is obviously available).
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DASH_HTML = (REPO / "app" / "web" / "templates" / "dashboard.html").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


# ── stat-card icons → Feather SVG via the media_glyph() macro ─────────────────


def test_media_glyph_macro_defined_with_four_kinds():
    macro = DASH_HTML[DASH_HTML.index("{% macro media_glyph(kind)"):]
    macro = macro[:macro.index("{%- endmacro %}")]
    assert 'stroke="currentColor"' in macro  # inherits .stat-glyph colour
    for kind in ("movies", "tv", "anime", "collections"):
        assert f"kind == '{kind}'" in macro, kind
    # v0.50.38: anime shares the TV glyph (the user) — the sparkle path is gone.
    assert "<polyline points=\"17 2 12 7 7 2\"/>" in macro  # tv antenna
    assert "M12 3l2.2 6.8" not in macro  # old sparkle path retired


def test_no_unicode_stat_glyphs_remain():
    for old in ("▶", "▭", "✦", "▦"):
        assert f'<span class="stat-glyph">{old}</span>' not in DASH_HTML


def test_css_sizes_the_stat_glyph_svg():
    assert ".stat-glyph svg {" in APP_CSS
    block = APP_CSS[APP_CSS.index(".stat-glyph svg {"):APP_CSS.index(".stat-glyph svg {") + 120]
    assert "width: 16px" in block


def test_carousel_film_tv_icons_match_macro():
    # the film + tv + collections paths in the macro mirror app.js _recentTypeIcon.
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    macro = DASH_HTML[DASH_HTML.index("{% macro media_glyph(kind)"):]
    macro = macro[:macro.index("{%- endmacro %}")]
    icon = js[js.index("function _recentTypeIcon(mt)"):]
    icon = icon[:icon.index("\n  }")]
    for path in ('<polyline points="17 2 12 7 7 2"/>',  # tv
                 '<path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/>'):  # coll
        assert path in macro and path in icon, path


# ── carousel scrollbar visible when auto-scroll is OFF ───────────────────────


def test_strip_has_visible_styled_scrollbar_by_default():
    # WebKit: explicit ::-webkit-scrollbar dimensions = non-overlay (always
    # shown on overflow). Firefox: scrollbar-width: thin.
    assert ".recent-strip::-webkit-scrollbar { height: 8px; }" in APP_CSS
    assert ".recent-strip::-webkit-scrollbar-thumb {" in APP_CSS
    strip = APP_CSS[APP_CSS.index(".recent-strip {"):APP_CSS.index(".recent-strip::-webkit-scrollbar {")]
    assert "scrollbar-width: thin" in strip


def test_autoscroll_still_hides_the_scrollbar():
    # The hide rules must come AFTER the styled bar so they win the cascade when
    # auto-scrolling.
    styled = APP_CSS.index(".recent-strip::-webkit-scrollbar {")
    hidden = APP_CSS.index(".recent-strip-autoscroll::-webkit-scrollbar { display: none; }")
    assert hidden > styled
    assert ".recent-strip-autoscroll { scrollbar-width: none; }" in APP_CSS
