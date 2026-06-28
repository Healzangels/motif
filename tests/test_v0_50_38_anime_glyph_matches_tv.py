"""v0.50.38 — the dashboard ANIME stat-card glyph matches the TV glyph.

the user: "make the Anime icon the same as the TV icon in both sections" — i.e.
the // ANIME THEMED coverage card AND the // PLEX ANIME card. Both call the
media_glyph('anime') macro, so folding anime into the tv branch changes both at
once. The old 4-point sparkle path is retired.
"""
from __future__ import annotations

from pathlib import Path

DASH = (Path(__file__).resolve().parent.parent
        / "app" / "web" / "templates" / "dashboard.html").read_text()


def _macro() -> str:
    start = DASH.index("{% macro media_glyph(kind)")
    return DASH[start:DASH.index("{%- endmacro %}", start)]


def test_anime_shares_the_tv_branch():
    macro = _macro()
    # tv + anime render the same SVG via one combined branch
    assert "kind == 'tv' or kind == 'anime'" in macro
    # the tv television path is the shared glyph; the old sparkle is gone
    assert '<polyline points="17 2 12 7 7 2"/>' in macro
    assert "M12 3l2.2 6.8" not in macro


def test_both_anime_cards_still_call_the_macro():
    # the THEMED coverage card + the PLEX card both render media_glyph('anime')
    assert DASH.count("media_glyph('anime')") == 2
