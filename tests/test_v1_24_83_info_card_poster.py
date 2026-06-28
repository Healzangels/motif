"""v1.24.83 — movie/show poster on the // MOTIF INFO card (the user, MTDP-style).

A poster hero sits beside the title/scope block, sourced from the existing
/api/plex/art/{rk} Plex proxy the dashboard carousel already uses. posterRk
prefers the clicked edition's rating_key, falling back to a placement's
plex_rating_key for the 2-arg openInfoDialog callers. The <img> is removed on
404 / non-art so the hero collapses to just the meta (no broken-image box).
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def test_info_card_poster_hero_render():
    assert 'class="info-hero"' in APP_JS
    assert 'class="info-hero-meta"' in APP_JS
    assert 'class="info-poster"' in APP_JS
    assert "/api/plex/art/${encodeURIComponent(posterRk)}" in APP_JS
    # rk source: clicked rating_key, else a placement's plex_rating_key.
    assert "const posterRk = String(ratingKey" in APP_JS
    assert "p.plex_rating_key" in APP_JS
    # all-digits guard (the proxy 400s on non-digits).
    assert "/^\\d+$/.test(posterRk)" in APP_JS
    # 404 → remove the img so the hero collapses to just the meta.
    assert "body.querySelector('.info-poster')" in APP_JS


def test_info_poster_css():
    assert ".info-hero {" in APP_CSS
    assert ".info-poster {" in APP_CSS
    assert ".info-hero-meta {" in APP_CSS
    _s = APP_CSS.index(".info-poster {")
    block = APP_CSS[_s:APP_CSS.index("}", _s) + 1]
    assert "aspect-ratio: 2 / 3" in block
    # v1.24.85 regression guard: a flex item's min-width defaults to `auto` =
    # the <img>'s intrinsic width, which overrides flex-basis and blows the
    # poster up to full card width. width + min-width:0 cap it to 120px — both
    # are load-bearing (the v1.24.84 "redundant width" removal regressed this).
    assert "width: 120px" in block  # v1.24.92: back to A's 120 (B's 180 lived at v1.24.90)
    assert "min-width: 0" in block
