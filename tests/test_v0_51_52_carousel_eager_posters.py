"""v0.51.52 — RECENTLY ADDED carousel posters eager-load (no lazy pop-in).

the user: the carousel posters "sometimes don't load right away ... load in late,
looks a bit off". Cause: each of the ~40 posters was `loading='lazy'`, so
off-screen tiles didn't fetch until scrolled near — and the strip auto-scrolls
through all of them by default, so lazy just spread the SAME art-proxy fetches
across the scroll (the pop-in). Now eager + decoding='async', with the first ~8
(initial viewport) at high fetch priority; the art proxy's 1-day Cache-Control
makes it a one-time cost per browser.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def _recent_img_block():
    i = APP_JS.index("img.className = 'recent-poster';")
    return APP_JS[i:i + 900]


def test_carousel_posters_are_not_lazy():
    block = _recent_img_block()
    assert "loading = 'lazy'" not in block, (
        "v0.51.52: the RECENTLY ADDED posters must eager-load, not lazy — lazy "
        "made off-screen tiles pop in late as the strip auto-scrolled"
    )


def test_carousel_posters_async_decode_and_priority():
    block = _recent_img_block()
    assert "img.decoding = 'async'" in block
    # the initial-viewport tiles get high fetch priority so they paint first.
    assert "idx < 8" in block
    assert "img.fetchPriority = 'high'" in block
