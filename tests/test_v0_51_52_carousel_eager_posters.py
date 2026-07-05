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
    # v0.51.82: widened 900 → 1400 — the paced-loader breadcrumb comment grew the
    # img-creation block, pushing `img.decoding` past the old window. Safe: neither
    # `fetchPriority` nor a spaced `loading = 'lazy'` appears anywhere in app.js.
    i = APP_JS.index("img.className = 'recent-poster';")
    return APP_JS[i:i + 1400]


def test_carousel_posters_are_not_lazy():
    block = _recent_img_block()
    assert "loading = 'lazy'" not in block, (
        "v0.51.52: the RECENTLY ADDED posters must eager-load, not lazy — lazy "
        "made off-screen tiles pop in late as the strip auto-scrolled"
    )


def test_carousel_posters_async_decode_default_priority():
    block = _recent_img_block()
    assert "img.decoding = 'async'" in block
    # v0.51.56 (code-review): posters eager-load at DEFAULT priority — no
    # fetchPriority='high' that would contend with the dashboard's data XHRs.
    assert "fetchPriority" not in block
