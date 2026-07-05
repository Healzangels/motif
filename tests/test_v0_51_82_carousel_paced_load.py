"""v0.51.82 — RECENTLY ADDED carousel posters load through a paced queue.

the user: "while autoscrolling and not clicked on the window the carousel
posters aren't loading". Cause: v0.51.52 made all ~40 posters eager (good — no
lazy pop-in), but assigning all ~40 img.src at once saturates the browser's
~6-connection-per-host cap; the long pending image queue is then deprioritized
to a near-stall while the WINDOW IS UNFOCUSED (Chromium/Brave lower the priority
of not-yet-started loads on blur), so the tail posters stayed blank until a
click refocused the window.

Fix: stage each URL on data-src and load through a bounded-concurrency queue
(_loadCarouselPosters) — only a few requests in flight at once, so there's no
deprioritizable backlog. Still eager (every tile loads on arrival), just paced.

Source-shape guards (the behavior is a browser-timing property that can't be
exercised headless — the loading strategy is pinned in JS so it can't silently
regress back to all-at-once or to native lazy).
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def test_posters_are_staged_on_data_src_not_immediate_src():
    """The render loop must stage the art URL on data-src, not assign img.src
    directly (which would queue all ~40 at once again)."""
    i = APP_JS.index("img.className = 'recent-poster';")
    block = APP_JS[i:i + 1400]
    assert "img.dataset.src = `/api/plex/art/" in block, (
        "v0.51.82: the poster URL must be staged on data-src for the paced loader")
    assert "img.src = `/api/plex/art/" not in block, (
        "v0.51.82: don't assign img.src directly in the render loop — that "
        "re-creates the all-at-once queue that stalls while unfocused")


def test_bounded_concurrency_loader_exists_and_is_called():
    """A _loadCarouselPosters queue with a small concurrency cap must exist and
    be invoked from the carousel render."""
    assert "function _loadCarouselPosters(strip)" in APP_JS
    assert "_loadCarouselPosters(strip);" in APP_JS, (
        "the render must invoke the paced loader")
    # a small in-flight cap (single digits) — not a rename that loads everything.
    loader = APP_JS[APP_JS.index("function _loadCarouselPosters(strip)"):]
    loader = loader[:loader.index("\n  }\n") + 4]
    assert "const CONCURRENCY = " in loader
    import re
    m = re.search(r"const CONCURRENCY = (\d+)", loader)
    assert m and 2 <= int(m.group(1)) <= 6, (
        "concurrency should stay under the ~6/host cap with headroom for XHRs")


def test_loader_frees_the_slot_on_load_or_error():
    """Each finished (or errored) tile must free its slot so the next one
    starts — otherwise a single failing poster would stall the whole queue."""
    loader = APP_JS[APP_JS.index("function _loadCarouselPosters(strip)"):]
    loader = loader[:loader.index("\n  }\n") + 4]
    assert "addEventListener('load', advance)" in loader
    assert "addEventListener('error', advance)" in loader
    assert "startNext()" in loader, "the advance callback must pump the queue"


def test_still_eager_not_native_lazy():
    """Regression guard for the v0.51.52 intent: the fix must not reach for
    native loading='lazy' (viewport-gated pop-in) as a shortcut."""
    i = APP_JS.index("img.className = 'recent-poster';")
    block = APP_JS[i:i + 1400]
    assert "loading = 'lazy'" not in block
