"""v0.51.87 — RECENTLY ADDED carousel: pause auto-scroll while the window is
unfocused, so the loaded posters actually paint.

the user (deployed): "still seeing the not loading when not focussed". A live
console probe proved the posters were ALL fetched + decoded the whole time the
window was unfocused (loaded=40, fetching=0, notStarted=0) — so v0.51.82's
fetch-side paced loader fixed a non-issue. The real cause is PAINT: an unfocused
window throttles compositing, and the per-30ms `scrollLeft += 1` auto-scroll
churn perpetually outran the throttled paint (console showed "Forced reflow" ×60
+ rAF-handler violations), so scrolled-in tiles showed a stale blank frame until
a click refocused.

Fix: the auto-scroll tick already bailed on `document.hidden` (tab hidden); now
it also bails on `!document.hasFocus()` (window unfocused). A window `blur`
listener nudges one repaint so the frozen strip settles on its loaded posters
instead of the last blank scroll frame.

Source guards (a paint-timing property of an unfocused real browser window — it
can't be exercised headless).
"""
from __future__ import annotations

from pathlib import Path

from _slice_helpers import slice_to_next

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def _autoscroll_fn() -> str:
    # v0.51.285: was a fixed `[i:i + 4200]` window — the rAF rewrite's comments
    # grew the function past it and the blur listener fell off the end (the
    # .261 bug class). Anchored now to the next sibling function.
    return slice_to_next(
        APP_JS,
        "function _setupCarouselAutoScroll(",
        "\n  function ", "\n  async function ")


def test_tick_pauses_on_window_blur():
    fn = _autoscroll_fn()
    # the tick bail must include the window-unfocused guard alongside the
    # existing hidden-tab / hover / dialog guards.
    assert "!document.hasFocus()" in fn, (
        "v0.51.87: auto-scroll must pause while the WINDOW is unfocused, not "
        "only when the TAB is hidden — the scrollLeft churn outruns the "
        "throttled paint and the loaded posters go blank")
    assert "document.hidden" in fn, "the hidden-tab guard must remain"


def test_blur_forces_a_settling_repaint():
    fn = _autoscroll_fn()
    assert "addEventListener('blur'" in fn, (
        "v0.51.87: a window blur listener must force the frozen strip to "
        "repaint its loaded posters (not the last throttled blank frame)")
    # the repaint nudge touches the strip's paint (opacity) + reverts it.
    blur_idx = fn.index("addEventListener('blur'")
    blur_block = fn[blur_idx:blur_idx + 240]
    assert "strip.style.opacity" in blur_block
