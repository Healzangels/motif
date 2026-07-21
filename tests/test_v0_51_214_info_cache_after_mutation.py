"""v0.51.214 — the in-dialog loudness mutations must invalidate the info-card cache.

_infoFetch serves a cached payload for 6000ms per URL, and a cache HIT does NOT refresh
its timestamp — so the window runs from the FIRST fetch, not the last read. The three
in-dialog loudness mutations (// LEVEL THIS THEME, // UNDO LEVELING, // RE-MEASURE) each
re-open the card 700-900ms later to re-read the row they changed. That re-open landed
INSIDE the window, so it replayed the pre-mutation payload and overwrote the fresh result
the handler had just painted ("now -18.4 LUFS" reverting to the old number after ~700ms).

The row-menu handler already clears the cache on any non-info action; these buttons live
inside the dialog and bypassed it. Clearing at mutation-success (not merely before the
re-open) means every subsequent read is fresh, not just the timed one.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()

_MEASURE_ANCHOR = 'data-act="loud-measure"]\')?.addEventListener'


def _level_undo_handler() -> str:
    i = APP_JS.index("for (const act of ['loud-normalize', 'loud-undo'])")
    return APP_JS[i:APP_JS.index(_MEASURE_ANCHOR, i)]


def _measure_handler() -> str:
    i = APP_JS.index(_MEASURE_ANCHOR)
    nxt = APP_JS.find("?.addEventListener", i + 60)
    return APP_JS[i:nxt if nxt != -1 else len(APP_JS)]


def _assert_clears_between_success_and_reopen(blk: str, what: str) -> None:
    assert "_infoPrefetch.clear()" in blk, f"{what} must invalidate the info-card cache"
    ok_guard = blk.index("if (!r.ok || !j.ok)")
    clear = blk.index("_infoPrefetch.clear()")
    reopen = blk.index("openInfoDialog(")
    assert ok_guard < clear, (
        f"{what} must clear only AFTER the response is known good — clearing on a failed "
        "request throws away a valid cache for nothing")
    assert clear < reopen, (
        f"{what} must clear BEFORE the re-open, or the re-open replays the pre-mutation "
        "payload and destroys the result just painted")


def test_level_and_undo_invalidate_the_cache():
    _assert_clears_between_success_and_reopen(_level_undo_handler(), "LEVEL / UNDO")


def test_remeasure_invalidates_the_cache():
    _assert_clears_between_success_and_reopen(_measure_handler(), "RE-MEASURE")


def test_a_cache_hit_still_does_not_extend_its_own_window():
    """The bug is fixed by invalidating on mutation, NOT by refreshing ts on read. A hit
    that re-stamped ts would keep a hot row's payload alive indefinitely under hover
    prefetch — a staler cache, not a fresher one."""
    i = APP_JS.index("function _infoFetch(url)")
    fn = APP_JS[i:APP_JS.index("function prefetchInfo(", i)]
    hit = fn[fn.index("const hit ="):fn.index("const promise =")]
    assert "ts:" not in hit and "ts =" not in hit, (
        "_infoFetch must not re-stamp ts on a cache hit")


def test_every_clear_site_is_accounted_for():
    """5 sites: dialog-close reset, the row-menu non-info action, the two in-dialog
    loudness handlers, and (v0.51.218) the edition picker — which re-opens the card at a
    DIFFERENT scope, so without the clear the re-open replays the ambiguous payload and
    the chosen cut never appears. A 6th appearing without a test means a new path was
    found to need this — pin it here so the reasoning travels with it."""
    assert APP_JS.count("_infoPrefetch.clear()") == 5
