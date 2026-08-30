"""v0.51.209 — notification-inbox review fixes.

An end-to-end review of the shipped inbox (v0.51.147→154) surfaced two real defects in
bindNotifInbox + one stale comment:

  A. The group header is `role="button" tabindex="0"` (focusable, announced as a button)
     but had NO keydown handler — only a mouse click expanded it, so the grouping feature
     was keyboard-inoperable. Now Enter/Space expand it via the shared toggleGroupHead,
     which both the click and keydown paths call.
  B. Click-through routed every non-movie to /tv, so a `collection` notification opened
     on the /tv page. Now collection → /collections.

Guards are JS-source-shape (the DOM keydown path needs a browser); the click-through
DATA contract is already covered behaviorally by test_v0_51_151.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def _inbox_fn() -> str:
    """The bindNotifInbox function body — anchor asserts here so they can't match
    unrelated code elsewhere in app.js."""
    i = APP_JS.index("function bindNotifInbox()")
    j = APP_JS.index("\n  function ", i + 1)   # next top-level function
    return APP_JS[i:j]


def test_group_header_is_keyboard_operable():
    fn = _inbox_fn()
    # the shared toggle exists...
    assert "function toggleGroupHead(head)" in fn
    # ...and BOTH the click path and a keydown path drive it.
    assert "if (head) { toggleGroupHead(head); return; }" in fn
    assert "listEl.addEventListener('keydown'" in fn
    # the keydown handler activates on Enter or Space and suppresses Space-scroll.
    # v0.51.213: bound by the listener's end, not a fixed 400-byte window — v0.51.213's
    # nested-control bail pushed preventDefault past it, failing this as a phantom
    # invariant break when the invariant was intact (the v0.51.141-143 slice trap).
    i = fn.index("listEl.addEventListener('keydown'")
    kd = fn[i:fn.index("document.addEventListener('keydown'", i)]
    assert "'Enter'" in kd and "' '" in kd
    assert "e.preventDefault()" in kd
    assert "toggleGroupHead(head)" in kd


def test_clickthrough_routes_collection_to_its_own_tab():
    fn = _inbox_fn()
    # movie → /movies, collection → /collections, else → /tv (was: everything-not-movie → /tv).
    assert "'collection' ? '/collections'" in fn
    assert "=== 'movie' ? '/movies'" in fn
    # the old blanket "everything else is /tv" ternary must be gone.
    # v0.51.309: the movie/tv pair is a legitimate TAIL now (collection and
    # anime peel off first) — pin that both special branches precede it
    # instead of banning the substring outright.
    assert fn.index("'collection' ? '/collections'") < fn.index(
        "=== 'movie' ? '/movies'")
    assert fn.index("dataset.anime === '1' ? '/anime'") < fn.index(
        "=== 'movie' ? '/movies'")
