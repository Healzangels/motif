"""v0.50.92 — no STANDARD-chip flash on client-side library tab switch.

Navigating (in-place) to a tab last viewed in 4K briefly showed the STANDARD
chip active before flipping to 4K: switchLibraryTab fetches the bare nav href
/<tab> (no ?fourk), so the server-rendered fragment has STANDARD active;
curChips.replaceWith paints it, then hydrateLibraryStateForTab reads the
persisted motif:variant:<tab> and lights the 4K chip a frame later.

Fix: pre-apply the persisted variant to the fetched chips fragment BEFORE the
replaceWith, so the first paint is already correct.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def _switch_fn() -> str:
    i = APP_JS.index("async function switchLibraryTab(")
    j = APP_JS.index("\n  async function loadLibrary(", i)
    return APP_JS[i:j]


def test_variant_applied_before_chip_swap():
    body = _switch_fn()
    swap = body.index("curChips.replaceWith(newChips)")
    # the pre-apply logic must run BEFORE the swap
    pre = body[:swap]
    assert "motif:variant:" in pre, (
        "v0.50.92: switchLibraryTab must read the persisted variant before "
        "swapping the chips in"
    )
    assert "newChips.querySelector('[data-fourk=\"1\"]')" in pre or \
           'newChips.querySelector("[data-fourk=\\"1\\"]")' in pre or \
           "data-fourk" in pre, "must target the fetched fragment's fourk chips"
    assert "chip-active" in pre, "must set chip-active on the fragment pre-swap"


def test_variant_respects_url_param_over_storage():
    """A ?fourk= in the target URL must win over localStorage (deep-link)."""
    body = _switch_fn()
    swap = body.index("curChips.replaceWith(newChips)")
    pre = body[:swap]
    # URL param is checked first, storage is the fallback. Anchor on the real
    # code (localStorage.getItem call), NOT a comment that mentions the key.
    up = pre.index("url.searchParams.has('fourk')")
    sp = pre.index("localStorage.getItem('motif:variant:")
    assert up < sp, "the ?fourk URL param must be checked before the storage fallback"


def test_variant_pre_apply_respects_availability():
    """Only retarget when the wanted chip is actually shown (the server hides an
    unavailable variant via display:none) — so a stale 'fourk' preference for a
    now-removed 4K section doesn't force an empty/invalid variant."""
    body = _switch_fn()
    swap = body.index("curChips.replaceWith(newChips)")
    pre = body[:swap]
    assert "style.display !== 'none'" in pre


def test_collections_skipped():
    """Collections has no fourk axis — the pre-apply must be gated off it."""
    body = _switch_fn()
    swap = body.index("curChips.replaceWith(newChips)")
    pre = body[body.index("newChips"):swap]
    assert "tab !== 'collections'" in pre
