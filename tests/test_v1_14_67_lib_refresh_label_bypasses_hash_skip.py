"""v1.14.67 — library REFRESH label bypasses the v1.14.37 hash-skip.

the user screenshot repro on v1.14.66:

> "when toggling between libraries and standard and 4k the
>  refresh button text becomes inconsistent with the location
>  you are browsing."

Repro: on /tv with the //4K chip clicked, the REFRESH button
read "// REFRESH TV SHOWS" instead of "// REFRESH 4K TV SHOWS".

## Root cause

v1.14.37 added a hash-skip optimization to refreshTopbarStatus
that returns early when the /api/stats payload is byte-identical
to the prior tick. The library REFRESH button label is computed
by `libraryRefreshLabel()` from CLIENT-side state
(`libraryState.fourk` + the `#library-tab` DOM value) — neither
of which is part of the /api/stats payload. So toggling the //4K
chip:

  1. Sets libraryState.fourk = true
  2. Calls refreshTopbarStatus() to re-paint
  3. /api/stats response is byte-identical (no server change)
  4. Hash-skip returns early
  5. Button label stays "// REFRESH TV SHOWS"

The busy/disabled state DOES come from /api/stats, so it
correctly stays gated by the hash check. Only the label form
needs the bypass.

## Fix

Insert a small early-update block in refreshTopbarStatus BEFORE
the hash-skip return. It re-derives `libraryRefreshLabel()` and
sets the button text — but only if the button isn't currently
showing "// REFRESHING…" (don't clobber the busy label).
"""
from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
JS = REPO / "app" / "web" / "static" / "app.js"


def _refresh_topbar_status_block() -> str:
    """Slice from `async function refreshTopbarStatus()` through
    the first `} catch` so test assertions are scoped to that
    function body."""
    js = JS.read_text()
    start = js.index("async function refreshTopbarStatus()")
    # First `} catch` after the function start closes the try.
    catch = js.index("} catch", start)
    return js[start:catch]


def test_lib_refresh_label_updates_before_hash_skip():
    """The early-update block must sit between the await and the
    hash-skip return. v1.14.67 marker pins the rationale."""
    body = _refresh_topbar_status_block()
    # Locate the three structural anchors in order.
    await_idx = body.index("await api('GET', '/api/stats')")
    early_idx = body.index("v1.14.67 + v1.14.68: client-state label update")
    skip_idx = body.index("if (refreshTopbarStatus._lastHash === newHash) return;")
    assert await_idx < early_idx < skip_idx, (
        "v1.14.67 early-label-update block must be between the "
        "/api/stats await and the hash-skip return. Got: "
        f"await@{await_idx}, early@{early_idx}, skip@{skip_idx}"
    )


def test_lib_refresh_label_block_invokes_helper():
    """The early-update block must invoke
    updateLibraryRefreshBtnLabel(). v1.14.67 originally inlined
    the DOM mutation here; v1.14.68 extracted to a helper so
    the //4K toggle handler can call it synchronously. The
    invariant pinned here is "the early-update site exists" —
    the helper's internals (libraryRefreshLabel call, busy
    guard) are pinned by test_v1_14_68_*."""
    body = _refresh_topbar_status_block()
    anchor = body.index("v1.14.67 + v1.14.68: client-state label update")
    block = body[anchor:anchor + 1500]
    assert "updateLibraryRefreshBtnLabel()" in block, (
        "Early-update block must call updateLibraryRefreshBtnLabel() — "
        "the helper that v1.14.68 extracted from the inline form."
    )


def test_lib_refresh_label_block_does_not_reinline_dom_work():
    """Regression guard: the v1.14.67 inline form (direct
    document.getElementById + textContent assign) must NOT come
    back. Both call sites (toggle handler + early-update block)
    go through the helper. Re-inlining would create a
    correctness drift surface — change the helper, miss the
    inline site (or vice versa)."""
    body = _refresh_topbar_status_block()
    anchor = body.index("v1.14.67 + v1.14.68: client-state label update")
    block = body[anchor:anchor + 1500]
    # The pre-extraction local var name must not survive in
    # this area.
    assert "_lrBtnEarly" not in block
    # The early-update block must NOT contain a direct textContent
    # assign — that's the helper's job. Use a regex narrow to the
    # idle-label form to avoid matching unrelated textContent
    # writes elsewhere in the function.
    pattern = re.compile(
        r"\.textContent\s*=\s*`//\s*REFRESH\s*\$\{libraryRefreshLabel\(\)\}`"
    )
    assert not pattern.search(block), (
        "Early-update block re-inlined the textContent assign "
        "that v1.14.68 extracted into updateLibraryRefreshBtnLabel(). "
        "Both call sites must go through the helper or the "
        "logic drifts."
    )


def test_v1_14_37_hash_skip_marker_intact():
    """The v1.14.37 hash-skip optimization must still be the
    primary gate for the rest of the function. v1.14.67 only
    bypasses it for the client-state label update — the broader
    paint-skip stays in place. Pin the marker so the
    optimization isn't accidentally removed alongside the
    bypass."""
    body = _refresh_topbar_status_block()
    assert "v1.14.37: hash-skip the rest of the function" in body
    # The hash-skip return statement is still present.
    assert "if (refreshTopbarStatus._lastHash === newHash) return;" in body
