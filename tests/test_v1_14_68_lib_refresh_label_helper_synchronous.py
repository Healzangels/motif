"""v1.14.68 — sync label update on //4K toggle (no network RTT).

the user v1.14.67 follow-up:

> "when swapping between libraries or standard and 4k the
>  buttons for refresh library are changing but there is a
>  noticeable delay when swapping and the text updating"

## Root cause

v1.14.67's fix put the early-update block INSIDE
refreshTopbarStatus, which awaits /api/stats before any DOM
update. The //4K toggle handler called refreshTopbarStatus(),
but the await added a 50-200ms network round-trip BEFORE the
label flipped — visible delay between the chip-active class
flipping and the button text catching up.

## Fix

Extract the v1.14.67 inline block into a module-scope helper
`updateLibraryRefreshBtnLabel()`. Call it:

  - From the //4K toggle handler (synchronously, pre-async),
    so the label flips on the same paint as the chip-active
    class.
  - From refreshTopbarStatus's pre-hash-skip block (the
    v1.14.67 site, just calls the helper now), as a poll-tick
    safety net for state changes that didn't go through a
    click handler — e.g. adaptLibraryFourkToggle's auto-flip
    on page load.
"""
from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
JS = REPO / "app" / "web" / "static" / "app.js"


def test_helper_function_exists_at_module_scope():
    """`updateLibraryRefreshBtnLabel` must be defined at module
    scope (next to libraryRefreshLabel) so both call sites can
    reach it without re-derivation."""
    js = JS.read_text()
    # Function declaration. v0.51.117: takes a `tightenOnly` param (the section
    # switch passes true — lock but never optimistically unlock).
    pattern = re.compile(
        r"function\s+updateLibraryRefreshBtnLabel\s*\(\s*tightenOnly\s*\)\s*\{"
    )
    assert pattern.search(js), (
        "v1.14.68 helper updateLibraryRefreshBtnLabel must be "
        "defined as a module-scope function so the //4K toggle "
        "handler + refreshTopbarStatus can both reach it."
    )


def test_helper_sets_busy_state_explicitly_when_running():
    """v1.14.68 used a textContent !== '// REFRESHING…' guard
    to prevent clobbering a busy label set elsewhere. v1.14.79
    superseded that pattern: the helper now COMPUTES busy
    itself (myTabBusy + pipeline + grace) and explicitly sets
    the // REFRESHING… label when busy. The guard isn't needed
    because the helper is now the single source of truth — no
    one else races to set the label.

    What this test pins (post-v1.14.79): when stillBusy=true,
    the helper sets the busy label explicitly. The label
    can't drift because the helper either sets it or sets the
    idle counterpart — never leaves it alone."""
    js = JS.read_text()
    fn_start = js.index("function updateLibraryRefreshBtnLabel(")
    fn_body = js[fn_start:fn_start + 3000]
    # The busy-branch label assignment is present.
    assert "btn.textContent = '// REFRESHING…'" in fn_body


def test_helper_uses_libraryRefreshLabel_for_idle_text():
    """The idle text must come from libraryRefreshLabel() —
    pinning the call so a future refactor can't inline a stale
    label expression that misses libraryState.fourk updates.

    v1.14.79: helper grew to ~30 lines (now sets disabled
    state too); slice widened from 1000 → 3000 chars."""
    js = JS.read_text()
    fn_start = js.index("function updateLibraryRefreshBtnLabel(")
    fn_body = js[fn_start:fn_start + 3000]
    pattern = re.compile(
        r"\.textContent\s*=\s*`//\s*REFRESH\s*\$\{libraryRefreshLabel\(\)\}`"
    )
    assert pattern.search(fn_body), (
        "Helper must set textContent via libraryRefreshLabel() — "
        "the client-state-only helper that reads "
        "libraryState.fourk + the library-tab DOM value."
    )


def test_fourk_toggle_handler_calls_helper_synchronously():
    """The //4K toggle handler must call the helper BEFORE the
    refreshTopbarStatus() call (which is async/awaits network).
    The synchronous call eliminates the visible delay between
    the chip-active flip and the label update."""
    js = JS.read_text()
    # Find the toggle handler.
    handler_anchor = js.index("// 4K toggle")
    # Look at the next ~1500 chars (handler body).
    handler = js[handler_anchor:handler_anchor + 2000]
    # The fourk assignment, the helper call, and the refresh
    # call must all appear in this block, in that order.
    fourk_idx = handler.index("libraryState.fourk = b.dataset.fourk")
    helper_idx = handler.index("updateLibraryRefreshBtnLabel()")
    # Match the call form, NOT the substring in any
    # "refreshTopbarStatus() call below" comment. v1.15.108
    # added a `.catch(() => {})` suffix to harden against
    # unhandled rejections — anchor on the call site, not the
    # exact trailing punctuation.
    refresh_idx = handler.index("refreshTopbarStatus().catch")
    assert fourk_idx < helper_idx < refresh_idx, (
        "v1.14.68 ordering: fourk assignment → helper call → "
        "refreshTopbarStatus(). Got: "
        f"fourk@{fourk_idx}, helper@{helper_idx}, "
        f"refresh@{refresh_idx}"
    )


def test_refresh_topbar_status_calls_helper_not_inline_block():
    """The v1.14.67 inline block was extracted to the helper.
    refreshTopbarStatus must call the helper, not inline the
    DOM manipulation. Pinning so a future "DRY violation" lint
    or refactor can't accidentally re-inline."""
    js = JS.read_text()
    fn_start = js.index("async function refreshTopbarStatus()")
    # Slice from function start (covers the early-update area + the hash-skip).
    # v0.51.120 added the pre-bail enum-stash write, so widen the window.
    body = js[fn_start:fn_start + 4500]
    # Helper call must be present.
    assert "updateLibraryRefreshBtnLabel()" in body
    # The pre-extraction inline form must NOT survive in this
    # area (the inline assignment to a local _lrBtnEarly var).
    assert "_lrBtnEarly" not in body, (
        "v1.14.67 inline `_lrBtnEarly` block survived the "
        "v1.14.68 extraction — duplicate update logic."
    )


def test_helper_is_called_above_hash_skip():
    """The helper call inside refreshTopbarStatus must sit
    BEFORE the hash-skip return — same v1.14.67 invariant. If
    it moves below, hash-skipped ticks won't update the label
    and the bug recurs."""
    js = JS.read_text()
    fn_start = js.index("async function refreshTopbarStatus()")
    body = js[fn_start:fn_start + 6500]  # v0.51.120 widened (pre-bail stash write)
    helper_idx = body.index("updateLibraryRefreshBtnLabel()")
    skip_idx = body.index(
        "if (refreshTopbarStatus._lastHash === newHash) return;"
    )
    assert helper_idx < skip_idx, (
        f"Helper call sits at {helper_idx} but hash-skip is at "
        f"{skip_idx} — helper must run first or hash-skipped "
        "ticks won't update the label."
    )
