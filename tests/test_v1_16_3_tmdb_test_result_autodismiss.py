"""v1.16.3 — TMDB TEST KEY result text auto-dismisses.

the user on v1.16.2:

> can we make the credentials valid text disappear after a bit
> like the other text we have

The `✓ credentials valid` (or `✗ <error>`) status line that
appears next to // TEST KEY after a click was sticking around
indefinitely. Every other action button in motif clears its
result text after a short window — bulk-probe summary at 5s,
ACK FAILURES at 2.5s, etc. The TEST KEY handler was the
outlier.

## Fix

Wrap the result-text-setting paths in `setTimeout(clear, N)`:
  - success → 5s (short, the user just needs confirmation)
  - error   → 8s (longer, the user needs to read the failure)

A per-click timer cancellation (`clearTimeout` on the previous
handle) keeps rapid re-clicks coherent — each click resets the
clock.

## Tests
"""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"


def _tmdb_handler_block() -> str:
    """Slice out the tmdb-test-btn click handler. Anchor on the
    getElementById call + walk to the function's outer closing
    brace via a brace-counter."""
    js = APP_JS.read_text()
    start = js.index("const tmdbBtn = document.getElementById('tmdb-test-btn')")
    # Walk forward to the matching `}` of the outer `if (tmdbBtn) { ... }` block.
    if_idx = js.index("if (tmdbBtn) {", start)
    depth = 0
    i = if_idx
    while i < len(js):
        c = js[i]
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return js[start:i + 1]
        i += 1
    raise AssertionError("if-block close brace not found")


def test_success_path_schedules_autoclear():
    """Success branch must schedule a clear-the-text timeout."""
    block = _tmdb_handler_block()
    # Find the r.ok branch and check that clearTmdbResult is
    # called with a reasonable success delay (around 5s).
    ok_idx = block.index("if (r.ok)")
    ok_window = block[ok_idx:ok_idx + 500]
    assert "clearTmdbResult(5000)" in ok_window, (
        "v1.16.3: success branch must schedule a 5-second "
        "auto-clear so the ✓ credentials valid text doesn't "
        "stick around forever."
    )


def test_error_path_schedules_autoclear_with_longer_window():
    """Error branches (server-said-no + JS exception) auto-clear
    too, but with a longer window so the user has time to read."""
    block = _tmdb_handler_block()
    # The server-error path (else of if r.ok).
    else_idx = block.index("} else {", block.index("if (r.ok)"))
    else_window = block[else_idx:else_idx + 500]
    assert "clearTmdbResult(8000)" in else_window, (
        "v1.16.3: server-error branch must auto-clear at 8s "
        "(longer than success so the user has time to read)."
    )
    # The JS-exception (catch) path.
    catch_idx = block.index("} catch (e) {")
    catch_window = block[catch_idx:catch_idx + 500]
    assert "clearTmdbResult(8000)" in catch_window, (
        "v1.16.3: JS-exception branch must also auto-clear at "
        "8s — leaving an exception message sitting forever is "
        "the original bug class."
    )


def test_clear_helper_uses_clear_timeout_to_cancel_prior():
    """Rapid re-clicks must cancel the prior timer so the
    'testing...' state of a new click isn't immediately
    overwritten by the prior click's stale auto-clear."""
    block = _tmdb_handler_block()
    # The helper definition must reset any prior tmdbResultTimer.
    helper_idx = block.index("const clearTmdbResult")
    helper_window = block[helper_idx:helper_idx + 500]
    assert "clearTimeout(tmdbResultTimer)" in helper_window, (
        "v1.16.3: clearTmdbResult helper must cancel any prior "
        "pending auto-clear before scheduling a new one — "
        "otherwise rapid click-test-click sequences race."
    )
    # The click handler must also cancel before showing the
    # transient '... testing' state.
    click_idx = block.index("addEventListener('click'")
    click_window = block[click_idx:click_idx + 800]
    assert "clearTimeout(tmdbResultTimer)" in click_window, (
        "v1.16.3: click handler must cancel any prior auto-"
        "clear before setting the '... testing' transient state."
    )


def test_clear_helper_resets_both_text_and_color():
    """When the timer fires, BOTH the text and the tone must reset
    so the next click doesn't inherit a stale green/red tone while
    the text is empty.

    v1.19.81 migrated #tmdb-test-result off the inline `style.color`
    palette onto the shared `.form-status` + `.form-status-ok/fail`
    tone classes (see test_v1_19_81_settings_uniformity.py). The
    clear helper now resets by reassigning `className = 'form-status'`
    (which drops any tone class) instead of clearing `style.color`."""
    block = _tmdb_handler_block()
    helper_idx = block.index("const clearTmdbResult")
    helper_window = block[helper_idx:helper_idx + 500]
    assert "r.textContent = ''" in helper_window
    assert "r.className = 'form-status'" in helper_window, (
        "v1.19.81: auto-clear must reset the tone by reassigning "
        "className back to bare 'form-status'. Otherwise a future "
        "success after an error would briefly flash green text in "
        "a red-toned box."
    )
