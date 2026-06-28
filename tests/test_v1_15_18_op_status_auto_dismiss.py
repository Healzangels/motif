"""v1.15.18 — auto-dismiss op-status messages after a short delay.

the user v1.15.17 follow-up: "for the reprobe failures button when
you click cancel it says cancelled until the page is refreshed
maybe make it dismiss the message after a small duration, same
with on completion of a run since that info lives in the info
card [LIVE OPS] as well".

## Pre-fix

`_watchOpForCompletion` set the status text on terminal
transitions (✓ done — N processed / × cancelled / ✗ failed)
and never cleared it. The text persisted next to the //
PROBE TDB URLS / // REPROBE FAILURES / // REPROBE PLEX THEMES
buttons until the operator refreshed the page. The same info
already lives in the LIVE OPS // LAST OPS drawer, so the
inline status is a transient signal — should fade once read.

## Fix

New `_autoDismissOpStatus(statusEl, ms=8000)` helper that
clears textContent + form-status-* classes after the delay.
Snapshot guard: if a fresh run sets new text before the
timer fires, the dismiss leaves the new text alone (only
clears the snapshot we captured).

Wired into all four terminal branches in
`_watchOpForCompletion`:
  - ✓ done — N processed
  - × cancelled
  - ✗ <other status>
  - ✓ done — see // LIVE OPS for the run summary (sawActive
    + op gone fast-path)

8 seconds — long enough to read at a glance, short enough
that a stale status doesn't confuse the next action.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"


def test_auto_dismiss_helper_function_defined():
    """The helper must exist and be named consistently. Pin
    the name so future refactors don't accidentally rename
    it without updating the four call sites."""
    src = APP_JS.read_text()
    assert "function _autoDismissOpStatus(statusEl" in src, (
        "v1.15.18: _autoDismissOpStatus helper required for "
        "auto-dismissing op-status messages"
    )


def test_helper_clears_textcontent_and_classes():
    """The helper must clear both textContent and the
    form-status-* classes — the colored text color is what
    makes a stale '✓ done' visually loud, so just clearing
    text without dropping the class would leave a colored
    empty span behind."""
    src = APP_JS.read_text()
    fn_anchor = src.index("function _autoDismissOpStatus")
    fn_block = src[fn_anchor:fn_anchor + 1500]
    assert "statusEl.textContent = ''" in fn_block
    assert "statusEl.classList.remove" in fn_block
    assert "form-status-ok" in fn_block
    assert "form-status-fail" in fn_block


def test_helper_uses_snapshot_guard():
    """A fresh run between the terminal status set + the
    timer fire must not have its message wiped by the stale
    timer. The helper captures statusEl.textContent at call
    time and only clears if it still matches when the timer
    fires."""
    src = APP_JS.read_text()
    fn_anchor = src.index("function _autoDismissOpStatus")
    fn_block = src[fn_anchor:fn_anchor + 1500]
    assert "snapshot" in fn_block
    assert "statusEl.textContent !== snapshot" in fn_block, (
        "v1.15.18: the dismiss timer must compare textContent "
        "against the snapshot taken at call time so a fresh "
        "run's message isn't wiped by a stale timer"
    )


def test_helper_default_delay_is_eight_seconds():
    """8s is the documented default — long enough to read,
    short enough that stale statuses don't accumulate.
    Optional ms parameter for callers that want to override."""
    src = APP_JS.read_text()
    fn_anchor = src.index("function _autoDismissOpStatus")
    fn_block = src[fn_anchor:fn_anchor + 1500]
    assert "8000" in fn_block


def test_terminal_branches_call_auto_dismiss():
    """All four terminal branches in _watchOpForCompletion
    must call _autoDismissOpStatus. Pin the call sites so a
    new branch added later doesn't silently skip the dismiss."""
    src = APP_JS.read_text()
    watcher_anchor = src.index("function _watchOpForCompletion(")
    watcher_end = src.index("function _autoDismissOpStatus", watcher_anchor)
    watcher_body = src[watcher_anchor:watcher_end]
    # Should contain at least 2 _autoDismissOpStatus calls (one
    # in the sawActive fast-path, one in the terminal-status
    # branch covering all three terminal cases via fall-through).
    n_calls = watcher_body.count("_autoDismissOpStatus(statusEl)")
    assert n_calls >= 2, (
        f"v1.15.18: expected ≥2 _autoDismissOpStatus calls in "
        f"_watchOpForCompletion (sawActive fast-path + terminal "
        f"branches); got {n_calls}"
    )


def test_dismiss_after_done_branch():
    """The '✓ done — N processed' branch specifically must
    schedule a dismiss. Pin via proximity check (textContent
    set with 'done — ' followed within the same block by an
    _autoDismissOpStatus call)."""
    src = APP_JS.read_text()
    done_anchor = src.index("`✓ done — ${processed} processed`")
    # Look forward up to 800 chars for the dismiss call.
    after_done = src[done_anchor:done_anchor + 800]
    assert "_autoDismissOpStatus" in after_done, (
        "v1.15.18: the '✓ done — N processed' branch must "
        "schedule auto-dismiss"
    )


def test_dismiss_after_cancelled_branch():
    """The '× cancelled' branch — the user's specific complaint —
    must schedule a dismiss."""
    src = APP_JS.read_text()
    cancelled_anchor = src.index("'× cancelled'")
    after_cancelled = src[cancelled_anchor:cancelled_anchor + 800]
    assert "_autoDismissOpStatus" in after_cancelled, (
        "v1.15.18: the '× cancelled' branch must schedule "
        "auto-dismiss (the user's specific repro)"
    )
