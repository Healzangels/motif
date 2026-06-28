"""v1.22.70 (audit round 2, Batch B #6) — two stuck-UI wedges in app.js.

(1) syncWatcher click-path missing un-primed cleanup. The click handler
POSTs /api/sync/now then polls /api/stats every 2s, arming `primed` on
the first tick that sees the sync in flight. If the job finished (or
failed terminally) inside the POST→first-tick gap, primed never armed:
the watcher polled FOREVER, and — because refreshTopbarStatus's unlock
is gated on `!syncWatcher` (CLAUDE.md bug class 6) — the SYNC button
wedged at // SYNCING… until page reload. The v1.13.29 reload-path
watcher always had the un-primed clear branch; the click path didn't.

(2) OFFLINE pill wedge. refreshTopbarStatus's catch flips the idle pill
to OFFLINE, but the IDLE restore lives in the success path BELOW the
v1.14.37 hash gate (whose safety comment wrongly claimed the recovery
wasn't hash-gated). After a transient blip on an idle install, the next
successful payload usually hashes byte-identical to the pre-blip one →
the gate returned early → the pill stayed OFFLINE indefinitely. Fix
mirrors v1.20.60 (loadQueue): the catch clears _lastHash so the next
success always re-paints.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


_RELOAD_MARKER = "was `primed = true`"


def _click_watcher_region() -> str:
    """The click-path watcher: first setInterval after the sync POST,
    ending where the reload-path (v1.13.29) begins."""
    start = APP_JS.index("'/api/sync/now'")
    end = APP_JS.index(_RELOAD_MARKER, start)
    return APP_JS[start:end]


def test_click_path_has_unprimed_clear_branch():
    region = _click_watcher_region()
    i = region.index("else if (!primed && inFlight === 0)")
    branch = region[i:i + 1500]
    assert "clearInterval(syncWatcher)" in branch
    assert "syncWatcher = null" in branch
    # It must NOT flash DONE for a run it never observed busy.
    assert "setSyncButtonState('done')" not in branch


def test_reload_path_unprimed_branch_still_present():
    """Mirror parity lock: the v1.13.29 reload-path branch this fix
    copied must itself stay in place."""
    i = APP_JS.index(_RELOAD_MARKER)
    region = APP_JS[i:i + 4000]
    assert "else if (!primed && inFlight2 === 0)" in region


def test_offline_catch_clears_last_hash():
    """The OFFLINE flip and the hash clear live in the same catch, in
    that order, before the v1.12.2 error log."""
    i = APP_JS.index("idle.classList.add('op-pill-offline');")
    after = APP_JS[i:i + 900]
    j = after.index("refreshTopbarStatus._lastHash = '';")
    k = after.index("refreshTopbarStatus failed:")
    assert j < k, "hash clear must precede the v1.12.2 error log"


def test_hash_gate_still_present():
    """The v1.14.37 gate the fix compensates for must still exist —
    if a refactor removes it, the catch-side clear becomes dead code
    and this file should be re-evaluated."""
    assert "refreshTopbarStatus._lastHash === newHash" in APP_JS
    assert "refreshTopbarStatus._lastHash = newHash" in APP_JS
