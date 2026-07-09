"""v0.51.105 — adaptive topbar poll so the RE-PUSH badge tracks row state.

the user: after fixing a re-push, the upper-right RE-PUSH indicator lingered a
while after the row already resolved. refreshTopbarStatus (which owns the
RE-PUSH/FAIL/UPD badges) polled on a FIXED 10s setInterval; a per-row `place`
job (the re-push) emits no op_progress row, so it doesn't fire the
motif:ops-state-changed event that force-refreshes the topbar — the badge waited
for the next 10s tick while libraryRapidPoll (~2s) had already resolved the row.

v0.51.105 makes the topbar poll ADAPTIVE: 2s while any mutating op is active,
10s idle — reading the same anyMutatingOpActive signal refreshTopbarStatus
already stashes. So badges track rows during ops.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def test_topbar_poll_is_adaptive_not_fixed_10s():
    # the old fixed-interval driver is gone.
    assert "setInterval(refreshTopbarStatus, 10000)" not in APP_JS, (
        "v0.51.105: the fixed 10s topbar setInterval must be replaced by the "
        "adaptive self-scheduling poll")
    # the adaptive self-scheduling loop exists.
    assert "function _scheduleTopbarPoll(" in APP_JS
    i = APP_JS.index("function _scheduleTopbarPoll(")
    block = APP_JS[i:i + 700]
    # it re-invokes refreshTopbarStatus, then picks the next delay from the
    # mutating-op signal: 2s active / 10s idle.
    assert "refreshTopbarStatus()" in block
    assert "anyMutatingOpActive" in block
    assert "active ? 2000 : 10000" in block
    # and it's actually kicked off.
    assert "_scheduleTopbarPoll(10000);" in APP_JS
