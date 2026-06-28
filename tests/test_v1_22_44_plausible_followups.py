"""v1.22.44 (holistic audit — plausible-finding follow-ups).

Two of the five PLAUSIBLE finder items verified as genuine (low-severity)
silent failures and fixed; the other three were refuted or deferred (see the
session journal):

- F2 (ops.js cancel handlers): a failed cancel (non-2xx — the op already went
  terminal → 404/409, or a 500) reset the button with NO poll and NO message,
  so the user saw a stale CANCEL button and no signal. Now both the per-op and
  bulk cancel handlers ALWAYS force a reconciling poll (the success branch
  already did) so the drawer re-fetches reality.
- W6 (scheduler._stuck_job_sweep): the last_error claimed "will retry per
  backoff schedule" but the sweep sets status='failed' TERMINALLY (it never
  routes through _mark_failed's attempts/backoff ladder). Message corrected.

(W3 worker._mark_transient retry arithmetic — REFUTED, terminates correctly;
 S5 plex_enum auto-download — REFUTED, by-design toggle; S4 _fetch_index page
 count — deferred, mitigated by the 5%/50-row drop-sweep cap + self-correction.)
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
OPS_JS = (REPO / "app" / "web" / "static" / "ops.js").read_text()
SCHED_PY = (REPO / "app" / "core" / "scheduler.py").read_text()


# ── F2: cancel handlers always reconcile ──────────────────────


def _handler(anchor: str) -> str:
    idx = OPS_JS.index(anchor)
    return OPS_JS[idx:idx + 1100]


def test_per_op_cancel_always_polls_on_failure():
    block = _handler("postCancel(opId).then((ok) => {")
    # The reset is still gated on !ok...
    assert "if (!ok) {" in block
    # ...but the poll is no longer inside an `else` — it runs unconditionally.
    reset_end = block.index("}", block.index("if (!ok) {"))
    after_reset = block[reset_end:reset_end + 600]
    assert "else {" not in after_reset, (
        "v1.22.44: the reconciling poll must NOT be gated behind `else` — a "
        "failed cancel must still re-poll so the UI isn't left stale")
    assert "setTimeout(poll, 200)" in after_reset


def test_bulk_cancel_always_polls_on_failure():
    block = _handler("postBulkCancel(jobType).then((ok) => {")
    assert "if (!ok) {" in block
    reset_end = block.index("}", block.index("if (!ok) {"))
    after_reset = block[reset_end:reset_end + 600]
    assert "else {" not in after_reset, (
        "v1.22.44: bulk-cancel must also re-poll on failure")
    assert "setTimeout(poll, 200)" in after_reset


# ── W6: stuck-sweep last_error is accurate (terminal, not retry) ─


def test_stuck_sweep_message_is_accurate():
    idx = SCHED_PY.index("def _stuck_job_sweep")
    # v1.24.20 widened 4000→6000: the heartbeat fast-reclaim path lengthened
    # the function body, pushing the message past the old window.
    body = SCHED_PY[idx:idx + 6000]
    assert "Reset to failed (terminal)" in body, (
        "v1.22.44: the stuck-sweep message must say the reset is TERMINAL")
    assert "will retry per backoff schedule" not in body, (
        "v1.22.44: the inaccurate auto-retry claim must be gone — the sweep "
        "sets status='failed' directly, not via _mark_failed's backoff ladder")
