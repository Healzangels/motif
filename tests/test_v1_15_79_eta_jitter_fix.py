"""v1.15.79 — kill misleading ETA 0s on TDB sync apply-paths stage.

the user: "sometime on the themerrdb sync will see it stall and
sit at a stage for a while with no eta."

Root cause analysis from his repro: ops drawer showed
"Applying 9733 changed paths | 4945/9733 | RATE 12703.8/s
| ETA 0s | ELAPSED 1m 54s". A single batch-flush sample at
12,703/s (the burst rate during a multi-row DB upsert)
dominated the 10-sample tail used by smoothedRate(); ETA
= remaining / spike-rate = 4788/12703 ≈ 0.38s → rounded to
"0s" on screen. The user reads it as "stalled, nothing
happening" while it's actually still processing items
between bursts at the longer-term ~43/s average rate.

Two fixes in ops.js:

1. Widen smoothedRate from last-10 → ALL throughput samples
   (the full ~30-sample buffer = ~9s of history). Spikes get
   diluted enough that ETA tracks completion better.

2. Sanity-clamp eta(): when computed ETA < 1s but remaining >
   50 items, return null. UI renders "—" via fmtDuration's
   `!isFinite(seconds)` branch — honest "calculating" instead
   of misleading "0s".

The progress bar itself was never stuck — server-side ticks
update via op_progress.update_progress every 0.3s in the
apply loop. Only the ETA computation was misleading.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
OPS_JS = REPO / "app" / "web" / "static" / "ops.js"


def test_smoothed_rate_uses_full_throughput_buffer():
    """smoothedRate must average the full throughput buffer (not
    `.slice(-10)`). The narrow window made batch-flush spikes
    dominate the smoothed value."""
    js = OPS_JS.read_text()
    fn_start = js.index("function smoothedRate(")
    fn_end = js.index("function eta(", fn_start)
    body = js[fn_start:fn_end]
    assert ".slice(-10)" not in body, (
        "v1.15.79: smoothedRate must not slice the last 10 samples "
        "— that narrow window let single batch-flush spikes dominate "
        "the smoothed value, producing ETA 0s with thousands of "
        "items still to process"
    )
    # Must reduce over the full buffer.
    assert "throughput.reduce" in body, (
        "v1.15.79: smoothedRate must reduce over the full throughput "
        "buffer for spike-dilution"
    )


def test_eta_returns_null_when_sub_1s_with_remaining_work():
    """eta() must clamp to null when computed value < 1s AND
    remaining > 50 items — that combination signals the rate
    sample is too volatile to trust for projection. UI then
    renders "—" via fmtDuration's invalid-input branch."""
    js = OPS_JS.read_text()
    fn_start = js.index("function eta(")
    # The eta() body is short — anchor on the next top-level function.
    fn_end = js.index("function pctOf(", fn_start)
    body = js[fn_start:fn_end]
    assert "projected < 1" in body, (
        "v1.15.79: eta() must check `projected < 1` for the "
        "sanity-clamp"
    )
    assert "remaining > 50" in body, (
        "v1.15.79: clamp must gate on remaining > 50 items so the "
        "legitimate near-done case (5 items left) still shows '<1s'"
    )
    assert "return null" in body, (
        "v1.15.79: clamp must return null so fmtDuration renders '—'"
    )


def test_fmt_duration_renders_em_dash_for_null():
    """Counter-guard: fmtDuration's null/invalid branch must
    return '—' (em dash). The clamp returns null and the UI
    needs to render something sensible — '—' communicates
    "calculating, not stalled."""
    js = OPS_JS.read_text()
    fn_start = js.index("function fmtDuration(")
    fn_end = js.index("function fmtClock(", fn_start)
    body = js[fn_start:fn_end]
    # The null/NaN/negative guard must return em-dash, not "0s".
    assert "return '—'" in body, (
        "v1.15.79: fmtDuration must return '—' for invalid input "
        "so the clamped ETA renders as the calculating placeholder"
    )
    assert "isFinite(seconds)" in body
