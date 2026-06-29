"""v0.50.40 — Live Ops drawer display accuracy (audit, frontend half).

From the 4-agent drawer-numbers audit:
  1. A cancelled/failed real-bar op snapped its progress bar to 100% (only
     status==='done' should), falsely reading as complete.
  2. The RUN INSIGHT done_summary STATS block rendered on a CANCELLED op (some
     workers stamp done_summary right before cancelling), so it read as a
     completion readout while the headline said "Cancelled".
  3. peak/s + avg/s used .toFixed(0) (no grouping) next to done_summary's fmtNum,
     e.g. "10,514 items · 207301 avg/s".
  4. the live RATE pill used .toFixed(1) at all magnitudes ("10000.0/s").
"""
from __future__ import annotations

from pathlib import Path

OPS = (Path(__file__).resolve().parent.parent / "app" / "web" / "static" / "ops.js").read_text()


def test_cancelled_failed_bar_freezes_not_full():
    i = OPS.index("if (!showLiveSections) {")
    block = OPS[i:i + 700]
    assert "op.status === 'done' ? 100 : Math.round(pctOf(op) || 0)" in block
    # the bar width is the conditional finPct, not an unconditional 100%
    assert "width:${finPct}%" in block


def test_done_summary_stats_skipped_on_cancelled():
    assert "Array.isArray(ds) && op.status !== 'cancelled'" in OPS


def test_peak_avg_comma_grouped_like_done_summary():
    assert "stats.push({ label: 'peak/s', value: fmtNum(Math.round(peak)) })" in OPS
    assert "stats.push({ label: 'avg/s', value: fmtNum(Math.round(avg)) })" in OPS
    # the old un-grouped toFixed(0) STATS values are gone
    assert "value: peak.toFixed(0)" not in OPS
    assert "value: avg.toFixed(0)" not in OPS


def test_rate_pill_is_magnitude_aware():
    assert "function fmtRate(rate)" in OPS
    assert "return rate < 10 ? rate.toFixed(1) : fmtNum(Math.round(rate));" in OPS
    assert "${fmtRate(rate)}/s" in OPS
    # both pill sites use the helper; no raw toFixed(1)/s left
    assert "${rate.toFixed(1)}/s" not in OPS
