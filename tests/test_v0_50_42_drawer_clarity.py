"""v0.50.42 — Live Ops drawer clarity (audit, soft-clarity trio).

Frontend-only readout clarity, the third installment of the drawer-numbers audit
(v0.50.40 display + v0.50.41 backend accuracy):

  1. ETA over an hour is dominated by rate noise → bucket it as ">1h" instead of a
     false-precision "7h 23m". Measured ELAPSED/RAN keep fmtDuration.
  2. The live RATE pill (recent ~10s smoothed) and the STATS avg/s (whole-run)
     legitimately differ; tooltips on each say so, so the two readouts don't read
     as a contradiction.
  3. The THROUGHPUT chart-header peak is comma-grouped (fmtNum) to match the STATS
     peak/s readout — was max.toFixed(0) ("12703") beside STATS' grouped "12,703".
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
OPS = (REPO / "app" / "web" / "static" / "ops.js").read_text()


def test_fmt_eta_helper_clamps_over_one_hour():
    assert "function fmtEta(seconds)" in OPS
    # the clamp: anything past an hour buckets to ">1h"
    assert "if (seconds > 3600) return '>1h';" in OPS


def test_eta_render_sites_use_fmt_eta_not_fmt_duration():
    # both ETA display sites route through fmtEta (card render + in-place update)
    assert "${esc(fmtEta(etaSec))}" in OPS
    assert "metaUpdate('eta', fmtEta(etaSec))" in OPS
    # the pre-fix fmtDuration(etaSec) forms are gone
    assert "fmtDuration(etaSec)" not in OPS


def test_elapsed_still_uses_fmt_duration():
    # measured durations stay precise — the clamp is ETA-only
    assert "metaUpdate('elapsed', fmtDuration(elapsed))" in OPS


def test_rate_pill_has_disambiguation_tooltip():
    rate_idx = OPS.index('data-meta-key="rate"')
    window = OPS[rate_idx:rate_idx + 400]
    assert "title=" in window
    assert "Recent throughput" in window
    assert "RUN INSIGHT" in window


def test_stats_avg_and_peak_have_hint_tooltips():
    assert "Whole-run average" in OPS
    assert "Highest single 1s throughput sample" in OPS
    # the stat tile renders the hint as a title attribute
    assert "${s.hint ? ` title=\"${esc(s.hint)}\"` : ''}" in OPS


def test_throughput_header_peak_is_comma_grouped():
    # header peak now matches STATS peak/s grouping
    assert "peak ${fmtNum(Math.round(max))}" in OPS
    # the un-grouped form is gone
    assert "peak ${max.toFixed(0)}" not in OPS
