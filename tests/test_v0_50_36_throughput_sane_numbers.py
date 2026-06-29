"""v0.50.36 / v0.50.39 — Live Ops RUN INSIGHT shows sane peak/s + avg/s.

the user saw "400000 peak/s · 207301 avg/s" on a 10,514-item / 1m17s Plex refresh
— a run that really averaged ~136/s. Root cause: progress.py sampled throughput as
delta/dt with a 0.001s dt floor, so a fast batch (a 10k-row upsert advancing
processed_total twice within a millisecond) divided by ~0 and the per-sample rate
exploded to ~400000 — polluting peak/s, avg/s, the live items/sec pill, the ETA and
the sparkline.

v0.50.39 fixes it at the source: progress.py floors dt at 1.0s, so `rate` is a sane
items/sec everywhere. The RUN INSIGHT then reports avg/s = total processed /
wall-clock elapsed and peak/s = the max (now-sane) sample rate. (v0.50.36 first did
this with a renderer-side bucket reconstruction; v0.50.39 moved the fix to the root
so the pill/ETA/bars/tooltips are sane too, and dropped the bucket math.)
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
OPS = (REPO / "app" / "web" / "static" / "ops.js").read_text()
PROGRESS = (REPO / "app" / "core" / "progress.py").read_text()


def test_progress_floors_dt_at_one_second():
    # the root-cause fix: a sub-second burst can no longer divide by ~0
    assert "max((now_dt - prev_ts).total_seconds(), 1.0)" in PROGRESS
    # the old explosive 0.001s floor code form is gone (the comment may still
    # reference the value as archaeology, so pin the code expression specifically)
    assert "total_seconds(), 0.001)" not in PROGRESS


def test_avg_is_wall_clock_not_mean_of_rates():
    assert "const avg = elapsedS > 0 ? processed / elapsedS : 0;" in OPS
    assert "const avg = tp.reduce((a, x) => a + (x.rate || 0), 0) / tp.length;" not in OPS


def test_peak_is_max_sample_rate_now_that_rate_is_sane():
    assert "const peak = tp.length ? Math.max(0, ...tp.map((x) => x.rate || 0)) : 0;" in OPS
    # the v0.50.36 renderer-side bucket reconstruction is gone (root-cause fix)
    assert "buckets.set(" not in OPS


def test_stats_readout_uses_the_helper_and_chart_header_agrees():
    assert "const { peak, avg } = _throughputStats(op);" in OPS
    # chart header rides the same max the bars normalize to (no separate sanePeak)
    assert "peak ${max.toFixed(0)}" in OPS
