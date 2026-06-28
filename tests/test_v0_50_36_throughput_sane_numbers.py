"""v0.50.36 — Live Ops RUN INSIGHT shows sane peak/s + avg/s numbers.

the user saw "400000 peak/s · 207301 avg/s" on a 10,514-item / 1m17s Plex refresh
— a run that really averaged ~136/s. Cause: progress.py samples throughput as
delta/dt with a 0.001s dt floor, so a fast batch (a 10k-row upsert advancing
processed_total in a sub-second burst) divides by ~0 and the rate explodes. The
old STATS block took max(rate) for peak and mean(rate) for avg — both inflated.

v0.50.36 reports honest numbers via a shared _throughputStats(op):
  - avg/s = total processed / wall-clock elapsed (op.started_at → finished_at)
  - peak/s = the busiest WHOLE-SECOND window (items per sample = rate×dt, bucket
    by second, take the max) — truthful, not a 1ms micro-burst artifact.
Both the STATS readout and the THROUGHPUT chart header use it.
"""
from __future__ import annotations

from pathlib import Path

OPS = (Path(__file__).resolve().parent.parent / "app" / "web" / "static" / "ops.js").read_text()


def test_throughput_stats_helper_exists():
    assert "function _throughputStats(op)" in OPS


def test_avg_is_wall_clock_not_mean_of_rates():
    # avg = processed / elapsed seconds (the honest average)
    assert "const avg = elapsedS > 0 ? processed / elapsedS : 0;" in OPS
    # the old mean-of-rates avg/s computation is gone
    assert "const avg = tp.reduce((a, x) => a + (x.rate || 0), 0) / tp.length;" not in OPS


def test_peak_is_busiest_whole_second_window():
    # reconstruct items per sample (rate × dt) and bucket by whole second
    assert "buckets.set(sec, (buckets.get(sec) || 0) + (x.rate || 0) * dt);" in OPS
    assert "peak = Math.max(...buckets.values());" in OPS
    # the STATS block no longer takes a raw max(rate) for peak/s
    assert "const peak = Math.max(...tp.map((x) => x.rate || 0));" not in OPS


def test_both_surfaces_use_the_shared_helper():
    # STATS readout + the THROUGHPUT chart header both route through _throughputStats
    assert "const { peak, avg } = _throughputStats(op);" in OPS
    assert "const sanePeak = _throughputStats(op).peak;" in OPS
    assert "peak ${sanePeak.toFixed(0)}" in OPS
