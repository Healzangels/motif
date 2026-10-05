"""v0.51.357 — why the library stat batch is 8, measured rather than assumed.

The batch size looks like free throughput: on a dev machine a warm stat is ~2 us, the pool's own scheduling costs
~9 us per stat, and raising _LIB_STAT_BATCH from 8 to 64 cuts a 5,091-stat view from 47.5 ms to 39.3 ms. That
number is a trap. It measures scheduling overhead, which only dominates when the syscall is free — and on the
operator's NAS a stat is ~0.5 ms (measured), so the pool is I/O-bound and 16 workers saturate it whatever the
batching. Swept at that latency (2026-10-05, .cache/perf/batch_tradeoff.py, 5,091 stats heavy + a 50-stat page
alongside):

    batch     heavy view     a page loading alongside
        8        215.9 ms                     3.9 ms
       16        215.8 ms                    10.0 ms
       32        232.0 ms                    19.4 ms
       64        213.9 ms                    19.6 ms
      128        258.8 ms                    62.5 ms

The heavy view is flat; the page's wait tracks the batch exactly, because every worker is busy with a batch of
that size and a light request waits for one to drain: wait ≈ batch x stat latency (8 x 0.5 = 4 ms, measured 3.9;
128 x 0.5 = 64 ms, measured 62.5). So the batch is a head-of-line budget, not a throughput knob — which is what
v0.51.346's one-line comment says, now with the numbers behind it.
"""
from __future__ import annotations

import threading
import time

from app.web.api import _LIB_STAT_BATCH, _LIB_STAT_WORKERS, _lib_stat_map

# the operator's measured prod stat latency (motif-prod-stat-latency): ~0.5 ms on the NAS
PROD_STAT_MS = 0.5
# what a page loading while a heavy view runs should not exceed, by the wait ≈ batch x latency relation above
HEAD_OF_LINE_BUDGET_MS = 5.0


def test_the_batch_keeps_a_light_page_under_the_head_of_line_budget():
    """Raising the batch does not speed the heavy view up (it is I/O-bound) and slows every page that loads
    during one. If this fails because someone raised the batch, re-read the sweep in the docstring first."""
    worst_wait_ms = _LIB_STAT_BATCH * PROD_STAT_MS
    assert worst_wait_ms <= HEAD_OF_LINE_BUDGET_MS, (
        f"_LIB_STAT_BATCH={_LIB_STAT_BATCH} puts a light page {worst_wait_ms:.1f} ms behind a heavy view on the "
        f"operator's NAS; the budget is {HEAD_OF_LINE_BUDGET_MS} ms")


def test_a_heavy_view_cannot_park_its_whole_backlog_in_front_of_the_next_page():
    """The mechanism behind the budget: a request queues at most _LIB_STAT_WORKERS batches at a time, so a page
    arriving mid-sweep waits for ONE batch to drain, not for thousands of stats. Measuring running concurrency
    does not test this — there are only 16 worker threads either way; what the cap bounds is QUEUE depth, so the
    test has to be the wait itself. Timed with a sleeping stat (the GIL is released, as in a real one) and
    asserted as a ratio, so a loaded machine inflates both sides together."""
    heavy_jobs = _LIB_STAT_WORKERS * _LIB_STAT_BATCH * 8
    out = {}

    def slow(job):
        time.sleep(0.001)
        return False

    def heavy():
        t = time.perf_counter()
        _lib_stat_map(slow, [(True, f"h{i}") for i in range(heavy_jobs)])
        out["heavy_ms"] = (time.perf_counter() - t) * 1000

    th = threading.Thread(target=heavy)
    th.start()
    time.sleep(0.02)                      # let the heavy request fill the queue
    t = time.perf_counter()
    _lib_stat_map(slow, [(True, f"l{i}") for i in range(_LIB_STAT_BATCH)])
    light_ms = (time.perf_counter() - t) * 1000
    th.join()

    assert light_ms < out["heavy_ms"] / 2, (
        f"a {_LIB_STAT_BATCH}-stat page waited {light_ms:.0f} ms behind a {heavy_jobs}-stat view that took "
        f"{out['heavy_ms']:.0f} ms — the in-flight cap is what keeps that wait to about one batch")


def test_the_pool_still_returns_results_in_job_order():
    """The batching is invisible to the caller — results come back per job, in order, however they were grouped."""
    out = _lib_stat_map(lambda job: job[1], [(True, i) for i in range(_LIB_STAT_BATCH * 3 + 1)])
    assert out == list(range(_LIB_STAT_BATCH * 3 + 1))
