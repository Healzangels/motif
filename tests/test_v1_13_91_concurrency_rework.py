"""v1.13.91 — shared TokenBucket + wired download_concurrency.

The perf audit caught the biggest perf surprise of the session:
`settings.download_concurrency` is exposed in the UI and persisted
to config but **`grep -rn download_concurrency app/` finds zero
readers.** The worker pool was hardcoded to ("long", "general") =
two threads. the user would bump the knob 1→3, see no change, conclude
"downloads are just slow."

Related: each worker had its OWN `TokenBucket` with full capacity.
So `rate=60` with 2 threads gave aggregate 120/hr — bumping the
rate didn't linearly bump real throughput, and the knob was
dishonest.

v1.13.91 fixes both atomically:

1. **One shared TokenBucket** across every worker that downloads.
   The rate setting now means rate.

2. **Spawn `settings.download_concurrency` general workers** (was:
   hardcoded 1). Clamped to [1, 8]. The long worker stays a single
   thread (multi-thread sync/plex_enum/scan would contend on
   writer locks against itself).

These are coupled: doing #2 without #1 would multiply effective
rate by N (each new worker getting its own bucket) and the user's
"60/hr" setting would silently become "180/hr" with concurrency=3.

Tests pin the wiring via static guards on the production code +
behavioral assertions about the spawned thread count.
"""
from __future__ import annotations

import tempfile
import threading
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent

# v1.22.30: start_worker now runs a one-time _reclaim_orphan_jobs(settings.
# db_path) before spawning threads (the per-thread reset moved out of run()
# to kill a cross-thread duplicate-download race). The behavioral thread-count
# tests therefore need a settings stub with a REAL db_path. A freshly
# init_db'd temp DB makes the reclaim a clean no-op (no 'running' rows).
from app.core.db import init_db  # noqa: E402

_STUB_DB = Path(tempfile.mkdtemp(prefix="motif-v1391-")) / "motif.db"
init_db(_STUB_DB)


# ── Static guards: production code uses one shared bucket ────


def test_start_worker_creates_one_shared_bucket():
    """The pre-fix loop constructed a fresh TokenBucket per worker.
    Post-fix there's a single `shared_bucket` instance handed to
    every worker. Static guard against a regression that
    re-introduces per-thread buckets."""
    src = (REPO / "app" / "core" / "worker.py").read_text()
    fn_anchor = src.index("def start_worker(settings: Settings,")
    fn_body = src[fn_anchor:fn_anchor + 6000]
    # Exactly ONE TokenBucket constructor call in the function body.
    assert fn_body.count("TokenBucket(") == 1, (
        "v1.13.91: there must be exactly one TokenBucket "
        "instantiation in start_worker (the shared one). Pre-fix "
        "had one per worker — bumping rate didn't honor the setting."
    )
    # The variable name we use is `shared_bucket`.
    assert "shared_bucket = TokenBucket(" in fn_body


def test_start_worker_passes_shared_bucket_to_all_workers():
    """Every Worker(...) construction in start_worker must use the
    same bucket variable. Pin via the bucket=shared_bucket kwarg."""
    src = (REPO / "app" / "core" / "worker.py").read_text()
    fn_anchor = src.index("def start_worker(settings: Settings,")
    fn_body = src[fn_anchor:fn_anchor + 6000]
    # Both Worker constructions reference shared_bucket.
    bucket_uses = fn_body.count("bucket=shared_bucket,")
    assert bucket_uses >= 2, (
        f"v1.13.91: expected ≥2 bucket=shared_bucket uses (one for "
        f"long, ≥1 for general); found {bucket_uses}"
    )


# ── Static guards: download_concurrency is wired ─────────────


def test_start_worker_reads_download_concurrency():
    """Pre-v1.13.91 the setting was unused. Post-fix start_worker
    reads settings.download_concurrency and uses it to determine
    the general-worker count."""
    src = (REPO / "app" / "core" / "worker.py").read_text()
    assert "settings.download_concurrency" in src
    fn_anchor = src.index("def start_worker(settings: Settings,")
    fn_body = src[fn_anchor:fn_anchor + 6000]
    assert "settings.download_concurrency" in fn_body, (
        "settings.download_concurrency must be read inside "
        "start_worker — pre-fix it was a placebo knob"
    )


def test_general_worker_count_clamped_to_safe_range():
    """The clamp is [1, 8]: 1 floor preserves single-worker mode,
    8 ceiling protects against runaway settings + SQLite writer-
    lock contention. Pin the literal."""
    src = (REPO / "app" / "core" / "worker.py").read_text()
    fn_anchor = src.index("def start_worker(settings: Settings,")
    fn_body = src[fn_anchor:fn_anchor + 6000]
    assert "max(1, min(8, settings.download_concurrency or 1))" in fn_body, (
        "v1.13.91: general worker count must be clamped via "
        "max(1, min(8, settings.download_concurrency or 1)) — "
        "different clamp would change perf characteristics"
    )


# ── Behavioral: spawn the requested number of threads ────────


class _StubSettings:
    """Minimal Settings stub for testing thread-spawn count.
    start_worker reads .download_rate_per_hour, .download_concurrency,
    and (v1.22.30) .db_path for the one-time orphan-job reclaim."""
    def __init__(self, *, concurrency: int, rate: int = 60):
        self.download_rate_per_hour = rate
        self.download_concurrency = concurrency
        self.db_path = _STUB_DB


def _count_threads(settings, stop_event):
    """Call start_worker, count returned threads, then immediately
    stop. Each thread is a daemon so the test process can exit
    even if cleanup is incomplete."""
    from app.core.worker import start_worker
    threads = start_worker(settings, stop_event)
    return len(threads), threads


def test_spawns_one_general_worker_when_concurrency_one():
    """Default concurrency=1: 1 long + 1 download + 1 place = 3 threads
    (v1.20.40 split the general pool into a download worker + a dedicated
    concurrent place worker)."""
    stop = threading.Event()
    stop.set()  # Cause workers to exit immediately.
    n, threads = _count_threads(_StubSettings(concurrency=1), stop)
    assert n == 3, f"expected 3 threads (1 long + 1 download + 1 place), got {n}"
    # Wait for all threads to exit (stop_event already set).
    for t in threads:
        t.join(timeout=2.0)


def test_spawns_three_general_workers_when_concurrency_three():
    """concurrency=3 → 1 long + 3 download + 1 place = 5 threads."""
    stop = threading.Event()
    stop.set()
    n, threads = _count_threads(_StubSettings(concurrency=3), stop)
    assert n == 5, f"expected 5 threads (1 long + 3 download + 1 place), got {n}"
    for t in threads:
        t.join(timeout=2.0)


def test_clamps_concurrency_to_eight_max():
    """Runaway setting (concurrency=99) gets clamped to 8 download
    workers → 1 long + 8 download + 1 place = 10 threads."""
    stop = threading.Event()
    stop.set()
    n, threads = _count_threads(_StubSettings(concurrency=99), stop)
    assert n == 10, f"expected 10 threads (1 long + 8 download + 1 place), got {n}"
    for t in threads:
        t.join(timeout=2.0)


def test_clamps_concurrency_zero_to_one_floor():
    """concurrency=0 (broken config) clamps to 1 download worker —
    1 long + 1 download + 1 place = 3."""
    stop = threading.Event()
    stop.set()
    n, threads = _count_threads(_StubSettings(concurrency=0), stop)
    assert n == 3, f"expected 3 threads (1 long + 1 download + 1 place), got {n}"
    for t in threads:
        t.join(timeout=2.0)


def test_clamps_negative_concurrency_to_one_floor():
    """concurrency=-5 (impossible but defensive): clamps to 1 download
    worker → 3 threads total."""
    stop = threading.Event()
    stop.set()
    n, threads = _count_threads(_StubSettings(concurrency=-5), stop)
    assert n == 3, f"expected 3 threads (1 long + 1 download + 1 place), got {n}"
    for t in threads:
        t.join(timeout=2.0)


# ── Long worker stays a single thread (no multi-long parallelism) ─


def test_long_worker_is_always_single_thread():
    """Even with concurrency=8, only ONE long worker spawns.
    Multi-thread sync/plex_enum/scan would contend on writer
    locks against itself (sync writes themes; plex_enum writes
    plex_items; both are full-table operations). Pin the
    invariant."""
    src = (REPO / "app" / "core" / "worker.py").read_text()
    fn_anchor = src.index("def start_worker(settings: Settings,")
    fn_body = src[fn_anchor:fn_anchor + 6000]
    # The long worker construction is NOT inside the for-loop.
    long_anchor = fn_body.index('Worker(settings=settings, stop_event=stop_event,\n'
                                 '                    bucket=shared_bucket,\n'
                                 '                    job_type_filter=_LONG_JOB_TYPES,')
    # The for-loop starts AFTER the long worker is constructed.
    loop_anchor = fn_body.index("for i in range(general_n):")
    assert long_anchor < loop_anchor, (
        "Long worker must be constructed OUTSIDE the general-worker "
        "for-loop. Multi-long would deadlock writer locks."
    )


def test_thread_names_distinguish_general_workers_when_n_gt_1():
    """When concurrency > 1, the download worker threads get suffixes
    download-1, download-2, etc. so `ps`/`top` show distinct names.
    v1.20.40 renamed the pool general → download (the place worker is
    its own single thread)."""
    src = (REPO / "app" / "core" / "worker.py").read_text()
    fn_anchor = src.index("def start_worker(settings: Settings,")
    fn_body = src[fn_anchor:fn_anchor + 8000]
    assert 'label = f"download-{i + 1}" if general_n > 1 else "download"' in fn_body
