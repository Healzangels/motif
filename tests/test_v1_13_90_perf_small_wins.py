"""v1.13.90 — small perf wins from the perf audit.

Two independent fixes from the perf-audit triage doc:

1. **Memoize `_compute_next_cron_fire`**: the function ran on every
   /api/stats hit (15s topbar polling × every page × every
   post-action 1100ms refresh). Each call imports apscheduler,
   parses the crontab, computes the next fire — for a value that
   changes only at cron-fire boundaries (typically minutes apart).
   60s TTL added.

2. **Bump TV per-show fallback workers 8 → 16**: when Plex omits
   `<Location>` on bulk listing, plex.py fans out per-item
   /library/metadata GETs via ThreadPoolExecutor. ~5K shows ÷ 8
   workers = ~625 sequential round-trips. Doubled to 16 — Plex
   tolerates per the inline comment, no DB-side contention.
   ~30-40s → ~15-20s on the user's library.

Both are zero behavior change beyond the speedup — pure performance
tuning. Static guards pin the production code.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── Fix 1: cron memoization ──────────────────────────────────


def test_cron_compute_has_module_level_cache():
    """The cache dict + TTL constants must be defined at the same
    closure level as _compute_next_cron_fire so they survive
    across calls."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    assert "_cron_fire_cache: dict = {" in src
    assert "_cron_fire_cache_ttl = 60.0" in src


def test_cron_compute_returns_cached_within_ttl():
    """The function must check the cache BEFORE the apscheduler
    import (which is the actual cost). Static guard against a
    refactor that moves the cache check below the import."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_start = src.index("def _compute_next_cron_fire(cron_expr: str)")
    body = src[fn_start:fn_start + 2000]
    # Cache check appears BEFORE the apscheduler import.
    cache_pos = body.index("(now_mono - cache[\"ts\"]) < _cron_fire_cache_ttl")
    apsched_pos = body.index("from apscheduler.triggers.cron")
    assert cache_pos < apsched_pos, (
        "Cache hit must short-circuit BEFORE the apscheduler "
        "import — that's the whole point of the memo"
    )


def test_cron_compute_caches_by_expr():
    """The cache key is the cron expression, so different cron
    strings get separate cached values. Static guard."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_start = src.index("def _compute_next_cron_fire(cron_expr: str)")
    body = src[fn_start:fn_start + 2000]
    assert 'cache["key"] == cron_expr' in body
    assert 'cache["key"] = cron_expr' in body  # write on miss


def test_cron_compute_caches_none_results_too():
    """Failed parses (invalid cron) should also be cached so we
    don't repeatedly try-and-fail. Pin via the unconditional
    cache write at the end of the function (after both success
    and exception paths set `value`)."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_start = src.index("def _compute_next_cron_fire(cron_expr: str)")
    body = src[fn_start:fn_start + 2000]
    # The except branch sets `value = None`, then fall-through
    # writes to cache.
    except_pos = body.index("except Exception:")
    write_pos = body.index('cache["value"] = value')
    assert except_pos < write_pos, (
        "Cache write must happen AFTER the except, so None "
        "results from a bad cron are also cached"
    )


# ── Fix 2: TV per-show fallback worker bump ──────────────────


def test_tv_fallback_uses_bulk_metadata_not_per_item_threadpool():
    """v1.13.90 bumped the per-item fallback ThreadPool from
    8 → 16 workers. v1.14.76 superseded the per-item pool
    entirely with bulk /library/metadata fetches (50 ids per
    call, 4 concurrent batches) — a strictly better speedup
    than any worker-count tweak.

    The fallback callsite must now invoke get_item_paths_bulk;
    the pre-fix per-item ThreadPool wrap (def _fill / map(_fill,
    missing) / max_workers=16) must be gone from the fallback
    block. The bulk method internally still uses a small
    ThreadPool (max_workers=4) — but that's a different call
    site, not a regression."""
    src = (REPO / "app" / "core" / "plex.py").read_text()
    fallback_anchor = src.index("v1.11.83: per-item folder_path fallback")
    fallback_block = src[fallback_anchor:fallback_anchor + 3000]
    # The new bulk callsite is present.
    assert "get_item_paths_bulk(" in fallback_block, (
        "v1.14.76: fallback must use the bulk metadata method"
    )
    # The pre-fix per-item ThreadPool shape must be gone.
    assert "max_workers=16" not in fallback_block, (
        "v1.14.76: per-item ThreadPool wrap was retired — bulk "
        "method handles concurrency internally with smaller fan-out"
    )
    assert "def _fill(" not in fallback_block
