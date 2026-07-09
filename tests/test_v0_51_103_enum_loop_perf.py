"""v0.51.103 — plex-refresh perf: parallelize the health FS stats + bump upsert batch.

Tier-1 #2 + #4. The two health passes (verify_placement_health /
verify_canonical_health) stat every theme.mp3 over the network mount; that stat
is now spread across a bounded thread pool (the bucketing/accounting stays
serial, so present/missing/skipped/prune + the mount-fault cap are unchanged —
covered behaviorally by the v0.51.101 + v1.23.25/37 tests). And _UPSERT_BATCH
200→400 halves the number of inter-batch sleep yields on a large section.

These guards pin the perf shape so a refactor doesn't silently revert it.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SRC = (REPO / "app" / "core" / "plex_enum.py").read_text()


def _fn_body(name: str) -> str:
    i = SRC.index(f"def {name}(")
    # bound at the next top-level def.
    j = SRC.index("\ndef ", i + 1)
    return SRC[i:j]


def test_both_health_passes_parallelize_the_stat():
    for fn in ("verify_placement_health", "verify_canonical_health"):
        body = _fn_body(fn)
        assert "ThreadPoolExecutor" in body, (
            f"{fn} must stat across a thread pool (v0.51.103)")
        assert "_ex.map(_stat_present, rows)" in body, (
            f"{fn} must map the per-row stat over the pool")


def test_upsert_batch_raised():
    assert "_UPSERT_BATCH = 400" in SRC, (
        "v0.51.103 raised _UPSERT_BATCH 200→400 to halve the inter-batch "
        "sleep yields on a large section")
    # the resolve chunk_size is deliberately NOT widened (its 7-UPDATE chunks
    # hold the writer lock ~1-2s — see resolve_theme_ids' sleep note).
    assert "chunk_size: int = 500" in SRC
