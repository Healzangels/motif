"""v1.23.88 — the sync apply-flush skips per-item Plex work for UNCHANGED themes.

the user's prod sync (the v1.23.80 timing instrumentation paying off):
`sync apply timing: 352.8s total — read_json 10.7s, DB flush 279.2s
(5616 unique items), loop/other 63.0s` — the DB flush was the real bottleneck,
NOT git reads (which v1.23.80 had targeted).

Root cause: _flush_sync_batch computed _plex_supplies_theme + an `in_plex`
plex_items join (52k rows) for EVERY item, but those are consumed only inside the
`if is_new:` / `elif url_changed:` branches. On a no-change sync (new=0/updated=0)
every one of the 5616 items was unchanged, so ~5600 of those 52k-row joins ran
and were thrown away. The fix is an early `continue` for unchanged items —
_upsert_theme already bumped last_seen_sync_at, so nothing else is owed.

This is a pure no-op-skip optimization (the skipped branches had no effect for
unchanged items anyway), so the full suite's new/changed/notification/enqueue
tests prove correctness; this guard just pins the early-out so it can't regress.
"""
from __future__ import annotations

from pathlib import Path

SYNC_PY = (Path(__file__).resolve().parent.parent
           / "app" / "core" / "sync.py").read_text()


def test_flush_skips_unchanged_items_before_the_plex_joins():
    # the early-out must sit AFTER _upsert_theme (so its write still happens)
    # and BEFORE the _plex_supplies_theme / in_plex joins (so they're skipped).
    upsert_i = SYNC_PY.index("is_new, url_changed, old_video_id, old_url = _upsert_theme(")
    gate_i = SYNC_PY.index("if not (is_new or url_changed):", upsert_i)
    plex_i = SYNC_PY.index("plex_supplies = _plex_supplies_theme(", upsert_i)
    assert upsert_i < gate_i < plex_i, (
        "the unchanged-item early-out must run after _upsert_theme and before "
        "the per-item plex_items joins"
    )
    # it's a continue (skip the rest of the per-item body), not a return/break.
    after = SYNC_PY[gate_i:gate_i + 120]
    assert "continue" in after


def test_v1_23_88_version_pin():
    init_py = (Path(__file__).resolve().parent.parent
               / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
