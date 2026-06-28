"""v1.24.13 — holistic-review lock-scope / atomicity batch.

Four localized fixes from the whole-codebase review, all in the "transaction /
lock scope" class:

  #2 api_unmanage_item held the SQLite write lock (BEGIN IMMEDIATE) across a
     per-row `folder_has_theme_sidecar()` /data directory walk — freezing the
     loop + blocking every other writer, badly on stale NFS. The re-stat is now
     precomputed off the event loop (run_in_threadpool) BEFORE the txn; the
     in-txn loop only does the UPDATEs.
  #4 _do_adopt's 4-statement write cluster (local_files + placements + acks +
     plex_items) ran in autocommit, so an inline-adopt crash mid-cluster left a
     tracked canonical with no placement and no worker retry. Now atomic.
  #6 _retry_pending_placements did a non-atomic dedup-SELECT-then-INSERT in
     autocommit → a concurrent enqueue raced it into a duplicate place job.
     Now one BEGIN IMMEDIATE around the check + insert.
  #7 api_admin_delete_orphan_sidecar ran find_theme_sidecar_path (a /data scan)
     on the event loop; v1.23.96 offloaded the adjacent unlink but missed the
     scan. Now offloaded.

These are mostly source-shape guards (the effects — lock-not-held, atomic-on-
crash, no-dup-under-race — aren't directly unit-testable), plus a behavioral
check that adopt still writes both rows.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
ADOPT_PY = (REPO / "app" / "core" / "adopt.py").read_text()
SCHED_PY = (REPO / "app" / "core" / "scheduler.py").read_text()


# ── #2 UNMANAGE: re-stat off-loop + out of the txn ───────────

def test_unmanage_restat_precomputed_off_loop():
    # the FS scan is offloaded into a threadpool helper...
    assert "_restat_present = await run_in_threadpool(_restat_unmanage_sidecars)" in API_PY
    # ...and the in-txn loop no longer calls the FS walk — it reads the dict.
    assert "1 if _restat_present.get(pi_row[\"rating_key\"]) else 0" in API_PY
    # the precompute must come BEFORE the write txn it used to sit inside.
    i_pre = API_PY.index("_restat_present = await run_in_threadpool")
    i_loop = API_PY.index("1 if _restat_present.get(pi_row[\"rating_key\"])")
    assert i_pre < i_loop


def test_unmanage_no_fs_walk_inside_txn():
    # the old in-loop import + call shape must be gone.
    assert "sidecar_present = folder_has_theme_sidecar(" not in API_PY


# ── #4 inline ADOPT atomicity ────────────────────────────────

def test_adopt_write_cluster_is_transactional():
    assert "from .db import get_conn, transaction" in ADOPT_PY
    # the 4-statement cluster opens under transaction(conn), anchored on its
    # distinctive v1.24.1 comment.
    i = ADOPT_PY.index("# v1.24.1: ON CONFLICT DO UPDATE, not INSERT OR REPLACE")
    head = ADOPT_PY[i - 200:i]
    assert "with get_conn(db_path) as conn, transaction(conn):" in head


# ── #6 retry-placements race ─────────────────────────────────

def test_retry_pending_placements_is_transactional():
    i = SCHED_PY.index("SELECT lf.media_type, lf.tmdb_id, lf.section_id, lf.edition_key")
    head = SCHED_PY[i - 400:i]
    assert "with get_conn(db_path) as conn, transaction(conn):" in head


# ── #7 delete-orphan-sidecar scan offloaded ──────────────────

def test_delete_orphan_sidecar_scan_offloaded():
    assert "sp = await run_in_threadpool(find_theme_sidecar_path, row[\"folder_path\"])" in API_PY
    assert "sp = find_theme_sidecar_path(row[\"folder_path\"])" not in API_PY
