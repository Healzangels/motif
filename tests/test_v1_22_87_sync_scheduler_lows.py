"""v1.22.87 (audit round 2, Batch D #4) — sync/scheduler LOWs.

(1) Orphan promotion re-keyed all five (media_type, tmdb_id) child
tables but NOT in-flight jobs — a pending download/place enqueued at
the synthetic negative id (SET URL on an unmatched item) died
confusingly when the daily sync promoted the id mid-window: the
worker resolved the override/themes URL at the OLD id (already
re-keyed away → empty) and the user's requested download failed as a
mystery job error. Promotion now re-keys pending/running jobs too.

(2) stats.collections_seen (added v1.18.0 "so the dashboard sparkline
can show collection sync activity") was never persisted — sync_runs
had no column and both finishing UPDATEs dropped it, so a
git-differential run touching only movie_collections/ paths recorded
an empty-looking run that did real work. Schema v65 adds the column;
both UPDATEs write it.

(3) The hourly placement retry sweep's in-flight-job dedup was
edition-blind (inconsistent with its own edition-aware placements
JOIN and the v1.21.82 per-edition enqueue dedup) — edition A's
pending/stuck place job deferred edition B's retry sweep-over-sweep.
Both sweep queries now match the payload edition.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from app.core.db import CURRENT_SCHEMA_VERSION, init_db

REPO = Path(__file__).resolve().parent.parent
SYNC_PY = (REPO / "app" / "core" / "sync.py").read_text()
SCHED_PY = (REPO / "app" / "core" / "scheduler.py").read_text()


# ── (1) promotion re-keys jobs ───────────────────────────────


def test_promotion_rekeys_inflight_jobs():
    i = SYNC_PY.index("v1.22.87: re-key in-flight jobs too")
    block = SYNC_PY[i:i + 800]
    assert ("UPDATE jobs SET tmdb_id = ? WHERE media_type = ? "
            in block)
    assert "status IN ('pending', 'running')" in block


# ── (2) collections_seen persisted ───────────────────────────


def test_schema_v65_adds_collections_seen(tmp_path):
    # v1.23.25: >= 65 so this stops drifting on every future schema bump —
    # the test's real subject is that the v65 migration added collections_seen.
    assert CURRENT_SCHEMA_VERSION >= 65
    db = tmp_path / "m.db"
    init_db(db)
    with sqlite3.connect(db) as conn:
        cols = {r[1] for r in conn.execute(
            "PRAGMA table_info(sync_runs)")}
    assert "collections_seen" in cols


def test_both_sync_runs_updates_write_collections_seen():
    assert SYNC_PY.count("collections_seen = ?") == 2, (
        "v1.22.87: the success AND failure UPDATEs must persist the "
        "counter"
    )
    assert SYNC_PY.count("stats.collections_seen,") >= 2


def test_v65_migration_is_idempotent(tmp_path):
    from app.core.db import _migrate_v64_to_v65
    db = tmp_path / "m.db"
    init_db(db)
    with sqlite3.connect(db) as conn:
        _migrate_v64_to_v65(conn)  # second run must not raise
        cols = [r[1] for r in conn.execute(
            "PRAGMA table_info(sync_runs)")]
    assert cols.count("collections_seen") == 1


# ── (3) retry sweep per-edition dedup ────────────────────────


def test_retry_sweep_job_dedup_is_edition_aware():
    # v1.24.29: 4 occurrences now — the two retry-sweep queries (main +
    # locked-rows summary) PLUS _restore_lost_placements' TWO branches (sidecar
    # + the v1.24.29 plex_upload re-push), all sharing the v1.21.82 per-edition
    # payload pattern so one edition's pending place job never suppresses
    # another's re-place.
    assert SCHED_PY.count(
        "json_extract(j.payload, '$.edition_key')") == 4, (
        "v1.22.87/v1.24.29: every scheduler jobs-dedup must match the "
        "payload edition"
    )
