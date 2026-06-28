"""v1.23.23 — sync auto-downloads are held until the summary dispatches.

the user's 2026-06-13 report: the per-theme "Theme added" cards (Bad Guys 2 /
Chainsaw Man / Cosmic Princess) arrived in Discord BEFORE the "Motif sync — 7
new" summary that explained them. Cause: run_sync enqueues each newly-found
theme's download inline (sync.py), the worker's download+place threads run them
in parallel, and a fast download fires theme_added before sync_completed (which
only dispatches after run_sync returns).

Fix (the user chose "keep the cards, but after the summary"): a sync-discovered
auto-download is parked at the _SYNC_HOLD_NEXT_RUN_AT sentinel — a far-future
next_run_at the worker's claim query (status='pending' AND next_run_at <= now)
skips — until _do_sync releases it via release_sync_held_downloads, right AFTER
dispatching the summary. The download pipeline's own latency then keeps
theme_added a step behind the already-dispatched summary. A hold orphaned by a
worker that died mid-sync is swept at boot by _reclaim_orphan_jobs (and by the
next sync's release).
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from app.core.db import init_db
from app.core.events import now_iso
from app.core.sync import (
    _enqueue_download,
    _SYNC_HOLD_NEXT_RUN_AT,
    release_sync_held_downloads,
)
from app.core.worker import _reclaim_orphan_jobs


REPO = Path(__file__).resolve().parent.parent
WORKER_PY = (REPO / "app" / "core" / "worker.py").read_text()


def _conn(db: Path) -> sqlite3.Connection:
    c = sqlite3.connect(db)
    c.row_factory = sqlite3.Row
    return c


def _seed_movie_row(db: Path, tmdb_id: int = 700) -> None:
    now = now_iso()
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, included, "
            "  is_anime, is_4k, themes_subdir, discovered_at, last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 1, 0, 0, 'movies', ?, ?)",
            (now, now))
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type, title, "
            "  title_norm, guid_tmdb, has_theme, first_seen_at, last_seen_at) "
            "VALUES ('rk1', '1', 'movie', 'Bad Guys 2', 'bad guys 2', ?, 0, ?, ?)",
            (tmdb_id, now, now))
        conn.commit()


def _job_rows(db: Path) -> list[sqlite3.Row]:
    with _conn(db) as conn:
        return conn.execute(
            "SELECT id, status, next_run_at FROM jobs WHERE job_type='download'"
        ).fetchall()


def test_hold_for_summary_parks_at_sentinel(tmp_path):
    db = tmp_path / "m.db"
    init_db(db)
    _seed_movie_row(db)
    with _conn(db) as conn:
        n = _enqueue_download(
            conn, media_type="movie", tmdb_id=700, reason="new",
            hold_for_summary=True)
        conn.commit()
    assert n == 1
    rows = _job_rows(db)
    assert len(rows) == 1
    assert rows[0]["next_run_at"] == _SYNC_HOLD_NEXT_RUN_AT
    assert rows[0]["status"] == "pending"


def test_default_enqueue_is_runnable_now(tmp_path):
    # the non-sync (manual / default) path must NOT hold — it stays runnable.
    db = tmp_path / "m.db"
    init_db(db)
    _seed_movie_row(db)
    with _conn(db) as conn:
        _enqueue_download(conn, media_type="movie", tmdb_id=700, reason="new")
        conn.commit()
    rows = _job_rows(db)
    assert rows[0]["next_run_at"] != _SYNC_HOLD_NEXT_RUN_AT
    assert rows[0]["next_run_at"] <= now_iso()


def test_claim_query_skips_a_held_job(tmp_path):
    # the worker's claim predicate (status='pending' AND next_run_at <= now)
    # must NOT return a sentinel-parked job — that's the whole hold mechanism.
    db = tmp_path / "m.db"
    init_db(db)
    _seed_movie_row(db)
    with _conn(db) as conn:
        _enqueue_download(
            conn, media_type="movie", tmdb_id=700, reason="new",
            hold_for_summary=True)
        conn.commit()
    with _conn(db) as conn:
        claimable = conn.execute(
            "SELECT id FROM jobs WHERE status='pending' "
            "  AND (next_run_at IS NULL OR next_run_at <= ?)",
            (now_iso(),),
        ).fetchall()
    assert claimable == []


def test_release_makes_held_runnable_and_is_idempotent(tmp_path):
    db = tmp_path / "m.db"
    init_db(db)
    _seed_movie_row(db)
    with _conn(db) as conn:
        _enqueue_download(
            conn, media_type="movie", tmdb_id=700, reason="new",
            hold_for_summary=True)
        conn.commit()
    released = release_sync_held_downloads(db)
    assert released == 1
    rows = _job_rows(db)
    assert rows[0]["next_run_at"] != _SYNC_HOLD_NEXT_RUN_AT
    assert rows[0]["next_run_at"] <= now_iso()
    # now claimable
    with _conn(db) as conn:
        claimable = conn.execute(
            "SELECT id FROM jobs WHERE status='pending' "
            "  AND (next_run_at IS NULL OR next_run_at <= ?)",
            (now_iso(),),
        ).fetchall()
    assert len(claimable) == 1
    # idempotent — second release finds nothing held.
    assert release_sync_held_downloads(db) == 0


def test_boot_reclaim_releases_orphaned_holds(tmp_path):
    # a worker that died mid-sync left a hold parked forever; _reclaim_orphan_jobs
    # (one-time, pre-spawn) must release it so the theme isn't stranded.
    db = tmp_path / "m.db"
    init_db(db)
    _seed_movie_row(db)
    with _conn(db) as conn:
        _enqueue_download(
            conn, media_type="movie", tmdb_id=700, reason="new",
            hold_for_summary=True)
        conn.commit()
    _reclaim_orphan_jobs(db)
    rows = _job_rows(db)
    assert rows[0]["next_run_at"] != _SYNC_HOLD_NEXT_RUN_AT
    assert rows[0]["next_run_at"] <= now_iso()


def test_release_happens_after_summary_dispatch_in_worker():
    # ordering intent: the post-summary release must come AFTER the
    # sync_completed dispatch in _do_sync, never before — else the cards could
    # still race the summary.
    summary_idx = WORKER_PY.index('event_kind="sync_completed"')
    # the post-summary release call (the one assigned to `_released`).
    post = WORKER_PY.index("_released = release_sync_held_downloads")
    assert post > summary_idx, (
        "the post-summary release must follow the sync_completed dispatch"
    )
