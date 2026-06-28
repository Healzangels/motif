"""v1.21.44 — review fixes on the v1.21.38/H1 + v1.21.41/M1 audit fixes.

F1 (sync.py, was H1): the drop sweep skipped on ANY stats.errors > 0. A
single chronically-erroring per-item fetch (a perma-malformed JSON, or an
item left in pages.json whose JSON 404s) pinned stats.errors >= 1 EVERY
run → drop detection permanently disabled. Fix: gate only on an INCOMPLETE
INDEX (a swallowed page = unenumerable missing items); for PER-ITEM fetch
errors, EXCLUDE just those ids from stamping so the sweep still runs.

F2 (scheduler.py, was M1): placement_error jobs are now marked failed
(FAIL dot) but the hourly retry sweep still re-enqueued them → a fresh
unacked failed job every hour (FAIL dot re-lights after ack, unbounded
pile). Fix: add placement_error:% to the sweep skip-list (the user's call:
surface once + stick; no hourly auto-retry churn).
"""
from __future__ import annotations

from pathlib import Path

import pytest

from app.core.db import get_conn, init_db


@pytest.fixture
def db_path(tmp_path):
    db = tmp_path / "m.db"
    init_db(db)
    return db


def _seed_theme(db, *, tmdb_id, last_seen, mt="movie"):
    with get_conn(db) as conn:
        conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, upstream_source, "
            "  last_seen_sync_at, first_seen_sync_at) "
            "VALUES (?, ?, ?, 'themoviedb', ?, ?)",
            (mt, tmdb_id, f"T{tmdb_id}", last_seen, last_seen))


def _drop(db, tmdb_id, mt="movie"):
    with get_conn(db) as conn:
        r = conn.execute(
            "SELECT tdb_dropped_at FROM themes WHERE media_type=? AND tmdb_id=?",
            (mt, tmdb_id)).fetchone()
    return r["tdb_dropped_at"] if r else "MISSING"


# ── F1 unit: the sweep excludes the given ids ────────────────


def test_sweep_excludes_errored_ids(db_path):
    from app.core.sync import _detect_and_stamp_drops_full_walk
    _seed_theme(db_path, tmdb_id=100, last_seen="2020-01-01T00:00:00Z")
    _seed_theme(db_path, tmdb_id=200, last_seen="2020-01-01T00:00:00Z")
    n = _detect_and_stamp_drops_full_walk(
        db_path, sync_ts="2026-05-15T12:00:00Z", media_types_seen={"movie"},
        exclude_by_mt={"movie": {100}})
    assert n == 1
    assert _drop(db_path, 100) is None, "excluded id must NOT be stamped"
    assert _drop(db_path, 200) == "2026-05-15T12:00:00Z", "the other IS stamped"


# ── F1 behavioral: a per-item error doesn't disable the sweep ──


def test_per_item_error_excludes_id_but_sweep_still_runs(db_path, monkeypatch):
    """The exact v1.21.38 starvation scenario: item 100 is in the index but
    its fetch errors EVERY run. Pre-fix that skipped the whole sweep (drops
    permanently off). Post-fix: 100 is excluded (not mis-stamped) but the
    sweep STILL RUNS and stamps the genuinely-removed item 200."""
    import app.core.sync as sync
    _seed_theme(db_path, tmdb_id=100, last_seen="2020-01-01T00:00:00Z")  # errors
    _seed_theme(db_path, tmdb_id=200, last_seen="2020-01-01T00:00:00Z")  # real drop

    def fake_index(client, base_url, media_path):
        if media_path == "movies":
            return ([{"id": 100, "imdb_id": None}], 0)  # 100 in index, clean
        return ([], 0)

    monkeypatch.setattr(sync, "_fetch_index", fake_index)
    # Item 100's per-item fetch fails → _do_fetch returns ('error', 100).
    monkeypatch.setattr(sync, "_fetch_item", lambda *a, **k: None)
    sync.run_sync(db_path, "http://fake.invalid", source="remote",
                  enqueue_downloads=False)
    assert _drop(db_path, 100) is None, (
        "the errored (in-index, unfetched) item must NOT be mis-stamped")
    assert _drop(db_path, 200) is not None, (
        "a genuinely-removed item IS still stamped — the sweep is NOT "
        "disabled by a per-item error (the v1.21.38 starvation fix)")


# ── F2: the retry sweep skips placement_error rows ───────────


def _seed_unplaced_local(db, *, tmdb_id, reason):
    # local_files FKs to themes(media_type, tmdb_id) + plex_sections(section_id).
    _seed_theme(db, tmdb_id=tmdb_id, last_seen="2026-01-01T00:00:00Z")
    with get_conn(db) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO plex_sections (section_id, title, type, "
            "  is_anime, is_4k, themes_subdir, included, discovered_at, "
            "  last_seen_at) "
            "VALUES ('1','Movies','movie',0,0,'movies',1,'2026-01-01',"
            "        '2026-01-01')")
        conn.execute(
            "INSERT INTO local_files (media_type, tmdb_id, section_id, "
            "  file_path, file_sha256, file_size, downloaded_at, "
            "  source_video_id, provenance, source_kind, "
            "  last_place_attempt_at, last_place_attempt_reason) "
            "VALUES ('movie', ?, '1', 'movies/x/theme.mp3', ?, 10, "
            "        '2026-01-01', 'vid', 'auto', 'themerrdb', "
            "        '2026-01-01', ?)",
            (tmdb_id, f"sha{tmdb_id}", reason))


def _place_jobs(db, tmdb_id):
    with get_conn(db) as conn:
        return conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE job_type='place' AND tmdb_id=?",
            (tmdb_id,)).fetchone()[0]


def test_retry_sweep_skips_placement_error(db_path):
    from app.core.scheduler import _retry_pending_placements
    _seed_unplaced_local(db_path, tmdb_id=500,
                         reason="placement_error:disk full")
    _retry_pending_placements(db_path)
    assert _place_jobs(db_path, 500) == 0, (
        "v1.21.44: a placement_error row must NOT be re-enqueued by the "
        "hourly sweep (it's a marked-failed job + FAIL dot, no churn)")


def test_retry_sweep_still_retries_a_non_skipped_reason(db_path):
    """Discriminator: a non-skip-listed reason (no_match) IS re-enqueued —
    proves the sweep isn't trivially enqueueing nothing."""
    from app.core.scheduler import _retry_pending_placements
    _seed_unplaced_local(db_path, tmdb_id=501, reason="no_match")
    _retry_pending_placements(db_path)
    assert _place_jobs(db_path, 501) == 1, (
        "a no_match row must still be re-enqueued (sweep works)")


def test_scheduler_skiplist_includes_placement_error():
    sched = (Path(__file__).resolve().parent.parent
             / "app" / "core" / "scheduler.py").read_text()
    assert "NOT LIKE 'placement_error:%'" in sched
