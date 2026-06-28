"""v1.13.76 — retry sweep skips permanent placement-skip reasons.

The hourly _retry_pending_placements scheduler used to re-enqueue
EVERY local_files row that lacked a matching placements.media_folder,
forever, regardless of why the previous place attempt failed. For
"permanent" reasons (plex_has_theme, existing_theme:*) the worker
would silently DEBUG-log the skip and the user saw nothing but the
hourly sweep noise: "Retry sweep enqueued 1 placement jobs" →
"Indexed /data/media/movies" → (no Placed in line) → repeat.

The fix scopes the sweep to rows whose last attempt either:
  - never happened (last_place_attempt_reason IS NULL), or
  - failed for a transient reason (no_match — Radarr import lag).

v1.21.44: placement_error:* is now SKIPPED too (not retried) — a real
hardlink/copy I/O failure is marked job=failed + lights the FAIL dot,
so an hourly re-sweep would churn fresh failed jobs. (And backup_only,
v1.19.21, is skipped — user intent is "keep the backup, don't place".)

User-driven actions (PURGE, REPLACE TDB, etc.) re-enqueue via the
API directly so they're unaffected by this filter.

These tests pin the filter SQL by exercising it against a fresh DB
seeded with rows in each skip-reason category.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.core.db import init_db


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@pytest.fixture
def db(tmp_path: Path) -> Path:
    p = tmp_path / "motif.db"
    init_db(p)
    return p


def _insert_row(conn, *, tmdb_id: int, last_reason: str | None,
                title: str = "x"):
    """Theme + local_files row with no matching placement (downloaded
    but unplaced — the state the retry sweep targets)."""
    now = _now_iso()
    conn.execute(
        "INSERT INTO themes ("
        "  media_type, tmdb_id, title, upstream_source,"
        "  last_seen_sync_at, first_seen_sync_at"
        ") VALUES ('movie', ?, ?, 'imdb', ?, ?)",
        (tmdb_id, title, now, now),
    )
    conn.execute(
        "INSERT INTO local_files ("
        "  media_type, tmdb_id, section_id, file_path, source_video_id,"
        "  downloaded_at, last_place_attempt_at, last_place_attempt_reason"
        ") VALUES ('movie', ?, '1', 'x.mp3', 'vid', ?, ?, ?)",
        (tmdb_id, now,
         now if last_reason else None, last_reason),
    )


def _eligible_tmdb_ids(db: Path) -> set[int]:
    """Run the retry-sweep query directly and return the tmdb_ids it
    would re-enqueue. Mirrors the scheduler's SQL exactly so a future
    edit to the query has to update this helper too — that's the
    point: tests pin the SQL shape. v1.21.45: re-synced after the mirror
    drifted from production (missing the v1.19.21 'backup_only' skip AND
    the v1.21.44 'placement_error:%' skip). The behavioral guard against
    future drift is test_v1_21_44's call to the REAL function."""
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT lf.tmdb_id
            FROM local_files lf
            LEFT JOIN placements p
              ON p.media_type = lf.media_type
             AND p.tmdb_id = lf.tmdb_id
             AND p.section_id = lf.section_id
            WHERE p.media_folder IS NULL
              AND NOT EXISTS (
                  SELECT 1 FROM jobs j
                  WHERE j.job_type = 'place' AND j.media_type = lf.media_type
                    AND j.tmdb_id = lf.tmdb_id
                    AND j.section_id = lf.section_id
                    AND j.status IN ('pending', 'running')
              )
              AND (lf.last_place_attempt_reason IS NULL
                   OR (lf.last_place_attempt_reason != 'plex_has_theme'
                       AND lf.last_place_attempt_reason != 'backup_only'
                       AND lf.last_place_attempt_reason NOT LIKE 'existing_theme:%'
                       AND lf.last_place_attempt_reason NOT LIKE 'placement_error:%'))
              -- v1.18.94 widening — see test_v1_18_94_plex_rejection_skip.py.
              AND NOT (
                  lf.last_place_attempt_reason LIKE 'plex_rejected:%'
                  AND (
                      SELECT COUNT(*) FROM jobs j2
                      WHERE j2.job_type = 'place'
                        AND j2.media_type = lf.media_type
                        AND j2.tmdb_id = lf.tmdb_id
                        AND j2.section_id = lf.section_id
                        AND j2.status = 'failed'
                        AND j2.finished_at IS NOT NULL
                        -- v1.19.5: julianday() to match scheduler.py.
                        AND julianday(j2.finished_at)
                            > julianday('now', '-24 hours')
                  ) >= 2
              )
            """
        ).fetchall()
    return {r["tmdb_id"] for r in rows}


def test_eligible_when_never_attempted(db):
    """Brand new download (Radarr just imported the row) — sweep
    should pick it up."""
    with sqlite3.connect(db) as conn:
        _insert_row(conn, tmdb_id=1, last_reason=None)
    assert _eligible_tmdb_ids(db) == {1}


def test_eligible_when_last_reason_was_no_match(db):
    """no_match = Plex folder missing. Radarr might still import.
    Keep retrying."""
    with sqlite3.connect(db) as conn:
        _insert_row(conn, tmdb_id=1, last_reason="no_match")
    assert _eligible_tmdb_ids(db) == {1}


def test_skipped_when_placement_error(db):
    """v1.21.44: a real hardlink/copy I/O failure (disk full / EPERM /
    cross-FS) is now marked job=failed (FAIL dot) and SKIPPED by the
    sweep — no hourly auto-retry churn. Was eligible pre-v1.21.44."""
    with sqlite3.connect(db) as conn:
        _insert_row(conn, tmdb_id=1,
                    last_reason="placement_error:[Errno 13] Permission denied")
    assert _eligible_tmdb_ids(db) == set()


def test_skipped_when_plex_has_theme(db):
    """The actual bug: Plex serves its own theme; motif won't
    overwrite without explicit force. Re-enqueueing every hour
    is pointless — user must PURGE / REPLACE to trigger a real
    new attempt, and that path goes through the API directly,
    not the sweep."""
    with sqlite3.connect(db) as conn:
        _insert_row(conn, tmdb_id=1, last_reason="plex_has_theme")
    assert _eligible_tmdb_ids(db) == set()


def test_skipped_when_existing_theme(db):
    """A sidecar already in the Plex folder. Same as plex_has_theme
    semantically — won't change without user action."""
    with sqlite3.connect(db) as conn:
        _insert_row(conn, tmdb_id=1, last_reason="existing_theme:theme.mp3")
    assert _eligible_tmdb_ids(db) == set()


def test_skipped_when_existing_theme_with_arbitrary_filename(db):
    """existing_theme: prefix carries the filename suffix; the LIKE
    pattern matches any suffix."""
    with sqlite3.connect(db) as conn:
        _insert_row(conn, tmdb_id=1, last_reason="existing_theme:custom.m4a")
    assert _eligible_tmdb_ids(db) == set()


def test_mixed_population_partitions_correctly(db):
    """Multiple rows with different reasons — only transient and
    never-attempted ones come through."""
    with sqlite3.connect(db) as conn:
        _insert_row(conn, tmdb_id=1, last_reason=None)
        _insert_row(conn, tmdb_id=2, last_reason="no_match")
        _insert_row(conn, tmdb_id=3, last_reason="plex_has_theme")
        _insert_row(conn, tmdb_id=4, last_reason="existing_theme:foo.mp3")
        # v1.21.44: placement_error is now SKIPPED (was eligible).
        _insert_row(conn, tmdb_id=5, last_reason="placement_error:disk full")
        _insert_row(conn, tmdb_id=6, last_reason="backup_only")  # v1.19.21 skip
    assert _eligible_tmdb_ids(db) == {1, 2}


def test_skipped_when_placement_already_exists(db):
    """Independent guard: rows with a placements row don't appear
    regardless of last_place_attempt_reason. Pin alongside the new
    filter so they don't clobber each other."""
    now = _now_iso()
    with sqlite3.connect(db) as conn:
        _insert_row(conn, tmdb_id=1, last_reason=None)
        # Create a matching placement.
        conn.execute(
            "INSERT INTO placements ("
            "  media_type, tmdb_id, section_id, media_folder,"
            "  placed_at, placement_kind, plex_refreshed"
            ") VALUES ('movie', 1, '1', '/data/media/movies/x', ?, 'hardlink', 0)",
            (now,),
        )
    assert _eligible_tmdb_ids(db) == set()


def test_skipped_when_pending_place_job_already_queued(db):
    """Independent guard: rows with a pending or running place job
    don't get a duplicate enqueue."""
    now = _now_iso()
    with sqlite3.connect(db) as conn:
        _insert_row(conn, tmdb_id=1, last_reason=None)
        conn.execute(
            "INSERT INTO jobs ("
            "  job_type, media_type, tmdb_id, section_id,"
            "  payload, status, created_at, next_run_at"
            ") VALUES ('place', 'movie', 1, '1', '{}', 'pending', ?, ?)",
            (now, now),
        )
    assert _eligible_tmdb_ids(db) == set()


# ── log-level promotion guard ─────────────────────────────────

def test_place_outcome_log_level_is_unconditional_info():
    """v1.13.76 promoted the place outcome log from DEBUG (when
    skip) to INFO (always). Pre-fix the silent-skip pattern hid the
    reason from default Docker logs. Static guard against a
    regression that re-introduces the conditional level."""
    worker_py = Path(__file__).resolve().parent.parent / "app" / "core" / "worker.py"
    src = worker_py.read_text()
    # The conditional pattern that the fix removed.
    assert 'level="INFO" if outcome.placed else "DEBUG"' not in src, (
        "v1.13.76 dropped the conditional log level — re-adding it "
        "would resurrect the silent-skip pattern"
    )
    # The unconditional INFO line that replaced it (anchored by
    # the surrounding component='place' + Skipped placement: text).
    assert "Skipped placement:" in src
