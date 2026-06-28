"""v1.13.75 (schema v43) — backfill failure_kind from events log.

Pre-v1.13.74 a buggy unconditional clear in worker._record_local_file
+ REVERT + SET URL handlers wiped themes.failure_kind whenever any
download succeeded (even a user-URL or adopt success on a row whose
TDB URL was still demonstrably dead). The forward-fix landed in
v1.13.74; this migration repairs the backlog by reconstructing
failure_kind from the events log.

Algorithm pinned by these tests:

  - Restore failure_kind/message/at from the most recent
    download-WARNING event whose detail.youtube_url == themes.youtube_url
    (URL-match guard: if TDB updated the URL since the failure,
    skip — the old failure no longer applies).
  - Suppress restoration if any later success event landed for the
    SAME video_id (TDB-URL re-attempt actually worked since).
  - Skip rows where themes.youtube_url or youtube_video_id is NULL.
  - Idempotent: re-running on rows with failure_kind already set
    is a no-op.
  - On non-zero restoration count, write
    runtime_settings['v1_13_75_backfill_count'] for the dashboard
    banner.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from app.core.db import CURRENT_SCHEMA_VERSION, init_db


def _iso(dt: datetime) -> str:
    return dt.isoformat(timespec="seconds")


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _insert_theme(
    conn, *, tmdb_id: int, title: str = "Test",
    youtube_url: str | None = "https://www.youtube.com/watch?v=DEAD",
    youtube_video_id: str | None = "DEAD",
    failure_kind: str | None = None,
    failure_message: str | None = None,
    failure_at: str | None = None,
):
    now = _iso(_now())
    conn.execute(
        "INSERT INTO themes ("
        "  media_type, tmdb_id, title, upstream_source,"
        "  youtube_url, youtube_video_id,"
        "  last_seen_sync_at, first_seen_sync_at,"
        "  failure_kind, failure_message, failure_at"
        ") VALUES ('movie', ?, ?, 'imdb', ?, ?, ?, ?, ?, ?, ?)",
        (tmdb_id, title, youtube_url, youtube_video_id,
         now, now, failure_kind, failure_message, failure_at),
    )


def _insert_failure_event(conn, *, tmdb_id: int, ts: datetime,
                          youtube_url: str, kind: str = "video_removed",
                          raw: str | None = None):
    detail = json.dumps({
        "kind": kind,
        "youtube_url": youtube_url,
        "needs_manual_override": True,
        "raw": raw or f"ERROR: [youtube] foo: Video unavailable",
    })
    conn.execute(
        "INSERT INTO events (ts, level, component, media_type, tmdb_id, message, detail) "
        "VALUES (?, 'WARNING', 'download', 'movie', ?, ?, ?)",
        (_iso(ts), tmdb_id, "Download failed", detail),
    )


def _insert_success_event(conn, *, tmdb_id: int, ts: datetime,
                          video_id: str, section_id: str = "1"):
    detail = json.dumps({"size": 1234, "video_id": video_id,
                         "section_id": section_id})
    conn.execute(
        "INSERT INTO events (ts, level, component, media_type, tmdb_id, message, detail) "
        "VALUES (?, 'INFO', 'download', 'movie', ?, ?, ?)",
        (_iso(ts), tmdb_id, "Downloaded theme for foo", detail),
    )


def _failure_state(db: Path, tmdb_id: int):
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT failure_kind, failure_message, failure_at "
            "FROM themes WHERE tmdb_id = ?",
            (tmdb_id,),
        ).fetchone()
    return row


@pytest.fixture
def db(tmp_path: Path) -> Path:
    p = tmp_path / "motif.db"
    init_db(p)
    return p


# ── schema version bumped ──────────────────────────────────────

def test_schema_at_v43(db):
    with sqlite3.connect(db) as conn:
        v = conn.execute(
            "SELECT MAX(version) FROM schema_version",
        ).fetchone()[0]
    assert v == CURRENT_SCHEMA_VERSION
    assert CURRENT_SCHEMA_VERSION >= 43


# ── core happy path ────────────────────────────────────────────

def _run_v43(db: Path):
    """Re-invoke the migration on a fresh-init DB. init_db already
    ran v43, so we manually clear failure_kind first to simulate
    the pre-fix wipe state, then call _migrate_v42_to_v43 directly."""
    from app.core.db import _migrate_v42_to_v43
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        _migrate_v42_to_v43(conn)
        conn.commit()


def test_restores_failure_when_url_matches_and_no_later_success(db):
    """The reported scenario: failure event exists for the row's
    current TDB URL, no later success ever landed → restore."""
    failure_ts = _now() - timedelta(hours=2)
    with sqlite3.connect(db) as conn:
        _insert_theme(conn, tmdb_id=1)
        _insert_failure_event(
            conn, tmdb_id=1, ts=failure_ts,
            youtube_url="https://www.youtube.com/watch?v=DEAD",
            kind="video_removed",
            raw="ERROR: video gone",
        )
    _run_v43(db)
    state = _failure_state(db, 1)
    assert state["failure_kind"] == "video_removed"
    assert state["failure_message"] == "ERROR: video gone"
    assert state["failure_at"] == _iso(failure_ts)


def test_skips_when_url_no_longer_matches(db):
    """TDB updated the URL since the failure — old failure no longer
    applies, leave row alone."""
    with sqlite3.connect(db) as conn:
        _insert_theme(conn, tmdb_id=1,
                      youtube_url="https://www.youtube.com/watch?v=NEW",
                      youtube_video_id="NEW")
        # Failure logged against the OLD URL (now superseded by TDB update).
        _insert_failure_event(
            conn, tmdb_id=1, ts=_now() - timedelta(hours=2),
            youtube_url="https://www.youtube.com/watch?v=OLD",
        )
    _run_v43(db)
    assert _failure_state(db, 1)["failure_kind"] is None


def test_suppresses_when_later_tdb_success_exists(db):
    """A successful TDB-URL download landed AFTER the failure — TDB
    works again, leave failure_kind cleared."""
    base = _now()
    with sqlite3.connect(db) as conn:
        _insert_theme(conn, tmdb_id=1)
        _insert_failure_event(
            conn, tmdb_id=1, ts=base - timedelta(hours=3),
            youtube_url="https://www.youtube.com/watch?v=DEAD",
        )
        _insert_success_event(
            conn, tmdb_id=1, ts=base - timedelta(hours=1),
            video_id="DEAD",  # matches themes.youtube_video_id
        )
    _run_v43(db)
    assert _failure_state(db, 1)["failure_kind"] is None


def test_does_not_suppress_when_later_user_url_success(db):
    """The exact bug scenario: failure for TDB URL, later success
    for a user URL (different video_id). TDB is still broken;
    user-URL success doesn't tell us anything about TDB. Restore."""
    base = _now()
    with sqlite3.connect(db) as conn:
        _insert_theme(conn, tmdb_id=1,
                      youtube_url="https://www.youtube.com/watch?v=DEAD",
                      youtube_video_id="DEAD")
        _insert_failure_event(
            conn, tmdb_id=1, ts=base - timedelta(hours=3),
            youtube_url="https://www.youtube.com/watch?v=DEAD",
        )
        # Success has a DIFFERENT video_id — user URL.
        _insert_success_event(
            conn, tmdb_id=1, ts=base - timedelta(hours=1),
            video_id="USER_URL_VID",
        )
    _run_v43(db)
    assert _failure_state(db, 1)["failure_kind"] == "video_removed"


def test_restores_only_when_failure_kind_is_null(db):
    """Idempotency: rows that already have a failure_kind set are
    not touched. Re-running migration is safe."""
    with sqlite3.connect(db) as conn:
        _insert_theme(conn, tmdb_id=1, failure_kind="cookies_expired",
                      failure_message="orig msg",
                      failure_at=_iso(_now() - timedelta(days=10)))
        # Newer failure event in the log for a DIFFERENT kind — must
        # not overwrite the existing failure_kind on the row.
        _insert_failure_event(
            conn, tmdb_id=1, ts=_now() - timedelta(hours=1),
            youtube_url="https://www.youtube.com/watch?v=DEAD",
            kind="video_removed",
        )
    _run_v43(db)
    state = _failure_state(db, 1)
    assert state["failure_kind"] == "cookies_expired"
    assert state["failure_message"] == "orig msg"


def test_skips_when_youtube_url_is_null(db):
    """Orphan-style row with no TDB URL — nothing to attribute the
    failure to. Skip."""
    with sqlite3.connect(db) as conn:
        _insert_theme(conn, tmdb_id=1, youtube_url=None,
                      youtube_video_id=None)
        _insert_failure_event(
            conn, tmdb_id=1, ts=_now() - timedelta(hours=1),
            youtube_url="https://www.youtube.com/watch?v=DEAD",
        )
    _run_v43(db)
    assert _failure_state(db, 1)["failure_kind"] is None


def test_skips_when_youtube_video_id_is_null(db):
    """Without youtube_video_id we can't run the success-suppression
    check. Skip rather than risk over-restoration."""
    with sqlite3.connect(db) as conn:
        _insert_theme(conn, tmdb_id=1,
                      youtube_url="https://www.youtube.com/watch?v=DEAD",
                      youtube_video_id=None)
        _insert_failure_event(
            conn, tmdb_id=1, ts=_now() - timedelta(hours=1),
            youtube_url="https://www.youtube.com/watch?v=DEAD",
        )
    _run_v43(db)
    assert _failure_state(db, 1)["failure_kind"] is None


def test_picks_most_recent_failure_when_multiple_exist(db):
    """Multiple failures over time — restore from the LATEST one."""
    base = _now()
    with sqlite3.connect(db) as conn:
        _insert_theme(conn, tmdb_id=1)
        _insert_failure_event(
            conn, tmdb_id=1, ts=base - timedelta(days=5),
            youtube_url="https://www.youtube.com/watch?v=DEAD",
            kind="cookies_expired", raw="old failure",
        )
        _insert_failure_event(
            conn, tmdb_id=1, ts=base - timedelta(hours=1),
            youtube_url="https://www.youtube.com/watch?v=DEAD",
            kind="video_removed", raw="newer failure",
        )
    _run_v43(db)
    state = _failure_state(db, 1)
    assert state["failure_kind"] == "video_removed"
    assert state["failure_message"] == "newer failure"


# ── banner marker ──────────────────────────────────────────────

def test_writes_runtime_settings_banner_marker_on_restoration(db):
    """When N>0 rows are restored, write
    runtime_settings['v1_13_75_backfill_count'] = N so the dashboard
    banner can surface the count."""
    with sqlite3.connect(db) as conn:
        _insert_theme(conn, tmdb_id=1)
        _insert_theme(conn, tmdb_id=2)
        _insert_failure_event(
            conn, tmdb_id=1, ts=_now() - timedelta(hours=1),
            youtube_url="https://www.youtube.com/watch?v=DEAD",
        )
        _insert_failure_event(
            conn, tmdb_id=2, ts=_now() - timedelta(hours=1),
            youtube_url="https://www.youtube.com/watch?v=DEAD",
        )
    _run_v43(db)
    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT value FROM runtime_settings "
            "WHERE key = 'v1_13_75_backfill_count'",
        ).fetchone()
    assert row is not None
    assert int(row[0]) == 2


def test_no_banner_marker_when_no_rows_restored(db):
    """Empty DB / no qualifying failures → no banner marker, so the
    user doesn't see a stale banner on an upgrade that didn't need
    the backfill."""
    _run_v43(db)
    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT value FROM runtime_settings "
            "WHERE key = 'v1_13_75_backfill_count'",
        ).fetchone()
    assert row is None


# ── idempotency ────────────────────────────────────────────────

def test_idempotent_second_run_is_noop(db):
    """Re-running the migration on a DB that already had it applied
    must not double-apply. Critical because init_db re-runs through
    the migration chain on every startup."""
    base = _now()
    with sqlite3.connect(db) as conn:
        _insert_theme(conn, tmdb_id=1)
        _insert_failure_event(
            conn, tmdb_id=1, ts=base - timedelta(hours=2),
            youtube_url="https://www.youtube.com/watch?v=DEAD",
            kind="video_removed", raw="orig",
        )
    _run_v43(db)
    state_after_first = _failure_state(db, 1)
    _run_v43(db)
    state_after_second = _failure_state(db, 1)
    assert tuple(state_after_first) == tuple(state_after_second)
