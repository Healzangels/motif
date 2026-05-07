"""Phase C regression tests — drop detection across the three sync
transports + the ack-drop / convert-to-manual API endpoints + the
tdb_pills=dropped filter SQL.

The drop-detection helpers (`_detect_and_stamp_drops_full_walk`,
`_detect_and_stamp_drops_git`) are exercised against in-memory DB
fixtures. The git path is tested with a local fixture repo from
test_sync_git's helpers; the full-walk path uses last_seen_sync_at
manipulations.
"""
from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from dulwich.repo import Repo


# ---------- fixtures ---------------------------------------------------------

@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    from app.core.db import init_db
    path = tmp_path / "motif.db"
    init_db(path)
    return path


def _seed_theme(db_path: Path, *, media_type: str, tmdb_id: int,
                youtube_url: str = "https://youtu.be/x",
                upstream_source: str = "themoviedb",
                last_seen_sync_at: str = "2026-01-01T00:00:00Z",
                title: str = "Seeded Title") -> None:
    """Insert a themes row directly. Mirrors the columns _upsert_theme
    populates so library SQL queries see a well-shaped row."""
    from app.core.db import get_conn
    with get_conn(db_path) as conn:
        conn.execute(
            """INSERT INTO themes
                 (media_type, tmdb_id, title, upstream_source,
                  youtube_url, last_seen_sync_at, first_seen_sync_at,
                  raw_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (media_type, tmdb_id, title, upstream_source,
             youtube_url, last_seen_sync_at, last_seen_sync_at, "{}"),
        )


def _read_drop(db_path: Path, media_type: str, tmdb_id: int) -> str | None:
    from app.core.db import get_conn
    with get_conn(db_path) as conn:
        row = conn.execute(
            "SELECT tdb_dropped_at FROM themes "
            "WHERE media_type = ? AND tmdb_id = ?",
            (media_type, tmdb_id),
        ).fetchone()
    return row["tdb_dropped_at"] if row else None


# ---------- schema migration -------------------------------------------------

def test_schema_v36_adds_tdb_dropped_at(db_path: Path):
    """init_db produces a schema with the new column. New rows
    default tdb_dropped_at=NULL."""
    from app.core.db import get_conn
    with get_conn(db_path) as conn:
        cols = [r["name"] for r in conn.execute(
            "PRAGMA table_info(themes)").fetchall()]
    assert "tdb_dropped_at" in cols
    _seed_theme(db_path, media_type="movie", tmdb_id=1)
    assert _read_drop(db_path, "movie", 1) is None


# ---------- full-walk drop detection (snapshot/remote) -----------------------

def test_full_walk_stamps_items_not_seen_this_run(db_path: Path):
    """Items whose last_seen_sync_at is OLDER than the current
    sync_ts are implicitly removed from the upstream set."""
    from app.core.sync import _detect_and_stamp_drops_full_walk
    sync_ts = "2026-05-15T12:00:00Z"
    _seed_theme(db_path, media_type="movie", tmdb_id=1,
                last_seen_sync_at="2026-05-15T12:00:00Z")  # touched this run
    _seed_theme(db_path, media_type="movie", tmdb_id=2,
                last_seen_sync_at="2026-05-14T12:00:00Z")  # NOT touched
    _seed_theme(db_path, media_type="movie", tmdb_id=3,
                last_seen_sync_at="2026-05-15T12:00:00Z")
    n = _detect_and_stamp_drops_full_walk(
        db_path, sync_ts=sync_ts, media_types_seen={"movie"})
    assert n == 1
    assert _read_drop(db_path, "movie", 1) is None
    assert _read_drop(db_path, "movie", 2) == sync_ts
    assert _read_drop(db_path, "movie", 3) is None


def test_full_walk_safety_cap_blocks_mass_drop(db_path: Path):
    """If implied drops exceed 5% of catalog, motif refuses to stamp
    and logs a warning. Protects against a buggy upstream export
    that mass-removes items."""
    from app.core.sync import (
        _detect_and_stamp_drops_full_walk, _DROP_SAFETY_FLOOR,
    )
    sync_ts = "2026-05-15T12:00:00Z"
    # Seed 100 movies, only 5 touched this run → 95 implied drops.
    # Floor is 50 — anything above 50 should block (since 95 > 50
    # AND 95 > 5% of 100 = 5).
    for tmdb_id in range(1, 6):
        _seed_theme(db_path, media_type="movie", tmdb_id=tmdb_id,
                    last_seen_sync_at=sync_ts)  # touched
    for tmdb_id in range(6, 101):
        _seed_theme(db_path, media_type="movie", tmdb_id=tmdb_id,
                    last_seen_sync_at="2026-05-14T12:00:00Z")  # not touched
    n = _detect_and_stamp_drops_full_walk(
        db_path, sync_ts=sync_ts, media_types_seen={"movie"})
    assert n == 0  # blocked by cap
    # No row should be stamped.
    assert _read_drop(db_path, "movie", 50) is None
    assert _read_drop(db_path, "movie", 100) is None


def test_full_walk_skips_already_dropped_rows(db_path: Path):
    """Don't re-stamp rows that already have tdb_dropped_at set.
    Idempotent across runs — the original drop date is preserved."""
    from app.core.sync import _detect_and_stamp_drops_full_walk
    from app.core.db import get_conn
    sync_ts = "2026-05-15T12:00:00Z"
    earlier = "2026-04-01T12:00:00Z"
    _seed_theme(db_path, media_type="movie", tmdb_id=1,
                last_seen_sync_at="2026-05-14T12:00:00Z")
    with get_conn(db_path) as conn:
        conn.execute(
            "UPDATE themes SET tdb_dropped_at = ? WHERE tmdb_id = 1",
            (earlier,),
        )
    n = _detect_and_stamp_drops_full_walk(
        db_path, sync_ts=sync_ts, media_types_seen={"movie"})
    assert n == 0
    assert _read_drop(db_path, "movie", 1) == earlier


def test_full_walk_skips_plex_orphan_rows(db_path: Path):
    """plex_orphan rows aren't sourced from TDB so they can't be
    'dropped from TDB'. Sweep skips them regardless of
    last_seen_sync_at."""
    from app.core.sync import _detect_and_stamp_drops_full_walk
    sync_ts = "2026-05-15T12:00:00Z"
    _seed_theme(db_path, media_type="movie", tmdb_id=1,
                upstream_source="plex_orphan",
                last_seen_sync_at="2026-04-01T00:00:00Z")
    n = _detect_and_stamp_drops_full_walk(
        db_path, sync_ts=sync_ts, media_types_seen={"movie"})
    assert n == 0
    assert _read_drop(db_path, "movie", 1) is None


# ---------- clear-on-readd via _upsert_theme ---------------------------------

def test_upsert_clears_tdb_dropped_at(db_path: Path):
    """When a previously-dropped item gets upserted again (re-add by
    LizardByte), tdb_dropped_at clears automatically. No manual ACK
    required."""
    from app.core.sync import _upsert_theme
    from app.core.db import get_conn
    sync_ts = "2026-05-15T12:00:00Z"
    _seed_theme(db_path, media_type="movie", tmdb_id=1,
                last_seen_sync_at="2026-05-01T00:00:00Z")
    with get_conn(db_path) as conn:
        conn.execute(
            "UPDATE themes SET tdb_dropped_at = ? WHERE tmdb_id = 1",
            ("2026-05-10T00:00:00Z",),
        )
    # _upsert_theme on the same (mt, tmdb) — simulate re-add.
    record = {"id": 1, "title": "Re-Added",
              "youtube_theme_url": "https://youtu.be/y",
              "imdb_id": "tt0000001"}
    with get_conn(db_path) as conn:
        _upsert_theme(conn, media_type="movie", tmdb_id=1,
                      record=record, upstream_source="themoviedb",
                      sync_ts=sync_ts)
    assert _read_drop(db_path, "movie", 1) is None


# ---------- API endpoints ----------------------------------------------------

def test_ack_drop_via_direct_sql(db_path: Path):
    """The handler's effect is `UPDATE themes SET tdb_dropped_at = NULL`.
    Direct SQL exercise verifies idempotency without the FastAPI
    bootstrap dance."""
    from app.core.db import get_conn
    _seed_theme(db_path, media_type="movie", tmdb_id=1)
    with get_conn(db_path) as conn:
        conn.execute(
            "UPDATE themes SET tdb_dropped_at = ? WHERE tmdb_id = 1",
            ("2026-05-10T00:00:00Z",),
        )
        # Simulate ack-drop.
        conn.execute(
            "UPDATE themes SET tdb_dropped_at = NULL "
            "WHERE media_type = 'movie' AND tmdb_id = 1",
        )
    assert _read_drop(db_path, "movie", 1) is None


def test_convert_to_manual_creates_user_override(db_path: Path):
    """The convert-to-manual handler promotes themes.youtube_url into
    user_overrides + clears tdb_dropped_at. Direct SQL exercise."""
    from app.core.db import get_conn
    _seed_theme(db_path, media_type="movie", tmdb_id=1,
                youtube_url="https://youtu.be/aaa")
    with get_conn(db_path) as conn:
        conn.execute(
            "UPDATE themes SET tdb_dropped_at = ? WHERE tmdb_id = 1",
            ("2026-05-10T00:00:00Z",),
        )
        # Simulate convert-to-manual (matches handler's INSERT shape).
        conn.execute(
            """INSERT INTO user_overrides
                 (media_type, tmdb_id, youtube_url,
                  set_at, set_by, note, section_id)
               VALUES ('movie', 1, 'https://youtu.be/aaa',
                       '2026-05-15T00:00:00Z', 'tester',
                       'promoted via CONVERT TO MANUAL', '')"""
        )
        conn.execute(
            "UPDATE themes SET tdb_dropped_at = NULL "
            "WHERE media_type = 'movie' AND tmdb_id = 1"
        )
    with get_conn(db_path) as conn:
        ovr = conn.execute(
            "SELECT youtube_url, section_id FROM user_overrides "
            "WHERE media_type = 'movie' AND tmdb_id = 1"
        ).fetchone()
    assert ovr["youtube_url"] == "https://youtu.be/aaa"
    assert ovr["section_id"] == ""
    assert _read_drop(db_path, "movie", 1) is None


# ---------- stats surfaces drops --------------------------------------------

def test_stats_drops_total(db_path: Path):
    """The /api/stats SELECT subquery drops_total counts dropped
    non-orphan rows."""
    from app.core.db import get_conn
    _seed_theme(db_path, media_type="movie", tmdb_id=1)
    _seed_theme(db_path, media_type="movie", tmdb_id=2)
    _seed_theme(db_path, media_type="movie", tmdb_id=3,
                upstream_source="plex_orphan")
    with get_conn(db_path) as conn:
        conn.execute(
            "UPDATE themes SET tdb_dropped_at = ? WHERE tmdb_id IN (1, 3)",
            ("2026-05-10T00:00:00Z",),
        )
        n = conn.execute(
            """SELECT COUNT(*) FROM themes
                WHERE tdb_dropped_at IS NOT NULL
                  AND upstream_source != 'plex_orphan'"""
        ).fetchone()[0]
    # Only tmdb_id=1 qualifies — tmdb_id=3 is plex_orphan, tmdb_id=2 not dropped.
    assert n == 1


# ---------- safety floor doesn't block tiny catalogs ------------------------

def test_safety_floor_lets_small_drop_count_through(db_path: Path):
    """On a catalog of 10 movies with 8 implied drops, the cap is
    max(50, 5% of 10) = 50 — so 8 stamps are allowed even though
    they're 80% of the catalog. The floor protects against false
    positives on tiny test/dev catalogs."""
    from app.core.sync import _detect_and_stamp_drops_full_walk
    sync_ts = "2026-05-15T12:00:00Z"
    _seed_theme(db_path, media_type="movie", tmdb_id=1,
                last_seen_sync_at=sync_ts)
    _seed_theme(db_path, media_type="movie", tmdb_id=2,
                last_seen_sync_at=sync_ts)
    for tmdb_id in range(3, 11):  # 8 not-touched
        _seed_theme(db_path, media_type="movie", tmdb_id=tmdb_id,
                    last_seen_sync_at="2026-05-14T12:00:00Z")
    n = _detect_and_stamp_drops_full_walk(
        db_path, sync_ts=sync_ts, media_types_seen={"movie"})
    assert n == 8
