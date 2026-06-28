"""v1.13.55: pre-place +P capture regression guard.

Pins the SQL semantics for opportunistic plex_independent_theme=1
capture that lives at the top of Worker._do_place. The actual
worker call sits behind an APScheduler + plex client + filesystem
chain that's expensive to fixture; this test exercises the same
SQL the worker runs against a seeded DB so any future refactor
preserves the intent.

Pre-fix scenario (the bug this guards against):
- Row is P-only: pi.has_theme=1, pi.local_theme_file=0,
  pi.plex_independent_theme IS NULL (column was added in v1.13.38
  but plex_enum hadn't observed it yet, or the user upgraded into
  a state where local_theme_file was already 1 at the time).
- User clicks REPLACE TDB. Worker downloads + places motif's
  sidecar. Now local_theme_file=1.
- Next plex_enum: sidecar=1 → indep_observation=None →
  COALESCE preserves the NULL.
- Frontend renders the row as bare T with no +P composite dot
  even though Plex still has its independent theme.

Fix: capture plex_independent_theme=1 BEFORE the place runs,
guarded by `WHERE plex_independent_theme IS NULL AND has_theme=1
AND local_theme_file=0` so we never overwrite a definitive prior
observation and never set 1 on a row that's already past the
observation window.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.core.db import get_conn, init_db


@pytest.fixture
def fresh_db(tmp_path: Path) -> Path:
    db = tmp_path / "test.db"
    init_db(db)
    return db


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _seed_plex_item(conn, *, rating_key: str, has_theme: int,
                    local_theme_file: int,
                    plex_independent_theme: int | None,
                    section_id: str = "1",
                    media_type: str = "movie",
                    tmdb_id: str = "100"):
    """Seed a plex_items row in the requested state."""
    try:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, "
            "                           is_4k, is_anime, included) "
            "VALUES (?, ?, ?, 0, 0, 1)",
            (section_id, f"Sec{section_id}", media_type),
        )
    except sqlite3.IntegrityError:
        pass
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, media_type, "
        "                        guid_tmdb, title, has_theme, "
        "                        local_theme_file, plex_independent_theme, "
        "                        first_seen_at, last_seen_at) "
        "VALUES (?, ?, ?, ?, 'Test', ?, ?, ?, ?, ?)",
        (rating_key, section_id, media_type, tmdb_id,
         has_theme, local_theme_file, plex_independent_theme,
         _now_iso(), _now_iso()),
    )


# The exact SQL the worker fires at line ~1336 of worker.py.
_CAPTURE_SQL = (
    "UPDATE plex_items "
    "   SET plex_independent_theme = 1 "
    " WHERE rating_key = ? "
    "   AND plex_independent_theme IS NULL "
    "   AND has_theme = 1 "
    "   AND local_theme_file = 0"
)


def test_capture_p_only_with_null_indep(fresh_db: Path):
    """The bug case: P-only row with NULL plex_independent_theme.
    Capture should set it to 1."""
    with get_conn(fresh_db) as conn:
        _seed_plex_item(conn, rating_key="rk-bug",
                        has_theme=1, local_theme_file=0,
                        plex_independent_theme=None)
        conn.execute(_CAPTURE_SQL, ("rk-bug",))
        row = conn.execute(
            "SELECT plex_independent_theme FROM plex_items "
            "WHERE rating_key = 'rk-bug'",
        ).fetchone()
    assert row["plex_independent_theme"] == 1


def test_capture_no_op_when_indep_already_set(fresh_db: Path):
    """If plex_independent_theme was already observed (1 or 0),
    capture must NOT overwrite it. Guard: WHERE
    plex_independent_theme IS NULL."""
    with get_conn(fresh_db) as conn:
        _seed_plex_item(conn, rating_key="rk-known-1",
                        has_theme=1, local_theme_file=0,
                        plex_independent_theme=1)
        _seed_plex_item(conn, rating_key="rk-known-0",
                        has_theme=1, local_theme_file=0,
                        plex_independent_theme=0,
                        section_id="2")
        conn.execute(_CAPTURE_SQL, ("rk-known-1",))
        conn.execute(_CAPTURE_SQL, ("rk-known-0",))
        rows = conn.execute(
            "SELECT rating_key, plex_independent_theme "
            "FROM plex_items WHERE rating_key LIKE 'rk-known-%' "
            "ORDER BY rating_key",
        ).fetchall()
    assert rows[0]["plex_independent_theme"] == 0   # rk-known-0 unchanged
    assert rows[1]["plex_independent_theme"] == 1   # rk-known-1 unchanged


def test_capture_no_op_when_sidecar_already_present(fresh_db: Path):
    """If local_theme_file=1, the observation window has already
    closed (we can't tell whether Plex's has_theme is from its own
    theme or from the sidecar). Capture must skip."""
    with get_conn(fresh_db) as conn:
        _seed_plex_item(conn, rating_key="rk-sidecar",
                        has_theme=1, local_theme_file=1,
                        plex_independent_theme=None)
        conn.execute(_CAPTURE_SQL, ("rk-sidecar",))
        row = conn.execute(
            "SELECT plex_independent_theme FROM plex_items "
            "WHERE rating_key = 'rk-sidecar'",
        ).fetchone()
    assert row["plex_independent_theme"] is None


def test_capture_no_op_when_no_plex_theme(fresh_db: Path):
    """If has_theme=0, Plex doesn't serve any theme. Capture must
    skip — there's no +P to capture."""
    with get_conn(fresh_db) as conn:
        _seed_plex_item(conn, rating_key="rk-none",
                        has_theme=0, local_theme_file=0,
                        plex_independent_theme=None)
        conn.execute(_CAPTURE_SQL, ("rk-none",))
        row = conn.execute(
            "SELECT plex_independent_theme FROM plex_items "
            "WHERE rating_key = 'rk-none'",
        ).fetchone()
    assert row["plex_independent_theme"] is None


def test_capture_idempotent(fresh_db: Path):
    """Running the capture twice on the same row leaves it at 1
    (the guard skips the second call because indep is now NOT
    NULL). Useful for: the place worker retries / a place job
    re-fires after a crash."""
    with get_conn(fresh_db) as conn:
        _seed_plex_item(conn, rating_key="rk-idem",
                        has_theme=1, local_theme_file=0,
                        plex_independent_theme=None)
        conn.execute(_CAPTURE_SQL, ("rk-idem",))
        # Simulate the place running and flipping local_theme_file=1
        conn.execute(
            "UPDATE plex_items SET local_theme_file = 1 "
            "WHERE rating_key = 'rk-idem'",
        )
        # Second capture (e.g., retry) must be a no-op.
        conn.execute(_CAPTURE_SQL, ("rk-idem",))
        row = conn.execute(
            "SELECT plex_independent_theme, local_theme_file "
            "FROM plex_items WHERE rating_key = 'rk-idem'",
        ).fetchone()
    assert row["plex_independent_theme"] == 1
    assert row["local_theme_file"] == 1
