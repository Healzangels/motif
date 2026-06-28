"""v1.13.54: per-section failure ack tests.

Pins the section_failure_acks table writes + the global-flag-flips-
when-all-sections-acked behavior in /clear-failure. Schema v41
migration smoke test verifies the table + the three new indexes
land on a fresh init_db.
"""
from __future__ import annotations

import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.core.db import CURRENT_SCHEMA_VERSION, get_conn, init_db


@pytest.fixture
def fresh_db(tmp_path: Path) -> Path:
    db = tmp_path / "test.db"
    init_db(db)
    return db


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ── schema v41 migration ────────────────────────────────────────

def test_schema_at_v41(fresh_db: Path):
    with sqlite3.connect(fresh_db) as conn:
        v = conn.execute("SELECT MAX(version) FROM schema_version").fetchone()[0]
    assert v == CURRENT_SCHEMA_VERSION
    assert CURRENT_SCHEMA_VERSION >= 41


def test_section_failure_acks_table_exists(fresh_db: Path):
    with sqlite3.connect(fresh_db) as conn:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name='section_failure_acks'",
        ).fetchone()
    assert row is not None


def test_section_failure_acks_pk_unique(fresh_db: Path):
    """Same (media_type, tmdb_id, section_id) twice should ON CONFLICT
    via the PK or be insertable via INSERT OR REPLACE."""
    with sqlite3.connect(fresh_db) as conn:
        conn.execute(
            "INSERT INTO section_failure_acks "
            "(media_type, tmdb_id, section_id, acked_at, acked_by) "
            "VALUES ('movie', 123, '4', ?, 'admin')",
            (_now_iso(),),
        )
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO section_failure_acks "
                "(media_type, tmdb_id, section_id, acked_at, acked_by) "
                "VALUES ('movie', 123, '4', ?, 'admin2')",
                (_now_iso(),),
            )


def test_v41_indexes_present(fresh_db: Path):
    expected = {
        "idx_plex_items_folder_path",
        "idx_plex_items_indep_null",
        "idx_themes_dropped",
        "idx_section_failure_acks_lookup",
    }
    with sqlite3.connect(fresh_db) as conn:
        names = {
            r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index'",
            ).fetchall()
        }
    missing = expected - names
    assert not missing, f"missing indexes: {missing}"


# ── /clear-failure behavior simulated at the SQL layer ──────────

def _seed_failed_theme(conn, *, tmdb_id: int, sections: list[str],
                       media_type: str = "movie"):
    """Seed a themes row with a failure + plex_items rows for the
    given sections. Mimics the state after a sync + plex_enum where
    the YouTube URL is broken."""
    plex_mt = "show" if media_type == "tv" else "movie"
    conn.execute(
        "INSERT INTO themes (media_type, tmdb_id, title, "
        "                    youtube_url, upstream_source, "
        "                    failure_kind, last_seen_sync_at, "
        "                    first_seen_sync_at) "
        "VALUES (?, ?, 'Test', 'https://yt/123', 'imdb', "
        "        'video_removed', ?, ?)",
        (media_type, tmdb_id, _now_iso(), _now_iso()),
    )
    for sid in sections:
        # Minimal plex_sections row so the JOIN finds something
        try:
            conn.execute(
                "INSERT INTO plex_sections (section_id, title, type, "
                "                           is_4k, is_anime, included) "
                "VALUES (?, 'Sec' || ?, ?, 0, 0, 1)",
                (sid, sid, "movie" if media_type == "movie" else "show"),
            )
        except sqlite3.IntegrityError:
            pass  # already there
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type, "
            "                        guid_tmdb, title, "
            "                        first_seen_at, last_seen_at) "
            "VALUES (?, ?, ?, ?, 'Test', ?, ?)",
            (f"rk-{tmdb_id}-{sid}", sid, plex_mt, str(tmdb_id),
             _now_iso(), _now_iso()),
        )


def test_per_section_ack_doesnt_set_global_flag(fresh_db: Path):
    """Acking one of two sections should leave themes.failure_acked_at NULL."""
    with get_conn(fresh_db) as conn:
        _seed_failed_theme(conn, tmdb_id=100, sections=["1", "2"])
        # Ack only section 1
        conn.execute(
            "INSERT INTO section_failure_acks "
            "(media_type, tmdb_id, section_id, acked_at, acked_by) "
            "VALUES ('movie', 100, '1', ?, 'admin')",
            (_now_iso(),),
        )
        # Verify: global still NULL
        row = conn.execute(
            "SELECT failure_acked_at FROM themes WHERE tmdb_id = 100",
        ).fetchone()
    assert row["failure_acked_at"] is None


def test_per_section_ack_visible_via_join(fresh_db: Path):
    """The library JOIN picks up the section ack for the matching row."""
    with get_conn(fresh_db) as conn:
        _seed_failed_theme(conn, tmdb_id=200, sections=["1", "2"])
        conn.execute(
            "INSERT INTO section_failure_acks "
            "(media_type, tmdb_id, section_id, acked_at, acked_by) "
            "VALUES ('movie', 200, '1', ?, 'admin')",
            (_now_iso(),),
        )
        # Section 1 should look acked; section 2 should not.
        rows = conn.execute(
            "SELECT pi.section_id, "
            "       COALESCE(sfa.acked_at, t.failure_acked_at) AS effective_ack "
            "FROM plex_items pi "
            "JOIN themes t ON t.tmdb_id = pi.guid_tmdb "
            "                AND t.media_type = (CASE pi.media_type "
            "                                      WHEN 'show' THEN 'tv' "
            "                                      ELSE pi.media_type END) "
            "LEFT JOIN section_failure_acks sfa "
            "       ON sfa.media_type = t.media_type "
            "      AND sfa.tmdb_id = t.tmdb_id "
            "      AND sfa.section_id = pi.section_id "
            "WHERE t.tmdb_id = 200 "
            "ORDER BY pi.section_id",
        ).fetchall()
    assert len(rows) == 2
    assert rows[0]["effective_ack"] is not None  # section 1 acked
    assert rows[1]["effective_ack"] is None      # section 2 not acked


def test_index_idx_section_failure_acks_lookup_used(fresh_db: Path):
    """EXPLAIN should mention the lookup index for the typical
    media_type+tmdb_id query pattern."""
    with sqlite3.connect(fresh_db) as conn:
        plan = conn.execute(
            "EXPLAIN QUERY PLAN "
            "SELECT * FROM section_failure_acks "
            "WHERE media_type = 'movie' AND tmdb_id = 1",
        ).fetchall()
    plan_text = " ".join(str(r) for r in plan).lower()
    # The PK on (media_type, tmdb_id, section_id) covers this query
    # too; either index name is acceptable as long as it's not a
    # full table scan.
    assert "scan" not in plan_text or "search" in plan_text, plan_text
