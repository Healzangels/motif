"""v0.51.157 — schema v72: local_files loudness columns (loudness feature, Phase 0/B).

The read-only LOUDNESS AUDIT (a later tag) measures each local-bytes theme with
ffmpeg (see test_v0_51_156) and stores the result on the row. This tag lands the
five additive columns that hold it:

    loudness_i, loudness_tp, loudness_lra   -- the EBU R128 figures (REAL)
    loudness_measured_at                    -- when (TEXT ISO)
    loudness_measured_sha256                -- WHICH bytes (TEXT) — stale-detect

All additive, idempotent via _add_column (crash-loop-safe: re-running the migration
on an already-migrated table must be a no-op, per the v1.24.50 pattern). Existing
rows read NULL until the audit op runs. Nothing here mutates a file.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from app.core import db as core_db
from app.core.db import CURRENT_SCHEMA_VERSION, get_conn, init_db

_LOUDNESS_COLS = {
    "loudness_i": "REAL",
    "loudness_tp": "REAL",
    "loudness_lra": "REAL",
    "loudness_measured_at": "TEXT",
    "loudness_measured_sha256": "TEXT",
}


def _local_files_cols(conn: sqlite3.Connection) -> dict[str, str]:
    return {r[1]: r[2] for r in conn.execute("PRAGMA table_info(local_files)")}


def test_schema_version_is_at_least_72():
    # v0.51.168 added v73 (normalization state); this tag's columns must survive every
    # later migration, so pin the FLOOR + assert the columns below, not an exact head.
    assert CURRENT_SCHEMA_VERSION >= 72


def test_fresh_db_has_loudness_columns(tmp_path: Path):
    db_path = tmp_path / "motif.db"
    init_db(db_path)
    with get_conn(db_path) as conn:
        cols = _local_files_cols(conn)
        for name, decl in _LOUDNESS_COLS.items():
            assert name in cols, f"{name} missing from local_files after fresh init"
            assert cols[name].upper() == decl, f"{name} should be {decl}, got {cols[name]}"


def test_fresh_db_stamps_at_least_v72(tmp_path: Path):
    db_path = tmp_path / "motif.db"
    init_db(db_path)
    with get_conn(db_path) as conn:
        v = conn.execute(
            "SELECT MAX(version) FROM schema_version"
        ).fetchone()[0]
        # a fresh DB runs the whole chain → the CURRENT head (v73 as of v0.51.168), not
        # this tag's v72. Assert the floor; the columns are what this tag actually guards.
        assert v >= 72


def test_migration_is_idempotent(tmp_path: Path):
    # crash-loop safety: the runner can re-enter a half-applied migration (column
    # committed, version stamp not) — re-running must NOT raise "duplicate column".
    db_path = tmp_path / "motif.db"
    init_db(db_path)  # already has the columns
    with get_conn(db_path) as conn:
        core_db._migrate_v71_to_v72(conn)  # no-op, must not raise
        core_db._migrate_v71_to_v72(conn)  # twice for good measure
        cols = _local_files_cols(conn)
        assert all(name in cols for name in _LOUDNESS_COLS)


def test_loudness_values_round_trip(tmp_path: Path):
    # the columns must actually accept + return the float/text measurement shape the
    # audit op will write (guards against a wrong-affinity typo).
    db_path = tmp_path / "motif.db"
    init_db(db_path)
    with get_conn(db_path) as conn:
        # affinity-only check — skip the themes/plex_sections parents (FK off).
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute(
            "INSERT INTO local_files (media_type, tmdb_id, section_id, file_path, "
            "downloaded_at, source_video_id, loudness_i, loudness_tp, loudness_lra, "
            "loudness_measured_at, loudness_measured_sha256) "
            "VALUES ('movie', 555, '1', 'a/theme.mp3', '2026-07-15', 'vid', "
            "-24.30, -5.12, 7.30, '2026-07-15T00:00:00Z', 'abc123')"
        )
        row = conn.execute(
            "SELECT loudness_i, loudness_tp, loudness_lra, loudness_measured_sha256 "
            "FROM local_files WHERE tmdb_id = 555"
        ).fetchone()
        assert row[0] == -24.30 and row[1] == -5.12 and row[2] == 7.30
        assert row[3] == "abc123"
