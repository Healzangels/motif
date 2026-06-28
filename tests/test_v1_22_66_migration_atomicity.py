"""v1.22.66 (audit round 2, Batch B #2) — three table-rebuild
migrations were non-atomic.

The v26→v27 (user_overrides), v30→v31 (pending_updates) and v39→v40
(op_progress) rebuilds ran `conn.executescript()` WITHOUT BEGIN/COMMIT.
executescript commits any pending transaction and then runs the script
statement-by-statement in autocommit — each DDL/DML durable
individually. A container kill between `DROP TABLE x` and
`ALTER TABLE x_new RENAME TO x` left the data stranded in the shadow
table with the version unstamped → the next boot re-ran the step →
`CREATE TABLE x_new` failed "table already exists" → init_db raised on
EVERY boot (the v1.19.73 incident class, which proved this trigger
happens in practice). The later op_progress rebuilds (v47/v48/v49/
v52/v59) always had BEGIN/COMMIT; these three predate the convention.

With BEGIN/COMMIT a mid-script kill applies NOTHING (clean re-run),
and a kill after COMMIT-but-before-stamp re-runs a script that is
naturally re-runnable (full rebuild of an already-rebuilt table).
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DB_PY = (REPO / "app" / "core" / "db.py").read_text()


def _script_of(fn_name: str) -> str:
    i = DB_PY.index(f"def {fn_name}(")
    return DB_PY[i:DB_PY.index("\ndef ", i + 1)]


def test_all_three_rebuilds_are_wrapped():
    """Source pin: each previously-bare script opens with BEGIN; and
    closes with COMMIT; (the v49 sibling shape)."""
    for fn in ("_migrate_v26_to_v27", "_migrate_v30_to_v31",
               "_migrate_v39_to_v40"):
        body = _script_of(fn)
        assert "BEGIN;" in body, f"{fn}: script must open a transaction"
        assert "COMMIT;" in body, f"{fn}: script must commit explicitly"
        # BEGIN must precede the first CREATE TABLE.
        assert body.index("BEGIN;") < body.index("CREATE TABLE"), fn


def test_v27_rebuild_round_trips_with_the_wrap(tmp_path):
    """Behavioral: the wrapped v27 script still migrates a v26-shaped
    user_overrides table correctly (data carried, section_id defaulted
    to '')."""
    from app.core.db import _migrate_v26_to_v27
    db = tmp_path / "m.db"
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    conn.executescript(
        "CREATE TABLE user_overrides ("
        " media_type TEXT NOT NULL, tmdb_id INTEGER NOT NULL,"
        " theme_id INTEGER, youtube_url TEXT NOT NULL,"
        " set_at TEXT NOT NULL, set_by TEXT, note TEXT,"
        " PRIMARY KEY (media_type, tmdb_id));")
    conn.execute(
        "INSERT INTO user_overrides VALUES"
        " ('movie', 1, NULL, 'https://yt/x', '2026', 'admin', NULL)")
    conn.commit()

    _migrate_v26_to_v27(conn)

    rows = conn.execute(
        "SELECT media_type, tmdb_id, youtube_url, section_id"
        " FROM user_overrides").fetchall()
    assert len(rows) == 1
    assert rows[0]["section_id"] == ""
    assert rows[0]["youtube_url"] == "https://yt/x"
    # No shadow table left behind.
    assert conn.execute(
        "SELECT 1 FROM sqlite_master WHERE name='user_overrides_v27'"
    ).fetchone() is None
    conn.close()
