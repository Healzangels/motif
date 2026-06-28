"""v1.24.51 — destructive-migration crash-loop hardening (v29→v30).

v1.24.50 covered the ADDITIVE class. v29→v30 is the one reachable DESTRUCTIVE
migration that wasn't crash-safe: it backfills previous_urls from
themes.previous_youtube_url then DROPs that column, all in one executescript
(autocommits each statement). A container kill after the DROP committed — with
the schema_version still behind — re-ran the INSERT...SELECT against the dropped
column → "no such column: previous_youtube_url" crash-loop on every boot.

The fix gates the backfill + DROP on the source column still existing (mirrors
v52→v53), dropping each column independently. (The other reachable rebuilds are
already crash-safe: v26→v27 + the v30+ rebuilds are BEGIN/COMMIT-atomic per the
v1.22.66 audit; v54→v55/v57→v58/v59→v60 use the idempotent _widen_check helper.)
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from app.core.db import _migrate_v29_to_v30

REPO = Path(__file__).resolve().parent.parent


def _themes_v29(conn):
    # the v29-era themes shape carrying the pre-v30 one-step snapshot columns
    # (PK on (media_type, tmdb_id) so previous_urls' FK can reference it).
    conn.execute(
        """
        CREATE TABLE themes (
            media_type            TEXT NOT NULL,
            tmdb_id               INTEGER NOT NULL,
            previous_youtube_url  TEXT,
            previous_youtube_kind TEXT,
            youtube_edited_at     TEXT,
            last_seen_sync_at     TEXT,
            PRIMARY KEY (media_type, tmdb_id)
        )
        """)


def test_v29_to_v30_migrates_then_reruns_clean(tmp_path):
    conn = sqlite3.connect(tmp_path / "v29.db")
    _themes_v29(conn)
    conn.execute(
        "INSERT INTO themes (media_type, tmdb_id, previous_youtube_url, "
        "previous_youtube_kind, last_seen_sync_at) "
        "VALUES ('movie', 1, 'https://y/x', 'user', '2026-01-01')")
    conn.commit()

    # forward migration: backfills previous_urls + drops the source columns
    _migrate_v29_to_v30(conn)
    conn.commit()
    cols = {r[1] for r in conn.execute("PRAGMA table_info(themes)")}
    assert "previous_youtube_url" not in cols
    assert "previous_youtube_kind" not in cols
    assert conn.execute(
        "SELECT youtube_url, kind FROM previous_urls").fetchall() == [("https://y/x", "user")]

    # THE CRASH-LOOP: re-run against the migrated DB (column gone, version was
    # behind) must be a clean no-op, NOT "no such column: previous_youtube_url".
    _migrate_v29_to_v30(conn)
    conn.commit()
    assert conn.execute("SELECT COUNT(*) FROM previous_urls").fetchone()[0] == 1


def test_v29_to_v30_completes_after_crash_between_the_two_drops(tmp_path):
    # Simulate a kill AFTER previous_youtube_url dropped but BEFORE
    # previous_youtube_kind: re-run must finish the job (drop the stray column),
    # not skip it or crash.
    conn = sqlite3.connect(tmp_path / "partial.db")
    _themes_v29(conn)
    conn.commit()
    _migrate_v29_to_v30(conn)
    conn.commit()
    # partial state: url already gone (dropped first), kind lingering
    conn.execute("ALTER TABLE themes ADD COLUMN previous_youtube_kind TEXT")
    conn.commit()
    _migrate_v29_to_v30(conn)  # must drop the stray kind, no crash
    conn.commit()
    cols = {r[1] for r in conn.execute("PRAGMA table_info(themes)")}
    assert "previous_youtube_kind" not in cols


def test_v29_to_v30_guards_the_drop_on_table_info():
    # source pin: the backfill + DROP is gated on the source column, not bare.
    src = (REPO / "app" / "core" / "db.py").read_text()
    i = src.index("def _migrate_v29_to_v30(")
    body = src[i:src.index("\ndef ", i + 1)]
    assert 'PRAGMA table_info(themes)' in body
    assert 'if "previous_youtube_url" in cols' in body
    # the bare unconditional drops are gone
    assert "ALTER TABLE themes DROP COLUMN previous_youtube_url;" not in body


def test_reachable_inline_rebuilds_stay_begin_commit_atomic():
    # Structural guard: every reachable migration that rebuilds a table inline
    # (DROP TABLE + RENAME TO in its own body) must keep its BEGIN/COMMIT, so a
    # crash mid-rebuild rolls back atomically instead of stranding the shadow
    # table and crash-looping (the v1.19.73 incident the v1.22.66 audit fixed).
    # Helper-based rebuilds (_widen_check_constraint) are covered by their own
    # v1.19.73 idempotency tests and don't match this filter.
    import inspect
    import re as _re
    import app.core.db as db
    checked = []
    for name in dir(db):
        m = _re.match(r"_migrate_v(\d+)_to_v(\d+)$", name)
        if not (m and int(m.group(1)) >= 21):
            continue
        body = inspect.getsource(getattr(db, name))
        if "DROP TABLE" in body and "RENAME TO" in body:
            assert "BEGIN" in body and "COMMIT" in body, (
                f"{name} rebuilds a table inline but lost its BEGIN/COMMIT "
                f"atomicity — a mid-rebuild crash would strand the shadow table")
            checked.append(name)
    assert len(checked) >= 7  # the known reachable inline rebuilds
