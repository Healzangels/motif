"""v1.23.99 (audit) — two safe LOW fixes from the silent-failure sweep.

(1) scheduler._stuck_job_sweep used last_error = COALESCE(last_error, ?), so a
    job re-claimed after a transient failure (the re-claim doesn't clear
    last_error) kept that stale error and MASKED the accurate "worker likely
    hung" terminal message. Now it overwrites — the sweep only hits running jobs
    past the runtime threshold, where "hung" is the correct reason.

(2) adopt._do_keep used INSERT OR REPLACE INTO user_overrides, which deletes +
    re-inserts the row → any unlisted column (notably `intent`) silently reset to
    its 'replace' default. A scan KEEP on a row previously set intent='backup'
    (via PROMOTE / backup-only) flipped it to active-replace. Now ON CONFLICT DO
    UPDATE preserves intent.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from app.core.adopt import _do_keep
from app.core.db import init_db
from app.core.scheduler import _stuck_job_sweep

REPO = Path(__file__).resolve().parent.parent
SCHED_PY = (REPO / "app" / "core" / "scheduler.py").read_text()
ADOPT_PY = (REPO / "app" / "core" / "adopt.py").read_text()
OLD = "2020-01-01T00:00:00+00:00"


# ── (1) stuck-job sweep overwrites the stale last_error ──


def test_stuck_sweep_overwrites_stale_last_error(tmp_path):
    db = tmp_path / "motif.db"
    init_db(db)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO jobs (id, job_type, media_type, tmdb_id, section_id,"
            " payload, status, created_at, next_run_at, started_at, last_error,"
            " attempts) VALUES (1,'download','movie',500,'1','{}','running',"
            " ?, ?, ?, 'database is locked', 2)", (OLD, OLD, OLD))
        conn.commit()
    _stuck_job_sweep(db)
    with sqlite3.connect(db) as conn:
        status, last_error = conn.execute(
            "SELECT status, last_error FROM jobs WHERE id = 1").fetchone()
    assert status == "failed"
    assert "worker likely hung" in last_error, (
        "v1.23.99: the sweep's accurate terminal message must not be masked")
    assert "database is locked" not in last_error, (
        "the stale prior-attempt error must be overwritten, not COALESCE-kept")


def test_sweep_does_not_coalesce_last_error():
    blk = SCHED_PY[SCHED_PY.index("def _stuck_job_sweep"):]
    blk = blk[:blk.index("\n\ndef ") if "\n\ndef " in blk else len(blk)]
    assert "last_error = COALESCE(last_error, ?)" not in blk
    assert "last_error = ? " in blk


# ── (2) _do_keep preserves an existing backup intent ──


def test_do_keep_preserves_backup_intent(tmp_path):
    db = tmp_path / "motif.db"
    init_db(db)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source,"
            " last_seen_sync_at, first_seen_sync_at, youtube_url)"
            " VALUES (1,'movie',500,'M','imdb','t','t','https://y')")
        # an existing override deliberately set intent='backup' (PROMOTE flow).
        conn.execute(
            "INSERT INTO user_overrides (media_type, tmdb_id, theme_id,"
            " youtube_url, set_at, set_by, note, section_id, edition_key, intent)"
            " VALUES ('movie',500,1,'https://y','t','u','n','','','backup')")
        conn.commit()
    _do_keep(db, {"theme_id": 1}, "test")
    with sqlite3.connect(db) as conn:
        intent = conn.execute(
            "SELECT intent FROM user_overrides WHERE media_type='movie'"
            " AND tmdb_id=500 AND section_id='' AND edition_key=''").fetchone()[0]
    assert intent == "backup", (
        "v1.23.99: scan KEEP must NOT reset a backup override to replace")


def test_do_keep_uses_on_conflict_not_or_replace():
    blk = ADOPT_PY[ADOPT_PY.index("def _do_keep"):]
    blk = blk[:blk.index("\n\ndef ") if "\n\ndef " in blk else len(blk)]
    assert "INSERT OR REPLACE INTO user_overrides" not in blk
    assert "ON CONFLICT(media_type, tmdb_id, section_id, edition_key) DO UPDATE" in blk


def test_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
