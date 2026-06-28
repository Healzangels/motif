"""v1.20.16 — audit round 3 LOW cleanup pass.

L1: a dropped title's pending update is moot (TDB no longer publishes
    the theme) but was left in place, so the row showed a stale,
    un-actionable ACCEPT alongside the gray TDB◌ drop pill. Fix: clear
    the pending update when stamping tdb_dropped_at (both drop paths).
L2: _detect_and_stamp_drops_git's imdb→tmdb resolution used fetchone()
    with no ORDER BY — non-deterministic if two themes share an imdb_id.
    Fix: ORDER BY tmdb_id LIMIT 1.
events: the per-batch `with sqlite3.connect(...) as conn:` in the event
    flusher committed but never closed the connection (leaned on
    refcounting/GC). Fix: explicit close().
"""
from __future__ import annotations

import sqlite3
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
SYNC_PY = (REPO / "app" / "core" / "sync.py").read_text()
EVENTS_PY = (REPO / "app" / "core" / "events.py").read_text()
NOW = "2026-06-01T00:00:00"
OLD = "2026-01-01T00:00:00"


# ── L1: drop clears the moot pending update (behavioral) ─────


def test_drop_clears_pending_update(tmp_path):
    from app.core.db import init_db
    from app.core.sync import _detect_and_stamp_drops_full_walk
    db = tmp_path / "m.db"
    init_db(db)
    with sqlite3.connect(db) as c:
        # A themes row last seen in an OLD sync → implicitly dropped this
        # run; plus a pending upstream_changed.
        c.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, upstream_source, "
            "youtube_url, last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('tv',1,'X','imdb','u',?,?)", (OLD, OLD))
        c.execute(
            "INSERT INTO pending_updates (media_type, tmdb_id, section_id, "
            "kind, new_youtube_url, decision, detected_at) "
            "VALUES ('tv',1,'','upstream_changed','u','pending',?)", (OLD,))
        c.commit()

    n = _detect_and_stamp_drops_full_walk(
        db, sync_ts=NOW, media_types_seen={"tv"})
    assert n == 1, "the unseen row should be stamped dropped"
    with sqlite3.connect(db) as c:
        dropped = c.execute(
            "SELECT tdb_dropped_at FROM themes WHERE tmdb_id=1").fetchone()[0]
        pend = c.execute(
            "SELECT COUNT(*) FROM pending_updates WHERE tmdb_id=1"
        ).fetchone()[0]
    assert dropped is not None
    assert pend == 0, (
        "v1.20.16 L1: the moot pending update must be cleared when the "
        "title is stamped dropped"
    )


def test_drop_keeps_pending_for_surviving_title(tmp_path):
    """Counter-guard: a title still seen this run keeps its pending."""
    from app.core.db import init_db
    from app.core.sync import _detect_and_stamp_drops_full_walk
    db = tmp_path / "m.db"
    init_db(db)
    with sqlite3.connect(db) as c:
        # last_seen_sync_at == NOW → NOT dropped.
        c.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, upstream_source, "
            "youtube_url, last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('tv',2,'Y','imdb','u',?,?)", (NOW, OLD))
        c.execute(
            "INSERT INTO pending_updates (media_type, tmdb_id, section_id, "
            "kind, new_youtube_url, decision, detected_at) "
            "VALUES ('tv',2,'','upstream_changed','u','pending',?)", (OLD,))
        c.commit()
    _detect_and_stamp_drops_full_walk(db, sync_ts=NOW, media_types_seen={"tv"})
    with sqlite3.connect(db) as c:
        pend = c.execute(
            "SELECT COUNT(*) FROM pending_updates WHERE tmdb_id=2"
        ).fetchone()[0]
    assert pend == 1, "a surviving title's pending update must be untouched"


def test_git_drop_path_also_clears_pending():
    """Source pin: the git drop path mirrors the full-walk pending clear."""
    fn = SYNC_PY[SYNC_PY.index("def _detect_and_stamp_drops_git("):]
    fn = fn[:fn.index("\ndef ", 1)]
    assert "DELETE FROM pending_updates" in fn, (
        "v1.20.16 L1: the git drop path must also clear the moot pending"
    )


# ── L2: deterministic imdb→tmdb resolution ───────────────────


def test_git_imdb_resolution_is_deterministic():
    fn = SYNC_PY[SYNC_PY.index("def _detect_and_stamp_drops_git("):]
    fn = fn[:fn.index("\ndef ", 1)]
    idx = fn.index("SELECT tmdb_id FROM themes")
    assert "ORDER BY tmdb_id LIMIT 1" in fn[idx:idx + 200], (
        "v1.20.16 L2: the imdb→tmdb resolution must ORDER BY for "
        "determinism when two rows share an imdb_id"
    )


# ── events: explicit close ───────────────────────────────────


def test_event_flusher_closes_connection():
    idx = EVENTS_PY.index("insert_sql, batch")
    block = EVENTS_PY[idx - 400:idx + 200]
    assert "conn.close()" in block, (
        "v1.20.16: the event flusher must explicitly close its per-batch "
        "connection (the `with sqlite3.connect()` idiom doesn't close)"
    )


def test_v1_20_16_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
