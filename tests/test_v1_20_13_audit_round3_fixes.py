"""v1.20.13 — audit round 3 correctness fixes.

H1 (HIGH, sync): the sync fast-path (fingerprint unchanged) only bumped
last_seen_sync_at and never cleared tdb_dropped_at. A title dropped
upstream and later RE-PUBLISHED byte-identical (transient delete/restore,
or a re-key landing the same content) hit the fast-path → the drop stamp
was never cleared → the row stayed SRC=– / gray TDB◌ FOREVER. The
slow-path UPDATE branches clear it; the fast-path was the gap. Since the
row IS in this sync's walk it's definitionally not dropped, so clearing
is always correct.

MED (concurrency, worker): the movie/TV `_do_place` success-write ran two
statements (placements UPSERT + local_files mismatch-clear) in bare
autocommit, so a concurrent reader could see the placement row before
mismatch_state cleared (transient inconsistent SRC/DL/LINK render). The
sibling collection path already wraps the same write in transaction().
Wrapped it to match.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
WORKER_PY = (REPO / "app" / "core" / "worker.py").read_text()
NOW = "2026-06-01T00:00:00"


# ── H1: fast-path clears the drop stamp on re-add ────────────


def test_fast_path_clears_drop_stamp_on_reappear(tmp_path):
    from app.core.db import init_db
    from app.core.sync import _upsert_theme, _record_fingerprint
    db = tmp_path / "m.db"
    init_db(db)
    record = {"imdb_id": "tt1", "title": "X", "youtube_theme_url": "u"}
    fp = _record_fingerprint(record)
    c = sqlite3.connect(db)
    c.row_factory = sqlite3.Row
    # A dropped row whose stored fingerprint matches the incoming record
    # (the byte-identical re-publish case → the fast-path fires).
    c.execute(
        "INSERT INTO themes (media_type, tmdb_id, title, upstream_source, "
        "  youtube_url, tdb_content_fingerprint, tdb_dropped_at, "
        "  last_seen_sync_at, first_seen_sync_at) "
        "VALUES ('tv',1,'X','imdb','https://yt/x',?, "
        "  '2026-01-01T00:00:00','2026-01-01T00:00:00','2026-01-01T00:00:00')",
        (fp,))
    c.commit()

    res = _upsert_theme(c, media_type="tv", tmdb_id=1, record=record,
                        upstream_source="imdb", sync_ts=NOW)
    c.commit()
    # The fast-path must have fired (nothing changed).
    assert res[0] is False and res[1] is False
    row = c.execute(
        "SELECT tdb_dropped_at, last_seen_sync_at FROM themes "
        "WHERE tmdb_id=1").fetchone()
    assert row["tdb_dropped_at"] is None, (
        "v1.20.13 H1: the fast-path must clear tdb_dropped_at on a "
        "byte-identical re-add — else a re-published title stays gray-"
        "dropped forever"
    )
    assert row["last_seen_sync_at"] == NOW, "last_seen_sync_at still bumps"


def test_slow_path_still_clears_drop_stamp(tmp_path):
    """Counter-guard: a CHANGED record (fingerprint differs) takes the
    slow path, which also clears the drop stamp (unchanged behavior)."""
    from app.core.db import init_db
    from app.core.sync import _upsert_theme, _record_fingerprint
    db = tmp_path / "m.db"
    init_db(db)
    old_record = {"imdb_id": "tt2", "title": "Y", "youtube_theme_url": "old"}
    fp = _record_fingerprint(old_record)
    c = sqlite3.connect(db)
    c.row_factory = sqlite3.Row
    c.execute(
        "INSERT INTO themes (media_type, tmdb_id, title, upstream_source, "
        "  youtube_url, tdb_content_fingerprint, tdb_dropped_at, "
        "  last_seen_sync_at, first_seen_sync_at) "
        "VALUES ('tv',2,'Y','imdb','https://yt/old',?, "
        "  '2026-01-01T00:00:00','2026-01-01T00:00:00','2026-01-01T00:00:00')",
        (fp,))
    c.commit()
    new_record = {"imdb_id": "tt2", "title": "Y",
                  "youtube_theme_url": "https://www.youtube.com/watch?v=NEW"}
    _upsert_theme(c, media_type="tv", tmdb_id=2, record=new_record,
                  upstream_source="imdb", sync_ts=NOW)
    c.commit()
    row = c.execute(
        "SELECT tdb_dropped_at FROM themes WHERE tmdb_id=2").fetchone()
    assert row["tdb_dropped_at"] is None


# ── MED: _do_place success-write is transactional ────────────


def test_do_place_success_write_is_transactional():
    idx = WORKER_PY.index("if outcome.placed:")
    block = WORKER_PY[idx:idx + 1000]
    assert ("with get_conn(self.settings.db_path) as conn, transaction(conn):"
            in block), (
        "v1.20.13: the movie/TV _do_place success-write (placements UPSERT "
        "+ mismatch clear) must run in one transaction so a reader can't "
        "see a half-written state"
    )


def test_v1_20_13_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
