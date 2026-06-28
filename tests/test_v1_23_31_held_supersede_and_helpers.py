"""v1.23.31 — code-review follow-ups.

#6: a MANUAL _enqueue_download (RE-DOWNLOAD / ACCEPT UPDATE / SET URL) now
   supersedes a still-HELD sync auto-download (parked at the sentinel, not yet
   running) instead of being deduped to a silent no-op until the sync's
   post-summary release. A genuinely running / already-runnable download still
   dedups.
#7: release_held_downloads(conn) — the one statement the end-of-sync release AND
   the boot reclaim share.
#8: delete_superseded_sidecar_placement(conn, …) — the one predicate the worker
   place path AND verify_placement_health's prune share.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from app.core.db import init_db
from app.core.events import now_iso
from app.core.sync import _enqueue_download, _SYNC_HOLD_NEXT_RUN_AT


REPO = Path(__file__).resolve().parent.parent
WORKER = (REPO / "app" / "core" / "worker.py").read_text()
PLEX_ENUM = (REPO / "app" / "core" / "plex_enum.py").read_text()


def _conn(db: Path) -> sqlite3.Connection:
    c = sqlite3.connect(db)
    c.row_factory = sqlite3.Row
    return c


def _seed_movie(conn, tmdb_id=700):
    now = now_iso()
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, included, is_anime,"
        "  is_4k, themes_subdir, discovered_at, last_seen_at) "
        "VALUES ('1','Movies','movie',1,0,0,'movies',?,?)", (now, now))
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, media_type, title, "
        "  title_norm, guid_tmdb, has_theme, first_seen_at, last_seen_at) "
        "VALUES ('rk1','1','movie','M','m',?,0,?,?)", (tmdb_id, now, now))


def _add_download(conn, *, status, next_run_at, tmdb_id=700, payload='{"reason":"new"}'):
    conn.execute(
        "INSERT INTO jobs (job_type, media_type, tmdb_id, section_id, payload, "
        "  status, created_at, next_run_at) "
        "VALUES ('download','movie',?,'1',?,?,?,?)",
        (tmdb_id, payload, status, now_iso(), next_run_at))


def _downloads(db):
    with _conn(db) as conn:
        return conn.execute(
            "SELECT id, status, next_run_at, payload FROM jobs "
            "WHERE job_type='download' AND tmdb_id=700").fetchall()


def test_manual_enqueue_supersedes_held_download(tmp_path):
    db = tmp_path / "m.db"
    init_db(db)
    with _conn(db) as conn:
        _seed_movie(conn)
        _add_download(conn, status="pending", next_run_at=_SYNC_HOLD_NEXT_RUN_AT)
        conn.commit()
    with _conn(db) as conn:
        n = _enqueue_download(conn, media_type="movie", tmdb_id=700,
                              reason="url_changed", force_place=True)
        conn.commit()
    assert n == 1, "the manual enqueue must insert its own runnable download"
    rows = _downloads(db)
    assert len(rows) == 1, "the held row was replaced, not duplicated"
    r = rows[0]
    assert r["next_run_at"] != _SYNC_HOLD_NEXT_RUN_AT  # runnable now
    assert r["next_run_at"] <= now_iso()
    assert "force_place" in r["payload"]                # manual intent kept


def test_manual_enqueue_dedups_a_running_download(tmp_path):
    # a genuinely in-flight download must NOT be superseded (don't double-run).
    db = tmp_path / "m.db"
    init_db(db)
    with _conn(db) as conn:
        _seed_movie(conn)
        _add_download(conn, status="running", next_run_at=now_iso())
        conn.commit()
    with _conn(db) as conn:
        n = _enqueue_download(conn, media_type="movie", tmdb_id=700,
                              reason="url_changed", force_place=True)
        conn.commit()
    assert n == 0
    rows = _downloads(db)
    assert len(rows) == 1 and rows[0]["status"] == "running"
    assert "force_place" not in rows[0]["payload"]      # untouched


def test_hold_enqueue_does_not_supersede_a_held_row(tmp_path):
    # a sync re-discovery (hold_for_summary=True) finding its own held row must
    # dedup, not duplicate the hold.
    db = tmp_path / "m.db"
    init_db(db)
    with _conn(db) as conn:
        _seed_movie(conn)
        _add_download(conn, status="pending", next_run_at=_SYNC_HOLD_NEXT_RUN_AT)
        conn.commit()
    with _conn(db) as conn:
        n = _enqueue_download(conn, media_type="movie", tmdb_id=700,
                              reason="new", hold_for_summary=True)
        conn.commit()
    assert n == 0
    rows = _downloads(db)
    assert len(rows) == 1 and rows[0]["next_run_at"] == _SYNC_HOLD_NEXT_RUN_AT


# ── #7 / #8 shared-helper source-pins ────────────────────────


def test_boot_reclaim_uses_shared_release_helper():
    # the boot reclaim must call release_held_downloads, not inline the UPDATE.
    i = WORKER.index("orphan sync-held download(s) at startup")
    block = WORKER[i - 600:i + 100]
    assert "release_held_downloads(conn)" in block


def test_both_prune_sites_use_shared_delete_helper():
    # worker place path + the enum prune share one predicate helper.
    assert "delete_superseded_sidecar_placement(" in WORKER
    assert "delete_superseded_sidecar_placement(" in PLEX_ENUM
    # the raw inline DELETE FROM placements (superseded predicate) is gone from
    # the worker place path.
    i = WORKER.index("superseded sidecar PLACEMENT RECORD")
    assert "DELETE FROM placements" not in WORKER[i:i + 1400]
