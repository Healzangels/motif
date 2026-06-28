"""v1.24.1 — review follow-ups: adopt fake-success + _do_adopt OR-REPLACE + stats loop-block.

From the bugs/silent-failures review on the v1.24.0 tree.

#1 (HIGH, class 9 + fake-success): adopt_finding's broad `except Exception` swallowed
any _do_adopt/_do_replace/_do_keep failure, built {action:failed}, then UNCONDITIONALLY
stamped adopted_at — so a TRANSIENT blip (database is locked, source sidecar momentarily
gone, ENOSPC) became a permanent fake-"adopted": the worker (which ignores the return)
marked the job DONE, and the idempotence guard + the API's adopted_at/jobs dedup blocked
every retry. Now it re-raises → worker _mark_failed bounded retry, adopted_at stays NULL.

#3 (MED): _do_adopt used INSERT OR REPLACE on local_files + placements, which
delete+reinsert on a re-adopt and silently NULL every unlisted column — the health
columns (canonical_present / canonical_health_checked_at; plex_rating_key /
theme_present / placement_health_checked_at) that drive the NEEDS WORK / DL / PL sorts.
Now ON CONFLICT DO UPDATE preserves them (the v1.23.99 _do_keep fix, applied to adopt).

#2 (HIGH, class 12): api_stats offloaded its heavy SQL but called _disk_status_for_stats
(shutil.disk_usage → statvfs) INLINE in the async body; a stalled Unraid/NFS mount would
freeze the event loop. Now computed via run_in_threadpool. The standing AST lint couldn't
see it (nested def + disk_usage not in its blocking set).
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from app.core.db import get_conn, init_db

REPO = Path(__file__).resolve().parent.parent
ADOPT_PY = (REPO / "app" / "core" / "adopt.py").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()
NOW = "2026-06-20T00:00:00+00:00"
TMDB = 770


@pytest.fixture
def settings(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    (tmp_path / "themes").mkdir(parents=True, exist_ok=True)
    s._cfg.paths.themes_dir = str(tmp_path / "themes")
    init_db(s.db_path)
    return s


# ── #1: adopt_finding re-raises on failure, leaves the finding re-decidable ──

def _seed_finding(db) -> int:
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO scan_runs (id, started_at, status)"
            " VALUES (1, ?, 'complete')", (NOW,))
        cur = conn.execute(
            "INSERT INTO scan_findings (scan_run_id, section_id, section_type,"
            " media_folder, file_path, file_size, file_mtime, file_sha256,"
            " finding_kind, theme_id) VALUES (1,'1','movie','/m/f',"
            " '/m/f/theme.mp3', 9, ?, 'ab', 'content_mismatch', NULL)", (NOW,))
        conn.commit()
        return cur.lastrowid


def test_adopt_finding_reraises_and_stays_retryable(settings, monkeypatch):
    db = settings.db_path
    fid = _seed_finding(db)
    import app.core.adopt as adopt_mod

    def boom(*a, **k):
        raise OSError("database is locked")  # a transient blip

    monkeypatch.setattr(adopt_mod, "_do_adopt", boom)
    with pytest.raises(OSError):
        adopt_mod.adopt_finding(db, finding_id=fid, decision="adopt",
                                decided_by="t", settings=settings)
    with sqlite3.connect(db) as conn:
        adopted_at, outcome = conn.execute(
            "SELECT adopted_at, adopt_outcome FROM scan_findings WHERE id=?",
            (fid,)).fetchone()
    assert adopted_at is None, (
        "v1.24.1: a failed adopt must NOT stamp adopted_at — the finding must "
        "stay re-decidable (pre-fix it became a permanent fake-'adopted')")
    assert outcome is None


def test_adopt_finding_no_longer_swallows_to_failed_outcome():
    blk = ADOPT_PY[ADOPT_PY.index("def adopt_finding"):]
    blk = blk[:blk.index("\n\ndef ")]
    assert '{"action": "failed"' not in blk, (
        "v1.24.1: the swallow-and-stamp path is gone; failures re-raise")
    assert 'log.exception("Adopt finding %d failed", finding_id)\n        raise\n' in blk


# ── #3: _do_adopt preserves health columns on re-adopt (ON CONFLICT) ──

def test_do_adopt_preserves_health_columns_on_readopt(settings, tmp_path):
    s = settings
    db = s.db_path
    media_folder = tmp_path / "Movies" / "LotR (2001)"  # no edition tag → ''
    media_folder.mkdir(parents=True, exist_ok=True)
    src = media_folder / "theme.mp3"
    src.write_bytes(b"ID3newbytes")
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        cur = conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, year,"
            " upstream_source, last_seen_sync_at, first_seen_sync_at)"
            " VALUES ('movie',?,'LotR','2001','imdb',?,?)", (TMDB, NOW, NOW))
        tid = cur.lastrowid
        # pre-existing rows (a prior adopt) WITH health state stamped.
        conn.execute(
            "INSERT INTO local_files (media_type, tmdb_id, section_id,"
            " edition_key, theme_id, file_path, downloaded_at, source_video_id,"
            " source_kind, canonical_present, canonical_health_checked_at)"
            " VALUES ('movie',?,'1','',?,'movies/old/theme.mp3',?, 'oldvid',"
            " 'adopt', 1, ?)", (TMDB, tid, NOW, NOW))
        conn.execute(
            "INSERT INTO placements (media_type, tmdb_id, section_id,"
            " edition_key, theme_id, media_folder, placed_at, placement_kind,"
            " plex_rating_key, theme_present, placement_health_checked_at)"
            " VALUES ('movie',?,'1','',?,?,?, 'hardlink', 'rk999', 1, ?)",
            (TMDB, tid, str(media_folder), NOW, NOW))
        conn.commit()

    finding = {
        "section_id": "1", "section_type": "movie",
        "finding_kind": "content_mismatch", "theme_id": tid,
        "file_path": str(src), "file_sha256": "cd" * 32,
        "file_size": src.stat().st_size, "media_folder": str(media_folder),
    }
    from app.core.adopt import _do_adopt
    _do_adopt(db, finding, s, decided_by="t")

    with get_conn(db) as conn:
        lf = conn.execute(
            "SELECT file_sha256, canonical_present, canonical_health_checked_at"
            " FROM local_files WHERE tmdb_id=? AND edition_key=''",
            (TMDB,)).fetchone()
        pl = conn.execute(
            "SELECT plex_rating_key, theme_present, placement_health_checked_at"
            " FROM placements WHERE tmdb_id=? AND edition_key=''",
            (TMDB,)).fetchone()
    # health state preserved (pre-fix OR REPLACE nulled all of these):
    assert lf["canonical_present"] == 1 and lf["canonical_health_checked_at"] == NOW, (
        "v1.24.1: re-adopt must not reset local_files health columns")
    assert (pl["plex_rating_key"] == "rk999" and pl["theme_present"] == 1
            and pl["placement_health_checked_at"] == NOW), (
        "v1.24.1: re-adopt must not reset placement health columns")
    # ...while the adopt DID update the content columns:
    assert lf["file_sha256"] == "cd" * 32


def test_do_adopt_uses_on_conflict_not_or_replace():
    blk = ADOPT_PY[ADOPT_PY.index("def _do_adopt"):]
    blk = blk[:blk.index("\n\ndef ")]
    assert "INSERT OR REPLACE INTO local_files" not in blk
    assert "INSERT OR REPLACE INTO placements" not in blk
    assert ("ON CONFLICT(media_type, tmdb_id, section_id, edition_key) "
            "DO UPDATE") in blk
    assert ("ON CONFLICT(media_type, tmdb_id, section_id, media_folder, "
            "edition_key) DO UPDATE") in blk


# ── #2: stats disk telemetry computed off the event loop ──

def test_api_stats_disk_offloaded_to_threadpool():
    start = API_PY.index("async def api_stats")
    blk = API_PY[start:API_PY.index("api_sync_history", start)]
    assert "await run_in_threadpool(_disk_status_for_stats" in blk, (
        "v1.24.1: the statvfs disk call must run off the event loop")
    assert '"disk": _disk_status_for_stats(settings)' not in blk, (
        "the inline blocking call must be gone")
