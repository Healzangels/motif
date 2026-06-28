"""v1.21.63 — per-edition theme isolation, C2c chain fix.

REDOWNLOAD is download + auto-place. The download job carries edition_key
(v1.21.62), and _record_local_file writes the edition-keyed local_files row
(B2a) — but the CHAINED place job it enqueues hardcoded its payload
('{"force":true,...}' / '{}') without edition_key, so the auto-place would
target the standard '' folder instead of the edition's. This propagates the
download's edition_key into the chained place payload.
"""
from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path


NOW = "2026-06-04T00:00:00Z"


def _settings(tmp_path):
    from app.config import Settings
    from app.core.db import init_db
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    return s


def _worker(settings):
    from app.core.worker import Worker, TokenBucket
    return Worker(settings=settings, stop_event=threading.Event(),
                  bucket=TokenBucket(60, 60))


def _seed(db, *, tmdb_id, section_id="1"):
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO plex_sections (section_id, title, type,"
            " is_anime, is_4k, themes_subdir, included, discovered_at,"
            " last_seen_at) VALUES (?,?,'movie',0,0,'movies',1,?,?)",
            (section_id, "Movies", NOW, NOW))
        conn.execute(
            "INSERT OR IGNORE INTO themes (media_type, tmdb_id, title,"
            " upstream_source, last_seen_sync_at, first_seen_sync_at,"
            " youtube_url) VALUES ('movie',?,'X','imdb',?,?,'u')",
            (tmdb_id, NOW, NOW))
        conn.commit()


def _place_jobs(db, tmdb_id):
    with sqlite3.connect(db) as conn:
        return [json.loads(p) for (p,) in conn.execute(
            "SELECT payload FROM jobs WHERE job_type='place' AND tmdb_id=?",
            (tmdb_id,))]


def _record(worker, *, tmdb_id, edition_key, auto_place=True):
    payload = {"auto_place": auto_place}
    if edition_key:
        payload["edition_key"] = edition_key
    worker._record_local_file(
        media_type="movie", tmdb_id=tmdb_id, section_id="1",
        rel_path=f"movies/X ({edition_key or 'std'}).mp3", sha256="a" * 64,
        size=1, video_id="v", provenance="auto", source_kind="themerrdb",
        job_payload=json.dumps(payload))


def test_chained_place_job_carries_edition_key(tmp_path):
    s = _settings(tmp_path)
    _seed(s.db_path, tmdb_id=120)
    _record(_worker(s), tmdb_id=120, edition_key="extended")
    jobs = _place_jobs(s.db_path, 120)
    assert len(jobs) == 1
    assert jobs[0].get("edition_key") == "extended"


def test_chained_place_job_standard_omits_edition(tmp_path):
    s = _settings(tmp_path)
    _seed(s.db_path, tmdb_id=121)
    _record(_worker(s), tmdb_id=121, edition_key="")
    jobs = _place_jobs(s.db_path, 121)
    assert len(jobs) == 1
    assert "edition_key" not in jobs[0]  # '' -> payload omits it (today)
