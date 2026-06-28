"""v1.21.65 — per-edition theme isolation, C2c place-source fix.

A gap exposed by going live with per-edition downloads (C2c/C3):
_do_place read its SOURCE local_files row by the bare (mt, tmdb, section)
3-tuple, so a per-edition place job on a multi-edition title could stage an
ARBITRARY sibling edition's .mp3. Now it reads the source for THIS edition
(prefer the edition's own local_files, fall back to the shared '').
"""
from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from types import SimpleNamespace


NOW = "2026-06-04T00:00:00Z"


def _settings(tmp_path):
    from app.config import Settings
    from app.core.db import init_db
    from app.core.runtime import set_dry_run
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    s._cfg.paths.themes_dir = str(tmp_path / "themes")
    set_dry_run(s.db_path, False, updated_by="test")
    return s


def _worker(settings):
    from app.core.worker import Worker, TokenBucket
    return Worker(settings=settings, stop_event=threading.Event(),
                  bucket=TokenBucket(60, 60))


def _seed(db):
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        cur = conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, upstream_source,"
            " last_seen_sync_at, first_seen_sync_at, youtube_url)"
            " VALUES ('movie',55,'V','imdb',?,?,'u')", (NOW, NOW))
        tid = cur.lastrowid
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type,"
            " theme_id, guid_tmdb, title, year, edition_key, folder_path,"
            " first_seen_at, last_seen_at) VALUES ('rk-ext','1','movie',?,55,"
            "'V','2001','extended','/data/Movies/V (2001) {edition-Extended}',"
            "?,?)", (tid, NOW, NOW))
        for ek, fp in (("", "movies/std.mp3"), ("extended", "movies/ext.mp3")):
            conn.execute(
                "INSERT INTO local_files (media_type, tmdb_id, section_id,"
                " edition_key, file_path, downloaded_at, source_video_id,"
                " provenance, source_kind) VALUES ('movie',55,'1',?,?,?, 'v',"
                "'auto','themerrdb')", (ek, fp, NOW))
        conn.commit()


def test_do_place_uses_this_editions_source_file(tmp_path, monkeypatch):
    s = _settings(tmp_path)
    _seed(s.db_path)
    # both source files exist on disk.
    for fp in ("movies/std.mp3", "movies/ext.mp3"):
        p = Path(s.themes_dir) / fp
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"ID3" + b"\x00" * 16)

    captured: dict = {}

    def fake_place(*args, **kwargs):
        captured["source_file"] = str(kwargs.get("source_file"))
        return SimpleNamespace(
            placed=True,
            target_folder=Path("/data/Movies/V (2001) {edition-Extended}"),
            kind="hardlink", plex_rating_key="rk-ext", plex_refreshed=True,
            reason="placed")

    monkeypatch.setattr("app.core.worker.place_theme", fake_place)
    monkeypatch.setattr("app.core.worker.Worker._plex_client",
                        lambda self: None)

    payload = json.dumps({"edition_key": "extended", "kind": "file"})
    with sqlite3.connect(s.db_path) as conn:
        conn.execute(
            "INSERT INTO jobs (job_type, media_type, tmdb_id, section_id,"
            " payload, status, created_at) VALUES ('place','movie',55,'1',?,"
            "'running',?)", (payload, NOW))
        conn.commit()
    c = sqlite3.connect(s.db_path)
    c.row_factory = sqlite3.Row
    job = c.execute("SELECT * FROM jobs WHERE tmdb_id=55").fetchone()

    _worker(s)._do_place(job)

    # the place used the EXTENDED edition's source, not the standard one.
    assert captured["source_file"].endswith("movies/ext.mp3"), \
        captured["source_file"]
