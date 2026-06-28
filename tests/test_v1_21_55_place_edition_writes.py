"""v1.21.55 — per-edition theme isolation, Phase B2b (placement side).

Makes _do_place edition-aware: it passes the job payload's edition_key to
place_theme as edition_raw (so the matcher targets the right edition
folder), keys the placements row by the edition of the folder it
PHYSICALLY landed in, and scopes its local_files outcome UPDATEs to that
edition. edition_key='' (every job in production until C2) reproduces
today exactly — this is inert in prod, driven here through a real
_do_place with place_theme mocked at its boundary.
"""
from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest


NOW = "2026-06-04T00:00:00Z"


def _settings(tmp_path):
    from app.config import Settings
    from app.core.db import init_db
    from app.core.runtime import set_dry_run
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    # is_paths_ready() + section_themes_dir_by_subdir need themes_dir set.
    s._cfg.paths.themes_dir = str(tmp_path / "themes")
    # dry_run_default is True in a fresh config; turn it off so _do_place
    # runs the real placement path (not the dry preview).
    set_dry_run(s.db_path, False, updated_by="test")
    return s


def _worker(settings):
    from app.core.worker import Worker, TokenBucket
    return Worker(settings=settings, stop_event=threading.Event(),
                  bucket=TokenBucket(60, 60))


def _seed(db, *, tmdb_id, section_id, edition_key, rel_path):
    """A movie row ready to place: section, themes, the linked plex_items
    (so cached_rk resolves), and an edition-keyed local_files row."""
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO plex_sections (section_id, title, type,"
            " is_anime, is_4k, themes_subdir, included, discovered_at,"
            " last_seen_at) VALUES (?,?,'movie',0,0,'movies',1,?,?)",
            (section_id, "Movies", NOW, NOW))
        cur = conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, upstream_source,"
            " last_seen_sync_at, first_seen_sync_at, youtube_url)"
            " VALUES ('movie',?,'LotR','imdb',?,?,'u')", (tmdb_id, NOW, NOW))
        theme_id = cur.lastrowid
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type,"
            " title, year, guid_tmdb, folder_path, has_theme, theme_id,"
            " first_seen_at, last_seen_at) VALUES (?,?, 'movie','LotR',"
            " '2001', ?, ?, 0, ?, ?, ?)",
            (f"rk-{edition_key or 'std'}", section_id, tmdb_id,
             f"/data/Movies/LotR (2001)"
             + (f" {{edition-{edition_key}}}" if edition_key else ""),
             theme_id, NOW, NOW))
        conn.execute(
            "INSERT INTO local_files (media_type, tmdb_id, section_id,"
            " edition_key, file_path, downloaded_at, source_video_id,"
            " provenance, source_kind) VALUES ('movie',?,?,?,?,?, 'v','auto',"
            " 'themerrdb')", (tmdb_id, section_id, edition_key, rel_path, NOW))
        conn.commit()


def _place_job(db, *, tmdb_id, section_id, edition_key):
    payload = json.dumps({"edition_key": edition_key, "kind": "file"})
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO jobs (job_type, media_type, tmdb_id, section_id,"
            " payload, status, created_at) VALUES ('place','movie',?,?,?,"
            " 'running',?)", (tmdb_id, section_id, payload, NOW))
        conn.commit()
    c = sqlite3.connect(db)
    c.row_factory = sqlite3.Row
    return c.execute("SELECT * FROM jobs WHERE tmdb_id=?",
                     (tmdb_id,)).fetchone()


def _mock_place_theme(captured, target_folder):
    """Replace place_theme: record the edition_raw it was given, and return
    a placed outcome whose target_folder is `target_folder` (the physical
    Plex folder the placement landed in)."""
    def fake(*args, **kwargs):
        captured["edition_raw"] = kwargs.get("edition_raw")
        return SimpleNamespace(
            placed=True, target_folder=Path(target_folder), kind="hardlink",
            plex_rating_key=kwargs.get("cached_rk"), plex_refreshed=True,
            reason="placed")
    return fake


def test_do_place_passes_edition_raw_and_keys_placement(tmp_path, monkeypatch):
    s = _settings(tmp_path)
    rel = "movies/LotR (2001) {edition-extended}/theme.mp3"
    _seed(s.db_path, tmdb_id=120, section_id="1", edition_key="extended",
          rel_path=rel)
    # the source file must exist for _do_place to read it.
    src = Path(s.themes_dir) / rel
    src.parent.mkdir(parents=True, exist_ok=True)
    src.write_bytes(b"ID3" + b"\x00" * 64)
    target = "/data/Movies/LotR (2001) {edition-extended}"

    captured: dict = {}
    monkeypatch.setattr("app.core.worker.place_theme",
                        _mock_place_theme(captured, target))
    monkeypatch.setattr("app.core.worker.Worker._plex_client",
                        lambda self: None)

    job = _place_job(s.db_path, tmdb_id=120, section_id="1",
                     edition_key="extended")
    _worker(s)._do_place(job)

    # place_theme got the edition from the payload, not hardcoded ''.
    assert captured["edition_raw"] == "extended"
    with sqlite3.connect(s.db_path) as conn:
        rows = conn.execute(
            "SELECT edition_key, media_folder FROM placements"
            " WHERE tmdb_id=120").fetchall()
    assert len(rows) == 1
    # placement keyed by the edition of the folder it physically placed in.
    assert rows[0][0] == "extended"
    assert rows[0][1] == target


def test_do_place_standard_is_behavior_preserving(tmp_path, monkeypatch):
    """The '' path (every job in production): edition_raw='' to place_theme,
    placement keyed '' — exactly today."""
    s = _settings(tmp_path)
    rel = "movies/LotR (2001)/theme.mp3"
    _seed(s.db_path, tmdb_id=121, section_id="1", edition_key="",
          rel_path=rel)
    src = Path(s.themes_dir) / rel
    src.parent.mkdir(parents=True, exist_ok=True)
    src.write_bytes(b"ID3" + b"\x00" * 64)
    target = "/data/Movies/LotR (2001)"

    captured: dict = {}
    monkeypatch.setattr("app.core.worker.place_theme",
                        _mock_place_theme(captured, target))
    monkeypatch.setattr("app.core.worker.Worker._plex_client",
                        lambda self: None)

    job = _place_job(s.db_path, tmdb_id=121, section_id="1", edition_key="")
    _worker(s)._do_place(job)

    assert captured["edition_raw"] == ""
    with sqlite3.connect(s.db_path) as conn:
        row = conn.execute(
            "SELECT edition_key FROM placements WHERE tmdb_id=121").fetchone()
    assert row[0] == ""
