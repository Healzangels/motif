"""v1.21.53/54 — per-edition theme isolation, Phase B2a (download side).

Makes the worker's download/local_files write path edition-aware: the
staging folder (canonical_theme_subdir) and the local_files row both carry
the edition_key resolved from the job payload. edition_key='' (every job
in production until C2 wires enqueue) reproduces today's path + row
exactly — so this is inert in prod, exercised here by passing a payload.

Pins: the canonical staging path appends {edition-<key>} only for non-''
editions; _record_local_file stamps local_files.edition_key from the
payload; and two editions of one (tmdb, section) write INDEPENDENT
local_files rows (the v63-PK payoff, driven through the real worker).
"""
from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path

import pytest

from app.core.canonical import canonical_theme_subdir
from app.core.db import init_db


NOW = "2026-06-04T00:00:00Z"


# ── canonical staging path: edition tag only for non-'' ──


def test_canonical_subdir_standard_unchanged():
    assert canonical_theme_subdir("The Matrix", "1999") == "The Matrix (1999)"
    # explicit '' must match the no-arg form exactly (behavior-preserving).
    assert canonical_theme_subdir("The Matrix", "1999", "") == \
        canonical_theme_subdir("The Matrix", "1999")


def test_canonical_subdir_appends_edition_tag():
    assert canonical_theme_subdir("LotR", "2001", "extended") == \
        "LotR (2001) {edition-extended}"


def test_canonical_subdir_edition_without_year():
    assert canonical_theme_subdir("NoYear", None, "imax") == \
        "NoYear {edition-imax}"


# ── behavioral: _record_local_file stamps local_files.edition_key ──


def _settings(tmp_path):
    from app.config import Settings
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    return s


def _worker(settings):
    from app.core.worker import Worker, TokenBucket
    return Worker(settings=settings, stop_event=threading.Event(),
                  bucket=TokenBucket(60, 60))


def _seed_section_and_theme(db, *, tmdb_id, section_id="1"):
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


def _record(worker, *, tmdb_id, section_id, rel_path, video_id, edition_key):
    payload = json.dumps({"edition_key": edition_key}) if edition_key else "{}"
    worker._record_local_file(
        media_type="movie", tmdb_id=tmdb_id, section_id=section_id,
        rel_path=rel_path, sha256="a" * 64, size=123, video_id=video_id,
        provenance="auto", source_kind="themerrdb", job_payload=payload)


def _local_files(db, tmdb_id):
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        return conn.execute(
            "SELECT edition_key, file_path, source_video_id FROM local_files"
            " WHERE tmdb_id=? ORDER BY edition_key", (tmdb_id,)).fetchall()


def test_record_local_file_standard_stamps_empty_edition(tmp_path):
    s = _settings(tmp_path)
    _seed_section_and_theme(s.db_path, tmdb_id=100)
    _record(_worker(s), tmdb_id=100, section_id="1",
            rel_path="movies/X (2000)/theme.mp3", video_id="std",
            edition_key="")
    rows = _local_files(s.db_path, 100)
    assert len(rows) == 1
    assert rows[0]["edition_key"] == ""


def test_record_local_file_stamps_edition_from_payload(tmp_path):
    s = _settings(tmp_path)
    _seed_section_and_theme(s.db_path, tmdb_id=120)
    _record(_worker(s), tmdb_id=120, section_id="1",
            rel_path="movies/LotR (2001) {edition-extended}/theme.mp3",
            video_id="ext", edition_key="extended")
    rows = _local_files(s.db_path, 120)
    assert len(rows) == 1
    assert rows[0]["edition_key"] == "extended"


def test_two_editions_write_independent_local_files_rows(tmp_path):
    """THE payoff, through the real worker: the standard + Extended themes
    of one (tmdb, section) land as TWO independent local_files rows instead
    of the second clobbering the first."""
    s = _settings(tmp_path)
    _seed_section_and_theme(s.db_path, tmdb_id=120)
    w = _worker(s)
    _record(w, tmdb_id=120, section_id="1",
            rel_path="movies/LotR (2001)/theme.mp3",
            video_id="std", edition_key="")
    _record(w, tmdb_id=120, section_id="1",
            rel_path="movies/LotR (2001) {edition-extended}/theme.mp3",
            video_id="ext", edition_key="extended")
    rows = _local_files(s.db_path, 120)
    assert len(rows) == 2
    by_ed = {r["edition_key"]: r["source_video_id"] for r in rows}
    assert by_ed == {"": "std", "extended": "ext"}
