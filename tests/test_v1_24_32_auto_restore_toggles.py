"""v1.24.32 — independent toggles for the two auto-restore branches.

the user wanted settings switches to enable/disable the auto-recovery behaviors,
chose SEPARATE sidecar vs plex_upload re-push toggles. _restore_lost_placements
now gates its two branches on placement.auto_restore_sidecar /
placement.auto_restore_plex_upload (both default ON = the v1.24.26/.29 behavior).
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.core import scheduler
from app.core.db import get_conn, init_db

NOW = datetime.now(timezone.utc).isoformat(timespec="seconds")
REPO = Path(__file__).resolve().parent.parent


def _settings(db, *, sidecar=True, upload=True):
    return SimpleNamespace(db_path=db, cfg=SimpleNamespace(
        notifications=None,
        placement=SimpleNamespace(auto_restore_sidecar=sidecar,
                                  auto_restore_plex_upload=upload)))


@pytest.fixture
def db(tmp_path, monkeypatch):
    monkeypatch.setattr("app.core.notify.dispatch", lambda *a, **k: None)
    p = tmp_path / "motif.db"
    init_db(p)
    return p


def _section(c):
    c.execute("INSERT OR IGNORE INTO plex_sections (section_id, title, type, "
              " is_anime, is_4k, themes_subdir, included, discovered_at, "
              " last_seen_at) VALUES ('1','Movies','movie',0,0,'movies',1,?,?)",
              (NOW, NOW))


def _seed_sidecar_missing(c, tmpdir):
    """A hardlink placement whose theme.mp3 is gone from a LIVE Plex folder."""
    folder = str(tmpdir / "Movie A")
    (tmpdir / "Movie A").mkdir(parents=True)  # folder exists, no theme.mp3
    _section(c)
    c.execute("INSERT INTO themes (id, media_type, tmdb_id, title, year, "
              " upstream_source, last_seen_sync_at, first_seen_sync_at) "
              "VALUES (10, 'movie', 111, 'Movie A', '2001', 'imdb', ?, ?)",
              (NOW, NOW))
    c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, "
              " edition_key, file_path, source_kind, source_video_id, "
              " downloaded_at, last_place_attempt_reason) "
              "VALUES ('movie', 111, '1', '', 'a.mp3', 'themerrdb', 'v', ?, 'placed')",
              (NOW,))
    c.execute("INSERT INTO placements (theme_id, media_type, tmdb_id, section_id, "
              " edition_key, media_folder, placed_at, placement_kind, "
              " plex_refreshed, theme_present) "
              "VALUES (10,'movie',111,'1','',?,?, 'hardlink', 1, 0)", (folder, NOW))
    c.execute("INSERT INTO plex_items (rating_key, section_id, media_type, "
              " theme_id, guid_tmdb, title, edition_key, folder_path, has_theme, "
              " first_seen_at, last_seen_at) "
              "VALUES ('rk-a','1','movie',10,111,'Movie A','',?,0,?,?)",
              (folder, NOW, NOW))


def _seed_stale_upload(c):
    """A plex_upload placement whose uploaded rk is dead + a live bonded item."""
    _section(c)
    c.execute("INSERT INTO themes (id, media_type, tmdb_id, title, year, "
              " upstream_source, last_seen_sync_at, first_seen_sync_at) "
              "VALUES (20, 'movie', -29, 'Avenue Q', '2003', 'plex_orphan', ?, ?)",
              (NOW, NOW))
    c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, "
              " edition_key, file_path, source_kind, source_video_id, "
              " downloaded_at, last_place_attempt_reason) "
              "VALUES ('movie', -29, '1', '', 'q.mp3', 'url', 'v', ?, 'placed')",
              (NOW,))
    c.execute("INSERT INTO placements (theme_id, media_type, tmdb_id, section_id, "
              " edition_key, media_folder, placed_at, placement_kind, "
              " plex_rating_key, plex_refreshed, theme_present) "
              "VALUES (20,'movie',-29,'1','','',?, 'plex_upload', 'dead', 1, 0)",
              (NOW,))
    c.execute("INSERT INTO plex_items (rating_key, section_id, media_type, "
              " theme_id, guid_tmdb, title, edition_key, has_theme, "
              " first_seen_at, last_seen_at) "
              "VALUES ('rk-live','1','movie',20,-29,'Avenue Q','',0,?,?)",
              (NOW, NOW))


def _place_tmdbs(db):
    with sqlite3.connect(db) as c:
        return sorted(r[0] for r in c.execute(
            "SELECT tmdb_id FROM jobs WHERE job_type='place'").fetchall())


# ── gating behavior ─────────────────────────────────────────────────────

def test_both_on_restores_both(db, tmp_path):
    with get_conn(db) as c:
        _seed_sidecar_missing(c, tmp_path / "media")
        _seed_stale_upload(c)
        c.commit()
    scheduler._restore_lost_placements(_settings(db, sidecar=True, upload=True))
    assert _place_tmdbs(db) == [-29, 111]


def test_sidecar_off_skips_only_sidecar(db, tmp_path):
    with get_conn(db) as c:
        _seed_sidecar_missing(c, tmp_path / "media")
        _seed_stale_upload(c)
        c.commit()
    scheduler._restore_lost_placements(_settings(db, sidecar=False, upload=True))
    assert _place_tmdbs(db) == [-29]  # only the plex_upload re-push


def test_upload_off_skips_only_upload(db, tmp_path):
    with get_conn(db) as c:
        _seed_sidecar_missing(c, tmp_path / "media")
        _seed_stale_upload(c)
        c.commit()
    scheduler._restore_lost_placements(_settings(db, sidecar=True, upload=False))
    assert _place_tmdbs(db) == [111]  # only the sidecar re-place


def test_both_off_no_op(db, tmp_path):
    with get_conn(db) as c:
        _seed_sidecar_missing(c, tmp_path / "media")
        _seed_stale_upload(c)
        c.commit()
    scheduler._restore_lost_placements(_settings(db, sidecar=False, upload=False))
    assert _place_tmdbs(db) == []


def test_missing_config_defaults_on(db, tmp_path):
    """A settings object with no placement section (older yaml / the v1.24.26/.29
    test fixtures) keeps the always-on behavior via getattr defaults."""
    with get_conn(db) as c:
        _seed_stale_upload(c)
        c.commit()
    legacy = SimpleNamespace(db_path=db, cfg=SimpleNamespace(notifications=None))
    scheduler._restore_lost_placements(legacy)
    assert _place_tmdbs(db) == [-29]


# ── source pins ─────────────────────────────────────────────────────────

def test_config_fields_default_true():
    from app.core.config_file import PlacementConfig
    pc = PlacementConfig()
    assert pc.auto_restore_sidecar is True
    assert pc.auto_restore_plex_upload is True


def test_settings_toggles_present():
    html = (REPO / "app" / "web" / "templates" / "settings.html").read_text()
    assert 'data-cfg-field="placement.auto_restore_sidecar"' in html
    assert 'data-cfg-field="placement.auto_restore_plex_upload"' in html


def test_scheduler_gates_both_branches():
    src = (REPO / "app" / "core" / "scheduler.py").read_text()
    assert "auto_restore_sidecar" in src and "auto_restore_plex_upload" in src
    assert "if do_sidecar else []" in src
    assert "if do_upload else []" in src
