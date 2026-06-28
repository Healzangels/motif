"""v1.24.35 — code-review fixes #1 (re-push kind) + #2 (canonical gate).

#1: _restore_lost_placements enqueued place jobs with no `kind`, so the worker
    used placement.default_method ('file') — silently re-deploying a stale
    plex_upload as a SIDECAR instead of re-uploading. Now the payload carries
    kind='api' for plex_upload and kind='file' for sidecar (matching the
    placement being restored).
#2: neither branch checked the canonical exists; a CONFIRMED-missing canonical
    (canonical_present=0) raised in the worker without a skip-reason stamp →
    doomed hourly re-enqueue + false notification. Now gated on
    canonical_present != 0.
"""
from __future__ import annotations

import json
import sqlite3

import pytest

from app.core import scheduler
from app.core.db import get_conn

# Reuse the v1.24.29 plex_upload seeder + the v1.24.26 sidecar shape.
from test_v1_24_29_plex_upload_auto_repush import _seed as _seed_pu, _settings, NOW


@pytest.fixture
def db(tmp_path, monkeypatch):
    monkeypatch.setattr("app.core.notify.dispatch", lambda *a, **k: None)
    from app.core.db import init_db
    p = tmp_path / "motif.db"
    init_db(p)
    return p


def _place_payloads(db):
    with sqlite3.connect(db) as c:
        return [json.loads(r[0]) for r in c.execute(
            "SELECT payload FROM jobs WHERE job_type='place'").fetchall()]


# ── #1: kind carried per placement type ─────────────────────────────────

def test_plex_upload_repush_carries_kind_api(db):
    _seed_pu(db)  # a stale plex_upload (Avenue Q shape); seeder opens its own conn
    scheduler._restore_lost_placements(_settings(db))
    pl = _place_payloads(db)
    assert len(pl) == 1
    assert pl[0]["kind"] == "api", "a plex_upload re-push must go via the API"


def test_sidecar_repush_carries_kind_file(db, tmp_path):
    # a hardlink placement whose Plex-folder theme.mp3 is gone (live folder)
    folder = tmp_path / "Movie A"
    folder.mkdir()
    with get_conn(db) as c:
        c.execute("INSERT INTO plex_sections (section_id, title, type, is_anime,"
                  " is_4k, themes_subdir, included, discovered_at, last_seen_at) "
                  "VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        c.execute("INSERT INTO themes (id, media_type, tmdb_id, title, year, "
                  " upstream_source, last_seen_sync_at, first_seen_sync_at) "
                  "VALUES (10,'movie',111,'A','2001','imdb',?,?)", (NOW, NOW))
        c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, "
                  " edition_key, theme_id, file_path, source_kind, "
                  " source_video_id, downloaded_at, canonical_present, "
                  " last_place_attempt_reason) "
                  "VALUES ('movie',111,'1','',10,'a.mp3','themerrdb','v',?,1,'placed')",
                  (NOW,))
        c.execute("INSERT INTO placements (theme_id, media_type, tmdb_id, "
                  " section_id, edition_key, media_folder, placed_at, "
                  " placement_kind, plex_refreshed, theme_present) "
                  "VALUES (10,'movie',111,'1','',?,?, 'hardlink',1,0)",
                  (str(folder), NOW))
        c.execute("INSERT INTO plex_items (rating_key, section_id, media_type, "
                  " theme_id, guid_tmdb, title, edition_key, folder_path, "
                  " has_theme, first_seen_at, last_seen_at) "
                  "VALUES ('rk','1','movie',10,111,'A','',?,0,?,?)",
                  (str(folder), NOW, NOW))
        c.commit()
    scheduler._restore_lost_placements(_settings(db))
    pl = _place_payloads(db)
    assert len(pl) == 1
    assert pl[0]["kind"] == "file", "a sidecar re-push must re-hardlink, not upload"


# ── #2: canonical_present=0 gate ────────────────────────────────────────

def test_skips_when_canonical_confirmed_missing(db):
    _seed_pu(db)
    with get_conn(db) as c:
        c.execute("UPDATE local_files SET canonical_present=0 WHERE tmdb_id=-29")
        c.commit()
    scheduler._restore_lost_placements(_settings(db))
    assert _place_payloads(db) == [], "must not enqueue a doomed place"


def test_unverified_canonical_still_restores(db):
    _seed_pu(db)
    with get_conn(db) as c:
        c.execute("UPDATE local_files SET canonical_present=NULL WHERE tmdb_id=-29")
        c.commit()
    scheduler._restore_lost_placements(_settings(db))
    assert len(_place_payloads(db)) == 1, "NULL (unverified) stays eligible"


# ── source pins ─────────────────────────────────────────────────────────

def test_fixes_present_in_source():
    from pathlib import Path
    src = (Path(__file__).resolve().parent.parent
           / "app" / "core" / "scheduler.py").read_text()
    assert "COALESCE(lf.canonical_present, 1) != 0" in src
    assert '"kind": ("api" if r["placement_kind"]' in src
