"""v1.21.66 — REPLACE TDB / cached_rk per-edition isolation.

the user's bug (v1.21.65, real library): SOURCE-menu REPLACE TDB on the
"Sam Takes a Step" edition of a multi-edition movie placed the theme into
the *Theatrical* folder, "then it fails and doesn't place anything" (his
LotR theme is >10MB → Plex upload 500s → hardlink-sidecar fallback into
whatever folder cached_rk pointed at). Two root causes:

  1. REPLACE TDB enqueued an edition-LESS download — its chained place
     job carried no edition_key, so the worker couldn't know which
     edition to target. (adopt.replace_with_themerrdb + the endpoint.)

  2. The place worker resolved `cached_rk` (the rk it operates on) by
     (theme_id/guid_tmdb, section_id) LIMIT 1 — an ARBITRARY edition
     across a multi-edition title. the user's log: `_do_place_collection:
     ... section_id=1 cached_rk=167699` (Theatrical) for a place job that
     targeted rk 6281 (Sam Takes a Step).

This pins both: the endpoint now threads the row's edition_key into the
download payload (and scopes the override capture/delete to that edition
so a sibling edition's override survives), and `_do_place_collection`
resolves cached_rk for the NAMED edition's plex_items row.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import threading
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core.auth import create_admin, init_auth_schema
from app.core.db import init_db
from app.core.editions import edition_key_for_folder
from app.core.events import now_iso


NOW = now_iso()
THEATRICAL_RK = "167699"
SAM_RK = "6281"
SAM_FOLDER = "/data/Movies/LotR (2001) {edition-Sam Takes a Step}"
THEAT_FOLDER = "/data/Movies/LotR (2001) {edition-Theatrical}"
SAM_KEY = edition_key_for_folder(SAM_FOLDER)        # 'sam takes a step'
THEAT_KEY = edition_key_for_folder(THEAT_FOLDER)    # 'theatrical'


# ── worker harness (mirrors test_v1_21_65) ───────────────────────

def _settings(tmp_path, *, dry):
    from app.config import Settings
    from app.core.runtime import set_dry_run
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    s._cfg.paths.themes_dir = str(tmp_path / "themes")
    set_dry_run(s.db_path, dry, updated_by="test")
    return s


def _worker(settings):
    from app.core.worker import Worker, TokenBucket
    return Worker(settings=settings, stop_event=threading.Event(),
                  bucket=TokenBucket(60, 60))


def _seed_two_editions(db):
    """Theatrical (rk 167699) + Sam Takes a Step (rk 6281): two plex_items
    rows, one tmdb/theme/section, distinct edition_key."""
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        cur = conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, year,"
            " upstream_source, last_seen_sync_at, first_seen_sync_at,"
            " youtube_url) VALUES ('movie',120,'LotR','2001','imdb',?,?,"
            "'https://www.youtube.com/watch?v=tdb00000001')", (NOW, NOW))
        tid = cur.lastrowid
        for rk, ek, fp in (
            (THEATRICAL_RK, THEAT_KEY, THEAT_FOLDER),
            (SAM_RK, SAM_KEY, SAM_FOLDER),
        ):
            conn.execute(
                "INSERT INTO plex_items (rating_key, section_id, media_type,"
                " theme_id, guid_tmdb, title, year, edition_key, folder_path,"
                " has_theme, first_seen_at, last_seen_at) VALUES (?,'1',"
                "'movie',?,120,'LotR','2001',?,?,0,?,?)",
                (rk, tid, ek, fp, NOW, NOW))
        conn.commit()
    return tid


# ── Root cause #2: cached_rk resolves the NAMED edition ──────────

def test_do_place_collection_resolves_named_editions_rk(tmp_path, caplog):
    """A place job naming edition 'sam takes a step' must resolve
    cached_rk = 6281 (Sam), NOT 167699 (Theatrical) — the exact rk
    mismatch from the user's docker logs."""
    s = _settings(tmp_path, dry=True)
    tid = _seed_two_editions(s.db_path)

    payload = json.dumps({"edition_key": SAM_KEY, "kind": "api"})
    with sqlite3.connect(s.db_path) as conn:
        conn.execute(
            "INSERT INTO jobs (job_type, media_type, tmdb_id, section_id,"
            " payload, status, created_at) VALUES ('place','movie',120,'1',"
            "?,'running',?)", (payload, NOW))
        conn.commit()
    c = sqlite3.connect(s.db_path)
    c.row_factory = sqlite3.Row
    job = c.execute("SELECT * FROM jobs WHERE tmdb_id=120").fetchone()
    theme = c.execute("SELECT * FROM themes WHERE id=?", (tid,)).fetchone()

    with caplog.at_level(logging.INFO, logger="app.core.worker"):
        _worker(s)._do_place_collection(job=job, theme=theme, local=None)

    line = next((r.getMessage() for r in caplog.records
                 if "_do_place_collection: job=" in r.getMessage()), None)
    assert line is not None, "expected the _do_place_collection state log"
    assert f"cached_rk={SAM_RK}" in line, line
    assert f"cached_rk={THEATRICAL_RK}" not in line, line


# ── Endpoint: REPLACE TDB threads edition_key + scopes override ──

@pytest.fixture
def app_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    from app.web.api import create_app
    app = create_app(settings)
    return TestClient(app), db


def _seed_override(conn, section_id, edition_key, url):
    conn.execute(
        "INSERT INTO user_overrides (media_type, tmdb_id, youtube_url,"
        " set_at, set_by, section_id, edition_key) VALUES ('movie',120,?,?,"
        "'testadmin',?,?)", (url, NOW, section_id, edition_key))


def test_replace_tdb_enqueues_download_with_edition_key(app_client):
    """POST replace-with-themerrdb on the Sam rk enqueues a download
    whose payload carries edition_key='sam takes a step' so the chained
    place job lands in the Sam folder/rk."""
    client, db = app_client
    with sqlite3.connect(db) as conn:
        _seed_two_editions(db)
        conn.commit()

    r = client.post(
        f"/api/plex_items/{SAM_RK}/replace-with-themerrdb",
        headers={"X-Authentik-Username": "testadmin"},
    )
    assert r.status_code == 200, r.text

    with sqlite3.connect(db) as conn:
        rows = conn.execute(
            "SELECT payload FROM jobs WHERE job_type='download'"
            " AND tmdb_id=120 AND section_id='1'").fetchall()
    assert len(rows) == 1, rows
    pl = json.loads(rows[0][0])
    assert pl.get("edition_key") == SAM_KEY, pl
    assert pl.get("force_place") is True, pl


def test_replace_tdb_spares_sibling_edition_override(app_client):
    """REPLACE TDB on Sam deletes Sam's own override but leaves the
    Theatrical edition's override intact (per-edition scoping)."""
    client, db = app_client
    with sqlite3.connect(db) as conn:
        _seed_two_editions(db)
        _seed_override(conn, "1", THEAT_KEY,
                       "https://www.youtube.com/watch?v=theat0000001")
        _seed_override(conn, "1", SAM_KEY,
                       "https://www.youtube.com/watch?v=sam000000001")
        conn.commit()

    r = client.post(
        f"/api/plex_items/{SAM_RK}/replace-with-themerrdb",
        headers={"X-Authentik-Username": "testadmin"},
    )
    assert r.status_code == 200, r.text

    with sqlite3.connect(db) as conn:
        remaining = conn.execute(
            "SELECT edition_key FROM user_overrides WHERE tmdb_id=120"
        ).fetchall()
    keys = {row[0] for row in remaining}
    assert keys == {THEAT_KEY}, (
        f"REPLACE TDB on Sam must drop ONLY Sam's override; the "
        f"Theatrical sibling must survive. Remaining: {keys}")
