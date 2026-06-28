"""v1.21.69 — PUSH/REPLACE + SWITCH PLACEMENT edition scope.

Audit-found (same class as the REPLACE TDB bug the user hit, and he'd
already exercised both on the LotR title): api_replace_item and
api_switch_placement took no rating_key, so they
  - DELETE FROM placements WHERE (media_type, tmdb_id)  -> ALL editions
  - enqueued place jobs with no edition_key -> the worker's cached_rk
    resolved an arbitrary sibling edition.

Both now accept the clicked row's rating_key, scope every placements
read/DELETE + the api-teardown rk lookup by edition_key, and tag the
enqueued place job payload with edition_key. Absent rating_key = legacy
title/section-wide behavior.
"""
from __future__ import annotations

import json
import sqlite3

import pytest
from fastapi.testclient import TestClient

from app.core.auth import create_admin, init_auth_schema
from app.core.db import init_db
from app.core.editions import edition_key_for_folder
from app.core.events import now_iso


NOW = now_iso()
THEAT_RK = "167699"
SAM_RK = "676271"
THEAT_FOLDER = "/data/Movies/LotR (2001) {edition-Theatrical}"
SAM_FOLDER = "/data/Movies/LotR (2001) {edition-Sam Takes a Step}"
THEAT_KEY = edition_key_for_folder(THEAT_FOLDER)
SAM_KEY = edition_key_for_folder(SAM_FOLDER)


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


def _seed(db):
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        cur = conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, year,"
            " upstream_source, last_seen_sync_at, first_seen_sync_at,"
            " youtube_url) VALUES ('movie',120,'LotR','2001','imdb',?,?,'u')",
            (NOW, NOW))
        tid = cur.lastrowid
        for rk, ek, fp in (
            (THEAT_RK, THEAT_KEY, THEAT_FOLDER),
            (SAM_RK, SAM_KEY, SAM_FOLDER),
        ):
            conn.execute(
                "INSERT INTO plex_items (rating_key, section_id, media_type,"
                " theme_id, guid_tmdb, title, year, edition_key, folder_path,"
                " has_theme, first_seen_at, last_seen_at) VALUES (?,'1',"
                "'movie',?,120,'LotR','2001',?,?,1,?,?)",
                (rk, tid, ek, fp, NOW, NOW))
            conn.execute(
                "INSERT INTO local_files (media_type, tmdb_id, section_id,"
                " edition_key, file_path, downloaded_at, source_video_id,"
                " provenance, source_kind) VALUES ('movie',120,'1',?,?,?,"
                "'v','auto','themerrdb')", (ek, f"movies/{ek}.mp3", NOW))
            conn.execute(
                "INSERT INTO placements (media_type, tmdb_id, section_id,"
                " theme_id, media_folder, placed_at, placement_kind,"
                " plex_refreshed, provenance, edition_key) VALUES ('movie',"
                "120,'1',?,?,?, 'hardlink',1,'auto',?)", (tid, fp, NOW, ek))
        conn.commit()
    return tid


def _place_job_payload(db):
    with sqlite3.connect(db) as conn:
        rows = conn.execute(
            "SELECT payload FROM jobs WHERE job_type='place' AND tmdb_id=120"
        ).fetchall()
    assert len(rows) == 1, rows
    return json.loads(rows[0][0])


def _surviving_placement_keys(db):
    with sqlite3.connect(db) as conn:
        rows = conn.execute(
            "SELECT edition_key FROM placements WHERE tmdb_id=120").fetchall()
    return {r[0] for r in rows}


def test_replace_scopes_to_clicked_edition(app_client):
    client, db = app_client
    _seed(db)
    # kind='file' makes the endpoint DELETE the prior placement before the
    # re-place (the kind-transition path), exercising the edition-scoped
    # DELETE. Plain REPLACE (no kind) skips the DELETE entirely.
    r = client.post(
        f"/api/items/movie/120/replace?rating_key={THEAT_RK}",
        json={"kind": "file"},
        headers={"X-Authentik-Username": "testadmin"},
    )
    assert r.status_code == 200, r.text
    # Only Theatrical's placement was dropped (to be re-placed); Sam survives.
    assert _surviving_placement_keys(db) == {SAM_KEY}
    # The enqueued place job targets the Theatrical edition.
    assert _place_job_payload(db).get("edition_key") == THEAT_KEY


def test_switch_placement_scopes_to_clicked_edition(app_client):
    client, db = app_client
    _seed(db)
    r = client.post(
        f"/api/items/movie/120/switch-placement?rating_key={SAM_RK}",
        headers={"X-Authentik-Username": "testadmin"},
    )
    assert r.status_code == 200, r.text
    # Switching Sam drops only Sam's placement; Theatrical survives.
    assert _surviving_placement_keys(db) == {THEAT_KEY}
    pl = _place_job_payload(db)
    assert pl.get("edition_key") == SAM_KEY
    # hardlink → switch target is 'api'.
    assert pl.get("kind") == "api"
