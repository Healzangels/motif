"""v1.21.74 — library "downloading" dot scoped to the edition.

the user: DOWNLOAD TDB BACKUP on the Extended edition lit the DL/downloading
indicator on ALL THREE LotR editions. The /api/library two-join's
job_in_flight subquery matched in-flight download/place jobs by
(media_type, tmdb_id, section_id) — but jobs carry no edition_key COLUMN
(it lives in the payload JSON since v1.21.54), so one edition's job lit the
dot on every edition of the title in that section.

job_in_flight now also matches the payload's edition_key (via json_extract,
guarded by json_valid) against pi.edition_key. A job with no edition_key in
payload ('' / legacy) maps to the '' edition.
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
A_RK = "167699"
B_RK = "167709"
A_FOLDER = "/data/Movies/LotR (2001) {edition-Theatrical}"
B_FOLDER = "/data/Movies/LotR (2001) {edition-Extended Edition}"
A_KEY = edition_key_for_folder(A_FOLDER)   # 'theatrical'
B_KEY = edition_key_for_folder(B_FOLDER)   # 'extended edition'


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


def _seed(db, job_edition):
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
        for rk, ek, fp in ((A_RK, A_KEY, A_FOLDER), (B_RK, B_KEY, B_FOLDER)):
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
        # An in-flight download for ONE edition.
        conn.execute(
            "INSERT INTO jobs (job_type, media_type, tmdb_id, section_id,"
            " payload, status, created_at) VALUES ('download','movie',120,"
            "'1',?,'running',?)",
            (json.dumps({"edition_key": job_edition}), NOW))
        conn.commit()


def _rows_by_edition(client):
    r = client.get("/api/library?tab=movies",
                   headers={"X-Authentik-Username": "testadmin"})
    assert r.status_code == 200, r.text
    items = r.json()["items"]
    # The seed DB holds only this title's rows; key by edition_key.
    return {it["edition_key"]: it for it in items}


def test_downloading_dot_only_on_the_downloading_edition(app_client):
    client, db = app_client
    _seed(db, job_edition=B_KEY)   # Extended is downloading

    rows = _rows_by_edition(client)
    assert set(rows) == {A_KEY, B_KEY}, list(rows)
    # Only the Extended edition shows a job in flight.
    assert rows[B_KEY]["job_in_flight"] == "download", rows[B_KEY]
    assert not rows[A_KEY]["job_in_flight"], (
        f"Theatrical lit the downloading dot from Extended's job: "
        f"{rows[A_KEY]['job_in_flight']}")
