"""v1.21.79 — KEEP MISMATCH scoped to the clicked edition (+ section).

api_keep_mismatch acked mismatch_state by (media_type, tmdb_id) only — not
section, not edition. So KEEP MISMATCH on one edition acked EVERY edition's
(and every section's) pending mismatch for the title. It now takes the row's
rating_key, resolves its section + edition, and scopes the ack to that row.
"""
from __future__ import annotations

import sqlite3

import pytest
from fastapi.testclient import TestClient

from app.core.auth import create_admin, init_auth_schema
from app.core.db import init_db
from app.core.editions import edition_key_for_folder
from app.core.events import now_iso


NOW = now_iso()
A_RK = "167699"
B_RK = "676271"
A_FOLDER = "/data/Movies/LotR (2001) {edition-Theatrical}"
B_FOLDER = "/data/Movies/LotR (2001) {edition-Sam Takes a Step}"
A_KEY = edition_key_for_folder(A_FOLDER)
B_KEY = edition_key_for_folder(B_FOLDER)


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
        for rk, ek, fp in ((A_RK, A_KEY, A_FOLDER), (B_RK, B_KEY, B_FOLDER)):
            conn.execute(
                "INSERT INTO plex_items (rating_key, section_id, media_type,"
                " theme_id, guid_tmdb, title, year, edition_key, folder_path,"
                " has_theme, first_seen_at, last_seen_at) VALUES (?,'1',"
                "'movie',?,120,'LotR','2001',?,?,1,?,?)",
                (rk, tid, ek, fp, NOW, NOW))
            # Both editions are in a pending mismatch.
            conn.execute(
                "INSERT INTO local_files (media_type, tmdb_id, section_id,"
                " edition_key, file_path, downloaded_at, source_video_id,"
                " provenance, source_kind, mismatch_state) VALUES ('movie',"
                "120,'1',?,?,?, 'v','auto','themerrdb','pending')",
                (ek, f"movies/{ek}.mp3", NOW))
        conn.commit()


def _states(db):
    with sqlite3.connect(db) as conn:
        return dict(conn.execute(
            "SELECT edition_key, mismatch_state FROM local_files"
            " WHERE tmdb_id=120").fetchall())


def test_keep_mismatch_acks_only_clicked_edition(app_client):
    client, db = app_client
    _seed(db)
    r = client.post(
        f"/api/items/movie/120/keep-mismatch?rating_key={A_RK}",
        headers={"X-Authentik-Username": "testadmin"},
    )
    assert r.status_code == 200, r.text

    st = _states(db)
    assert st[A_KEY] == "acked", st
    assert st[B_KEY] == "pending", (
        f"KEEP MISMATCH on Theatrical acked Sam's mismatch too: {st}")
