"""v1.21.78 — UPLOAD MP3 is per-edition (mismatch no longer bleeds).

the user: setting the "Sam Takes a Step" edition with a manual MP3 upload put
ALL THREE LotR editions into the amber !/M (mismatch) state. api_upload_theme
wrote the canonical to the STANDARD ('') folder, keyed the local_files row +
mismatch_state to edition_key='', and detected the existing placement by
(mt, tmdb, section) ignoring edition — so the '' row's mismatch_state bled
onto every edition via the two-join COALESCE(lf_e.mismatch_state,
lf_g.mismatch_state).

The endpoint now resolves the clicked row's edition (from rating_key →
folder) and scopes the canonical folder, the placement/mismatch detection,
the local_files write, the place-job payload, and the backup-only stamp to it.
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
A_RK = "167699"   # Theatrical — placed, NOT uploaded to
B_RK = "676271"   # Sam Takes a Step — the upload target
A_FOLDER = "/data/Movies/LotR (2001) {edition-Theatrical}"
B_FOLDER = "/data/Movies/LotR (2001) {edition-Sam Takes a Step}"
A_KEY = edition_key_for_folder(A_FOLDER)
B_KEY = edition_key_for_folder(B_FOLDER)
MP3 = b"ID3" + b"\x00" * 64   # passes _looks_like_audio


@pytest.fixture
def app_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    settings._cfg.paths.themes_dir = str(tmp_path / "themes")
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
            conn.execute(
                "INSERT INTO local_files (media_type, tmdb_id, section_id,"
                " edition_key, file_path, downloaded_at, source_video_id,"
                " provenance, source_kind) VALUES ('movie',120,'1',?,?,?,"
                "'v','auto','themerrdb')", (ek, f"movies/{ek}.mp3", NOW))
            # Both editions are placed → upload onto one makes IT a mismatch.
            conn.execute(
                "INSERT INTO placements (media_type, tmdb_id, section_id,"
                " theme_id, media_folder, placed_at, placement_kind,"
                " plex_refreshed, provenance, edition_key) VALUES ('movie',"
                "120,'1',?,?,?, 'hardlink',1,'auto',?)", (tid, fp, NOW, ek))
        conn.commit()
    return tid


def _mismatch_by_edition(db):
    with sqlite3.connect(db) as conn:
        return dict(conn.execute(
            "SELECT edition_key, mismatch_state FROM local_files"
            " WHERE tmdb_id=120").fetchall())


def test_upload_mismatch_scoped_to_clicked_edition(app_client):
    client, db = app_client
    _seed(db)

    r = client.post(
        f"/api/plex_items/{B_RK}/upload-theme",
        files={"file": ("theme.mp3", MP3, "audio/mpeg")},
        headers={"X-Authentik-Username": "testadmin"},
    )
    assert r.status_code == 200, r.text

    mm = _mismatch_by_edition(db)
    # ONLY the uploaded edition (Sam) is a mismatch; Theatrical is untouched,
    # and no stray '' row was created to bleed onto the others.
    assert mm.get(B_KEY) == "pending", mm
    assert mm.get(A_KEY) is None, mm
    assert "" not in mm, f"upload created a stray '' local_files row: {mm}"
