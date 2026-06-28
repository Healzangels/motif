"""v1.21.67 — UNPLACE / LET PLEX SERVE physical-unlink edition scope.

the user (real library): LET PLEX SERVE on the Theatrical edition of a
multi-edition title left the "Sam Takes a Step" edition's hardlink
showing as MISSING. Root cause: api_unplace_item resolved the row's
edition (_unplace_edition) and scoped the placements DELETE + local_files
UPDATE by it (v1.21.61), but the `placements` SELECT that drives the
PHYSICAL theme.mp3 unlink + Plex teardown was NOT edition-scoped — it
pulled every edition's placement for the section. So the unlink loop
deleted the sibling edition's theme.mp3 from disk while the scoped DELETE
left its placements row intact, leaving it pointing at a now-missing file.

This pins: UNPLACE on one edition's rating_key unlinks ONLY that edition's
file and DB row; sibling editions are untouched.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core.auth import create_admin, init_auth_schema
from app.core.db import init_db
from app.core.editions import edition_key_for_folder
from app.core.events import now_iso


NOW = now_iso()
THEAT_RK = "167699"
SAM_RK = "676271"


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
    return TestClient(app), db, tmp_path


def _seed(db, theat_folder, sam_folder):
    theat_key = edition_key_for_folder(str(theat_folder))
    sam_key = edition_key_for_folder(str(sam_folder))
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
            (THEAT_RK, theat_key, str(theat_folder)),
            (SAM_RK, sam_key, str(sam_folder)),
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
                "'v','auto','themerrdb')",
                (ek, f"movies/{ek or 'std'}.mp3", NOW))
            conn.execute(
                "INSERT INTO placements (media_type, tmdb_id, section_id,"
                " theme_id, media_folder, placed_at, placement_kind,"
                " plex_refreshed, provenance, edition_key) VALUES ('movie',"
                "120,'1',?,?,?, 'hardlink',1,'auto',?)",
                (tid, str(fp), NOW, ek))
        conn.commit()
    return theat_key, sam_key


def test_unplace_one_edition_spares_sibling_file_and_row(app_client):
    client, db, tmp_path = app_client
    theat_folder = tmp_path / "movies" / "LotR (2001) {edition-Theatrical}"
    sam_folder = tmp_path / "movies" / "LotR (2001) {edition-Sam Takes a Step}"
    for folder in (theat_folder, sam_folder):
        folder.mkdir(parents=True, exist_ok=True)
        (folder / "theme.mp3").write_bytes(b"ID3" + b"\x00" * 16)
    theat_key, sam_key = _seed(db, theat_folder, sam_folder)

    # LET PLEX SERVE on the Theatrical edition (sends its rating_key).
    r = client.post(
        f"/api/items/movie/120/unplace?section_id=1&rating_key={THEAT_RK}",
        headers={"X-Authentik-Username": "testadmin"},
    )
    assert r.status_code == 200, r.text

    # Theatrical's file + placement row are gone...
    assert not (theat_folder / "theme.mp3").exists(), \
        "Theatrical's theme.mp3 should be unlinked"
    # ...but Sam's file + placement row MUST survive.
    assert (sam_folder / "theme.mp3").exists(), \
        "v1.21.67 regression: UNPLACE on Theatrical deleted Sam's HL file"
    with sqlite3.connect(db) as conn:
        rows = conn.execute(
            "SELECT edition_key FROM placements WHERE tmdb_id=120"
        ).fetchall()
    keys = {row[0] for row in rows}
    assert keys == {sam_key}, (
        f"Only the Theatrical placement row should be deleted; Sam's "
        f"must survive. Remaining placement edition_keys: {keys}")
