"""v1.21.68 — INFO card (api_item) edition-aware read.

the user: opening the INFO card on different editions of one title showed
IDENTICAL data — every card's tag read "edition: Sam Takes a Step", and
the downloaded / placed-in paths + applied URL were one arbitrary
edition's. Root cause: api_item scoped reads by section only, so on a
multi-edition title it surfaced whichever edition's row sorted first.

api_item now takes the clicked row's rating_key, resolves its edition, and
narrows local_files / placements / override / pending_update / the
{edition-X} label to THAT edition (prefer edition, fall back to shared '').
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
THEAT_RK = "167699"
SAM_RK = "676271"
THEAT_FOLDER = "/data/Movies/LotR (2001) {edition-Theatrical}"
SAM_FOLDER = "/data/Movies/LotR (2001) {edition-Sam Takes a Step}"


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
    theat_key = edition_key_for_folder(THEAT_FOLDER)
    sam_key = edition_key_for_folder(SAM_FOLDER)
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
        for rk, ek, fp, lf, url in (
            (THEAT_RK, theat_key, THEAT_FOLDER, "movies/theat.mp3",
             "https://www.youtube.com/watch?v=theat0000001"),
            (SAM_RK, sam_key, SAM_FOLDER, "movies/sam.mp3",
             "https://www.youtube.com/watch?v=sam000000001"),
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
                "'v','auto','themerrdb')", (ek, lf, NOW))
            conn.execute(
                "INSERT INTO placements (media_type, tmdb_id, section_id,"
                " theme_id, media_folder, placed_at, placement_kind,"
                " plex_refreshed, provenance, edition_key) VALUES ('movie',"
                "120,'1',?,?,?, 'hardlink',1,'auto',?)", (tid, fp, NOW, ek))
            conn.execute(
                "INSERT INTO user_overrides (media_type, tmdb_id, youtube_url,"
                " set_at, set_by, section_id, edition_key) VALUES ('movie',"
                "120,?,?,'testadmin','1',?)", (url, NOW, ek))
        conn.commit()
    return theat_key, sam_key


def _get(client, rk):
    r = client.get(
        f"/api/items/movie/120?section_id=1&rating_key={rk}",
        headers={"X-Authentik-Username": "testadmin"},
    )
    assert r.status_code == 200, r.text
    return r.json()


def test_info_card_scopes_to_clicked_edition(app_client):
    client, db = app_client
    _seed(db)

    theat = _get(client, THEAT_RK)
    sam = _get(client, SAM_RK)

    # Edition tag is the clicked edition's RAW label, not a shared sibling.
    assert theat["section_context"]["edition"] == "Theatrical", \
        theat["section_context"]
    assert sam["section_context"]["edition"] == "Sam Takes a Step", \
        sam["section_context"]

    # downloaded / placed-in / applied-url all belong to the clicked edition.
    assert theat["local_file"]["file_path"] == "movies/theat.mp3"
    assert sam["local_file"]["file_path"] == "movies/sam.mp3"
    assert theat["placements"][0]["media_folder"] == THEAT_FOLDER
    assert sam["placements"][0]["media_folder"] == SAM_FOLDER
    assert theat["override"]["youtube_url"].endswith("theat0000001")
    assert sam["override"]["youtube_url"].endswith("sam000000001")


def test_info_card_without_rating_key_is_legacy(app_client):
    """No rating_key → legacy section-only behavior (unchanged): the card
    still resolves, just without edition narrowing."""
    client, db = app_client
    _seed(db)
    r = client.get(
        "/api/items/movie/120?section_id=1",
        headers={"X-Authentik-Username": "testadmin"},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    # Both editions' placements are visible (section-wide, no narrowing).
    folders = {p["media_folder"] for p in body["placements"]}
    assert folders == {THEAT_FOLDER, SAM_FOLDER}, folders
