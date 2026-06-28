"""v1.21.90 — INFO-card theme.mp3 player serves the clicked edition + TV-edition
upload isolation.

Issue 1 (movie): api_item_theme_audio resolved the file by (mt, tmdb, section)
with no edition, so a multi-edition title's INFO player streamed an arbitrary
edition's canonical (the user: the Sam edition played the old T song). Now it
takes rating_key -> edition and prefers that edition's local_files row.

Issue 2 (TV): a diagnostic — confirm a folder-tagged TV/anime edition upload
keys local_files to its own edition (does NOT bleed onto the sibling). If this
passes, a real-world bleed means the editions are Plex-metadata (editionTitle)
editions WITHOUT a {edition-X} folder tag, which motif's folder-tag model
can't currently discriminate.
"""
from __future__ import annotations

import io
import sqlite3

import pytest
from fastapi.testclient import TestClient

from app.core.auth import create_admin, init_auth_schema
from app.core.db import init_db
from app.core.events import now_iso


NOW = now_iso()
AUTH = {"X-Authentik-Username": "testadmin"}


@pytest.fixture
def app_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    (tmp_path / "themes").mkdir(parents=True, exist_ok=True)
    settings._cfg.paths.themes_dir = str(tmp_path / "themes")
    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    from app.web.api import create_app
    return TestClient(create_app(settings)), db, settings


# ── Issue 1: the INFO-card player serves the CLICKED edition ─────────


def test_theme_audio_serves_clicked_edition(app_client):
    client, db, settings = app_client
    themes = settings.themes_dir
    # Two movie editions, distinct canonical files with distinct contents.
    files = {"": b"STD-theme-bytes", "sam takes a step": b"SAM-theme-bytes"}
    for ek, content in files.items():
        rel = f"movies/{ek or 'std'}.mp3"
        p = themes / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(content)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        cur = conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, year,"
            " upstream_source, last_seen_sync_at, first_seen_sync_at)"
            " VALUES ('movie',120,'LotR','2001','imdb',?,?)", (NOW, NOW))
        tid = cur.lastrowid
        for rk, ek, fp in (("111", "", "/d/LotR (2001)"),
                           ("222", "sam takes a step",
                            "/d/LotR (2001) {edition-Sam Takes a Step}")):
            conn.execute(
                "INSERT INTO plex_items (rating_key, section_id, media_type,"
                " theme_id, guid_tmdb, title, year, edition_key, folder_path,"
                " has_theme, first_seen_at, last_seen_at) VALUES (?,'1','movie',"
                "?,120,'LotR','2001',?,?,1,?,?)", (rk, tid, ek, fp, NOW, NOW))
            conn.execute(
                "INSERT INTO local_files (media_type, tmdb_id, section_id,"
                " edition_key, file_path, downloaded_at, source_video_id,"
                " provenance, source_kind) VALUES ('movie',120,'1',?,?,?,'v',"
                "'manual','upload')", (ek, f"movies/{ek or 'std'}.mp3", NOW))
        conn.commit()

    r = client.get("/api/items/movie/120/theme.mp3?section_id=1&rating_key=222",
                   headers=AUTH)
    assert r.status_code == 200, r.text
    assert r.content == b"SAM-theme-bytes", "must serve the Sam edition's file"

    r0 = client.get("/api/items/movie/120/theme.mp3?section_id=1&rating_key=111",
                    headers=AUTH)
    assert r0.content == b"STD-theme-bytes", "must serve the standard edition's file"


# ── Issue 2: folder-tagged TV edition upload stays on its own edition ─


def test_tv_edition_upload_does_not_bleed(app_client):
    client, db, settings = app_client
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('3','Anime','show',1,0,'anime',1,?,?)", (NOW, NOW))
        cur = conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, year,"
            " upstream_source, last_seen_sync_at, first_seen_sync_at)"
            " VALUES ('tv',900,'Yu-Gi-Oh','2000','imdb',?,?)", (NOW, NOW))
        tid = cur.lastrowid
        for rk, ek, fp in (
            ("701", "odex dub", "/d/Anime/Yu-Gi-Oh (2000) {edition-Odex Dub}"),
            ("702", "uncut", "/d/Anime/Yu-Gi-Oh (2000) {edition-Uncut}"),
        ):
            conn.execute(
                "INSERT INTO plex_items (rating_key, section_id, media_type,"
                " theme_id, guid_tmdb, title, year, edition_key, folder_path,"
                " has_theme, first_seen_at, last_seen_at) VALUES (?,'3','show',"
                "?,900,'Yu-Gi-Oh','2000',?,?,0,?,?)", (rk, tid, ek, fp, NOW, NOW))
        conn.commit()

    # Upload an MP3 onto the Odex Dub edition.
    r = client.post(
        "/api/plex_items/701/upload-theme", headers=AUTH,
        files={"file": ("theme.mp3", io.BytesIO(b"ID3" + b"\x00" * 64), "audio/mpeg")})
    assert r.status_code == 200, r.text

    with sqlite3.connect(db) as conn:
        rows = {r[0]: r[1] for r in conn.execute(
            "SELECT edition_key, source_kind FROM local_files"
            " WHERE media_type='tv' AND tmdb_id=900")}
    assert rows.get("odex dub") == "upload", ("Odex Dub gets the upload", rows)
    assert "uncut" not in rows, ("Uncut edition must NOT be impacted", rows)


def test_v1_21_90_version_pin():
    from pathlib import Path
    init_py = (Path(__file__).resolve().parent.parent / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
