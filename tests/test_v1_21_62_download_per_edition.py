"""v1.21.62 — per-edition theme isolation, Phase C2c (download actions).

REDOWNLOAD + DOWNLOAD BACKUP become edition-capable: when the row's
rating_key is sent, the endpoint resolves the edition_key and threads it
into the enqueued download job's payload, which the worker (B2a/B2b) reads
to stage + place the theme into THAT edition's folder. Absent rating_key =
edition_key='' = today's behavior (the JS wiring lands in the C3 pass).
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


NOW = "2026-06-04T00:00:00Z"
AUTH = {"X-Authentik-Username": "testadmin"}


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    return TestClient(create_app(s)), s.db_path


def _seed(db):
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        conn.execute(
            "INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source,"
            " last_seen_sync_at, first_seen_sync_at, youtube_url) VALUES"
            " (90,'movie',900,'W','imdb',?,?,'https://youtube.com/watch?v=x')",
            (NOW, NOW))
        for rk, ek, folder in (
            ("rk-std", "", "/data/Movies/W (2000)"),
            ("rk-ext", "extended", "/data/Movies/W (2000) {edition-Extended}"),
        ):
            conn.execute(
                "INSERT INTO plex_items (rating_key, section_id, media_type,"
                " theme_id, guid_tmdb, title, edition_key, folder_path,"
                " first_seen_at, last_seen_at)"
                " VALUES (?,?, 'movie',90,900,'W',?,?,?,?)",
                (rk, "1", ek, folder, NOW, NOW))
        conn.commit()


def _download_payloads(db, tmdb_id=900):
    with sqlite3.connect(db) as conn:
        return [json.loads(p) for (p,) in conn.execute(
            "SELECT payload FROM jobs WHERE job_type='download' AND tmdb_id=?",
            (tmdb_id,))]


def test_redownload_with_rk_threads_edition_into_payload(client):
    c, db = client
    _seed(db)
    r = c.post("/api/items/movie/900/redownload?section_id=1&rating_key=rk-ext",
               headers=AUTH)
    assert r.status_code == 200, r.text
    payloads = _download_payloads(db)
    assert len(payloads) == 1
    assert payloads[0].get("edition_key") == "extended"


def test_redownload_without_rk_is_legacy(client):
    c, db = client
    _seed(db)
    r = c.post("/api/items/movie/900/redownload?section_id=1", headers=AUTH)
    assert r.status_code == 200, r.text
    payloads = _download_payloads(db)
    assert len(payloads) == 1
    # no edition_key in the payload -> worker defaults to '' (today).
    assert "edition_key" not in payloads[0]


def test_download_backup_with_rk_threads_edition(client):
    c, db = client
    _seed(db)
    r = c.post("/api/items/movie/900/download-backup?section_id=1"
               "&rating_key=rk-ext", headers=AUTH)
    assert r.status_code == 200, r.text
    payloads = _download_payloads(db)
    assert len(payloads) == 1
    assert payloads[0].get("edition_key") == "extended"
    # backup => no auto_place.
    assert payloads[0].get("auto_place") is False
