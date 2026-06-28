"""v1.21.85 — bulk PUSH + bulk DOWNLOAD respect editions.

Audit found both bulk paths dropped the row's edition for multi-edition titles:
- bulk DOWNLOAD / DOWNLOAD&REPLACE (/api/library/download-batch) omitted
  rating_key, so _enqueue_download defaulted edition_key='' and staged/placed
  the STANDARD edition's content for a tagged-edition selection.
- bulk PUSH TO PLEX (app.js) dropped it.rating_key, so the per-row /replace
  ran title-wide and could place into a sibling edition's folder.

Now download-batch resolves per-item rating_key -> edition_key into the
download job; bulk PUSH threads rating_key into /replace (which is already
edition-aware, v1.21.69).
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core.auth import create_admin, init_auth_schema
from app.core.db import init_db
from app.core.events import now_iso


REPO = Path(__file__).resolve().parent.parent
NOW = now_iso()
AUTH = {"X-Authentik-Username": "testadmin"}
TMDB = 120
EXT_RK = "222"
EXT_FOLDER = "/data/Movies/LotR (2001) {edition-Extended}"


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
    return TestClient(create_app(settings)), db


def _seed(db):
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        cur = conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, year,"
            " upstream_source, last_seen_sync_at, first_seen_sync_at,"
            " youtube_url) VALUES ('movie',?,'LotR','2001','imdb',?,?,"
            "'https://www.youtube.com/watch?v=tdburl00001')", (TMDB, NOW, NOW))
        tid = cur.lastrowid
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type,"
            " theme_id, guid_tmdb, title, year, edition_key, folder_path,"
            " has_theme, first_seen_at, last_seen_at) VALUES (?,'1','movie',"
            "?,?,'LotR','2001','extended',?,1,?,?)",
            (EXT_RK, tid, TMDB, EXT_FOLDER, NOW, NOW))
        conn.commit()


# ── behavioral: download-batch threads the per-item edition ──────────


def test_download_batch_enqueues_with_edition(app_client):
    client, db = app_client
    _seed(db)
    r = client.post("/api/library/download-batch", headers=AUTH, json={
        "items": [{"media_type": "movie", "tmdb_id": TMDB,
                   "section_id": "1", "rating_key": EXT_RK}]})
    assert r.status_code == 200, r.text
    with sqlite3.connect(db) as conn:
        payloads = [json.loads(p) for (p,) in conn.execute(
            "SELECT payload FROM jobs WHERE job_type='download' AND tmdb_id=?",
            (TMDB,))]
    assert payloads, "a download job should have been enqueued"
    assert payloads[0].get("edition_key") == "extended", payloads


# ── source lint: the bulk JS paths carry rating_key ─────────────────


def test_bulk_push_threads_rating_key():
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # the bulk-PUSH candidate carries rk and the /replace call appends it.
    anchor = js.index("Push ${candidates.length}")
    block = js[anchor - 2500:anchor + 2500]
    assert "rk: it.rating_key" in block, "bulk PUSH candidate must carry rating_key"
    assert "rating_key=${encodeURIComponent(c.rk)}" in block, (
        "bulk PUSH /replace call must append rating_key")


def test_bulk_download_items_carry_rating_key():
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # both download-batch item builders include rating_key: it.rating_key.
    assert js.count("rating_key: it.rating_key,") >= 2, (
        "both /download-batch item builders must include rating_key")


def test_v1_21_85_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
