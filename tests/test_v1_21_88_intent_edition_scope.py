"""v1.21.88 — PROMOTE TO ACTIVE / MARK AS BACKUP (set_override_intent) respects
editions.

Audit found api_set_override_intent entirely section-scoped (no rating_key): the
user_overrides intent UPDATE, the BK local_files reads/clears, the plex_cloud
placement INSERT (edition_key omitted -> ''), and the force-place enqueue all
ignored the edition. So PROMOTE/MARK-AS-BACKUP on one edition flipped a sibling
edition's intent and force-placed it. Now it resolves rating_key -> edition_key
and narrows every read/write to it.
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
THEAT_RK = "111"
EXT_RK = "222"
THEAT_FOLDER = "/data/Movies/LotR (2001)"
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
            " youtube_url) VALUES ('movie',?,'LotR','2001','imdb',?,?,'u')",
            (TMDB, NOW, NOW))
        tid = cur.lastrowid
        for rk, ek, fp in ((THEAT_RK, "", THEAT_FOLDER),
                           (EXT_RK, "extended", EXT_FOLDER)):
            conn.execute(
                "INSERT INTO plex_items (rating_key, section_id, media_type,"
                " theme_id, guid_tmdb, title, year, edition_key, folder_path,"
                " has_theme, first_seen_at, last_seen_at) VALUES (?,'1',"
                "'movie',?,?,'LotR','2001',?,?,1,?,?)",
                (rk, tid, TMDB, ek, fp, NOW, NOW))
            conn.execute(
                "INSERT INTO local_files (media_type, tmdb_id, section_id,"
                " edition_key, file_path, downloaded_at, source_video_id,"
                " provenance, source_kind) VALUES ('movie',?,'1',?,?,?,"
                "'v','manual','url')", (TMDB, ek, f"movies/{ek or 'std'}.mp3", NOW))
            # both editions' overrides start at intent='backup'.
            conn.execute(
                "INSERT INTO user_overrides (media_type, tmdb_id, youtube_url,"
                " set_at, set_by, section_id, edition_key, intent) VALUES"
                " ('movie',?,?,?,'testadmin','1',?,'backup')",
                (TMDB, f"u-{ek}", NOW, ek))
        conn.commit()
    return tid


def _intent(db, ek):
    with sqlite3.connect(db) as conn:
        return conn.execute(
            "SELECT intent FROM user_overrides WHERE media_type='movie'"
            " AND tmdb_id=? AND edition_key=?", (TMDB, ek)).fetchone()[0]


def test_promote_one_edition_leaves_sibling_intent(app_client):
    client, db = app_client
    _seed(db)
    r = client.post(
        f"/api/items/movie/{TMDB}/intent?section_id=1&rating_key={EXT_RK}",
        headers=AUTH, json={"intent": "replace"})
    assert r.status_code == 200, r.text

    assert _intent(db, "extended") == "replace", "Extended promoted"
    assert _intent(db, "") == "backup", "Theatrical's intent must NOT flip"

    # The force-place job is keyed to Extended.
    with sqlite3.connect(db) as conn:
        payloads = [json.loads(p) for (p,) in conn.execute(
            "SELECT payload FROM jobs WHERE job_type='place' AND tmdb_id=?",
            (TMDB,))]
    assert payloads, "a force-place job should be enqueued"
    assert all(p.get("edition_key") == "extended" for p in payloads), payloads


def test_v1_21_88_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
