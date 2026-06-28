"""v1.21.89 — CLEAR override (DELETE /override) respects editions.

Audit found api_clear_override's user_overrides DELETE section-scoped only, so
clearing one edition's override deleted every edition's override sharing the
section. Now it resolves rating_key -> edition_key and narrows the SELECT +
DELETE. (Endpoint-only: no live JS caller uses this DELETE route today, but the
edition bleed is real for any API/programmatic caller.)
"""
from __future__ import annotations

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
        conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, year,"
            " upstream_source, last_seen_sync_at, first_seen_sync_at)"
            " VALUES ('movie',?,'LotR','2001','imdb',?,?)", (TMDB, NOW, NOW))
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type,"
            " guid_tmdb, title, year, edition_key, folder_path, has_theme,"
            " first_seen_at, last_seen_at) VALUES (?,'1','movie',?,'LotR',"
            "'2001','extended',?,1,?,?)", (EXT_RK, TMDB, EXT_FOLDER, NOW, NOW))
        for ek in ("", "extended"):
            conn.execute(
                "INSERT INTO user_overrides (media_type, tmdb_id, youtube_url,"
                " set_at, set_by, section_id, edition_key) VALUES ('movie',"
                "?,?,?,'testadmin','1',?)", (TMDB, f"u-{ek}", NOW, ek))
        conn.commit()


def _has_ovr(db, ek):
    with sqlite3.connect(db) as conn:
        return conn.execute(
            "SELECT COUNT(*) FROM user_overrides WHERE media_type='movie'"
            " AND tmdb_id=? AND edition_key=?", (TMDB, ek)).fetchone()[0] == 1


def test_clear_override_scopes_to_edition(app_client):
    client, db = app_client
    _seed(db)
    r = client.delete(
        f"/api/items/movie/{TMDB}/override?section_id=1&rating_key={EXT_RK}",
        headers=AUTH)
    assert r.status_code == 200, r.text
    assert not _has_ovr(db, "extended"), "Extended's override should be cleared"
    assert _has_ovr(db, ""), "Theatrical's override must survive"


def test_v1_21_89_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
