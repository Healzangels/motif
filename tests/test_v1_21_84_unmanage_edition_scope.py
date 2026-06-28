"""v1.21.84 — UNMANAGE respects editions (data-loss class).

Same bug the v1.21.83 PURGE fix closed, in the sibling endpoint: editions of
one title can share ONE Plex section, so api_unmanage_item's section-only
DELETEs of local_files / placements / user_overrides + the canonical unlink
destroyed every edition sharing the section. Now it resolves rating_key ->
edition_key and narrows every destructive op to the clicked edition.
"""
from __future__ import annotations

import sqlite3

import pytest
from fastapi.testclient import TestClient

from app.core.auth import create_admin, init_auth_schema
from app.core.db import init_db
from app.core.events import now_iso


NOW = now_iso()
AUTH = {"X-Authentik-Username": "testadmin"}
TMDB = 120
THEAT_RK = "111"
SAM_RK = "222"


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


def _seed(db, settings):
    """Two editions of one title in one section; each with a canonical theme
    file under themes_dir + a local_files row + placement + override."""
    themes_dir = settings.themes_dir
    canon = {}
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
        for rk, ek in ((THEAT_RK, "theatrical"), (SAM_RK, "sam takes a step")):
            rel = f"movies/{ek}.mp3"
            p = themes_dir / rel
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(b"ID3audio")
            canon[ek] = p
            folder = f"/data/Movies/LotR (2001) {{edition-{ek}}}"
            conn.execute(
                "INSERT INTO plex_items (rating_key, section_id, media_type,"
                " theme_id, guid_tmdb, title, year, edition_key, folder_path,"
                " has_theme, local_theme_file, first_seen_at, last_seen_at)"
                " VALUES (?,'1','movie',?,?,'LotR','2001',?,?,1,1,?,?)",
                (rk, tid, TMDB, ek, folder, NOW, NOW))
            conn.execute(
                "INSERT INTO local_files (media_type, tmdb_id, section_id,"
                " edition_key, file_path, downloaded_at, source_video_id,"
                " provenance, source_kind) VALUES ('movie',?,'1',?,?,?,"
                "'v','manual','upload')", (TMDB, ek, rel, NOW))
            conn.execute(
                "INSERT INTO placements (media_type, tmdb_id, section_id,"
                " theme_id, media_folder, placed_at, placement_kind,"
                " plex_refreshed, provenance, edition_key) VALUES ('movie',"
                "?,'1',?,?,?, 'hardlink',1,'manual',?)", (TMDB, tid, folder, NOW, ek))
            conn.execute(
                "INSERT INTO user_overrides (media_type, tmdb_id, youtube_url,"
                " set_at, set_by, section_id, edition_key) VALUES ('movie',"
                "120,?,?,'testadmin','1',?)", (f"u-{ek}", NOW, ek))
        conn.commit()
    return canon


def _counts(db, ek):
    with sqlite3.connect(db) as conn:
        q = lambda t: conn.execute(
            f"SELECT COUNT(*) FROM {t} WHERE media_type='movie' AND tmdb_id=?"
            " AND edition_key=?", (TMDB, ek)).fetchone()[0]
        return q("local_files"), q("placements"), q("user_overrides")


def test_unmanage_one_edition_leaves_siblings(app_client):
    client, db, settings = app_client
    canon = _seed(db, settings)

    r = client.post(
        f"/api/items/movie/{TMDB}/unmanage?section_id=1&rating_key={SAM_RK}",
        headers=AUTH)
    assert r.status_code == 200, r.text

    # Sam unmanaged: rows gone + its canonical unlinked.
    assert _counts(db, "sam takes a step") == (0, 0, 0), "Sam should be unmanaged"
    assert not canon["sam takes a step"].exists(), "Sam's canonical unlinked"

    # Theatrical SURVIVES — rows + canonical intact.
    assert _counts(db, "theatrical") == (1, 1, 1), "Theatrical must survive UNMANAGE"
    assert canon["theatrical"].exists(), "Theatrical's canonical must survive"


def test_v1_21_84_version_pin():
    from pathlib import Path
    init_py = (Path(__file__).resolve().parent.parent / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
