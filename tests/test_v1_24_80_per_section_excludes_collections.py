"""v1.24.80 — PER-SECTION COVERAGE excludes collection items.

Collections live as media_type='collection' plex_items inside the movie/show
Plex sections, but they have their own tab AND their own synthetic Collections
row in the coverage payload. Counting them in the per-section movie/show/anime
totals inflated each section TOTAL (so it didn't match the /movies|/tv|/anime tab
the row drills into) AND double-counted them against the Collections row. The
per-section query now excludes media_type='collection'.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

AUTH = {"X-Authentik-Username": "testadmin"}
NOW = "2026-01-01T00:00:00Z"


@pytest.fixture
def admin_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="testadmin", password="testpassword")
    conn = sqlite3.connect(settings.db_path)
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, included, is_4k, "
        "is_anime, discovered_at, last_seen_at) "
        "VALUES ('1','Movies','movie',1,0,0,?,?)", (NOW, NOW))
    # one real movie + one collection item, both in the Movies section
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, media_type, title, year,"
        " guid_tmdb, theme_id, local_theme_file, has_theme, plex_theme_verified_ok,"
        " first_seen_at, last_seen_at) "
        "VALUES ('m1','1','movie','A Movie','2024',101,NULL,0,0,NULL,?,?)",
        (NOW, NOW))
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, media_type, title, year,"
        " guid_tmdb, theme_id, local_theme_file, has_theme, plex_theme_verified_ok,"
        " first_seen_at, last_seen_at) "
        "VALUES ('c1','1','collection','A Collection','2024',201,NULL,0,0,NULL,?,?)",
        (NOW, NOW))
    conn.commit()
    conn.close()
    return TestClient(create_app(settings))


def test_per_section_total_excludes_collections(admin_client):
    data = admin_client.get("/api/sections/coverage", headers=AUTH).json()
    secs = {s["title"]: s for s in data["sections"]}
    # the Movies section counts only the movie item (not the collection).
    assert secs["Movies"]["total"] == 1, secs["Movies"]
    # collections still surface in their own row, counted once.
    assert "Collections" in secs, list(secs)
    assert secs["Collections"]["total"] == 1
    assert secs["Collections"]["tab"] == "collections"
