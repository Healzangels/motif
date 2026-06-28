"""v1.21.60 — per-edition theme isolation, Phase C2 (SET URL goes live).

The first per-row action to become edition-aware — and the user's exact bug:
SET URL on one edition bled to all. api_manual_url now resolves the
edition_key from the row's rating_key (its plex_items.folder_path) and
writes the user_overrides row keyed to THAT edition; the /api/library
applied_youtube_url COALESCE gained an edition tier (section+edition ->
section+'' -> global -> TDB) so the override surfaces only on its edition.

Exercises the real endpoint + the real read (v1.18.81). A standard row
(edition_key='') behaves exactly as before.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


NOW = "2026-06-04T00:00:00Z"
AUTH = {"X-Authentik-Username": "testadmin"}
# The endpoint canonicalizes youtube URLs to the www. form.
WWW = "https://www.youtube.com/watch?v="
TDB = "https://youtube.com/watch?v=TDBTDBTDBTD"  # seeded raw into themes


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
            " last_seen_sync_at, first_seen_sync_at, youtube_url)"
            " VALUES (70,'movie',700,'Z','imdb',?,?,"
            "'https://youtube.com/watch?v=TDBTDBTDBTD')", (NOW, NOW))
        for rk, ek, folder in (
            ("rk-std", "", "/data/Movies/Z (2000)"),
            ("rk-ext", "extended", "/data/Movies/Z (2000) {edition-Extended}"),
        ):
            conn.execute(
                "INSERT INTO plex_items (rating_key, section_id, media_type,"
                " theme_id, guid_tmdb, title, edition_key, folder_path,"
                " first_seen_at, last_seen_at)"
                " VALUES (?,?, 'movie',70,700,'Z',?,?,?,?)",
                (rk, "1", ek, folder, NOW, NOW))
        conn.commit()


def _applied(client, rk, tmdb_id=700):
    rows = client.get("/api/library?tab=movies", headers=AUTH).json()["items"]
    for row in rows:
        if row["rating_key"] == rk:
            return row["applied_youtube_url"]
    raise AssertionError(f"row {rk} not found")


def test_set_url_on_tagged_edition_does_not_bleed(client):
    """THE bug fix: SET URL on the Extended (tagged) edition keys the
    override to 'extended' — it shows ONLY on Extended; the standard row
    falls through to TDB. Pre-edition this override hit the shared
    (mt,tmdb,section) row and bled to every edition."""
    c, db = client
    _seed(db)
    r = c.post("/api/plex_items/rk-ext/manual-url", headers=AUTH,
               json={"youtube_url": "https://youtube.com/watch?v=EXTEXTEXTEX"})
    assert r.status_code == 200, r.text
    assert _applied(c, "rk-ext") == WWW + "EXTEXTEXTEX"
    assert _applied(c, "rk-std") == TDB  # no bleed to the standard edition
    with sqlite3.connect(db) as conn:
        keys = [r[0] for r in conn.execute(
            "SELECT edition_key FROM user_overrides WHERE tmdb_id=700")]
    assert keys == ["extended"]


def test_standard_override_is_the_section_default(client):
    """SET URL on the untagged STANDARD edition (edition_key='') writes the
    section's shared default — tagged editions WITHOUT their own override
    inherit it via the read's '' fallback tier. This is the intended two-
    tier 'fall back to shared until each gets its own' behavior."""
    c, db = client
    _seed(db)
    r = c.post("/api/plex_items/rk-std/manual-url", headers=AUTH,
               json={"youtube_url": "https://youtube.com/watch?v=STDSTDSTDST"})
    assert r.status_code == 200, r.text
    assert _applied(c, "rk-std") == WWW + "STDSTDSTDST"
    # Extended has no override of its own -> inherits the '' default.
    assert _applied(c, "rk-ext") == WWW + "STDSTDSTDST"
    with sqlite3.connect(db) as conn:
        ek = conn.execute(
            "SELECT edition_key FROM user_overrides WHERE tmdb_id=700"
        ).fetchone()[0]
    assert ek == ""


def test_two_editions_hold_independent_urls(client):
    """The payoff: once each edition has its OWN URL, they're independent —
    Extended's own override wins over the inherited '' default."""
    c, db = client
    _seed(db)
    c.post("/api/plex_items/rk-std/manual-url", headers=AUTH,
           json={"youtube_url": "https://youtube.com/watch?v=STDSTDSTDST"})
    c.post("/api/plex_items/rk-ext/manual-url", headers=AUTH,
           json={"youtube_url": "https://youtube.com/watch?v=EXTEXTEXTEX"})
    assert _applied(c, "rk-std") == WWW + "STDSTDSTDST"
    assert _applied(c, "rk-ext") == WWW + "EXTEXTEXTEX"
