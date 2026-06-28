"""v1.21.59 — per-edition theme isolation, C1 dup-proof.

C1 (v1.21.58) used an `OR edition_key=''` fallback in the library joins —
dup-free only while there's <=1 placement per title. v1.21.59 replaces it
with the structural two-join (placements -> p_e + p_g, local_files ->
lf_e + lf_g, read via COALESCE) so it stays dup-free even when an edition
has its OWN placement AND the shared '' placement coexist (the post-C2
shape). This MUST land before C2 writes per-edition placements.

This test pins the exact scenario the OR-fallback could NOT survive: two
placements (one '' standard, one 'extended') for one title. The Extended
row must render ONCE, showing its OWN placement (not the standard's, not
duplicated).
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


NOW = "2026-06-04T00:00:00Z"
AUTH = {"X-Authentik-Username": "testadmin"}


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
    return TestClient(create_app(settings)), settings.db_path


def _seed_mixed_placements(db):
    """One title (tmdb 600), TWO plex_items (standard + Extended), and TWO
    placements: a '' standard one AND an 'extended' one. This is the
    post-C2 shape the OR-fallback would double-match."""
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        conn.execute(
            "INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source,"
            " last_seen_sync_at, first_seen_sync_at, youtube_url)"
            " VALUES (60,'movie',600,'Y','imdb',?,?,'u')", (NOW, NOW))
        for rk, ek, folder in (
            ("rk-s", "", "/data/Movies/Y (2000)"),
            ("rk-x", "extended", "/data/Movies/Y (2000) {edition-Extended}"),
        ):
            conn.execute(
                "INSERT INTO plex_items (rating_key, section_id, media_type,"
                " theme_id, guid_tmdb, title, edition_key, folder_path,"
                " first_seen_at, last_seen_at)"
                " VALUES (?,?, 'movie',60,600,'Y',?,?,?,?)",
                (rk, "1", ek, folder, NOW, NOW))
        for ek, folder in (
            ("", "/data/Movies/Y (2000)"),
            ("extended", "/data/Movies/Y (2000) {edition-Extended}"),
        ):
            conn.execute(
                "INSERT INTO placements (theme_id, media_type, tmdb_id,"
                " section_id, edition_key, media_folder, placed_at,"
                " placement_kind, plex_refreshed, provenance) VALUES"
                " (60,'movie',600,'1',?,?,?,'hardlink',1,'auto')",
                (ek, folder, NOW))
        conn.commit()


def _rows(client, tmdb_id=600):
    r = client.get("/api/library?tab=movies", headers=AUTH)
    assert r.status_code == 200, r.text
    return [row for row in r.json()["items"]
            if row.get("guid_tmdb") == tmdb_id]


def test_mixed_placements_do_not_duplicate(admin_client):
    """Two placements ('' + 'extended') for one title → still exactly 2
    rows (one per edition), NOT 3+ from the Extended row double-matching."""
    client, db = admin_client
    _seed_mixed_placements(db)
    rks = sorted(row["rating_key"] for row in _rows(client))
    assert rks == ["rk-s", "rk-x"], rks  # no duplication


def test_each_edition_shows_its_own_placement(admin_client):
    """The two-join PREFERS the edition's own placement: the Extended row
    shows the Extended folder, the standard row shows the standard folder —
    not swapped, not shared."""
    client, db = admin_client
    _seed_mixed_placements(db)
    by_rk = {row["rating_key"]: row for row in _rows(client)}
    assert by_rk["rk-x"]["media_folder"] == \
        "/data/Movies/Y (2000) {edition-Extended}"
    assert by_rk["rk-s"]["media_folder"] == "/data/Movies/Y (2000)"
