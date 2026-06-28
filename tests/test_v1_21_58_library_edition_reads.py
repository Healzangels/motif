"""v1.21.58 — per-edition theme isolation, Phase C1 (read narrowing).

The /api/library row query now scopes its placements + local_files joins by
edition: prefer THIS edition's row, fall back to the shared '' row. Each
plex_items row IS one edition, so an edition's own placement shows ONLY on
that edition's row instead of bleeding to every sibling (the read side of
the user's LET-PLEX-SERVE bug). Dup-free (<=1 placement per title today; the
structural two-join dup-proof lands before C2 writes per-edition rows) and
behavior-preserving where everything is still '' .

These tests exercise the real endpoint (v1.18.81 rule), not source text.
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


def _seed_two_editions(db):
    """One movie (tmdb 500) as TWO plex_items: standard + Extended. Only the
    Extended edition has its own placement; the download ('' local_files) is
    shared (not yet re-keyed). This is the post-B / pre-C2 transition shape."""
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        conn.execute(
            "INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source,"
            " last_seen_sync_at, first_seen_sync_at, youtube_url)"
            " VALUES (50,'movie',500,'X','imdb',?,?,'u')", (NOW, NOW))
        for rk, ek, folder in (
            ("rk-std", "", "/data/Movies/X (2000)"),
            ("rk-ext", "extended", "/data/Movies/X (2000) {edition-Extended}"),
        ):
            conn.execute(
                "INSERT INTO plex_items (rating_key, section_id, media_type,"
                " theme_id, guid_tmdb, title, edition_key, has_theme,"
                " local_theme_file, folder_path, first_seen_at, last_seen_at)"
                " VALUES (?,?, 'movie',50,500,'X',?,0,0,?,?,?)",
                (rk, "1", ek, folder, NOW, NOW))
        # Placement ONLY for the Extended edition.
        conn.execute(
            "INSERT INTO placements (theme_id, media_type, tmdb_id, section_id,"
            " edition_key, media_folder, placed_at, placement_kind,"
            " plex_refreshed, provenance) VALUES (50,'movie',500,'1','extended',"
            " '/data/Movies/X (2000) {edition-Extended}',?,'hardlink',1,'auto')",
            (NOW,))
        # Shared '' download (the historical single theme).
        conn.execute(
            "INSERT INTO local_files (media_type, tmdb_id, section_id,"
            " edition_key, file_path, downloaded_at, source_video_id,"
            " provenance, source_kind) VALUES ('movie',500,'1','','x.mp3',?,"
            "'vid','auto','themerrdb')", (NOW,))
        conn.commit()


def _rows_for(client, tmdb_id=500):
    r = client.get("/api/library?tab=movies", headers=AUTH)
    assert r.status_code == 200, r.text
    return [row for row in r.json()["items"]
            if row.get("guid_tmdb") == tmdb_id]


def test_two_editions_render_without_duplication(admin_client):
    client, db = admin_client
    _seed_two_editions(db)
    rows = _rows_for(client)
    rks = [row["rating_key"] for row in rows]
    assert sorted(rks) == ["rk-ext", "rk-std"], rks  # exactly 2, no dup


def test_edition_placement_shows_only_on_its_own_row(admin_client):
    """The Extended placement appears on rk-ext (placed) but NOT on rk-std
    (which has no '' placement -> unplaced). Pre-C1 the 3-tuple join put the
    one placement on BOTH editions — the user's bleed bug, read side."""
    client, db = admin_client
    _seed_two_editions(db)
    by_rk = {row["rating_key"]: row for row in _rows_for(client)}
    ext, std = by_rk["rk-ext"], by_rk["rk-std"]
    assert ext.get("media_folder"), "Extended row must show its own placement"
    assert not std.get("media_folder"), (
        "standard row must NOT borrow the Extended placement (the read-side "
        "bleed fix)")


def test_edition_key_exposed_and_shared_download_falls_back(admin_client):
    client, db = admin_client
    _seed_two_editions(db)
    by_rk = {row["rating_key"]: row for row in _rows_for(client)}
    # edition_key is exposed per row.
    assert by_rk["rk-std"].get("edition_key") == ""
    assert by_rk["rk-ext"].get("edition_key") == "extended"
    # the shared '' download is visible on BOTH editions via the fallback.
    assert by_rk["rk-std"].get("file_path")
    assert by_rk["rk-ext"].get("file_path")
