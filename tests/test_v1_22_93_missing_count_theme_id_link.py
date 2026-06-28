"""v1.22.93 — missing-themes banner + DOWNLOAD MISSING see theme_id links.

Follow-up to v1.22.91 (the user: "if this a fix you recommend lets do
it"). The missing-themes banner count (sql_missing_count) and the
bulk DOWNLOAD MISSING enqueue both joined themes via guid_tmdb only.
Plex collections are guid-less (NULL guid_tmdb) and link to their
TDB record via plex_items.theme_id (the v1.18.2 title_norm resolve),
so the Collections tab's banner always read 0 missing and DOWNLOAD
MISSING enqueued nothing there — even with TDB themes available.
Both queries now accept the theme_id linkage (the worker.py OR
shape); rows that already have a local file stay excluded, so
nothing re-downloads.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core.auth import create_admin, init_auth_schema
from app.core.db import init_db
from app.core.events import now_iso


@pytest.fixture
def app_client(tmp_path, monkeypatch):
    """Mirror of the v1.14.59 behavioral fixture — fresh tmp DB +
    forward-auth TestClient."""
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
    app = create_app(settings)
    return TestClient(app), db


_H = {"X-Authentik-Username": "testadmin"}


def _seed_linked_collections(db: Path) -> None:
    """One guid-less Plex collection linked via theme_id with NO local
    file (the missing one), and one with a file (must stay excluded)."""
    now = now_iso()
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, "
            "  included, is_anime, is_4k, themes_subdir, "
            "  discovered_at, last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 1, 0, 0, 'movies', ?, ?)",
            (now, now),
        )
        cur = conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, title_norm, "
            "  year, upstream_source, youtube_url, "
            "  last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('collection', 11716, 'Addams Family Collection', "
            "  'addams family collection', NULL, 'themoviedb', "
            "  'https://www.youtube.com/watch?v=X6QzbvH-ZNo', ?, ?)",
            (now, now),
        )
        missing_tid = cur.lastrowid
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type, "
            "  title, title_norm, guid_tmdb, theme_id, has_theme, "
            "  first_seen_at, last_seen_at) "
            "VALUES ('262805', '1', 'collection', "
            "  'Addams Family Collection', 'addams family collection', "
            "  NULL, ?, 0, ?, ?)",
            (missing_tid, now, now),
        )
        cur = conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, title_norm, "
            "  year, upstream_source, youtube_url, "
            "  last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('collection', 1565, '28 Days Collection', "
            "  '28 days collection', NULL, 'themoviedb', "
            "  'https://www.youtube.com/watch?v=FRvYglZr_zQ', ?, ?)",
            (now, now),
        )
        themed_tid = cur.lastrowid
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type, "
            "  title, title_norm, guid_tmdb, theme_id, has_theme, "
            "  first_seen_at, last_seen_at) "
            "VALUES ('262806', '1', 'collection', '28 Days Collection', "
            "  '28 days collection', NULL, ?, 1, ?, ?)",
            (themed_tid, now, now),
        )
        # the themed one already has its downloaded backup file.
        conn.execute(
            "INSERT INTO local_files (media_type, tmdb_id, section_id, "
            "  edition_key, file_path, source_video_id, provenance, "
            "  source_kind, downloaded_at) "
            "VALUES ('collection', 1565, '1', '', "
            "  '/data/themes/c/theme.mp3', 'FRvYglZr_zQ', 'auto', "
            "  'themerrdb', ?)",
            (now,),
        )
        conn.commit()


def test_collections_missing_count_sees_theme_id_links(app_client):
    client, db = app_client
    _seed_linked_collections(db)
    r = client.get("/api/library?tab=collections", headers=_H)
    assert r.status_code == 200
    data = r.json()
    assert data["missing_count"] == 1, (
        "v1.22.93: the guid-less theme_id-linked collection with no "
        "local file must count as missing (pre-fix: 0 — the banner "
        "never showed for collections); the one WITH a file must not"
    )


def test_download_missing_enqueues_theme_id_linked(app_client):
    client, db = app_client
    _seed_linked_collections(db)
    r = client.post("/api/library/download-missing",
                    json={"tab": "collections"}, headers=_H)
    assert r.status_code == 200
    assert r.json()["enqueued"] == 1
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        jobs = conn.execute(
            "SELECT media_type, tmdb_id FROM jobs "
            "WHERE job_type = 'download' AND status = 'pending'",
        ).fetchall()
    assert [(j["media_type"], j["tmdb_id"]) for j in jobs] == [
        ("collection", 11716)
    ], (
        "only the file-less linked collection gets a download; the "
        "already-downloaded one is excluded by lf.file_path"
    )


def test_guid_linked_movie_still_counted(app_client):
    """Regression guard: the original guid-only path still counts."""
    client, db = app_client
    now = now_iso()
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, "
            "  included, is_anime, is_4k, themes_subdir, "
            "  discovered_at, last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 1, 0, 0, 'movies', ?, ?)",
            (now, now),
        )
        conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, title_norm, "
            "  year, upstream_source, youtube_url, "
            "  last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('movie', 9001, 'Plain Movie', 'plain movie', "
            "  '2020', 'themoviedb', "
            "  'https://www.youtube.com/watch?v=abcdefghijk', ?, ?)",
            (now, now),
        )
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type, "
            "  title, title_norm, guid_tmdb, theme_id, has_theme, "
            "  first_seen_at, last_seen_at) "
            "VALUES ('900', '1', 'movie', 'Plain Movie', 'plain movie', "
            "  9001, NULL, 0, ?, ?)",
            (now, now),
        )
        conn.commit()
    r = client.get("/api/library?tab=movies", headers=_H)
    assert r.status_code == 200
    assert r.json()["missing_count"] == 1


def test_double_linked_row_counts_once(app_client):
    """A row carrying BOTH linkages (guid + theme_id to the same
    record) must count once — COUNT(DISTINCT pi.rating_key) guards
    the OR fan-out."""
    client, db = app_client
    now = now_iso()
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, "
            "  included, is_anime, is_4k, themes_subdir, "
            "  discovered_at, last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 1, 0, 0, 'movies', ?, ?)",
            (now, now),
        )
        cur = conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, title_norm, "
            "  year, upstream_source, youtube_url, "
            "  last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('movie', 9002, 'Both Links', 'both links', '2021', "
            "  'themoviedb', "
            "  'https://www.youtube.com/watch?v=abcdefghijk', ?, ?)",
            (now, now),
        )
        tid = cur.lastrowid
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type, "
            "  title, title_norm, guid_tmdb, theme_id, has_theme, "
            "  first_seen_at, last_seen_at) "
            "VALUES ('901', '1', 'movie', 'Both Links', 'both links', "
            "  9002, ?, 0, ?, ?)",
            (tid, now, now),
        )
        conn.commit()
    r = client.get("/api/library?tab=movies", headers=_H)
    assert r.status_code == 200
    assert r.json()["missing_count"] == 1
