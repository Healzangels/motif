"""v1.23.7 — ↩ restorable ATTN pill (find the post-PURGE zombies).

the user: "is there a way to surface rows that are like this? I don't
think we have a way to filter find rows that are in this state."
The state: PURGE deliberately keeps the previous-URL snapshot
(v1.12.64) and zombies the orphan themes row (v1.17.24) so REVERT
stays one click away — but those rows looked like plain unthemed
rows with no way to sweep them.

New ATTN token 'restore': SQL branch mirrors the has_previous_url
SELECT CASE (snapshot exists AND differs from the current canonical
— the exact gate that surfaces RESTORE in the SOURCE menu). Wired
across every token surface: api valid-set, both JS allowlists, the
chip + its CSS tone, and this behavioral pipe.
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
LIB_HTML = (REPO / "app" / "web" / "templates" / "library.html").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


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


_H = {"X-Authentik-Username": "testadmin"}


def _seed(db: Path) -> None:
    """One post-PURGE zombie (orphan themes row, NULL urls, surviving
    previous_urls snapshot) and one plain unthemed control row."""
    now = now_iso()
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, "
            "  included, is_anime, is_4k, themes_subdir, "
            "  discovered_at, last_seen_at) "
            "VALUES ('2', 'TV Shows', 'show', 1, 0, 0, 'tv', ?, ?)",
            (now, now),
        )
        # the zombie: orphan upstream, urls NULLed by purge.
        cur = conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, title_norm, "
            "  year, upstream_source, youtube_url, youtube_video_id, "
            "  last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('tv', -901, 'Castle Impossible', "
            "  'castle impossible', '2025', 'plex_orphan', NULL, NULL, "
            "  ?, ?)",
            (now, now),
        )
        zombie_tid = cur.lastrowid
        conn.execute(
            "INSERT INTO previous_urls (media_type, tmdb_id, section_id, "
            "  youtube_url, kind, captured_at) "
            "VALUES ('tv', -901, '2', "
            "  'https://www.facebook.com/HGTV/videos/x/705368522162731/', "
            "  'user', ?)",
            (now,),
        )
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type, "
            "  title, title_norm, guid_tmdb, theme_id, has_theme, "
            "  first_seen_at, last_seen_at) "
            "VALUES ('607835', '2', 'show', 'Castle Impossible', "
            "  'castle impossible', -901, ?, 0, ?, ?)",
            (zombie_tid, now, now),
        )
        # control: plain unthemed row, no snapshot.
        cur = conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, title_norm, "
            "  year, upstream_source, youtube_url, "
            "  last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('tv', 7001, 'Plain Show', 'plain show', '2020', "
            "  'themoviedb', 'https://www.youtube.com/watch?v=abcdefghijk', "
            "  ?, ?)",
            (now, now),
        )
        plain_tid = cur.lastrowid
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type, "
            "  title, title_norm, guid_tmdb, theme_id, has_theme, "
            "  first_seen_at, last_seen_at) "
            "VALUES ('700', '2', 'show', 'Plain Show', 'plain show', "
            "  7001, ?, 0, ?, ?)",
            (plain_tid, now, now),
        )
        conn.commit()


def test_restore_pill_filters_to_zombie_rows(app_client):
    client, db = app_client
    _seed(db)
    r = client.get("/api/library?tab=tv&attn_pills=restore", headers=_H)
    assert r.status_code == 200
    titles = [it["plex_title"] for it in r.json()["items"]]
    assert titles == ["Castle Impossible"], (
        "only the snapshot-bearing zombie matches; the plain row "
        "must not"
    )


def test_unfiltered_view_still_shows_both(app_client):
    client, db = app_client
    _seed(db)
    r = client.get("/api/library?tab=tv", headers=_H)
    assert r.status_code == 200
    titles = sorted(it["plex_title"] for it in r.json()["items"])
    assert titles == ["Castle Impossible", "Plain Show"]


def test_restored_row_drops_out_of_filter(app_client):
    """Once the snapshot is re-applied (RESTORE writes the override
    back), the snapshot equals the current canonical → the row must
    leave the filter (the revert-would-be-a-no-op gate)."""
    client, db = app_client
    _seed(db)
    now = now_iso()
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO user_overrides (media_type, tmdb_id, "
            "  section_id, youtube_url, set_at) "
            "VALUES ('tv', -901, '2', "
            "  'https://www.facebook.com/HGTV/videos/x/705368522162731/', "
            "  ?)",
            (now,),
        )
        conn.commit()
    r = client.get("/api/library?tab=tv&attn_pills=restore", headers=_H)
    assert r.status_code == 200
    assert r.json()["items"] == []


def test_chip_and_css_surfaces():
    assert 'data-attn-pill="restore"' in LIB_HTML
    assert "↩" in LIB_HTML
    assert ".attn-pill-restore {" in APP_CSS
