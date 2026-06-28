"""v1.21.93 — LET PLEX SERVE / UNPLACE '' fallback for metadata-only editions.

the user's repro (real library, v1.21.91): LET PLEX SERVE on +P movie editions
(Godzilla Minus One "Minus Color", Watchmen) fired the prompt but did
nothing — the log showed `Unplaced by admin | placements_total: 0`.

Root cause: these are metadata-only editions. Plex stamps an editionTitle so
plex_items.folder_path carries {edition-Minus Color} → edition_key='minus
color' (the ED column shows it), but the placement + local_files were written
UN-TAGGED at edition_key='' (the download/place ran before the edition
plumbing, or auto-enqueued without an edition). The INFO card (api_item,
v1.21.68) bridges the gap with a transitional '' fallback — so it SHOWS
"placed: hardlink". But api_unplace_item (v1.21.67) scoped strictly to the
rk's edition ('minus color') with NO fallback → found 0 placements → no-op.
The card showed a placement LET PLEX SERVE couldn't touch.

Fix: mirror api_item — when the clicked edition has no placement of its own
but a shared '' placement exists, operate on the '' row (consistent across
the worklist SELECT, the DELETE, and the local_files stamp). The regression
guard confirms the fallback does NOT fire when the edition HAS its own
placement (so v1.21.67's no-sibling-bleed contract holds).
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


NOW = "2026-06-04T00:00:00Z"
AUTH = {"X-Authentik-Username": "testadmin"}


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


def _section_and_theme(conn):
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, is_anime,"
        " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
        " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
    conn.execute(
        "INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source,"
        " last_seen_sync_at, first_seen_sync_at) VALUES (80,'movie',800,'G',"
        "'imdb',?,?)", (NOW, NOW))


def _plex_item(conn, rk, ek, folder):
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, media_type, theme_id,"
        " guid_tmdb, title, edition_key, folder_path, has_theme, first_seen_at,"
        " last_seen_at) VALUES (?, '1','movie',80,800,'G',?,?,1,?,?)",
        (rk, ek, folder, NOW, NOW))


def _placement(conn, ek, folder):
    conn.execute(
        "INSERT INTO placements (theme_id, media_type, tmdb_id, section_id,"
        " edition_key, media_folder, placed_at, placement_kind, plex_refreshed)"
        " VALUES (80,'movie',800,'1',?,?,?,'hardlink',1)", (ek, folder, NOW))


def _local_file(conn, ek, reason="placed"):
    conn.execute(
        "INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key,"
        " file_path, downloaded_at, source_video_id, provenance, source_kind,"
        " last_place_attempt_reason) VALUES ('movie',800,'1',?,?,?, 'v','auto',"
        "'themerrdb',?)", (ek, f"g-{ek or 'std'}.mp3", NOW, reason))


def _placements(db):
    with sqlite3.connect(db) as conn:
        return sorted(r[0] for r in conn.execute(
            "SELECT edition_key FROM placements WHERE tmdb_id=800"))


# ── THE fix: metadata edition ('minus color') with only a '' placement ──


def test_unplace_metadata_edition_falls_back_to_empty_placement(client):
    """The plex_items row is tagged 'minus color' (folder_path carries the
    edition tag) but the placement lives un-tagged at ''. LET PLEX SERVE via
    the row's rk must find + remove that '' placement, not no-op."""
    c, db = client
    with sqlite3.connect(db) as conn:
        _section_and_theme(conn)
        _plex_item(conn, "rk-mc", "minus color",
                   "/data/Movies/G (2023) {edition-Minus Color}")
        _placement(conn, "", "/data/Movies/G (2023)")   # un-tagged
        _local_file(conn, "")
        conn.commit()

    r = c.post("/api/items/movie/800/unplace?section_id=1&rating_key=rk-mc",
               headers=AUTH)
    assert r.status_code == 200, r.text
    assert _placements(db) == []          # the '' placement was removed
    with sqlite3.connect(db) as conn:
        reason = conn.execute(
            "SELECT last_place_attempt_reason FROM local_files"
            " WHERE tmdb_id=800 AND edition_key=''").fetchone()[0]
    assert reason == "backup_only"        # the '' local_files row got stamped


# ── Regression: the fallback must NOT fire when the edition owns a placement ──


def test_unplace_edition_with_own_placement_does_not_touch_empty(client):
    """Properly-tagged edition WITH its own placement + a separate '' (standard
    sibling) placement: LET PLEX SERVE on the tagged edition removes ONLY its
    own — v1.21.67's no-sibling-bleed contract holds (no '' fallback)."""
    c, db = client
    with sqlite3.connect(db) as conn:
        _section_and_theme(conn)
        _plex_item(conn, "rk-std", "", "/data/Movies/G (2023)")
        _plex_item(conn, "rk-ext", "extended",
                   "/data/Movies/G (2023) {edition-Extended}")
        _placement(conn, "", "/data/Movies/G (2023)")
        _placement(conn, "extended", "/data/Movies/G (2023) {edition-Extended}")
        _local_file(conn, "")
        _local_file(conn, "extended")
        conn.commit()

    r = c.post("/api/items/movie/800/unplace?section_id=1&rating_key=rk-ext",
               headers=AUTH)
    assert r.status_code == 200, r.text
    assert _placements(db) == [""]        # ONLY the standard '' survives
    with sqlite3.connect(db) as conn:
        reasons = dict(conn.execute(
            "SELECT edition_key, last_place_attempt_reason FROM local_files"
            " WHERE tmdb_id=800"))
    assert reasons["extended"] == "backup_only"
    assert reasons[""] == "placed"        # standard '' local_files untouched


def test_v1_21_93_version_pin():
    init_py = (Path(__file__).resolve().parent.parent
               / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
