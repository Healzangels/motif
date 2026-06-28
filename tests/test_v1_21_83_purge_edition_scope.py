"""v1.21.83 — PURGE / FORGET respects editions (data-loss class).

the user's repro: three LotR editions (Theatrical / Extended / Sam Takes a Step)
all live in ONE Plex section. RE-DOWNLOAD on Sam left it stuck; he hit PURGE to
reset — and PURGE destroyed ALL THREE editions (audit: placements_unlinked: 2).

api_forget_item scoped its placement / local_files queries + DELETEs + the
plex_items has_theme/local_theme_file flag-clearing by (media_type, tmdb_id,
section_id) only. Since all editions share the section, section scope can't
isolate them. Now it threads rating_key -> edition_key (v1.21.66 chokepoint)
so PURGE on one edition leaves the siblings' files, rows, and flags intact.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

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
    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    from app.web.api import create_app
    return TestClient(create_app(settings)), db, tmp_path


def _seed(db, tmp_path):
    """Two editions of one title in one section, each with its own theme
    file + placement + local_files row."""
    # folder_path MUST carry the {edition-X} tag — that's what the backend
    # resolves rating_key -> edition_key from (edition_key_for_folder).
    theat_dir = tmp_path / "LotR (2001) {edition-Theatrical}"
    sam_dir = tmp_path / "LotR (2001) {edition-Sam Takes a Step}"
    for d in (theat_dir, sam_dir):
        d.mkdir(parents=True, exist_ok=True)
        (d / "theme.mp3").write_bytes(b"ID3audio")
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
        for rk, ek, mf in (
            (THEAT_RK, "theatrical", str(theat_dir)),
            (SAM_RK, "sam takes a step", str(sam_dir)),
        ):
            conn.execute(
                "INSERT INTO plex_items (rating_key, section_id, media_type,"
                " theme_id, guid_tmdb, title, year, edition_key, folder_path,"
                " has_theme, local_theme_file, first_seen_at, last_seen_at)"
                " VALUES (?,'1','movie',?,?,'LotR','2001',?,?,1,1,?,?)",
                (rk, tid, TMDB, ek, mf, NOW, NOW))
            conn.execute(
                "INSERT INTO local_files (media_type, tmdb_id, section_id,"
                " edition_key, file_path, downloaded_at, source_video_id,"
                " provenance, source_kind) VALUES ('movie',?,'1',?,?,?,"
                "'v','auto','themerrdb')", (TMDB, ek, f"movies/{ek}.mp3", NOW))
            conn.execute(
                "INSERT INTO placements (media_type, tmdb_id, section_id,"
                " theme_id, media_folder, placed_at, placement_kind,"
                " plex_refreshed, provenance, edition_key) VALUES ('movie',"
                "?,'1',?,?,?, 'hardlink',1,'auto',?)", (TMDB, tid, mf, NOW, ek))
        conn.commit()
    return theat_dir, sam_dir


def _counts(db, edition_key):
    with sqlite3.connect(db) as conn:
        p = conn.execute(
            "SELECT COUNT(*) FROM placements WHERE media_type='movie'"
            " AND tmdb_id=? AND edition_key=?", (TMDB, edition_key)).fetchone()[0]
        lf = conn.execute(
            "SELECT COUNT(*) FROM local_files WHERE media_type='movie'"
            " AND tmdb_id=? AND edition_key=?", (TMDB, edition_key)).fetchone()[0]
    return p, lf


def _plex_item(db, rk):
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        return conn.execute(
            "SELECT has_theme, local_theme_file FROM plex_items"
            " WHERE rating_key=?", (rk,)).fetchone()


def test_purge_one_edition_leaves_siblings(app_client):
    client, db, tmp_path = app_client
    theat_dir, sam_dir = _seed(db, tmp_path)

    r = client.post(
        f"/api/items/movie/{TMDB}/forget?section_id=1&rating_key={SAM_RK}",
        headers=AUTH)
    assert r.status_code == 200, r.text

    # Sam's rows + file are gone.
    assert _counts(db, "sam takes a step") == (0, 0), "Sam should be purged"
    assert not (sam_dir / "theme.mp3").exists(), "Sam's theme file unlinked"

    # Theatrical SURVIVES — rows, file, and flags all intact.
    assert _counts(db, "theatrical") == (1, 1), "Theatrical must survive PURGE"
    assert (theat_dir / "theme.mp3").exists(), "Theatrical's file must survive"
    theat = _plex_item(db, THEAT_RK)
    assert theat["has_theme"] == 1 and theat["local_theme_file"] == 1, (
        "Theatrical's plex_items flags must NOT be cleared", dict(theat))


def test_purge_reports_only_the_edition_placement(app_client):
    """The audit/response count reflects ONE edition's placement, not both."""
    client, db, tmp_path = app_client
    _seed(db, tmp_path)
    r = client.post(
        f"/api/items/movie/{TMDB}/forget?section_id=1&rating_key={SAM_RK}",
        headers=AUTH)
    assert r.json().get("placements_unlinked") == 1, r.json()


def test_v1_21_83_version_pin():
    init_py = (Path(__file__).resolve().parent.parent / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
