"""v1.21.96 (Phase 0) — read-only edition-diagnostics admin endpoint.

Characterizes every multi-edition title so the per-edition theme-independence
architecture is chosen from real on-disk data: distinct {edition-X} folders
(→ per-edition sidecar works, any size) vs a shared physical folder (→ sidecar
inherently shared; per-rk upload only, blocked over Plex's ~10MB ceiling), plus
the historical '' mis-key (placement/local_files at '' while plex_items is
tagged — the user's Watchmen/Godzilla shape).
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
    return TestClient(create_app(s)), s.db_path, tmp_path


def _section(conn):
    conn.execute(
        "INSERT OR IGNORE INTO plex_sections (section_id, title, type, is_anime,"
        " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
        " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))


def _theme(conn, tid, tmdb, title):
    conn.execute(
        "INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source,"
        " last_seen_sync_at, first_seen_sync_at) VALUES (?,'movie',?,?,'imdb',"
        "?,?)", (tid, tmdb, title, NOW, NOW))


def _item(conn, rk, tid, tmdb, title, ek, folder):
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, media_type, theme_id,"
        " guid_tmdb, title, year, edition_key, folder_path, has_theme,"
        " first_seen_at, last_seen_at) VALUES (?, '1','movie',?,?,?,'2000',?,"
        "?,1,?,?)", (rk, tid, tmdb, title, ek, folder, NOW, NOW))


def _placement(conn, tid, tmdb, ek, folder):
    conn.execute(
        "INSERT INTO placements (theme_id, media_type, tmdb_id, section_id,"
        " edition_key, media_folder, placed_at, placement_kind, plex_refreshed)"
        " VALUES (?,'movie',?,'1',?,?,?,'hardlink',1)", (tid, tmdb, ek, folder, NOW))


def _local(conn, tmdb, ek, size):
    conn.execute(
        "INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key,"
        " file_path, file_size, downloaded_at, source_video_id, provenance,"
        " source_kind) VALUES ('movie',?,'1',?,?,?,?, 'v','auto','themerrdb')",
        (tmdb, ek, f"x-{ek or 'std'}.mp3", size, NOW))


def _mk_theme_file(folder: Path, size: int):
    folder.mkdir(parents=True, exist_ok=True)
    with open(folder / "theme.mp3", "wb") as f:
        f.truncate(size)  # sparse — no real disk use


def _seed(db, data):
    movies = data / "Movies"
    # Title A — DISTINCT folders, small themes, properly keyed.
    a_th = movies / "A (2000) {edition-Theatrical}"
    a_ex = movies / "A (2000) {edition-Extended}"
    _mk_theme_file(a_th, 2 * 1024 * 1024)
    _mk_theme_file(a_ex, 2 * 1024 * 1024)
    # Title B — SHARED folder, 11MB theme (over ceiling), '' mis-keyed.
    b_dir = movies / "B (2009)"
    _mk_theme_file(b_dir, 11 * 1024 * 1024)
    with sqlite3.connect(db) as conn:
        _section(conn)
        _theme(conn, 10, 100, "A")
        _item(conn, "a1", 10, 100, "A", "theatrical", str(a_th))
        _item(conn, "a2", 10, 100, "A", "extended", str(a_ex))
        _placement(conn, 10, 100, "theatrical", str(a_th))
        _placement(conn, 10, 100, "extended", str(a_ex))
        _local(conn, 100, "theatrical", 2 * 1024 * 1024)
        _local(conn, 100, "extended", 2 * 1024 * 1024)
        _theme(conn, 20, 200, "B")
        for rk, ek in (("b1", "theatrical cut"), ("b2", "midnight"),
                       ("b3", "director's cut")):
            _item(conn, rk, 20, 200, "B", ek, str(b_dir))   # ALL share b_dir
        _placement(conn, 20, 200, "", str(b_dir))      # mis-keyed at ''
        _local(conn, 200, "", 11 * 1024 * 1024)
        conn.commit()


def _by_title(payload, title):
    return next(t for t in payload["titles"] if t["title"] == title)


def test_diagnostic_characterizes_distinct_and_shared(client):
    c, db, tmp = client
    _seed(db, tmp / "data")
    r = c.get("/api/admin/edition-diagnostics", headers=AUTH)
    assert r.status_code == 200, r.text
    body = r.json()

    a = _by_title(body, "A")
    assert a["folder_verdict"] == "distinct-folders"
    assert a["empty_edition_miskey"] is False
    assert a["theme_over_ceiling"] is False
    assert a["edition_count"] == 2

    b = _by_title(body, "B")
    assert b["folder_verdict"] == "shared-folder", b
    assert b["empty_edition_miskey"] is True, b
    assert b["theme_over_ceiling"] is True, b
    assert b["edition_count"] == 3
    # all three b editions see the one shared theme.mp3
    assert all(e["theme_mp3_present"] for e in b["editions"])


def test_diagnostic_rollup(client):
    c, db, tmp = client
    _seed(db, tmp / "data")
    s = c.get("/api/admin/edition-diagnostics", headers=AUTH).json()["summary"]
    assert s["multi_edition_titles"] == 2
    assert s["distinct_folder_titles"] == 1
    assert s["shared_folder_titles"] == 1
    assert s["empty_miskey_titles"] == 1
    assert s["shared_folder_over_ceiling"] == 1


def test_diagnostic_admin_gated(client):
    c, db, tmp = client
    _seed(db, tmp / "data")
    r = c.get("/api/admin/edition-diagnostics")  # no auth header
    assert r.status_code in (401, 403), r.status_code


def test_diagnostic_runs_off_event_loop():
    """FS stats over /data must run off the event loop (v1.21.20 rule)."""
    src = (Path(__file__).resolve().parent.parent
           / "app" / "web" / "api.py").read_text()
    idx = src.index('"/api/admin/edition-diagnostics"')
    block = src[idx:idx + 12000]
    assert "run_in_threadpool(_run)" in block


def test_v1_21_96_version_pin():
    init_py = (Path(__file__).resolve().parent.parent
               / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
