"""v1.21.72 — ADOPT-FROM-PLEX + RESTORE-CANONICAL edition scope.

Audit-found (MED). Both endpoints joined local_files ⋈ placements on
(media_type, tmdb_id, section_id) only — so on a multi-edition title a
local_files row cross-joined an ARBITRARY sibling edition's placement, and
the per-row UPDATEs (placement_kind / mismatch_state clear) landed on the
wrong edition. Both now carry `AND p.edition_key = lf.edition_key` in the
JOIN and scope their UPDATEs by the row's own edition_key.
"""
from __future__ import annotations

import sqlite3

import pytest
from fastapi.testclient import TestClient

from app.core.auth import create_admin, init_auth_schema
from app.core.db import init_db
from app.core.events import now_iso


NOW = now_iso()


@pytest.fixture
def app_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    settings._cfg.paths.themes_dir = str(tmp_path / "themes")
    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    from app.web.api import create_app
    app = create_app(settings)
    return TestClient(app), db, tmp_path, settings


def _base_rows(conn, tid):
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, is_anime,"
        " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
        " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))


def _lf(conn, ek, rel, mismatch):
    conn.execute(
        "INSERT INTO local_files (media_type, tmdb_id, section_id,"
        " edition_key, file_path, downloaded_at, source_video_id,"
        " provenance, source_kind, file_sha256, file_size, mismatch_state)"
        " VALUES ('movie',120,'1',?,?,?, 'v','auto','themerrdb','x',3,?)",
        (ek, rel, NOW, mismatch))


def _pl(conn, tid, ek, folder, kind):
    conn.execute(
        "INSERT INTO placements (media_type, tmdb_id, section_id, theme_id,"
        " media_folder, placed_at, placement_kind, plex_refreshed,"
        " provenance, edition_key) VALUES ('movie',120,'1',?,?,?,?,1,'auto',?)",
        (tid, str(folder), NOW, kind, ek))


def test_adopt_pairs_each_edition_with_its_own_placement(app_client):
    """Edition A (mismatch) must re-adopt folder A's file — NOT folder B's
    (the cross-join pre-fix paired A's row with B's placement too)."""
    client, db, tmp_path, settings = app_client
    folder_a = tmp_path / "media" / "LotR {edition-A}"
    folder_b = tmp_path / "media" / "LotR {edition-B}"
    for folder, content in ((folder_a, b"AAA-edition-A-bytes"),
                            (folder_b, b"BBB-edition-B-bytes")):
        folder.mkdir(parents=True, exist_ok=True)
        (folder / "theme.mp3").write_bytes(content)
    (tmp_path / "themes" / "movies").mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db) as conn:
        cur = conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, year,"
            " upstream_source, last_seen_sync_at, first_seen_sync_at,"
            " youtube_url) VALUES ('movie',120,'LotR','2001','imdb',?,?,'u')",
            (NOW, NOW))
        tid = cur.lastrowid
        _base_rows(conn, tid)
        _lf(conn, "a", "movies/a.mp3", "pending")   # the mismatch row
        _lf(conn, "b", "movies/b.mp3", None)
        _pl(conn, tid, "a", folder_a, "hardlink")
        _pl(conn, tid, "b", folder_b, "hardlink")
        conn.commit()

    r = client.post("/api/items/movie/120/adopt-from-plex",
                    headers={"X-Authentik-Username": "testadmin"})
    assert r.status_code == 200, r.text

    canon_a = (tmp_path / "themes" / "movies" / "a.mp3").read_bytes()
    assert canon_a == b"AAA-edition-A-bytes", (
        "edition A's canonical must come from folder A, not a sibling")


def test_restore_canonical_placement_kind_scoped_to_edition(app_client):
    """Restoring edition A's missing canonical must not clobber edition B's
    placement_kind (the UPDATE was (mt,tmdb,section)-wide pre-fix)."""
    client, db, tmp_path, settings = app_client
    folder_a = tmp_path / "media" / "LotR {edition-A}"
    folder_a.mkdir(parents=True, exist_ok=True)
    (folder_a / "theme.mp3").write_bytes(b"AAA")
    themes_movies = tmp_path / "themes" / "movies"
    themes_movies.mkdir(parents=True, exist_ok=True)
    # Edition B's canonical EXISTS → B is skipped by restore.
    (themes_movies / "b.mp3").write_bytes(b"BBB")

    with sqlite3.connect(db) as conn:
        cur = conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, year,"
            " upstream_source, last_seen_sync_at, first_seen_sync_at,"
            " youtube_url) VALUES ('movie',120,'LotR','2001','imdb',?,?,'u')",
            (NOW, NOW))
        tid = cur.lastrowid
        _base_rows(conn, tid)
        _lf(conn, "a", "movies/a.mp3", None)   # canonical missing → restored
        _lf(conn, "b", "movies/b.mp3", None)   # canonical present → skipped
        _pl(conn, tid, "a", folder_a, "copy")
        _pl(conn, tid, "b", tmp_path / "media" / "B", "copy")
        conn.commit()

    r = client.post("/api/items/movie/120/restore-canonical",
                    headers={"X-Authentik-Username": "testadmin"})
    assert r.status_code == 200, r.text

    with sqlite3.connect(db) as conn:
        kinds = dict(conn.execute(
            "SELECT edition_key, placement_kind FROM placements"
            " WHERE tmdb_id=120").fetchall())
    # A was restored (copy → hardlink); B must be UNTOUCHED at 'copy'.
    assert kinds["a"] == "hardlink", kinds
    assert kinds["b"] == "copy", (
        f"restoring edition A clobbered edition B's placement_kind: {kinds}")
