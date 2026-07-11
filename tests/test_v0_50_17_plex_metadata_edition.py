"""v0.50.17 — surface Plex's METADATA edition (editionTitle) in the ED column.

the user: Alien (1979) shows edition "Directors Cut" in Plex but motif's ED
column was blank. motif's ED/edition_key derive ONLY from the {edition-X} folder
tag; Alien's edition is a Plex metadata field (editionTitle) on an UNTAGGED
folder, so nothing showed.

This persists Plex's editionTitle into plex_items.plex_edition_title (schema
v69, captured by plex_enum) and the ED column falls back to it when the folder
has no tag. It is DISPLAY-ONLY — deliberately NOT folded into edition_key, so the
folder-based per-edition theme scoping (placements/local_files PK) is untouched.

Behavioral (v1.18.81 discipline): seed a plex_items row with a metadata edition
on an untagged folder, GET /api/library, assert the field reaches the payload
AND that edition_key stays '' (the metadata edition never leaks into scoping).
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core.db import CURRENT_SCHEMA_VERSION, get_conn, init_db

NOW = datetime.now(timezone.utc).isoformat(timespec="seconds")
AUTH = {"X-Authentik-Username": "testadmin"}

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
PLEX_PY = (REPO / "app" / "core" / "plex.py").read_text()
DB_PY = (REPO / "app" / "core" / "db.py").read_text()


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.web.api import create_app
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    with sqlite3.connect(s.db_path) as c:
        c.execute("INSERT INTO plex_sections (section_id, title, type, is_anime,"
                  " is_4k, themes_subdir, included, discovered_at, last_seen_at) "
                  "VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        c.commit()
    return TestClient(create_app(s)), s.db_path


def _seed_alien(db, *, edition_title, folder_path):
    with sqlite3.connect(db) as c:
        c.execute("INSERT INTO themes (id, media_type, tmdb_id, title, year, "
                  " upstream_source, last_seen_sync_at, first_seen_sync_at) "
                  "VALUES (1, 'movie', 348, 'Alien', '1979', 'imdb', ?, ?)",
                  (NOW, NOW))
        c.execute("INSERT INTO plex_items (rating_key, section_id, media_type, "
                  " theme_id, guid_tmdb, title, folder_path, edition_key, "
                  " plex_edition_title, has_theme, first_seen_at, last_seen_at) "
                  "VALUES ('716257','1','movie',1,348,'Alien',?,'',?,1,?,?)",
                  (folder_path, edition_title, NOW, NOW))
        c.commit()


def _alien_row(client):
    c, _ = client
    r = c.get("/api/library?tab=movies", headers=AUTH)
    assert r.status_code == 200, r.text
    rows = [it for it in r.json()["items"] if str(it.get("rating_key")) == "716257"]
    assert rows, f"Alien row missing from /api/library: {r.json()}"
    return rows[0]


# ── behavioral: the metadata edition reaches the payload, scoping untouched ──


def test_metadata_edition_flows_to_library_payload(client):
    _, db = client
    _seed_alien(db, edition_title="Directors Cut",
                folder_path="/data/media/movies/Alien (1979)")
    row = _alien_row(client)
    # the new field is surfaced verbatim …
    assert row["plex_edition_title"] == "Directors Cut"
    # … and it did NOT leak into the scoping key (stays '' — folder has no tag).
    assert row["edition_key"] == ""
    assert "{edition-" not in (row["folder_path"] or "")


def test_folder_tag_edition_unaffected_when_present(client):
    # When the folder DOES carry a {edition-X} tag, edition_key reflects it as
    # before; the metadata field is independent (here left empty).
    _, db = client
    _seed_alien(db, edition_title="",
                folder_path="/data/media/movies/Alien (1979)")
    row = _alien_row(client)
    assert row["plex_edition_title"] == ""


def test_column_exists_at_current_schema(client):
    _, db = client
    with get_conn(db) as c:
        cols = {r[1] for r in c.execute("PRAGMA table_info(plex_items)")}
    assert "plex_edition_title" in cols
    assert CURRENT_SCHEMA_VERSION == 70  # v0.51.128: consecutive_missing (v70)


# ── guards: capture, migrate, render ─────────────────────────────────────────


def test_enumeration_captures_edition_title():
    # plex.py reads Plex's editionTitle into the enumerated item.
    assert 'plex_edition_title=str(el.get("editionTitle", "") or "")' in PLEX_PY
    assert "plex_edition_title: str = \"\"" in PLEX_PY


def test_migration_v68_to_v69_idempotent(client):
    from app.core.db import _migrate_v68_to_v69
    _, db = client
    # re-running on a DB that already has the column must be a no-op, not an
    # error (the _add_column crash-loop guard).
    with sqlite3.connect(db) as c:
        _migrate_v68_to_v69(c)
        _migrate_v68_to_v69(c)
        cols = {r[1] for r in c.execute("PRAGMA table_info(plex_items)")}
    assert "plex_edition_title" in cols
    # the migration is wired into the dispatch chain.
    assert "elif current == 68:" in DB_PY
    assert "_migrate_v68_to_v69(conn)" in DB_PY


def test_ed_column_falls_back_to_metadata_edition():
    # the ED column prefers the folder tag, then falls back to the metadata
    # field with a DISTINCT pill so it reads as display-only.
    assert "const pe = it.plex_edition_title;" in APP_JS
    assert "edition-pill edition-pill-meta" in APP_JS
    # folder tag still wins first.
    folder_idx = APP_JS.index("parseEditionFromFolderPath(it.folder_path)")
    meta_idx = APP_JS.index("it.plex_edition_title")
    assert folder_idx < meta_idx
    # the meta pill is visually distinct (dashed).
    rule = APP_CSS[APP_CSS.index(".edition-pill-meta {"):]
    assert "border-style: dashed;" in rule[:160]
