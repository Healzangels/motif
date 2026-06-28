"""v1.20.10 — the !UPD pill clears on DOWNLOAD SUCCESS, not at click.

Silent-bug audit Finding 2 (the user: "I want the pill to be accurate
states"): DOWNLOAD TDB BACKUP / REPLACE TDB flipped the
new_theme_available pending update to 'accepted' synchronously at click,
before the download ran — so a later download failure left the pill
cleared with no theme actually backed up.

Fix: move the resolution into the worker's post-download success path
(_record_local_file). The pill now clears only once motif actually HOLDS
the TDB theme (local_files written), and stays up if the download fails.
Self-healing: ANY TDB download path (ACCEPT UPDATE / DOWNLOAD TDB BACKUP
/ REPLACE TDB) clears it. The resolver moved
api.py::_resolve_new_theme_pending_update ->
core.sync.resolve_new_theme_pending_update so the worker can call it
without a web->core import.

This file owns the resolver-logic coverage (incl. the v1.20.9 Finding-1
kind-scoping guard, now unit-tested directly against the core function).
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
SYNC_PY = (REPO / "app" / "core" / "sync.py").read_text()
WORKER_PY = (REPO / "app" / "core" / "worker.py").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()
NOW = "2026-05-29T00:00:00"


# ── core resolver logic (sync.resolve_new_theme_pending_update) ──

def _conn(tmp_path):
    from app.core.db import init_db
    db = tmp_path / "m.db"
    init_db(db)
    c = sqlite3.connect(db)
    c.row_factory = sqlite3.Row
    c.execute(
        "INSERT OR IGNORE INTO plex_sections (section_id, title, type, "
        "is_anime, is_4k, themes_subdir, included, discovered_at, "
        "last_seen_at) VALUES ('3','Anime','show',1,0,'anime',1,?,?)",
        (NOW, NOW))
    return c


def test_resolver_clears_pure_new_theme(tmp_path):
    """A section inheriting the global '' new_theme pending gets a
    per-section accepted new_theme_available row → pill clears."""
    from app.core.sync import resolve_new_theme_pending_update
    c = _conn(tmp_path)
    c.execute(
        "INSERT INTO pending_updates (media_type, tmdb_id, section_id, kind, "
        "new_youtube_url, decision, detected_at) "
        "VALUES ('tv',1,'','new_theme_available','u','pending',?)", (NOW,))
    c.commit()
    out = resolve_new_theme_pending_update(
        c, media_type="tv", tmdb_id=1, section_id="3",
        decided_by="auto:download")
    c.commit()
    assert out is True
    row = c.execute("SELECT kind, decision FROM pending_updates "
                    "WHERE tmdb_id=1 AND section_id='3'").fetchone()
    assert tuple(row) == ("new_theme_available", "accepted")


def test_resolver_does_not_clobber_declined_upstream(tmp_path):
    """Finding 1 (v1.20.9) guard, now unit-tested on the core function:
    a coexisting per-section DECLINED upstream change must never be
    flipped by the new_theme resolver."""
    from app.core.sync import resolve_new_theme_pending_update
    c = _conn(tmp_path)
    c.execute(
        "INSERT INTO pending_updates (media_type, tmdb_id, section_id, kind, "
        "new_youtube_url, decision, decision_by, detected_at) "
        "VALUES ('tv',2,'3','upstream_changed','rolled','declined','admin',?)",
        (NOW,))
    c.execute(
        "INSERT INTO pending_updates (media_type, tmdb_id, section_id, kind, "
        "new_youtube_url, decision, detected_at) "
        "VALUES ('tv',2,'','new_theme_available','u','pending',?)", (NOW,))
    c.commit()
    out = resolve_new_theme_pending_update(
        c, media_type="tv", tmdb_id=2, section_id="3",
        decided_by="auto:download")
    c.commit()
    assert out is False
    sec = c.execute("SELECT kind, decision FROM pending_updates "
                    "WHERE tmdb_id=2 AND section_id='3'").fetchone()
    assert tuple(sec) == ("upstream_changed", "declined")


def test_resolver_noop_on_non_pending(tmp_path):
    from app.core.sync import resolve_new_theme_pending_update
    c = _conn(tmp_path)
    c.execute(
        "INSERT INTO pending_updates (media_type, tmdb_id, section_id, kind, "
        "new_youtube_url, decision, detected_at) "
        "VALUES ('tv',3,'3','new_theme_available','u','declined',?)", (NOW,))
    c.commit()
    out = resolve_new_theme_pending_update(
        c, media_type="tv", tmdb_id=3, section_id="3",
        decided_by="auto:download")
    assert out is False


# ── worker hook (source-pin — test_v1_13_81 _record_local_file pattern) ──

def test_worker_record_local_file_resolves_new_theme():
    fn_start = WORKER_PY.index("def _record_local_file(")
    nxt = WORKER_PY.index("\n    def ", fn_start + 1)
    fn = WORKER_PY[fn_start:nxt]
    assert "resolve_new_theme_pending_update(" in fn, (
        "v1.20.10: _record_local_file must resolve the new_theme pill on "
        "download success"
    )
    idx = fn.index("resolve_new_theme_pending_update(")
    assert 'source_kind == "themerrdb"' in fn[idx - 200:idx], (
        "v1.20.10: the resolve must be scoped to TDB downloads — a "
        "user-URL/upload success doesn't mean TDB's theme was taken"
    )


def test_worker_imports_resolver_from_sync():
    assert ("from .sync import run_sync, resolve_new_theme_pending_update"
            in WORKER_PY)


def test_resolver_lives_in_core_not_web():
    assert "def resolve_new_theme_pending_update(" in SYNC_PY
    assert "_resolve_new_theme_pending_update(" not in API_PY, (
        "v1.20.10: api.py must no longer call the resolver synchronously "
        "(the worker resolves on download success)"
    )


# ── endpoint behavioral: click does NOT clear the pill ──

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
    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    return TestClient(create_app(settings)), db


AUTH = {"X-Authentik-Username": "testadmin"}


def test_download_backup_does_not_clear_pill_at_click(admin_client):
    """The click only enqueues — the !UPD pill must STAY pending until the
    worker resolves it on download success (accurate state)."""
    client, db = admin_client
    url = "https://www.youtube.com/watch?v=NEW8001"
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO plex_sections (section_id, title, type, "
            "is_anime, is_4k, themes_subdir, included, discovered_at, "
            "last_seen_at) VALUES ('3','Anime','show',1,0,'anime',1,?,?)",
            (NOW, NOW))
        cur = conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, upstream_source, "
            "last_seen_sync_at, first_seen_sync_at, youtube_url) "
            "VALUES ('tv',8001,'X','imdb',?,?,?)", (NOW, NOW, url))
        tid = cur.lastrowid
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type, "
            "theme_id, guid_tmdb, title, year, has_theme, first_seen_at, "
            "last_seen_at) VALUES ('rk8001','3','show',?,8001,'X',2024,0,?,?)",
            (tid, NOW, NOW))
        conn.execute(
            "INSERT INTO pending_updates (media_type, tmdb_id, section_id, "
            "kind, new_youtube_url, decision, detected_at) "
            "VALUES ('tv',8001,'3','new_theme_available',?,'pending',?)",
            (url, NOW))
        conn.commit()

    r = client.post("/api/items/tv/8001/download-backup?section_id=3",
                    headers=AUTH)
    assert r.status_code == 200, r.text
    with sqlite3.connect(db) as conn:
        dec = conn.execute(
            "SELECT decision FROM pending_updates WHERE tmdb_id=8001 "
            "AND section_id='3'").fetchone()[0]
    assert dec == "pending", (
        "v1.20.10: DOWNLOAD TDB BACKUP must NOT clear the pill at click — "
        "the worker clears it on download success (so a failed backup "
        "leaves the pill up). Got a synchronous clear."
    )


def test_v1_20_10_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
