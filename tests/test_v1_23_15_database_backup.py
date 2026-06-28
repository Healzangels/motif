"""v1.23.15 — database backup settings feature (tag 1: manual).

the user: "create a new settings feature to backup the database." This
tag adds consistent VACUUM INTO snapshots of motif.db to
/config/backups, with admin endpoints to create / list / download /
delete and a // DATABASE settings card.

Covers the db_backup module (snapshot is a valid DB with the same
data; filename validation blocks traversal), the four admin
endpoints (incl. the 403 gate + the path-traversal rejection on
download/delete), and the settings UI + SSR drift sites.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core import db_backup
from app.core.auth import create_admin, init_auth_schema
from app.core.db import init_db


REPO = Path(__file__).resolve().parent.parent


# ── module: db_backup ────────────────────────────────────────


def _seed_db(path: Path) -> None:
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        conn.executemany("INSERT INTO t (v) VALUES (?)",
                         [("alpha",), ("beta",), ("gamma",)])
        conn.commit()


def test_create_backup_is_a_valid_consistent_copy(tmp_path):
    db = tmp_path / "motif.db"
    _seed_db(db)
    bf = db_backup.create_backup(db, tmp_path, now_stamp="20260612-143000")
    assert bf.name == "motif-20260612-143000.db"
    assert bf.created_at == "2026-06-12T14:30:00+00:00"
    dest = db_backup.backups_dir(tmp_path) / bf.name
    assert dest.is_file()
    # The snapshot opens as a real DB and carries the same rows.
    with sqlite3.connect(dest) as conn:
        rows = [r[0] for r in conn.execute("SELECT v FROM t ORDER BY id")]
    assert rows == ["alpha", "beta", "gamma"]


def test_create_backup_never_clobbers_same_second(tmp_path):
    db = tmp_path / "motif.db"
    _seed_db(db)
    db_backup.create_backup(db, tmp_path, now_stamp="20260612-143000")
    with pytest.raises(FileExistsError):
        db_backup.create_backup(db, tmp_path, now_stamp="20260612-143000")


def test_create_backup_missing_source_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        db_backup.create_backup(tmp_path / "nope.db", tmp_path,
                               now_stamp="20260612-143000")


def test_list_backups_newest_first_ignores_junk(tmp_path):
    db = tmp_path / "motif.db"
    _seed_db(db)
    db_backup.create_backup(db, tmp_path, now_stamp="20260101-000000")
    db_backup.create_backup(db, tmp_path, now_stamp="20260612-143000")
    # a non-matching file in the dir must be ignored, not listed.
    (db_backup.backups_dir(tmp_path) / "notes.txt").write_text("hi")
    names = [b.name for b in db_backup.list_backups(tmp_path)]
    assert names == ["motif-20260612-143000.db", "motif-20260101-000000.db"]


def test_filename_validation_blocks_traversal(tmp_path):
    assert db_backup.is_backup_name("motif-20260612-143000.db")
    for bad in ("../../etc/passwd", "motif.db", "../motif-20260612-143000.db",
                "motif-2026.db", "", ".", ".."):
        assert not db_backup.is_backup_name(bad)
    # resolve_backup returns None for an invalid name even if a file
    # by some path exists.
    assert db_backup.resolve_backup(tmp_path, "../../etc/passwd") is None
    with pytest.raises(ValueError):
        db_backup.delete_backup(tmp_path, "../../etc/passwd")


def test_delete_backup(tmp_path):
    db = tmp_path / "motif.db"
    _seed_db(db)
    bf = db_backup.create_backup(db, tmp_path, now_stamp="20260612-143000")
    assert db_backup.delete_backup(tmp_path, bf.name) is True
    assert db_backup.delete_backup(tmp_path, bf.name) is False  # gone now


# ── endpoints ────────────────────────────────────────────────


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
    return TestClient(create_app(settings)), tmp_path


_H = {"X-Authentik-Username": "testadmin"}


def test_create_list_download_delete_roundtrip(app_client):
    client, cfg = app_client
    # create
    r = client.post("/api/admin/database-backup", headers=_H)
    assert r.status_code == 200, r.text
    name = r.json()["backup"]["name"]
    assert name.startswith("motif-") and name.endswith(".db")
    # list shows it
    r = client.get("/api/admin/database-backups", headers=_H)
    assert r.status_code == 200
    listed = [b["name"] for b in r.json()["backups"]]
    assert name in listed
    # download returns the bytes (a valid sqlite header)
    r = client.get(f"/api/admin/database-backup/download/{name}", headers=_H)
    assert r.status_code == 200
    assert r.content[:16] == b"SQLite format 3\x00"
    # delete
    r = client.post("/api/admin/database-backup/delete",
                    json={"name": name}, headers=_H)
    assert r.status_code == 200
    r = client.get("/api/admin/database-backups", headers=_H)
    assert name not in [b["name"] for b in r.json()["backups"]]


def test_download_rejects_traversal(app_client):
    client, cfg = app_client
    # a name that isn't a motif-<ts>.db must 404, never serve a file.
    r = client.get("/api/admin/database-backup/download/motif.db", headers=_H)
    assert r.status_code == 404
    r = client.post("/api/admin/database-backup/delete",
                    json={"name": "../../etc/passwd"}, headers=_H)
    assert r.status_code == 400


def test_endpoints_require_admin(app_client):
    client, cfg = app_client
    # no forward-auth header → unauthenticated → rejected before any
    # work (401 from the auth middleware, or 403 from _require_admin
    # for an authenticated-but-non-admin principal). Either way the
    # endpoints are NOT open.
    for resp in (
        client.post("/api/admin/database-backup"),
        client.get("/api/admin/database-backups"),
        client.post("/api/admin/database-backup/delete", json={"name": "x"}),
    ):
        assert resp.status_code in (401, 403), resp.status_code


# ── UI surfaces + SSR drift ──────────────────────────────────


def test_settings_database_tab_and_card_present():
    html = (REPO / "app" / "web" / "templates" / "settings.html").read_text()
    assert 'data-tab="database"' in html
    assert 'data-panel="database"' in html
    assert 'id="database-backup-create-btn"' in html
    assert 'id="database-backup-list"' in html


def test_ssr_panel_allowlist_includes_database():
    """The base.html head-script allowlist + both app.css SSR blocks
    must learn the new panel or the deep-link / first-paint breaks
    (the v1.15.65 drift class)."""
    base = (REPO / "app" / "web" / "templates" / "base.html").read_text()
    assert "'database'" in base
    css = (REPO / "app" / "web" / "static" / "app.css").read_text()
    assert ('html[data-settings-tab="database"] '
            '.tab-panel[data-panel="database"]') in css
    assert ('html[data-settings-tab="database"] '
            '#settings-tabs .tab[data-tab="database"]') in css


def test_app_js_binds_backup_card():
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert "function bindDatabaseBackup()" in js
    assert "bindDatabaseBackup();" in js
    assert "/api/admin/database-backup" in js
