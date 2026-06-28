"""v1.23.17 — database restore (tag 3 of 3, data-loss class).

A restore STAGES the chosen snapshot (an existing server-side backup
or an uploaded .db); the swap is applied at the NEXT boot via
db_backup.apply_pending_restore (main.py), before any connection opens
— no live overwrite, stale WAL removed, current db safety-copied first.
Validation refuses non-SQLite files, integrity failures, and schemas
NEWER than the running build.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core import db_backup
from app.core.auth import create_admin, init_auth_schema
from app.core.db import CURRENT_SCHEMA_VERSION, init_db


REPO = Path(__file__).resolve().parent.parent


def _make_db(path: Path, *, schema_version: int, marker: str) -> None:
    """A minimal motif-shaped DB: a schema_version table + a marker row
    so a restore is detectable by content."""
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE schema_version "
                     "(version INTEGER PRIMARY KEY, applied_at TEXT)")
        conn.execute("INSERT INTO schema_version VALUES (?, '2026-01-01')",
                     (schema_version,))
        conn.execute("CREATE TABLE marker (v TEXT)")
        conn.execute("INSERT INTO marker VALUES (?)", (marker,))
        conn.commit()


# ── inspect / validate ───────────────────────────────────────


def test_inspect_accepts_valid_db(tmp_path):
    db = tmp_path / "snap.db"
    _make_db(db, schema_version=CURRENT_SCHEMA_VERSION, marker="ok")
    c = db_backup.inspect_restore_source(db)
    assert c.ok and c.schema_version == CURRENT_SCHEMA_VERSION


def test_inspect_rejects_non_sqlite(tmp_path):
    f = tmp_path / "notadb.db"
    f.write_bytes(b"this is not a database")
    c = db_backup.inspect_restore_source(f)
    assert not c.ok and "header" in (c.error or "")


def test_inspect_rejects_newer_schema(tmp_path):
    db = tmp_path / "future.db"
    _make_db(db, schema_version=CURRENT_SCHEMA_VERSION + 1, marker="future")
    c = db_backup.inspect_restore_source(db)
    assert not c.ok and "newer" in (c.error or "")


def test_inspect_rejects_db_without_schema_version(tmp_path):
    db = tmp_path / "plain.db"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE x (id INTEGER)")
        conn.commit()
    c = db_backup.inspect_restore_source(db)
    assert not c.ok and "schema_version" in (c.error or "")


# ── stage / cancel / apply ───────────────────────────────────


def test_stage_then_cancel(tmp_path):
    live = tmp_path / "motif.db"
    _make_db(live, schema_version=CURRENT_SCHEMA_VERSION, marker="live")
    snap = tmp_path / "snap.db"
    _make_db(snap, schema_version=CURRENT_SCHEMA_VERSION, marker="snap")
    db_backup.stage_restore(live, snap)
    assert db_backup.restore_pending_path(live).exists()
    assert db_backup.cancel_pending_restore(live) is True
    assert not db_backup.restore_pending_path(live).exists()


def test_apply_pending_restore_swaps_and_safety_copies(tmp_path):
    live = tmp_path / "motif.db"
    _make_db(live, schema_version=CURRENT_SCHEMA_VERSION, marker="LIVE")
    snap = tmp_path / "snap.db"
    _make_db(snap, schema_version=CURRENT_SCHEMA_VERSION, marker="SNAP")
    db_backup.stage_restore(live, snap)
    res = db_backup.apply_pending_restore(
        live, tmp_path, now_stamp="20260612-150000")
    assert res and res["applied"] is True
    # live db now carries the snapshot's marker.
    with sqlite3.connect(live) as conn:
        assert conn.execute("SELECT v FROM marker").fetchone()[0] == "SNAP"
    # the pending file is consumed.
    assert not db_backup.restore_pending_path(live).exists()
    # a pre-restore safety backup of the OLD db exists + holds "LIVE".
    assert res["safety_backup"]
    safety = db_backup.backups_dir(tmp_path) / res["safety_backup"]
    with sqlite3.connect(safety) as conn:
        assert conn.execute("SELECT v FROM marker").fetchone()[0] == "LIVE"


def test_apply_noop_when_nothing_pending(tmp_path):
    live = tmp_path / "motif.db"
    _make_db(live, schema_version=CURRENT_SCHEMA_VERSION, marker="live")
    assert db_backup.apply_pending_restore(
        live, tmp_path, now_stamp="20260612-150000") is None


def test_apply_rejects_corrupt_pending_keeps_live(tmp_path):
    live = tmp_path / "motif.db"
    _make_db(live, schema_version=CURRENT_SCHEMA_VERSION, marker="LIVE")
    # hand-write a corrupt pending file (bypasses stage_restore's validation).
    db_backup.restore_pending_path(live).write_bytes(b"corrupt not a db")
    res = db_backup.apply_pending_restore(
        live, tmp_path, now_stamp="20260612-150000")
    assert res and res["applied"] is False
    # live db untouched (still LIVE), bad pending discarded.
    with sqlite3.connect(live) as conn:
        assert conn.execute("SELECT v FROM marker").fetchone()[0] == "LIVE"
    assert not db_backup.restore_pending_path(live).exists()


# ── endpoints ────────────────────────────────────────────────


@pytest.fixture
def app_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="testadmin", password="testpassword")
    from app.web.api import create_app
    return TestClient(create_app(settings)), tmp_path


_H = {"X-Authentik-Username": "testadmin"}


def test_restore_from_backup_stages_and_pending_lifecycle(app_client):
    client, cfg = app_client
    # make a server-side backup of the (real) live db, then restore it.
    name = client.post("/api/admin/database-backup", headers=_H
                       ).json()["backup"]["name"]
    r = client.post("/api/admin/database-restore",
                    json={"name": name}, headers=_H)
    assert r.status_code == 200, r.text
    assert r.json()["restart_required"] is True
    # pending now reports true; cancel clears it.
    assert client.get("/api/admin/database-restore/pending",
                      headers=_H).json()["pending"] is True
    assert client.post("/api/admin/database-restore/cancel",
                       headers=_H).json()["cancelled"] is True
    assert client.get("/api/admin/database-restore/pending",
                      headers=_H).json()["pending"] is False


def test_restore_unknown_backup_404(app_client):
    client, cfg = app_client
    r = client.post("/api/admin/database-restore",
                    json={"name": "motif-20200101-000000.db"}, headers=_H)
    assert r.status_code == 404


def test_restore_upload_validates(app_client):
    client, cfg = app_client
    # a non-DB upload is rejected 422 (validation), nothing staged.
    r = client.post(
        "/api/admin/database-restore/upload",
        files={"file": ("bad.db", b"not a sqlite db", "application/octet-stream")},
        headers=_H,
    )
    assert r.status_code == 422
    assert client.get("/api/admin/database-restore/pending",
                      headers=_H).json()["pending"] is False
    # a valid motif-shaped db stages successfully.
    good = cfg / "good.db"
    _make_db(good, schema_version=CURRENT_SCHEMA_VERSION, marker="up")
    r = client.post(
        "/api/admin/database-restore/upload",
        files={"file": ("good.db", good.read_bytes(), "application/octet-stream")},
        headers=_H,
    )
    assert r.status_code == 200, r.text
    assert r.json()["restart_required"] is True


def test_restore_endpoints_require_admin(app_client):
    client, cfg = app_client
    for resp in (
        client.post("/api/admin/database-restore", json={"name": "x"}),
        client.get("/api/admin/database-restore/pending"),
        client.post("/api/admin/database-restore/cancel"),
    ):
        assert resp.status_code in (401, 403), resp.status_code


# ── boot wiring + UI ─────────────────────────────────────────


def test_main_applies_pending_restore_before_init_db():
    src = (REPO / "app" / "main.py").read_text()
    apply_i = src.index("apply_pending_restore")
    init_i = src.index("init_db(settings.db_path)")
    assert apply_i < init_i, (
        "the restore swap MUST happen before init_db opens a connection"
    )
    # v1.23.18: it must also run before the FIRST log_event(), which
    # spawns the events-flusher thread that opens its own long-lived
    # sqlite connection — a connection held across the os.replace is the
    # corruption race the code-review caught.
    first_log_event = src.index("log_event(")
    assert apply_i < first_log_event, (
        "apply_pending_restore must run before any log_event() — the "
        "flusher thread it starts opens a connection that would hold "
        "the old inode across the swap"
    )


def test_settings_restore_card_and_js_present():
    html = (REPO / "app" / "web" / "templates" / "settings.html").read_text()
    assert 'id="database-restore-pending"' in html
    assert 'id="database-restore-upload-btn"' in html
    assert 'id="database-restore-cancel-btn"' in html
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert "data-backup-restore" in js
    assert "/api/admin/database-restore" in js
