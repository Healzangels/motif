"""v1.23.16 — scheduled database backups + retention (tag 2 of 3).

Adds an optional nightly cron (DatabaseBackupConfig: enabled / cron /
retention) on top of v1.23.15's manual backups. The cron is read once
at scheduler boot; enabled + retention are read fresh on each tick so
toggling takes effect without a restart. After each scheduled snapshot
the retention prune keeps the newest N.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core import db_backup
from app.core.auth import create_admin, init_auth_schema
from app.core.config_file import MotifConfig, validate
from app.core.db import init_db


REPO = Path(__file__).resolve().parent.parent


def _seed_db(path: Path) -> None:
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        conn.commit()


# ── config ───────────────────────────────────────────────────


def test_config_defaults_and_validation():
    cfg = MotifConfig()
    assert cfg.database_backup.enabled is False
    assert cfg.database_backup.cron == "0 4 * * *"
    assert cfg.database_backup.retention == 7
    assert validate(cfg, require_themes_dir=False) == []
    # bad cron + negative retention surface errors.
    cfg.database_backup.cron = "nope"
    cfg.database_backup.retention = -1
    errs = validate(cfg, require_themes_dir=False)
    assert any("database_backup.cron" in e for e in errs)
    assert any("database_backup.retention" in e for e in errs)


def test_env_overrides(monkeypatch):
    from app.core.config_file import env_overrides_present
    monkeypatch.setenv("MOTIF_DB_BACKUP_ENABLED", "true")
    monkeypatch.setenv("MOTIF_DB_BACKUP_RETENTION", "3")
    present = env_overrides_present()
    assert present.get("database_backup.enabled") == "MOTIF_DB_BACKUP_ENABLED"
    assert present.get("database_backup.retention") == "MOTIF_DB_BACKUP_RETENTION"


# ── retention prune ──────────────────────────────────────────


def test_prune_keeps_newest_n(tmp_path):
    db = tmp_path / "motif.db"
    _seed_db(db)
    for stamp in ("20260101-000000", "20260102-000000",
                  "20260103-000000", "20260104-000000"):
        db_backup.create_backup(db, tmp_path, now_stamp=stamp)
    removed = db_backup.prune_backups(tmp_path, retention=2)
    # the two OLDEST are removed.
    assert sorted(removed) == ["motif-20260101-000000.db",
                               "motif-20260102-000000.db"]
    kept = [b.name for b in db_backup.list_backups(tmp_path)]
    assert kept == ["motif-20260104-000000.db", "motif-20260103-000000.db"]


def test_prune_zero_keeps_all(tmp_path):
    db = tmp_path / "motif.db"
    _seed_db(db)
    db_backup.create_backup(db, tmp_path, now_stamp="20260101-000000")
    assert db_backup.prune_backups(tmp_path, retention=0) == []
    assert len(db_backup.list_backups(tmp_path)) == 1


# ── scheduler job ────────────────────────────────────────────


def _settings(tmp_path, monkeypatch, *, enabled, retention=7):
    monkeypatch.setenv("MOTIF_DB_BACKUP_ENABLED", "true" if enabled else "false")
    monkeypatch.setenv("MOTIF_DB_BACKUP_RETENTION", str(retention))
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    from app.config import Settings
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    return s


def test_scheduled_job_creates_and_prunes(tmp_path, monkeypatch):
    from app.core.scheduler import _scheduled_database_backup
    s = _settings(tmp_path, monkeypatch, enabled=True, retention=2)
    assert s.db_backup_enabled is True
    # pre-seed 2 old snapshots so the new one trips the retention=2 prune.
    db_backup.create_backup(s.db_path, tmp_path, now_stamp="20260101-000000")
    db_backup.create_backup(s.db_path, tmp_path, now_stamp="20260102-000000")
    _scheduled_database_backup(s)
    names = [b.name for b in db_backup.list_backups(tmp_path)]
    assert len(names) == 2, names  # the new one + 1 kept; oldest pruned
    assert "motif-20260101-000000.db" not in names  # oldest gone


def test_scheduled_job_noops_when_disabled(tmp_path, monkeypatch):
    from app.core.scheduler import _scheduled_database_backup
    s = _settings(tmp_path, monkeypatch, enabled=False)
    assert s.db_backup_enabled is False
    _scheduled_database_backup(s)
    assert db_backup.list_backups(tmp_path) == []


# ── PATCH round-trip + UI surfaces ───────────────────────────


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
    return TestClient(create_app(settings)), settings


_H = {"X-Authentik-Username": "testadmin"}


def test_patch_config_persists_backup_section(app_client):
    client, settings = app_client
    r = client.patch(
        "/api/config",
        json={"database_backup": {"enabled": True, "cron": "30 2 * * *",
                                  "retention": 3}},
        headers=_H,
    )
    assert r.status_code == 200, r.text
    settings.reload()
    assert settings.db_backup_enabled is True
    assert settings.db_backup_cron == "30 2 * * *"
    assert settings.db_backup_retention == 3


def test_patch_config_rejects_bad_cron(app_client):
    client, settings = app_client
    r = client.patch(
        "/api/config",
        json={"database_backup": {"cron": "not a cron"}},
        headers=_H,
    )
    assert r.status_code == 400


def test_settings_scheduled_form_present():
    html = (REPO / "app" / "web" / "templates" / "settings.html").read_text()
    assert 'data-config-form="database_backup"' in html
    assert 'data-cfg-field="database_backup.enabled"' in html
    assert 'data-cfg-field="database_backup.cron"' in html
    assert 'data-cfg-field="database_backup.retention"' in html
    assert 'data-save="database_backup"' in html


def test_allowed_top_level_includes_backup_section():
    api = (REPO / "app" / "web" / "api.py").read_text()
    # the PATCH allowlist must learn the section or every save 400s.
    i = api.index("_ALLOWED_TOP_LEVEL = {")
    block = api[i:api.index("}", i)]
    assert '"database_backup"' in block


def test_scheduler_registers_backup_job():
    src = (REPO / "app" / "core" / "scheduler.py").read_text()
    assert "def _scheduled_database_backup(" in src
    assert 'id="database_backup"' in src
