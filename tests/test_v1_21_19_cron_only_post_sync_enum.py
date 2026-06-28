"""v1.21.19 — cron-only post-sync-enum sub-toggle.

the user wanted finer control over AUTO-REFRESH PLEX AFTER SYNC: the main
toggle should enable the post-sync Plex enumeration for BOTH the
scheduled (cron) sync and a manually-kicked // SYNC THEMERRDB click; a
new sub-toggle should re-enable it for the CRON sync ONLY (so the daily
sync keeps self-healing while manual clicks stay enum-free).

Mechanism: both sync enqueue sites stamp a `trigger` into the job payload
('cron' from scheduler._enqueue_sync, 'manual' from /api/sync/now), and
_do_sync routes the decision through _should_enum_after_sync():

    do_enum = auto_enum_after_sync OR (trigger == 'cron' AND auto_enum_after_cron_sync)
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core.auth import create_admin, init_auth_schema
from app.core.db import get_conn, init_db
from app.core.scheduler import _enqueue_sync
from app.core.worker import _should_enum_after_sync


REPO = Path(__file__).resolve().parent.parent
FWD_HDR = {"X-Authentik-Username": "testadmin"}


# ── the decision contract (truth table) ─────────────────────

@pytest.mark.parametrize("main,sub,trigger,expected", [
    # main ON → enum regardless of sub / trigger
    (True,  False, "cron",   True),
    (True,  False, "manual", True),
    (True,  True,  "manual", True),
    (True,  False, None,     True),
    # main OFF + sub ON → cron only
    (False, True,  "cron",   True),
    (False, True,  "manual", False),
    (False, True,  None,     False),   # legacy job (no trigger) ≠ cron
    # main OFF + sub OFF → never
    (False, False, "cron",   False),
    (False, False, "manual", False),
])
def test_should_enum_after_sync_truth_table(main, sub, trigger, expected):
    assert _should_enum_after_sync(main, sub, trigger) is expected


# ── enqueue sites stamp the trigger ─────────────────────────

def test_scheduler_stamps_trigger_cron(tmp_path):
    db = tmp_path / "motif.db"
    init_db(db)
    _enqueue_sync(db)
    with get_conn(db) as conn:
        row = conn.execute(
            "SELECT payload FROM jobs WHERE job_type = 'sync'"
        ).fetchone()
    assert row is not None
    assert json.loads(row["payload"]) == {"trigger": "cron"}


def _make_app(tmp_path, monkeypatch):
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


def _sync_job_payload(db):
    with get_conn(db) as conn:
        row = conn.execute(
            "SELECT payload FROM jobs WHERE job_type = 'sync' "
            "ORDER BY id DESC LIMIT 1"
        ).fetchone()
    return json.loads(row["payload"]) if row else None


def test_manual_sync_stamps_trigger_manual(tmp_path, monkeypatch):
    client, settings = _make_app(tmp_path, monkeypatch)
    r = client.post("/api/sync/now", json={}, headers=FWD_HDR)
    assert r.status_code == 200, r.text
    assert _sync_job_payload(settings.db_path).get("trigger") == "manual"


def test_manual_metadata_only_keeps_trigger_and_flag(tmp_path, monkeypatch):
    client, settings = _make_app(tmp_path, monkeypatch)
    r = client.post("/api/sync/now", json={"metadata_only": True}, headers=FWD_HDR)
    assert r.status_code == 200, r.text
    payload = _sync_job_payload(settings.db_path)
    assert payload.get("trigger") == "manual"
    assert payload.get("enqueue_downloads") is False   # metadata_only preserved


# ── config field plumbing ───────────────────────────────────

def test_config_field_default_off_and_env_override():
    import os, tempfile
    from app.core.config_file import MotifConfig
    assert MotifConfig().sync.auto_enum_after_cron_sync is False
    os.environ["MOTIF_AUTO_ENUM_AFTER_CRON_SYNC"] = "true"
    try:
        from app.config import Settings
        d = Path(tempfile.mkdtemp())
        s = Settings(config_dir=d, data_dir=d / "data")
        assert s.sync_auto_enum_after_cron_sync is True
    finally:
        os.environ.pop("MOTIF_AUTO_ENUM_AFTER_CRON_SYNC", None)


# ── UI surface (source pins) ────────────────────────────────

def test_settings_has_nested_sub_checkbox():
    html = (REPO / "app" / "web" / "templates" / "settings.html").read_text()
    assert 'data-cfg-field="sync.auto_enum_after_cron_sync"' in html
    assert "form-checkbox-sub" in html


def test_css_has_sub_checkbox_rule():
    css = (REPO / "app" / "web" / "static" / "app.css").read_text()
    assert ".form-checkbox-sub" in css


def test_version_bumped():
    assert '__version__ = "0.' in (REPO / "app" / "__init__.py").read_text()
