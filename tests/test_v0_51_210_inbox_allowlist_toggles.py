"""v0.51.210 — in-app INBOX allowlist toggles.

Per-kind toggles (Settings → NOTIFICATIONS → IN-APP INBOX) that choose which event kinds
land in the topbar INBOX drawer, INDEPENDENT of the Apprise/Discord send-toggles. Stored
as notifications.inbox_events (default all ON = the pre-toggle behaviour). notify.dispatch
gates the inbox record on this map — so a kind can be off for Discord yet on in the inbox,
or turned off from the inbox entirely.

The gate is exercised behaviorally (dispatch → count the inbox rows); the PATCH round-trip
through the real endpoint proves the closed-set + config plumbing; the UI is source-pinned.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from starlette.testclient import TestClient

from app.core import notify, notify_inbox
from app.core.config_file import NotificationsConfig, _DEFAULT_INBOX_EVENTS
from app.core.db import init_db

REPO = Path(__file__).resolve().parent.parent
SETTINGS_HTML = (REPO / "app" / "web" / "templates" / "settings.html").read_text()
AUTH = {"X-Authentik-Username": "testadmin"}


# ── the default map + the allowlist mirror ───────────────────────────────────

def test_default_inbox_events_mirrors_the_allowlist_all_on():
    # the config default map MUST key-match notify_inbox.INBOX_EVENT_KINDS (they're
    # duplicated to keep config_file free of an app-core import — this is the guard).
    assert set(_DEFAULT_INBOX_EVENTS) == set(notify_inbox.INBOX_EVENT_KINDS)
    assert all(_DEFAULT_INBOX_EVENTS.values()), "every inbox kind defaults ON"


def test_fresh_config_carries_all_inbox_toggles_on():
    c = NotificationsConfig()
    assert c.inbox_events == {k: True for k in notify_inbox.INBOX_EVENT_KINDS}


# ── the notify gate (the core behavioral contract) ───────────────────────────

@pytest.fixture
def db(tmp_path):
    p = tmp_path / "motif.db"
    init_db(p)
    return p


def test_toggle_off_suppresses_the_inbox_record(db):
    cfg = NotificationsConfig()
    cfg.inbox_events["theme_added"] = False        # off for the inbox
    notify.dispatch(db, cfg, event_kind="theme_added", title="🎵 Theme added — X", body="")
    assert notify_inbox.count_unread(db) == 0
    assert notify_inbox.list_notifications(db) == []


def test_toggle_on_records_even_when_apprise_is_off(db):
    # default: theme_added inbox ON, Apprise events[theme_added] OFF (its default), no urls.
    cfg = NotificationsConfig()
    assert cfg.events.get("theme_added") is False   # Discord off by default...
    assert cfg.inbox_events["theme_added"] is True   # ...inbox on by default
    notify.dispatch(db, cfg, event_kind="theme_added", title="🎵 Theme added — Y", body="")
    assert notify_inbox.count_unread(db) == 1        # inbox recorded regardless of Apprise


def test_non_inbox_kind_never_records_regardless_of_toggles(db):
    cfg = NotificationsConfig()
    notify.dispatch(db, cfg, event_kind="sync_completed", title="sync done", body="")
    assert notify_inbox.count_unread(db) == 0        # sync_completed isn't an inbox kind


# ── PATCH round-trip: closed-set + config plumbing ───────────────────────────

@pytest.fixture
def client_and_db(tmp_path, monkeypatch):
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
    return TestClient(create_app(s)), s.db_path


def test_patch_toggles_inbox_event_and_drops_unknown_keys(client_and_db):
    c, _ = client_and_db
    # a valid kind flips; an unknown key is dropped by the closed-set (mirrors events).
    r = c.patch("/api/config", json={"notifications": {"inbox_events": {
        "plex_theme_lost": False, "totally_bogus_key": True}}}, headers=AUTH)
    assert r.status_code == 200, r.text
    cfg = c.get("/api/config", headers=AUTH).json()["config"]
    ie = cfg["notifications"]["inbox_events"]
    assert ie["plex_theme_lost"] is False           # the real toggle persisted
    assert "totally_bogus_key" not in ie            # unknown key dropped, not stored
    # the untouched kinds keep their default (merge, not replace).
    assert ie["theme_added"] is True


# ── UI ───────────────────────────────────────────────────────────────────────

def test_settings_has_the_inbox_toggle_group():
    assert "// IN-APP INBOX" in SETTINGS_HTML
    for kind in notify_inbox.INBOX_EVENT_KINDS:
        assert f'data-cfg-field="notifications.inbox_events.{kind}"' in SETTINGS_HTML, kind
    assert 'data-save="notifications">// SAVE INBOX' in SETTINGS_HTML
