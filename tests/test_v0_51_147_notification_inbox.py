"""v0.51.147 — in-app notification center, backend foundation.

The `notifications` table (schema v71) + notify_inbox.py + the record hook at the
notify dispatch chokepoint + the read/dismiss/seen endpoints + the /api/stats badge
count. The load-bearing behaviour: an INBOX-kind event is recorded to the inbox
UNCONDITIONALLY of the per-event Apprise send-toggle, so the drawer surfaces
auto-added themes even when that kind's Discord toggle is off ("outside Discord").
"""
from __future__ import annotations

import sqlite3
import types
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core import db as dbmod
from app.core import notify, notify_inbox

REPO = Path(__file__).resolve().parent.parent
NOTIFY_PY = (REPO / "app" / "core" / "notify.py").read_text()
AUTH = {"X-Authentik-Username": "testadmin"}


def _cfg(*, events: dict, sinks: list[str] | None = None):
    """A minimal stand-in for NotificationsConfig — dispatch only reads .events /
    .apprise_urls / .apprise_external_url."""
    return types.SimpleNamespace(
        events=events, apprise_urls=sinks or [], apprise_external_url="")


# ── schema v71 ───────────────────────────────────────────────

def test_fresh_db_has_notifications_table_at_v71(tmp_path):
    db = tmp_path / "fresh.db"
    dbmod.init_db(db)
    conn = sqlite3.connect(db)
    try:
        assert conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='notifications'"
        ).fetchone()
        assert conn.execute("SELECT MAX(version) FROM schema_version").fetchone()[0] == 71
    finally:
        conn.close()


def test_migration_v70_to_v71_creates_the_table(tmp_path):
    """Simulate an existing v70 install (table absent) → init_db runs the
    v70→v71 branch → the table appears and the version advances."""
    db = tmp_path / "mig.db"
    dbmod.init_db(db)
    conn = sqlite3.connect(db)
    with conn:
        conn.execute("DROP TABLE notifications")
        conn.execute("UPDATE schema_version SET version = 70")
    conn.close()
    dbmod.init_db(db)  # re-init → migration path
    conn = sqlite3.connect(db)
    try:
        assert conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='notifications'"
        ).fetchone()
        assert conn.execute("SELECT MAX(version) FROM schema_version").fetchone()[0] == 71
    finally:
        conn.close()


# ── notify_inbox helpers ─────────────────────────────────────

def test_inbox_helpers_roundtrip(tmp_path):
    db = tmp_path / "h.db"
    dbmod.init_db(db)
    notify_inbox.record_notification(db, event_kind="theme_added", severity="info",
                                     title="The Batman (2022)", body="auto-added")
    notify_inbox.record_notification(db, event_kind="plex_theme_lost", severity="warning",
                                     title="Andor", body="lost")
    assert notify_inbox.count_unread(db) == 2
    rows = notify_inbox.list_notifications(db)
    assert [r["title"] for r in rows] == ["Andor", "The Batman (2022)"]  # newest first
    assert all(r["seen"] is False for r in rows)
    # mark-seen clears the badge but keeps the rows
    assert notify_inbox.mark_seen(db) == 2
    assert notify_inbox.count_unread(db) == 0
    assert len(notify_inbox.list_notifications(db)) == 2
    # dismiss one, then clear-all
    assert notify_inbox.dismiss_notification(db, rows[0]["id"]) == 1
    assert len(notify_inbox.list_notifications(db)) == 1
    assert notify_inbox.dismiss_all(db) == 1
    assert notify_inbox.list_notifications(db) == []


# ── the crux: recorded UNCONDITIONALLY of the Apprise toggle ──

def test_dispatch_records_inbox_even_when_apprise_toggle_off(tmp_path):
    db = tmp_path / "d.db"
    dbmod.init_db(db)
    # theme_added is a known kind but its Apprise toggle is OFF and there are no sinks.
    notify.dispatch(db, _cfg(events={"theme_added": False}),
                    event_kind="theme_added", title="Dune (2021)", body="auto-added")
    rows = notify_inbox.list_notifications(db)
    assert len(rows) == 1 and rows[0]["event_kind"] == "theme_added"


def test_dispatch_skips_non_inbox_kinds(tmp_path):
    db = tmp_path / "n.db"
    dbmod.init_db(db)
    # sync_completed is a real kind but NOT in the inbox allowlist.
    notify.dispatch(db, _cfg(events={"sync_completed": True}),
                    event_kind="sync_completed", title="Sync done", body="…")
    assert notify_inbox.list_notifications(db) == []


def test_record_inbox_false_suppresses(tmp_path):
    """The coalescer passes _record_inbox=False on its own sends so a bulk burst
    (recorded per-item in dispatch_coalesced) isn't double-counted."""
    db = tmp_path / "s.db"
    dbmod.init_db(db)
    notify.dispatch(db, _cfg(events={"theme_added": True}),
                    event_kind="theme_added", title="X", body="y", _record_inbox=False)
    assert notify_inbox.list_notifications(db) == []


def test_coalesced_records_per_item_before_gate(tmp_path):
    db = tmp_path / "c.db"
    dbmod.init_db(db)
    for title in ("A (2020)", "B (2021)", "C (2022)"):
        notify.dispatch_coalesced(
            db, _cfg(events={"theme_added": False}),  # toggle off
            event_kind="theme_added", item_label=title,
            single_title=title, single_body="added",
            batch_title_fn=lambda n: f"{n} themes",
            batch_body_fn=lambda labels, buckets=None: ", ".join(labels),
            bulk=True)
    rows = notify_inbox.list_notifications(db)
    assert {r["title"] for r in rows} == {"A (2020)", "B (2021)", "C (2022)"}


def test_coalescer_sends_pass_record_inbox_false():
    """Guard the double-count avoidance at the source: every dispatch() call the
    coalescer makes carries _record_inbox=False."""
    i = NOTIFY_PY.index("def _dispatch_batch")
    batch = NOTIFY_PY[i:i + 1400]
    assert batch.count("_record_inbox=False") >= 2  # 1-item + ≥2-summary sends
    # the bulk=False single-send in dispatch_coalesced too
    j = NOTIFY_PY.index("def dispatch_coalesced")
    assert "_record_inbox=False" in NOTIFY_PY[j:j + 2600]


# ── endpoints + /api/stats badge ─────────────────────────────

@pytest.fixture
def admin_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.web.api import create_app
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    dbmod.init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="testadmin", password="testpassword")
    return TestClient(create_app(settings)), settings.db_path


def test_endpoints_list_seen_dismiss_and_stats_badge(admin_client):
    client, db = admin_client
    notify_inbox.record_notification(db, event_kind="theme_added", severity="info",
                                     title="One", body="a")
    notify_inbox.record_notification(db, event_kind="theme_auto_restored", severity="info",
                                     title="Two", body="b")

    r = client.get("/api/notifications", headers=AUTH)
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True and body["unread"] == 2
    assert [n["title"] for n in body["notifications"]] == ["Two", "One"]

    # stats badge reflects unread
    assert client.get("/api/stats", headers=AUTH).json().get("notifications_unread") == 2

    # mark seen → badge clears, rows remain
    assert client.post("/api/notifications/seen", headers=AUTH).json()["marked"] == 2
    assert client.get("/api/notifications", headers=AUTH).json()["unread"] == 0
    assert len(client.get("/api/notifications", headers=AUTH).json()["notifications"]) == 2

    # dismiss one
    nid = body["notifications"][0]["id"]
    assert client.post(f"/api/notifications/{nid}/dismiss", headers=AUTH).json()["dismissed"] == 1
    assert len(client.get("/api/notifications", headers=AUTH).json()["notifications"]) == 1

    # clear all
    assert client.post("/api/notifications/dismiss-all", headers=AUTH).json()["dismissed"] == 1
    assert client.get("/api/notifications", headers=AUTH).json()["notifications"] == []
