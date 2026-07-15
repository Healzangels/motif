"""v0.51.148 — in-app notification center: the INBOX pill + drawer UI (phase 2).

Phase 1 (v0.51.147) landed the backend — the `notifications` table, the
`notify_inbox` module, the four endpoints, and `notifications_unread` in
/api/stats. This tag renders it: an always-visible INBOX topbar pill that lights
green with the unread count, and a slide-in NOTIFICATIONS drawer (reusing the
LIVE-OPS drawer shell) with a fixed per-tier stripe, per-row × dismiss, and a
CLEAR ALL. Opening the drawer marks the unread set seen (badge → 0).

Behavioral: render an authenticated page and assert the pill + drawer markup
reach the HTML. Source guards pin the app.js wiring + ops.css styling — the
drawer's open/close is animation + fetch, which a faithful browser exercises but
a fast unit test can't, so the UI-tag convention pins the load-bearing lines.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

NOW = datetime.now(timezone.utc).isoformat(timespec="seconds")
AUTH = {"X-Authentik-Username": "testadmin"}

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
OPS_CSS = (REPO / "app" / "web" / "static" / "ops.css").read_text()
BASE_HTML = (REPO / "app" / "web" / "templates" / "base.html").read_text()


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
    with sqlite3.connect(s.db_path) as c:
        c.execute("INSERT INTO plex_sections (section_id, title, type, is_anime,"
                  " is_4k, themes_subdir, included, discovered_at, last_seen_at) "
                  "VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        c.commit()
    return TestClient(create_app(s)), s.db_path


# ── behavioral: the pill + drawer render into the authenticated topbar ──


def test_inbox_pill_and_drawer_render(client):
    c, _ = client
    r = c.get("/", headers=AUTH)
    assert r.status_code == 200, r.text
    html = r.text
    # the always-visible INBOX pill
    assert 'id="topbar-inbox-badge"' in html
    assert "op-pill op-notif" in html
    assert 'id="topbar-inbox-count"' in html
    assert ">INBOX<" in html
    # the drawer reuses the ops-drawer shell + carries the list + clear-all
    assert 'id="notif-drawer"' in html
    assert 'id="notif-list"' in html
    assert 'id="notif-clear-all"' in html


def test_basehtml_pill_is_button_and_always_visible():
    # the INBOX pill is a <button> (it opens a drawer, not a filter deep-link
    # like the attention pills) and is NOT server-rendered hidden — always
    # visible, dim at rest; only its count span carries `hidden` until JS shows
    # the number.
    i = BASE_HTML.index('id="topbar-inbox-badge"')
    tag_start = BASE_HTML.rfind("<", 0, i)
    assert BASE_HTML.startswith("<button", tag_start)
    btn_close = BASE_HTML.index(">", i)
    btn_tag = BASE_HTML[tag_start:btn_close]
    assert "op-notif" in btn_tag
    assert "hidden" not in btn_tag


# ── source guards: JS wiring ──────────────────────────────────────────────


def test_appjs_wires_inbox_badge_and_drawer():
    # badge count wired from the stats poll
    assert "stats.notifications_unread" in APP_JS
    assert "inboxBadge.classList.add('has-unread')" in APP_JS
    # the drawer binder is defined AND invoked at init
    assert "function bindNotifInbox()" in APP_JS
    assert "bindNotifInbox();" in APP_JS
    # it talks to every phase-1 endpoint
    assert "api('GET', '/api/notifications')" in APP_JS
    assert "'/api/notifications/seen'" in APP_JS
    assert "'/api/notifications/dismiss-all'" in APP_JS
    assert "/api/notifications/${id}/dismiss" in APP_JS


def test_appjs_marks_seen_on_open():
    # opening the drawer marks the unread set seen (badge → 0) — the POST /seen
    # lives in load(), gated on there being an unseen row so a re-open of an
    # all-seen inbox doesn't re-POST.
    idx = APP_JS.index("function bindNotifInbox()")
    body = APP_JS[idx:APP_JS.index("function bindUploadDialog()", idx)]
    assert "items.some((i) => !i.seen)" in body
    assert "/api/notifications/seen" in body


# ── source guards: CSS ────────────────────────────────────────────────────


def test_opscss_pill_and_tier_stripes():
    # the pill's green is opt-in via .has-unread (dim at rest)
    assert ".op-pill.op-notif.has-unread" in OPS_CSS
    # the three FIXED tier stripes (green add / cyan available / amber FYI-loss)
    assert ".notif-row.tier-add" in OPS_CSS
    assert ".notif-row.tier-avail" in OPS_CSS
    assert ".notif-row.tier-fyi" in OPS_CSS
    # unread wash + seen-dim
    assert ".notif-row.unread" in OPS_CSS
    assert ".notif-row.seen .notif-title" in OPS_CSS
