"""v0.51.98 — SSR the two dashboard hero-button busy states.

the user: "when navigating to the dashboard when a themerrdb sync and
refresh plex is running and the buttons should show as syncing and
refreshing they briefly show as clickable when shouldn't be, they do
change but it takes a second."

Root cause: dashboard.html rendered // SYNC THEMERRDB / // REFRESH PLEX
as plain, un-disabled buttons. Their busy/disabled state was owned
entirely by refreshTopbarStatus, which only lands on the FIRST /api/stats
poll (~1s, and /api/stats carries a 1s TTL cache + hash-skip). So a nav
to /dashboard WHILE a sync/refresh was already running showed the buttons
clickable for ~1s before the JS locked them.

Fix (mirrors the v1.15.45 / v1.15.55 / v1.15.78 SSR flash-kill pattern):
_dashboard_ssr_state() now bakes two busy flags —

  * sync_btn_busy       = themerrdb_sync_in_flight>0 OR cascade enum>0
                          (mirror app.js dashSyncBtnBusy; scope='cascade'
                          only — a manual REFRESH PLEX must NOT lock SYNC)
  * refresh_plex_btn_busy = any in-flight plex_enum
                          (mirror app.js plexRefreshing / plexEnumBusy)

— and dashboard.html renders `disabled` + the busy label on first paint
when the flag is set. The label strings match app.js origLabel/busyLabel
exactly (autoEnum spells both phases) so the first poll is idempotent.

These tests exercise the real GET /dashboard -> SSR SQL pipe (behavioral,
per the v1.18.81 phantom-fix lesson) plus static guards on the flag
plumbing.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

REPO = Path(__file__).resolve().parent.parent
DASH_HTML = (REPO / "app" / "web" / "templates" / "dashboard.html").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()

NOW = datetime.now(timezone.utc).isoformat(timespec="seconds")
AUTH = {"X-Authentik-Username": "testadmin"}


def _build(tmp_path, monkeypatch, *, auto_enum: bool):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv(
        "MOTIF_AUTO_ENUM_AFTER_SYNC", "true" if auto_enum else "false")
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    assert s.sync_auto_enum_after_sync is auto_enum, (
        "env override MOTIF_AUTO_ENUM_AFTER_SYNC didn't take — the test's "
        "auto_enum precondition is wrong"
    )
    return TestClient(create_app(s)), s.db_path


def _job(db_path, job_type, *, status="running", scope=None):
    payload = None if scope is None else json.dumps({"scope": scope})
    with sqlite3.connect(db_path) as c:
        c.execute(
            "INSERT INTO jobs (job_type, status, payload, created_at) "
            "VALUES (?,?,?,?)",
            (job_type, status, payload, NOW),
        )
        c.commit()


def _btn(html: str, btn_id: str) -> str:
    i = html.index(f'id="{btn_id}"')
    start = html.rfind("<button", 0, i)
    end = html.index("</button>", i) + len("</button>")
    return html[start:end]


# ── Behavioral: SYNC THEMERRDB button ────────────────────────────────


def test_sync_button_ssr_disabled_when_sync_running(tmp_path, monkeypatch):
    """A running `sync` job → GET /dashboard paints // SYNC THEMERRDB
    disabled + the busy label on first paint (no clickable flash)."""
    client, db = _build(tmp_path, monkeypatch, auto_enum=True)
    _job(db, "sync")
    r = client.get("/", headers=AUTH)
    assert r.status_code == 200, r.text
    btn = _btn(r.text, "sync-now-btn")
    assert "disabled" in btn, btn
    # autoEnum on → two-phase busy label, matching app.js busyLabel. The
    # label must sit flush between > and </button> (no whitespace) so the
    # rendered textContent === app.js dataset busyLabel exactly — otherwise
    # the first refreshTopbarStatus tick rewrites it (a churn, not idempotent).
    assert btn.endswith(">// SYNCING THEMERRDB + REFRESH PLEX…</button>"), btn


def test_sync_button_ssr_idle_when_nothing_running(tmp_path, monkeypatch):
    """No in-flight jobs → the button paints enabled with the idle
    label (and NOT the busy label)."""
    client, db = _build(tmp_path, monkeypatch, auto_enum=True)
    r = client.get("/", headers=AUTH)
    assert r.status_code == 200, r.text
    btn = _btn(r.text, "sync-now-btn")
    assert "disabled" not in btn, btn
    assert "SYNCING" not in btn, btn
    # autoEnum on → two-phase idle label (matches app.js origLabel), so a
    # sibling idle-label flicker doesn't survive either.
    assert "// SYNC THEMERRDB + REFRESH PLEX" in btn, btn


def test_sync_button_locked_by_enum_when_auto_enum_on(tmp_path, monkeypatch):
    """With auto_enum ON, a running plex_enum (the post-sync cascade, or
    any background enum) locks SYNC — mirror of app.js bindDashboard's
    page-load probe `initialBusy = tdb + enum` in that mode."""
    client, db = _build(tmp_path, monkeypatch, auto_enum=True)
    _job(db, "plex_enum", scope="cascade")
    r = client.get("/", headers=AUTH)
    assert r.status_code == 200, r.text
    assert "disabled" in _btn(r.text, "sync-now-btn")


def test_sync_button_not_locked_by_manual_refresh(tmp_path, monkeypatch):
    """With auto_enum OFF (the config where BOTH buttons show), a manual
    REFRESH PLEX (plex_enum) must NOT lock SYNC — they're independent
    ops (v1.13.77), and bindDashboard's probe counts tdb only here."""
    client, db = _build(tmp_path, monkeypatch, auto_enum=False)
    _job(db, "plex_enum", scope="scan_all")
    r = client.get("/", headers=AUTH)
    assert r.status_code == 200, r.text
    assert "disabled" not in _btn(r.text, "sync-now-btn")


# ── Behavioral: REFRESH PLEX button ──────────────────────────────────


def test_refresh_plex_button_ssr_disabled_when_enum_running(
        tmp_path, monkeypatch):
    """A running plex_enum → the REFRESH PLEX button (present only when
    auto_enum is off) paints disabled + // REFRESHING PLEX… ."""
    client, db = _build(tmp_path, monkeypatch, auto_enum=False)
    _job(db, "plex_enum", scope="scan_all")
    r = client.get("/", headers=AUTH)
    assert r.status_code == 200, r.text
    btn = _btn(r.text, "sync-plex-btn")
    assert "disabled" in btn, btn
    # flush label (see the sync-button test for why exactness matters).
    assert btn.endswith(">// REFRESHING PLEX…</button>"), btn


def test_refresh_plex_button_ssr_idle_when_nothing_running(
        tmp_path, monkeypatch):
    client, db = _build(tmp_path, monkeypatch, auto_enum=False)
    r = client.get("/", headers=AUTH)
    assert r.status_code == 200, r.text
    btn = _btn(r.text, "sync-plex-btn")
    assert "disabled" not in btn, btn
    assert "REFRESHING" not in btn, btn
    assert "// REFRESH PLEX" in btn, btn


# ── Static guards: flag plumbing survives a refactor ─────────────────


def test_dashboard_ssr_state_declares_button_busy_flags():
    anchor = API_PY.index("def _dashboard_ssr_state(")
    header = API_PY[anchor:anchor + 12000]  # the state-dict defaults.
    assert '"sync_btn_busy": False' in header, (
        "v0.51.98: _dashboard_ssr_state must default sync_btn_busy so a "
        "DB-error fallback renders an idle (not wrongly-disabled) button"
    )
    assert '"refresh_plex_btn_busy": False' in header
    # The SYNC derivation mirrors app.js bindDashboard's page-load probe:
    # tdb>0 OR (auto_enum AND enum>0). It must read the LIVE auto_enum
    # setting (not hard-code either mode) so the two agree on init.
    assert (
        'state["sync_btn_busy"] = (_tdb_if > 0) or (_auto_enum and _enum_if > 0)'
        in API_PY
    ), "v0.51.98: SYNC lock must mirror bindDashboard's tdb OR (autoEnum AND enum)"
    assert "_auto_enum = bool(settings.sync_auto_enum_after_sync)" in API_PY
    assert 'state["refresh_plex_btn_busy"] = _enum_if > 0' in API_PY


def test_dashboard_template_buttons_carry_ssr_busy_gate():
    # Both hero buttons must branch on the SSR flag in their open tag.
    sync = _btn(DASH_HTML, "sync-now-btn")
    assert "_sync_busy" in sync and "disabled" in sync, sync
    plex = _btn(DASH_HTML, "sync-plex-btn")
    assert "_plex_busy" in plex and "disabled" in plex, plex
