"""v1.20.22 — ops-drawer expand-to-2-column run view.

the user: the drawer "feels cramped... hard to tell what's going on,"
and the timeline labels show a `?` help-cursor that "doesn't do
anything." This adds:
  - GET /api/ops/{op_id}/events — the run's FULL event log, scoped by
    the op's op_progress time window (events have no op_id FK).
  - a click-to-expand 2-column card (drawer widens, right pane = run
    log) in ops.js + ops.css.
  - fixes the dead `?` cursor (only steps with a distinct long-form
    get cursor:help now).
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
OPS_JS = (REPO / "app" / "web" / "static" / "ops.js").read_text()
OPS_CSS = (REPO / "app" / "web" / "static" / "ops.css").read_text()
AUTH = {"X-Authentik-Username": "testadmin"}


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


# ── backend: /api/ops/{op_id}/events ─────────────────────────


def _seed_op(db, op_id, started, finished):
    from app.core import progress as op
    op.start_progress(db, op_id, "tdb_sync", stage="x")
    with sqlite3.connect(db) as c:
        c.execute(
            "UPDATE op_progress SET started_at=?, finished_at=?, "
            "status=? WHERE op_id=?",
            (started, finished, "done" if finished else "running", op_id))
        c.commit()


def _seed_event(db, ts, message, level="INFO", component="sync"):
    with sqlite3.connect(db) as c:
        c.execute(
            "INSERT INTO events (ts, level, component, message) "
            "VALUES (?, ?, ?, ?)", (ts, level, component, message))
        c.commit()


def test_op_events_scoped_to_run_window(admin_client):
    client, db = admin_client
    _seed_op(db, "run-1", "2026-05-30T10:00:00+00:00",
             "2026-05-30T10:05:00+00:00")
    _seed_event(db, "2026-05-30T09:59:00+00:00", "before window")
    _seed_event(db, "2026-05-30T10:02:00+00:00", "in window A")
    _seed_event(db, "2026-05-30T10:04:00+00:00", "in window B")
    _seed_event(db, "2026-05-30T10:06:00+00:00", "after window")
    r = client.get("/api/ops/run-1/events", headers=AUTH)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["scoped"] is True
    msgs = [e["message"] for e in body["events"]]
    assert msgs == ["in window A", "in window B"], (
        "v1.20.22: only events inside the run's [started, finished] "
        "window, in chronological order"
    )
    assert body["live"] is False


def test_op_events_live_op_has_no_upper_bound(admin_client):
    client, db = admin_client
    _seed_op(db, "run-live", "2026-05-30T10:00:00+00:00", None)
    _seed_event(db, "2026-05-30T09:59:00+00:00", "before")
    _seed_event(db, "2026-05-30T10:30:00+00:00", "during (open-ended)")
    r = client.get("/api/ops/run-live/events", headers=AUTH)
    body = r.json()
    assert body["live"] is True
    msgs = [e["message"] for e in body["events"]]
    assert "during (open-ended)" in msgs and "before" not in msgs


def test_op_events_unknown_op_is_unscoped(admin_client):
    client, db = admin_client
    r = client.get("/api/ops/does-not-exist/events", headers=AUTH)
    assert r.status_code == 200
    body = r.json()
    assert body["scoped"] is False
    assert body["events"] == []


def test_op_events_requires_admin(admin_client):
    client, db = admin_client
    r = client.get("/api/ops/run-1/events")  # no auth header
    assert r.status_code in (401, 403)


# ── ops.js expand wiring ─────────────────────────────────────


def test_js_expand_machinery_present():
    # v1.20.30: applyDrawerWide() removed — the run log stacks in place
    # rather than widening the whole panel, so there's no width state.
    for fn in ("function toggleExpand(", "function renderExpandedDetail(",
               "function fetchOpEvents("):
        assert fn in OPS_JS, f"missing {fn}"
    # card is click-to-expand (real ops only) + wraps content in -main
    assert "op-card-expandable" in OPS_JS
    assert '"op-card-main"' in OPS_JS or "op-card-main" in OPS_JS
    # the run log fetches the new endpoint.
    assert "/api/ops/" in OPS_JS and "/events" in OPS_JS
    # v1.21.27: expand/collapse no longer re-keys the structural hash (that
    # forced a full card DOM replace — the click flicker). It's handled in
    # place in _updateCardInPlace, which injects/removes the detail child.
    assert "exp: op.op_id === state.expandedOpId" not in OPS_JS
    assert "el.insertAdjacentHTML" in OPS_JS


def test_js_detail_pane_click_does_not_collapse():
    # clicks inside the run log must be guarded so reading/scrolling it
    # doesn't collapse the card. v1.20.41 renders the run log in-card
    # (accordion) as .op-card-detail again.
    assert "e.target.closest('.op-card-detail')) return" in OPS_JS


def test_js_dead_help_cursor_fixed():
    # only steps with a distinct long-form get the help class now.
    assert "s.long && s.long !== s.label" in OPS_JS
    assert 'class="has-help"' in OPS_JS


# ── ops.css expand styling ───────────────────────────────────


def test_css_expand_rules_present():
    # v1.20.30: the .ops-drawer-wide width override was removed — the run
    # log stacks beneath the card (see test_v1_20_30) instead of widening
    # the panel. The expand + runlog styling hooks still exist.
    assert ".op-card-expanded {" in OPS_CSS
    assert ".op-card-runlog {" in OPS_CSS
    # the dead cursor:help moved onto the .has-help variant only.
    assert ".op-card-timeline-labels > span.has-help { cursor: help; }" in OPS_CSS


def test_drawer_expand_hint_present_and_toggled():
    """v0.50.82: a persistent hint makes the click-to-expand affordance discoverable
    (the ▸ wasn't obvious — the user). It's hidden when there's nothing to expand."""
    base = (REPO / "app" / "web" / "templates" / "base.html").read_text()
    assert 'id="ops-drawer-hint"' in base
    assert "click an op to expand" in base
    # ops.js hides the hint when there are no active/finished cards.
    assert ("hint.style.display = (active.length || finished.length) ? '' : 'none';"
            in OPS_JS)
    # the class is styled (real CSS rule, not an invented class).
    assert ".ops-drawer-hint {" in OPS_CSS


def test_v1_20_22_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
