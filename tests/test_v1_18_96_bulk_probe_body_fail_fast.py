"""v1.18.96 — BULK PROBE TDB fails fast on malformed body.

the user's 2026-05-23 21:48:31 repro on the deployed v1.18.92
image:

    WARNING app.web.api  BULK PROBE TDB: body parse failed
    (Expecting value: line 1 column 1 (char 0)) — defaulting
    to all-rows scope. If the UI bulk selection was intended,
    retry the action.

The fall-through silently triggered an all-rows probe across
5,332 entries, burning the YouTube rate-limit budget. The probe
bailed at 422 with "15 consecutive transient results (likely
YouTube rate-limit; throttle persists up to an hour, retry
then)."

## Root cause

The endpoint hit `await request.json()` which raises on
both:
  - intentional no-body (settings PROBE TDB URLS button posts
    nothing — meant to probe all rows)
  - non-empty malformed body (UI bug, transport hiccup)

Both paths landed in `body = {}` → no scope → all-rows.

## Fix

Read the raw body bytes first:
  - empty bytes → intentional no body → defaults to all-rows
    (preserves the v1.15.1 contract for the settings button)
  - non-empty bytes that fail json.loads → HTTP 400 with a
    clear "send no body for all-rows OR send {items: [...]}"
    message
  - non-empty bytes that parse fine → existing scope-extraction
    logic runs as before

Also reordered the slot acquisition: try_acquire was called
BEFORE body parsing pre-v1.18.96. A 400 with the slot already
claimed would leave op_progress holding the slot until the 24h
prune, blocking concurrent good probes with 409. v1.18.96 moves
try_acquire AFTER body parsing so bad input fails without
holding a resource. Race window between parse + claim widens
slightly but no resource leak on bad input.

## What's pinned

- Empty body → 200 + scope=all-rows (preserves UX)
- Body `{}` → 200 + scope=all-rows (preserves UX)
- Body `{"items": [...]}` → 200 + scope=selection (preserves UX)
- Non-empty malformed body → 400 with a clear message
- Op slot NOT claimed on 400 path (no resource leak)
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"


# ── Source-level pins ────────────────────────────────────────


def test_handler_reads_raw_body_first():
    """The handler must read request.body() (raw bytes) before
    attempting json.loads, so it can distinguish empty body
    (intentional) from non-empty unparseable (bug)."""
    src = API_PY.read_text()
    fn_idx = src.index('@app.post("/api/admin/bulk-probe-tdb")')
    fn_end = src.index("@app.post(\"/api/admin/reprobe-tdb-failures\")",
                       fn_idx)
    body = src[fn_idx:fn_end]
    assert "await request.body()" in body, (
        "v1.18.96: handler must read raw bytes to distinguish "
        "empty body (intentional all-rows) from non-empty "
        "malformed body (bug)"
    )
    # The empty-body short-circuit returns body={} without
    # attempting json.loads (mimics no-body path).
    assert "if not raw_body" in body


def test_handler_raises_400_on_malformed_body():
    """Non-empty body that fails json.loads must raise
    HTTPException(400), not silently default to all-rows."""
    src = API_PY.read_text()
    fn_idx = src.index('@app.post("/api/admin/bulk-probe-tdb")')
    fn_end = src.index("@app.post(\"/api/admin/reprobe-tdb-failures\")",
                       fn_idx)
    body = src[fn_idx:fn_end]
    # The 400 must reference the parse-fail path.
    assert "status_code=400" in body, (
        "v1.18.96: malformed body must raise HTTPException(400)"
    )
    # And the user-facing detail message must explain both paths
    # (empty body = all rows; populated body = selection).
    assert "no body" in body and "items" in body, (
        "v1.18.96: 400 detail must explain both calling shapes"
    )


def test_handler_acquires_slot_after_body_parse():
    """try_acquire must run AFTER body parsing so a 400 on bad
    input doesn't leave the op_progress slot claimed and
    blocking concurrent good probes."""
    src = API_PY.read_text()
    fn_idx = src.index('@app.post("/api/admin/bulk-probe-tdb")')
    fn_end = src.index("@app.post(\"/api/admin/reprobe-tdb-failures\")",
                       fn_idx)
    body = src[fn_idx:fn_end]
    parse_idx = body.index("await request.body()")
    # Anchor on the actual try_acquire CALL (not comment mentions
    # of the symbol). The call is inside `run_in_threadpool(...)`.
    acquire_idx = body.index("op_progress.try_acquire,")
    assert parse_idx < acquire_idx, (
        "v1.18.96: try_acquire must run AFTER body parsing — "
        "otherwise a 400 on malformed input leaves the slot "
        "claimed and 409s concurrent good probes"
    )


def test_handler_keeps_warning_log_on_400_path():
    """Per v1.18.5 cold-path-needs-MORE-logging: defensive
    code that rejects input must log WHY at WARNING so the
    operator can correlate the user's "got a 400" report with
    docker log evidence."""
    src = API_PY.read_text()
    fn_idx = src.index('@app.post("/api/admin/bulk-probe-tdb")')
    fn_end = src.index("@app.post(\"/api/admin/reprobe-tdb-failures\")",
                       fn_idx)
    body = src[fn_idx:fn_end]
    assert "log.warning" in body, (
        "v1.18.96: malformed-body 400 path must log.warning"
    )


# ── End-to-end behavioral tests ──────────────────────────────


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
    return TestClient(create_app(settings))


AUTH = {"X-Authentik-Username": "testadmin"}


def _slot_status(db: Path) -> str | None:
    """Read op_progress.status for the bulk-probe-tdb slot.
    None means no row (slot free). Used to verify slot leak
    behavior on 400 paths."""
    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT status FROM op_progress WHERE op_id = ?",
            ("bulk-probe-tdb",),
        ).fetchone()
        return row[0] if row else None


def _settings_db(tmp_path):
    from app.config import Settings
    return Settings(
        config_dir=tmp_path, data_dir=tmp_path / "data",
    ).db_path


def test_empty_body_succeeds_as_all_rows_scope(admin_client):
    """The settings PROBE TDB URLS button posts no body. This
    is the v1.15.1 contract — must still work post-fix."""
    r = admin_client.post(
        "/api/admin/bulk-probe-tdb", headers=AUTH,
    )
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["ok"] is True
    assert data["scope_count"] == 0, (
        f"empty body → all rows (scope_count=0); got {data}"
    )


def test_body_with_items_succeeds_as_selection_scope(
    admin_client, tmp_path,
):
    """Body with {items: [...]} runs scoped probe. Acquire
    the slot then end the test — we don't care that the
    background thread does nothing useful in tests."""
    r = admin_client.post(
        "/api/admin/bulk-probe-tdb", headers=AUTH,
        json={"items": [
            {"media_type": "movie", "tmdb_id": 1},
            {"media_type": "movie", "tmdb_id": 2},
        ]},
    )
    assert r.status_code == 200, r.text
    assert r.json()["scope_count"] == 2


def test_malformed_body_returns_400(admin_client):
    """The v1.18.96 fix: non-empty body that fails json.loads
    must 400, not silently default to all-rows."""
    r = admin_client.post(
        "/api/admin/bulk-probe-tdb",
        content=b"{not valid json",
        headers={**AUTH, "Content-Type": "application/json"},
    )
    assert r.status_code == 400, (
        f"v1.18.96: malformed body must 400, got "
        f"{r.status_code}: {r.text}"
    )
    # Detail explains both calling shapes.
    detail = r.json().get("detail", "")
    assert "no body" in detail
    assert "items" in detail


def test_malformed_body_does_not_claim_op_slot(
    admin_client, tmp_path,
):
    """Pre-v1.18.96 the try_acquire ran BEFORE body parsing.
    A 400 on bad input left op_progress holding the slot,
    blocking concurrent good probes with 409. v1.18.96
    reordered so the slot is only claimed after parse
    succeeds."""
    db = _settings_db(tmp_path)
    # Slot starts free.
    assert _slot_status(db) is None
    # Send malformed body → 400.
    r = admin_client.post(
        "/api/admin/bulk-probe-tdb",
        content=b"garbage",
        headers={**AUTH, "Content-Type": "application/json"},
    )
    assert r.status_code == 400
    # Slot must still be free — no row inserted because
    # try_acquire never ran.
    assert _slot_status(db) is None, (
        f"v1.18.96: malformed-body 400 must NOT claim the "
        f"op_progress slot, got status={_slot_status(db)}"
    )


def test_malformed_body_does_not_block_subsequent_probe(
    admin_client, tmp_path,
):
    """Pre-fix: malformed body claimed the slot → concurrent
    valid probe would 409. Post-fix: malformed body 400s
    without claiming → next valid probe runs."""
    # Bad request — gets 400.
    r1 = admin_client.post(
        "/api/admin/bulk-probe-tdb",
        content=b"definitely not json",
        headers={**AUTH, "Content-Type": "application/json"},
    )
    assert r1.status_code == 400
    # Immediately follow with a valid no-body request.
    r2 = admin_client.post(
        "/api/admin/bulk-probe-tdb", headers=AUTH,
    )
    assert r2.status_code == 200, (
        f"v1.18.96: valid probe after malformed body must "
        f"succeed (slot must not have been claimed by the "
        f"failed request), got {r2.status_code}: {r2.text}"
    )


def test_empty_json_object_treated_as_all_rows(admin_client):
    """Body `{}` parses fine but has no items → falls through
    to all-rows. Preserves existing behavior."""
    r = admin_client.post(
        "/api/admin/bulk-probe-tdb", headers=AUTH,
        json={},
    )
    assert r.status_code == 200
    assert r.json()["scope_count"] == 0


def test_empty_items_list_treated_as_all_rows(admin_client):
    """Body `{"items": []}` parses fine; empty items list
    falls through to all-rows per the existing
    `isinstance(raw_items, list) and raw_items` truthy check.
    Preserves existing behavior — not part of v1.18.96's
    scope (separate concern if the user wants to tighten)."""
    r = admin_client.post(
        "/api/admin/bulk-probe-tdb", headers=AUTH,
        json={"items": []},
    )
    assert r.status_code == 200
    assert r.json()["scope_count"] == 0


def test_malformed_body_logs_warning(admin_client, caplog):
    """Per CLAUDE.md class-9 + v1.18.5 cold-path-needs-MORE-
    logging: the 400 path must log.warning so the operator
    can correlate the user's '400 error' report with docker
    log evidence of WHICH request was bad."""
    import logging
    caplog.set_level(logging.WARNING)
    r = admin_client.post(
        "/api/admin/bulk-probe-tdb",
        content=b"{invalid",
        headers={**AUTH, "Content-Type": "application/json"},
    )
    assert r.status_code == 400
    # The log line must mention the parse failure.
    warnings = [
        rec.message for rec in caplog.records
        if rec.levelno >= logging.WARNING
        and "BULK PROBE TDB" in rec.message
    ]
    assert warnings, (
        f"v1.18.96: 400 path must log.warning with "
        f"BULK PROBE TDB context; got records: "
        f"{[r.message for r in caplog.records]}"
    )
    # The message must explicitly mention "body parse failed"
    # so a grep finds it (consistent with the pre-v1.18.96 log
    # phrasing for log-trail continuity).
    assert any("body parse failed" in w for w in warnings)
