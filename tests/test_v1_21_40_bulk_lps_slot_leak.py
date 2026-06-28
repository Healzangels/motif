"""v1.21.40 — BULK LET PLEX SERVE validates the body BEFORE claiming the op slot.

Silent-failure audit finding H3: api_admin_bulk_let_plex_serve ran
try_acquire('bulk-lps') BEFORE the `if not targets: raise 400`. A
malformed/empty body 400'd AFTER claiming the slot, leaving a PENDING
op_progress row that prune_finished (finished-only) and request_cancel
(running-only) never clear → every future BULK LET PLEX SERVE returned
409 "already running" forever, with no running op visible, until a
process restart. Fix mirrors bulk-probe-tdb (v1.18.96): validate first,
acquire only when about to do real work.
"""
from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core.auth import create_admin, init_auth_schema
from app.core.db import get_conn, init_db


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


HDR = {"X-Authentik-Username": "testadmin"}


def test_bulk_lps_bad_body_400_does_not_leak_op_slot(app_client):
    client, settings = app_client
    # Empty items → 400 (no valid targets).
    r1 = client.post("/api/admin/bulk-let-plex-serve",
                     json={"items": []}, headers=HDR)
    assert r1.status_code == 400, r1.text
    # The slot must NOT have leaked: a SECOND bad request must ALSO 400,
    # not 409 "already running" (the pre-fix permanent-lockout symptom).
    r2 = client.post("/api/admin/bulk-let-plex-serve",
                     json={"items": []}, headers=HDR)
    assert r2.status_code == 400, (
        "v1.21.40: a 400 must not leak the op slot — the second bad "
        f"request must also 400, not 409. Got {r2.status_code}: {r2.text}")
    # And no pending bulk-lps op_progress row was left behind.
    with get_conn(settings.db_path) as conn:
        rows = conn.execute(
            "SELECT status FROM op_progress WHERE op_id = 'bulk-lps'"
        ).fetchall()
    assert not rows, (
        "no bulk-lps op_progress row should leak on a 400; got "
        f"{[r['status'] for r in rows]}")


def test_bulk_lps_malformed_body_also_400_no_leak(app_client):
    client, settings = app_client
    # Non-JSON body → parse fails → body={} → no targets → 400.
    r1 = client.post("/api/admin/bulk-let-plex-serve",
                     content="not json", headers=HDR)
    assert r1.status_code == 400, r1.text
    r2 = client.post("/api/admin/bulk-let-plex-serve",
                     content="not json", headers=HDR)
    assert r2.status_code == 400, (
        f"second malformed request must also 400, not 409. Got "
        f"{r2.status_code}: {r2.text}")
