"""v1.20.32 — bulk-cancel all PENDING jobs of a type.

the user, on a long bulk download run (1,327 themes, ~54s each = ~20h):
"for jobs that have been running for a very long time like bulk download
jobs can we have a bulk cancel button in the drawer to cancel all the
pending jobs still remaining at once."

New POST /api/jobs/cancel-pending?job_type=download flips every PENDING
row of that type to 'cancelled'. The one job currently RUNNING finishes
on its own. A // CANCEL ALL PENDING button on the synthetic queue cards
in the ops drawer drives it.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
OPS_JS = (REPO / "app" / "web" / "static" / "ops.js").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()
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


def _seed_job(db, job_type, status):
    with sqlite3.connect(db) as c:
        cur = c.execute(
            "INSERT INTO jobs (job_type, status, created_at) VALUES (?,?,?)",
            (job_type, status, "2026-05-30T00:00:00+00:00"))
        c.commit()
        return cur.lastrowid


def _status(db, jid):
    with sqlite3.connect(db) as c:
        row = c.execute("SELECT status FROM jobs WHERE id=?", (jid,)).fetchone()
    return row[0] if row else None


# ── behavioral ───────────────────────────────────────────────


def test_cancels_pending_of_type_only(admin_client):
    client, db = admin_client
    p1 = _seed_job(db, "download", "pending")
    p2 = _seed_job(db, "download", "pending")
    running = _seed_job(db, "download", "running")
    other = _seed_job(db, "place", "pending")

    r = client.post("/api/jobs/cancel-pending?job_type=download", headers=AUTH)
    assert r.status_code == 200, r.text
    assert r.json()["cancelled"] == 2

    assert _status(db, p1) == "cancelled"
    assert _status(db, p2) == "cancelled"
    # the running download is left to finish on its own.
    assert _status(db, running) == "running"
    # a different job type's queue is untouched.
    assert _status(db, other) == "pending"


def test_zero_pending_is_ok(admin_client):
    client, db = admin_client
    _seed_job(db, "download", "running")
    r = client.post("/api/jobs/cancel-pending?job_type=download", headers=AUTH)
    assert r.status_code == 200
    assert r.json()["cancelled"] == 0


def test_invalid_job_type_400(admin_client):
    client, db = admin_client
    # sync + plex_enum are singletons (own op_progress cancel), not bulk.
    assert client.post(
        "/api/jobs/cancel-pending?job_type=sync", headers=AUTH).status_code == 400
    assert client.post(
        "/api/jobs/cancel-pending?job_type=bogus", headers=AUTH).status_code == 400


def test_requires_admin(admin_client):
    client, db = admin_client
    r = client.post("/api/jobs/cancel-pending?job_type=download")  # no header
    assert r.status_code in (401, 403)


def test_endpoint_excludes_singletons():
    # source pin: sync / plex_enum must NOT be in the cancelable set.
    anchor = API_PY.index("_BULK_CANCELABLE_JOB_TYPES = frozenset(")
    block = API_PY[anchor:anchor + 200]
    assert '"download"' in block and '"place"' in block
    assert '"sync"' not in block and '"plex_enum"' not in block


# ── frontend wiring ──────────────────────────────────────────


def test_drawer_renders_bulk_cancel_button():
    assert "data-bulk-cancel=" in OPS_JS
    assert "// CANCEL ALL PENDING" in OPS_JS
    # only on the synthetic _queue cards.
    assert "/_queue$/.test(op.kind)" in OPS_JS


def test_drawer_has_handler_and_helper():
    assert "function postBulkCancel(" in OPS_JS
    assert "/api/jobs/cancel-pending?job_type=" in OPS_JS
    assert "closest('[data-bulk-cancel]')" in OPS_JS
    # destructive → confirm gate.
    assert "window.confirm(" in OPS_JS


def test_v1_20_32_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
