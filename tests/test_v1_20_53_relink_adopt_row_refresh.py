"""v1.20.53 — relink/adopt join the row-refresh contract.

Design-system audit (2026-05-31) found a row-refresh contract gap: the
`relink` and `adopt` per-row jobs (place-pool job types that mutate
LINK/SRC/PL row state) were covered by NEITHER refresh mechanism:
  - the /api/library `job_in_flight` subquery only matches download/place;
  - the global `perJobBusy` union (refreshTopbarStatus) listed
    download/place/scan/refresh but not relink/adopt.
So a relink (Storage page) or adopt (Scan page) sweep left library chips
stale until the 30s background tick if the user navigated to a library
tab mid-run. CLAUDE.md even claimed all six per-row jobs set
`job_in_flight` — only download/place do.

Fix: expose relink_in_flight + adopt_in_flight in /api/stats and fold
them into perJobBusy. Behavioral test (exercises the endpoint, per the
v1.18.81 backend→frontend rule) + a perJobBusy source pin.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
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
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="testadmin", password="testpassword")
    client = TestClient(create_app(settings))
    client._db_path = str(settings.db_path)
    return client


def _enqueue(db_path, job_type, status="pending"):
    con = sqlite3.connect(db_path)
    con.execute(
        "INSERT INTO jobs (job_type, status, created_at) VALUES (?, ?, ?)",
        (job_type, status, "2026-05-31T00:00:00"),
    )
    con.commit()
    con.close()


def test_stats_exposes_relink_and_adopt_in_flight(admin_client):
    """A pending relink + a running adopt job must surface in
    /api/stats queue counts (the signal perJobBusy reads). Insert
    BEFORE the first GET so the /api/stats 1s-TTL cache (bug class #7)
    computes fresh on the cache-miss."""
    _enqueue(admin_client._db_path, "relink", "pending")
    _enqueue(admin_client._db_path, "adopt", "running")

    q = admin_client.get("/api/stats", headers=AUTH).json()["queue"]
    assert q["relink_in_flight"] == 1, "pending relink must count"
    assert q["adopt_in_flight"] == 1, "running adopt must count"


def test_done_relink_adopt_do_not_count(admin_client):
    """Only pending/running count — a finished sweep shouldn't keep the
    row-refresh signal hot."""
    _enqueue(admin_client._db_path, "relink", "done")
    _enqueue(admin_client._db_path, "adopt", "cancelled")
    q = admin_client.get("/api/stats", headers=AUTH).json()["queue"]
    assert q["relink_in_flight"] == 0
    assert q["adopt_in_flight"] == 0


def test_perjobbusy_includes_relink_and_adopt():
    """The JS row-refresh union must read both new counts, or the signal
    still misses relink/adopt sweeps."""
    anchor = APP_JS.index("const perJobBusy = (")
    body = APP_JS[anchor:anchor + 700]
    assert "q.relink_in_flight" in body
    assert "q.adopt_in_flight" in body


def test_v1_20_53_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
