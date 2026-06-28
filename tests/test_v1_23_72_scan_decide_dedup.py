"""v1.23.72 — jobs-table dedup for the scan + decide-finding enqueue races.

Deferred from the v1.23.69 audit: api_trigger_scan + api_decide_finding(/bulk)
ran a dedup check + INSERT INTO jobs in plain autocommit, and jobs has no UNIQUE.
A plain transaction wrap couldn't fix them because each dedups against a
WORKER-stamped field (scan_runs.status / scan_findings.adopted_at) that neither
request has changed yet — so even serialized, both pass and both insert. The fix
adds a JOBS-table dedup (existing pending/running job for the same target) inside
a BEGIN IMMEDIATE, mirroring api_sync_now / api_relink_all (v1.23.69).
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

REPO = Path(__file__).resolve().parent.parent
API = (REPO / "app" / "web" / "api.py").read_text()
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
    return settings.db_path, TestClient(create_app(settings))


def _job_count(db, job_type):
    with sqlite3.connect(db) as c:
        return c.execute("SELECT count(*) FROM jobs WHERE job_type = ?",
                         (job_type,)).fetchone()[0]


def _seed_finding(db, finding_id=1):
    with sqlite3.connect(db) as c:
        c.execute("INSERT OR IGNORE INTO scan_runs (id, started_at, status) "
                  "VALUES (1, datetime('now'), 'complete')")
        c.execute(
            "INSERT INTO scan_findings (id, scan_run_id, section_id, section_type, "
            "media_folder, file_path, file_size, file_mtime, file_sha256, finding_kind) "
            "VALUES (?, 1, '1', 'movie', '/data/m/x', 'x/theme.mp3', 100, "
            "datetime('now'), 'abc', 'orphan_resolvable')", (finding_id,))
        c.commit()


# ── source pins: dedup against jobs + BEGIN IMMEDIATE at all three sites ──


def test_scan_dedups_against_jobs_table():
    i = API.index("async def api_trigger_scan(")
    body = API[i:i + 1500]
    assert "with get_conn(db) as conn, transaction(conn):" in body
    assert "job_type = 'scan' " in body and "status IN ('pending','running')" in body


def test_decide_finding_dedups_against_jobs_table():
    i = API.index("async def api_decide_finding(")
    body = API[i:i + 1900]
    assert "with get_conn(db) as conn, transaction(conn):" in body
    assert "json_extract(payload, '$.finding_id') = ?" in body


def test_decide_bulk_per_row_txn_and_dedup():
    i = API.index("async def api_decide_findings_bulk(")
    body = API[i:i + 2400]
    assert "with transaction(conn):" in body, "per-row BEGIN IMMEDIATE"
    assert "json_extract(payload, '$.finding_id') = ?" in body


# ── behavioral: a pending adopt job blocks a duplicate (single + bulk) ──


def test_decide_finding_is_noop_when_adopt_job_already_pending(admin_client):
    db, client = admin_client
    _seed_finding(db, 1)
    with sqlite3.connect(db) as c:
        c.execute("INSERT INTO jobs (job_type, payload, status, created_at, next_run_at) "
                  "VALUES ('adopt', '{\"finding_id\": 1}', 'pending', "
                  "datetime('now'), datetime('now'))")
        c.commit()
    assert _job_count(db, "adopt") == 1
    r = client.post("/api/scans/findings/1/decision",
                    json={"decision": "adopt"}, headers=AUTH)
    assert r.status_code == 200, r.text
    assert "already queued" in r.json().get("note", "")
    assert _job_count(db, "adopt") == 1, "must NOT enqueue a duplicate adopt job"


def test_decide_finding_happy_path_still_enqueues(admin_client):
    db, client = admin_client
    _seed_finding(db, 2)
    r = client.post("/api/scans/findings/2/decision",
                    json={"decision": "adopt"}, headers=AUTH)
    assert r.status_code == 200, r.text
    assert _job_count(db, "adopt") == 1, "a clean finding must enqueue one adopt job"


def test_decide_bulk_skips_finding_with_pending_job(admin_client):
    db, client = admin_client
    _seed_finding(db, 1)
    _seed_finding(db, 2)
    with sqlite3.connect(db) as c:
        c.execute("INSERT INTO jobs (job_type, payload, status, created_at, next_run_at) "
                  "VALUES ('adopt', '{\"finding_id\": 1}', 'pending', "
                  "datetime('now'), datetime('now'))")
        c.commit()
    r = client.post("/api/scans/findings/decisions/bulk",
                    json={"finding_ids": [1, 2], "decision": "adopt"}, headers=AUTH)
    assert r.status_code == 200, r.text
    assert r.json()["enqueued"] == 1, "finding 1 already queued → only 2 enqueues"
    # one pre-seeded + one for finding 2 = 2 total, NOT 3.
    assert _job_count(db, "adopt") == 2
