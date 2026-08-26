"""v0.51.299 — holistic review wave 8: the relink dedup, behaviorally.

The v1.23.69 double-enqueue race fix on POST /api/storage/relink was
guarded only by a source pin satisfiable by ANY transaction line in the
handler — no test ever called the endpoint, so the already_queued contract
was unexercised (the v1.18.81 phantom-guard class).
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

AUTH = {"X-Authentik-Username": "testadmin"}


@pytest.fixture
def client(tmp_path, monkeypatch):
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    from fastapi.testclient import TestClient
    (tmp_path / "data").mkdir()
    (tmp_path / "motif.yaml").write_text("paths: {}\n")
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    return TestClient(create_app(s)), s.db_path


def test_second_relink_post_dedupes_to_the_same_job(client):
    c, db = client
    r1 = c.post("/api/storage/relink", headers=AUTH)
    assert r1.status_code == 200, r1.text
    j1 = r1.json()
    assert j1["ok"] is True and j1.get("job_id")
    assert not j1.get("already_queued")
    r2 = c.post("/api/storage/relink", headers=AUTH)
    j2 = r2.json()
    assert j2.get("already_queued") is True, (
        "the v1.23.69 dedup: a second click must not double-enqueue")
    assert j2["job_id"] == j1["job_id"], "the SAME sweep job is returned"
    with sqlite3.connect(db) as conn:
        rows = conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE job_type='relink' "
            "AND media_type IS NULL").fetchone()[0]
    assert rows == 1, "exactly one sweep job exists"


def test_done_sweep_allows_a_fresh_enqueue(client):
    c, db = client
    j1 = c.post("/api/storage/relink", headers=AUTH).json()["job_id"]
    with sqlite3.connect(db) as conn:
        conn.execute("UPDATE jobs SET status='done' WHERE id=?", (j1,))
        conn.commit()
    j2 = c.post("/api/storage/relink", headers=AUTH).json()
    assert not j2.get("already_queued") and j2["job_id"] != j1, (
        "a finished sweep must not block the next one")
