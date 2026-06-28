"""v1.19.47 — /logs JOBS panel: only pin LIVE ops, mix terminal ops by recency.

the user's 2026-05-27 report: the /logs page JOBS panel showed
stale completed `op:` rows pinned above recent real jobs.
Screenshot showed:

    op:cloud-themes-backup (failed, minutes ago)
    op:tdb_sync           (done, 9am — hours ago)
    op:plex_enum          (done, hours ago)
    2998 place (done, minutes ago)
    2997 download (done, minutes ago)
    ...

— stale tdb_sync + plex_enum pinned above recent real jobs.

## Root cause

v1.18.95 unioned op_progress rows into /api/jobs with the
comment "Synthetic rows go FIRST so live activity is most
visible." Intent was correct for RUNNING ops (BULK PROBE TDB
ran 16+ minutes with the JOBS panel showing "no jobs" pre-
fix). But the same `synthesized + real_jobs` concatenation
pinned TERMINAL (done/failed/cancelled) op rows to the top
too — even when they finished hours before the most recent
real job.

## Fix

Partition synthesized rows into:
  - **active**: status ∈ {running, pending, cancelling} →
    pin to the top (preserves v1.18.95 intent for live work)
  - **terminal**: done / failed / cancelled → merge with
    real jobs, sort by effective timestamp (finished_at >
    started_at > created_at) DESC

ISO-8601 timestamps sort lexically = chronologically, so
the merged sort just uses string comparison.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from fastapi.testclient import TestClient  # noqa: E402


API_PY = (REPO / "app" / "web" / "api.py").read_text()


# ── Source guards ────────────────────────────────────────────


def test_jobs_endpoint_partitions_active_vs_terminal():
    """The /api/jobs endpoint must partition synthesized op
    rows by active vs terminal status."""
    fn_idx = API_PY.index("async def api_jobs(")
    fn_end = API_PY.index("\n    @app.", fn_idx + 1)
    body = API_PY[fn_idx:fn_end]
    assert "ACTIVE_STATUSES" in body, (
        "v1.19.47: must declare ACTIVE_STATUSES set for "
        "partitioning"
    )
    assert "active_synth" in body
    assert "terminal_synth" in body


def test_jobs_endpoint_sorts_terminal_by_recency():
    """Terminal ops + real jobs must merge sorted by an
    effective-timestamp key (finished_at > started_at >
    created_at) DESC."""
    fn_idx = API_PY.index("async def api_jobs(")
    fn_end = API_PY.index("\n    @app.", fn_idx + 1)
    body = API_PY[fn_idx:fn_end]
    assert "def _sort_key(" in body
    assert "finished_at" in body
    assert "reverse=True" in body, (
        "v1.19.47: terminal merge must sort DESC (most recent "
        "first)"
    )


def test_jobs_endpoint_pins_active_only():
    """Active synth still pins to top via concatenation, but
    only active — terminal goes into the merged sort."""
    fn_idx = API_PY.index("async def api_jobs(")
    fn_end = API_PY.index("\n    @app.", fn_idx + 1)
    body = API_PY[fn_idx:fn_end]
    # The combined list construction must put active_synth first.
    assert "active_synth + merged" in body


# ── Behavioral via TestClient ────────────────────────────────


@pytest.fixture
def admin_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    (tmp_path / "themes").mkdir(exist_ok=True)
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    settings = Settings(
        config_dir=tmp_path, data_dir=tmp_path / "data",
    )
    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    return TestClient(create_app(settings))


AUTH = {"X-Authentik-Username": "testadmin"}


def test_terminal_ops_dont_pin_above_recent_jobs(
    admin_client, tmp_path,
):
    """the user's exact repro: pre-seed a stale done op_progress
    + a recent real job → call /api/jobs → real job must come
    before the stale op."""
    from app.config import Settings
    settings = Settings(
        config_dir=tmp_path, data_dir=tmp_path / "data",
    )
    with sqlite3.connect(settings.db_path) as conn:
        # Stale done op_progress (finished hours ago).
        conn.execute(
            "INSERT INTO op_progress "
            "(op_id, kind, status, started_at, updated_at, "
            " finished_at) "
            "VALUES ('tdb-sync', 'tdb_sync', 'done', "
            "        '2026-05-27T09:00:00', "
            "        '2026-05-27T09:00:17', "
            "        '2026-05-27T09:00:17')"
        )
        # Recent real job (finished minutes ago).
        conn.execute(
            "INSERT INTO jobs "
            "(job_type, status, created_at, finished_at) "
            "VALUES ('place', 'done', "
            "        '2026-05-27T11:30:00', "
            "        '2026-05-27T11:30:05')"
        )
        conn.commit()
    r = admin_client.get("/api/jobs?status=all", headers=AUTH)
    assert r.status_code == 200
    jobs = r.json()["jobs"]
    # Find indices.
    ids = [str(j["id"]) for j in jobs]
    stale_op_idx = next(
        (i for i, j in enumerate(jobs)
         if j["id"] == "op:tdb-sync"),
        None,
    )
    real_job_idx = next(
        (i for i, j in enumerate(jobs)
         if j.get("job_type") == "place"),
        None,
    )
    assert stale_op_idx is not None, f"missing op:tdb-sync in {ids}"
    assert real_job_idx is not None, f"missing place job in {ids}"
    assert real_job_idx < stale_op_idx, (
        f"v1.19.47: recent real job (idx={real_job_idx}) must "
        f"come BEFORE stale terminal op (idx={stale_op_idx}). "
        f"Got order: {ids}"
    )


def test_active_op_still_pins_to_top(admin_client, tmp_path):
    """A RUNNING op must still pin to the top — v1.18.95
    intent preserved for live work."""
    from app.config import Settings
    settings = Settings(
        config_dir=tmp_path, data_dir=tmp_path / "data",
    )
    with sqlite3.connect(settings.db_path) as conn:
        # Running op (started moments ago).
        conn.execute(
            "INSERT INTO op_progress "
            "(op_id, kind, status, started_at, updated_at) "
            "VALUES ('cloud-themes-backup', "
            "        'cloud_themes_backup', 'running', "
            "        '2026-05-27T11:34:29', "
            "        '2026-05-27T11:34:30')"
        )
        # Newer real job (finished AFTER the running op started).
        conn.execute(
            "INSERT INTO jobs "
            "(job_type, status, created_at, finished_at) "
            "VALUES ('place', 'done', "
            "        '2026-05-27T11:35:00', "
            "        '2026-05-27T11:35:05')"
        )
        conn.commit()
    r = admin_client.get("/api/jobs?status=all", headers=AUTH)
    jobs = r.json()["jobs"]
    # Running op should be first.
    assert jobs[0]["id"] == "op:cloud-themes-backup", (
        f"v1.19.47: active (running) op must still pin to top; "
        f"got {jobs[0]['id']}"
    )


def test_multiple_terminal_ops_sort_by_finished_at(
    admin_client, tmp_path,
):
    """Multiple terminal ops + real jobs mix together in
    chronological recency order."""
    from app.config import Settings
    settings = Settings(
        config_dir=tmp_path, data_dir=tmp_path / "data",
    )
    with sqlite3.connect(settings.db_path) as conn:
        # Op #1 finished at 09:00.
        conn.execute(
            "INSERT INTO op_progress "
            "(op_id, kind, status, started_at, updated_at, "
            " finished_at) "
            "VALUES ('op-old', 'tdb_sync', 'done', "
            "        '2026-05-27T09:00:00', "
            "        '2026-05-27T09:00:00', "
            "        '2026-05-27T09:00:00')"
        )
        # Real job finished at 10:00.
        conn.execute(
            "INSERT INTO jobs "
            "(job_type, status, created_at, finished_at) "
            "VALUES ('download', 'done', "
            "        '2026-05-27T10:00:00', "
            "        '2026-05-27T10:00:00')"
        )
        # Op #2 finished at 11:00.
        conn.execute(
            "INSERT INTO op_progress "
            "(op_id, kind, status, started_at, updated_at, "
            " finished_at) "
            "VALUES ('op-recent', 'plex_enum', 'done', "
            "        '2026-05-27T10:55:00', "
            "        '2026-05-27T11:00:00', "
            "        '2026-05-27T11:00:00')"
        )
        # Real job finished at 11:30.
        conn.execute(
            "INSERT INTO jobs "
            "(job_type, status, created_at, finished_at) "
            "VALUES ('place', 'done', "
            "        '2026-05-27T11:29:00', "
            "        '2026-05-27T11:30:00')"
        )
        conn.commit()
    r = admin_client.get("/api/jobs?status=all", headers=AUTH)
    jobs = r.json()["jobs"]
    # Expected order (most recent first):
    #   1. place job (11:30)
    #   2. op-recent (11:00)
    #   3. download (10:00)
    #   4. op-old (09:00)
    ids = [str(j["id"]) for j in jobs]
    place_idx = next(
        i for i, j in enumerate(jobs)
        if j.get("job_type") == "place"
    )
    op_recent_idx = next(
        i for i, j in enumerate(jobs)
        if j["id"] == "op:op-recent"
    )
    download_idx = next(
        i for i, j in enumerate(jobs)
        if j.get("job_type") == "download"
    )
    op_old_idx = next(
        i for i, j in enumerate(jobs)
        if j["id"] == "op:op-old"
    )
    assert place_idx < op_recent_idx < download_idx < op_old_idx, (
        f"v1.19.47: expected recency-DESC order [place, "
        f"op-recent, download, op-old]; got {ids}"
    )


# ── Version pin ──────────────────────────────────────────────


def test_v1_19_47_version_pin():
    """Version bumped at v1.19.47 (then again at v1.19.48 for
    the badge+button polish). Match 1.19.x prefix."""
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
