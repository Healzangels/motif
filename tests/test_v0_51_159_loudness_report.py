"""v0.51.159 — the LOUDNESS AUDIT report view (loudness feature, Phase 0/D).

loudness_audit.build_report() aggregates the stored v72 measurements into the shape
the /admin/loudness page renders: distribution stats, a fixed-bin histogram,
loudest/quietest outliers (with identity for INFO-card deep-links), and a compact
value array for the client-side target-preview slider. Verifies the aggregation +
the report endpoint contract + the page route.
"""
from __future__ import annotations

import threading  # noqa: F401 (parity with sibling test's import block)
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import app.web.api as apimod
from app.core.auth import create_admin, init_auth_schema
from app.core.db import get_conn, init_db
from app.core import loudness_audit

FWD_HDR = {"X-Authentik-Username": "testadmin"}


def _seed_measured(conn, tid, i, tp, *, ek=""):
    conn.execute(
        "INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key, "
        "file_path, file_sha256, downloaded_at, source_video_id, loudness_i, loudness_tp) "
        "VALUES ('movie', ?, '1', ?, ?, ?, '2026-07-15', 'v', ?, ?)",
        (tid, ek, f"movie/{tid}.mp3", f"sha{tid}", i, tp),
    )


@pytest.fixture
def db(tmp_path):
    p = tmp_path / "motif.db"
    init_db(p)
    return p


# ── aggregation ──────────────────────────────────────────────────────────────

def test_build_report_stats_and_outliers(db):
    with get_conn(db) as conn:
        conn.execute("PRAGMA foreign_keys = OFF")
        for tid, i, tp in [(1, -14.0, -1.0), (2, -31.0, -9.0),
                           (3, -23.0, -5.0), (4, -24.0, -6.0), (5, -22.5, -4.0)]:
            _seed_measured(conn, tid, i, tp)
        # a hashed-but-unmeasured row + a no-sha row (excluded from measured/total)
        conn.execute(
            "INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key, "
            "file_path, file_sha256, downloaded_at, source_video_id) "
            "VALUES ('movie', 9, '1', '', 'movie/9.mp3', 'sha9', '2026-07-15', 'v')")
        conn.commit()
        rep = loudness_audit.build_report(conn)

    assert rep["measured"] == 5
    assert rep["unmeasured"] == 1        # row 9 (hashed, not yet measured)
    assert rep["total_local_bytes"] == 6
    assert rep["stats"]["min"] == -31.0 and rep["stats"]["max"] == -14.0
    assert rep["stats"]["median"] == -23.0
    # LUFS: higher (closer to 0) = louder
    assert rep["loudest"][0]["loudness_i"] == -14.0
    assert rep["quietest"][0]["loudness_i"] == -31.0
    # histogram covers every value
    assert sum(b["count"] for b in rep["histogram"]) == 5
    assert len(rep["values"]) == 5


def test_build_report_empty(db):
    with get_conn(db) as conn:
        rep = loudness_audit.build_report(conn)
    assert rep["measured"] == 0
    assert rep["stats"] is None
    assert rep["histogram"] == []
    assert rep["loudest"] == [] and rep["quietest"] == []


def test_build_report_outlier_has_pk_for_deeplink(db):
    with get_conn(db) as conn:
        conn.execute("PRAGMA foreign_keys = OFF")
        _seed_measured(conn, 42, -12.0, -1.0, ek="director")
        conn.commit()
        rep = loudness_audit.build_report(conn)
    o = rep["loudest"][0]
    # the INFO-card deep-link needs media_type + tmdb_id + section_id.
    assert o["media_type"] == "movie" and o["tmdb_id"] == 42 and o["section_id"] == "1"
    assert o["title"]  # falls back to "movie/42" when no themes row


# ── endpoint + page ──────────────────────────────────────────────────────────

def _make_app(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="testadmin", password="testpassword")
    with get_conn(settings.db_path) as conn:
        conn.execute("PRAGMA foreign_keys = OFF")
        _seed_measured(conn, 1, -18.0, -2.0)
        conn.commit()
    return TestClient(apimod.create_app(settings))


def test_report_endpoint_returns_aggregate(tmp_path, monkeypatch):
    client = _make_app(tmp_path, monkeypatch)
    r = client.get("/api/admin/loudness-report", headers=FWD_HDR)
    assert r.status_code == 200
    body = r.json()
    assert body["measured"] == 1
    assert body["stats"]["median"] == -18.0
    assert body["values"] == [[-18.0, -2.0]]


def test_report_endpoint_requires_admin(tmp_path, monkeypatch):
    client = _make_app(tmp_path, monkeypatch)
    assert client.get("/api/admin/loudness-report").status_code != 200


def test_loudness_page_renders(tmp_path, monkeypatch):
    client = _make_app(tmp_path, monkeypatch)
    r = client.get("/admin/loudness", headers=FWD_HDR)
    assert r.status_code == 200
    assert "LOUDNESS AUDIT" in r.text
    assert 'id="loudness-report"' in r.text
    assert 'id="loud-target"' in r.text  # the target-preview slider
