"""v0.51.158 — the read-only LOUDNESS AUDIT sweep (loudness feature, Phase 0/C).

app/core/loudness_audit.py walks motif's local-bytes themes, measures each with
loudness.measure_loudness (ffmpeg, stubbed here — container-only) and stores the
result on the local_files row. Verifies:
  - rows_needing_measure: selects hashed local_files, skips sha-current rows, keeps
    never-measured + changed-sha rows.
  - record_measurement: stamps the row's own PK only (edition-scoped — no bleed).
  - run_loudness_audit: measures the needing rows, stores successes, counts failures
    (measure→None) + already-current + no-sha, all in the summary.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from app.core import loudness_audit
from app.core.db import get_conn, init_db


def _seed(conn, mt, tid, sec, ed, sha, *, measured_sha=None, path=None):
    conn.execute(
        "INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key, "
        "file_path, file_sha256, downloaded_at, source_video_id, "
        "loudness_measured_sha256) "
        "VALUES (?, ?, ?, ?, ?, ?, '2026-07-15', 'vid', ?)",
        (mt, tid, sec, ed, path or f"{mt}/{tid}/theme.mp3", sha, measured_sha),
    )


@pytest.fixture
def db(tmp_path):
    p = tmp_path / "motif.db"
    init_db(p)
    return p


def test_rows_needing_measure_selects_and_skips(db):
    with get_conn(db) as conn:
        conn.execute("PRAGMA foreign_keys = OFF")
        _seed(conn, "movie", 1, "1", "", "a1")                     # never measured
        _seed(conn, "movie", 2, "1", "", "b1", measured_sha="OLD")  # sha changed
        _seed(conn, "movie", 3, "1", "", "c1", measured_sha="c1")   # already current
        _seed(conn, "movie", 4, "1", "", None)                     # no sha → skipped
        conn.commit()
        rows = loudness_audit.rows_needing_measure(conn)
    tids = sorted(r["tmdb_id"] for r in rows)
    assert tids == [1, 2]  # current (3) + no-sha (4) excluded


def test_rows_needing_measure_remeasure_all(db):
    with get_conn(db) as conn:
        conn.execute("PRAGMA foreign_keys = OFF")
        _seed(conn, "movie", 3, "1", "", "c1", measured_sha="c1")  # already current
        conn.commit()
        rows = loudness_audit.rows_needing_measure(conn, remeasure_all=True)
    assert [r["tmdb_id"] for r in rows] == [3]  # forced back in


def test_record_measurement_is_edition_scoped(db):
    with get_conn(db) as conn:
        conn.execute("PRAGMA foreign_keys = OFF")
        _seed(conn, "movie", 7, "1", "", "std")
        _seed(conn, "movie", 7, "1", "director", "dir")
        conn.commit()
        std_row = conn.execute(
            "SELECT * FROM local_files WHERE tmdb_id=7 AND edition_key=''"
        ).fetchone()
        loudness_audit.record_measurement(
            conn, std_row,
            {"loudness_i": -18.0, "true_peak": -2.0, "lra": 6.0}, "2026-07-15T00:00:00Z",
        )
        conn.commit()
        std = conn.execute(
            "SELECT loudness_i FROM local_files WHERE tmdb_id=7 AND edition_key=''"
        ).fetchone()
        other = conn.execute(
            "SELECT loudness_i FROM local_files WHERE tmdb_id=7 AND edition_key='director'"
        ).fetchone()
    assert std["loudness_i"] == -18.0
    assert other["loudness_i"] is None  # sibling edition untouched


def test_run_loudness_audit_measures_stores_and_counts(db, monkeypatch):
    with get_conn(db) as conn:
        conn.execute("PRAGMA foreign_keys = OFF")
        _seed(conn, "movie", 1, "1", "", "a1", path="good/a.mp3")            # → measured
        _seed(conn, "movie", 2, "1", "", "b1", path="bad/b.mp3")            # → failed
        _seed(conn, "movie", 3, "1", "", "c1", measured_sha="c1", path="good/c.mp3")  # current
        _seed(conn, "movie", 4, "1", "", None, path="good/d.mp3")          # no sha
        conn.commit()

    def _stub(path, *a, **k):
        return None if "bad" in str(path) else {"loudness_i": -20.0, "true_peak": -3.0, "lra": 5.0}

    monkeypatch.setattr(loudness_audit, "measure_loudness", _stub)
    summary = loudness_audit.run_loudness_audit(db, themes_dir=None)

    assert summary["to_measure"] == 2
    assert summary["measured"] == 1
    assert summary["failed"] == 1
    assert summary["total_local_bytes"] == 3   # rows 1,2,3 (with sha)
    assert summary["already_current"] == 1     # row 3
    assert summary["skipped_no_sha"] == 1      # row 4

    with get_conn(db) as conn:
        good = conn.execute(
            "SELECT loudness_i, loudness_tp, loudness_lra, loudness_measured_sha256 "
            "FROM local_files WHERE tmdb_id=1"
        ).fetchone()
        bad = conn.execute(
            "SELECT loudness_i, loudness_measured_at FROM local_files WHERE tmdb_id=2"
        ).fetchone()
    assert good["loudness_i"] == -20.0 and good["loudness_tp"] == -3.0
    assert good["loudness_lra"] == 5.0
    assert good["loudness_measured_sha256"] == "a1"  # stamped from file_sha256
    assert bad["loudness_i"] is None and bad["loudness_measured_at"] is None  # failed, unstamped


def test_run_loudness_audit_progress_callback(db, monkeypatch):
    with get_conn(db) as conn:
        conn.execute("PRAGMA foreign_keys = OFF")
        _seed(conn, "movie", 1, "1", "", "a1", path="good/a.mp3")
        _seed(conn, "movie", 2, "1", "", "b1", path="good/b.mp3")
        conn.commit()
    monkeypatch.setattr(
        loudness_audit, "measure_loudness",
        lambda p, *a, **k: {"loudness_i": -20.0, "true_peak": -3.0, "lra": 5.0},
    )
    seen = []
    loudness_audit.run_loudness_audit(
        db, themes_dir=None, progress_cb=lambda d, t: seen.append((d, t)),
    )
    assert seen[0] == (0, 2)   # initial
    assert seen[-1] == (2, 2)  # final done == total


# ── endpoint wiring (backend→frontend contract, v1.18.81 lesson) ──────────────

import threading  # noqa: E402
import time  # noqa: E402

from fastapi.testclient import TestClient  # noqa: E402

import app.web.api as apimod  # noqa: E402
from app.core.auth import create_admin, init_auth_schema  # noqa: E402

FWD_HDR = {"X-Authentik-Username": "testadmin"}


@pytest.fixture(autouse=True)
def _reset_loudness_state():
    # module-level (process-shared) op state — reset between tests.
    apimod._LOUDNESS_AUDIT_STATE.clear()
    apimod._LOUDNESS_AUDIT_STATE.update(status="idle")
    yield
    apimod._LOUDNESS_AUDIT_STATE.clear()
    apimod._LOUDNESS_AUDIT_STATE.update(status="idle")


def _make_app(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="testadmin", password="testpassword")
    return TestClient(apimod.create_app(settings))


def _poll_until(client, pred, tries=100, delay=0.03):
    st = None
    for _ in range(tries):
        st = client.get("/api/admin/loudness-audit/status", headers=FWD_HDR).json()
        if pred(st):
            return st
        time.sleep(delay)
    return st


def test_endpoint_status_idle_before_run(tmp_path, monkeypatch):
    client = _make_app(tmp_path, monkeypatch)
    st = client.get("/api/admin/loudness-audit/status", headers=FWD_HDR).json()
    assert st["status"] == "idle"
    assert not st.get("summary")


def test_endpoint_start_progress_alreadyrunning_then_done(tmp_path, monkeypatch):
    gate = threading.Event()

    def fake_run(db_path, *, themes_dir=None, progress_cb=None, **k):
        if progress_cb:
            progress_cb(1, 2)
        gate.wait(timeout=5)
        if progress_cb:
            progress_cb(2, 2)
        return {"to_measure": 2, "measured": 2, "failed": 0,
                "total_local_bytes": 5, "already_current": 3, "skipped_no_sha": 0}

    monkeypatch.setattr(loudness_audit, "run_loudness_audit", fake_run)
    client = _make_app(tmp_path, monkeypatch)

    r = client.post("/api/admin/loudness-audit/start", headers=FWD_HDR)
    assert r.status_code == 200 and r.json()["started"] is True

    st = _poll_until(client, lambda s: s.get("done") == 1 and s["status"] == "running")
    assert st["status"] == "running" and st["done"] == 1 and st["total"] == 2

    # a second start while running is a no-op
    r2 = client.post("/api/admin/loudness-audit/start", headers=FWD_HDR)
    assert r2.json() == {"ok": True, "started": False, "already_running": True}

    gate.set()
    done = _poll_until(client, lambda s: s["status"] == "done")
    assert done["status"] == "done"
    assert done["summary"]["measured"] == 2
    assert done["summary"]["total_local_bytes"] == 5


def test_endpoint_requires_admin(tmp_path, monkeypatch):
    client = _make_app(tmp_path, monkeypatch)
    # no forward-auth header → rejected (not 200)
    assert client.post("/api/admin/loudness-audit/start").status_code != 200
    assert client.get("/api/admin/loudness-audit/status").status_code != 200
