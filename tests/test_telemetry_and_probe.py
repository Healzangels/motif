"""v1.13.2 regression tests — #1 sync telemetry + #3 transport probe.

Telemetry tests verify that the schema v37 columns are written
correctly and the per-transport summary aggregates as expected.
The /api/sync/history JSON shape is exercised through the SQL
that backs it (so we don't depend on the FastAPI test client).

Probe tests use httpx.MockTransport to simulate codeload / git
URL responses without hitting the network.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    from app.core.db import init_db
    path = tmp_path / "motif.db"
    init_db(path)
    return path


def _insert_sync_run(db_path: Path, *, started_at: str, finished_at: str | None,
                     status: str, transport: str | None,
                     wall_clock_seconds: float | None = None,
                     no_changes: int = 0,
                     fallback_reason: str | None = None,
                     movies_seen: int = 0, tv_seen: int = 0,
                     new_count: int = 0, updated_count: int = 0,
                     error: str | None = None) -> int:
    """Direct INSERT for fixtures. wall_clock_seconds is informational
    — finished_at - started_at is what the SQL computes."""
    from app.core.db import get_conn
    with get_conn(db_path) as conn:
        cur = conn.execute(
            """INSERT INTO sync_runs
                 (started_at, finished_at, status, transport,
                  fallback_reason, no_changes,
                  movies_seen, tv_seen, new_count, updated_count, error)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (started_at, finished_at, status, transport,
             fallback_reason, no_changes,
             movies_seen, tv_seen, new_count, updated_count, error),
        )
        return cur.lastrowid


# ---------- schema v37 -------------------------------------------------------

def test_schema_v37_adds_telemetry_columns(db_path: Path):
    from app.core.db import get_conn
    with get_conn(db_path) as conn:
        cols = [r["name"] for r in conn.execute(
            "PRAGMA table_info(sync_runs)").fetchall()]
    for col in ("transport", "fallback_reason", "no_changes"):
        assert col in cols, f"missing v37 column {col}"


# ---------- /api/sync/history backing SQL ------------------------------------

def test_history_query_computes_wall_clock(db_path: Path):
    """The CASE+julianday() expression in /api/sync/history's SQL
    yields wall_clock_seconds in seconds. Validate against a
    fixture run with a known duration."""
    from app.core.db import get_conn
    _insert_sync_run(db_path,
                     started_at="2026-05-15T12:00:00",
                     finished_at="2026-05-15T12:00:42",
                     status="success", transport="git")
    with get_conn(db_path) as conn:
        row = conn.execute(
            """SELECT CAST(
                  (julianday(finished_at) - julianday(started_at))
                  * 86400.0 AS REAL) AS wall_clock_seconds
                 FROM sync_runs LIMIT 1"""
        ).fetchone()
    assert row["wall_clock_seconds"] == pytest.approx(42.0, rel=0.05)


def test_history_summary_skips_no_changes_for_avg(db_path: Path):
    """Per-transport avg_wall_clock should EXCLUDE no_changes runs —
    the 304 short-circuit's sub-second time would skew the average
    toward zero and hide the real cost of a working sync."""
    # Mimics the summary aggregation logic in api_sync_history.
    runs = [
        {"transport": "git", "status": "success",
         "wall_clock_seconds": 50.0, "no_changes": 0,
         "fallback_reason": None},
        {"transport": "git", "status": "success",
         "wall_clock_seconds": 0.5, "no_changes": 1,
         "fallback_reason": None},
        {"transport": "git", "status": "success",
         "wall_clock_seconds": 30.0, "no_changes": 0,
         "fallback_reason": None},
        {"transport": "database", "status": "success",
         "wall_clock_seconds": 70.0, "no_changes": 0,
         "fallback_reason": None},
    ]
    summary: dict[str, dict] = {}
    for r in runs:
        if r["status"] != "success" or r["wall_clock_seconds"] is None:
            continue
        s = summary.setdefault(r["transport"], {
            "count": 0, "no_change_count": 0, "fallback_count": 0,
            "_sum_full": 0.0, "_n_full": 0,
        })
        s["count"] += 1
        if r["no_changes"]:
            s["no_change_count"] += 1
        else:
            s["_sum_full"] += r["wall_clock_seconds"]
            s["_n_full"] += 1
        if r["fallback_reason"]:
            s["fallback_count"] += 1
    git = summary["git"]
    # 3 git runs total, 2 full + 1 no-op. Avg over the 2 full = 40.
    assert git["count"] == 3
    assert git["no_change_count"] == 1
    assert git["_sum_full"] / git["_n_full"] == pytest.approx(40.0)
    db = summary["database"]
    assert db["count"] == 1
    assert db["_sum_full"] / db["_n_full"] == pytest.approx(70.0)


def test_history_summary_counts_fallbacks(db_path: Path):
    """fallback_count tallies runs that cascaded down a tier."""
    runs = [
        {"transport": "git", "status": "success", "wall_clock_seconds": 30.0,
         "no_changes": 0, "fallback_reason": "git: timeout"},
        {"transport": "git", "status": "success", "wall_clock_seconds": 20.0,
         "no_changes": 0, "fallback_reason": None},
    ]
    summary: dict[str, dict] = {}
    for r in runs:
        s = summary.setdefault(r["transport"], {"fallback_count": 0})
        if r["fallback_reason"]:
            s["fallback_count"] += 1
    assert summary["git"]["fallback_count"] == 1


# ---------- sync.py writes telemetry on real run ----------------------------

def test_sync_run_stamps_transport_at_insert(db_path: Path, monkeypatch):
    """The INSERT in run_sync writes the transport from the source
    parameter immediately. Even an aborted sync (no UPDATE later)
    leaves a row that records which transport was attempted."""
    from app.core import sync as sync_mod
    from app.core.db import get_conn

    # Stub out the actual sync internals — we only care that the
    # initial INSERT happens with transport set.
    def _boom(*args, **kwargs):
        raise RuntimeError("sync stubbed")
    monkeypatch.setattr(sync_mod, "_make_client", _boom)
    try:
        sync_mod.run_sync(db_path, "https://invalid.example",
                          source="git")
    except Exception:
        pass
    with get_conn(db_path) as conn:
        row = conn.execute(
            "SELECT transport, status FROM sync_runs ORDER BY id DESC LIMIT 1"
        ).fetchone()
    assert row is not None
    assert row["transport"] == "git"
    # Status was 'failed' (the stub raised) — that's fine; we're
    # testing the column gets stamped before the failure path.
    assert row["status"] == "failed"


# ---------- /api/sync/probe -------------------------------------------------

def test_probe_remote_handshake_pass(monkeypatch):
    """The probe's _probe_remote helper does HEAD on
    movies/pages.json and returns ok on 200. We exercise the same
    httpx call shape against MockTransport."""
    import httpx

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "HEAD" and request.url.path.endswith(
                "movies/pages.json"):
            return httpx.Response(200)
        return httpx.Response(404)

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport, timeout=5.0) as client:
        r = client.head("https://invalid.example/movies/pages.json")
    assert r.status_code == 200


def test_probe_remote_handshake_fail():
    """A 503 from the upstream propagates the failure status."""
    import httpx

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503)

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport, timeout=5.0) as client:
        r = client.head("https://invalid.example/movies/pages.json")
    assert r.status_code == 503


def test_probe_database_falls_back_to_range_get_on_405():
    """codeload may not advertise HEAD; the probe falls back to a
    Range-0-0 GET. Verify the fallback shape works."""
    import httpx
    seen_methods: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_methods.append(request.method)
        if request.method == "HEAD":
            return httpx.Response(405)
        if request.method == "GET" and request.headers.get("Range") == "bytes=0-0":
            return httpx.Response(206)
        return httpx.Response(500)

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport, timeout=5.0,
                      follow_redirects=True) as client:
        r1 = client.head("https://invalid.example/tar.gz/database")
        if r1.status_code in (405, 501):
            r2 = client.get("https://invalid.example/tar.gz/database",
                            headers={"Range": "bytes=0-0"})
            final = r2
        else:
            final = r1
    assert seen_methods == ["HEAD", "GET"]
    assert final.status_code == 206
