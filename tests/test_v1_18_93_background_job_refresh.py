"""v1.18.93 — extend row-refresh contract to background-worker jobs.

the user flagged a recurring symptom: queue-worker autonomously
picks up a TDB download (e.g., Cowboys & Aliens), runs the full
download → place → link pipeline server-side, and the row in
/movies stays showing the pre-completion state (amber DL, +P
SRC, no PL/LINK) until hard browser refresh. Topbar reads IDLE
correctly because the queue has drained; the row data the user
is staring at is just stale.

## Root cause

v1.18.52 built the row-refresh contract around three signals:
themerrdb_sync_in_flight + plex_enum_in_flight + op_progress_running.
The accompanying comment explicitly opted per-row jobs OUT with:

    "Per-row jobs already trigger rapid-poll via per-row
    job_in_flight, so they don't need to be in this signal."

That's true for USER-CLICK-INITIATED jobs (the click handler
explicitly calls libraryRapidPoll() inline). For BACKGROUND-
WORKER-INITIATED jobs — TDB queue auto-pick, post-place refresh
chain, etc. — no click handler runs, so nothing fires
libraryRapidPoll. The row's job_in_flight is correctly set on
the server, but it's only CONSULTED during a loadLibrary call;
without rapid-poll, the next loadLibrary is 30 seconds away.

## Fix

Extend `anyMutatingOpActive` to also union four per-job queue
depths that /api/stats already exposes:

  download_in_flight + place_in_flight + scan_in_flight + refresh_in_flight

These are raw `jobs` table counts in ('pending','running') state.
When the queue-worker enqueues an auto-download, the count
flips 0 → 1 → the contract's false→true transition kicks
libraryRapidPoll → row data refreshes on every rapid-poll tick
until the queue drains. True→false fires the one-shot
loadLibrary to capture the final state.

The per-row job_in_flight signal still drives row chip rendering
(amber DL, etc.); this widening is just what makes rapid-poll
FIRE when no click occurred.

## What's pinned

- anyMutatingOpActive includes the new perJobBusy term and all
  four /api/stats fields.
- /api/stats still emits the four per-job _in_flight fields
  (preexisting from v1.11.7).
- End-to-end: inserting a `jobs` row in status='pending' makes
  /api/stats return download_in_flight=1.
- CLAUDE.md Row-refresh contract section documents the widening.
"""
from __future__ import annotations

import re
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"
API_PY = REPO / "app" / "web" / "api.py"
CLAUDE_MD = REPO / "CLAUDE.md"


# ── JS contract: anyMutatingOpActive includes the new term ───


def test_app_js_defines_per_job_busy_term():
    """refreshTopbarStatus must compute a perJobBusy boolean
    from the four per-job _in_flight counts exposed by
    /api/stats."""
    src = APP_JS.read_text()
    assert "perJobBusy" in src, (
        "v1.18.93: app.js must define perJobBusy"
    )
    # v0.50.68: the four queue-depth fields moved into perJobSum (the raw sum,
    # used for the hero-wave intensity level); perJobBusy now DERIVES from it.
    # The row-refresh signal still unions all four counts, just via perJobSum.
    assert "const perJobBusy = perJobSum > 0;" in src
    idx = src.index("perJobSum = (")
    block = src[idx:idx + 600]
    for field in (
        "download_in_flight",
        "place_in_flight",
        "scan_in_flight",
        "refresh_in_flight",
    ):
        assert field in block, (
            f"v1.18.93: perJobSum must include {field}"
        )


def test_any_mutating_op_active_includes_per_job_busy():
    """The unified signal must now union perJobBusy alongside
    the v1.18.52 three (themerrdbBusy/plexEnumBusy/
    opProgressRunning)."""
    src = APP_JS.read_text()
    idx = src.index("anyMutatingOpActive = ")
    block = src[idx:idx + 400]
    # All four signal sources present.
    assert "themerrdbBusy" in block
    assert "plexEnumBusy" in block
    assert "opProgressRunning" in block
    assert "perJobBusy" in block, (
        "v1.18.93: anyMutatingOpActive must include perJobBusy "
        "so background-worker-initiated jobs trip the transition "
        "signal (rapid-poll kick + one-shot loadLibrary)"
    )


def test_marker_references_v1_18_52_lineage():
    """The v1.18.93 marker must explain why the v1.18.52 comment's
    'per-row jobs are covered by job_in_flight' assumption was
    only half-right (true for clicks, false for background jobs)."""
    src = APP_JS.read_text()
    idx = src.index("v1.18.93")
    block = src[idx:idx + 1500]
    # Must reference the click-vs-background distinction so future
    # archaeology sees the WHY without digging.
    assert "background" in block.lower() or "worker" in block.lower(), (
        "v1.18.93: marker should reference the background-worker "
        "case that the v1.18.52 comment missed"
    )


# ── /api/stats still exposes the four per-job depths ─────────


def test_api_stats_exposes_per_job_depths():
    """/api/stats must surface download_in_flight, place_in_flight,
    scan_in_flight, refresh_in_flight in the queue block. These
    have been there since v1.11.7 (download/place) and v1.12.15
    (refresh); v1.18.93 just consumes them in the contract."""
    src = API_PY.read_text()
    for field in (
        '"download_in_flight": row["download_in_flight"]',
        '"place_in_flight": row["place_in_flight"]',
        '"scan_in_flight": row["scan_in_flight"]',
        '"refresh_in_flight": row["refresh_in_flight"]',
    ):
        assert field in src, (
            f"v1.18.93 depends on /api/stats exposing {field}"
        )


# ── Documentation guard ──────────────────────────────────────


def test_claude_md_documents_v1_18_93_widening():
    """CLAUDE.md's Row-refresh contract section must list the four
    per-job _in_flight fields as signal sources, with the v1.18.93
    marker so future readers see the rationale."""
    src = CLAUDE_MD.read_text()
    flat = " ".join(src.split())
    # The new bullet must mention the four fields + v1.18.93.
    assert "download_in_flight" in flat
    assert "place_in_flight" in flat
    assert "scan_in_flight" in flat
    assert "refresh_in_flight" in flat
    assert "v1.18.93" in flat, (
        "v1.18.93: CLAUDE.md must mark the widening with the tag"
    )


# ── End-to-end: jobs row → /api/stats reflects it ────────────


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


def test_api_stats_baseline_zero_per_job_depths(admin_client):
    """Fresh install — every per-job _in_flight count starts at 0."""
    r = admin_client.get("/api/stats", headers=AUTH)
    assert r.status_code == 200
    q = r.json()["queue"]
    for field in (
        "download_in_flight",
        "place_in_flight",
        "scan_in_flight",
        "refresh_in_flight",
    ):
        assert field in q, f"/api/stats must include {field}"
        assert q[field] == 0


def test_api_stats_reflects_pending_download_job(
    admin_client, tmp_path,
):
    """Insert a `jobs` row with job_type='download' and
    status='pending'. /api/stats must count it in
    download_in_flight. This is the end-to-end pin: a background-
    worker-enqueued download flips the signal that v1.18.93 now
    folds into anyMutatingOpActive."""
    from app.config import Settings
    from app.core.db import get_conn
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    with get_conn(settings.db_path) as conn:
        conn.execute(
            "INSERT INTO jobs "
            "  (job_type, status, media_type, tmdb_id, "
            "   created_at) "
            "VALUES ('download', 'pending', 'movie', 12345, "
            "        '2026-05-23T00:00:00Z')"
        )
        conn.commit()

    # /api/stats has a 1s cache TTL — bust it if the first read
    # caught the pre-insert snapshot.
    r = admin_client.get("/api/stats", headers=AUTH)
    q = r.json()["queue"]
    if q["download_in_flight"] == 0:
        time.sleep(1.2)
        r = admin_client.get("/api/stats", headers=AUTH)
        q = r.json()["queue"]
    assert q["download_in_flight"] >= 1, (
        f"v1.18.93: expected download_in_flight >= 1, got "
        f"{q['download_in_flight']}"
    )
