"""v0.51.63 — a library-section refresh no longer locks the /collections REFRESH.

the user: "when doing a refresh of a library section only it shouldn't lock the
collection refresh button." The button locked on the global plex_enum_in_flight
(any enum). A /collections refresh tags its plex_enum jobs scope='collections';
a section refresh tags scope='movies'/'tv'/'anime'. /api/stats now surfaces a
COLLECTIONS-scoped count and the button gates on that, so a section-only refresh
doesn't trip it. The count still stays true through the collection refresh's
reconcile stage (reconcile is a stage INSIDE the plex_enum job), so it doesn't
re-enable mid-scan (the v0.50.14 concern).
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.core.db import init_db


REPO = Path(__file__).resolve().parent.parent


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@pytest.fixture
def db(tmp_path: Path) -> Path:
    p = tmp_path / "motif.db"
    init_db(p)
    return p


def _enqueue_plex_enum(conn, *, scope, status: str = "pending"):
    payload = json.dumps({"scope": scope}) if scope else "{}"
    conn.execute(
        "INSERT INTO jobs (job_type, payload, status, created_at, next_run_at) "
        "VALUES ('plex_enum', ?, ?, ?, ?)",
        (payload, status, _now_iso(), _now_iso()),
    )


def _collections_count(db: Path) -> int:
    """Mirror the server-side collections-scoped count in /api/stats."""
    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM jobs "
            "WHERE job_type = 'plex_enum' "
            "  AND status IN ('pending','running') "
            "  AND json_extract(payload, '$.scope') = 'collections'"
        ).fetchone()
    return row[0]


def test_collections_scope_counts(db):
    with sqlite3.connect(db) as conn:
        _enqueue_plex_enum(conn, scope="collections")
    assert _collections_count(db) == 1


def test_section_refresh_does_not_count(db):
    """THE FIX: a plain library-section refresh (scope movies/tv/anime) must NOT
    light up the collections count — that's what used to lock the button."""
    with sqlite3.connect(db) as conn:
        _enqueue_plex_enum(conn, scope="movies")
        _enqueue_plex_enum(conn, scope="tv")
        _enqueue_plex_enum(conn, scope="anime")
        _enqueue_plex_enum(conn, scope="movies-4k")
        _enqueue_plex_enum(conn, scope="movies-all")
    assert _collections_count(db) == 0


def test_pipeline_scopes_do_not_count(db):
    """cascade/scan_all lock collections via the separate pipelineBusy check, not
    this count — so they must not be double-counted here."""
    with sqlite3.connect(db) as conn:
        _enqueue_plex_enum(conn, scope="cascade")
        _enqueue_plex_enum(conn, scope="scan_all")
    assert _collections_count(db) == 0


def test_untagged_and_completed_excluded(db):
    with sqlite3.connect(db) as conn:
        _enqueue_plex_enum(conn, scope=None)
        _enqueue_plex_enum(conn, scope="collections", status="done")
        _enqueue_plex_enum(conn, scope="collections", status="failed")
        _enqueue_plex_enum(conn, scope="collections", status="cancelled")
    assert _collections_count(db) == 0


def test_running_counts_through_reconcile(db):
    """A running collections job (its reconcile stage keeps it 'running') keeps
    the count > 0 — so the button doesn't re-enable mid-scan."""
    with sqlite3.connect(db) as conn:
        _enqueue_plex_enum(conn, scope="collections", status="running")
    assert _collections_count(db) == 1


def test_api_stats_exposes_the_count():
    api_py = (REPO / "app" / "web" / "api.py").read_text()
    assert "AS plex_enum_collections_in_flight" in api_py
    assert '"plex_enum_collections_in_flight": row["plex_enum_collections_in_flight"]' in api_py


def test_js_gates_collections_button_on_the_scoped_count():
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert "const collectionsEnumBusy = q.plex_enum_collections_in_flight > 0;" in js
    assert "? collectionsEnumBusy" in js


def test_v0_51_63_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
