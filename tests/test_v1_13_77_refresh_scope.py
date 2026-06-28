"""v1.13.77 — dashboard REFRESH PLEX no longer leaks into TDB SYNC.

Three sub-bugs in the dashboard REFRESH PLEX flow, all sharing the
"global busy signal scoped too widely" pattern documented in the
v1.13.69 opsSyncActive overreach class:

  Bug A (visual): a cyan ::after dot pulsed beside library nav
    items during plex_enum. the user flagged it as visual noise —
    the topbar mini-bar + button label already carry the signal.

  Bug B (logic): clicking dashboard // REFRESH PLEX flipped
    dashboard // SYNC THEMERRDB to // SYNCING… because both locks
    used globalEnumPipeline (which includes scan_all).
    SYNC THEMERRDB should only lock during a TDB sync OR its
    cascade enum follow-on — manual REFRESH PLEX is independent.

  Bug C (race): library // REFRESH <NAME> button rendered
    clickable for ~1s after the user navigated from dashboard
    // REFRESH PLEX → /movies, until the first /api/stats poll
    caught up. The fix renders the button disabled on initial
    paint when a global refresh is already in flight server-side.

These tests pin the SQL + template behavior. The CSS rule removal
(Bug A) is asserted via a static-text guard.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.core.db import init_db


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@pytest.fixture
def db(tmp_path: Path) -> Path:
    p = tmp_path / "motif.db"
    init_db(p)
    return p


def _enqueue_plex_enum(conn, *, scope: str | None, status: str = "pending"):
    """Insert a plex_enum job with the given scope tag."""
    payload = json.dumps({"scope": scope}) if scope else "{}"
    conn.execute(
        "INSERT INTO jobs (job_type, payload, status, created_at, next_run_at) "
        "VALUES ('plex_enum', ?, ?, ?, ?)",
        (payload, status, _now_iso(), _now_iso()),
    )


def _pipeline_count(db: Path) -> int:
    """Mirror the server-side `_pipeline_in_flight` predicate so the
    template-rendering test pins the SQL shape."""
    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM jobs "
            "WHERE job_type = 'plex_enum' "
            "  AND status IN ('pending','running') "
            "  AND json_extract(payload, '$.scope') IN ('cascade','scan_all')"
        ).fetchone()
    return row[0]


def _cascade_count(db: Path) -> int:
    """Mirror the new server-side cascade-only count for /api/stats."""
    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM jobs "
            "WHERE job_type = 'plex_enum' "
            "  AND status IN ('pending','running') "
            "  AND json_extract(payload, '$.scope') = 'cascade'"
        ).fetchone()
    return row[0]


# ── Bug B: cascade-only count for SYNC THEMERRDB lock ─────────

def test_cascade_count_includes_cascade_jobs(db):
    """Sync→enum follow-on (scope=cascade) lights up the cascade
    count → dash SYNC THEMERRDB stays locked through the cascade
    phase even after themerrdbBusy flips false."""
    with sqlite3.connect(db) as conn:
        _enqueue_plex_enum(conn, scope="cascade")
    assert _cascade_count(db) == 1


def test_cascade_count_excludes_scan_all_jobs(db):
    """Manual REFRESH PLEX (scope=scan_all) must NOT light up the
    cascade count — the bug was that dash SYNC THEMERRDB locked
    for it. After v1.13.77, only true cascades count."""
    with sqlite3.connect(db) as conn:
        _enqueue_plex_enum(conn, scope="scan_all")
    assert _cascade_count(db) == 0


def test_cascade_count_excludes_per_section_jobs(db):
    """Per-library // REFRESH <NAME> (no scope tag) is independent."""
    with sqlite3.connect(db) as conn:
        _enqueue_plex_enum(conn, scope=None)
    assert _cascade_count(db) == 0


def test_cascade_count_excludes_completed_jobs(db):
    """Status filter pins to pending/running only."""
    with sqlite3.connect(db) as conn:
        _enqueue_plex_enum(conn, scope="cascade", status="done")
        _enqueue_plex_enum(conn, scope="cascade", status="failed")
        _enqueue_plex_enum(conn, scope="cascade", status="cancelled")
    assert _cascade_count(db) == 0


def test_cascade_and_scan_all_partition_cleanly(db):
    """Mixed population: cascade and scan_all coexist; only
    cascade counts toward the SYNC THEMERRDB lock signal."""
    with sqlite3.connect(db) as conn:
        _enqueue_plex_enum(conn, scope="cascade")
        _enqueue_plex_enum(conn, scope="cascade")
        _enqueue_plex_enum(conn, scope="scan_all")
    assert _cascade_count(db) == 2
    assert _pipeline_count(db) == 3  # both kinds


# ── Bug C: pipeline_in_flight predicate for template ──────────

def test_pipeline_count_includes_cascade_and_scan_all(db):
    """The library template-locking signal must include BOTH
    cascade (sync follow-on) and scan_all (manual REFRESH PLEX) —
    both warrant locking the per-tab refresh button until the
    sweep ends."""
    with sqlite3.connect(db) as conn:
        _enqueue_plex_enum(conn, scope="cascade")
        _enqueue_plex_enum(conn, scope="scan_all")
    assert _pipeline_count(db) == 2


def test_pipeline_count_excludes_per_section_jobs(db):
    """Per-section // REFRESH <NAME> (no scope tag) doesn't lock
    OTHER tabs' buttons — only its own (handled by myTabBusy on
    the JS side, not the SSR pipeline_in_flight signal)."""
    with sqlite3.connect(db) as conn:
        _enqueue_plex_enum(conn, scope=None)
    assert _pipeline_count(db) == 0


# ── Bug A: nav-busy ::after dot removed ───────────────────────

def test_nav_busy_after_pseudo_removed():
    """v1.13.77 dropped the `.nav a.nav-busy::after` rule that
    drew the cyan pulsing dot. The .nav-busy class itself stays
    (cheap, harmless) but the visible ::after is gone. Static
    guard against re-adding the rule."""
    css = (Path(__file__).resolve().parent.parent
           / "app" / "web" / "static" / "app.css").read_text()
    # The deleted rule's signature.
    assert ".nav a.nav-busy::after" not in css, (
        "v1.13.77 removed the cyan nav-busy ::after dot — "
        "re-adding it would resurrect the visual noise the user flagged"
    )


# ── Bug B: dash SYNC THEMERRDB lock no longer uses globalEnumPipeline ─

def test_dash_sync_btn_busy_uses_cascade_signal_not_pipeline():
    """v1.13.77 narrowed the SYNC THEMERRDB lock from
    `themerrdbBusy || globalEnumPipeline` to
    `themerrdbBusy || cascadeInFlight`. Static guard against
    a regression that re-widens the lock and resurrects the
    'manual REFRESH PLEX makes SYNC THEMERRDB show as syncing' bug."""
    js = (Path(__file__).resolve().parent.parent
          / "app" / "web" / "static" / "app.js").read_text()
    assert "const dashSyncBtnBusy = themerrdbBusy || cascadeInFlight" in js
    # Old broken pattern must NOT reappear.
    assert "dashSyncBtnBusy = themerrdbBusy || globalEnumPipeline" not in js


def test_cascade_signal_reads_from_api_stats():
    """The cascadeInFlight var must be derived from
    q.plex_enum_cascade_in_flight (the new /api/stats field).
    Static guard so a future refactor doesn't break the wiring."""
    js = (Path(__file__).resolve().parent.parent
          / "app" / "web" / "static" / "app.js").read_text()
    assert "q.plex_enum_cascade_in_flight" in js


# ── Bug C: library template renders button disabled when busy ─

def test_library_template_renders_button_disabled_under_pipeline():
    """The library.html refresh button must include a `{% if
    pipeline_in_flight %}disabled{% endif %}` guard so the SSR
    paint matches the JS-driven busy state. Verified by string
    match on the template — no Jinja env needed."""
    html = (Path(__file__).resolve().parent.parent
            / "app" / "web" / "templates" / "library.html").read_text()
    # Pin the literal Jinja conditional + the busy label.
    assert "{% if pipeline_in_flight %}disabled{% endif %}" in html
    assert "{% if pipeline_in_flight %}// REFRESHING…" in html


def test_api_stats_response_includes_cascade_count_field():
    """The /api/stats response must surface plex_enum_cascade_in_flight
    so the JS layer can read it. Pin the response shape."""
    api_py = (Path(__file__).resolve().parent.parent
              / "app" / "web" / "api.py").read_text()
    # Pin both the SELECT alias and the JSON output key.
    assert "AS plex_enum_cascade_in_flight" in api_py
    assert '"plex_enum_cascade_in_flight": row["plex_enum_cascade_in_flight"]' in api_py
