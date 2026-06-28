"""v1.14.77 — suppress the "1 library refresh queued" synth flash on click → worker-pickup window.

the user: "when only one refresh is going it will briefly flash 1
queued in the status bar."

The v1.13.87 plex_enum_pending synth row was emitted whenever
ANY plex_enum job had status='pending'. The intent was to
surface "queued behind a running enum" — but it also fired for
the brief click → worker-pickup gap (~1-2s while the worker's
idle wait elapses) on a single-refresh click. During that gap:
  pending_n = 1, running_n = 0
  → synth fires: "1 library refresh queued"
  → topbar mini-bar shows it for ~1s
  → worker claims the job → pending_n=0, running_n=1
  → synth disappears, real op_progress takes over

The "queued" framing is wrong when nothing's running yet —
it's just "about to start." The optimistic placeholder
(`// REFRESHING ...`) already covers that gap with the right
framing.

## Fix

Gate the synth on a REAL-queue condition:
  - 2+ pending stacked, OR
  - 1+ pending AND 1+ running (queued behind a running)

The single-pending-no-running case (the user's flash) suppresses.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from app.core import db as core_db  # noqa: E402
from app.core.progress import load_active  # noqa: E402


def _seed_jobs(db_path: Path,
               pending: int = 0,
               running: int = 0) -> None:
    """Insert N pending + M running plex_enum jobs into a
    motif-shaped DB so load_active sees them."""
    with sqlite3.connect(db_path) as conn:
        for _ in range(pending):
            conn.execute(
                "INSERT INTO jobs (job_type, status, payload, "
                " created_at, next_run_at) "
                "VALUES ('plex_enum', 'pending', '{}', "
                " '2025-01-01T00:00:00Z', '2025-01-01T00:00:00Z')"
            )
        for _ in range(running):
            conn.execute(
                "INSERT INTO jobs (job_type, status, payload, "
                " created_at, next_run_at, started_at) "
                "VALUES ('plex_enum', 'running', '{}', "
                " '2025-01-01T00:00:00Z', '2025-01-01T00:00:00Z', "
                " '2025-01-01T00:00:00Z')"
            )
        conn.commit()


def _has_pending_synth(rows: list[dict]) -> bool:
    return any(
        r.get("kind") == "plex_enum_pending"
        for r in rows
    )


# ── The bug case: single pending, no running → MUST NOT emit ───


def test_single_pending_no_running_does_not_emit_synth(tmp_path):
    """the user's flash: 1 pending, 0 running (the brief click →
    worker-pickup window). Pre-fix this fired the synth and
    showed "1 LIBRARY REFRESH QUEUED" for ~1s. v1.14.77
    suppresses since there's no real queue — the optimistic
    placeholder already covers this state."""
    db = tmp_path / "motif.db"
    core_db.init_db(db)
    _seed_jobs(db, pending=1, running=0)
    rows = load_active(db)
    assert not _has_pending_synth(rows), (
        "Single pending with no running should NOT emit the "
        "'queued' synth — that's the v1.14.77 flash fix."
    )


# ── Real-queue cases: MUST still emit ─────────────────────────


def test_pending_behind_running_emits_synth(tmp_path):
    """1 pending + 1 running = real queue (the v1.13.87 intent
    case). the user's original repro: click REFRESH on lib A,
    navigate to lib B, click REFRESH again. Synth must still
    fire so the user sees the queued lib B."""
    db = tmp_path / "motif.db"
    core_db.init_db(db)
    _seed_jobs(db, pending=1, running=1)
    rows = load_active(db)
    assert _has_pending_synth(rows), (
        "Pending behind running must emit the synth — v1.13.87's "
        "original use case (queued behind running)."
    )
    # Label reflects the count of pending (not running + pending).
    synth = next(r for r in rows
                 if r.get("kind") == "plex_enum_pending")
    assert "1 library refresh queued" in synth["stage_label"]


def test_two_or_more_pending_emits_synth(tmp_path):
    """2+ pending stacked = genuine queue depth (e.g., user
    queued multiple library scans before the worker started any).
    Synth fires with the actual count."""
    db = tmp_path / "motif.db"
    core_db.init_db(db)
    _seed_jobs(db, pending=3, running=0)
    rows = load_active(db)
    assert _has_pending_synth(rows)
    synth = next(r for r in rows
                 if r.get("kind") == "plex_enum_pending")
    assert "3 library refreshes queued" in synth["stage_label"]


# ── No-job cases: MUST NOT emit ───────────────────────────────


def test_zero_pending_zero_running_does_not_emit_synth(tmp_path):
    """Empty queue → no synth. Sanity guard."""
    db = tmp_path / "motif.db"
    core_db.init_db(db)
    rows = load_active(db)
    assert not _has_pending_synth(rows)


def test_only_running_no_pending_does_not_emit_synth(tmp_path):
    """1 running, 0 pending → no synth. The real op_progress
    row covers it."""
    db = tmp_path / "motif.db"
    core_db.init_db(db)
    _seed_jobs(db, pending=0, running=1)
    rows = load_active(db)
    assert not _has_pending_synth(rows)


# ── Cross-check: the v1.13.87 intent (queue visibility) holds ─


def test_v1_13_87_intent_preserved_for_real_queues(tmp_path):
    """Belt-and-suspenders: the v1.13.87 marker rationale was
    'click REFRESH on lib A, navigate to lib B and click REFRESH
    again, the second job pendings behind the first and the
    drawer shows nothing about it.' v1.14.77 must NOT regress
    that — pending+running shapes still emit."""
    db = tmp_path / "motif.db"
    core_db.init_db(db)
    # Multi-pending behind running: should still surface.
    _seed_jobs(db, pending=2, running=1)
    rows = load_active(db)
    assert _has_pending_synth(rows)
