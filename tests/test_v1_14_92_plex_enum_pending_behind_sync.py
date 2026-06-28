"""v1.14.92 — plex_enum_pending synth fires when queued behind sync.

the user: "when you start a themerrdb sync and start just one
library refresh with plex it doesn't show any indicator of it
being queued or a + 1 Queued"

Repro: SYNC THEMERRDB running, user clicks // REFRESH on one
library. The plex_enum job sits as pending in the jobs table
(long-worker is single-threaded for sync/plex_enum/scan).
Pre-fix the topbar showed neither a +N QUEUED badge nor a
drawer card for the queued refresh.

## Root cause

The plex_enum_pending synth gate (introduced v1.13.87,
tightened v1.14.77) only considered plex_enum vs plex_enum:

    is_real_queue = (
        plex_enum_pending_n >= 2
        or (plex_enum_pending_n >= 1
            and plex_enum_running_n >= 1)
    )

the user's case has `plex_enum_pending=1, plex_enum_running=0`
(sync is the running long-worker). Neither condition fires,
synth row never emitted, topbar badge never lights, drawer
card never renders.

Asymmetry with v1.14.90: when I added `tdb_sync_pending` for
the inverse case (sync queued behind plex_enum/scan), I
correctly considered cross-kind blockers — but didn't update
plex_enum_pending's gate to mirror it.

## Fix

Extend the SQL to also pull `sync` / `scan` running counts
into a `plex_enum_other_long_running_n` column. Extend the
real-queue condition to fire when:
  - 2+ plex_enum pending stacked, OR
  - 1+ plex_enum pending AND (1+ plex_enum running
    OR 1+ sync/scan running)

Symmetric with `tdb_sync_pending`'s gate from v1.14.90.

The +N QUEUED badge auto-lights once the synth row appears,
since it reads `queue_depth` from the synth's `detail` dict
(no ops.js change needed).
"""
from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

import pytest

from app.core import db as db_module
from app.core import progress as progress_module


REPO = Path(__file__).resolve().parent.parent
PROGRESS_PY = REPO / "app" / "core" / "progress.py"


# ── Static-text guards on the gate ─────────────────────────────


def test_plex_enum_pending_gate_includes_other_long_running():
    """The plex_enum_pending real-queue gate must reference the
    sync/scan running count. Pre-fix the gate only checked
    plex_enum's own running count."""
    src = PROGRESS_PY.read_text()
    # Anchor on the real-queue condition block.
    anchor = src.index("# Real-queue conditions:")
    block = src[anchor:anchor + 1500]
    assert "plex_enum_other_long_running_n" in block, (
        "Gate must reference the cross-kind blocker count"
    )
    # The condition must OR plex_enum_running_n with the cross-
    # kind count.
    assert "plex_enum_running_n >= 1" in block
    assert "plex_enum_other_long_running_n >= 1" in block


def test_v1_14_92_marker_explains_the_cross_kind_expansion():
    """A v1.14.92 marker on the gate documents the user's repro
    (SYNC THEMERRDB + 1 library refresh) so a future tightening
    refactor sees the rationale."""
    src = PROGRESS_PY.read_text()
    anchor = src.index("# Real-queue conditions:")
    block = src[anchor:anchor + 1500]
    assert "v1.14.92" in block
    assert "SYNC THEMERRDB" in block or "behind sync" in block.lower()


def test_sql_pulls_sync_and_scan_running_counts():
    """The pending_running query must also pull the sync/scan
    running count via a separate SUM(CASE) column. Single-query
    avoids round-trips."""
    src = PROGRESS_PY.read_text()
    # Anchor on the SQL.
    sql_anchor = src.index(
        "SUM(CASE WHEN job_type='plex_enum' AND status='pending'"
    )
    sql_block = src[sql_anchor:sql_anchor + 1200]
    assert "job_type IN ('sync','scan')" in sql_block
    assert "AS other_long_running_n" in sql_block


# ── Behavioral integration tests ───────────────────────────────


def _setup_db_with_jobs(jobs):
    """Fresh in-temp DB with the given (job_type, status) rows
    inserted into the jobs table."""
    fd = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    fd.close()
    db_path = Path(fd.name)
    db_module.init_db(db_path)
    with db_module.get_conn(db_path) as conn:
        for job_type, status in jobs:
            conn.execute(
                "INSERT INTO jobs (job_type, payload, status, "
                "                  attempts, max_attempts, created_at) "
                "VALUES (?, '{}', ?, 0, 3, datetime('now'))",
                (job_type, status),
            )
    return db_path


def test_synth_emits_when_plex_enum_pending_behind_sync():
    """The exact the user repro: 1 sync running + 1 plex_enum
    pending. Pre-fix no synth row emitted; fix should emit the
    plex_enum_pending card with queue_depth=1."""
    db_path = _setup_db_with_jobs([
        ("sync", "running"),
        ("plex_enum", "pending"),
    ])
    try:
        ops = progress_module.load_active(db_path)
        synth = [o for o in ops if o["kind"] == "plex_enum_pending"]
        assert len(synth) == 1, (
            f"Expected one plex_enum_pending synth row when "
            f"plex_enum is queued behind sync; got {len(synth)} "
            f"of {len(ops)} total ops: "
            f"{[o['kind'] for o in ops]}"
        )
        assert synth[0]["detail"]["queue_depth"] == 1
        assert synth[0]["status"] == "pending"
    finally:
        db_path.unlink(missing_ok=True)


def test_synth_emits_when_plex_enum_pending_behind_scan():
    """Same shape but the blocker is a scan (post-place disk
    sweep) instead of a sync. Both are long-worker jobs and
    serialize plex_enum behind them."""
    db_path = _setup_db_with_jobs([
        ("scan", "running"),
        ("plex_enum", "pending"),
    ])
    try:
        ops = progress_module.load_active(db_path)
        synth = [o for o in ops if o["kind"] == "plex_enum_pending"]
        assert len(synth) == 1
        assert synth[0]["detail"]["queue_depth"] == 1
    finally:
        db_path.unlink(missing_ok=True)


def test_synth_does_not_emit_for_solo_pending_no_blocker():
    """Pure 1-pending with NOTHING running should NOT emit. The
    worker's brief click→pickup window naturally has this state
    (1 pending, 0 running) — emitting would flash a misleading
    'queued' card for ~1-2s after a single REFRESH click. The
    optimistic placeholder covers this gap with the right
    framing. v1.14.77 enforces this; v1.14.92 must preserve it."""
    db_path = _setup_db_with_jobs([
        ("plex_enum", "pending"),
    ])
    try:
        ops = progress_module.load_active(db_path)
        synth = [o for o in ops if o["kind"] == "plex_enum_pending"]
        assert len(synth) == 0, (
            "Solo pending with no running blocker must NOT emit a "
            "queued synth card (would mislead during the brief "
            "worker pickup window)"
        )
    finally:
        db_path.unlink(missing_ok=True)


def test_synth_emits_for_two_pending_no_running():
    """v1.14.77's "2+ pending stacked" case still fires — that's
    a genuine queue depth, not a transient pickup-window state."""
    db_path = _setup_db_with_jobs([
        ("plex_enum", "pending"),
        ("plex_enum", "pending"),
    ])
    try:
        ops = progress_module.load_active(db_path)
        synth = [o for o in ops if o["kind"] == "plex_enum_pending"]
        assert len(synth) == 1
        assert synth[0]["detail"]["queue_depth"] == 2
    finally:
        db_path.unlink(missing_ok=True)


def test_synth_emits_for_pending_behind_running_plex_enum():
    """v1.13.87's original case still fires — plex_enum queued
    behind a running plex_enum (different library)."""
    db_path = _setup_db_with_jobs([
        ("plex_enum", "running"),
        ("plex_enum", "pending"),
    ])
    try:
        ops = progress_module.load_active(db_path)
        synth = [o for o in ops if o["kind"] == "plex_enum_pending"]
        assert len(synth) == 1
        assert synth[0]["detail"]["queue_depth"] == 1
    finally:
        db_path.unlink(missing_ok=True)
