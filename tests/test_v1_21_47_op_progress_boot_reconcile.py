"""v1.21.47 — boot reconciliation for stale op_progress rows.

the user's incident: a v1.21.46 redeploy killed a `tdb_sync` op at 96%
mid-apply. The boot sequence cleans up `jobs` (main.py zombie sweep)
and `sync_runs` (worker.py orphan sweep), but NOT `op_progress` — so
that row sat as status='running' (then 'cancelling' after a cancel
click) with no live thread to finish it.

Symptoms this caused, both reproduced/guarded below:
  1. The LIVE OPS panel rendered the phantom forever — ELAPSED grew as
     wall-clock-since-start while stage_current stayed frozen — and a
     restart couldn't clear it because no process owned the row.
  2. try_acquire returned False while the stale row existed, so a fresh
     // SYNC THEMERRDB couldn't even start.

Fix: `progress.reset_stale_on_boot(db_path)` flips every non-terminal
op_progress row to 'failed' at startup, called from main.py right after
the jobs zombie sweep. Safe because no worker thread has started yet.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.core.db import init_db
from app.core import progress


REPO = Path(__file__).resolve().parent.parent


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@pytest.fixture
def db(tmp_path: Path) -> Path:
    p = tmp_path / "motif.db"
    init_db(p)
    return p


def _insert_op(db: Path, *, op_id: str, kind: str, status: str,
               finished_at: str | None = None):
    now = _now_iso()
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO op_progress "
            "  (op_id, kind, status, started_at, updated_at, finished_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (op_id, kind, status, now, now, finished_at),
        )


def _status_of(db: Path, op_id: str) -> tuple[str, str | None]:
    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT status, finished_at FROM op_progress WHERE op_id = ?",
            (op_id,),
        ).fetchone()
    return row[0], row[1]


# ── the reset flips only non-terminal rows ───────────────────


def test_reset_flips_running_cancelling_pending_to_failed(db):
    _insert_op(db, op_id="s1", kind="tdb_sync", status="running")
    _insert_op(db, op_id="s2", kind="plex_enum", status="cancelling")
    _insert_op(db, op_id="s3", kind="bulk_probe_tdb", status="pending")
    n = progress.reset_stale_on_boot(db)
    assert n == 3
    for op_id in ("s1", "s2", "s3"):
        status, finished_at = _status_of(db, op_id)
        assert status == "failed", op_id
        assert finished_at is not None, op_id


def test_reset_leaves_terminal_rows_untouched(db):
    _insert_op(db, op_id="d1", kind="tdb_sync", status="done",
               finished_at="2026-06-03T00:00:00+00:00")
    _insert_op(db, op_id="f1", kind="plex_enum", status="failed",
               finished_at="2026-06-03T00:00:00+00:00")
    _insert_op(db, op_id="c1", kind="bulk_lps", status="cancelled",
               finished_at="2026-06-03T00:00:00+00:00")
    n = progress.reset_stale_on_boot(db)
    assert n == 0
    # finished_at preserved exactly (COALESCE didn't overwrite).
    assert _status_of(db, "d1") == ("done", "2026-06-03T00:00:00+00:00")
    assert _status_of(db, "f1") == ("failed", "2026-06-03T00:00:00+00:00")
    assert _status_of(db, "c1") == ("cancelled", "2026-06-03T00:00:00+00:00")


def test_reset_is_idempotent_noop_on_clean_db(db):
    """A clean shutdown leaves no non-terminal rows — the sweep is a
    no-op and never raises on an empty table."""
    assert progress.reset_stale_on_boot(db) == 0
    assert progress.reset_stale_on_boot(db) == 0


# ── the phantom no longer shows as ACTIVE ────────────────────


def test_reset_clears_phantom_from_load_active(db):
    """The exact symptom: a stuck 'cancelling' row rendering in LIVE OPS.
    After the reset it must no longer appear in load_active's active set."""
    _insert_op(db, op_id="tdb-sync", kind="tdb_sync", status="cancelling")
    before = progress.load_active(db)
    assert any(
        op.get("status") in ("running", "cancelling")
        and op.get("op_id") == "tdb-sync"
        for op in before
    ), "phantom should render as active before the reset"
    progress.reset_stale_on_boot(db)
    after = progress.load_active(db)
    assert not any(
        op.get("op_id") == "tdb-sync"
        and op.get("status") in ("running", "cancelling")
        for op in after
    ), "phantom must be gone from the active set after the reset"


# ── the stale row no longer blocks a fresh op ────────────────


def test_reset_unblocks_try_acquire(db):
    """the user couldn't re-run the sync because try_acquire saw the stale
    'cancelling' row and returned False. After the reset, the same
    op_id can be acquired again."""
    _insert_op(db, op_id="tdb-sync", kind="tdb_sync", status="cancelling")
    # Blocked while the phantom is non-terminal.
    assert progress.try_acquire(db, "tdb-sync", "tdb_sync") is False
    progress.reset_stale_on_boot(db)
    # Now the slot is free.
    assert progress.try_acquire(db, "tdb-sync", "tdb_sync") is True


# ── main.py wires the sweep at boot ──────────────────────────


def test_main_calls_reset_stale_on_boot():
    """The boot path must actually call the helper — otherwise the fix
    is dead code and the phantom persists. Pin the call site."""
    main_src = (REPO / "app" / "main.py").read_text()
    assert "reset_stale_on_boot(settings.db_path)" in main_src, (
        "main.py startup must call progress.reset_stale_on_boot so the "
        "phantom-op_progress sweep actually runs"
    )
    # It must sit AFTER the jobs zombie sweep (same boot-reconciliation
    # cluster) — anchor on the zombie-sweep's log line preceding it.
    assert (
        main_src.index("Zombie running-job sweep skipped")
        < main_src.index("reset_stale_on_boot")
    )
