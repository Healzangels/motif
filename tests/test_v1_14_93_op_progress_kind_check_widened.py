"""v1.14.93 — op_progress.kind CHECK widened to allow bulk_probe_tdb.

the user's logs from a v1.14.91 retest:

    2026-05-13 20:30:48 INFO    api    BULK PROBE TDB started by admin
    Exception in thread bulk-probe-tdb:
    Traceback (most recent call last):
      File ".../app/web/api.py", line 2269, in _bulk_probe_tdb_run
        op_progress.start_progress(
      File ".../app/core/progress.py", line 56, in start_progress
        conn.execute(
    sqlite3.IntegrityError: CHECK constraint failed: kind IN ('tdb_sync',
                                                              'plex_enum',
                                                              'reprobe_plex_themes')

And: "ran into some issues with the let plex serve bulk action.
I also didn't ever see the status of the bulk url probe — nothing
ended up happening"

## Root cause

The op_progress.kind CHECK constraint at db.py was last touched
at v1.13.x (when reprobe_plex_themes was added). v1.14.29
introduced bulk_probe_tdb but never widened the CHECK. Every
BULK PROBE TDB click silently failed:

  1. POST /api/admin/bulk-probe-tdb → spawns the worker thread,
     returns 200 OK to the client.
  2. Worker thread immediately calls op_progress.start_progress
     with kind='bulk_probe_tdb'.
  3. SQLite rejects the INSERT (CHECK constraint).
  4. The thread dies; the route already returned. User sees
     "// PROBE TDB URLS started" alert in JS.
  5. LIVE OPS shows nothing — no op was ever registered.

The LET PLEX SERVE bulk action's v1.14.29 opt-in pre-flight
prompt makes this worse: it CALLS bulk-probe-tdb, then aborts
the LPS flow telling the user to wait for the probe to finish.
The probe never starts, so the user is stuck — re-clicking LPS
just shows the prompt again.

## Fix

Add schema migration v46 → v47 that widens the CHECK to also
allow 'bulk_probe_tdb'. SQLite can't ALTER CHECK in place, so
the migration recreates the table (op_progress holds runtime-
only state, no persistence concern).

The schema constant is updated alongside so fresh installs get
the wider CHECK directly.
"""
from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

from app.core import db as db_module
from app.core import progress as op_progress


REPO = Path(__file__).resolve().parent.parent
DB_PY = REPO / "app" / "core" / "db.py"


# ── Schema constants ───────────────────────────────────────────


def test_schema_version_bumped_to_at_least_47():
    """CURRENT_SCHEMA_VERSION must be ≥ 47 — v47 added
    bulk_probe_tdb to the kind CHECK. Forward-compatible: future
    tags can bump higher (v1.15.48 → v48 for status CHECK widening)
    without invalidating the v47 guarantee this test protects."""
    assert db_module.CURRENT_SCHEMA_VERSION >= 47


def test_schema_constant_includes_bulk_probe_tdb():
    """The SCHEMA constant's CREATE TABLE op_progress must list
    bulk_probe_tdb in the CHECK so fresh installs work without
    migration."""
    src = DB_PY.read_text()
    # Anchor on the op_progress CREATE TABLE.
    anchor = src.index("CREATE TABLE IF NOT EXISTS op_progress")
    # v1.19.45: window widened 2500 → 4500. The v1.19.45 kind-
    # CHECK widening (cloud_themes_backup add) brought a longer
    # rationale comment that pushed the other CHECK tokens past
    # the original 2500-char window.
    block = src[anchor:anchor + 4500]
    # The CHECK must mention all 4 valid kinds.
    assert "'bulk_probe_tdb'" in block, (
        "SCHEMA constant must include bulk_probe_tdb in the kind CHECK"
    )
    assert "'tdb_sync'" in block
    assert "'plex_enum'" in block
    assert "'reprobe_plex_themes'" in block


def test_v46_to_v47_migration_function_exists():
    """The migration function must be defined."""
    assert hasattr(db_module, "_migrate_v46_to_v47")


def test_v46_to_v47_dispatch_wired():
    """The init_db dispatcher must route v46 → v47 via the new
    migration function. Forgetting this leaves existing installs
    stuck at v46."""
    src = DB_PY.read_text()
    # The dispatch lives in init_db's elif chain.
    anchor = src.index("elif current == 45:")
    block = src[anchor:anchor + 800]
    assert "elif current == 46:" in block
    assert "_migrate_v46_to_v47(conn)" in block
    assert "current = 47" in block


# ── Behavioral integration ──────────────────────────────────────


def test_fresh_db_accepts_bulk_probe_tdb_kind():
    """A fresh DB built from SCHEMA must allow start_progress with
    kind='bulk_probe_tdb' — pre-fix this was the IntegrityError
    that killed the worker thread."""
    fd = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    fd.close()
    db_path = Path(fd.name)
    db_module.init_db(db_path)
    try:
        # The exact start_progress call _bulk_probe_tdb_run makes.
        op_progress.start_progress(
            db_path, op_id="bulk-probe-tdb", kind="bulk_probe_tdb",
            stage="probe", stage_label="Probing TDB URLs",
            stage_total=0, processed_est=0,
        )
        # And it should be readable back.
        rows = op_progress.load_active(db_path)
        assert any(r["kind"] == "bulk_probe_tdb" for r in rows), (
            f"bulk_probe_tdb op should be active after start; got "
            f"{[r['kind'] for r in rows]}"
        )
    finally:
        db_path.unlink(missing_ok=True)


def test_all_existing_kinds_still_accepted():
    """The widening must not have broken any of the previously-
    valid kinds (tdb_sync, plex_enum, reprobe_plex_themes)."""
    fd = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    fd.close()
    db_path = Path(fd.name)
    db_module.init_db(db_path)
    try:
        for kind in ("tdb_sync", "plex_enum", "reprobe_plex_themes"):
            op_progress.start_progress(
                db_path, op_id=f"test-{kind}", kind=kind,
                stage="x", stage_label="x",
                stage_total=0, processed_est=0,
            )
        rows = op_progress.load_active(db_path)
        kinds_present = {r["kind"] for r in rows}
        assert {"tdb_sync", "plex_enum", "reprobe_plex_themes"}.issubset(
            kinds_present
        ), f"Pre-existing kinds must still be accepted; got {kinds_present}"
    finally:
        db_path.unlink(missing_ok=True)


def test_unknown_kind_still_rejected():
    """The CHECK must still reject unknown kinds — widening was
    additive, not a removal of validation."""
    fd = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    fd.close()
    db_path = Path(fd.name)
    db_module.init_db(db_path)
    try:
        try:
            op_progress.start_progress(
                db_path, op_id="bogus", kind="not_a_real_kind",
                stage="x", stage_label="x",
                stage_total=0, processed_est=0,
            )
            assert False, "Unknown kind should have raised IntegrityError"
        except sqlite3.IntegrityError:
            pass  # Expected
    finally:
        db_path.unlink(missing_ok=True)


def test_v46_to_v47_migration_widens_existing_check():
    """The v46 → v47 migration applied to an existing v46 database
    must allow bulk_probe_tdb afterward. Simulates an upgrade
    from a pre-fix install."""
    fd = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    fd.close()
    db_path = Path(fd.name)
    # Build a v46-shaped op_progress directly so we can run the
    # migration in isolation.
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
            CREATE TABLE op_progress (
                op_id           TEXT PRIMARY KEY,
                kind            TEXT NOT NULL
                                   CHECK (kind IN ('tdb_sync', 'plex_enum',
                                                   'reprobe_plex_themes')),
                status          TEXT NOT NULL DEFAULT 'running'
                                   CHECK (status IN ('running', 'cancelling',
                                                     'done', 'failed', 'cancelled')),
                started_at      TEXT NOT NULL,
                updated_at      TEXT NOT NULL,
                finished_at     TEXT,
                stage           TEXT,
                stage_label     TEXT,
                stage_current   INTEGER NOT NULL DEFAULT 0,
                stage_total     INTEGER NOT NULL DEFAULT 0,
                processed_total INTEGER NOT NULL DEFAULT 0,
                processed_est   INTEGER NOT NULL DEFAULT 0,
                error_count     INTEGER NOT NULL DEFAULT 0,
                detail_json     TEXT
            )
        """)
        # Pre-existing row (representative of a real install with a
        # finished op in the table). Should survive the recreate.
        conn.execute(
            "INSERT INTO op_progress (op_id, kind, status, started_at, "
            "                         updated_at) "
            "VALUES ('old-op', 'tdb_sync', 'done', '2026-01-01', "
            "                                       '2026-01-01')"
        )
        conn.commit()
        # Pre-migration: bulk_probe_tdb should fail.
        try:
            conn.execute(
                "INSERT INTO op_progress (op_id, kind, status, started_at, "
                "                         updated_at) "
                "VALUES ('pre', 'bulk_probe_tdb', 'running', '2026-01-01', "
                "                                            '2026-01-01')"
            )
            assert False, "Pre-migration should reject bulk_probe_tdb"
        except sqlite3.IntegrityError:
            pass  # Expected
        # Run the migration.
        db_module._migrate_v46_to_v47(conn)
        conn.commit()
        # Post-migration: bulk_probe_tdb should succeed.
        conn.execute(
            "INSERT INTO op_progress (op_id, kind, status, started_at, "
            "                         updated_at) "
            "VALUES ('post', 'bulk_probe_tdb', 'running', '2026-01-01', "
            "                                             '2026-01-01')"
        )
        conn.commit()
        # And the pre-existing row survived.
        rows = list(conn.execute(
            "SELECT op_id, kind FROM op_progress ORDER BY op_id"
        ))
    assert ('old-op', 'tdb_sync') in rows
    assert ('post', 'bulk_probe_tdb') in rows
    db_path.unlink(missing_ok=True)
