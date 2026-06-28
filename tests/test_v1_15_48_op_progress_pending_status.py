"""v1.15.48 — fix `try_acquire` 500ing on op_progress CHECK constraint.

the user (docker log on // REPROBE FAILURES click in v1.15.46):
"sqlite3.IntegrityError: CHECK constraint failed: status IN
('running', 'cancelling', 'done', 'failed', 'cancelled')"

v1.15.37 added the `try_acquire` helper to close a TOCTOU race
on background-op slot claiming. The helper INSERTs into
op_progress with status='pending' (the route claims the slot
atomically; the spawned worker thread's start_progress
transitions pending → running). The design intent had 'pending'
as a valid op_progress.status — but the schema CHECK constraint
was never widened, so the INSERT 500'd on every call.

Bug class: shipped a code-side feature whose load-bearing SQL
INSERT was never exercised by a real database. v1.15.37 had
tests for try_acquire's atomicity semantics but they apparently
ran against a fixture without the CHECK constraint (or
sqlite_master's CHECK isn't enforced when the test stubs the
table). Either way, the wrong-half slipped through.

Impact: every single background-op click since v1.15.37 has
500'd in production —
  * // PROBE TDB URLS
  * // REPROBE FAILURES
  * // REPROBE PLEX THEMES
  * // BULK LET PLEX SERVE
Each route called try_acquire as the first server-side action;
the INSERT failed the CHECK; the 500 surfaced as "x 500:
Internal Server Error" in the action button's status line.

## Fix

Schema migration v47 → v48 widens the CHECK to include
'pending'. Same table-recreate pattern as v46→v47 (SQLite can't
ALTER CHECK in place). op_progress holds runtime-only state +
worker isn't started until after init_db returns → row-copy is
safe.

## Belt-and-suspenders

Two new tests:
1. Static-text guard on the SCHEMA constant (CHECK includes
   'pending').
2. Behavioral test that actually CALLS try_acquire against a
   fresh database initialized by `init_db()`. Would have caught
   v1.15.37 at write-time. This is the test the project should
   have had from the start.
"""
from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
DB_PY = REPO / "app" / "core" / "db.py"


def test_schema_v48_current_version_bumped():
    """CURRENT_SCHEMA_VERSION must be 48 — drives the migration
    chain's terminal condition. Without the bump, existing v47
    databases stay at v47 and never apply the v48 migration."""
    src = DB_PY.read_text()
    # v1.15.56: forward-compat — v48 added the status widening;
    # later versions (v49 etc) build on top. Test intent unchanged.
    import re
    m = re.search(r"CURRENT_SCHEMA_VERSION = (\d+)", src)
    assert m and int(m.group(1)) >= 48, (
        "v1.15.48: CURRENT_SCHEMA_VERSION must be 48 so existing "
        "v47 deployments pick up the v48 op_progress CHECK widening"
    )


def test_schema_constant_op_progress_check_includes_pending():
    """The SCHEMA constant (the source-of-truth for fresh-DB
    creation) must include 'pending' in op_progress.status CHECK.
    Pin so a future schema rewrite can't silently drop it and
    reintroduce the v1.15.37 bug."""
    src = DB_PY.read_text()
    # Anchor on the op_progress CREATE TABLE block.
    anchor = src.index("CREATE TABLE IF NOT EXISTS op_progress")
    # Find the status CHECK within that block. The block ends at
    # the next CREATE INDEX (op_progress's own index).
    end = src.index("CREATE INDEX IF NOT EXISTS idx_op_progress_status", anchor)
    block = src[anchor:end]
    # Find the status CHECK specifically.
    status_check_idx = block.index("status          TEXT NOT NULL")
    status_check = block[status_check_idx:status_check_idx + 600]
    assert "'pending'" in status_check, (
        "v1.15.48: op_progress.status CHECK must include 'pending' "
        "— v1.15.37's try_acquire INSERTs with this status. Without "
        "it, every background-op click 500s with IntegrityError."
    )


def test_v48_migration_function_exists():
    """The migration chain runs `_migrate_v47_to_v48` to upgrade
    existing v47 databases. Without it, an existing install stays
    at v47 with the broken CHECK constraint even after pulling
    the new image."""
    src = DB_PY.read_text()
    assert "def _migrate_v47_to_v48(" in src, (
        "v1.15.48: _migrate_v47_to_v48 helper required for existing "
        "deploys — fresh-DB installs get the v48 SCHEMA but in-place "
        "upgrades need the migration step"
    )
    # The migration must include 'pending' in the recreated CHECK.
    mig_anchor = src.index("def _migrate_v47_to_v48(")
    mig_end = src.index("\ndef _migrate", mig_anchor + 1)
    mig_body = src[mig_anchor:mig_end]
    assert "'pending'" in mig_body, (
        "v1.15.48: migration must add 'pending' to the recreated "
        "op_progress.status CHECK — otherwise it's a no-op"
    )
    # The migration must be wired into the chain.
    chain_anchor = src.index("elif current == 47:")
    chain_block = src[chain_anchor:chain_anchor + 200]
    assert "_migrate_v47_to_v48(conn)" in chain_block, (
        "v1.15.48: migration function exists but isn't called from "
        "the migration chain — existing v47 DBs will hit the "
        "'No migration from v47' RuntimeError"
    )


def test_try_acquire_against_real_initialized_db():
    """Behavioral test: call try_acquire against a database
    initialized via init_db(). Asserts the INSERT does NOT raise
    sqlite3.IntegrityError on the CHECK constraint.

    This is the test v1.15.37 should have shipped — proves the
    helper works end-to-end against a real schema, not a mocked
    fixture. Imports kept inside the test so collection errors
    don't cascade if the app package is in transient broken
    state during a refactor.
    """
    import sys
    if str(REPO) not in sys.path:
        sys.path.insert(0, str(REPO))
    from app.core.db import init_db
    from app.core.progress import try_acquire

    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        init_db(db_path)
        # Call try_acquire — the bug at v1.15.37 was that this
        # raised IntegrityError on the very first call. Post-v1.15.48
        # it must return True (slot claimed).
        try:
            ok = try_acquire(db_path, "test-op-id", "bulk_probe_tdb")
        except sqlite3.IntegrityError as e:
            assert False, (
                f"v1.15.48: try_acquire raised IntegrityError against "
                f"the canonical schema — the v48 migration didn't take. "
                f"Error: {e}"
            )
        assert ok is True, (
            "v1.15.48: first try_acquire on a fresh slot must return "
            "True (claim succeeded)"
        )
        # Second call with the same op_id must return False (slot
        # already held — atomicity contract). Proves the 'pending'
        # status check at progress.py:74-75 sees the row we just
        # inserted with status='pending'.
        ok2 = try_acquire(db_path, "test-op-id", "bulk_probe_tdb")
        assert ok2 is False, (
            "v1.15.48: second try_acquire on the same slot must "
            "return False (atomicity contract). If True, the status "
            "= 'pending' check at progress.py is broken."
        )


def test_op_progress_status_check_lists_all_expected_states():
    """Counter-guard: the widened CHECK must include ALL the
    states the code ever writes — 'pending' (try_acquire),
    'running' (start_progress), 'cancelling' (cancel_op),
    'done' / 'failed' / 'cancelled' (finish_progress). Pin
    the full set so a future "trim unused" refactor doesn't
    drop one and 500 a different surface."""
    src = DB_PY.read_text()
    anchor = src.index("def _migrate_v47_to_v48(")
    mig_end = src.index("\ndef _migrate", anchor + 1)
    mig_body = src[anchor:mig_end]
    for state in ("pending", "running", "cancelling",
                  "done", "failed", "cancelled"):
        assert f"'{state}'" in mig_body, (
            f"v1.15.48: v48 migration CHECK missing state {state!r} "
            "— some op_progress writer will 500 with IntegrityError"
        )
