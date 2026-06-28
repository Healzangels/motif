"""v1.15.56 — schema v49: widen op_progress.kind CHECK to include
'bulk_lps'.

the user (docker log on bulk // LET PLEX SERVE click after v1.15.49
deploy): `sqlite3.IntegrityError: CHECK constraint failed: kind
IN ('tdb_sync', 'plex_enum', 'reprobe_plex_themes',
'bulk_probe_tdb')` at try_acquire's INSERT.

## Bug

v1.15.28 added the bulk LPS server-side composite — route at
api.py:13905 calls `op_progress.try_acquire(db, "bulk-lps",
"bulk_lps")` with kind='bulk_lps'. The schema CHECK constraint
on op_progress.kind never widened, so every // ADOPT + LET PLEX
SERVE / // LET PLEX SERVE bulk click 500'd at the INSERT.

Same bug class as v1.15.48 (which widened the STATUS CHECK for
'pending'). Same pattern: code shipped, schema migration didn't.

## Fix

Schema migration v48 → v49 widens op_progress.kind CHECK to
include 'bulk_lps'. Same table-recreate pattern as v46→v47 +
v47→v48 (SQLite can't ALTER CHECK in place).

## Belt-and-suspenders (the test v1.15.28 should have shipped)

Behavioral test that actually CALLS try_acquire with kind=
'bulk_lps' against a fresh init_db()-built DB. v1.15.48 added
the equivalent for status='pending'; v1.15.56 adds it for
kind='bulk_lps'. The same lesson applies — integration tests
against the canonical schema catch CHECK drift at commit time.

Also a cross-source contract guard: every kind value passed
to try_acquire in api.py must appear in the SCHEMA's kind CHECK.
A grep-based test pins the cross-reference so the next new kind
trips the test if its schema migration is missed.
"""
from __future__ import annotations

import re
import sqlite3
import sys
import tempfile
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
DB_PY = REPO / "app" / "core" / "db.py"
API_PY = REPO / "app" / "web" / "api.py"


def test_schema_v49_current_version_bumped():
    """CURRENT_SCHEMA_VERSION must be 49 — without the bump,
    existing v48 databases stay at v48 and never apply the v49
    migration."""
    # v1.15.62: forward-compat — v49 added the kind widening;
    # later versions (v50 etc) build on top. Test intent unchanged.
    import re
    src = DB_PY.read_text()
    m = re.search(r"CURRENT_SCHEMA_VERSION = (\d+)", src)
    assert m and int(m.group(1)) >= 49, (
        "v1.15.56: CURRENT_SCHEMA_VERSION must be 49 so existing "
        "v48 deployments pick up the v49 op_progress kind widening"
    )


def test_schema_constant_op_progress_kind_includes_bulk_lps():
    """The SCHEMA constant (the source-of-truth for fresh-DB
    creation) must include 'bulk_lps' in op_progress.kind CHECK.
    Without it, fresh installs work for everything except bulk
    LPS — same per-tag stealth as the v1.15.48 status bug."""
    src = DB_PY.read_text()
    anchor = src.index("CREATE TABLE IF NOT EXISTS op_progress")
    end = src.index("CREATE INDEX IF NOT EXISTS idx_op_progress_status", anchor)
    block = src[anchor:end]
    # The kind CHECK must list bulk_lps among the allowed values.
    kind_check_idx = block.index("CHECK (kind IN")
    kind_check = block[kind_check_idx:kind_check_idx + 400]
    assert "'bulk_lps'" in kind_check, (
        "v1.15.56: op_progress.kind CHECK must include 'bulk_lps' "
        "— v1.15.28's bulk LPS server-side composite INSERTs with "
        "this kind. Without it, every // ADOPT + LET PLEX SERVE "
        "/ // LET PLEX SERVE bulk click 500s with IntegrityError."
    )


def test_v49_migration_function_exists_and_wired():
    """The migration chain runs `_migrate_v48_to_v49` to upgrade
    existing v48 databases. Without it, an existing install stays
    at v48 with the broken CHECK constraint even after pulling
    the new image."""
    src = DB_PY.read_text()
    assert "def _migrate_v48_to_v49(" in src, (
        "v1.15.56: _migrate_v48_to_v49 helper required for existing "
        "deploys — fresh-DB installs get the v49 SCHEMA, in-place "
        "upgrades need the migration step"
    )
    mig_anchor = src.index("def _migrate_v48_to_v49(")
    mig_end = src.index("\ndef _migrate", mig_anchor + 1)
    mig_body = src[mig_anchor:mig_end]
    assert "'bulk_lps'" in mig_body, (
        "v1.15.56: migration must add 'bulk_lps' to the recreated "
        "kind CHECK — otherwise it's a no-op"
    )
    # Migration wired into the chain.
    chain_anchor = src.index("elif current == 48:")
    chain_block = src[chain_anchor:chain_anchor + 200]
    assert "_migrate_v48_to_v49(conn)" in chain_block, (
        "v1.15.56: migration function exists but isn't called from "
        "the chain — existing v48 DBs will hit 'No migration from v48'"
    )


def test_try_acquire_against_real_initialized_db_with_bulk_lps_kind():
    """Behavioral test (the one v1.15.28 should have shipped):
    call try_acquire with kind='bulk_lps' against a fresh
    init_db()-built database. Asserts the INSERT does NOT raise
    IntegrityError on the kind CHECK.

    Mirrors v1.15.48's test_try_acquire_against_real_initialized_db
    pattern but pins the kind axis instead of the status axis."""
    if str(REPO) not in sys.path:
        sys.path.insert(0, str(REPO))
    from app.core.db import init_db
    from app.core.progress import try_acquire

    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        init_db(db_path)
        try:
            ok = try_acquire(db_path, "bulk-lps", "bulk_lps")
        except sqlite3.IntegrityError as e:
            assert False, (
                f"v1.15.56: try_acquire(kind='bulk_lps') raised "
                f"IntegrityError against the canonical schema — the "
                f"v49 migration didn't take. Error: {e}"
            )
        assert ok is True, (
            "v1.15.56: first try_acquire on bulk-lps must return True"
        )


def test_every_try_acquire_kind_in_code_appears_in_schema_check():
    """Cross-source contract guard: every kind value passed to
    try_acquire() in api.py MUST appear in the SCHEMA's kind CHECK.
    The next new bulk op type that lands without a schema migration
    will trip this test before deploy.

    v1.15.48 + v1.15.56 are both instances of the same code-vs-
    schema drift class — adding a third such test surface (or a
    fourth) without a guard would invite a v1.15.57+ recurrence."""
    api_src = API_PY.read_text()
    db_src = DB_PY.read_text()
    # Extract kind values from try_acquire() calls. Pattern:
    #     op_progress.try_acquire(..., "<op_id>", "<kind>")
    # The kind is the SECOND string arg.
    kinds_in_code = set(re.findall(
        r'op_progress\.try_acquire,\s*db,\s*"[^"]+",\s*"([^"]+)"',
        api_src,
    ))
    assert kinds_in_code, (
        "v1.15.56: test infrastructure failure — no try_acquire "
        "kind values extracted from api.py. Check the regex."
    )
    # Extract kind values from the SCHEMA CHECK constraint.
    schema_anchor = db_src.index("CREATE TABLE IF NOT EXISTS op_progress")
    schema_end = db_src.index("CREATE INDEX IF NOT EXISTS idx_op_progress_status",
                              schema_anchor)
    schema_block = db_src[schema_anchor:schema_end]
    check_idx = schema_block.index("CHECK (kind IN")
    check_block = schema_block[check_idx:check_idx + 500]
    kinds_in_schema = set(re.findall(r"'([a-z_]+)'", check_block))
    missing = kinds_in_code - kinds_in_schema
    assert not missing, (
        f"v1.15.56: try_acquire kind(s) {missing} called in api.py "
        f"are NOT in the SCHEMA's op_progress.kind CHECK. Every "
        "new kind value needs a schema migration to widen the "
        "CHECK — without it the INSERT 500s with IntegrityError. "
        "Add the kind to the SCHEMA constant + write a "
        "_migrate_vN_to_vM helper + bump CURRENT_SCHEMA_VERSION."
    )
