"""v1.24.22 — migration-chain structural guards (holistic-review Thread 1).

The deferred-#1 concern was that the forward-only schema migrations
(_migrate_vN_to_vM, 66 of them) had no end-to-end test — a bad or unwired
migration could crash-loop boot. This bundle locks the STRUCTURE without
touching any migration internals (the user's call: the catastrophic-risk zone —
v1.18.0 originated here — stays untouched; the crash-loop has never bitten in
60+ versions).

NOTE — why there is no "replay the whole chain on a v67 DB" test: it's invalid.
Old migrations rebuild tables to their THEN-current shape (e.g. _migrate_v1_to_v2
recreates `jobs` with 13 columns), which fails against the v67 table's 16 — a
test artifact, not a bug, since _migrate_v1_to_v2 only ever runs on a v1 DB in
production. So a full replay can never pass; these structural guards are the
right scope.
"""
from __future__ import annotations

import re
import sqlite3
from pathlib import Path

from app.core import db as core_db
from app.core.db import init_db

REPO = Path(__file__).resolve().parent.parent
DB_PY = (REPO / "app" / "core" / "db.py").read_text()

# Tables a fresh install must always have (a conservative core subset).
_CORE_TABLES = (
    "themes", "local_files", "placements", "jobs", "events", "plex_items",
    "plex_sections", "op_progress", "user_overrides", "schema_version",
)


# ── fresh install lands at the current version, fully built ───────

def test_fresh_init_lands_at_current_version(tmp_path):
    db = tmp_path / "m.db"
    init_db(db)
    with sqlite3.connect(db) as c:
        v = c.execute("SELECT MAX(version) FROM schema_version").fetchone()[0]
    assert v == core_db.CURRENT_SCHEMA_VERSION


def test_fresh_init_creates_core_tables(tmp_path):
    db = tmp_path / "m.db"
    init_db(db)
    with sqlite3.connect(db) as c:
        tables = {r[0] for r in c.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
    missing = [t for t in _CORE_TABLES if t not in tables]
    assert not missing, f"fresh init missing core tables: {missing}"


# ── the migration registry is contiguous + fully wired ───────────

def _migration_pairs() -> list[tuple[int, int]]:
    return sorted({
        (int(a), int(b))
        for a, b in re.findall(r'def _migrate_v(\d+)_to_v(\d+)\(', DB_PY)
    })


def test_migration_chain_is_contiguous_to_current():
    pairs = _migration_pairs()
    # every migration bumps the version by exactly 1
    for a, b in pairs:
        assert b == a + 1, f"_migrate_v{a}_to_v{b} must bump by exactly 1"
    froms = [a for a, _ in pairs]
    # covers every version 1..CURRENT-1 with no gap or duplicate
    assert froms == list(range(1, core_db.CURRENT_SCHEMA_VERSION)), (
        "migration chain must cover every version 1..CURRENT-1 with no gap — "
        f"got froms={froms}")
    assert max(b for _, b in pairs) == core_db.CURRENT_SCHEMA_VERSION, (
        "the highest migration target must equal CURRENT_SCHEMA_VERSION — a "
        "new migration landed without bumping CURRENT_SCHEMA_VERSION, or vice "
        "versa")


def test_every_defined_migration_is_wired_into_dispatch():
    # a migration function that exists but isn't called in init_db's dispatch
    # loop is dead — the upgrade would stall at that version.
    for n in range(1, core_db.CURRENT_SCHEMA_VERSION):
        assert f"_migrate_v{n}_to_v{n + 1}(conn)" in DB_PY, (
            f"migration v{n}->v{n + 1} is defined but never called in init_db")


# ── init_db is idempotent (re-run must be a clean no-op) ──────────

def test_init_db_is_idempotent(tmp_path):
    db = tmp_path / "m.db"
    init_db(db)
    # A second init on an already-current DB must NOT raise (the existing-DB
    # path re-runs the idempotent SCHEMA; the migration loop is skipped) and
    # must NOT append a duplicate version row.
    init_db(db)
    with sqlite3.connect(db) as c:
        v = c.execute("SELECT MAX(version) FROM schema_version").fetchone()[0]
        n_rows = c.execute("SELECT COUNT(*) FROM schema_version").fetchone()[0]
    assert v == core_db.CURRENT_SCHEMA_VERSION
    assert n_rows == 1, "re-init must not append a duplicate schema_version row"


def test_triple_init_stable(tmp_path):
    # belt-and-suspenders: three inits in a row stay stable + green.
    db = tmp_path / "m.db"
    init_db(db)
    init_db(db)
    init_db(db)
    with sqlite3.connect(db) as c:
        assert c.execute(
            "SELECT MAX(version) FROM schema_version"
        ).fetchone()[0] == core_db.CURRENT_SCHEMA_VERSION
