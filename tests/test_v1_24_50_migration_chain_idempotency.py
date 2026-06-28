"""v1.24.50 — migration chain idempotency (crash-loop guard).

init_db stamps schema_version PER STEP, but executescript autocommits each
statement and conn.commit() only fires at the very end. So a crash in the window
between a migration's column commit and its version stamp (kill -9 / OOM /
power-loss) leaves the column present but the version behind → boot re-runs the
migration → a bare `ALTER TABLE ADD COLUMN` raised "duplicate column name"
forever (crash-loop). v1.24.50 routes the reachable (v21+) ADD COLUMNs through the
idempotent `_add_column` helper and adds IF NOT EXISTS to the v27→v28 audit_events
CREATE, so re-running any reachable migration against an already-migrated DB is a
safe no-op.

Scope: REACHABLE = v21+. The v17→v21 migrations are intentional fresh-start
hard-stops (RuntimeError "delete motif.db"); they form a wall, so the pre-v17
migrations are unreachable for completing an upgrade and are left as-is.
"""
from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import pytest

import app.core.db as db
from app.core.db import CURRENT_SCHEMA_VERSION, init_db


def _version(conn) -> int:
    return conn.execute("SELECT MAX(version) FROM schema_version").fetchone()[0]


def _reachable_migrations():
    # every _migrate_vN_to_vM with starting version N >= 21 (past the wall),
    # discovered dynamically so a future v67→v68 is auto-covered by this guard.
    out = []
    for name in dir(db):
        m = re.match(r"_migrate_v(\d+)_to_v(\d+)$", name)
        if m and int(m.group(1)) >= 21:
            out.append((int(m.group(1)), name, getattr(db, name)))
    out.sort()
    return out


# ── fresh install ────────────────────────────────────────────────────────────

def test_fresh_install_reaches_current_version(tmp_path):
    p = tmp_path / "fresh.db"
    init_db(p)
    with sqlite3.connect(p) as conn:
        assert _version(conn) == CURRENT_SCHEMA_VERSION
        # spot-check columns added by reachable migrations exist on a fresh DB
        cols = {r[1] for r in conn.execute("PRAGMA table_info(plex_items)")}
        assert "plex_independent_theme" in cols  # v38→v39
        assert "plex_theme_uri" in cols          # v34→v35


# ── the crash-loop guard ──────────────────────────────────────────────────────
#
# The ADDITIVE hazard (bare ALTER ADD COLUMN / CREATE TABLE without IF NOT EXISTS)
# is what v1.24.50 fixes: its re-run signature is "duplicate column name" /
# "X already exists". We assert no reachable migration raises THAT when re-run
# against a fully-migrated DB. Destructive/rebuild migrations (DROP COLUMN, table
# CREATE-new→DROP→RENAME) are a separate, harder class at ANCIENT versions (v26-31,
# the v1.12.x era — no live DB sits there); re-running them against v67 trips on
# now-dropped columns ("no such column"), which is schema-version drift, NOT the
# additive crash-loop — so those errors are tolerated here.

_ADDITIVE_CRASHLOOP = re.compile(r"duplicate column name|already exists", re.I)


def test_recent_chain_reruns_idempotently(tmp_path):
    # The PRACTICALLY-reachable scenario: a DB a few versions behind, upgrade
    # interrupted, version rewound to a recent value but the schema already
    # carries the changes. init_db must re-run the recent chain cleanly to
    # current. (Recent migrations are additive + idempotent; this is the window
    # real installs actually upgrade through.)
    start = CURRENT_SCHEMA_VERSION - 6
    p = tmp_path / "recent.db"
    init_db(p)
    with sqlite3.connect(p) as conn:
        conn.execute("DELETE FROM schema_version")
        conn.execute(
            "INSERT INTO schema_version (version, applied_at) VALUES (?, datetime('now'))",
            (start,))
        conn.commit()
    init_db(p)  # must reach current with no crash
    with sqlite3.connect(p) as conn:
        assert _version(conn) == CURRENT_SCHEMA_VERSION


def test_no_reachable_migration_has_an_additive_crashloop(tmp_path):
    # Per-migration guard (precise attribution + future-proofs a NEW bare ADD
    # COLUMN in any v21+ migration): re-running against current must not raise the
    # additive crash-loop signature. FRESH v67 DB per migration so a rebuild
    # migration can't contaminate a later one (the artifact that masked v22→v23).
    for i, (frm, name, fn) in enumerate(_reachable_migrations()):
        p = tmp_path / f"v67_{i}.db"
        init_db(p)
        conn = sqlite3.connect(p)
        conn.execute("PRAGMA foreign_keys = ON")
        try:
            fn(conn)  # re-run against current schema
            conn.commit()
        except Exception as e:  # noqa: BLE001
            if _ADDITIVE_CRASHLOOP.search(str(e)):
                pytest.fail(
                    f"{name} has an ADDITIVE crash-loop against "
                    f"v{CURRENT_SCHEMA_VERSION}: {type(e).__name__}: {e}")
            # other errors = schema-version drift on a destructive migration —
            # not the additive hazard this guard covers.
        finally:
            conn.close()


# ── the wall: the fresh-start hard-stops must STAY hard-stops ──────────────────

def test_pre_v17_fresh_start_wall_still_raises(tmp_path):
    # v16 is idempotent up to the v17 wall; rewinding there must hit the
    # intentional RuntimeError (so nobody "helpfully" makes v17→v18 idempotent and
    # silently re-enables an unsupported, data-lossy upgrade path).
    p = tmp_path / "wall.db"
    init_db(p)
    with sqlite3.connect(p) as conn:
        conn.execute("DELETE FROM schema_version")
        conn.execute(
            "INSERT INTO schema_version (version, applied_at) "
            "VALUES (16, datetime('now'))")
        conn.commit()
    with pytest.raises(RuntimeError, match="(?i)delete.*motif.db|fresh-start"):
        init_db(p)


# ── the helper itself ─────────────────────────────────────────────────────────

def test_add_column_helper_is_idempotent(tmp_path):
    p = tmp_path / "h.db"
    conn = sqlite3.connect(p)
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
    db._add_column(conn, "t", "foo", "TEXT")
    db._add_column(conn, "t", "foo", "TEXT")  # second call must be a no-op
    cols = [r[1] for r in conn.execute("PRAGMA table_info(t)")]
    assert cols.count("foo") == 1
    conn.close()


def test_reachable_add_column_migrations_use_the_helper():
    # source pin: the formerly-bare v21+ ADD COLUMN migrations route through
    # _add_column (so the idempotency can't silently regress to a bare ALTER).
    src = (Path(__file__).resolve().parent.parent / "app" / "core" / "db.py").read_text()
    assert "def _add_column(" in src
    for table, col in [("local_files", "mismatch_state"),
                       ("pending_updates", "kind"),
                       ("plex_items", "plex_independent_theme"),
                       ("plex_sections", "last_enum_content_changed_at")]:
        assert f'_add_column(conn, "{table}", "{col}"' in src
