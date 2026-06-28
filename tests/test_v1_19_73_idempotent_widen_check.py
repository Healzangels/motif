"""v1.19.73 — `_widen_check_constraint` is idempotent across crashes.

## Repro

the user's 2026-05-28 v1.19.71 → v1.19.72 upgrade window: the
container restarted mid-migration, leaving `pending_updates_new`
behind from a partially-completed v59→v60 widening. Next boot:

    sqlite3.OperationalError: table pending_updates_new already exists

The recipe's CREATE TABLE step had committed but the subsequent
DROP/RENAME hadn't. With no self-heal, the only recovery was
manual `DROP TABLE pending_updates_new` via sqlite3 CLI while
motif was stopped.

## Fix

`_widen_check_constraint` now pre-cleans `{table}_new` before
the CREATE so an interrupted run self-heals on next boot. Logs
a warning when it finds + drops a stale shadow so the operator
sees the breadcrumb. Sub-pattern of the v1.18.7 cold-path
lesson: recovery / one-shot migration code paths need explicit
self-healing entries + INFO-or-warn logs at every branch.

## Why pre-drop is safe

The shadow table is mid-recipe scratch. Its content is a copy
of the original table BEFORE the original was dropped — by the
time the original has been dropped, the rename has happened and
no shadow exists. So a shadow that exists at boot is by
definition a discardable copy; the source data is still in the
real table.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))


def test_widen_check_constraint_drops_stale_shadow(tmp_path):
    """The helper must DROP TABLE {table}_new BEFORE the CREATE
    so a re-run of an interrupted recipe succeeds."""
    db_path = tmp_path / "shadow.db"
    conn = sqlite3.connect(str(db_path))
    # Build a v59-shape pending_updates with the narrow CHECK.
    conn.executescript(
        """
        CREATE TABLE pending_updates (
            media_type TEXT NOT NULL,
            tmdb_id INTEGER NOT NULL,
            section_id TEXT NOT NULL DEFAULT '',
            kind TEXT NOT NULL DEFAULT 'upstream_changed'
                CHECK (kind IN ('upstream_changed', 'urls_match')),
            PRIMARY KEY (media_type, tmdb_id, section_id)
        );
        """
    )
    # Seed actual data on the real table — make sure it survives.
    conn.execute(
        "INSERT INTO pending_updates (media_type, tmdb_id, kind) "
        "VALUES ('movie', 100, 'upstream_changed')"
    )
    # Simulate a prior interrupted migration — leftover shadow
    # table with stale partial data (e.g. one row).
    conn.executescript(
        """
        CREATE TABLE pending_updates_new (
            media_type TEXT NOT NULL,
            tmdb_id INTEGER NOT NULL,
            section_id TEXT NOT NULL DEFAULT '',
            kind TEXT NOT NULL DEFAULT 'upstream_changed'
                CHECK (kind IN ('upstream_changed', 'urls_match',
                                'new_theme_available')),
            PRIMARY KEY (media_type, tmdb_id, section_id)
        );
        INSERT INTO pending_updates_new (media_type, tmdb_id, kind)
            VALUES ('movie', 999, 'upstream_changed');
        """
    )
    conn.commit()
    # Pre-fix: the v59→v60 migration would crash at CREATE TABLE
    # pending_updates_new with "table already exists". Post-fix:
    # the helper drops the shadow + succeeds.
    from app.core.db import _migrate_v59_to_v60
    _migrate_v59_to_v60(conn)
    # Post-state: real table has the original row + the wider
    # CHECK. Shadow is gone (rename swapped it).
    rows = conn.execute(
        "SELECT media_type, tmdb_id, kind FROM pending_updates"
    ).fetchall()
    assert rows == [("movie", 100, "upstream_changed")], (
        "v1.19.73: pre-existing data on the real table must "
        "survive the recovery — the stale shadow's row must NOT "
        "have replaced it"
    )
    # Shadow must no longer exist.
    shadow = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' "
        "AND name='pending_updates_new'"
    ).fetchone()
    assert shadow is None
    # Post-fix the CHECK accepts the new kind.
    conn.execute(
        "INSERT INTO pending_updates (media_type, tmdb_id, kind) "
        "VALUES ('movie', 101, 'new_theme_available')"
    )
    conn.commit()
    conn.close()


def test_widen_check_constraint_no_shadow_no_drop(tmp_path):
    """When NO stale shadow exists, the helper must run cleanly
    without spurious DROP errors (the no-leftover happy path)."""
    db_path = tmp_path / "clean.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript(
        """
        CREATE TABLE pending_updates (
            media_type TEXT NOT NULL,
            tmdb_id INTEGER NOT NULL,
            section_id TEXT NOT NULL DEFAULT '',
            kind TEXT NOT NULL DEFAULT 'upstream_changed'
                CHECK (kind IN ('upstream_changed', 'urls_match')),
            PRIMARY KEY (media_type, tmdb_id, section_id)
        );
        INSERT INTO pending_updates (media_type, tmdb_id, kind)
            VALUES ('movie', 1, 'urls_match');
        """
    )
    conn.commit()
    from app.core.db import _migrate_v59_to_v60
    _migrate_v59_to_v60(conn)
    rows = conn.execute(
        "SELECT media_type, tmdb_id, kind FROM pending_updates"
    ).fetchall()
    assert rows == [("movie", 1, "urls_match")]
    conn.close()


def test_widen_check_constraint_logs_warning_on_shadow_drop(
    tmp_path, caplog
):
    """When a stale shadow IS found, the helper must log a
    warning so the operator sees the recovery breadcrumb in
    the docker log (v1.17.11 cold-path-needs-MORE-logging
    pattern)."""
    db_path = tmp_path / "shadow_log.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript(
        """
        CREATE TABLE pending_updates (
            media_type TEXT NOT NULL,
            tmdb_id INTEGER NOT NULL,
            section_id TEXT NOT NULL DEFAULT '',
            kind TEXT NOT NULL DEFAULT 'upstream_changed'
                CHECK (kind IN ('upstream_changed', 'urls_match')),
            PRIMARY KEY (media_type, tmdb_id, section_id)
        );
        CREATE TABLE pending_updates_new (
            media_type TEXT NOT NULL,
            tmdb_id INTEGER NOT NULL,
            section_id TEXT NOT NULL DEFAULT '',
            kind TEXT NOT NULL,
            PRIMARY KEY (media_type, tmdb_id, section_id)
        );
        """
    )
    conn.commit()
    from app.core.db import _migrate_v59_to_v60
    import logging
    with caplog.at_level(logging.WARNING, logger="app.core.db"):
        _migrate_v59_to_v60(conn)
    # The warning must mention BOTH the recovery context (so
    # an operator grepping for "stale" or "shadow" or
    # "interrupted" finds it) AND the table name.
    log_text = " ".join(r.getMessage() for r in caplog.records)
    assert "stale" in log_text.lower() or "shadow" in log_text.lower()
    assert "pending_updates" in log_text
    conn.close()


# ── Source-level idempotency guard ──────────────────────────────


def test_widen_check_constraint_source_has_pre_drop():
    """Source-level pin: the helper must call DROP TABLE on the
    `_new` shadow before the CREATE — protects against a future
    refactor accidentally removing the pre-clean."""
    db_py = (REPO / "app" / "core" / "db.py").read_text()
    start = db_py.index("def _widen_check_constraint")
    # Bound to the helper itself by the next `def` boundary —
    # the fixed-window slice broke as the helper grew past it
    # in v1.19.74.
    end = db_py.index("\ndef ", start + 1)
    fn = db_py[start:end]
    # Pre-drop must reference the `_new` suffix and DROP TABLE.
    assert "DROP TABLE" in fn
    assert "_new" in fn
    # The v1.19.73 marker pins WHY the pre-drop exists.
    assert "v1.19.73" in fn
    # Ordering: the pre-drop must come BEFORE the CREATE.
    drop_pos = fn.index(f"DROP TABLE")
    # The CREATE step is `conn.execute(new_create_sql)`.
    create_pos = fn.index("conn.execute(new_create_sql)")
    assert drop_pos < create_pos, (
        "v1.19.73: the stale-shadow DROP must precede the "
        "CREATE — otherwise re-runs still hit 'table exists'"
    )


def test_v1_19_73_version_pin():
    """Loose prefix — later tags continue the v1.19.x line."""
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
