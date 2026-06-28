"""v1.19.78 — FK-cascade guard actually takes effect on migrations.

Opus-4.8 audit HIGH-2. The class-9 FK-cascade lesson (v1.18.0 → v1.18.5)
established that table-rebuild migrations MUST run under `PRAGMA
foreign_keys = OFF` so `DROP TABLE <parent>` doesn't fire ON DELETE
CASCADE / SET NULL and nuke child rows (the v1.18.0 catastrophe wiped
local_files + placements + plex_items.theme_id when `themes` was
rebuilt).

The overlooked hole: `PRAGMA foreign_keys` is a SILENT no-op inside an
open transaction (a SQLite rule). `init_db` opens its connection in
legacy isolation mode (default, NOT autocommit like get_conn), and
line ~3928 runs `INSERT INTO schema_version` (DML) after EVERY
migration step — which leaves an implicit transaction open. So on any
MULTI-STEP upgrade, the widen migrations' `PRAGMA foreign_keys = OFF`
runs while a transaction is pending → it does NOTHING → the swap-dance
DROP fires cascades exactly like v1.18.0. The guard *looked* present
(the PRAGMA lines were all there); it just silently didn't work.

Proven empirically: single-step (widen is first loop iteration, no
prior DML) → FK OFF works; multi-step (prior schema_version INSERT) →
FK OFF is a no-op and DROP cascades. `_migrate_v54_to_v55` widens
`themes` — the exact cascade-parent — so a DB at schema < 54 upgrading
through (old-backup restore, dormant install) was a live data-loss
risk, not merely hypothetical.

v1.19.78 fix:
  1. each widen migration calls `conn.commit()` before `PRAGMA
     foreign_keys = OFF`, closing the pending txn so the pragma takes
     effect.
  2. `_widen_check_constraint` HARD-ASSERTS foreign_keys is actually
     OFF immediately before the destructive DROP — converting a silent
     cascade-loss into a loud, recoverable RuntimeError for ANY caller
     (present or future) that forgets the commit.
"""
from __future__ import annotations

import re
import sqlite3
import sys
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from app.core.db import _widen_check_constraint, _migrate_v59_to_v60


DB_PY = (REPO / "app" / "core" / "db.py").read_text()


# ── The catch-all guard: refuse to DROP a parent with FK still ON ──


def test_widen_refuses_drop_when_fk_enforcement_on(tmp_path):
    """If a caller reaches _widen_check_constraint with foreign_keys
    enforcement still ON (e.g. the PRAGMA OFF no-op'd under an open
    txn), the helper must RAISE before the destructive DROP rather
    than silently cascade-delete the child rows."""
    db = tmp_path / "t.db"
    conn = sqlite3.connect(db)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, "
                 "k TEXT CHECK (k IN ('a', 'b')))")
    conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, "
                 "p INTEGER REFERENCES parent(id) ON DELETE CASCADE)")
    conn.execute("INSERT INTO parent (id, k) VALUES (1, 'a')")
    conn.execute("INSERT INTO child (id, p) VALUES (10, 1)")
    conn.commit()

    # FK enforcement is ON and we deliberately do NOT turn it off —
    # this is the state the multi-step no-op leaves the connection in.
    with pytest.raises(RuntimeError, match="foreign_key"):
        _widen_check_constraint(conn, "parent", "k",
                                ("a", "b"), ("a", "b", "c"))

    # The DROP must NOT have run — parent table still present, child
    # rows intact (no silent cascade).
    assert conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='parent'"
    ).fetchone() is not None
    assert conn.execute("SELECT COUNT(*) FROM child").fetchone()[0] == 1
    conn.close()


# ── The fix: widen migration succeeds with a pending txn open ──────


def test_v59_to_v60_succeeds_with_pending_transaction(tmp_path):
    """Reproduce the multi-step state: a legacy-isolation connection
    with an open implicit transaction (as the prior step's
    `INSERT INTO schema_version` leaves it). The commit() in
    _migrate_v59_to_v60 must close that txn so PRAGMA foreign_keys=OFF
    takes effect and the new DROP-guard assertion does NOT fire.
    Without the commit, FK would stay ON and the migration would raise."""
    db = tmp_path / "t.db"
    conn = sqlite3.connect(db)  # legacy/default isolation, like init_db
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("CREATE TABLE themes ("
                 " media_type TEXT NOT NULL, tmdb_id INTEGER NOT NULL,"
                 " UNIQUE (media_type, tmdb_id))")
    conn.execute(
        "CREATE TABLE pending_updates ("
        " media_type TEXT NOT NULL, tmdb_id INTEGER NOT NULL,"
        " section_id TEXT NOT NULL DEFAULT '',"
        " new_youtube_url TEXT, detected_at TEXT NOT NULL, decision TEXT,"
        " kind TEXT NOT NULL DEFAULT 'upstream_changed'"
        "   CHECK (kind IN ('upstream_changed', 'urls_match')),"
        " PRIMARY KEY (media_type, tmdb_id, section_id),"
        " FOREIGN KEY (media_type, tmdb_id) REFERENCES themes (media_type, tmdb_id)"
        "   ON DELETE CASCADE)"
    )
    conn.execute("INSERT INTO themes (media_type, tmdb_id) VALUES ('movie', 1)")
    conn.execute("INSERT INTO themes (media_type, tmdb_id) VALUES ('movie', 2)")
    conn.execute(
        "INSERT INTO pending_updates (media_type, tmdb_id, detected_at, "
        "decision, kind) VALUES ('movie', 1, 'now', 'pending', 'upstream_changed')"
    )
    conn.execute(
        "INSERT INTO pending_updates (media_type, tmdb_id, detected_at, "
        "decision, kind) VALUES ('movie', 2, 'now', 'pending', 'urls_match')"
    )
    conn.commit()

    # Open a pending implicit transaction the way the prior migration
    # step's `INSERT INTO schema_version` does — this is what makes
    # `PRAGMA foreign_keys = OFF` a no-op in the buggy path.
    conn.execute("INSERT INTO themes (media_type, tmdb_id) VALUES ('movie', 3)")
    assert conn.in_transaction is True

    # Must not raise (commit() inside makes FK-OFF effective → the
    # DROP-guard assertion passes).
    _migrate_v59_to_v60(conn)

    # Data preserved through the swap-dance.
    assert conn.execute("SELECT COUNT(*) FROM pending_updates").fetchone()[0] == 2
    # CHECK was actually widened.
    create_sql = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='pending_updates'"
    ).fetchone()[0]
    assert "new_theme_available" in create_sql
    # And the new value is now insertable (the whole point).
    conn.execute("PRAGMA foreign_keys = OFF")
    conn.execute(
        "INSERT INTO pending_updates (media_type, tmdb_id, detected_at, "
        "decision, kind) VALUES ('movie', 9, 'now', 'pending', 'new_theme_available')"
    )
    conn.close()


def test_v59_to_v60_idempotent_after_widen(tmp_path):
    """Re-running the migration on an already-widened table is a
    no-op (v1.19.74 recovery branch B), still under the pending-txn
    state — proves the commit()+guard doesn't break idempotency."""
    db = tmp_path / "t.db"
    conn = sqlite3.connect(db)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("CREATE TABLE themes (media_type TEXT NOT NULL, "
                 "tmdb_id INTEGER NOT NULL, UNIQUE (media_type, tmdb_id))")
    conn.execute(
        "CREATE TABLE pending_updates ("
        " media_type TEXT NOT NULL, tmdb_id INTEGER NOT NULL,"
        " section_id TEXT NOT NULL DEFAULT '', detected_at TEXT NOT NULL,"
        " kind TEXT NOT NULL DEFAULT 'upstream_changed'"
        "   CHECK (kind IN ('upstream_changed', 'urls_match', 'new_theme_available')),"
        " PRIMARY KEY (media_type, tmdb_id, section_id))"
    )
    conn.commit()
    conn.execute("INSERT INTO themes (media_type, tmdb_id) VALUES ('movie', 5)")
    _migrate_v59_to_v60(conn)  # already widened → branch B no-op
    create_sql = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='pending_updates'"
    ).fetchone()[0]
    assert "new_theme_available" in create_sql
    conn.close()


# ── Source pins: the fix is present at every widen site ────────────


def _migration_slice(name: str) -> str:
    start = DB_PY.index(f"def {name}(")
    nxt = DB_PY.index("\ndef ", start + 1)
    return DB_PY[start:nxt]


@pytest.mark.parametrize("fn", [
    "_migrate_v54_to_v55",
    "_migrate_v57_to_v58",
    "_migrate_v59_to_v60",
    # v1.21.8 (audit HIGH): the hand-rolled v4→v5 table rebuild predated
    # the v1.19.78 fix and was the one unprotected DROP TABLE themes
    # site — same FK-OFF-is-a-no-op-under-a-pending-txn cascade-wipe risk.
    "_migrate_v4_to_v5",
])
def test_widen_migrations_commit_before_fk_off(fn):
    """Every table-rebuild migration must commit() before turning FK
    off, so the pragma isn't a no-op under a pending transaction."""
    body = _migration_slice(fn)
    commit_at = body.find("conn.commit()")
    # Anchor on the actual statement, not docstring prose mentioning it.
    off_at = body.find('conn.execute("PRAGMA foreign_keys = OFF")')
    assert commit_at != -1, f"{fn}: missing conn.commit() before FK OFF"
    assert off_at != -1, f"{fn}: missing PRAGMA foreign_keys = OFF call"
    assert commit_at < off_at, (
        f"{fn}: conn.commit() must come BEFORE `PRAGMA foreign_keys = OFF` "
        f"(v1.19.78 — otherwise the pragma is a silent no-op)"
    )


def test_widen_helper_asserts_fk_off_before_drop():
    """_widen_check_constraint must guard the destructive DROP with a
    foreign_keys==0 assertion (the catch-all that protects future
    callers who forget the commit)."""
    body = _migration_slice("_widen_check_constraint")
    assert 'PRAGMA foreign_keys").fetchone()[0] != 0' in body
    guard_at = body.index('PRAGMA foreign_keys").fetchone()[0] != 0')
    # The destructive parent DROP — distinct from the shadow
    # `DROP TABLE {table}_new` in the recovery branches.
    drop_at = body.index('DROP TABLE {table}")')
    assert guard_at < drop_at, (
        "v1.19.78: the FK-off assertion must sit BEFORE the DROP"
    )
