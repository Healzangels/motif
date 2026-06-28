"""v1.19.74 — trigger-aware _widen_check_constraint.

## Repro

the user's 2026-05-28 v1.19.73 boot got past the v1.19.73 shadow
pre-clean fix but hit a new failure at the ALTER TABLE RENAME
step:

    sqlite3.OperationalError: error in trigger
    trg_user_overrides_cleanup_urls_match:
    no such table: main.pending_updates

The trigger is defined on `user_overrides` (AFTER DELETE) but
references `pending_updates` in its body. SQLite 3.25+ tightened
ALTER TABLE RENAME validation: it walks the schema looking for
triggers/views that reference the renamed table, and refuses if
any reference is dangling. At the moment of RENAME the swap-
dance had already DROPped the original pending_updates, so the
trigger's reference was dangling.

the user's resulting DB state was WORSE than pre-v1.19.73:
- `pending_updates` table: GONE (dropped before RENAME)
- `pending_updates_new`: has all the data + widened CHECK
- trigger: still defined, dangling

## Fix

`_widen_check_constraint` now handles three states defensively:

1. **Recovery A — real missing + shadow present**: the user's
   exact state. Drop dangling triggers + RENAME shadow back to
   the real name. Return as if widening succeeded (the shadow
   was built with the new CHECK during the failed attempt).
2. **Recovery B — real present + CHECK already widened**:
   idempotent no-op. Used when a partial manual recovery
   already widened the table but didn't stamp schema_version.
3. **Proactive — drop referencing triggers before the swap-
   dance**. The triggers are recreated by `init_db`'s
   post-migration `executescript(SCHEMA)` via the
   `CREATE TRIGGER IF NOT EXISTS` statements still in SCHEMA.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))


# ── Recovery A: real missing + shadow present ──────────────────


def test_recovery_a_real_missing_shadow_present(tmp_path):
    """Simulate the user's exact 2026-05-28 stuck state — the
    RENAME failed mid-recipe leaving the real table dropped + the
    shadow holding all the data. The helper must recover by
    dropping the dangling trigger + renaming the shadow back."""
    db_path = tmp_path / "stuck.db"
    conn = sqlite3.connect(str(db_path))
    # user_overrides table (the trigger's host)
    conn.executescript(
        """
        CREATE TABLE user_overrides (
            media_type TEXT NOT NULL,
            tmdb_id INTEGER NOT NULL,
            section_id TEXT NOT NULL DEFAULT '',
            youtube_url TEXT,
            PRIMARY KEY (media_type, tmdb_id, section_id)
        );
        -- Shadow table from the failed attempt has the widened CHECK + data.
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
            VALUES ('movie', 42, 'upstream_changed');
        -- Trigger references pending_updates (now missing!).
        CREATE TRIGGER trg_user_overrides_cleanup_urls_match
            AFTER DELETE ON user_overrides
            FOR EACH ROW
            BEGIN
                DELETE FROM pending_updates
                WHERE media_type = OLD.media_type
                  AND tmdb_id = OLD.tmdb_id
                  AND kind = 'urls_match';
            END;
        """
    )
    conn.commit()
    # Pre-fix: any access touching the trigger crashes with
    # "no such table: main.pending_updates". Post-fix: the
    # helper detects + recovers.
    from app.core.db import _migrate_v59_to_v60
    _migrate_v59_to_v60(conn)
    # pending_updates must exist with the data from the shadow.
    rows = conn.execute(
        "SELECT media_type, tmdb_id, kind FROM pending_updates"
    ).fetchall()
    assert rows == [("movie", 42, "upstream_changed")]
    # Shadow gone.
    shadow = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' "
        "AND name='pending_updates_new'"
    ).fetchone()
    assert shadow is None
    # The widened CHECK accepts the new kind value.
    conn.execute(
        "INSERT INTO pending_updates (media_type, tmdb_id, kind) "
        "VALUES ('movie', 43, 'new_theme_available')"
    )
    conn.commit()
    conn.close()


# ── Recovery B: real present + CHECK already widened ───────────


def test_recovery_b_already_widened_is_noop(tmp_path):
    """If the real table already has the widened CHECK (e.g. a
    prior manual recovery), the helper must treat as no-op +
    drop any leftover shadow + return."""
    db_path = tmp_path / "widened.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript(
        """
        CREATE TABLE pending_updates (
            media_type TEXT NOT NULL,
            tmdb_id INTEGER NOT NULL,
            section_id TEXT NOT NULL DEFAULT '',
            kind TEXT NOT NULL DEFAULT 'upstream_changed'
                CHECK (kind IN ('upstream_changed', 'urls_match',
                                'new_theme_available')),
            PRIMARY KEY (media_type, tmdb_id, section_id)
        );
        INSERT INTO pending_updates (media_type, tmdb_id, kind)
            VALUES ('movie', 7, 'new_theme_available');
        """
    )
    conn.commit()
    # The helper should NOT raise "couldn't locate CHECK clause"
    # (which it would pre-v1.19.74 since the regex hunts the
    # OLD values).
    from app.core.db import _migrate_v59_to_v60
    _migrate_v59_to_v60(conn)
    # Data survives the no-op.
    rows = conn.execute(
        "SELECT media_type, tmdb_id, kind FROM pending_updates"
    ).fetchall()
    assert rows == [("movie", 7, "new_theme_available")]
    conn.close()


def test_recovery_b_drops_leftover_shadow(tmp_path):
    """The already-widened branch must also drop any leftover
    shadow so the DB doesn't accumulate orphan `_new` tables."""
    db_path = tmp_path / "widened_with_shadow.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript(
        """
        CREATE TABLE pending_updates (
            media_type TEXT NOT NULL,
            tmdb_id INTEGER NOT NULL,
            section_id TEXT NOT NULL DEFAULT '',
            kind TEXT NOT NULL DEFAULT 'upstream_changed'
                CHECK (kind IN ('upstream_changed', 'urls_match',
                                'new_theme_available')),
            PRIMARY KEY (media_type, tmdb_id, section_id)
        );
        CREATE TABLE pending_updates_new (
            media_type TEXT NOT NULL,
            tmdb_id INTEGER,
            section_id TEXT NOT NULL DEFAULT '',
            kind TEXT,
            PRIMARY KEY (media_type, tmdb_id, section_id)
        );
        """
    )
    conn.commit()
    from app.core.db import _migrate_v59_to_v60
    _migrate_v59_to_v60(conn)
    shadow = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' "
        "AND name='pending_updates_new'"
    ).fetchone()
    assert shadow is None
    conn.close()


# ── Proactive trigger-drop ──────────────────────────────────────


def test_proactive_trigger_drop_lets_rename_succeed(tmp_path):
    """Build a v59-shape DB with the trigger referencing
    pending_updates in its body. The helper must drop the
    trigger before the swap-dance so the RENAME doesn't fail."""
    db_path = tmp_path / "trigger_block.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript(
        """
        CREATE TABLE user_overrides (
            media_type TEXT NOT NULL,
            tmdb_id INTEGER NOT NULL,
            section_id TEXT NOT NULL DEFAULT '',
            youtube_url TEXT,
            PRIMARY KEY (media_type, tmdb_id, section_id)
        );
        CREATE TABLE pending_updates (
            media_type TEXT NOT NULL,
            tmdb_id INTEGER NOT NULL,
            section_id TEXT NOT NULL DEFAULT '',
            kind TEXT NOT NULL DEFAULT 'upstream_changed'
                CHECK (kind IN ('upstream_changed', 'urls_match')),
            PRIMARY KEY (media_type, tmdb_id, section_id)
        );
        INSERT INTO pending_updates (media_type, tmdb_id, kind)
            VALUES ('movie', 11, 'urls_match');
        CREATE TRIGGER trg_user_overrides_cleanup_urls_match
            AFTER DELETE ON user_overrides
            FOR EACH ROW
            BEGIN
                DELETE FROM pending_updates
                WHERE media_type = OLD.media_type
                  AND tmdb_id = OLD.tmdb_id
                  AND kind = 'urls_match';
            END;
        """
    )
    conn.commit()
    from app.core.db import _migrate_v59_to_v60
    _migrate_v59_to_v60(conn)
    # Real table exists with the widened CHECK + the data.
    rows = conn.execute(
        "SELECT media_type, tmdb_id, kind FROM pending_updates"
    ).fetchall()
    assert rows == [("movie", 11, "urls_match")]
    conn.execute(
        "INSERT INTO pending_updates (media_type, tmdb_id, kind) "
        "VALUES ('movie', 12, 'new_theme_available')"
    )
    conn.commit()
    # The trigger was DROPPED during the migration (it'll be
    # recreated by init_db's executescript(SCHEMA) — not
    # exercised here since we're calling _migrate_v59_to_v60
    # directly).
    triggers = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='trigger' "
        "AND name='trg_user_overrides_cleanup_urls_match'"
    ).fetchall()
    assert triggers == [], (
        "v1.19.74: the trigger must be dropped during migration. "
        "It'll be recreated by init_db's executescript(SCHEMA) "
        "via CREATE TRIGGER IF NOT EXISTS."
    )
    conn.close()


def test_schema_recreates_trigger_via_if_not_exists():
    """The proactive trigger-drop in _widen_check_constraint
    relies on init_db's post-migration executescript(SCHEMA)
    re-creating the trigger. Verify SCHEMA contains a
    `CREATE TRIGGER IF NOT EXISTS trg_user_overrides_cleanup_
    urls_match` statement so the drop is reversible."""
    db_py = (REPO / "app" / "core" / "db.py").read_text()
    # Locate the SCHEMA constant.
    schema_start = db_py.index("SCHEMA = ")
    schema_end = db_py.index('"""', db_py.index('"""', schema_start) + 3)
    schema_body = db_py[schema_start:schema_end + 3]
    assert (
        "CREATE TRIGGER IF NOT EXISTS "
        "trg_user_overrides_cleanup_urls_match" in schema_body
    ), (
        "v1.19.74: SCHEMA must keep the trigger's CREATE statement "
        "with IF NOT EXISTS so init_db's post-migration "
        "executescript recreates it after the swap-dance drops it. "
        "Without IF NOT EXISTS or without the statement at all, the "
        "trigger would be lost permanently across migrations."
    )


# ── _drop_triggers_referencing helper ──────────────────────────


def test_drop_triggers_referencing_finds_only_matching(tmp_path):
    """The helper should drop ONLY triggers whose body references
    the given table — leaving unrelated triggers intact."""
    db_path = tmp_path / "selective.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript(
        """
        CREATE TABLE pending_updates (
            x INTEGER PRIMARY KEY
        );
        CREATE TABLE local_files (
            y INTEGER PRIMARY KEY
        );
        CREATE TABLE user_overrides (
            z INTEGER PRIMARY KEY
        );
        -- Trigger A: references pending_updates (should be dropped).
        CREATE TRIGGER trg_a AFTER DELETE ON user_overrides
            FOR EACH ROW BEGIN
                DELETE FROM pending_updates WHERE x = OLD.z;
            END;
        -- Trigger B: references local_files (should be left alone).
        CREATE TRIGGER trg_b AFTER DELETE ON user_overrides
            FOR EACH ROW BEGIN
                DELETE FROM local_files WHERE y = OLD.z;
            END;
        """
    )
    conn.commit()
    from app.core.db import _drop_triggers_referencing
    _drop_triggers_referencing(conn, "pending_updates")
    names = sorted(
        n for (n,) in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger'"
        ).fetchall()
    )
    assert names == ["trg_b"], (
        "v1.19.74: _drop_triggers_referencing must drop only "
        "triggers whose body references the given table name. "
        f"Got: {names}"
    )
    conn.close()


# ── Source pin: ordering + presence ─────────────────────────────


def test_helper_calls_drop_triggers_referencing_before_rebuild():
    """Source-level pin: `_drop_triggers_referencing` must be
    called BEFORE the `conn.execute(new_create_sql)` step in
    `_widen_check_constraint`. Pre-fix the swap-dance ran without
    dropping referencing triggers → SQLite refused the RENAME."""
    db_py = (REPO / "app" / "core" / "db.py").read_text()
    start = db_py.index("def _widen_check_constraint")
    # Bound the slice to the helper itself, ending at the next
    # `def `. Pre-v1.19.74 a fixed 8000-char window was used —
    # broke as the helper grew past it.
    end = db_py.index("\ndef ", start + 1)
    fn = db_py[start:end]
    drop_pos = fn.index("_drop_triggers_referencing(conn, table)")
    create_pos = fn.index("conn.execute(new_create_sql)")
    assert drop_pos < create_pos, (
        "v1.19.74: _drop_triggers_referencing must be called "
        "BEFORE the table CREATE step in the swap-dance."
    )


def test_v1_19_74_version_pin():
    """Loose prefix — later tags continue the v1.19.x line."""
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
