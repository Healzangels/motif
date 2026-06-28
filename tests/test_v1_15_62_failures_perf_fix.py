"""v1.15.62 — perf regression fix: _FAILURES_SFA_FROM_SQL OR-join +
missing idx_plex_items_theme_id index.

the user: "everything is very slow loading over a minute to load
in a fresh tab or switch between tabs once opened. things in the
tabs also take forever to load but button clicks respond
instantly."

## Bug

v1.15.61 added an OR-join to `_FAILURES_SFA_FROM_SQL` so orphan
failures would count:
    JOIN plex_items pi
      ON (pi.guid_tmdb = t.tmdb_id
          AND pi.media_type = (CASE ...))
      OR pi.theme_id = t.id

SQLite's OR-join optimizer bailed on the CASE expression in the
guid_tmdb branch and degraded to ~cartesian plex_items scans.
With no `idx_plex_items_theme_id` either, every page load did
3+ full table scans via `_topbar_ssr_state`'s scalar subqueries
(failures_count + cookies_count + drops_count → 60s+ page loads).

Button clicks felt instant because they didn't hit those SSR
subqueries — only page loads / topbar refreshes did.

## Fix

1. Drop the OR. Use `pi.theme_id = t.id` exclusively. Catches
   both TDB-tracked rows AND orphans (synthetic theme creation
   sets pi.theme_id on the same UPDATE per api.py:~8934).
2. Add `idx_plex_items_theme_id` so the join uses the index.
3. Schema migration v49 → v50 creates the index on existing
   installs (CREATE INDEX IF NOT EXISTS — idempotent).

Mirrors the library main filter's join shape (`LEFT JOIN themes
t ON t.id = pi.theme_id`) — they agree even in the brief
pre-resolve-theme-ids window where theme_id is NULL.

Static-text guards consistent with v1.15.61 SFA-predicate test
patterns + v1.15.48/56 schema-migration test patterns.
"""
from __future__ import annotations

import sqlite3
import sys
import tempfile
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"
DB_PY = REPO / "app" / "core" / "db.py"


def test_failures_sfa_from_sql_uses_single_theme_id_join():
    """The constant must use ONLY `pi.theme_id = t.id` for the
    join, NOT the v1.15.61 OR-join (which tanked SQLite's query
    planner). Mirrors the library main filter's join shape so
    count and filter agree."""
    src = API_PY.read_text()
    anchor = src.index("_FAILURES_SFA_FROM_SQL = (")
    end = src.index("\n)", anchor) + 2
    sql = src[anchor:end]
    flat = " ".join(sql.split())
    assert "ON pi.theme_id = t.id" in flat, (
        "v1.15.62: must use single theme_id join — fast + indexed"
    )
    assert " OR pi.theme_id = t.id" not in flat, (
        "v1.15.62: v1.15.61's OR-join must be removed — SQLite "
        "optimizer bailed on it, causing 60s+ page loads"
    )
    assert "pi.guid_tmdb = t.tmdb_id" not in flat, (
        "v1.15.62: guid_tmdb join clause must be gone — theme_id "
        "alone covers both TDB rows AND orphans"
    )


def test_idx_plex_items_theme_id_in_schema():
    """The SCHEMA constant must include the new index. Without
    it, fresh installs would have no index → same perf bug
    re-emerges on first install."""
    src = DB_PY.read_text()
    assert "CREATE INDEX IF NOT EXISTS idx_plex_items_theme_id" in src, (
        "v1.15.62: idx_plex_items_theme_id must be in SCHEMA for "
        "fresh installs"
    )
    # Index must be on plex_items(theme_id).
    idx_anchor = src.index("idx_plex_items_theme_id")
    block = src[idx_anchor:idx_anchor + 200]
    assert "ON plex_items (theme_id)" in block, (
        "v1.15.62: idx_plex_items_theme_id must be on plex_items(theme_id)"
    )


def test_v50_migration_function_exists_and_wired():
    """Existing installs (v49) must get the index via the
    migration. Without it, in-place upgrades retain the perf bug."""
    src = DB_PY.read_text()
    assert "def _migrate_v49_to_v50(" in src, (
        "v1.15.62: _migrate_v49_to_v50 helper required for existing "
        "v49 deploys to get the new index"
    )
    mig_anchor = src.index("def _migrate_v49_to_v50(")
    mig_end = src.index("\ndef _migrate", mig_anchor + 1)
    mig_body = src[mig_anchor:mig_end]
    assert "idx_plex_items_theme_id" in mig_body
    assert "CREATE INDEX IF NOT EXISTS" in mig_body, (
        "v1.15.62: migration must use IF NOT EXISTS so re-runs "
        "are no-ops"
    )
    # Wired into the chain.
    chain_anchor = src.index("elif current == 49:")
    chain_block = src[chain_anchor:chain_anchor + 200]
    assert "_migrate_v49_to_v50(conn)" in chain_block, (
        "v1.15.62: v49→v50 migration must be wired into the chain"
    )


def test_current_schema_version_at_least_50():
    """Without the version bump, existing v49 databases stay at
    v49 and never apply the v50 migration. v1.15.62 bumped to 50;
    later migrations (v1.15.81 added v51) advance it further so
    we assert >= 50 rather than pinning to a single value."""
    import re as _re
    src = DB_PY.read_text()
    m = _re.search(r"CURRENT_SCHEMA_VERSION\s*=\s*(\d+)", src)
    assert m, "CURRENT_SCHEMA_VERSION constant must exist"
    assert int(m.group(1)) >= 50, (
        f"v1.15.62: CURRENT_SCHEMA_VERSION must be >= 50; got "
        f"{m.group(1)}"
    )


def test_init_db_creates_idx_plex_items_theme_id():
    """Behavioral test: init_db() against a fresh DB must
    actually create the index in the live schema. Catches
    SCHEMA-text-vs-runtime drift."""
    if str(REPO) not in sys.path:
        sys.path.insert(0, str(REPO))
    from app.core.db import init_db

    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        init_db(db_path)
        with sqlite3.connect(str(db_path)) as conn:
            row = conn.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='index' AND name='idx_plex_items_theme_id'"
            ).fetchone()
        assert row is not None, (
            "v1.15.62: init_db() must create idx_plex_items_theme_id — "
            "without it, _FAILURES_SFA_FROM_SQL subqueries are slow"
        )
