"""v1.17.17 — orphan row recovery path.

the user's screenshot showed the SOURCE menu on a `plex_orphan`
row (TDB pill = "NO TDB") offering RE-DOWNLOAD TDB as the only
recovery action — confusing AND functionally broken (the
"themerrdb URL" stored on an orphan is actually the URL
captured during the original ADOPT, not a real ThemerrDB URL).

Two interacting bugs:

## Bug A — frontend gate missing isOrphan

`app.js:7696` SOURCE-menu gate for DOWNLOAD / RE-DOWNLOAD TDB
gated on `themed && themeId && hasDownloadUrl` but didn't
exclude `isOrphan`. Plex-orphan rows have a `themes` row + a
captured `youtube_url`, so the gate passed and the
TDB-flavored action appeared. Fix: add `&& !isOrphan` to the
gate.

## Bug B — capture mis-labels orphan-sourced URLs

`_capture_previous_url` (api.py) fell back to
`themes.youtube_url` when no user_override existed, and
hardcoded `kind='themerrdb'` regardless of `upstream_source`.
For `plex_orphan` rows the URL came from adopt, not TDB.

The mis-labeled kind triggered the `revert_redundant` SQL
branch at `api.py:1977-1980` post-PURGE:

    lf.file_path IS NULL                              -- post-PURGE
    AND COALESCE(pv_sec.kind, pv_global.kind) = 'themerrdb'  -- mis-label
    AND COALESCE(pv_sec.hidden_url, pv_global.hidden_url) IS NULL
    AND COALESCE(pv_sec.youtube_url, pv_global.youtube_url) = t.youtube_url

`revert_redundant=1` hides RESTORE in the SOURCE menu (gate at
`app.js:7951` AND's `!it.revert_redundant`). Combined with
Bug A, the user was left with no working recovery path.

Fix: when capturing from a plex_orphan-sourced themes row, use
`kind='user'` so the redundancy branch doesn't fire. The
worker handles user-kind RESTORE by writing the URL to
`user_overrides` and queuing a download — exactly the
"re-instate my adopted theme" flow the user wants.

## Schema v54

The bugs combine: even after the v1.17.17 deploy lands, your
existing `previous_urls` rows captured with the wrong kind
stay stranded. Schema v54 retroactively flips them:

    UPDATE previous_urls
    SET kind = 'user'
    WHERE kind = 'themerrdb'
      AND EXISTS (
        SELECT 1 FROM themes
        WHERE themes.media_type = previous_urls.media_type
          AND themes.tmdb_id   = previous_urls.tmdb_id
          AND themes.upstream_source = 'plex_orphan'
      );

Idempotent (the WHERE clause already constrains to wrong-kind+
orphan-source).
"""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"
API_PY = REPO / "app" / "web" / "api.py"
DB_PY = REPO / "app" / "core" / "db.py"
APP_INIT = REPO / "app" / "__init__.py"


# ── Bug A: frontend gate ─────────────────────────────────────


def test_download_tdb_gate_excludes_orphan_rows():
    """The SOURCE-menu DOWNLOAD/RE-DOWNLOAD TDB gate must
    include `&& !isOrphan` so plex_orphan rows don't see the
    TDB-flavored action."""
    src = APP_JS.read_text()
    # Find the gate (single anchor inside the menu builder).
    idx = src.index(
        "// DOWNLOAD TDB / RE-DOWNLOAD TDB — T-source rows where the")
    end = src.index("if (!tdbBlocked && hasDownloadUrl) {", idx)
    block = src[idx:end]
    assert "!isOrphan" in block, (
        "v1.17.17: SOURCE-menu DOWNLOAD/RE-DOWNLOAD TDB gate "
        "must exclude orphan rows — plex_orphan has no TDB URL "
        "to fetch from. See the user's NO-TDB-row screenshot."
    )


# ── Bug B: backend capture kind ──────────────────────────────


def test_capture_orphan_kind_is_user():
    """`_capture_previous_url` must select `kind='user'` for
    plex_orphan-sourced captures so the revert_redundant SQL
    doesn't suppress RESTORE post-PURGE."""
    src = API_PY.read_text()
    # Locate the capture-kind classification block.
    idx = src.index("def _capture_previous_url(")
    end = src.index("def ", idx + 1)
    fn = src[idx:end]
    # The themes SELECT must pull upstream_source so the
    # classification has the data it needs.
    assert "upstream_source FROM themes" in fn, (
        "v1.17.17: capture must SELECT upstream_source so kind "
        "classification can branch on plex_orphan."
    )
    # The orphan branch must exist.
    assert 'upstream_source"] == "plex_orphan"' in fn, (
        "v1.17.17: capture must check upstream_source == "
        "'plex_orphan' before assigning kind."
    )
    # And the orphan branch must emit kind='user'.
    orphan_block = fn[fn.index('upstream_source"] == "plex_orphan"'):]
    assert orphan_block.split("\n", 4)[1].strip().startswith(
        'prev_kind = "user"'
    ) or 'prev_kind = "user"' in orphan_block[:200], (
        "v1.17.17: plex_orphan capture must assign "
        "prev_kind='user'."
    )


def test_capture_legacy_themerrdb_kind_preserved():
    """Counter-pin: non-orphan themes rows still get
    `kind='themerrdb'` — we only changed the orphan branch."""
    src = API_PY.read_text()
    idx = src.index("def _capture_previous_url(")
    end = src.index("def ", idx + 1)
    fn = src[idx:end]
    assert 'prev_kind = "themerrdb"' in fn, (
        "v1.17.17: non-orphan capture must still emit "
        "kind='themerrdb' — only the orphan branch changes."
    )


# ── Schema v54 migration ─────────────────────────────────────


def test_schema_version_bumped_to_v54():
    src = DB_PY.read_text()
    m = re.search(r"CURRENT_SCHEMA_VERSION\s*=\s*(\d+)", src)
    assert m
    assert int(m.group(1)) >= 54, (
        f"v1.17.17: CURRENT_SCHEMA_VERSION must be >= 54 "
        f"(got {m.group(1)})."
    )


def test_v53_to_v54_migration_exists():
    src = DB_PY.read_text()
    assert "def _migrate_v53_to_v54(" in src, (
        "v1.17.17: _migrate_v53_to_v54 must exist."
    )
    idx = src.index("def _migrate_v53_to_v54(")
    end = src.index("def ", idx + 1)
    body = src[idx:end]
    assert "UPDATE previous_urls" in body, (
        "v1.17.17: migration must UPDATE previous_urls."
    )
    assert "SET kind = 'user'" in body
    assert "kind = 'themerrdb'" in body
    assert "upstream_source = 'plex_orphan'" in body, (
        "v1.17.17: migration must filter on themes."
        "upstream_source = 'plex_orphan'."
    )
    # Chain wiring.
    assert "elif current == 53:" in src and "_migrate_v53_to_v54(conn)" in src, (
        "v1.17.17: init_db migration ladder must include the "
        "v53→v54 step."
    )


@pytest.fixture
def fresh_db(tmp_path: Path) -> Path:
    db = tmp_path / "motif.db"
    from app.core.db import init_db
    init_db(db)
    return db


def _seed_orphan_theme_with_captured_url(
    db: Path, *, tmdb_id: int, captured_url: str,
):
    """Seed a plex_orphan themes row + its previous_urls capture
    (with the buggy kind='themerrdb' that pre-v1.17.17 wrote)."""
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO themes "
            "  (media_type, tmdb_id, title, year, youtube_url, "
            "   upstream_source, last_seen_sync_at, "
            "   first_seen_sync_at) "
            "VALUES (?, ?, 'Some Orphan', '2020', ?, "
            "        'plex_orphan', datetime('now'), datetime('now'))",
            ("movie", tmdb_id, captured_url),
        )
        conn.execute(
            "INSERT INTO previous_urls "
            "  (media_type, tmdb_id, section_id, youtube_url, "
            "   kind, captured_at) "
            "VALUES (?, ?, '', ?, 'themerrdb', datetime('now'))",
            ("movie", tmdb_id, captured_url),
        )
        conn.commit()


def test_migration_flips_kind_for_orphan_sourced_rows(fresh_db):
    """End-to-end: seed a row in the pre-v1.17.17 buggy state,
    run the migration, verify the kind is flipped."""
    _seed_orphan_theme_with_captured_url(
        fresh_db, tmdb_id=42,
        captured_url="https://www.youtube.com/watch?v=xxx",
    )
    # The migration runs at init_db time, but init_db already
    # ran in the fixture (at the current latest schema). Re-run
    # the migration function explicitly to verify it's idempotent
    # + does what we expect on this seeded state.
    from app.core.db import _migrate_v53_to_v54
    with sqlite3.connect(fresh_db) as conn:
        _migrate_v53_to_v54(conn)
        row = conn.execute(
            "SELECT kind FROM previous_urls "
            "WHERE media_type = 'movie' AND tmdb_id = 42",
        ).fetchone()
    assert row[0] == "user", (
        f"v1.17.17: migration must flip kind to 'user' for "
        f"plex_orphan-sourced rows (got {row[0]!r})."
    )


def test_migration_leaves_real_themerrdb_rows_alone(fresh_db):
    """Counter-pin: a previous_urls row whose source theme is
    a real ThemerrDB row (`upstream_source` ∈ themoviedb/imdb)
    must keep `kind='themerrdb'`."""
    with sqlite3.connect(fresh_db) as conn:
        conn.execute(
            "INSERT INTO themes "
            "  (media_type, tmdb_id, title, year, youtube_url, "
            "   upstream_source, last_seen_sync_at, "
            "   first_seen_sync_at) "
            "VALUES (?, ?, 'Real TDB Title', '2020', "
            "        'https://youtu.be/yyy', 'themoviedb', "
            "        datetime('now'), datetime('now'))",
            ("movie", 99),
        )
        conn.execute(
            "INSERT INTO previous_urls "
            "  (media_type, tmdb_id, section_id, youtube_url, "
            "   kind, captured_at) "
            "VALUES (?, ?, '', 'https://youtu.be/yyy', "
            "        'themerrdb', datetime('now'))",
            ("movie", 99),
        )
        conn.commit()
    from app.core.db import _migrate_v53_to_v54
    with sqlite3.connect(fresh_db) as conn:
        _migrate_v53_to_v54(conn)
        row = conn.execute(
            "SELECT kind FROM previous_urls "
            "WHERE media_type = 'movie' AND tmdb_id = 99",
        ).fetchone()
    assert row[0] == "themerrdb", (
        "v1.17.17: real-TDB-sourced captures must keep their "
        "kind unchanged. Only orphan-sourced captures flip."
    )


def test_migration_is_idempotent(fresh_db):
    """Re-running the migration must be a no-op."""
    _seed_orphan_theme_with_captured_url(
        fresh_db, tmdb_id=42,
        captured_url="https://www.youtube.com/watch?v=xxx",
    )
    from app.core.db import _migrate_v53_to_v54
    with sqlite3.connect(fresh_db) as conn:
        _migrate_v53_to_v54(conn)  # first run flips
        _migrate_v53_to_v54(conn)  # second run is a no-op
        row = conn.execute(
            "SELECT kind FROM previous_urls "
            "WHERE media_type = 'movie' AND tmdb_id = 42",
        ).fetchone()
    assert row[0] == "user"


# ── Version pin (soft floor) ─────────────────────────────────


def test_version_pinned_at_or_above_1_17_17():
    src = APP_INIT.read_text()
    m = re.search(r'__version__\s*=\s*"(\d+)\.(\d+)\.(\d+)"', src)
    assert m
    found = tuple(int(x) for x in m.groups())
    assert found >= (0, 17, 17), (
        f"v1.17.17: __version__ must be >= 1.17.17 "
        f"(found {'.'.join(str(x) for x in found)})."
    )
