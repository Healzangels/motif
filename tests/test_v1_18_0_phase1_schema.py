"""v1.18.0 Phase 1 — schema v55 migration for Plex Collections.

Phase 1 of the v1.18.0 multi-tag build adds the new media_type
('collection') + placement_kind ('plex_upload') values to the
existing CHECK constraints. SQLite can't ALTER CHECK in place,
so each affected table goes through the table-recreate dance —
but with a robust `_widen_check_constraint` helper that reads
the live CREATE SQL from sqlite_master and regex-replaces just
the CHECK clause. This avoids the brittleness of duplicating
column lists in the migration (50+ tags of schema evolution
have happened on some of these tables).

Tables affected:
- themes.media_type: + 'collection'
- plex_items.media_type: + 'collection'
- placements.placement_kind: + 'plex_upload'
- previous_urls.media_type: + 'collection'
- section_failure_acks.media_type: + 'collection'

`local_files` and `user_overrides` already have CHECK-less
media_type — they accept arbitrary strings, no change needed.
`pending_updates.media_type` is also CHECK-less.

Subsequent v1.18.0 phases (sync, plex_enum, worker place
adapter, UI) build on top of this schema foundation.
"""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
DB_PY = REPO / "app" / "core" / "db.py"


# ── Schema-level structural pins ──────────────────────────────


def test_schema_version_at_or_above_55():
    src = DB_PY.read_text()
    m = re.search(r"CURRENT_SCHEMA_VERSION\s*=\s*(\d+)", src)
    assert m
    assert int(m.group(1)) >= 55, (
        f"v1.18.0 Phase 1: CURRENT_SCHEMA_VERSION must be >= 55 "
        f"(got {m.group(1)})."
    )


def test_schema_strings_include_collection_for_media_type():
    """The top-level SCHEMA strings used by fresh installs must
    list 'collection' alongside 'movie'/'tv' on the relevant
    tables. Pin each so a future cleanup doesn't quietly drop
    the new value."""
    src = DB_PY.read_text()
    # themes
    assert "media_type IN ('movie', 'tv', 'collection')" in src, (
        "v1.18.0: themes.media_type CHECK must include 'collection'."
    )
    # plex_items (uses 'show' not 'tv' — Plex's own type string)
    assert "media_type IN ('movie', 'show', 'collection')" in src, (
        "v1.18.0: plex_items.media_type CHECK must include "
        "'collection' (Plex's `<Directory type=\"collection\">` "
        "value)."
    )
    # previous_urls + section_failure_acks both use 'movie','tv'
    # — there should be at least 3 occurrences of the
    # ('movie', 'tv', 'collection') pattern total (themes +
    # previous_urls + section_failure_acks).
    occurrences = src.count("media_type IN ('movie', 'tv', 'collection')")
    assert occurrences >= 3, (
        f"v1.18.0: expected 'collection' added to themes / "
        f"previous_urls / section_failure_acks CHECKs; found "
        f"{occurrences} occurrence(s)."
    )


def test_schema_strings_include_plex_upload_placement_kind():
    src = DB_PY.read_text()
    assert (
        "placement_kind IN ('hardlink', 'copy', 'symlink', 'plex_upload')"
        in src
    ), (
        "v1.18.0: placements.placement_kind CHECK must include "
        "'plex_upload' for collection theme uploads via POST "
        "/library/metadata/{rk}/themes."
    )


# ── Migration function exists + wired ────────────────────────


def test_migration_function_exists():
    src = DB_PY.read_text()
    assert "def _migrate_v54_to_v55(" in src, (
        "v1.18.0: _migrate_v54_to_v55 must exist."
    )
    # The helper that does the actual CHECK widening.
    assert "def _widen_check_constraint(" in src, (
        "v1.18.0: _widen_check_constraint helper must exist."
    )


def test_migration_wired_into_chain():
    """init_db's migration ladder must call _migrate_v54_to_v55
    at the v54 step."""
    src = DB_PY.read_text()
    assert "elif current == 54:" in src and "_migrate_v54_to_v55(conn)" in src


# ── Fresh-install end-to-end (init_db on an empty DB) ────────


@pytest.fixture
def fresh_db(tmp_path: Path) -> Path:
    db = tmp_path / "motif.db"
    from app.core.db import init_db
    init_db(db)
    return db


def test_fresh_install_accepts_collection_media_type(fresh_db):
    """A fresh DB at v55 must accept inserts with
    media_type='collection' on each of the 5 widened tables."""
    with sqlite3.connect(fresh_db) as conn:
        # themes
        conn.execute(
            "INSERT INTO themes "
            "  (media_type, tmdb_id, title, year, upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('collection', 1241, 'Harry Potter Collection', "
            "        '2001', 'themoviedb', "
            "        datetime('now'), datetime('now'))"
        )
        # previous_urls
        conn.execute(
            "INSERT INTO previous_urls "
            "  (media_type, tmdb_id, section_id, youtube_url, "
            "   kind, captured_at) "
            "VALUES ('collection', 1241, '1', "
            "        'https://youtu.be/xxx', 'user', "
            "        datetime('now'))"
        )
        # section_failure_acks
        conn.execute(
            "INSERT INTO section_failure_acks "
            "  (media_type, tmdb_id, section_id, acked_at, acked_by) "
            "VALUES ('collection', 1241, '1', "
            "        datetime('now'), 'test')"
        )
        # plex_items (uses Plex's 'collection' type string —
        # different from media_type for movies/'show' for TV)
        conn.execute(
            "INSERT INTO plex_items "
            "  (rating_key, section_id, media_type, title, "
            "   year, first_seen_at, last_seen_at) "
            "VALUES ('99999', '1', 'collection', "
            "        'Harry Potter Collection', '2001', "
            "        datetime('now'), datetime('now'))"
        )
        conn.commit()


def test_fresh_install_accepts_plex_upload_placement_kind(fresh_db):
    """A fresh DB at v55 must accept placement_kind='plex_upload'
    — the new value for HTTP-POST-based collection theme
    uploads."""
    with sqlite3.connect(fresh_db) as conn:
        # Need a themes row first (FK target).
        conn.execute(
            "INSERT INTO themes "
            "  (media_type, tmdb_id, title, upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('collection', 1241, 'Harry Potter', "
            "        'themoviedb', datetime('now'), datetime('now'))"
        )
        # And a plex_sections row (FK target).
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, included, "
            "   discovered_at, last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 1, "
            "        datetime('now'), datetime('now'))"
        )
        # The actual placement row with the new kind.
        conn.execute(
            "INSERT INTO placements "
            "  (media_type, tmdb_id, section_id, media_folder, "
            "   placed_at, placement_kind) "
            "VALUES ('collection', 1241, '1', "
            "        'plex://collection/99999', "
            "        datetime('now'), 'plex_upload')"
        )
        conn.commit()


# ── Migration path (existing v54 install upgrades to v55) ────


def _build_v54_db(db: Path) -> None:
    """Build a fresh DB at schema v54 by temporarily lowering
    CURRENT_SCHEMA_VERSION before init_db, then restoring it.
    The migration ladder will pick up v54→v55 the next time
    init_db runs on this DB."""
    # We can't easily build a real v54 DB without forking the
    # module, so we directly create the minimal v54 shape +
    # stamp the version. The migration is what we're testing.
    with sqlite3.connect(db) as conn:
        conn.executescript("""
            CREATE TABLE schema_version (
                version INTEGER PRIMARY KEY,
                applied_at TEXT NOT NULL
            );
            CREATE TABLE themes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                media_type TEXT NOT NULL CHECK (media_type IN ('movie', 'tv')),
                tmdb_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                upstream_source TEXT NOT NULL
                    CHECK (upstream_source IN ('imdb', 'themoviedb', 'plex_orphan')),
                last_seen_sync_at TEXT NOT NULL,
                first_seen_sync_at TEXT NOT NULL,
                UNIQUE (media_type, tmdb_id)
            );
            CREATE TABLE plex_items (
                rating_key TEXT PRIMARY KEY,
                section_id TEXT NOT NULL,
                media_type TEXT NOT NULL CHECK (media_type IN ('movie', 'show')),
                title TEXT NOT NULL,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL
            );
            CREATE TABLE placements (
                media_type TEXT NOT NULL,
                tmdb_id INTEGER NOT NULL,
                section_id TEXT NOT NULL,
                media_folder TEXT NOT NULL,
                placed_at TEXT NOT NULL,
                placement_kind TEXT NOT NULL
                    CHECK (placement_kind IN ('hardlink', 'copy', 'symlink')),
                PRIMARY KEY (media_type, tmdb_id, section_id, media_folder)
            );
            CREATE TABLE previous_urls (
                media_type TEXT NOT NULL CHECK (media_type IN ('movie', 'tv')),
                tmdb_id INTEGER NOT NULL,
                section_id TEXT NOT NULL DEFAULT '',
                youtube_url TEXT NOT NULL,
                kind TEXT NOT NULL CHECK (kind IN ('user', 'themerrdb')),
                captured_at TEXT NOT NULL,
                PRIMARY KEY (media_type, tmdb_id, section_id)
            );
            CREATE TABLE section_failure_acks (
                media_type TEXT NOT NULL CHECK (media_type IN ('movie', 'tv')),
                tmdb_id INTEGER NOT NULL,
                section_id TEXT NOT NULL,
                acked_at TEXT NOT NULL,
                acked_by TEXT,
                PRIMARY KEY (media_type, tmdb_id, section_id)
            );
            INSERT INTO schema_version (version, applied_at)
            VALUES (54, datetime('now'));
        """)


def test_widen_check_constraint_keeps_existing_data(tmp_path: Path):
    """End-to-end: seed a minimal v54 DB with rows, run the
    widening helper directly on one table, verify rows survive
    and the new value is now accepted."""
    db = tmp_path / "motif.db"
    _build_v54_db(db)
    with sqlite3.connect(db) as conn:
        # Seed a row with the existing-valid value.
        conn.execute(
            "INSERT INTO themes "
            "  (media_type, tmdb_id, title, upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('movie', 42, 'Foo', 'themoviedb', "
            "        datetime('now'), datetime('now'))"
        )
        conn.commit()
        # Pre-widening: 'collection' is rejected.
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO themes "
                "  (media_type, tmdb_id, title, upstream_source, "
                "   last_seen_sync_at, first_seen_sync_at) "
                "VALUES ('collection', 1241, 'HP', 'themoviedb', "
                "        datetime('now'), datetime('now'))"
            )
        # Run the widening helper.
        from app.core.db import _widen_check_constraint
        _widen_check_constraint(
            conn, "themes", "media_type",
            ("movie", "tv"), ("movie", "tv", "collection"),
        )
        # Pre-existing row survived.
        row = conn.execute(
            "SELECT title FROM themes WHERE tmdb_id = 42"
        ).fetchone()
        assert row is not None and row[0] == "Foo", (
            "v55 widening must preserve pre-existing rows."
        )
        # 'collection' now accepted.
        conn.execute(
            "INSERT INTO themes "
            "  (media_type, tmdb_id, title, upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('collection', 1241, 'HP', 'themoviedb', "
            "        datetime('now'), datetime('now'))"
        )


def test_full_migration_v54_to_v55_widens_all_tables(tmp_path: Path):
    """End-to-end: build a v54 DB, run the full migration,
    verify each affected table now accepts the new value."""
    db = tmp_path / "motif.db"
    _build_v54_db(db)
    with sqlite3.connect(db) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        from app.core.db import _migrate_v54_to_v55
        _migrate_v54_to_v55(conn)
        # All five tables now accept the new values.
        # themes
        conn.execute(
            "INSERT INTO themes "
            "  (media_type, tmdb_id, title, upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('collection', 1241, 'HP', 'themoviedb', "
            "        datetime('now'), datetime('now'))"
        )
        # plex_items (collection here is Plex's type string)
        conn.execute(
            "INSERT INTO plex_items "
            "  (rating_key, section_id, media_type, title, "
            "   first_seen_at, last_seen_at) "
            "VALUES ('99999', '1', 'collection', 'HP Collection', "
            "        datetime('now'), datetime('now'))"
        )
        # placements with the new kind
        conn.execute(
            "INSERT INTO placements "
            "  (media_type, tmdb_id, section_id, media_folder, "
            "   placed_at, placement_kind) "
            "VALUES ('collection', 1241, '1', 'plex://99999', "
            "        datetime('now'), 'plex_upload')"
        )
        # previous_urls
        conn.execute(
            "INSERT INTO previous_urls "
            "  (media_type, tmdb_id, section_id, youtube_url, "
            "   kind, captured_at) "
            "VALUES ('collection', 1241, '1', "
            "        'https://youtu.be/xxx', 'user', datetime('now'))"
        )
        # section_failure_acks
        conn.execute(
            "INSERT INTO section_failure_acks "
            "  (media_type, tmdb_id, section_id, acked_at, acked_by) "
            "VALUES ('collection', 1241, '1', datetime('now'), 'test')"
        )
        conn.commit()
