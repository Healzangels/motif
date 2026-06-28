"""v1.21.51 — per-edition theme isolation, Phase A1 (schema v62).

Adds an `edition_key` column to the four theme-state tables (local_files,
user_overrides, pending_updates, placements). Purely additive: '' = the
standard edition = today's behavior. v63 will fold edition_key into the
PKs; this tag only adds the column + backfills `placements` from the
hardlink folder's {edition-X} tag.

These tests pin: the column exists on all four tables with the right
default; the migration is idempotent + behavior-preserving (a v61 fixture
upgrades with zero row loss); and the placements backfill derives
edition_key from the folder tag.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from app.core.db import init_db, CURRENT_SCHEMA_VERSION


REPO = Path(__file__).resolve().parent.parent


@pytest.fixture
def db(tmp_path: Path) -> Path:
    p = tmp_path / "motif.db"
    init_db(p)
    return p


def _cols(db: Path, table: str) -> set[str]:
    with sqlite3.connect(db) as conn:
        return {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}


def test_schema_version_at_least_62():
    # v62 added the column; v63 folded it into the PK. >= so this A1 test
    # doesn't go stale every schema bump.
    assert CURRENT_SCHEMA_VERSION >= 62


def test_edition_key_column_on_all_four_tables(db):
    for table in ("local_files", "user_overrides", "pending_updates",
                  "placements"):
        assert "edition_key" in _cols(db, table), table


def test_edition_key_defaults_to_empty_string(db):
    """A row inserted without edition_key gets '' — so existing code paths
    (which don't yet set it) keep producing standard-edition rows."""
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,'t','t')")
        conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, upstream_source,"
            " last_seen_sync_at, first_seen_sync_at)"
            " VALUES ('movie',100,'X','imdb','t','t')")
        conn.execute(
            "INSERT INTO local_files (media_type, tmdb_id, section_id,"
            " file_path, downloaded_at, source_video_id)"
            " VALUES ('movie',100,'1','x.mp3','t','vid')")
        conn.commit()
        ek = conn.execute(
            "SELECT edition_key FROM local_files WHERE tmdb_id=100"
        ).fetchone()[0]
    assert ek == ""


# ── migration from a v61 fixture: idempotent + zero row loss ──


def test_rerun_init_db_is_idempotent(db):
    """init_db on an already-v62 DB must be a clean no-op (the migration's
    PRAGMA table_info guard prevents a duplicate-column error)."""
    init_db(db)  # second run
    init_db(db)  # third run
    for table in ("local_files", "user_overrides", "pending_updates",
                  "placements"):
        assert "edition_key" in _cols(db, table)


def test_placements_backfill_derives_edition_key_from_folder(tmp_path):
    """A v61 DB whose placements row has an {edition-Theatrical} folder
    must, after upgrade, carry edition_key='theatrical'; a standard folder
    stays ''; a plex_upload row (media_folder='') stays ''."""
    p = tmp_path / "m.db"
    init_db(p)
    # Seed three placements (the column exists at v62), then simulate a
    # pre-backfill state by clearing edition_key + stamping version 61, and
    # re-run init_db to drive _migrate_v61_to_v62's backfill.
    with sqlite3.connect(p) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,'t','t')")
        for tmdb, folder, kind in (
            (1, "/d/Title (2001) {edition-Theatrical}", "hardlink"),
            (2, "/d/Title2 (2001)", "hardlink"),
            (3, "", "plex_upload"),
        ):
            conn.execute(
                "INSERT INTO themes (media_type, tmdb_id, title,"
                " upstream_source, last_seen_sync_at, first_seen_sync_at)"
                " VALUES ('movie',?,'X','imdb','t','t')", (tmdb,))
            conn.execute(
                "INSERT INTO placements (media_type, tmdb_id, section_id,"
                " media_folder, placed_at, placement_kind, plex_refreshed,"
                " edition_key) VALUES ('movie',?,'1',?,'t',?,0,'')",
                (tmdb, folder, kind))
        conn.execute("DELETE FROM schema_version")
        conn.execute("INSERT INTO schema_version (version, applied_at)"
                     " VALUES (61,'t')")
        conn.commit()
    init_db(p)  # drives _migrate_v61_to_v62 (re-adds col is a no-op; backfills)
    with sqlite3.connect(p) as conn:
        keys = dict(conn.execute(
            "SELECT tmdb_id, edition_key FROM placements ORDER BY tmdb_id"))
    assert keys == {1: "theatrical", 2: "", 3: ""}
