"""v1.21.56 — per-edition theme isolation, Phase B3 (read-side prep).

Adds a folder-derived edition_key to plex_items (the Plex view), populated
by plex_enum + backfilled at boot, so Phase C's library JOIN can match
placements/local_files per edition WITHOUT a fragile SQL re-implementation
of normalize_edition — a stored column is the mirror. Also scopes the
scheduler's placement-retry sweep by edition so an Extended row missing
its placement actually gets re-enqueued (and the job carries its edition).

Purely additive / behavior-preserving for the standard ('' ) edition,
which is every install today.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from app.core.db import init_db, CURRENT_SCHEMA_VERSION


NOW = "2026-06-04T00:00:00Z"


@pytest.fixture
def db(tmp_path: Path) -> Path:
    p = tmp_path / "motif.db"
    init_db(p)
    return p


def _cols(db, table):
    with sqlite3.connect(db) as conn:
        return {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}


# ── schema ──


def test_schema_version_at_least_64():
    assert CURRENT_SCHEMA_VERSION >= 64


def test_plex_items_has_edition_key(db):
    assert "edition_key" in _cols(db, "plex_items")


def test_plex_items_edition_key_defaults_empty(db):
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type,"
            " title, folder_path, first_seen_at, last_seen_at)"
            " VALUES ('rk','1','movie','X','/d/X (2000)',?,?)", (NOW, NOW))
        ek = conn.execute(
            "SELECT edition_key FROM plex_items WHERE rating_key='rk'"
        ).fetchone()[0]
    assert ek == ""


# ── migration v63 → v64 backfill from folder_path ──


def test_migrate_backfills_edition_key_from_folder(tmp_path):
    """A v63 plex_items row with an {edition-X} folder must, after the
    v64 migration, carry the normalized edition_key; standard + empty
    folders stay ''."""
    p = tmp_path / "m.db"
    init_db(p)
    with sqlite3.connect(p) as conn:
        for rk, folder in (
            ("rk-ext", "/data/Movies/LotR (2001) {edition-Extended}"),
            ("rk-std", "/data/Movies/LotR (2001)"),
            ("rk-coll", ""),  # collection sentinel
        ):
            conn.execute(
                "INSERT INTO plex_items (rating_key, section_id, media_type,"
                " title, folder_path, edition_key, first_seen_at,"
                " last_seen_at) VALUES (?,?, 'movie','X',?, '', ?, ?)",
                (rk, "1", folder, NOW, NOW))
        conn.execute("DELETE FROM schema_version")
        conn.execute("INSERT INTO schema_version (version, applied_at)"
                     " VALUES (63, ?)", (NOW,))
        conn.commit()
    init_db(p)  # drives _migrate_v63_to_v64
    with sqlite3.connect(p) as conn:
        keys = dict(conn.execute(
            "SELECT rating_key, edition_key FROM plex_items"))
    assert keys == {"rk-ext": "extended", "rk-std": "", "rk-coll": ""}


# ── scheduler retry is edition-scoped + carries the edition ──


def _seed_for_retry(db, *, tmdb_id, edition_key, placement_edition):
    """A local_files row for `edition_key` plus a placement that exists only
    for `placement_edition`. If the two differ, the local_files row has NO
    matching placement → the retry sweep should re-enqueue it."""
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO plex_sections (section_id, title, type,"
            " is_anime, is_4k, themes_subdir, included, discovered_at,"
            " last_seen_at) VALUES ('1','M','movie',0,0,'movies',1,?,?)",
            (NOW, NOW))
        conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, upstream_source,"
            " last_seen_sync_at, first_seen_sync_at) VALUES ('movie',?,'X',"
            "'imdb',?,?)", (tmdb_id, NOW, NOW))
        conn.execute(
            "INSERT INTO local_files (media_type, tmdb_id, section_id,"
            " edition_key, file_path, downloaded_at, source_video_id,"
            " provenance, source_kind) VALUES ('movie',?,'1',?,?,?, 'v',"
            "'auto','themerrdb')",
            (tmdb_id, edition_key, f"movies/X ({edition_key or 'std'}).mp3",
             NOW))
        conn.execute(
            "INSERT INTO placements (media_type, tmdb_id, section_id,"
            " edition_key, media_folder, placed_at, placement_kind,"
            " plex_refreshed) VALUES ('movie',?,'1',?, '/d/X', ?, 'hardlink',"
            " 1)", (tmdb_id, placement_edition, NOW))
        conn.commit()


def _place_jobs(db, tmdb_id):
    with sqlite3.connect(db) as conn:
        return conn.execute(
            "SELECT payload FROM jobs WHERE job_type='place' AND tmdb_id=?",
            (tmdb_id,)).fetchall()


def test_retry_reenqueues_edition_missing_its_placement(db):
    """local_files 'extended' + a placement only for '' → the 'extended'
    row has no matching placement, so the edition-scoped JOIN selects it
    and the retry enqueues a place job carrying edition_key='extended'.
    Pre-fix (3-tuple JOIN) the '' placement masked it and it never retried."""
    from app.core.scheduler import _retry_pending_placements
    _seed_for_retry(db, tmdb_id=120, edition_key="extended",
                    placement_edition="")
    _retry_pending_placements(db)
    jobs = _place_jobs(db, 120)
    assert len(jobs) == 1
    assert json.loads(jobs[0][0]).get("edition_key") == "extended"


def test_retry_skips_edition_that_is_placed(db):
    """When the placement DOES exist for the same edition, the row is
    'placed' and must NOT be re-enqueued (behavior-preserving)."""
    from app.core.scheduler import _retry_pending_placements
    _seed_for_retry(db, tmdb_id=121, edition_key="extended",
                    placement_edition="extended")
    _retry_pending_placements(db)
    assert _place_jobs(db, 121) == []


def test_retry_standard_row_payload_resolves_empty(db):
    """The '' row (production today): re-enqueued with edition_key='' —
    functionally identical to the pre-v1.21.56 '{}' payload."""
    from app.core.scheduler import _retry_pending_placements
    _seed_for_retry(db, tmdb_id=122, edition_key="", placement_edition="x")
    _retry_pending_placements(db)
    jobs = _place_jobs(db, 122)
    assert len(jobs) == 1
    assert json.loads(jobs[0][0]).get("edition_key") == ""
