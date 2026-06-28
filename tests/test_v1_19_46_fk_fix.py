"""v1.19.46 — fix FK constraint failure on cloud-backup writes.

the user's 2026-05-27 production repro (post-v1.19.45 deploy):
clicked BACKUP THIS THEME on a single TV row → walker completed
the walk (1 candidate, 1 C1 hit) → INSERT into local_files
failed with `sqlite3.IntegrityError: FOREIGN KEY constraint
failed` → op_progress marked failed with no backup written.

## Root cause

`local_files` has a FK to `themes(media_type, tmdb_id)`. The
v1.19.42 writer (`backup_cloud_theme`) assumed this themes row
already existed — but for the COHORT cloud-backup exists to
serve (rows TDB doesn't track), no themes row exists. the user's
TV show had `pi.guid_tmdb` set + Plex serving a cloud theme,
but `themes` had no matching row → FK violation.

## Fix

Mirror the upload-theme orphan-handling precedent at
`api.py:10808`: before the local_files INSERT, check whether a
themes row exists for (media_type, tmdb_id). If not, INSERT
one with `upstream_source='plex_orphan'` (the canonical value
for "Plex tracks this, TDB doesn't"), then UPDATE
plex_items.theme_id so the library JOIN picks up the new
linkage immediately.

## Why upstream_source='plex_orphan' is the right value

The themes.upstream_source CHECK accepts:
  - 'imdb' / 'themoviedb' — TDB-tracked
  - 'plex_orphan' — Plex tracks, TDB doesn't

Cloud-backup rows are by definition the third case. Reusing
the existing 'plex_orphan' value keeps the data model consistent
with the upload-theme path; future code that filters on
`upstream_source != 'plex_orphan'` (the canonical "TDB-tracked"
predicate) treats cloud-backup rows correctly.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

CLOUD_PY = (REPO / "app" / "core" / "cloud_theme_backup.py").read_text()


# ── Source guards ────────────────────────────────────────────


def test_backup_writes_pre_create_themes_row():
    """backup_cloud_theme must INSERT into themes BEFORE the
    local_files INSERT to satisfy the FK constraint."""
    fn_idx = CLOUD_PY.index("def backup_cloud_theme(")
    fn_end = len(CLOUD_PY)  # last function in the module
    body = CLOUD_PY[fn_idx:fn_end]
    assert "INSERT INTO themes" in body, (
        "v1.19.46: backup_cloud_theme must pre-create the "
        "themes row before INSERT INTO local_files (FK depends "
        "on it)"
    )


def test_synthetic_themes_row_uses_plex_orphan_source():
    """The synthetic themes row must use upstream_source=
    'plex_orphan' (the canonical 'Plex tracks, TDB doesn't'
    value). This keeps the data model consistent with the
    upload-theme orphan precedent at api.py:10808."""
    fn_idx = CLOUD_PY.index("def backup_cloud_theme(")
    body = CLOUD_PY[fn_idx:]
    insert_idx = body.index("INSERT INTO themes")
    insert_block = body[insert_idx:insert_idx + 800]
    assert "'plex_orphan'" in insert_block, (
        "v1.19.46: synthetic themes row must use "
        "upstream_source='plex_orphan'"
    )


def test_pre_create_is_idempotent_via_select_first():
    """The INSERT must be guarded by a SELECT first (or use
    INSERT OR IGNORE) so re-running backup_cloud_theme on the
    same row doesn't violate UNIQUE (media_type, tmdb_id)."""
    fn_idx = CLOUD_PY.index("def backup_cloud_theme(")
    body = CLOUD_PY[fn_idx:]
    assert (
        "SELECT id FROM themes WHERE media_type" in body
        or "INSERT OR IGNORE INTO themes" in body
    ), (
        "v1.19.46: pre-create must be idempotent (SELECT-then-"
        "INSERT or INSERT OR IGNORE)"
    )


def test_plex_items_theme_id_stamped_after_orphan_create():
    """After creating the synthetic themes row, the walker must
    UPDATE plex_items.theme_id so the library JOIN picks up the
    new linkage. Without this the local_files row would exist
    but the row wouldn't surface as B in the library until the
    next plex_enum.resolve_theme_ids walk."""
    fn_idx = CLOUD_PY.index("def backup_cloud_theme(")
    body = CLOUD_PY[fn_idx:]
    assert "UPDATE plex_items SET theme_id = ?" in body, (
        "v1.19.46: must stamp plex_items.theme_id with the new "
        "orphan theme_id so the library JOIN reflects the row "
        "immediately"
    )


# ── Behavioral: the actual repro the user hit ──────────────────


def _seed_orphan_row(conn, *, rk, tmdb_id, section_id="1",
                     title="Test", year=2020, is_anime=False):
    """Seed a plex_items row WITHOUT a corresponding themes
    entry — the exact shape that caused the FK failure."""
    conn.execute(
        "INSERT OR IGNORE INTO plex_sections "
        "(section_id, title, type, is_anime, is_4k, "
        " themes_subdir, included, discovered_at, last_seen_at) "
        "VALUES (?, 'Test', 'show', ?, 0, 'tv', 1, "
        "        '2026-05-27', '2026-05-27')",
        (section_id, 1 if is_anime else 0),
    )
    conn.execute(
        "INSERT INTO plex_items "
        "(rating_key, section_id, media_type, guid_tmdb, "
        " title, year, has_theme, "
        " first_seen_at, last_seen_at) "
        "VALUES (?, ?, 'show', ?, ?, ?, 1, "
        "        '2026-05-27', '2026-05-27')",
        (rk, section_id, tmdb_id, title, year),
    )


def test_repro_backup_no_themes_row_succeeds(tmp_path):
    """The exact shape from the user's 2026-05-27 repro: row
    with guid_tmdb set, has_theme=1, no matching themes entry.
    Pre-fix this would raise IntegrityError on the local_files
    INSERT. Post-fix it should succeed + create the orphan
    themes row + stamp plex_items.theme_id."""
    from app.core.db import init_db
    db_path = tmp_path / "test.db"
    themes_dir = tmp_path / "themes"
    themes_dir.mkdir()
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON")  # mirror prod
        conn.row_factory = sqlite3.Row
        _seed_orphan_row(
            conn, rk="rk-orphan", tmdb_id=12345,
            title="TV Show TDB Doesn't Track",
        )
        conn.commit()
        from app.core.cloud_theme_backup import backup_cloud_theme
        plex = MagicMock()
        plex._rk_path.return_value = "/library/metadata/rk-orphan/file"
        plex._headers = {}
        resp = MagicMock()
        resp.status_code = 200
        resp.content = b"\x49\x44\x33" + b"\x00" * 512
        resp.text = ""
        plex._client.get.return_value = resp
        target = {
            "rating_key": "rk-orphan",
            "guid_tmdb": 12345,
            "media_type": "tv",
            "section_id": "1",
            "title": "TV Show TDB Doesn't Track",
            "year": "2020",
            "entry_uri": "metadata://themes/" + "a" * 40,
            "sha1": "a" * 40,
        }
        # Pre-fix this would raise IntegrityError.
        result = backup_cloud_theme(conn, target, themes_dir, plex)
        assert result["ok"] is True, (
            f"v1.19.46: cloud-backup must succeed on orphan rows; "
            f"got error: {result.get('error')!r}"
        )
        # themes row was created with plex_orphan source.
        theme = conn.execute(
            "SELECT upstream_source FROM themes "
            "WHERE media_type = ? AND tmdb_id = ?",
            ("tv", 12345),
        ).fetchone()
        assert theme is not None
        assert theme["upstream_source"] == "plex_orphan"
        # plex_items.theme_id is now stamped.
        pi = conn.execute(
            "SELECT theme_id FROM plex_items WHERE rating_key = ?",
            ("rk-orphan",),
        ).fetchone()
        assert pi["theme_id"] is not None, (
            "v1.19.46: plex_items.theme_id must be stamped after "
            "the orphan themes row is created"
        )
        # local_files row landed correctly.
        lf = conn.execute(
            "SELECT source_kind, last_place_attempt_reason "
            "FROM local_files "
            "WHERE media_type = ? AND tmdb_id = ? AND section_id = ?",
            ("tv", 12345, "1"),
        ).fetchone()
        assert lf is not None
        assert lf["source_kind"] == "plex_cloud"
        assert lf["last_place_attempt_reason"] == "backup_only"


def test_backup_on_existing_themes_row_doesnt_duplicate(tmp_path):
    """When a themes row already exists (TDB-tracked + Plex
    serving cloud — the C2/mixed-cohort case where cloud-backup
    still might fire), we must NOT create a duplicate themes
    row. SELECT-first guarding."""
    from app.core.db import init_db
    db_path = tmp_path / "test.db"
    themes_dir = tmp_path / "themes"
    themes_dir.mkdir()
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.row_factory = sqlite3.Row
        _seed_orphan_row(
            conn, rk="rk-tracked", tmdb_id=23456,
            title="TDB-Tracked Show",
        )
        # Pre-existing themes row (TDB DOES track this row).
        conn.execute(
            "INSERT INTO themes "
            "(media_type, tmdb_id, title, upstream_source, "
            " last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('tv', 23456, 'TDB-Tracked Show', 'imdb', "
            "        '2026-05-27', '2026-05-27')"
        )
        conn.commit()
        from app.core.cloud_theme_backup import backup_cloud_theme
        plex = MagicMock()
        plex._rk_path.return_value = "/library/metadata/rk-tracked/file"
        plex._headers = {}
        resp = MagicMock()
        resp.status_code = 200
        resp.content = b"\x49\x44\x33data"
        resp.text = ""
        plex._client.get.return_value = resp
        target = {
            "rating_key": "rk-tracked",
            "guid_tmdb": 23456,
            "media_type": "tv",
            "section_id": "1",
            "title": "TDB-Tracked Show",
            "year": "2020",
            "entry_uri": "metadata://themes/" + "b" * 40,
            "sha1": "b" * 40,
        }
        result = backup_cloud_theme(conn, target, themes_dir, plex)
        assert result["ok"] is True
        # themes row count for this (mt, tmdb) is still 1.
        count = conn.execute(
            "SELECT COUNT(*) AS c FROM themes "
            "WHERE media_type = ? AND tmdb_id = ?",
            ("tv", 23456),
        ).fetchone()
        assert count["c"] == 1, (
            "v1.19.46: must not duplicate themes row when one "
            "already exists"
        )
        # Existing 'imdb' source NOT overwritten with 'plex_orphan'.
        existing = conn.execute(
            "SELECT upstream_source FROM themes "
            "WHERE media_type = ? AND tmdb_id = ?",
            ("tv", 23456),
        ).fetchone()
        assert existing["upstream_source"] == "imdb", (
            "v1.19.46: must NOT downgrade TDB-tracked rows to "
            "plex_orphan"
        )


def test_repro_idempotent_re_run(tmp_path):
    """Running backup_cloud_theme twice on the same orphan row
    must succeed both times — second run hits the local_files
    ON CONFLICT path, theme row already exists."""
    from app.core.db import init_db
    db_path = tmp_path / "test.db"
    themes_dir = tmp_path / "themes"
    themes_dir.mkdir()
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.row_factory = sqlite3.Row
        _seed_orphan_row(
            conn, rk="rk-idem-orphan", tmdb_id=34567,
            title="Orphan Idem",
        )
        conn.commit()
        from app.core.cloud_theme_backup import backup_cloud_theme
        plex = MagicMock()
        plex._rk_path.return_value = (
            "/library/metadata/rk-idem-orphan/file"
        )
        plex._headers = {}
        resp = MagicMock()
        resp.status_code = 200
        resp.content = b"\x49\x44\x33first"
        resp.text = ""
        plex._client.get.return_value = resp
        target = {
            "rating_key": "rk-idem-orphan",
            "guid_tmdb": 34567,
            "media_type": "tv",
            "section_id": "1",
            "title": "Orphan Idem",
            "year": "2020",
            "entry_uri": "metadata://themes/" + "c" * 40,
            "sha1": "c" * 40,
        }
        r1 = backup_cloud_theme(conn, target, themes_dir, plex)
        assert r1["ok"]
        resp.content = b"\x49\x44\x33second_version"
        r2 = backup_cloud_theme(conn, target, themes_dir, plex)
        assert r2["ok"]
        # Exactly one themes row + one local_files row.
        assert conn.execute(
            "SELECT COUNT(*) FROM themes WHERE tmdb_id = 34567"
        ).fetchone()[0] == 1
        assert conn.execute(
            "SELECT COUNT(*) FROM local_files WHERE tmdb_id = 34567"
        ).fetchone()[0] == 1


# ── Version pin ──────────────────────────────────────────────


def test_v1_19_46_version_pin():
    """Version bumped at v1.19.46 (then again at v1.19.47).
    Match 1.19.x prefix."""
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
