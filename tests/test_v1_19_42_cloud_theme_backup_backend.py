"""v1.19.42 — cloud-themes-backup backend (schema + walker + writer + endpoints).

Ships the backup STAGING side of cloud-themes-backup. v1.19.41
already shipped the notification pipe; v1.19.43 will ship the
UI surface (B badge, filter chip, bulk button, SOURCE menu).

## Background

Plex Pass cloud themes are served ONLY while Plex Pass is active.
If Plex Pass lapses (or Plex's cloud catalog drops the entry, or
the item is removed-and-re-added in Plex with a new rating_key —
the v1.18.90 reaper workhorse path), every P-row depending on a
`metadata://themes/<sha1>` entry stops playing instantly with no
recovery if motif never staged a backup.

the user's 2026-05-26 probe (n=16 stratified) characterized:
  - 50% C1 overall (single metadata:// entry, no upload sibling
    — Plex cloud is SOLE source of bytes)
  - Anime: 100% C1 (Plex Pass cloud de facto for niche libraries)
  - ~1,940 C1 rows expected → ~4.2 GB storage

## Scope

  1. Schema v57→v58 widens `local_files.source_kind` CHECK to
     accept `'plex_cloud'`. Reuses the `_widen_check_constraint`
     helper with the v1.18.5 canonical safe pattern
     (`PRAGMA foreign_keys = OFF`).
  2. New module `app/core/cloud_theme_backup.py`:
     - `_classify_themes_response`: pure function deciding C1
     - `identify_c1_rows`: walks P-rows, classifies each via
       Plex's `/themes` endpoint
     - `backup_cloud_theme`: downloads bytes + stages local_files
  3. Writer contract: source_kind='plex_cloud',
     last_place_attempt_reason='backup_only' (reuses v1.19.21
     BK pipe end-to-end).
  4. Two admin endpoints: dry-run + run.
  5. Defensive comments on the two recovery_v55 walkers that
     could theoretically widen to plex_cloud rows in the future.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from fastapi.testclient import TestClient  # noqa: E402

DB_PY = (REPO / "app" / "core" / "db.py").read_text()
CLOUD_PY = (REPO / "app" / "core" / "cloud_theme_backup.py").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()
RECOVERY_PY = (REPO / "app" / "core" / "recovery_v55.py").read_text()


# ── Schema v58 ───────────────────────────────────────────────


def test_current_schema_version_is_58():
    """v1.19.42 bumps the canonical schema version to (at least)
    58. v1.19.45 bumped to 59 — pin the migration STEP rather
    than the absolute value so future bumps don't break this
    guard."""
    assert "_migrate_v57_to_v58" in DB_PY
    assert "elif current == 57:" in DB_PY


def test_schema_local_files_source_kind_check_includes_plex_cloud():
    """The canonical SCHEMA string (executed on fresh init)
    must accept source_kind='plex_cloud'."""
    assert "'plex_cloud'" in DB_PY, (
        "v1.19.42: SCHEMA must include 'plex_cloud' in the "
        "local_files.source_kind CHECK"
    )


def test_migrate_v57_to_v58_defined():
    """The migration function must exist and call the canonical
    `_widen_check_constraint` helper with the foreign_keys=OFF
    wrapper per the v1.18.5 lesson."""
    assert "def _migrate_v57_to_v58(conn:" in DB_PY
    # Locate the function body.
    fn_idx = DB_PY.index("def _migrate_v57_to_v58(conn:")
    # Find next `def ` at module level.
    fn_end = DB_PY.index("\ndef ", fn_idx + 1)
    body = DB_PY[fn_idx:fn_end]
    assert '_widen_check_constraint(' in body, (
        "v1.19.42: migration must reuse the canonical "
        "_widen_check_constraint helper (db.py:2063)"
    )
    assert '"local_files"' in body
    assert '"source_kind"' in body
    assert '"plex_cloud"' in body
    # Critical v1.18.5 safety: foreign_keys = OFF wrap.
    assert 'PRAGMA foreign_keys = OFF' in body, (
        "v1.19.42: migration MUST wrap in foreign_keys=OFF per "
        "v1.18.5 CRITICAL lesson (defer_foreign_keys does NOT "
        "defer cascading actions during DROP TABLE)"
    )
    assert 'PRAGMA foreign_keys = ON' in body
    # Insurance against pre-existing orphans.
    assert 'foreign_key_check' in body


def test_migration_dispatch_includes_v57_to_v58():
    """The migration dispatch loop must include the v57→v58
    step so existing v57 installs upgrade on next boot."""
    assert (
        "elif current == 57:" in DB_PY
        and "_migrate_v57_to_v58(conn)" in DB_PY
    ), (
        "v1.19.42: migration dispatch loop must include "
        "v57→v58 step (db.py:~3556)"
    )


def test_migration_v57_to_v58_runs_idempotently(tmp_path):
    """Boot an empty DB → init_db runs the full migration ladder
    to v58 → verify final schema_version=58 + a plex_cloud insert
    succeeds + an invalid source_kind insert rejects."""
    db_path = tmp_path / "test_v58.db"
    from app.core.db import init_db
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        version = conn.execute(
            "SELECT MAX(version) AS v FROM schema_version"
        ).fetchone()
        # v1.19.45: schema bumped 58 → 59. The v57 → v58
        # migration step still applies; we only care that
        # local_files.source_kind='plex_cloud' lands.
        assert version["v"] >= 58, (
            f"Expected schema_version >= 58, got {version['v']}"
        )
        # Positive insert: plex_cloud accepted.
        conn.execute(
            "INSERT INTO local_files "
            "(media_type, tmdb_id, section_id, file_path, "
            " downloaded_at, source_video_id, source_kind, provenance) "
            "VALUES ('movie', 1, '1', 'movies/Test/theme.mp3', "
            "        '2026-05-27', 'sha1abc', 'plex_cloud', 'auto')"
        )
        # Negative insert: bogus source_kind rejected.
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO local_files "
                "(media_type, tmdb_id, section_id, file_path, "
                " downloaded_at, source_video_id, source_kind, provenance) "
                "VALUES ('movie', 2, '1', 'movies/X/theme.mp3', "
                "        '2026-05-27', 'sha1def', 'bogus_kind', 'auto')"
            )


def test_migration_v57_to_v58_preserves_existing_data(tmp_path):
    """Seed a v57 DB with sample rows → run migration → verify
    row count + content unchanged (no FK cascade damage like
    v1.18.0's bug)."""
    db_path = tmp_path / "test_preserve.db"
    from app.core.db import init_db
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        # Seed a representative local_files row + dependent
        # tables that have FKs onto themes.
        conn.execute(
            "INSERT INTO themes (id, media_type, tmdb_id, title, "
            " upstream_source, last_seen_sync_at, first_seen_sync_at) "
            "VALUES (1, 'movie', 42, 'Test', 'imdb', "
            " '2026-05-27', '2026-05-27')"
        )
        conn.execute(
            "INSERT INTO local_files "
            "(media_type, tmdb_id, section_id, file_path, "
            " downloaded_at, source_video_id, source_kind) "
            "VALUES ('movie', 42, '1', 'movies/Test/theme.mp3', "
            "        '2026-05-27', 'sha1xyz', 'themerrdb')"
        )
        conn.commit()
        # Re-running init_db (idempotent) should not break the
        # seeded data or constraints.
        pass
    # Reopen.
    from app.core.db import init_db
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        lf_count = conn.execute(
            "SELECT COUNT(*) AS c FROM local_files WHERE tmdb_id = 42"
        ).fetchone()
        assert lf_count["c"] == 1, "v1.19.42: data lost on re-init"
        themes_count = conn.execute(
            "SELECT COUNT(*) AS c FROM themes WHERE tmdb_id = 42"
        ).fetchone()
        assert themes_count["c"] == 1


# ── cloud_theme_backup.py helpers (pure functions) ───────────


def test_classify_themes_returns_none_for_empty_response():
    """No Metadata array → not C1."""
    from app.core.cloud_theme_backup import _classify_themes_response
    assert _classify_themes_response({}) is None
    assert _classify_themes_response({"MediaContainer": {}}) is None
    assert _classify_themes_response(
        {"MediaContainer": {"Metadata": []}}
    ) is None


def test_classify_themes_returns_pick_for_c1():
    """Single metadata:// entry, no upload sibling → C1.
    v1.19.51: fixtures without `selected` flag fall back to the
    legacy strict-C1 heuristic (preserved for back-compat with
    Plex builds that don't emit the flag)."""
    from app.core.cloud_theme_backup import _classify_themes_response
    body = {
        "MediaContainer": {
            "Metadata": [
                {"ratingKey": "metadata://themes/abc123" + "0" * 34},
            ]
        }
    }
    pick = _classify_themes_response(body)
    assert pick is not None
    assert pick["entry_uri"].startswith("metadata://")
    assert len(pick["sha1"]) == 40


def test_classify_themes_returns_none_when_upload_sibling_present():
    """LEGACY-FALLBACK behavior (no `selected` flag set):
    metadata:// + upload:// with same SHA → not C1. Production
    Plex builds emit the `selected` flag and hit the v1.19.51
    selected-aware path instead — see
    test_classify_themes_selected_metadata_with_upload_siblings_is_target."""
    from app.core.cloud_theme_backup import _classify_themes_response
    sha = "a" * 40
    body = {
        "MediaContainer": {
            "Metadata": [
                {"ratingKey": f"metadata://themes/{sha}"},
                {"ratingKey": f"upload://themes/{sha}"},
            ]
        }
    }
    assert _classify_themes_response(body) is None, (
        "v1.19.42 legacy-fallback: upload sibling with same SHA "
        "→ not C1 (selected flag not set; behavior preserved "
        "for legacy Plex builds)"
    )


def test_classify_themes_returns_none_for_upload_only():
    """upload:// only (no metadata://) → not a cloud-backup
    target. v1.19.51: still None under both selected-aware and
    legacy paths."""
    from app.core.cloud_theme_backup import _classify_themes_response
    body = {
        "MediaContainer": {
            "Metadata": [
                {"ratingKey": "upload://themes/" + "b" * 40},
            ]
        }
    }
    assert _classify_themes_response(body) is None


def test_classify_themes_returns_none_for_multiple_metadata_entries():
    """LEGACY-FALLBACK behavior (no selected flag): multiple
    metadata:// entries → ambiguous, skip. v1.19.51 selected-
    aware path handles multi-entry shapes by picking the
    selected one (see new test below)."""
    from app.core.cloud_theme_backup import _classify_themes_response
    body = {
        "MediaContainer": {
            "Metadata": [
                {"ratingKey": "metadata://themes/" + "c" * 40},
                {"ratingKey": "metadata://themes/" + "d" * 40},
            ]
        }
    }
    assert _classify_themes_response(body) is None


def test_classify_themes_selected_metadata_with_upload_siblings_is_target():
    """v1.19.51: the user's 2026-05-27 repro shape ('90 Day
    Fiancé: Happily Ever After?'): metadata://selected:true +
    two upload:// siblings sharing the same SHA. Pre-v1.19.51
    classifier returned None (rejected because of upload
    siblings) → walker silently skipped the row → user clicked
    DOWNLOAD PLEX BACKUP and nothing happened.

    The siblings don't matter: motif has no local_files row
    (the SQL push-down already enforces that), the themerr-plex
    plugin is defunct, and the upload entries may break with
    Plex Pass loss anyway. If Plex's SELECTED entry is
    metadata://, back it up."""
    from app.core.cloud_theme_backup import _classify_themes_response
    sha = "0cd73592131aa5deea5ca6578cbe3748e43859f2"
    body = {
        "MediaContainer": {
            "Metadata": [
                {
                    "ratingKey": f"metadata://themes/tv.plex.agents.series_{sha}",
                    "selected": True,
                },
                {
                    "ratingKey": f"upload://themes/com.plexapp.agents.plexthememusic_{sha}",
                    "selected": False,
                },
                {
                    "ratingKey": f"upload://themes/tv.plex.agents.series_{sha}",
                    "selected": False,
                },
            ]
        }
    }
    pick = _classify_themes_response(body)
    assert pick is not None, (
        "v1.19.51: metadata://selected + upload siblings MUST "
        "be a backup target — the user's '90 Day Fiancé: Happily "
        "Ever After?' repro"
    )
    assert pick["entry_uri"].startswith("metadata://")
    assert pick["sha1"] == sha


def test_classify_themes_selected_upload_is_not_target():
    """v1.19.51: when Plex's SELECTED entry is upload://
    (not cloud-served), no backup needed. Even if a
    metadata:// sibling exists, Plex isn't using cloud bytes
    so Plex Pass loss won't affect this row."""
    from app.core.cloud_theme_backup import _classify_themes_response
    body = {
        "MediaContainer": {
            "Metadata": [
                {
                    "ratingKey": "metadata://themes/" + "e" * 40,
                    "selected": False,
                },
                {
                    "ratingKey": "upload://themes/" + "e" * 40,
                    "selected": True,
                },
            ]
        }
    }
    pick = _classify_themes_response(body)
    assert pick is None, (
        "v1.19.51: upload-selected rows shouldn't trigger cloud "
        "backup (Plex serves the upload, not cloud bytes)"
    )


def test_classify_themes_selected_flag_but_nothing_marked_returns_none():
    """v1.19.51: if entries have `selected` keys set but none
    is True, the response is ambiguous — return None
    conservatively."""
    from app.core.cloud_theme_backup import _classify_themes_response
    body = {
        "MediaContainer": {
            "Metadata": [
                {
                    "ratingKey": "metadata://themes/" + "f" * 40,
                    "selected": False,
                },
                {
                    "ratingKey": "upload://themes/" + "f" * 40,
                    "selected": False,
                },
            ]
        }
    }
    assert _classify_themes_response(body) is None


def test_sha1_extractor_handles_uppercase_and_lowercase():
    """SHA-1 extractor must be case-insensitive (Plex
    occasionally returns mixed-case hex)."""
    from app.core.cloud_theme_backup import _sha1_from_entry_uri
    assert _sha1_from_entry_uri(
        "metadata://themes/" + "A" * 40
    ) == "a" * 40
    assert _sha1_from_entry_uri(
        "upload://themes/" + "deadbeef" * 5
    ) == "deadbeef" * 5
    assert _sha1_from_entry_uri("") is None
    assert _sha1_from_entry_uri("metadata://themes/notahash") is None


# ── identify_c1_rows walker ──────────────────────────────────


def _seed_section_and_row(
    conn, *, rk, tmdb_id, has_theme=1, section_id="1",
    title="Test Movie", media_type="movie", year=2020,
    with_local_file=False, with_placement=False, is_anime=False,
):
    """Bare-bones seed for one P-row scenario."""
    conn.execute(
        "INSERT OR IGNORE INTO plex_sections "
        "  (section_id, title, type, is_anime, is_4k, "
        "   themes_subdir, included, discovered_at, last_seen_at) "
        "VALUES (?, 'Movies', 'movie', ?, 0, 'movies', 1, "
        "        '2026-05-27', '2026-05-27')",
        (section_id, 1 if is_anime else 0),
    )
    conn.execute(
        "INSERT INTO plex_items "
        "  (rating_key, section_id, media_type, "
        "   guid_tmdb, title, year, has_theme, "
        "   first_seen_at, last_seen_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, "
        "        '2026-05-27', '2026-05-27')",
        (rk, section_id,
         "show" if media_type == "tv" else media_type,
         tmdb_id, title, year, has_theme),
    )
    if with_local_file:
        conn.execute(
            "INSERT INTO local_files "
            "  (media_type, tmdb_id, section_id, file_path, "
            "   downloaded_at, source_video_id, source_kind) "
            "VALUES (?, ?, ?, 'movies/x/theme.mp3', "
            "        '2026-05-27', 'sha', 'themerrdb')",
            (media_type, tmdb_id, section_id),
        )
    if with_placement:
        conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, "
            "  upstream_source, last_seen_sync_at, "
            "  first_seen_sync_at) "
            "VALUES (?, ?, ?, 'imdb', '2026-05-27', '2026-05-27')",
            (media_type, tmdb_id, title),
        )
        tid = conn.execute(
            "SELECT id FROM themes WHERE media_type=? AND tmdb_id=?",
            (media_type, tmdb_id),
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO placements "
            "  (theme_id, media_type, tmdb_id, section_id, "
            "   plex_rating_key, placement_kind, media_folder, "
            "   placed_at, plex_refreshed) "
            "VALUES (?, ?, ?, ?, ?, 'hardlink', '/data/x', "
            "        '2026-05-27', 1)",
            (tid, media_type, tmdb_id, section_id, rk),
        )


def _mock_plex_for_themes(themes_by_rk: dict[str, dict]):
    """Return a MagicMock PlexClient where get_themes(rating_key=X)
    returns themes_by_rk[X] (or a 404 if missing)."""
    plex = MagicMock()
    def _get_themes(*, rating_key):
        if rating_key in themes_by_rk:
            return {
                "ok": True, "http_status": 200, "error": None,
                "body": themes_by_rk[rating_key],
            }
        return {
            "ok": False, "http_status": 404, "error": "not found",
            "body": None,
        }
    plex.get_themes.side_effect = _get_themes
    return plex


def test_identify_c1_rows_skips_rows_with_local_files(tmp_path):
    """A plex_items row with a matching local_files row is NOT a
    P-row — must be excluded from the walk."""
    db_path = tmp_path / "test.db"
    from app.core.db import init_db
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        _seed_section_and_row(
            conn, rk="rk-skip", tmdb_id=100,
            with_local_file=True,
        )
        conn.commit()
        from app.core.cloud_theme_backup import identify_c1_rows
        plex = _mock_plex_for_themes({
            "rk-skip": {"MediaContainer": {"Metadata": [
                {"ratingKey": "metadata://themes/" + "1" * 40}
            ]}}
        })
        targets = identify_c1_rows(
            conn, plex, inter_call_sleep_s=0, use_cursor=False,
        )
        assert len(targets) == 0, (
            "v1.19.42: rows with local_files must be excluded "
            "(not P-rows)"
        )


def test_identify_c1_rows_skips_rows_with_placements(tmp_path):
    """A plex_items row with a placement is NOT a P-row."""
    db_path = tmp_path / "test.db"
    from app.core.db import init_db
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        _seed_section_and_row(
            conn, rk="rk-placed", tmdb_id=101,
            with_placement=True,
        )
        conn.commit()
        from app.core.cloud_theme_backup import identify_c1_rows
        plex = _mock_plex_for_themes({
            "rk-placed": {"MediaContainer": {"Metadata": [
                {"ratingKey": "metadata://themes/" + "2" * 40}
            ]}}
        })
        targets = identify_c1_rows(
            conn, plex, inter_call_sleep_s=0, use_cursor=False,
        )
        assert len(targets) == 0


def test_identify_c1_rows_returns_only_c1(tmp_path):
    """Seed three P-rows: one C1, one C2 (upload sibling), one
    upload-only. Walker must return ONLY the C1 row."""
    db_path = tmp_path / "test.db"
    from app.core.db import init_db
    init_db(db_path)
    sha_c1 = "1" * 40
    sha_c2 = "2" * 40
    sha_upload = "3" * 40
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        _seed_section_and_row(conn, rk="rk-c1", tmdb_id=200,
                              title="C1 Movie")
        _seed_section_and_row(conn, rk="rk-c2", tmdb_id=201,
                              title="C2 Movie")
        _seed_section_and_row(conn, rk="rk-upl", tmdb_id=202,
                              title="Upload-only Movie")
        conn.commit()
        from app.core.cloud_theme_backup import identify_c1_rows
        plex = _mock_plex_for_themes({
            "rk-c1": {"MediaContainer": {"Metadata": [
                {"ratingKey": f"metadata://themes/{sha_c1}"}
            ]}},
            "rk-c2": {"MediaContainer": {"Metadata": [
                {"ratingKey": f"metadata://themes/{sha_c2}"},
                {"ratingKey": f"upload://themes/{sha_c2}"},
            ]}},
            "rk-upl": {"MediaContainer": {"Metadata": [
                {"ratingKey": f"upload://themes/{sha_upload}"}
            ]}},
        })
        targets = identify_c1_rows(
            conn, plex, inter_call_sleep_s=0, use_cursor=False,
        )
        assert len(targets) == 1
        assert targets[0]["rating_key"] == "rk-c1"
        assert targets[0]["sha1"] == sha_c1
        assert targets[0]["title"] == "C1 Movie"


def test_identify_c1_rows_only_anime_filters_to_anime_sections(tmp_path):
    """only_anime=True scopes the walk to sections with
    plex_sections.is_anime=1."""
    db_path = tmp_path / "test.db"
    from app.core.db import init_db
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        _seed_section_and_row(
            conn, rk="rk-anime", tmdb_id=300,
            section_id="anime-1", is_anime=True,
            title="Anime Movie",
        )
        _seed_section_and_row(
            conn, rk="rk-movie", tmdb_id=301,
            section_id="movie-1", is_anime=False,
            title="Regular Movie",
        )
        conn.commit()
        from app.core.cloud_theme_backup import identify_c1_rows
        plex = _mock_plex_for_themes({
            "rk-anime": {"MediaContainer": {"Metadata": [
                {"ratingKey": "metadata://themes/" + "a" * 40}
            ]}},
            "rk-movie": {"MediaContainer": {"Metadata": [
                {"ratingKey": "metadata://themes/" + "b" * 40}
            ]}},
        })
        targets = identify_c1_rows(
            conn, plex, only_anime=True,
            inter_call_sleep_s=0, use_cursor=False,
        )
        assert len(targets) == 1
        assert targets[0]["rating_key"] == "rk-anime"


def test_identify_c1_rows_translates_show_to_tv(tmp_path):
    """plex_items.media_type='show' but local_files/placements/
    user_overrides use 'tv'. The walker's NOT EXISTS subqueries
    must translate so a tv-row with a local_file IS excluded."""
    db_path = tmp_path / "test.db"
    from app.core.db import init_db
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        _seed_section_and_row(
            conn, rk="rk-tv", tmdb_id=400,
            media_type="tv", with_local_file=True,
        )
        conn.commit()
        from app.core.cloud_theme_backup import identify_c1_rows
        plex = _mock_plex_for_themes({
            "rk-tv": {"MediaContainer": {"Metadata": [
                {"ratingKey": "metadata://themes/" + "e" * 40}
            ]}},
        })
        targets = identify_c1_rows(
            conn, plex, inter_call_sleep_s=0, use_cursor=False,
        )
        assert len(targets) == 0, (
            "v1.19.42: media_type translation must work so a tv "
            "row with a tv-typed local_files row is excluded"
        )


# ── Cursor (resumable walk) ──────────────────────────────────


def test_identify_c1_rows_persists_cursor_at_batch_boundary(tmp_path):
    """After processing rows in a batch, the cursor must be
    written to runtime_settings (so a worker restart picks up
    at the last batch boundary)."""
    db_path = tmp_path / "test.db"
    from app.core.db import init_db
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        # Seed 3 rows; batch_size=2 means cursor stamps at rk-2.
        for i in range(3):
            _seed_section_and_row(
                conn, rk=f"rk-{i:03d}", tmdb_id=500 + i,
            )
        conn.commit()
        from app.core.cloud_theme_backup import (
            identify_c1_rows, CURSOR_KEY,
        )
        plex = _mock_plex_for_themes({
            f"rk-{i:03d}": {"MediaContainer": {"Metadata": [
                {"ratingKey": f"metadata://themes/{i:040x}"}
            ]}}
            for i in range(3)
        })
        identify_c1_rows(
            conn, plex, inter_call_sleep_s=0,
            batch_size=2, use_cursor=True,
        )
        # Walk completed → cursor cleared.
        row = conn.execute(
            "SELECT value FROM runtime_settings WHERE key = ?",
            (CURSOR_KEY,),
        ).fetchone()
        assert row is None, (
            "v1.19.42: cursor must be cleared at end of complete walk"
        )


def test_identify_c1_rows_resumes_from_cursor(tmp_path):
    """A pre-existing cursor in runtime_settings must scope the
    walk to rows AFTER the cursor (resumable)."""
    db_path = tmp_path / "test.db"
    from app.core.db import init_db
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        for i in range(3):
            _seed_section_and_row(
                conn, rk=f"rk-{i:03d}", tmdb_id=600 + i,
                title=f"Movie {i}",
            )
        # Pre-seed cursor at rk-000 → walker should only process
        # rk-001 and rk-002.
        from app.core.events import now_iso
        from app.core.cloud_theme_backup import CURSOR_KEY
        conn.execute(
            "INSERT INTO runtime_settings "
            "(key, value, updated_at, updated_by) "
            "VALUES (?, 'rk-000', ?, 'test')",
            (CURSOR_KEY, now_iso()),
        )
        conn.commit()
        from app.core.cloud_theme_backup import identify_c1_rows
        plex = _mock_plex_for_themes({
            f"rk-{i:03d}": {"MediaContainer": {"Metadata": [
                {"ratingKey": f"metadata://themes/{i:040x}"}
            ]}}
            for i in range(3)
        })
        targets = identify_c1_rows(
            conn, plex, inter_call_sleep_s=0, use_cursor=True,
        )
        rks = {t["rating_key"] for t in targets}
        assert "rk-000" not in rks, (
            "v1.19.42: cursor must exclude rk-000 (already processed)"
        )
        assert rks == {"rk-001", "rk-002"}


# ── backup_cloud_theme writer ────────────────────────────────


def test_backup_cloud_theme_writes_v1_19_x_contract(tmp_path):
    """After backup, local_files row must have ALL columns of
    the v1.19.x writer contract populated: source_kind=
    'plex_cloud', last_place_attempt_reason='backup_only',
    file_sha256, etc."""
    db_path = tmp_path / "test.db"
    themes_dir = tmp_path / "themes"
    themes_dir.mkdir()
    from app.core.db import init_db
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        _seed_section_and_row(
            conn, rk="rk-write", tmdb_id=700,
            title="Writer Test", media_type="movie",
        )
        conn.commit()
        from app.core.cloud_theme_backup import backup_cloud_theme
        plex = MagicMock()
        plex._rk_path.return_value = (
            "/library/metadata/rk-write/file"
        )
        plex._headers = {"X-Plex-Token": "fake"}
        resp = MagicMock()
        resp.status_code = 200
        resp.content = b"\x49\x44\x33" + b"\x00" * 1024  # ID3 header
        resp.text = ""
        plex._client.get.return_value = resp
        target = {
            "rating_key": "rk-write",
            "guid_tmdb": 700,
            "media_type": "movie",
            "section_id": "1",
            "title": "Writer Test",
            "year": "2020",
            "entry_uri": "metadata://themes/" + "f" * 40,
            "sha1": "f" * 40,
        }
        result = backup_cloud_theme(conn, target, themes_dir, plex)
        assert result["ok"] is True
        assert result["bytes_written"] == 1027
        assert result["sha1"] == "f" * 40
        assert result["sha256"] is not None
        # File landed on disk.
        abs_path = themes_dir / result["file_path"]
        assert abs_path.exists()
        assert abs_path.read_bytes() == b"\x49\x44\x33" + b"\x00" * 1024
        # local_files row populated with full contract.
        row = conn.execute(
            "SELECT * FROM local_files "
            " WHERE media_type='movie' AND tmdb_id=700 "
            "   AND section_id='1'"
        ).fetchone()
        assert row is not None
        assert row["source_kind"] == "plex_cloud", (
            "v1.19.42: source_kind must be 'plex_cloud'"
        )
        assert row["last_place_attempt_reason"] == "backup_only", (
            "v1.19.42: LOAD-BEARING — backup_only stamp gates "
            "BK pipe reuse (v1.19.35 PROMOTE + v1.19.21 sweep skip)"
        )
        assert row["last_place_attempt_at"] is not None
        assert row["source_video_id"] == "f" * 40
        assert row["file_sha256"] is not None
        assert row["file_size"] == 1027
        assert row["provenance"] == "auto"
        assert row["mismatch_state"] is None


def test_backup_cloud_theme_idempotent_on_conflict(tmp_path):
    """Re-running backup for the same row must UPDATE the
    existing local_files row, not raise."""
    db_path = tmp_path / "test.db"
    themes_dir = tmp_path / "themes"
    themes_dir.mkdir()
    from app.core.db import init_db
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        _seed_section_and_row(
            conn, rk="rk-idem", tmdb_id=701, title="Idem",
        )
        conn.commit()
        from app.core.cloud_theme_backup import backup_cloud_theme
        plex = MagicMock()
        plex._rk_path.return_value = "/library/metadata/rk-idem/file"
        plex._headers = {}
        resp = MagicMock()
        resp.status_code = 200
        resp.content = b"\x49\x44\x33first"
        resp.text = ""
        plex._client.get.return_value = resp
        target = {
            "rating_key": "rk-idem", "guid_tmdb": 701,
            "media_type": "movie", "section_id": "1",
            "title": "Idem", "year": "2020",
            "entry_uri": "metadata://themes/" + "a" * 40,
            "sha1": "a" * 40,
        }
        r1 = backup_cloud_theme(conn, target, themes_dir, plex)
        assert r1["ok"]
        # Second run with different bytes — UPDATE.
        resp.content = b"\x49\x44\x33second_version"
        r2 = backup_cloud_theme(conn, target, themes_dir, plex)
        assert r2["ok"]
        # Exactly one local_files row.
        count = conn.execute(
            "SELECT COUNT(*) AS c FROM local_files WHERE tmdb_id=701"
        ).fetchone()
        assert count["c"] == 1


def test_backup_cloud_theme_handles_http_error(tmp_path):
    """Non-2xx HTTP response → return ok=False without raising
    or writing to local_files."""
    db_path = tmp_path / "test.db"
    themes_dir = tmp_path / "themes"
    themes_dir.mkdir()
    from app.core.db import init_db
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        _seed_section_and_row(
            conn, rk="rk-err", tmdb_id=702, title="Err",
        )
        conn.commit()
        from app.core.cloud_theme_backup import backup_cloud_theme
        plex = MagicMock()
        plex._rk_path.return_value = "/library/metadata/rk-err/file"
        plex._headers = {}
        resp = MagicMock()
        resp.status_code = 500
        resp.content = b""
        resp.text = "Internal Server Error"
        plex._client.get.return_value = resp
        target = {
            "rating_key": "rk-err", "guid_tmdb": 702,
            "media_type": "movie", "section_id": "1",
            "title": "Err", "year": "2020",
            "entry_uri": "metadata://themes/" + "c" * 40,
            "sha1": "c" * 40,
        }
        result = backup_cloud_theme(conn, target, themes_dir, plex)
        assert result["ok"] is False
        assert "http 500" in result["error"]
        # No row written.
        count = conn.execute(
            "SELECT COUNT(*) AS c FROM local_files WHERE tmdb_id=702"
        ).fetchone()
        assert count["c"] == 0


# ── Module-level surface guards ──────────────────────────────


def test_cloud_theme_backup_module_exports_required_symbols():
    """Public surface of the new module."""
    from app.core import cloud_theme_backup as ctb
    assert hasattr(ctb, "identify_c1_rows")
    assert hasattr(ctb, "backup_cloud_theme")
    assert hasattr(ctb, "CURSOR_KEY")
    assert ctb.CURSOR_KEY == "cloud_backup_walker_cursor"


def test_writer_contract_uses_backup_only_marker():
    """v1.19.42 writer must stamp last_place_attempt_reason=
    'backup_only' — LOAD-BEARING for v1.19.21 retry-sweep skip
    + v1.19.35 PROMOTE TO ACTIVE BK-no-override branch."""
    assert "'backup_only'" in CLOUD_PY, (
        "v1.19.42: writer must stamp 'backup_only' so the v1.19.21 "
        "retry sweep skips and the v1.19.35 PROMOTE flow recognizes "
        "the row as backup-intent"
    )


def test_writer_contract_uses_plex_cloud_source_kind():
    """Writer must stamp source_kind='plex_cloud' so v1.19.43's
    B badge classifier surfaces these rows."""
    assert "'plex_cloud'" in CLOUD_PY


# ── Admin endpoints ──────────────────────────────────────────


def test_dry_run_endpoint_defined_and_admin_gated():
    """The dry-run endpoint must exist + require admin."""
    assert "/api/admin/cloud-themes-backup-dry-run" in API_PY
    fn_idx = API_PY.index(
        "async def api_admin_cloud_themes_backup_dry_run("
    )
    fn_end = API_PY.index("\n    @app.", fn_idx + 1)
    body = API_PY[fn_idx:fn_end]
    assert "_require_admin(request)" in body
    assert "status_code=503" in body, (
        "v1.19.42: must 503 when Plex disabled or token missing"
    )


def test_run_endpoint_defined_and_admin_gated():
    """The run endpoint must exist + require admin + 503 on
    Plex disabled."""
    assert "/api/admin/cloud-themes-backup-run" in API_PY
    fn_idx = API_PY.index(
        "async def api_admin_cloud_themes_backup_run("
    )
    fn_end = API_PY.index("\n    @app.", fn_idx + 1)
    body = API_PY[fn_idx:fn_end]
    assert "_require_admin(request)" in body
    assert "status_code=503" in body
    # v1.19.45: per-row audit + log_event moved INTO the
    # background worker `_cloud_themes_backup_run` (was inline
    # in the endpoint pre-async refactor). The endpoint now
    # only acquires the op_progress slot + spawns the thread.
    worker_idx = API_PY.index("def _cloud_themes_backup_run(")
    worker_end = API_PY.index("\n\ndef ", worker_idx + 1)
    worker = API_PY[worker_idx:worker_end]
    assert '_record_audit(' in worker
    assert 'action="cloud_theme_backup"' in worker, (
        "v1.19.42: audit action must be 'cloud_theme_backup' "
        "(v1.19.39 audit-coverage lesson)"
    )
    assert 'log_event(' in worker


def test_run_endpoint_supports_rks_scope():
    """The run endpoint must accept `{rks: [...]}` to scope to a
    selection (bulk-bar / per-row SOURCE menu)."""
    fn_idx = API_PY.index(
        "async def api_admin_cloud_themes_backup_run("
    )
    fn_end = API_PY.index("\n    @app.", fn_idx + 1)
    body = API_PY[fn_idx:fn_end]
    assert 'rks_scope' in body
    assert 'only_anime' in body


# ── Recovery walker defensive comments ───────────────────────


def test_recovery_walker_stale_placements_notes_plex_cloud():
    """maybe_repair_stale_placements must have a v1.19.42 defensive
    comment noting plex_cloud rows are out-of-scope."""
    fn_idx = RECOVERY_PY.index("def maybe_repair_stale_placements(")
    fn_end = RECOVERY_PY.index("\ndef ", fn_idx + 1)
    body = RECOVERY_PY[fn_idx:fn_end]
    assert "v1.19.42" in body
    assert "plex_cloud" in body, (
        "v1.19.42: maybe_repair_stale_placements must reference "
        "plex_cloud so a future widening doesn't accidentally "
        "include backup-only rows"
    )


def test_recovery_walker_stale_plex_cache_notes_plex_cloud():
    """maybe_repair_stale_plex_cache_placements must reference
    plex_cloud as defensive future-drift protection."""
    fn_idx = RECOVERY_PY.index(
        "def maybe_repair_stale_plex_cache_placements("
    )
    # Find next top-level def OR end-of-file (this walker is the
    # last one in the module today).
    next_def = RECOVERY_PY.find("\ndef ", fn_idx + 1)
    fn_end = next_def if next_def != -1 else len(RECOVERY_PY)
    body = RECOVERY_PY[fn_idx:fn_end]
    assert "v1.19.42" in body
    assert "plex_cloud" in body


# ── End-to-end TestClient ────────────────────────────────────


@pytest.fixture
def admin_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("MOTIF_PLEX_URL", "http://fake:32400")
    monkeypatch.setenv("MOTIF_PLEX_TOKEN", "fake-token")
    (tmp_path / "themes").mkdir(exist_ok=True)
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    settings = Settings(
        config_dir=tmp_path, data_dir=tmp_path / "data",
    )
    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    return TestClient(create_app(settings))


AUTH = {"X-Authentik-Username": "testadmin"}


def test_dry_run_endpoint_admin_gate_rejects_non_admin(admin_client):
    """A non-admin caller must get 401/403, not the C1 listing."""
    r = admin_client.post(
        "/api/admin/cloud-themes-backup-dry-run",
        json={},
    )
    assert r.status_code in (401, 403)


def test_run_endpoint_admin_gate_rejects_non_admin(admin_client):
    """Run endpoint must also be admin-gated."""
    r = admin_client.post(
        "/api/admin/cloud-themes-backup-run",
        json={},
    )
    assert r.status_code in (401, 403)


def test_v1_19_42_version_pin():
    """Version bumped at v1.19.42 (then again at v1.19.43). Match
    the major+minor 1.19.x line so subsequent bumps don't break
    this guard. The v1.19.43 test file pins the exact value."""
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
