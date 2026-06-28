"""v1.13.61: regression guard for the v42 stale-urls_match migration.

Pins the cleanup contract:
  - urls_match pending_updates with NO canonical (no local_files row
    or local_files.file_path IS NULL) → DELETED.
  - urls_match pending_updates WITH a canonical (local_files row
    exists with non-null file_path for the same section) → kept.
  - upstream_changed pending_updates → never touched (different kind).
  - Migration is idempotent (running twice doesn't re-delete or
    re-insert anything).

This complements the v1.13.55 canonical-gate (in api_manual_url)
that prevents NEW stale rows. The migration is for instances that
upgraded into v1.13.61 carrying pre-fix junk.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.core.db import CURRENT_SCHEMA_VERSION, _migrate_v41_to_v42, init_db


@pytest.fixture
def fresh_db(tmp_path: Path) -> Path:
    db = tmp_path / "test.db"
    init_db(db)
    return db


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _seed_section(conn, section_id: str, *, media_type: str = "movie"):
    try:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, "
            "                           is_4k, is_anime, included, "
            "                           discovered_at, last_seen_at) "
            "VALUES (?, ?, ?, 0, 0, 1, ?, ?)",
            (section_id, f"Sec{section_id}",
             "show" if media_type == "tv" else "movie",
             _now(), _now()),
        )
    except sqlite3.IntegrityError:
        pass


def _seed_theme(conn, *, tmdb_id: int, media_type: str = "movie"):
    conn.execute(
        "INSERT INTO themes (media_type, tmdb_id, title, youtube_url, "
        "                    youtube_video_id, upstream_source, "
        "                    last_seen_sync_at, first_seen_sync_at) "
        "VALUES (?, ?, 'Test', 'https://yt/X', 'X', 'imdb', ?, ?)",
        (media_type, tmdb_id, _now(), _now()),
    )


def _seed_canonical(conn, *, tmdb_id: int, section_id: str,
                    file_path: str = "/themes/test.mp3",
                    media_type: str = "movie"):
    _seed_section(conn, section_id, media_type=media_type)
    conn.execute(
        "INSERT INTO local_files (media_type, tmdb_id, section_id, "
        "                         file_path, file_size, file_sha256, "
        "                         downloaded_at, source_video_id, "
        "                         source_kind, provenance) "
        "VALUES (?, ?, ?, ?, 1024, 'sha', ?, 'X', 'themerrdb', 'auto')",
        (media_type, tmdb_id, section_id, file_path, _now()),
    )


def _seed_pending(conn, *, tmdb_id: int, section_id: str,
                  kind: str = "urls_match", media_type: str = "movie"):
    conn.execute(
        "INSERT INTO pending_updates (media_type, tmdb_id, section_id, "
        "  old_video_id, new_video_id, "
        "  old_youtube_url, new_youtube_url, upstream_edited_at, "
        "  detected_at, decision, kind) "
        "VALUES (?, ?, ?, 'OLD', 'NEW', "
        "        'https://yt/OLD', 'https://yt/NEW', ?, ?, "
        "        'pending', ?)",
        (media_type, tmdb_id, section_id, _now(), _now(), kind),
    )


# ── core contract ──────────────────────────────────────────────────

def test_no_canonical_urls_match_deleted(fresh_db: Path):
    with sqlite3.connect(fresh_db) as conn:
        conn.row_factory = sqlite3.Row
        _seed_section(conn, "1")
        _seed_theme(conn, tmdb_id=100)
        _seed_pending(conn, tmdb_id=100, section_id="1")
        # No canonical for tmdb_id=100, section_id=1
        before = conn.execute(
            "SELECT COUNT(*) FROM pending_updates WHERE tmdb_id = 100"
        ).fetchone()[0]
        assert before == 1
        _migrate_v41_to_v42(conn)
        after = conn.execute(
            "SELECT COUNT(*) FROM pending_updates WHERE tmdb_id = 100"
        ).fetchone()[0]
    assert after == 0


def test_with_canonical_urls_match_preserved(fresh_db: Path):
    """If a canonical exists for the section, the urls_match prompt
    is legitimate (real U → T conversion path). Migration must
    leave it alone."""
    with sqlite3.connect(fresh_db) as conn:
        conn.row_factory = sqlite3.Row
        _seed_theme(conn, tmdb_id=200)
        _seed_canonical(conn, tmdb_id=200, section_id="1")
        _seed_pending(conn, tmdb_id=200, section_id="1")
        _migrate_v41_to_v42(conn)
        rows = conn.execute(
            "SELECT * FROM pending_updates WHERE tmdb_id = 200"
        ).fetchall()
    assert len(rows) == 1


def test_upstream_changed_kind_never_touched(fresh_db: Path):
    """Migration only targets kind='urls_match'. A genuine
    upstream_changed pending_update on a no-canonical row is real
    TDB drift and must survive."""
    with sqlite3.connect(fresh_db) as conn:
        conn.row_factory = sqlite3.Row
        _seed_section(conn, "1")
        _seed_theme(conn, tmdb_id=300)
        _seed_pending(conn, tmdb_id=300, section_id="1",
                      kind="upstream_changed")
        # No canonical
        _migrate_v41_to_v42(conn)
        rows = conn.execute(
            "SELECT * FROM pending_updates WHERE tmdb_id = 300"
        ).fetchall()
    assert len(rows) == 1
    assert rows[0]["kind"] == "upstream_changed"


def test_section_scoped_canonical_check(fresh_db: Path):
    """Canonical in section 1 doesn't satisfy a urls_match in
    section 2. Per-section scoping keeps multi-section titles
    correct."""
    with sqlite3.connect(fresh_db) as conn:
        conn.row_factory = sqlite3.Row
        _seed_theme(conn, tmdb_id=400)
        _seed_canonical(conn, tmdb_id=400, section_id="1")
        _seed_section(conn, "2")
        # urls_match on section 2 (no canonical there)
        _seed_pending(conn, tmdb_id=400, section_id="2")
        _migrate_v41_to_v42(conn)
        rows = conn.execute(
            "SELECT section_id FROM pending_updates WHERE tmdb_id = 400"
        ).fetchall()
    assert rows == []


def test_migration_idempotent(fresh_db: Path):
    """Running v42 twice is a no-op the second time."""
    with sqlite3.connect(fresh_db) as conn:
        conn.row_factory = sqlite3.Row
        _seed_section(conn, "1")
        _seed_theme(conn, tmdb_id=500)
        _seed_pending(conn, tmdb_id=500, section_id="1")
        _migrate_v41_to_v42(conn)
        first = conn.execute(
            "SELECT COUNT(*) FROM pending_updates"
        ).fetchone()[0]
        _migrate_v41_to_v42(conn)
        second = conn.execute(
            "SELECT COUNT(*) FROM pending_updates"
        ).fetchone()[0]
    assert first == 0
    assert second == 0


def test_migration_runs_on_fresh_init(fresh_db: Path):
    """init_db on a virgin DB should reach the current schema version
    cleanly with no cleanup work to do (pending_updates starts empty).

    v1.13.75: pinned `>= 42` so future schema bumps don't break this
    guard; the v42 cleanup itself still runs as part of the chain.
    """
    with sqlite3.connect(fresh_db) as conn:
        v = conn.execute(
            "SELECT MAX(version) FROM schema_version"
        ).fetchone()[0]
    assert v == CURRENT_SCHEMA_VERSION
    assert CURRENT_SCHEMA_VERSION >= 42


def test_mixed_population(fresh_db: Path):
    """End-to-end: seed a mixed population (one stale, one
    legitimate, one upstream_changed) and verify only the stale
    urls_match drops."""
    with sqlite3.connect(fresh_db) as conn:
        conn.row_factory = sqlite3.Row
        _seed_theme(conn, tmdb_id=601)
        _seed_pending(conn, tmdb_id=601, section_id="1",
                      kind="urls_match")  # stale → DELETE
        _seed_theme(conn, tmdb_id=602)
        _seed_canonical(conn, tmdb_id=602, section_id="1")
        _seed_pending(conn, tmdb_id=602, section_id="1",
                      kind="urls_match")  # legit → keep
        _seed_theme(conn, tmdb_id=603)
        _seed_pending(conn, tmdb_id=603, section_id="1",
                      kind="upstream_changed")  # different kind → keep
        _migrate_v41_to_v42(conn)
        rows = conn.execute(
            "SELECT tmdb_id, kind FROM pending_updates ORDER BY tmdb_id"
        ).fetchall()
    assert [(r["tmdb_id"], r["kind"]) for r in rows] == [
        (602, "urls_match"),
        (603, "upstream_changed"),
    ]
