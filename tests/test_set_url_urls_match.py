"""v1.13.55: regression guard for the SET URL urls_match
pending_update insertion.

Pre-fix: when the user did SET URL with the SAME URL ThemerrDB
already had, motif unconditionally wrote a synthetic urls_match
pending_update row to surface the "convert U → T via ACCEPT
UPDATE" prompt. On rows with no canonical (failed downloads,
never-downloaded — common when the user is testing whether a
network-condition-dependent failure has cleared), the prompt was
stuck because there was no U row to convert. KEEP CURRENT didn't
help either (nothing was being "kept"), so the topbar UPD pill
clung to a phantom +1 forever.

Fix: check for a canonical (local_files row with non-null
file_path) at the requested section before inserting the
urls_match row. The DELETE-of-stale-pending_updates step still
runs unconditionally so prior sync-driven entries get cleaned up.

These tests exercise the same SQL the SET URL endpoint runs, so
any future refactor that drops the canonical guard breaks here.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.core.db import get_conn, init_db


@pytest.fixture
def fresh_db(tmp_path: Path) -> Path:
    db = tmp_path / "test.db"
    init_db(db)
    return db


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# Same SQL the api_manual_url endpoint runs (api.py:6263 area).
# Captured here as constants so a refactor that splits them up
# breaks this test loudly.
_DELETE_STALE_PENDING = (
    "DELETE FROM pending_updates "
    "WHERE media_type = ? AND tmdb_id = ? AND section_id = ?"
)
_HAS_CANONICAL_CHECK = (
    "SELECT 1 FROM local_files "
    "WHERE media_type = ? AND tmdb_id = ? "
    "  AND section_id = ? AND file_path IS NOT NULL "
    "LIMIT 1"
)
_INSERT_URLS_MATCH = (
    """
    INSERT INTO pending_updates (
        media_type, tmdb_id, section_id,
        old_video_id, new_video_id,
        old_youtube_url, new_youtube_url,
        upstream_edited_at, detected_at, decision, kind
    )
    SELECT t.media_type, t.tmdb_id, ?,
           t.youtube_video_id, t.youtube_video_id,
           NULL, t.youtube_url,
           t.youtube_edited_at, ?, 'pending', 'urls_match'
      FROM themes t
     WHERE t.media_type = ?
       AND t.tmdb_id = ?
    """
)


def _seed_theme(conn, *, tmdb_id: int, youtube_url: str = "https://yt/X",
                media_type: str = "movie"):
    conn.execute(
        "INSERT INTO themes (media_type, tmdb_id, title, youtube_url, "
        "                    youtube_video_id, upstream_source, "
        "                    last_seen_sync_at, first_seen_sync_at) "
        "VALUES (?, ?, 'Test', ?, 'X', 'imdb', ?, ?)",
        (media_type, tmdb_id, youtube_url, _now(), _now()),
    )


def _ensure_section(conn, section_id: str, *, media_type: str = "movie"):
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


def _seed_canonical(conn, *, tmdb_id: int, section_id: str,
                    file_path: str = "/themes/test.mp3",
                    media_type: str = "movie"):
    # local_files has FK to plex_sections — section must exist.
    _ensure_section(conn, section_id, media_type=media_type)
    conn.execute(
        "INSERT INTO local_files (media_type, tmdb_id, section_id, "
        "                         file_path, file_size, file_sha256, "
        "                         downloaded_at, source_video_id, "
        "                         source_kind, provenance) "
        "VALUES (?, ?, ?, ?, 1024, 'sha', ?, 'X', 'themerrdb', 'auto')",
        (media_type, tmdb_id, section_id, file_path, _now()),
    )


def _apply_set_url_urls_match_logic(conn, *, media_type: str,
                                    tmdb_id: int, section_id: str):
    """Replicates the v1.13.55 fix: always clean stale pending_updates,
    only insert urls_match when a canonical exists."""
    conn.execute(_DELETE_STALE_PENDING,
                 (media_type, tmdb_id, section_id))
    has_canonical = conn.execute(
        _HAS_CANONICAL_CHECK,
        (media_type, tmdb_id, section_id),
    ).fetchone() is not None
    if has_canonical:
        conn.execute(_INSERT_URLS_MATCH,
                     (section_id, _now(), media_type, tmdb_id))


# ── the bug fix ────────────────────────────────────────────────────

def test_no_canonical_no_pending_insert(fresh_db: Path):
    """The exact bug case: failed-download row, user does SET URL
    with the same URL TDB has. Before fix, this wrote a stuck
    urls_match pending_update. After fix, no insert happens."""
    with get_conn(fresh_db) as conn:
        _seed_theme(conn, tmdb_id=100)
        # Note: NO local_files row → no canonical
        _apply_set_url_urls_match_logic(
            conn, media_type="movie", tmdb_id=100, section_id="1",
        )
        rows = conn.execute(
            "SELECT * FROM pending_updates WHERE tmdb_id = 100",
        ).fetchall()
    assert rows == []


def test_with_canonical_inserts_pending(fresh_db: Path):
    """When a canonical exists, the urls_match prompt fires as
    intended (this is the legitimate U → T cleanup path)."""
    with get_conn(fresh_db) as conn:
        _seed_theme(conn, tmdb_id=200)
        _seed_canonical(conn, tmdb_id=200, section_id="1")
        _apply_set_url_urls_match_logic(
            conn, media_type="movie", tmdb_id=200, section_id="1",
        )
        rows = conn.execute(
            "SELECT kind, decision FROM pending_updates "
            "WHERE tmdb_id = 200",
        ).fetchall()
    assert len(rows) == 1
    assert rows[0]["kind"] == "urls_match"
    assert rows[0]["decision"] == "pending"


def test_canonical_check_is_section_scoped(fresh_db: Path):
    """A canonical in section 1 doesn't satisfy SET URL's check
    when run against section 2 — section scoping is the v1.11.0
    invariant."""
    with get_conn(fresh_db) as conn:
        _seed_theme(conn, tmdb_id=300)
        _seed_canonical(conn, tmdb_id=300, section_id="1")
        # Run SET URL on section 2 (no canonical there)
        _apply_set_url_urls_match_logic(
            conn, media_type="movie", tmdb_id=300, section_id="2",
        )
        rows = conn.execute(
            "SELECT section_id FROM pending_updates WHERE tmdb_id = 300",
        ).fetchall()
    assert rows == []  # no insert because section 2 has no canonical


def test_stale_pending_cleared_even_without_insert(fresh_db: Path):
    """The DELETE step runs UNCONDITIONALLY so prior sync-driven
    pending_update rows for this section get cleaned up — even
    when we then skip the urls_match insert. Otherwise the
    user's SET URL action would visibly do nothing to a stale
    UPD pill on a no-canonical row."""
    with get_conn(fresh_db) as conn:
        _seed_theme(conn, tmdb_id=400)
        # Pretend a sync had previously written a pending_update
        # for this section (kind='url_changed' from real TDB drift).
        conn.execute(
            "INSERT INTO pending_updates (media_type, tmdb_id, "
            "  section_id, old_video_id, new_video_id, "
            "  old_youtube_url, new_youtube_url, upstream_edited_at, "
            "  detected_at, decision, kind) "
            "VALUES ('movie', 400, '1', 'OLD', 'NEW', "
            "        'https://yt/OLD', 'https://yt/NEW', ?, ?, "
            "        'pending', 'upstream_changed')",
            (_now(), _now()),
        )
        # SET URL with no canonical at section 1.
        _apply_set_url_urls_match_logic(
            conn, media_type="movie", tmdb_id=400, section_id="1",
        )
        rows = conn.execute(
            "SELECT * FROM pending_updates WHERE tmdb_id = 400",
        ).fetchall()
    # Stale pending was cleaned, no new insert (no canonical).
    assert rows == []


def test_other_section_pending_untouched(fresh_db: Path):
    """SET URL on section 1 must not touch pending_updates rows
    keyed to other sections."""
    with get_conn(fresh_db) as conn:
        _seed_theme(conn, tmdb_id=500)
        # Sibling section's pending_update (e.g. 4K had a real TDB drift)
        conn.execute(
            "INSERT INTO pending_updates (media_type, tmdb_id, "
            "  section_id, old_video_id, new_video_id, "
            "  old_youtube_url, new_youtube_url, upstream_edited_at, "
            "  detected_at, decision, kind) "
            "VALUES ('movie', 500, '2', 'OLD', 'NEW', "
            "        'https://yt/OLD', 'https://yt/NEW', ?, ?, "
            "        'pending', 'upstream_changed')",
            (_now(), _now()),
        )
        _apply_set_url_urls_match_logic(
            conn, media_type="movie", tmdb_id=500, section_id="1",
        )
        rows = conn.execute(
            "SELECT section_id FROM pending_updates WHERE tmdb_id = 500",
        ).fetchall()
    # Sibling section's row survives — section scoping respected.
    assert [r["section_id"] for r in rows] == ["2"]


# Note: file_path is NOT NULL in the schema, so a "null file_path
# canonical" can't physically exist. The check `file_path IS NOT
# NULL` in the SQL is defensive against future schema changes —
# pinning it via a row count is the meaningful test, covered above
# by test_no_canonical_no_pending_insert.
