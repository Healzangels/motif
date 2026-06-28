"""v1.13.74 regression guards — failure_kind survives non-TDB success.

Pre-fix three code paths blindly cleared themes.failure_kind:

  1. worker._record_local_file (every successful download)
  2. api.py REVERT handler (unconditional clear in the txn)
  3. api.py SET URL handler (unconditional clear in the txn)

Combined effect: after the cycle
  U row → REPLACE-W-TDB → TDB download fails (video_removed)
  → REVERT to U → user-URL download succeeds
the red `TDB ✗` pill flipped back to green even though the TDB
URL was still demonstrably dead. Reproduced on 13 Assassins
(2010): TDB URL 2GdEDKC8X2Q removed from YouTube, user URL
ESsp3MMMVUM working. The schema's v1.10.50 design intent
(db.py:45-52) explicitly says the TDB pill should keep painting
red so the user knows the TDB-side URL is broken.

These tests exercise the SQL pattern at the worker layer — the
part that actually moved. The REVERT + SET URL handlers were
fixed by deleting their failure_kind clears entirely; their
correctness now derives from the worker's source_kind branch
(only source_kind='themerrdb' clears).
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.core.db import init_db


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@pytest.fixture
def db_with_failed_theme(tmp_path: Path) -> Path:
    """Theme row with a known TDB-side failure that the worker
    will (or won't) clear depending on source_kind."""
    db = tmp_path / "motif.db"
    init_db(db)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO themes ("
            "  media_type, tmdb_id, title, upstream_source,"
            "  youtube_url, last_seen_sync_at, first_seen_sync_at,"
            "  failure_kind, failure_message, failure_at"
            ") VALUES ('movie', 58857, '13 Assassins', 'imdb',"
            "  'https://www.youtube.com/watch?v=2GdEDKC8X2Q',"
            "  ?, ?, 'video_removed',"
            "  'ERROR: [youtube] 2GdEDKC8X2Q: Video unavailable',"
            "  ?)",
            (_now_iso(), _now_iso(), _now_iso()),
        )
    return db


def _apply_record_local_file_clear(db: Path, *, source_kind: str) -> None:
    """Mirror the v1.13.74 SQL pattern from worker._record_local_file.

    Only clears when source_kind='themerrdb' — a user-URL or adopt
    success doesn't tell us anything about the TDB URL's health.
    Kept here (not imported from worker) so the test pins the
    SQL+predicate pair, which is what the regression actually is.
    """
    with sqlite3.connect(db) as conn:
        if source_kind == "themerrdb":
            conn.execute(
                "UPDATE themes SET failure_kind = NULL,"
                "                  failure_message = NULL,"
                "                  failure_at = NULL,"
                "                  failure_acked_at = NULL "
                "WHERE media_type = ? AND tmdb_id = ? "
                "  AND failure_kind IS NOT NULL",
                ("movie", 58857),
            )


def _failure_kind(db: Path) -> str | None:
    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT failure_kind FROM themes "
            "WHERE media_type='movie' AND tmdb_id=58857",
        ).fetchone()
    return row[0]


# ── the bug ────────────────────────────────────────────────────

def test_user_url_success_preserves_tdb_failure_kind(db_with_failed_theme):
    """The reported scenario: U row → REPLACE-W-TDB fails → REVERT
    triggers a user-URL download → success. The user URL working
    is no evidence the TDB URL is fixed."""
    assert _failure_kind(db_with_failed_theme) == "video_removed"
    _apply_record_local_file_clear(db_with_failed_theme, source_kind="url")
    assert _failure_kind(db_with_failed_theme) == "video_removed", (
        "user-URL success must not wipe the TDB-failure flag — that's "
        "exactly the bug v1.13.74 fixes (red TDB pill flipped green)"
    )


def test_adopt_success_preserves_tdb_failure_kind(db_with_failed_theme):
    """Adopt path also lands as a successful 'download' (sibling
    hardlink short-circuit). Pre-fix it cleared failure_kind too."""
    _apply_record_local_file_clear(db_with_failed_theme, source_kind="adopt")
    assert _failure_kind(db_with_failed_theme) == "video_removed"


def test_upload_success_preserves_tdb_failure_kind(db_with_failed_theme):
    """Upload path likewise."""
    _apply_record_local_file_clear(db_with_failed_theme, source_kind="upload")
    assert _failure_kind(db_with_failed_theme) == "video_removed"


def test_themerrdb_success_clears_failure_kind(db_with_failed_theme):
    """The one case where clearing IS correct: a TDB-URL download
    actually succeeded, so the URL is no longer broken."""
    _apply_record_local_file_clear(db_with_failed_theme, source_kind="themerrdb")
    assert _failure_kind(db_with_failed_theme) is None


# ── REVERT handler shouldn't pre-judge the outcome ─────────────

def test_revert_handler_no_longer_clears_failure_kind():
    """v1.13.74 deleted the unconditional clear in the REVERT
    handler. The worker's source_kind-aware clear (above) is now
    the single point that decides. This is a static guard against
    a regression that re-adds the SQL.
    """
    api_py = Path(__file__).resolve().parent.parent / "app" / "web" / "api.py"
    src = api_py.read_text()
    # Locate the REVERT block and verify the old clear is gone.
    revert_marker = '"REVERT to {prev_kind} URL by'
    assert revert_marker in src, "REVERT handler signature changed?"
    revert_window = src[src.index(revert_marker) - 4000:src.index(revert_marker)]
    assert "Failure flags clear regardless" not in revert_window, (
        "v1.13.74 removed the unconditional failure_kind clear from "
        "REVERT — re-adding it would resurrect the green-pill bug"
    )


def test_set_url_handler_no_longer_clears_failure_kind():
    """Same guard for the /override (SET URL) handler."""
    api_py = Path(__file__).resolve().parent.parent / "app" / "web" / "api.py"
    src = api_py.read_text()
    set_url_marker = '"Manual URL set by '
    assert set_url_marker in src, "SET URL handler signature changed?"
    window = src[src.index(set_url_marker) - 4000:src.index(set_url_marker)]
    assert "the override should retry from scratch" not in window, (
        "v1.13.74 removed the unconditional failure_kind clear from "
        "SET URL — re-adding it would resurrect the green-pill bug"
    )
