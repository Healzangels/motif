"""v1.13.74 — recovery card surfaces REVERT TO USER URL when
a previous_url snapshot of kind='url' exists.

Pre-fix the recovery card on a row that just failed REPLACE-W-TDB
showed SET URL / UPLOAD MP3 / ACK FAILURE — but ignored the user
URL the system had already saved as previous_url. One click to
restore was strictly less work than re-typing the URL via SET URL.

The test pins:
  - the SQL the recovery endpoint uses to load the per-section
    previous_url snapshot
  - the predicate that decides REVERT is offered (prev_url exists
    AND kind='url' AND has a non-empty url)

Static-text guard pins the option's metadata (action/label/tone/
priority) so a future refactor of the recipes table can't quietly
drop or re-tone the button.
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
def db_with_failed_theme_and_user_prev(tmp_path: Path) -> Path:
    """Theme row with a video_removed failure AND a previous_urls
    snapshot of the user URL — the exact post-REPLACE-W-TDB shape
    the REVERT recovery option targets."""
    db = tmp_path / "motif.db"
    init_db(db)
    now = _now_iso()
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO themes ("
            "  media_type, tmdb_id, title, upstream_source,"
            "  youtube_url, last_seen_sync_at, first_seen_sync_at,"
            "  failure_kind, failure_message, failure_at"
            ") VALUES ('movie', 58857, '13 Assassins', 'imdb',"
            "  'https://www.youtube.com/watch?v=2GdEDKC8X2Q',"
            "  ?, ?, 'video_removed', '...removed...', ?)",
            (now, now, now),
        )
        # Per-section previous_url snapshot (v1.12.86 schema).
        conn.execute(
            "INSERT INTO previous_urls ("
            "  media_type, tmdb_id, section_id, youtube_url, kind, captured_at"
            ") VALUES ('movie', 58857, '1',"
            "  'https://www.youtube.com/watch?v=ESsp3MMMVUM', 'user', ?)",
            (now,),
        )
    return db


# ── load + predicate ──────────────────────────────────────────

def _load_and_decide(db: Path, *, section_id: str | None):
    """Mirror the v1.13.74 recovery-options snippet:
    load per-section previous_url, fall back to the '' snapshot,
    then decide if REVERT is offerable."""
    cols = "youtube_url, kind, captured_at"
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        row = None
        if section_id is not None:
            row = conn.execute(
                f"SELECT {cols} FROM previous_urls "
                "WHERE media_type='movie' AND tmdb_id=58857 AND section_id=?",
                (section_id,),
            ).fetchone()
        if row is None:
            row = conn.execute(
                f"SELECT {cols} FROM previous_urls "
                "WHERE media_type='movie' AND tmdb_id=58857 AND section_id=''",
            ).fetchone()
    available = bool(row and row["kind"] == "user" and row["youtube_url"])
    return available, (row["youtube_url"] if available else None)


def test_revert_offered_when_prev_url_is_user(db_with_failed_theme_and_user_prev):
    available, url = _load_and_decide(
        db_with_failed_theme_and_user_prev, section_id="1",
    )
    assert available is True
    assert url == "https://www.youtube.com/watch?v=ESsp3MMMVUM"


def test_revert_not_offered_when_prev_url_is_themerrdb(tmp_path):
    """Reverting to a TDB URL doesn't help — we'd just re-attempt
    the failed download. Suppression matches the existing UI text
    'revert: unavailable — the previous URL was a ThemerrDB URL'."""
    db = tmp_path / "motif.db"
    init_db(db)
    now = _now_iso()
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO themes ("
            "  media_type, tmdb_id, title, upstream_source,"
            "  youtube_url, last_seen_sync_at, first_seen_sync_at"
            ") VALUES ('movie', 58857, '13 Assassins', 'imdb',"
            "  'https://www.youtube.com/watch?v=2GdEDKC8X2Q', ?, ?)",
            (now, now),
        )
        conn.execute(
            "INSERT INTO previous_urls ("
            "  media_type, tmdb_id, section_id, youtube_url, kind, captured_at"
            ") VALUES ('movie', 58857, '1',"
            "  'https://www.youtube.com/watch?v=2GdEDKC8X2Q', 'themerrdb', ?)",
            (now,),
        )
    available, _ = _load_and_decide(db, section_id="1")
    assert available is False


def test_revert_not_offered_when_no_prev_url(tmp_path):
    db = tmp_path / "motif.db"
    init_db(db)
    now = _now_iso()
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO themes ("
            "  media_type, tmdb_id, title, upstream_source,"
            "  youtube_url, last_seen_sync_at, first_seen_sync_at"
            ") VALUES ('movie', 58857, '13 Assassins', 'imdb',"
            "  'https://www.youtube.com/watch?v=foo', ?, ?)",
            (now, now),
        )
    available, _ = _load_and_decide(db, section_id="1")
    assert available is False


def test_revert_falls_back_to_empty_section_snapshot(tmp_path):
    """When no per-section row exists, the '' (global) snapshot
    counts. Mirrors _load_previous_url's fallback chain."""
    db = tmp_path / "motif.db"
    init_db(db)
    now = _now_iso()
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO themes ("
            "  media_type, tmdb_id, title, upstream_source,"
            "  youtube_url, last_seen_sync_at, first_seen_sync_at"
            ") VALUES ('movie', 58857, '13 Assassins', 'imdb',"
            "  'https://www.youtube.com/watch?v=foo', ?, ?)",
            (now, now),
        )
        conn.execute(
            "INSERT INTO previous_urls ("
            "  media_type, tmdb_id, section_id, youtube_url, kind, captured_at"
            ") VALUES ('movie', 58857, '',"
            "  'https://www.youtube.com/watch?v=ESsp3MMMVUM', 'user', ?)",
            (now,),
        )
    available, url = _load_and_decide(db, section_id="1")
    assert available is True
    assert "ESsp3MMMVUM" in url


# ── option metadata pin ───────────────────────────────────────

def test_revert_option_metadata_in_recovery_recipes():
    """Pin the v1.13.74 option's action/label/tone/priority so a
    future refactor of the recipes block can't silently change
    its rendering.

    v1.14.40 added a SECOND REVERT TO USER URL option in the
    LPS-state branch (no-fail branch, priority 1) — this test
    pins the v1.13.74 ORIGINAL in the failure-recipes block,
    which lives further down the file. We anchor on the recipes
    dict marker so we always read the right one."""
    api_py = Path(__file__).resolve().parent.parent / "app" / "web" / "api.py"
    src = api_py.read_text()
    assert '"action": "revert"' in src
    assert '"label": "REVERT TO USER URL"' in src
    # tone='user' so the recovery card paints it in the U-source
    # palette (matches the SRC pill the action restores).
    assert '"tone": "user"' in src
    # priority 0 ranks ahead of SET URL / UPLOAD MP3 / smart-TRY-NEXT
    # extras so the cheapest recovery shows first. Anchor on the
    # recipes dict so we read the v1.13.74 entry, not the v1.14.40
    # LPS-branch entry that lives in the no-fail branch above.
    recipes_anchor = src.index('recipes: dict[str, list[dict]] = {')
    revert_in_recipes = src.index(
        '"label": "REVERT TO USER URL"', recipes_anchor,
    )
    block = src[revert_in_recipes - 200:revert_in_recipes + 400]
    assert '"priority": 0' in block
