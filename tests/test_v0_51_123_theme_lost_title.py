"""v0.51.123 — theme-lost notifications name the actual content, not tv/<tmdb>.

the user, on a Discord screenshot: "💔 Theme lost — tv/4656 … can we make
Theme lost — notifications show the actual name of the contents lost".

Root cause: `enrich_item` derives `display_title` only from the `themes` table.
A lost P-row with no ThemerrDB match has no `themes` row, so the title is empty
and `_safe_display_title` falls back to the bare "media_type/tmdb_id" id.

Fix: the plex_enum lost-theme candidate already carries `cand["title"]` (the
row's `plex_items.title`); pass it to `enrich_item(..., fallback_title=…)`,
which uses it for `display_title` ONLY when the `themes` lookup found no title.
A real themes title still wins. The test-trigger admin endpoint mirrors it.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from app.core.db import init_db
from app.core.notify_content import enrich_item


REPO = Path(__file__).resolve().parent.parent
PLEX_ENUM = (REPO / "app" / "core" / "plex_enum.py").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()


def _db(tmp_path) -> Path:
    db = tmp_path / "motif.db"
    init_db(db)
    return db


# ── enrich_item: fallback_title fills the gap, never overrides ────


def test_fallback_title_used_when_no_themes_row(tmp_path):
    """No `themes` row for the tmdb → display_title is the fallback title,
    NOT the bare 'tv/<tmdb>' id."""
    db = _db(tmp_path)
    ctx = enrich_item(
        db, media_type="tv", tmdb_id=4656,
        fallback_title="Detective Conan",
    )
    assert ctx["display_title"] == "Detective Conan"
    assert ctx["display_title"] != "tv/4656"


def test_real_themes_title_wins_over_fallback(tmp_path):
    """A genuine `themes` title beats the fallback — the fallback only fills
    the gap, it never overrides a known title."""
    db = _db(tmp_path)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO themes (id, media_type, tmdb_id, title, year,"
            " upstream_source, last_seen_sync_at, first_seen_sync_at,"
            " youtube_url) VALUES"
            " (1, 'tv', 4656, 'Real Title', 2001, 'imdb', ?, ?,"
            " 'https://y/watch?v=V')",
            ("2026-07-10T00:00:00", "2026-07-10T00:00:00"),
        )
        conn.commit()
    ctx = enrich_item(
        db, media_type="tv", tmdb_id=4656,
        fallback_title="Wrong Fallback",
    )
    assert "Real Title" in ctx["display_title"]
    assert "Wrong Fallback" not in ctx["display_title"]


def test_no_title_no_fallback_keeps_id_shape(tmp_path):
    """Unchanged behavior when neither a themes title nor a fallback is
    available: the bare id shape so the operator can still grep the row."""
    db = _db(tmp_path)
    ctx = enrich_item(db, media_type="tv", tmdb_id=4656)
    assert ctx["display_title"] == "tv/4656"


def test_fallback_title_keeps_edition_suffix(tmp_path):
    """The edition suffix still appends after the fallback title (the
    v1.21.75 subject-edition contract must survive the fallback path)."""
    db = _db(tmp_path)
    ctx = enrich_item(
        db, media_type="movie", tmdb_id=101,
        fallback_title="Some Movie",
        edition_key="4K",
    )
    assert ctx["display_title"].startswith("Some Movie")
    # edition label appended when edition_key resolves to one.
    assert "Some Movie" in ctx["display_title"]


# ── source pins: both dispatch sites pass the row's plex title ────


def test_plex_enum_dispatch_passes_fallback_title():
    """The live plex_enum lost-theme dispatch must pass the candidate's
    plex_items.title as fallback_title so the subject names the row."""
    anchor = PLEX_ENUM.index("lost_theme_candidates:")
    block = PLEX_ENUM[anchor:]
    dispatch = block[block.index("enrich_item(", block.index("if lost_theme_candidates:")):]
    assert "fallback_title=cand.get(\"title\")" in dispatch, (
        "plex_enum theme-lost dispatch must pass the candidate title"
    )


def test_candidate_carries_plex_title():
    """Guard the source of the fallback: the candidate dict carries the
    row's plex title."""
    assert '"title": cand["title"]' in PLEX_ENUM


def test_test_trigger_endpoint_passes_fallback_title():
    """The admin test-trigger endpoint mirrors production — passes the row's
    plex_items.title as fallback_title."""
    assert 'fallback_title=row["title"] or ""' in API_PY
