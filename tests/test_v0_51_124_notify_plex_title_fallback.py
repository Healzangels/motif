"""v0.51.124 — enrich_item falls back to plex_items.title for titleless rows.

v0.51.123 fixed the "💔 Theme lost —" subject by threading the candidate's
plex title through as `fallback_title`. This broadens it so EVERY notification
for a row with no `themes` entry names the content — most importantly the
"💾 Theme backed up" notification for cloud-backed P-rows, which flows through
enrich_item (worker.py) with no explicit fallback_title.

Fix: when neither the `themes` lookup nor `fallback_title` supplies a title,
enrich_item now looks up `plex_items.title` (+ year) by (guid_tmdb, plex
media_type, section_id) — Plex always knows the row's title. A real `themes`
title still wins; the id-shape fallback only applies when Plex has no row
either (e.g. a synthetic-tmdb row).
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from app.core.db import init_db
from app.core.notify_content import enrich_item


REPO = Path(__file__).resolve().parent.parent
NOTIFY_CONTENT = (REPO / "app" / "core" / "notify_content.py").read_text()
NOW = "2026-07-10T00:00:00"


def _db(tmp_path) -> Path:
    db = tmp_path / "motif.db"
    init_db(db)
    return db


def _plex_item(conn, *, rk, section_id, media_type, guid_tmdb, title, year=None):
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, media_type, guid_tmdb,"
        " title, year, has_theme, plex_independent_theme, first_seen_at,"
        " last_seen_at) VALUES (?, ?, ?, ?, ?, ?, 1, 0, ?, ?)",
        (rk, section_id, media_type, guid_tmdb, title, year, NOW, NOW),
    )


# ── plex_items.title fills the gap when no themes row ─────────


def test_plex_items_title_used_when_no_themes_row(tmp_path):
    """A tv row (plex media_type='show') with no themes entry → the subject
    names the plex title, not 'tv/4656'."""
    db = _db(tmp_path)
    with sqlite3.connect(db) as conn:
        _plex_item(conn, rk="r1", section_id="1", media_type="show",
                   guid_tmdb=4656, title="Detective Conan")
        conn.commit()
    ctx = enrich_item(db, media_type="tv", tmdb_id=4656, section_id="1")
    assert "Detective Conan" in ctx["display_title"]
    assert ctx["display_title"] != "tv/4656"


def test_plex_items_title_movie(tmp_path):
    """movie maps to plex media_type='movie' (no 'show' remap)."""
    db = _db(tmp_path)
    with sqlite3.connect(db) as conn:
        _plex_item(conn, rk="r2", section_id="2", media_type="movie",
                   guid_tmdb=101, title="Some Movie", year="1999")
        conn.commit()
    ctx = enrich_item(db, media_type="movie", tmdb_id=101, section_id="2")
    # year from plex_items is picked up too → "Title (Year)".
    assert ctx["display_title"] == "Some Movie (1999)"


def test_themes_title_wins_over_plex_items(tmp_path):
    """A real themes title beats the plex_items fallback."""
    db = _db(tmp_path)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO themes (id, media_type, tmdb_id, title, year,"
            " upstream_source, last_seen_sync_at, first_seen_sync_at,"
            " youtube_url) VALUES (1,'tv',4656,'TDB Title',2001,'imdb',?,?,"
            " 'https://y/watch?v=V')", (NOW, NOW))
        _plex_item(conn, rk="r3", section_id="1", media_type="show",
                   guid_tmdb=4656, title="Plex Title")
        conn.commit()
    ctx = enrich_item(db, media_type="tv", tmdb_id=4656, section_id="1")
    assert "TDB Title" in ctx["display_title"]
    assert "Plex Title" not in ctx["display_title"]


def test_no_plex_item_no_themes_keeps_id_shape(tmp_path):
    """Unchanged: with neither a themes row nor a plex_items row, the bare
    id shape survives (a synthetic-tmdb row still greps)."""
    db = _db(tmp_path)
    ctx = enrich_item(db, media_type="tv", tmdb_id=4656, section_id="1")
    assert ctx["display_title"] == "tv/4656"


def test_plex_items_matches_without_section_id(tmp_path):
    """When the caller has no section_id, the lookup still finds the title
    (matches any section for that guid_tmdb)."""
    db = _db(tmp_path)
    with sqlite3.connect(db) as conn:
        _plex_item(conn, rk="r4", section_id="7", media_type="show",
                   guid_tmdb=222, title="Sectionless Show")
        conn.commit()
    ctx = enrich_item(db, media_type="tv", tmdb_id=222)
    assert "Sectionless Show" in ctx["display_title"]


def test_explicit_fallback_title_still_wins_when_plex_missing(tmp_path):
    """The v0.51.123 fallback_title still backstops a row that Plex no longer
    has (deleted between enum + dispatch)."""
    db = _db(tmp_path)  # no plex_items, no themes
    ctx = enrich_item(db, media_type="tv", tmdb_id=999,
                      fallback_title="Gone From Plex")
    assert ctx["display_title"] == "Gone From Plex"


# ── source pin ────────────────────────────────────────────────


def test_enrich_item_queries_plex_items_for_title():
    """Guard the fallback query exists + is gated on a still-missing title."""
    assert "if not ctx.get(\"title\"):" in NOTIFY_CONTENT
    # v0.51.126: query gained a `pi` alias + a theme_id-linkage branch.
    assert "SELECT pi.title, pi.year FROM plex_items" in NOTIFY_CONTENT
