"""v0.51.126 — locate/title theme-linked rows by the theme's tmdb id.

A 💔 Theme lost alert names a row by COALESCE(themes.tmdb_id, plex_items.guid_tmdb)
(plex_enum.py:2427). For a theme-linked row whose plex_items.guid_tmdb is NULL
(anime / non-TMDB agents), that number is the THEMES tmdb_id — so the v0.51.125
search and the v0.51.124 title fallback, which keyed on guid_tmdb only, both
missed it (the user searched 31991 across every section, no results).

Fix: both the library search clause and enrich_item's plex_items title fallback
now ALSO match via pi.theme_id → themes.tmdb_id, so a guid_tmdb-NULL theme-linked
row is reachable by its theme tmdb id.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from app.core.notify_content import enrich_item

REPO = Path(__file__).resolve().parent.parent
NOW = "2026-01-01T00:00:00Z"


@pytest.fixture
def db_linked(tmp_path):
    from app.core.db import init_db
    db = tmp_path / "m.db"
    init_db(db)
    conn = sqlite3.connect(db)
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, included, "
        "discovered_at, last_seen_at) VALUES ('1','Movies','movie',1,?,?)",
        (NOW, NOW))
    # themes row with an EMPTY title (orphan/synthetic) so enrich_item step 1
    # finds no usable title — forcing the plex_items fallback path.
    conn.execute(
        "INSERT INTO themes (id, media_type, tmdb_id, title, year, "
        "upstream_source, last_seen_sync_at, first_seen_sync_at, youtube_url) "
        "VALUES (7, 'movie', 31991, '', NULL, 'plex_orphan', ?, ?, '')",
        (NOW, NOW))
    # plex_items row: guid_tmdb NULL, linked via theme_id → themes(31991).
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, media_type, title, "
        "year, guid_tmdb, theme_id, local_theme_file, has_theme, "
        "plex_theme_verified_ok, first_seen_at, last_seen_at) "
        "VALUES ('rkX', '1','movie', 'Linked Movie', '2024', NULL, 7, 0, 0, "
        "NULL, ?, ?)", (NOW, NOW))
    conn.commit()
    conn.close()
    return db


def _run(db, q):
    from app.web.api import _library_main_query
    return _library_main_query(
        db, tab="movies", fourk=False, q=q, status="all", page=1,
        per_page=200, sort="title", sort_dir="asc", tdb="any")


def _titles(res):
    return {r["plex_title"] for r in res["items"]}


def test_search_finds_theme_linked_row_by_theme_tmdb(db_linked):
    # guid_tmdb is NULL; the row is reachable via theme_id → themes.tmdb_id.
    assert _titles(_run(db_linked, "31991")) == {"Linked Movie"}


def test_search_miss_still_empty(db_linked):
    assert _titles(_run(db_linked, "555")) == set()


def test_enrich_item_uses_plex_title_via_theme_link(db_linked):
    # themes title is empty → the plex_items title is reached via the theme
    # linkage (guid_tmdb NULL), not a bare guid_tmdb match.
    ctx = enrich_item(db_linked, media_type="movie", tmdb_id=31991,
                      section_id="1")
    assert "Linked Movie" in ctx["display_title"]
    assert ctx["display_title"] != "movie/31991"


def test_source_pins_theme_linkage_match():
    api = (REPO / "app" / "web" / "api.py").read_text()
    nc = (REPO / "app" / "core" / "notify_content.py").read_text()
    assert "pi.theme_id IN" in api
    assert "pi.theme_id IN" in nc
