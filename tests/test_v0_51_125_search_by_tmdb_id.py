"""v0.51.125 — library search matches a TMDB id.

the user: after the 💔 Theme lost alerts named rows only as "tv/4656" (the
media_type/tmdb_id fallback), "I'm having trouble locating the rows that were
removed". The library search matched title + IMDb id only, so the TMDB id in
the alert was un-searchable.

Fix: _library_main_query now ORs `pi.guid_tmdb = ?` into the search clause for
all-digit queries, so pasting the number from an alert locates the row. Gated
to digits so a text search can't collide with a stray tmdb. Behavioral test
per the v1.18.81 rule (exercise the query builder, not just source text) — a
successful call also proves the COUNT path's param binding stays aligned.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent


@pytest.fixture
def db_search(tmp_path):
    from app.core.db import init_db
    db = tmp_path / "m.db"
    init_db(db)
    now = "2026-01-01T00:00:00Z"
    conn = sqlite3.connect(db)
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, included, "
        "discovered_at, last_seen_at) VALUES ('1','Movies','movie',1,?,?)",
        (now, now))
    # Two titleless (no themes row) plex_items — like the lost P-rows the
    # alert names by tmdb id. guid_tmdb set, theme_id NULL.
    for rk, title, tmdb in [("rkA", "Target Movie", 4656),
                            ("rkB", "Other Movie", 9999)]:
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type, title, "
            "year, guid_tmdb, theme_id, local_theme_file, has_theme, "
            "plex_theme_verified_ok, first_seen_at, last_seen_at) "
            "VALUES (?, '1','movie', ?, '2024', ?, NULL, 0, 0, NULL, ?, ?)",
            (rk, title, tmdb, now, now))
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


def test_numeric_query_matches_guid_tmdb(db_search):
    # the "4656" from "💔 Theme lost — tv/4656" now locates the row.
    assert _titles(_run(db_search, "4656")) == {"Target Movie"}


def test_numeric_query_no_match_is_empty(db_search):
    # a tmdb nobody has → no rows (and the count path doesn't misbind + crash).
    assert _titles(_run(db_search, "12345")) == set()


def test_text_query_still_matches_title(db_search):
    # non-numeric search is unchanged (title LIKE), no guid_tmdb branch.
    assert _titles(_run(db_search, "Other")) == {"Other Movie"}


def test_empty_query_returns_all(db_search):
    assert _titles(_run(db_search, "")) == {"Target Movie", "Other Movie"}


def test_source_pin_guid_tmdb_gated_on_digits():
    api = (REPO / "app" / "web" / "api.py").read_text()
    assert "OR pi.guid_tmdb = ?" in api
    assert "if q.isdigit():" in api
