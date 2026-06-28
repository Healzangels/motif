"""v1.24.75 — TDB ∅ filter chip surfaces tracked-but-themeless rows.

the user: even though only ~6 rows are in this state, add a filter chip so EVERY
TDB pill state is filterable. The new `empty` bucket = upstream_source set +
youtube_url empty + not dropped/failed/pending (mirrors the v1.24.73 TDB ∅ row
pill + computeTdbPill). The green `tdb` filter is tightened to REQUIRE a
non-empty youtube_url so green and empty are disjoint.

Behavioral test exercises _library_main_query (per the v1.18.81 rule: test the
endpoint, not just the source text). Mirror-check pins every layer that must
accept the new token (server _pset, JS classifier + ALL set + URL parser,
template chip) — same shape as test_v1_14_23 for 'dropped'.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()
LIBRARY_HTML = (REPO / "app" / "web" / "templates" / "library.html").read_text()


# ── behavioral: the filter partitions correctly ─────────────────────────────


@pytest.fixture
def db3(tmp_path):
    from app.core.db import init_db
    db = tmp_path / "m.db"
    init_db(db)
    now = "2026-01-01T00:00:00Z"
    conn = sqlite3.connect(db)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, included, "
        "discovered_at, last_seen_at) VALUES ('1','Movies','movie',1,?,?)",
        (now, now))
    # (tmdb, title, upstream_source, youtube_url)
    rows = [
        (201, "EMPTY", "themoviedb", None),       # ∅ — tracked, no theme video
        (202, "GREEN", "imdb", "https://yt/x"),   # healthy TDB
        (203, "ORPHAN", "plex_orphan", None),     # no TDB
    ]
    for tmdb, title, upstream, yurl in rows:
        conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, upstream_source, "
            "last_seen_sync_at, first_seen_sync_at, youtube_url) "
            "VALUES ('movie', ?, ?, ?, ?, ?, ?)",
            (tmdb, title, upstream, now, now, yurl))
        tid = conn.execute(
            "SELECT id FROM themes WHERE tmdb_id=?", (tmdb,)).fetchone()[0]
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type, title, "
            "year, guid_tmdb, theme_id, local_theme_file, has_theme, "
            "plex_theme_verified_ok, first_seen_at, last_seen_at) "
            "VALUES (?, '1','movie', ?, '2024', ?, ?, 0, 0, NULL, ?, ?)",
            (f"rk{tmdb}", title, tmdb, tid, now, now))
    conn.commit()
    conn.close()
    return db


def _run(db, **kw):
    from app.web.api import _library_main_query
    return _library_main_query(
        db, tab="movies", fourk=False, q="", status="all", page=1,
        per_page=200, sort="title", sort_dir="asc", tdb="any", **kw)


def _titles(res):
    return {r["plex_title"] for r in res["items"]}


def test_empty_filter_returns_only_the_themeless_row(db3):
    assert _titles(_run(db3, tdb_pills={"empty"})) == {"EMPTY"}


def test_green_tdb_filter_excludes_themeless(db3):
    # v1.24.75: green now requires a non-empty youtube_url — EMPTY drops out.
    assert _titles(_run(db3, tdb_pills={"tdb"})) == {"GREEN"}


def test_none_filter_unaffected_by_empty(db3):
    assert _titles(_run(db3, tdb_pills={"none"})) == {"ORPHAN"}


def test_axis_is_a_clean_partition(db3):
    # empty ⊎ tdb ⊎ none covers all three rows, no overlap.
    assert _titles(_run(db3, tdb_pills={"empty", "tdb", "none"})) == {
        "EMPTY", "GREEN", "ORPHAN"}


# ── mirror-check: every layer accepts the 'empty' token ─────────────────────


def test_server_pset_accepts_empty():
    assert '"empty"' in API_PY
    # tightened green branch requires a url; new empty branch requires none.
    assert "NULLIF(t.youtube_url, '') IS NOT NULL" in API_PY
    assert "NULLIF(t.youtube_url, '') IS NULL" in API_PY


def test_js_classifier_and_enumerations_include_empty():
    # computeTdbPill returns 'empty' for a url-less tracked row.
    assert "if (!it.youtube_url) return 'empty';" in APP_JS
    # ALL set + URL deep-link parser both carry 'empty'.
    assert ("values: ['tdb', 'update', 'cookies', 'dead', 'none', 'dropped', "
            "'empty']") in APP_JS
    assert ("new Set(['tdb','update','cookies','dead','none','dropped',"
            "'empty'])") in APP_JS


def test_template_has_empty_chip_button():
    assert 'data-tdb-pill="empty"' in LIBRARY_HTML
    assert 'tdb-pill-empty tdb-pill-btn' in LIBRARY_HTML
