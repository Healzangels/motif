"""v1.22.91 — theme_id-linked rows are IN Plex, not TDB-only.

the user's repro: the Collections tab's THEMERRDB-ONLY list showed
"28 Days/Weeks/Years Later Collection" and "Addams Family Collection"
as "(not in your Plex library)" — while their own INFO cards proved
the Plex collections exist (the worker skipped placement with
plex_has_theme; the cloud-backup walker staged a B theme from live
rk 262805).

Root cause: Plex collections carry no tmdb:// guid, so the v1.18.2
resolve links them to their TDB record via plex_items.theme_id
(title_norm match). The main library rows, the worker, and the
cloud-backup walker all honor that linkage — but
_library_not_in_plex's NOT EXISTS was guid-only, so every
title-linked collection ALSO rendered in the TDB-only list. Same
class covered guid-less legacy-agent movie/tv rows, and the
reprobe-failures visibility filter shared the blind spot.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()

TS = "2026-06-11T12:00:00"


@pytest.fixture
def linked_collection_fixture(tmp_path: Path):
    """A TDB collection whose Plex collection row is linked ONLY via
    theme_id (guid_tmdb NULL — the real shape Plex collections have),
    plus a genuinely-absent TDB collection as the control."""
    db_path = tmp_path / "motif.db"
    from app.core.db import init_db
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 0, 0, "
            "        'movies', 1, ?, ?)",
            (TS, TS),
        )
        cur = conn.execute(
            "INSERT INTO themes "
            "  (media_type, tmdb_id, title, title_norm, year, "
            "   upstream_source, last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('collection', 11716, 'Addams Family Collection', "
            "        'addams family collection', NULL, "
            "        'themoviedb', ?, ?)",
            (TS, TS),
        )
        linked_theme_id = cur.lastrowid
        # The Plex collection row: NO tmdb guid (Plex collections never
        # carry one) — linked solely by the v1.18.2 title_norm resolve.
        conn.execute(
            "INSERT INTO plex_items "
            "  (rating_key, section_id, media_type, title, title_norm, "
            "   guid_tmdb, theme_id, has_theme, first_seen_at, "
            "   last_seen_at) "
            "VALUES ('262805', '1', 'collection', "
            "        'Addams Family Collection', "
            "        'addams family collection', NULL, ?, 1, ?, ?)",
            (linked_theme_id, TS, TS),
        )
        # Control: a TDB collection genuinely absent from Plex.
        conn.execute(
            "INSERT INTO themes "
            "  (media_type, tmdb_id, title, title_norm, year, "
            "   upstream_source, last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('collection', 8001, 'Lonely Collection', "
            "        'lonely collection', NULL, 'themoviedb', ?, ?)",
            (TS, TS),
        )
        conn.commit()
    return db_path


def test_theme_id_linked_collection_not_listed_tdb_only(
        linked_collection_fixture):
    """The theme_id-linked collection must NOT appear in the
    not-in-plex list; the genuinely-absent one must."""
    from app.web.api import _library_not_in_plex
    result = _library_not_in_plex(
        linked_collection_fixture, tab="collections", fourk=False,
        q="", page=1, per_page=50,
    )
    titles = [it["plex_title"] for it in result["items"]]
    assert "Addams Family Collection" not in titles, (
        "v1.22.91: a Plex collection linked via theme_id (guid_tmdb "
        "NULL — the only linkage Plex collections can have) is IN "
        "Plex and must not render '(not in your Plex library)'"
    )
    assert titles == ["Lonely Collection"]
    assert result["total"] == 1


def test_guid_linked_row_still_excluded(linked_collection_fixture):
    """Regression guard: the original guid-based exclusion still
    works (a movie present via guid_tmdb stays out of the list)."""
    db_path = linked_collection_fixture
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO themes "
            "  (media_type, tmdb_id, title, title_norm, year, "
            "   upstream_source, last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('movie', 9001, 'Present Movie', 'present movie', "
            "        '2020', 'themoviedb', ?, ?)",
            (TS, TS),
        )
        conn.execute(
            "INSERT INTO plex_items "
            "  (rating_key, section_id, media_type, title, title_norm, "
            "   guid_tmdb, theme_id, has_theme, first_seen_at, "
            "   last_seen_at) "
            "VALUES ('555', '1', 'movie', 'Present Movie', "
            "        'present movie', 9001, NULL, 0, ?, ?)",
            (TS, TS),
        )
        conn.commit()
    from app.web.api import _library_not_in_plex
    result = _library_not_in_plex(
        db_path, tab="movies", fourk=False,
        q="", page=1, per_page=50,
    )
    assert [it["plex_title"] for it in result["items"]] == []


def test_excluded_section_does_not_count_as_in_plex(
        linked_collection_fixture):
    """A theme_id link through an EXCLUDED section must not hide the
    row — the included=1 join is part of the contract."""
    db_path = linked_collection_fixture
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE plex_sections SET included = 0 WHERE section_id = '1'")
        conn.commit()
    from app.web.api import _library_not_in_plex
    result = _library_not_in_plex(
        db_path, tab="collections", fourk=False,
        q="", page=1, per_page=50,
    )
    titles = sorted(it["plex_title"] for it in result["items"])
    assert titles == ["Addams Family Collection", "Lonely Collection"]


def test_reprobe_visibility_filter_accepts_theme_id_link():
    """The failures-only reprobe scope shares the fix — a guid-less
    theme_id-linked row's failure is re-probeable."""
    i = API_PY.index("(all red-pill failures, cooldown bypassed)")
    block = API_PY[max(0, i - 1600):i]
    assert "pi.theme_id = t.id" in block
    assert "OR (pi.guid_tmdb = t.tmdb_id" in block
