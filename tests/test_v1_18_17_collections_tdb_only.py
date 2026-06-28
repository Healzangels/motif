"""v1.18.17 — Collections tab TDB-only filter scopes to collections.

the user's audit on v1.18.16 Collections tab:

  > the theme in themerrdb but not in plex filter doesn't appear
  > to be showing themes only in its filter its showing other
  > content as well instead of only collections that themerrdb
  > has a theme for but that aren't in plex

Pre-fix `_library_not_in_plex` mapped `tab` to media_type via:

    media_type = "movie" if tab == "movies" else "tv"
    plex_media_type = "movie" if tab == "movies" else "show"

For `tab='collections'` this fell through to the `else` branch
and queried `t.media_type='tv'`, returning TV shows ThemerrDB
tracks but Plex doesn't have — irrelevant to the Collections
tab. the user's screenshot showed "16bit Sensation", "2 Stupid
Dogs", "4 Blocks" etc.

## Fix

Explicit branch in `_library_not_in_plex`:

    if tab == "movies":     → movie/movie
    elif tab == "collections": → collection/collection   (NEW)
    else (tv + anime):      → tv/show

Anime/TV mapping unchanged (per v1.12.10 anime≡TV semantics).
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

API_PY = REPO / "app" / "web" / "api.py"


# ── Source pin ────────────────────────────────────────────────


def test_not_in_plex_has_collections_branch():
    """`_library_not_in_plex` must include an explicit
    tab=='collections' branch."""
    src = API_PY.read_text()
    fn_idx = src.index("def _library_not_in_plex(")
    body = src[fn_idx:fn_idx + 4000]
    assert 'elif tab == "collections":' in body, (
        "v1.18.17: _library_not_in_plex must branch on "
        "tab=='collections' to set media_type='collection'"
    )
    assert 'media_type = "collection"' in body
    assert 'plex_media_type = "collection"' in body


def test_not_in_plex_drops_else_movie_fallthrough():
    """The pre-fix ternary `media_type = 'movie' if tab=='movies'
    else 'tv'` mapped collections → tv. The new control flow must
    not use that shape."""
    src = API_PY.read_text()
    fn_idx = src.index("def _library_not_in_plex(")
    body = src[fn_idx:fn_idx + 4000]
    assert 'media_type = "movie" if tab == "movies" else "tv"' not in body, (
        "v1.18.17: ternary fallthrough that mapped collections "
        "to tv must be removed"
    )


# ── End-to-end: collections filter returns only collections ───


@pytest.fixture
def mixed_tdb_fixture(tmp_path: Path):
    """Seed ThemerrDB-themed rows across all media types, plus
    one plex_items presence for each, then DELETE the collection
    plex_items so it counts as 'not in Plex'."""
    db_path = tmp_path / "motif.db"
    from app.core.db import init_db
    init_db(db_path)
    ts = "2026-05-20T16:00:00"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 0, 0, "
            "        'movies', 1, ?, ?)",
            (ts, ts),
        )
        # A TV show ThemerrDB tracks, NOT in Plex.
        conn.execute(
            "INSERT INTO themes "
            "  (media_type, tmdb_id, title, title_norm, year, "
            "   upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('tv', 7001, '2 Stupid Dogs', "
            "        '2 stupid dogs', '1993', 'themoviedb', ?, ?)",
            (ts, ts),
        )
        # A collection ThemerrDB tracks, NOT in Plex.
        conn.execute(
            "INSERT INTO themes "
            "  (media_type, tmdb_id, title, title_norm, year, "
            "   upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('collection', 8001, "
            "        'Lonely Collection', "
            "        'lonely collection', NULL, 'themoviedb', ?, ?)",
            (ts, ts),
        )
        # A movie ThemerrDB tracks, NOT in Plex.
        conn.execute(
            "INSERT INTO themes "
            "  (media_type, tmdb_id, title, title_norm, year, "
            "   upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('movie', 9001, 'Missing Movie', "
            "        'missing movie', '2020', 'themoviedb', ?, ?)",
            (ts, ts),
        )
        conn.commit()
    return db_path


def test_collections_tab_returns_only_collections(mixed_tdb_fixture):
    """On the Collections tab with not_in_plex filter, the result
    set must contain ONLY collection-typed themes — no movies, no
    TV shows."""
    db_path = mixed_tdb_fixture
    from app.web.api import _library_not_in_plex
    result = _library_not_in_plex(
        db_path, tab="collections", fourk=False,
        q="", page=1, per_page=50,
    )
    items = result["items"]
    assert len(items) == 1, (
        f"v1.18.17: collections tab not_in_plex should return only "
        f"the collection-typed TDB theme, got {len(items)}: "
        f"{[it['plex_title'] for it in items]}"
    )
    assert items[0]["theme_media_type"] == "collection"
    assert items[0]["plex_title"] == "Lonely Collection"


def test_tv_tab_returns_only_tv(mixed_tdb_fixture):
    """Regression guard: TV tab still returns TV themes (not
    affected by the collections branch addition)."""
    db_path = mixed_tdb_fixture
    from app.web.api import _library_not_in_plex
    result = _library_not_in_plex(
        db_path, tab="tv", fourk=False,
        q="", page=1, per_page=50,
    )
    items = result["items"]
    assert len(items) == 1
    assert items[0]["theme_media_type"] == "tv"
    assert items[0]["plex_title"] == "2 Stupid Dogs"


def test_movies_tab_returns_only_movies(mixed_tdb_fixture):
    """Regression guard: movies tab still returns movie themes."""
    db_path = mixed_tdb_fixture
    from app.web.api import _library_not_in_plex
    result = _library_not_in_plex(
        db_path, tab="movies", fourk=False,
        q="", page=1, per_page=50,
    )
    items = result["items"]
    assert len(items) == 1
    assert items[0]["theme_media_type"] == "movie"
    assert items[0]["plex_title"] == "Missing Movie"


def test_anime_tab_returns_tv_themes(mixed_tdb_fixture):
    """Anime tab maps to TV (per v1.12.10) — the underlying
    ThemerrDB tracking is TV records; tab scoping happens via
    section flags in the in-Plex path. Same TV themes surface on
    the anime tab's not_in_plex view."""
    db_path = mixed_tdb_fixture
    from app.web.api import _library_not_in_plex
    result = _library_not_in_plex(
        db_path, tab="anime", fourk=False,
        q="", page=1, per_page=50,
    )
    items = result["items"]
    assert len(items) == 1
    assert items[0]["theme_media_type"] == "tv"


def test_collections_tab_excludes_collections_already_in_plex(tmp_path: Path):
    """A ThemerrDB-tracked collection that already exists in
    plex_items must NOT appear in the not_in_plex result."""
    db_path = tmp_path / "motif.db"
    from app.core.db import init_db
    from app.web.api import _library_not_in_plex
    init_db(db_path)
    ts = "2026-05-20T16:00:00"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 0, 0, "
            "        'movies', 1, ?, ?)",
            (ts, ts),
        )
        conn.execute(
            "INSERT INTO themes "
            "  (media_type, tmdb_id, title, title_norm, year, "
            "   upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('collection', 9999, "
            "        'Owned Collection', 'owned collection', "
            "        NULL, 'themoviedb', ?, ?)",
            (ts, ts),
        )
        conn.execute(
            "INSERT INTO plex_items "
            "  (rating_key, section_id, media_type, title, "
            "   title_norm, year, folder_path, "
            "   guid_tmdb, first_seen_at, last_seen_at) "
            "VALUES ('500', '1', 'collection', 'Owned Collection', "
            "        'owned collection', NULL, '', "
            "        '9999', ?, ?)",
            (ts, ts),
        )
        conn.commit()
    result = _library_not_in_plex(
        db_path, tab="collections", fourk=False,
        q="", page=1, per_page=50,
    )
    assert result["total"] == 0, (
        "v1.18.17: collection that exists in plex_items must NOT "
        "appear in the not_in_plex result"
    )
