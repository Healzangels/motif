"""v1.24.25 — resolve_theme_ids re-links a re-added orphan-backed movie by title.

the user's repro: he SET URL'd a theme onto "Avenue Q (2003)" — a non-TDB movie,
so motif created a synthetic plex_orphan theme (negative tmdb_id) + downloaded
the canonical. He then DELETED + RE-ADDED the movie in Plex, which gives it a
NEW rating_key. After a plex_enum refresh the row showed SRC=— / no DL to
restore, even though the canonical (in /config/themes, not the Plex folder) and
the orphan theme + user_override all survive.

Root cause: the ONLY orphan re-link pass (sql_imdb) needs a guid_imdb. Avenue Q
is a stage musical with no IMDb guid in Plex, so the new plex_items row matched
NO pass (sql_title excludes orphans) → theme_id stayed NULL → the library JOIN
missed the surviving orphan/local_files → row looked empty.

Fix: a LAST resolve pass (sql_title_orphan) matches title_norm+year → a
plex_orphan theme when theme_id IS NULL, so a re-added guid-less SET-URL movie
re-bonds to its surviving orphan. Runs after every real-theme pass so real
matches always win first.
"""
from __future__ import annotations

from pathlib import Path

from app.core import plex_enum
from app.core.db import get_conn

# Reuse the v1.22.47 resolve-pass harness (_db seeds movie/tv sections).
from test_v1_22_47_imdb_real_resolve import _db, _theme, _item, _theme_id_of

REPO = Path(__file__).resolve().parent.parent


# ── the fix: re-added guid-less SET-URL movie re-links by title+year ──

def test_readded_orphan_movie_relinks_by_title_year():
    """The exact Avenue Q repro: a plex_orphan theme + a re-added plex_items row
    with NO guids (guid_tmdb/guid_imdb both NULL) but matching title_norm+year
    → re-bonds to the orphan. Pre-fix it stayed NULL (no orphan title pass)."""
    d = _db()
    with get_conn(d) as c:
        orphan = _theme(c, mt="movie", tmdb=-9001, imdb=None,
                        title="Avenue Q", year="2003", upstream="plex_orphan")
        _item(c, rk="rk-new", guid_tmdb=None, guid_imdb=None,
              title="Avenue Q", year="2003")
        c.commit()
    plex_enum.resolve_theme_ids(d)
    assert _theme_id_of(d, "rk-new") == orphan


def test_orphan_title_relink_bridges_tv_media_type():
    """A re-added 'show' row re-bonds to a 'tv' plex_orphan via the CASE bridge."""
    d = _db()
    with get_conn(d) as c:
        orphan = _theme(c, mt="tv", tmdb=-9100, imdb=None, title="Some Show",
                        year="2010", upstream="plex_orphan")
        _item(c, rk="rk", sec="tv", mt="show", title="Some Show", year="2010")
        c.commit()
    plex_enum.resolve_theme_ids(d)
    assert _theme_id_of(d, "rk") == orphan


# ── safety: real themes win, no overwrite, year + media_type disambiguate ──

def test_real_theme_still_wins_over_orphan_by_title():
    """If a REAL theme AND a plex_orphan both match title+year, the real one wins
    (sql_title runs before the new orphan pass)."""
    d = _db()
    with get_conn(d) as c:
        real = _theme(c, mt="movie", tmdb=4242, imdb=None, title="Dual",
                      year="2003", upstream="imdb")
        _theme(c, mt="movie", tmdb=-9002, imdb=None, title="Dual",
               year="2003", upstream="plex_orphan")
        _item(c, rk="rk", title="Dual", year="2003")
        c.commit()
    plex_enum.resolve_theme_ids(d)
    assert _theme_id_of(d, "rk") == real


def test_orphan_title_relink_does_not_overwrite_existing_link():
    """A row already linked is never re-pointed (theme_id IS NULL gate)."""
    d = _db()
    with get_conn(d) as c:
        keep = _theme(c, mt="movie", tmdb=11, imdb=None, title="Keep",
                      year="2003", upstream="imdb")
        orphan = _theme(c, mt="movie", tmdb=-9003, imdb=None, title="Keep",
                        year="2003", upstream="plex_orphan")
        _item(c, rk="rk", title="Keep", year="2003")
        c.execute("UPDATE plex_items SET theme_id=? WHERE rating_key='rk'",
                  (keep,))
        c.commit()
    plex_enum.resolve_theme_ids(d)
    assert _theme_id_of(d, "rk") == keep
    assert orphan != keep


def test_orphan_title_relink_requires_year_match():
    """Year disambiguates remakes — an orphan for a DIFFERENT year must NOT
    bond a row by title alone."""
    d = _db()
    with get_conn(d) as c:
        _theme(c, mt="movie", tmdb=-9004, imdb=None, title="Wonka",
               year="1971", upstream="plex_orphan")
        _item(c, rk="rk", title="Wonka", year="2023")  # different year
        c.commit()
    plex_enum.resolve_theme_ids(d)
    assert _theme_id_of(d, "rk") is None


def test_orphan_title_relink_respects_media_type():
    """A movie row must not bond a tv orphan that shares title+year."""
    d = _db()
    with get_conn(d) as c:
        _theme(c, mt="tv", tmdb=-9005, imdb=None, title="Crossover",
               year="2003", upstream="plex_orphan")
        _item(c, rk="rk", mt="movie", title="Crossover", year="2003")
        c.commit()
    plex_enum.resolve_theme_ids(d)
    assert _theme_id_of(d, "rk") is None


def test_yearless_row_links_via_v1_24_27_exactly_one_fallback():
    """v1.24.27 CHANGED this: a year-less row now bonds to the SINGLE matching
    plex_orphan via sql_title_orphan_yearless (Plex dropped the year on re-add —
    the user's Avenue Q came back year=''). The sql_title_orphan year-match pass
    still doesn't fire (no year to match); the year-less exactly-one fallback
    does. (Pre-v1.24.27 this asserted None — the full ambiguity/remake matrix
    that keeps the fallback safe lives in test_v1_24_27.)"""
    d = _db()
    with get_conn(d) as c:
        orphan = _theme(c, mt="movie", tmdb=-9006, imdb=None, title="NoYear",
                        year="2003", upstream="plex_orphan")
        _item(c, rk="rk", title="NoYear", year=None)
        c.commit()
    plex_enum.resolve_theme_ids(d)
    assert _theme_id_of(d, "rk") == orphan


# ── source pins: the orphan title pass exists + runs LAST ──────────

def test_sql_title_orphan_pass_exists_and_targets_orphans():
    src = (REPO / "app" / "core" / "plex_enum.py").read_text()
    assert "sql_title_orphan = " in src
    i = src.index("sql_title_orphan = ")
    body = src[i:i + 800]
    assert "t.title_norm = plex_items.title_norm" in body
    assert "t.year = plex_items.year" in body
    assert "t.upstream_source = 'plex_orphan'" in body
    assert "plex_items.theme_id IS NULL" in body


def test_sql_title_orphan_runs_after_every_real_theme_pass():
    src = (REPO / "app" / "core" / "plex_enum.py").read_text()
    exec_title = src.index("conn.execute(sql_title, chunk_params)")
    exec_coll = src.index("conn.execute(sql_collection_title, chunk_params)")
    exec_orphan = src.index("conn.execute(sql_title_orphan, chunk_params)")
    assert exec_title < exec_orphan, (
        "title-orphan must run AFTER the real-theme title pass (real wins)")
    assert exec_coll < exec_orphan
