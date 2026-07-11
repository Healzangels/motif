"""v0.51.133 — resolve_theme_ids accepts a media_type scope.

plex_enum runs the per-section resolve TWICE (after the items upsert, then after
the collections upsert), and both re-walked the WHOLE section — so a 10.5K-movie
section re-resolved all its items on the collections pass. Passing the pass's
media_type restricts each call to the rows it just touched.

This is purely a NARROWING of the rating_key set — the per-row match logic is
unchanged — so these tests prove: (a) a movie-scoped pass resolves movies and
leaves collections untouched, (b) a collection-scoped pass resolves collections,
and (c) the two scoped passes produce EXACTLY the state one unscoped pass would.
"""
from __future__ import annotations

import tempfile
from pathlib import Path

from app.core import plex_enum
from app.core.db import get_conn, init_db

NOW = "2026-07-11T00:00:00Z"


def _db():
    d = Path(tempfile.mkdtemp()) / "motif.db"
    init_db(d)
    with get_conn(d) as c:
        c.execute(
            "INSERT INTO plex_sections (section_id,title,type,included,is_anime,"
            " is_4k,themes_subdir,discovered_at,last_seen_at) "
            "VALUES ('mv','Movies','movie',1,0,0,'m',?,?)", (NOW, NOW))
        # a movie theme + a collection theme, each keyed by tmdb.
        c.execute(
            "INSERT INTO themes (media_type,tmdb_id,title,title_norm,year,"
            " upstream_source,youtube_url,first_seen_sync_at,last_seen_sync_at) "
            "VALUES ('movie',100,'Mov','mov','2001','themoviedb','u',?,?)",
            (NOW, NOW))
        c.execute(
            "INSERT INTO themes (media_type,tmdb_id,title,title_norm,year,"
            " upstream_source,youtube_url,first_seen_sync_at,last_seen_sync_at) "
            "VALUES ('collection',200,'Col','col','','themoviedb','u',?,?)",
            (NOW, NOW))
        # a movie row + a collection row, both in section 'mv' (collections live
        # in the same section as items).
        c.execute(
            "INSERT INTO plex_items (rating_key,section_id,media_type,guid_tmdb,"
            " title,title_norm,year,first_seen_at,last_seen_at) "
            "VALUES ('m1','mv','movie',100,'Mov','mov','2001',?,?)", (NOW, NOW))
        c.execute(
            "INSERT INTO plex_items (rating_key,section_id,media_type,guid_tmdb,"
            " title,title_norm,year,first_seen_at,last_seen_at) "
            "VALUES ('c1','mv','collection',200,'Col','col','',?,?)", (NOW, NOW))
        c.commit()
    return d


def _tid(d, rk):
    with get_conn(d) as c:
        return c.execute("SELECT theme_id FROM plex_items WHERE rating_key=?",
                         (rk,)).fetchone()[0]


def _theme(d, mt):
    with get_conn(d) as c:
        return c.execute("SELECT id FROM themes WHERE media_type=?",
                         (mt,)).fetchone()[0]


def test_movie_pass_resolves_movies_and_leaves_collections_untouched():
    d = _db()
    plex_enum.resolve_theme_ids(d, section_id="mv", media_type="movie")
    assert _tid(d, "m1") == _theme(d, "movie"), "movie must resolve on the movie pass"
    assert _tid(d, "c1") is None, "the collection must be untouched by the movie pass"


def test_collection_pass_resolves_collections():
    d = _db()
    plex_enum.resolve_theme_ids(d, section_id="mv", media_type="movie")
    plex_enum.resolve_theme_ids(d, section_id="mv", media_type="collection")
    assert _tid(d, "c1") == _theme(d, "collection")


def test_two_scoped_passes_equal_one_unscoped_pass():
    # scoped: items pass then collections pass.
    d_scoped = _db()
    plex_enum.resolve_theme_ids(d_scoped, section_id="mv", media_type="movie")
    plex_enum.resolve_theme_ids(d_scoped, section_id="mv", media_type="collection")
    # unscoped: one section-wide pass (the pre-v0.51.133 behaviour).
    d_all = _db()
    plex_enum.resolve_theme_ids(d_all, section_id="mv")
    for rk in ("m1", "c1"):
        assert _tid(d_scoped, rk) == _tid(d_all, rk), (
            f"{rk}: scoped two-pass must resolve identically to the unscoped pass")
    assert _tid(d_scoped, "m1") == _theme(d_scoped, "movie")
    assert _tid(d_scoped, "c1") == _theme(d_scoped, "collection")


def test_signature_has_media_type_param():
    import inspect
    assert "media_type" in inspect.signature(plex_enum.resolve_theme_ids).parameters
