"""v1.22.47 — resolve_theme_ids matches REAL ThemerrDB themes by imdb_id.

the user's question: rows shown as orphans that have IMDB matches — wouldn't the
imdb case match them so they wouldn't be orphaned? Confirmed gap: the only imdb
resolve pass (sql_imdb) filtered `upstream_source = 'plex_orphan'`, so it
re-bonded a Plex row to its own SYNTHETIC orphan theme but NEVER matched a real
TDB theme by imdb. A Plex row with only an imdb GUID (guid_tmdb NULL) whose real
TDB theme existed fell through to the fragile title_norm+year fallback and
orphaned when the title/year didn't align.

The new sql_imdb_real pass matches real themes (`upstream_source != 'plex_orphan'`)
by imdb_id, ordered AFTER the tmdb pass (canonical key) but BEFORE the orphan
re-bond + title fallback (imdb is a precise key). Gated `theme_id IS NULL` — it
only helps UNLINKED rows, never overwrites a link.

This file is the heavy-testing pass: the fix, plus regression locks on every
existing resolve pass (tmdb-preferred, orphan re-bond, title fallback, collection
title) and the safety edges (no overwrite, prefer-real-over-orphan, media_type
bridge, mismatch → no link).
"""
from __future__ import annotations

import tempfile
from pathlib import Path

from app.core import plex_enum
from app.core.db import get_conn, init_db

_NOW = "2026-06-09T09:00:00"
REPO = Path(__file__).resolve().parent.parent


def _db():
    d = Path(tempfile.mkdtemp()) / "motif.db"
    init_db(d)
    with get_conn(d) as c:
        c.execute(
            "INSERT INTO plex_sections (section_id,title,type,included,"
            " is_anime,is_4k,themes_subdir,discovered_at,last_seen_at) "
            "VALUES ('mv','Movies','movie',1,0,0,'m',?,?)", (_NOW, _NOW))
        c.execute(
            "INSERT INTO plex_sections (section_id,title,type,included,"
            " is_anime,is_4k,themes_subdir,discovered_at,last_seen_at) "
            "VALUES ('tv','TV','show',1,0,0,'t',?,?)", (_NOW, _NOW))
        c.commit()
    return d


def _theme(c, *, mt, tmdb, imdb=None, title="T", year="1999",
           upstream="imdb"):
    c.execute(
        "INSERT INTO themes (media_type,tmdb_id,imdb_id,title,title_norm,year,"
        " upstream_source,youtube_url,first_seen_sync_at,last_seen_sync_at) "
        "VALUES (?,?,?,?,?,?,?,'u',?,?)",
        (mt, tmdb, imdb, title, title.lower(), year, upstream, _NOW, _NOW))
    return c.execute("SELECT id FROM themes WHERE tmdb_id=? AND media_type=?",
                     (tmdb, mt)).fetchone()[0]


def _item(c, *, rk, sec="mv", mt="movie", guid_tmdb=None, guid_imdb=None,
          title="P", year="2001"):
    c.execute(
        "INSERT INTO plex_items (rating_key,section_id,media_type,guid_tmdb,"
        " guid_imdb,title,title_norm,year,first_seen_at,last_seen_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?)",
        (rk, sec, mt, guid_tmdb, guid_imdb, title, title.lower(), year,
         _NOW, _NOW))


def _theme_id_of(d, rk):
    with get_conn(d) as c:
        return c.execute("SELECT theme_id FROM plex_items WHERE rating_key=?",
                         (rk,)).fetchone()[0]


# ── the fix: real-theme imdb match ────────────────────────────


def test_imdb_only_row_links_to_real_theme():
    """The exact repro: imdb GUID set, NO tmdb GUID, title/year DON'T match
    the real theme → links via imdb id."""
    d = _db()
    with get_conn(d) as c:
        tid = _theme(c, mt="movie", tmdb=555, imdb="tt1234567",
                     title="The Real Title", year="1999")
        _item(c, rk="rk", guid_imdb="tt1234567",
              title="Totally Different", year="2001")
        c.commit()
    plex_enum.resolve_theme_ids(d)
    assert _theme_id_of(d, "rk") == tid


def test_imdb_real_match_bridges_tv_show_media_type():
    """A 'show' plex_item resolves against a 'tv' theme via the CASE bridge."""
    d = _db()
    with get_conn(d) as c:
        tid = _theme(c, mt="tv", tmdb=600, imdb="tt7000000", title="Show")
        _item(c, rk="rk", sec="tv", mt="show", guid_imdb="tt7000000",
              title="Different", year="2020")
        c.commit()
    plex_enum.resolve_theme_ids(d)
    assert _theme_id_of(d, "rk") == tid


# ── regression: existing passes unchanged ─────────────────────


def test_tmdb_still_preferred_over_imdb():
    """A row with BOTH guids links via the tmdb theme (canonical key), even
    when a DIFFERENT theme shares the imdb id."""
    d = _db()
    with get_conn(d) as c:
        tmdb_theme = _theme(c, mt="movie", tmdb=100, imdb="tt_other",
                            title="Via TMDB")
        # a different theme that also carries the row's imdb id
        _theme(c, mt="movie", tmdb=200, imdb="tt_shared", title="Via IMDB")
        _item(c, rk="rk", guid_tmdb="100", guid_imdb="tt_shared")
        c.commit()
    plex_enum.resolve_theme_ids(d)
    assert _theme_id_of(d, "rk") == tmdb_theme


def test_orphan_rebond_still_works_when_no_real_theme():
    """guid_imdb matches ONLY a synthetic plex_orphan theme → re-bonds to it
    (the v1.15.142 behavior is preserved)."""
    d = _db()
    with get_conn(d) as c:
        orphan = _theme(c, mt="movie", tmdb=-9001, imdb="tt_orphan",
                        title="Orphan", upstream="plex_orphan")
        _item(c, rk="rk", guid_imdb="tt_orphan")
        c.commit()
    plex_enum.resolve_theme_ids(d)
    assert _theme_id_of(d, "rk") == orphan


def test_imdb_real_preferred_over_orphan_when_both_exist():
    """If guid_imdb matches BOTH a real theme and a synthetic orphan, the REAL
    theme wins (imdb_real runs before the orphan re-bond)."""
    d = _db()
    with get_conn(d) as c:
        real = _theme(c, mt="movie", tmdb=300, imdb="tt_dup", title="Real",
                      upstream="imdb")
        _theme(c, mt="movie", tmdb=-9002, imdb="tt_dup", title="Orphan",
               upstream="plex_orphan")
        _item(c, rk="rk", guid_imdb="tt_dup")
        c.commit()
    plex_enum.resolve_theme_ids(d)
    assert _theme_id_of(d, "rk") == real


def test_title_year_fallback_still_works():
    """No guids at all, but title_norm + year match a real theme → title pass
    links it (unchanged)."""
    d = _db()
    with get_conn(d) as c:
        tid = _theme(c, mt="movie", tmdb=400, imdb=None, title="Match Me",
                     year="2010")
        _item(c, rk="rk", title="Match Me", year="2010")
        c.commit()
    plex_enum.resolve_theme_ids(d)
    assert _theme_id_of(d, "rk") == tid


def test_collection_title_match_still_works():
    """Year-less collection title pass unchanged."""
    d = _db()
    with get_conn(d) as c:
        c.execute(
            "INSERT INTO plex_sections (section_id,title,type,included,"
            " is_anime,is_4k,themes_subdir,discovered_at,last_seen_at) "
            "VALUES ('co','Collections','movie',1,0,0,'c',?,?)", (_NOW, _NOW))
        tid = _theme(c, mt="collection", tmdb=700, imdb=None,
                     title="Harry Potter Collection", year=None,
                     upstream="themoviedb")
        c.execute(
            "INSERT INTO plex_items (rating_key,section_id,media_type,"
            " guid_tmdb,guid_imdb,title,title_norm,year,first_seen_at,"
            " last_seen_at) VALUES ('rk','co','collection',NULL,NULL,"
            " 'Harry Potter Collection','harry potter collection',NULL,?,?)",
            (_NOW, _NOW))
        c.commit()
    plex_enum.resolve_theme_ids(d)
    assert _theme_id_of(d, "rk") == tid


# ── safety edges ──────────────────────────────────────────────


def test_imdb_real_does_not_overwrite_existing_link():
    """A row already linked (theme_id set) is never re-pointed by the imdb
    pass — it's gated on theme_id IS NULL."""
    d = _db()
    with get_conn(d) as c:
        keep = _theme(c, mt="movie", tmdb=100, imdb="tt_keep", title="Keep")
        other = _theme(c, mt="movie", tmdb=200, imdb="tt_match",
                       title="Other")
        _item(c, rk="rk", guid_imdb="tt_match")
        c.execute("UPDATE plex_items SET theme_id=? WHERE rating_key='rk'",
                  (keep,))
        c.commit()
    plex_enum.resolve_theme_ids(d)
    assert _theme_id_of(d, "rk") == keep  # not repointed to `other`
    assert other != keep


def test_imdb_mismatch_leaves_row_unlinked():
    d = _db()
    with get_conn(d) as c:
        _theme(c, mt="movie", tmdb=500, imdb="tt_aaa", title="A")
        _item(c, rk="rk", guid_imdb="tt_bbb")  # no theme has tt_bbb
        c.commit()
    plex_enum.resolve_theme_ids(d)
    assert _theme_id_of(d, "rk") is None


def test_imdb_real_respects_media_type():
    """A 'movie' row whose guid_imdb matches only a 'tv' theme's imdb must NOT
    link (the CASE bridge enforces media_type)."""
    d = _db()
    with get_conn(d) as c:
        _theme(c, mt="tv", tmdb=800, imdb="tt_xtype", title="A TV Thing")
        _item(c, rk="rk", mt="movie", guid_imdb="tt_xtype")
        c.commit()
    plex_enum.resolve_theme_ids(d)
    assert _theme_id_of(d, "rk") is None


def test_null_guid_imdb_does_not_fire_imdb_pass():
    """A row with no guid_imdb is untouched by the imdb pass (and has no other
    match) → stays NULL."""
    d = _db()
    with get_conn(d) as c:
        _theme(c, mt="movie", tmdb=900, imdb="tt_zzz", title="Z", year="1990")
        _item(c, rk="rk", guid_imdb=None, title="Nope", year="2001")
        c.commit()
    plex_enum.resolve_theme_ids(d)
    assert _theme_id_of(d, "rk") is None


# ── source pins: real-imdb pass + ordering ────────────────────


def test_imdb_real_pass_exists_and_targets_real_themes():
    src = (REPO / "app" / "core" / "plex_enum.py").read_text()
    assert "sql_imdb_real" in src
    idx = src.index("sql_imdb_real = ")
    body = src[idx:idx + 700]
    assert "t.imdb_id = plex_items.guid_imdb" in body
    assert "t.upstream_source != 'plex_orphan'" in body
    assert "plex_items.theme_id IS NULL" in body


def test_imdb_real_runs_after_tmdb_before_orphan_and_title():
    src = (REPO / "app" / "core" / "plex_enum.py").read_text()
    exec_tmdb = src.index("conn.execute(sql_tmdb, chunk_params)")
    exec_imdb_real = src.index("conn.execute(sql_imdb_real, chunk_params)")
    exec_imdb = src.index("conn.execute(sql_imdb, chunk_params)")
    exec_title = src.index("conn.execute(sql_title, chunk_params)")
    assert exec_tmdb < exec_imdb_real < exec_imdb < exec_title, (
        "v1.22.47: imdb-real must run after tmdb, before orphan re-bond + title")
