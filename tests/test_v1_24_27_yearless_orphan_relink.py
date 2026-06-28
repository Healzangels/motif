"""v1.24.27 — year-LESS orphan re-link (Avenue Q, the data-confirmed sequel).

Continuation of v1.24.25. the user's prod diagnostic on the re-added "Avenue Q"
showed the exact failure mode the v1.24.25 title+year orphan pass couldn't cover:

  plex_items: year='' (Plex re-added the movie with NO year — folder
              "Avenue Q ()"), no guids, theme_id=NULL
  themes:     the plex_orphan theme survives, year='2003'
  local_files/user_overrides: the canonical + URL survive

sql_title_orphan requires `t.year = plex_items.year`; '2003' != '' → no link →
the row stranded at SRC=— / no DL despite everything surviving.

The fix `sql_title_orphan_yearless` falls back to title_norm ALONE, but only
when the row has no usable year (NULL or '') AND exactly ONE plex_orphan theme
shares the title (the exactly-one-candidate gate keeps remakes safe). A row that
DOES carry a year still goes through the precise year-matching pass — so the
Wonka 1971-vs-2023 guard is untouched.
"""
from __future__ import annotations

from pathlib import Path

from app.core import plex_enum
from app.core.db import get_conn

from test_v1_22_47_imdb_real_resolve import _db, _theme, _item, _theme_id_of

REPO = Path(__file__).resolve().parent.parent


# ── the fix: a year-less re-added orphan movie re-links by title alone ──

def test_avenue_q_empty_year_relinks_to_single_orphan():
    """The exact prod repro: plex_items.year='' + one plex_orphan (year='2003')
    + no guids → bonds by title_norm. Pre-fix it stayed NULL (year mismatch)."""
    d = _db()
    with get_conn(d) as c:
        orphan = _theme(c, mt="movie", tmdb=-29, imdb=None, title="Avenue Q",
                        year="2003", upstream="plex_orphan")
        _item(c, rk="714864", guid_tmdb=None, guid_imdb=None,
              title="Avenue Q", year="")  # Plex lost the year on re-add
        c.commit()
    plex_enum.resolve_theme_ids(d)
    assert _theme_id_of(d, "714864") == orphan


def test_null_year_relinks_to_single_orphan():
    """year IS NULL is covered by the same fallback (some agents store NULL, not
    '')."""
    d = _db()
    with get_conn(d) as c:
        orphan = _theme(c, mt="movie", tmdb=-30, imdb=None, title="Solo Title",
                        year="1999", upstream="plex_orphan")
        _item(c, rk="rk", title="Solo Title", year=None)
        c.commit()
    plex_enum.resolve_theme_ids(d)
    assert _theme_id_of(d, "rk") == orphan


def test_yearless_show_bridges_to_tv_orphan():
    """A re-added year-less 'show' row bonds a 'tv' plex_orphan via the CASE."""
    d = _db()
    with get_conn(d) as c:
        orphan = _theme(c, mt="tv", tmdb=-31, imdb=None, title="Yearless Show",
                        year="2010", upstream="plex_orphan")
        _item(c, rk="rk", sec="tv", mt="show", title="Yearless Show", year="")
        c.commit()
    plex_enum.resolve_theme_ids(d)
    assert _theme_id_of(d, "rk") == orphan


# ── safety: the exactly-one gate + the year-present remake guard ──────

def test_yearless_row_with_two_orphans_is_ambiguous_no_link():
    """Two same-title plex_orphan themes (different years) + a year-less row →
    ambiguous without a year → the exactly-one gate leaves it NULL for the
    operator to resolve. This is the remake case the gate protects."""
    d = _db()
    with get_conn(d) as c:
        _theme(c, mt="movie", tmdb=-40, imdb=None, title="Twin",
               year="1971", upstream="plex_orphan")
        _theme(c, mt="movie", tmdb=-41, imdb=None, title="Twin",
               year="2023", upstream="plex_orphan")
        _item(c, rk="rk", title="Twin", year="")
        c.commit()
    plex_enum.resolve_theme_ids(d)
    assert _theme_id_of(d, "rk") is None


def test_year_present_mismatch_still_blocked_remake_guard():
    """A row that DOES carry a year is NOT eligible for the year-less fallback —
    it still requires the year to match via sql_title_orphan. So a Plex year that
    disagrees with the only orphan's year stays unlinked (the user's Wonka
    1971-vs-2023 guard is preserved; the year-less path can't bypass it)."""
    d = _db()
    with get_conn(d) as c:
        _theme(c, mt="movie", tmdb=-42, imdb=None, title="Wonka",
               year="1971", upstream="plex_orphan")
        _item(c, rk="rk", title="Wonka", year="2023")  # has a year, mismatched
        c.commit()
    plex_enum.resolve_theme_ids(d)
    assert _theme_id_of(d, "rk") is None


def test_yearless_fallback_targets_only_orphans():
    """The fallback bonds plex_orphan themes ONLY. A year-less row whose single
    same-title theme is a REAL (non-orphan) theme is left for the year-matching
    real passes — the orphan fallback must not bond it."""
    d = _db()
    with get_conn(d) as c:
        _theme(c, mt="movie", tmdb=500, imdb=None, title="RealOnly",
               year="2000", upstream="imdb")  # real, not orphan
        _item(c, rk="rk", title="RealOnly", year="")
        c.commit()
    plex_enum.resolve_theme_ids(d)
    assert _theme_id_of(d, "rk") is None


def test_yearless_fallback_respects_media_type():
    """A year-less movie row must not bond a tv orphan that shares the title."""
    d = _db()
    with get_conn(d) as c:
        _theme(c, mt="tv", tmdb=-43, imdb=None, title="Crossover",
               year="2003", upstream="plex_orphan")
        _item(c, rk="rk", mt="movie", title="Crossover", year="")
        c.commit()
    plex_enum.resolve_theme_ids(d)
    assert _theme_id_of(d, "rk") is None


def test_yearless_fallback_does_not_overwrite_existing_link():
    """A year-less row already linked is never re-pointed (theme_id IS NULL
    gate)."""
    d = _db()
    with get_conn(d) as c:
        keep = _theme(c, mt="movie", tmdb=-44, imdb=None, title="Keep",
                      year="2003", upstream="plex_orphan")
        other = _theme(c, mt="movie", tmdb=-45, imdb=None, title="Keep",
                       year="2009", upstream="plex_orphan")
        _item(c, rk="rk", title="Keep", year="")
        c.execute("UPDATE plex_items SET theme_id=? WHERE rating_key='rk'",
                  (keep,))
        c.commit()
    plex_enum.resolve_theme_ids(d)
    assert _theme_id_of(d, "rk") == keep
    assert other != keep


def test_year_match_still_wins_first_when_year_present():
    """When the row has a year that matches its orphan, the precise pass links it
    (and the year-less fallback never runs for it — its year is non-empty)."""
    d = _db()
    with get_conn(d) as c:
        orphan = _theme(c, mt="movie", tmdb=-46, imdb=None, title="Exact",
                        year="2003", upstream="plex_orphan")
        _item(c, rk="rk", title="Exact", year="2003")
        c.commit()
    plex_enum.resolve_theme_ids(d)
    assert _theme_id_of(d, "rk") == orphan


# ── source pins: the year-less pass exists + runs LAST ──────────────

def test_yearless_pass_exists_and_is_gated():
    src = (REPO / "app" / "core" / "plex_enum.py").read_text()
    assert "sql_title_orphan_yearless = " in src
    i = src.index("sql_title_orphan_yearless = ")
    body = src[i:i + 1400]
    assert "t.upstream_source = 'plex_orphan'" in body
    assert "plex_items.year IS NULL OR plex_items.year = ''" in body
    assert ") = 1" in body  # the exactly-one-candidate COUNT gate
    assert "plex_items.theme_id IS NULL" in body


def test_yearless_pass_runs_after_year_matching_orphan_pass():
    src = (REPO / "app" / "core" / "plex_enum.py").read_text()
    exec_match = src.index("conn.execute(sql_title_orphan, chunk_params)")
    exec_yearless = src.index(
        "conn.execute(sql_title_orphan_yearless, chunk_params)")
    assert exec_match < exec_yearless, (
        "year-less fallback must run AFTER the precise year-matching orphan pass")
