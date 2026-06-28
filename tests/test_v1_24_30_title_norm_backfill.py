"""v1.24.30 — backfill themes.title_norm so the whole orphan cohort can re-link.

The v1.24.27 prod diagnostic surfaced ~340 plex_orphan themes with
title_norm=NULL. The v1.24.25/.27 title re-link passes match
`t.title_norm = plex_items.title_norm`, and NULL = anything is FALSE in SQL — so
those orphans could NEVER self-recover on a Plex delete+re-add (the exact Avenue
Q failure, but with no recoverable signal at all). Root cause: 4 of the 5
orphan-creation paths (adopt ×2, bulk import, the collection SET-URL) never
stamped title_norm — only the v1.24.x movie SET-URL path did (which is why
Avenue Q itself had it).

Fix: resolve_theme_ids backfills title_norm (= normalize_title(title)) on any
theme missing it BEFORE its title-match passes run — the single chokepoint that
consumes title_norm. Every refresh self-heals the whole cohort + any future
NULL, no restart, idempotent.
"""
from __future__ import annotations

from app.core import plex_enum
from app.core.db import get_conn
from app.core.normalize import normalize_title

from test_v1_22_47_imdb_real_resolve import _db, _item, _theme_id_of, _NOW


def _null_tn_orphan(c, *, tmdb, title, year="2003", mt="movie"):
    c.execute(
        "INSERT INTO themes (media_type, tmdb_id, title, year, upstream_source, "
        " youtube_url, first_seen_sync_at, last_seen_sync_at, title_norm) "
        "VALUES (?, ?, ?, ?, 'plex_orphan', 'u', ?, ?, NULL)",
        (mt, tmdb, title, year, _NOW, _NOW))
    return c.execute("SELECT id FROM themes WHERE tmdb_id=? AND media_type=?",
                     (tmdb, mt)).fetchone()[0]


def _title_norm_of(d, tmdb):
    with get_conn(d) as c:
        return c.execute("SELECT title_norm FROM themes WHERE tmdb_id=?",
                         (tmdb,)).fetchone()[0]


# ── the backfill itself ─────────────────────────────────────────────────

def test_backfill_stamps_null_title_norm():
    d = _db()
    with get_conn(d) as c:
        _null_tn_orphan(c, tmdb=-77, title="Avenue Q")
        c.commit()
    plex_enum.resolve_theme_ids(d)
    assert _title_norm_of(d, -77) == normalize_title("Avenue Q")


def test_backfill_is_idempotent_and_preserves_existing():
    d = _db()
    with get_conn(d) as c:
        # a real theme that ALREADY has a (deliberately distinct) title_norm
        c.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, year, "
            " upstream_source, youtube_url, first_seen_sync_at, "
            " last_seen_sync_at, title_norm) "
            "VALUES ('movie', 500, 'Keep Me', '2001', 'imdb', 'u', ?, ?, "
            "'kept-norm')", (_NOW, _NOW))
        _null_tn_orphan(c, tmdb=-78, title="Fill Me")
        c.commit()
    plex_enum.resolve_theme_ids(d)
    plex_enum.resolve_theme_ids(d)  # second run must not churn / raise
    assert _title_norm_of(d, 500) == "kept-norm", "existing title_norm untouched"
    assert _title_norm_of(d, -78) == normalize_title("Fill Me")


# ── end-to-end: the cohort recovery the backfill unblocks ───────────────

def test_null_title_norm_orphan_relinks_after_backfill():
    """The payoff: a re-added item can re-link to a NULL-title_norm orphan
    because resolve backfills title_norm FIRST, then the title pass matches."""
    d = _db()
    with get_conn(d) as c:
        oid = _null_tn_orphan(c, tmdb=-79, title="Avenue Q", year="2003")
        _item(c, rk="rk-new", guid_tmdb=None, guid_imdb=None,
              title="Avenue Q", year="2003")
        c.commit()
    plex_enum.resolve_theme_ids(d)
    assert _theme_id_of(d, "rk-new") == oid


def test_without_backfill_null_title_norm_cannot_match():
    """Discriminator: with title_norm left NULL (no resolve run), the orphan is
    unmatched — proving the backfill is what unblocks the link."""
    d = _db()
    with get_conn(d) as c:
        _null_tn_orphan(c, tmdb=-80, title="Avenue Q", year="2003")
        _item(c, rk="rk", title="Avenue Q", year="2003")
        c.commit()
        # assert the raw join can't match a NULL title_norm (pre-backfill state)
        hit = c.execute(
            "SELECT t.id FROM themes t JOIN plex_items pi "
            " ON t.title_norm = pi.title_norm AND t.year = pi.year "
            "WHERE pi.rating_key='rk'").fetchone()
    assert hit is None


# ── source pins ─────────────────────────────────────────────────────────

def test_backfill_runs_before_title_passes():
    from pathlib import Path
    src = (Path(__file__).resolve().parent.parent
           / "app" / "core" / "plex_enum.py").read_text()
    assert "backfilled title_norm on" in src
    backfill_idx = src.index("backfilled title_norm on")
    title_pass_idx = src.index("sql_title_orphan = ")
    assert backfill_idx < title_pass_idx, (
        "the title_norm backfill must run BEFORE the title-match passes")
