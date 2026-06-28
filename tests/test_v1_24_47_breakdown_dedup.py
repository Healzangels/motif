"""v1.24.47 — LOW cleanup pass on the v1.24.43-45 review.

#7: the topbar cycle badges build their {tab, fourk, count} list from a GROUP BY
   (type, is_anime, is_4k, media_type) breakdown, but several GROUP rows collapse
   to one tab — an anime-section AND a movie-section collection both route to
   'collections'. The pre-fix comprehension emitted BOTH as separate cycle
   destinations, so a click "advancing to the next" landed on the URL you were
   already on (a dead click). _breakdown_tabs() now dedups by (tab, fourk).

#5: _breakdown_tab_hint() derives the owning tab from breakdown[0], folding the
   separate _REPUSH/_AWAIT_TAB_HINT_SQL LIMIT-1 queries (now removed) into the
   breakdown the badge already runs.
"""
from __future__ import annotations

from pathlib import Path

from app.web.api import _breakdown_tabs, _breakdown_tab_hint

REPO = Path(__file__).resolve().parent.parent


def _row(media_type, is_anime, typ, is_4k, n):
    # sqlite3.Row and plain dict both support row["key"] — dict is enough here.
    return {"item_media_type": media_type, "is_anime": is_anime,
            "type": typ, "is_4k": is_4k, "n": n}


# ── #7: dedup by (tab, fourk) ────────────────────────────────────────────────

def test_collision_collapses_to_one_tab_with_summed_count():
    # an anime-section collection + a movie-section collection both → collections
    rows = [_row("collection", 0, "movie", 0, 2),
            _row("collection", 1, "movie", 0, 3)]
    assert _breakdown_tabs(rows) == [
        {"tab": "collections", "fourk": False, "count": 5}]


def test_distinct_tabs_and_fourk_are_preserved_in_order():
    rows = [_row(None, 0, "movie", 0, 1),   # movies
            _row(None, 0, "movie", 1, 2),   # movies 4k (distinct fourk)
            _row(None, 1, "movie", 0, 3),   # anime
            _row(None, 0, "show", 0, 4)]    # tv
    assert _breakdown_tabs(rows) == [
        {"tab": "movies", "fourk": False, "count": 1},
        {"tab": "movies", "fourk": True, "count": 2},
        {"tab": "anime", "fourk": False, "count": 3},
        {"tab": "tv", "fourk": False, "count": 4}]


def test_empty_breakdown_is_empty_list_and_none_hint():
    assert _breakdown_tabs([]) == []
    assert _breakdown_tab_hint([]) is None


def test_tab_hint_is_the_first_deduped_tab():
    # rows arrive ORDER BY is_anime, type, is_4k → [0] is the deterministic owner
    rows = [_row("collection", 1, "movie", 0, 3),
            _row("collection", 0, "movie", 0, 2)]
    assert _breakdown_tab_hint(rows) == "collections"
    rows2 = [_row(None, 0, "show", 0, 1), _row(None, 0, "movie", 0, 1)]
    assert _breakdown_tab_hint(rows2) == "tv"


# ── #5: the redundant LIMIT-1 tab_hint SQL is gone ───────────────────────────

def test_redundant_tab_hint_queries_removed():
    src = (REPO / "app" / "web" / "api.py").read_text()
    assert "_REPUSH_TAB_HINT_SQL" not in src, "folded into breakdown[0] (review #5)"
    assert "_AWAIT_TAB_HINT_SQL" not in src
    # the cycle badges route their tab_hint + tabs through the shared helpers
    # (v0.50.34: the AWAIT badge + its awaiting_tab_breakdown_rows were removed).
    assert "_breakdown_tab_hint(repush_tab_breakdown_rows)" in src
    assert "_breakdown_tabs(drops_tab_breakdown_rows)" in src
