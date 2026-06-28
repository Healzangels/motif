"""v1.21.43 — LOW silent-failure audit follow-ups (L3 + L5).

L3 (sync.py): _classify_git_path returned None for an unrecognized path
and the differential-upsert loop `continue`d with NO log — so a ThemerrDB
database-branch layout restructure (every item path classifies as None)
would silently report 0-new/0-updated. Now a path with a KNOWN top-level
dir but an unrecognized kind/leaf warns once (the restructure signal);
truly-unrelated paths (README, pages.json, unknown top) stay silent.

L5 (dashboard-customize.js): saveLayout's `await fetch(PUT)` didn't check
r.ok — fetch doesn't reject on a 4xx/5xx, so a failed save silently lost
the layout, and the "next save retries" comment was fiction. Now it
throws on non-ok + surfaces the failure.
"""
from __future__ import annotations

import logging
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
DCJS = (REPO / "app" / "web" / "static" / "dashboard-customize.js").read_text()


# ── L3: git-path classifier breadcrumb on a restructure ──────


def test_known_top_unknown_kind_warns(caplog):
    import app.core.sync as sync
    sync._GIT_PATH_UNCLASSIFIED_WARNED = False
    with caplog.at_level(logging.WARNING):
        res = sync._classify_git_path("movies/themoviedb_v2/123.json")
    assert res is None
    assert any("layout may have changed" in r.getMessage()
               for r in caplog.records), (
        "a known-top unrecognized-kind path must warn (restructure signal)")


def test_themoviedb_non_int_leaf_warns(caplog):
    import app.core.sync as sync
    sync._GIT_PATH_UNCLASSIFIED_WARNED = False
    with caplog.at_level(logging.WARNING):
        res = sync._classify_git_path("movies/themoviedb/not-a-number.json")
    assert res is None
    assert any("layout may have changed" in r.getMessage()
               for r in caplog.records)


def test_legit_nonitem_paths_stay_silent(caplog):
    import app.core.sync as sync
    sync._GIT_PATH_UNCLASSIFIED_WARNED = False
    with caplog.at_level(logging.WARNING):
        assert sync._classify_git_path("README.md") is None       # 1 part
        assert sync._classify_git_path("movies/pages.json") is None  # 2 parts
        assert sync._classify_git_path("docs/foo/bar.json") is None  # unknown top
    assert not any("layout may have changed" in r.getMessage()
                   for r in caplog.records), (
        "legit non-item paths must NOT trigger the restructure warning")


def test_known_paths_still_classify():
    import app.core.sync as sync
    assert sync._classify_git_path(
        "movies/themoviedb/603.json") == ("movie", None, 603)
    assert sync._classify_git_path(
        "tv_shows/imdb/tt0903747.json") == ("tv", "tt0903747", None)
    assert sync._classify_git_path(
        "movie_collections/themoviedb/10.json") == ("collection", None, 10)


# ── L5: saveLayout surfaces a failed PUT ─────────────────────


def test_save_layout_checks_response_ok():
    assert "if (!r.ok) throw new Error" in DCJS, (
        "saveLayout must check r.ok — fetch doesn't reject on 4xx/5xx")


def test_save_layout_surfaces_failure_and_drops_fiction_comment():
    assert "console.warn('saveLayout failed" in DCJS
    assert "Dashboard layout could not be saved" in DCJS
    # the misleading "next save retries" comment is gone
    assert "the next save retries" not in DCJS
