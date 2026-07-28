"""v0.51.235 — an item-shaped git path we can't classify is a real miss.

Pre-fix the apply loop's `if classification is None: continue` treated a
per-item record it failed to parse exactly like a README edit: no error, so the
run reported 0 errors / 0 new / 0 updated, and the `stats.errors == 0` gate
advanced the git baseline — consuming that add/modify permanently (the v1.22.74
failure mode). v1.21.43 already warned once; this makes it COUNT.

The danger in counting is the mirror image: if a path that appears on every
sync were flagged, stats.errors would never be 0 and the baseline would pin
forever. The index artifacts (movies/all_page_N.json, movie_collections/
pages.json) are TWO-part paths, so they are not item-shaped — pinned below.
"""
from __future__ import annotations

from app.core.sync import _classify_git_path, _is_item_shaped_git_path


# ── the shape predicate ──────────────────────────────────────────────────

def test_real_item_records_are_item_shaped():
    for p in ("movies/imdb/tt0000001.json",
              "movies/themoviedb/100.json",
              "tv_shows/imdb/tt0000002.json",
              "tv_shows/themoviedb/200.json",
              "movie_collections/themoviedb/1241.json"):
        assert _is_item_shaped_git_path(p), p


def test_unparseable_but_item_shaped_paths_are_flagged():
    """These classify to None yet are clearly per-item records — the case that
    must block the baseline advance instead of being silently consumed."""
    for p in ("movies/themoviedb/abc.json",
              "movies/themoviedb/not-a-number.json",
              "movies/themoviedb_v2/123.json",
              "movie_collections/themoviedb/abc.json"):
        assert _classify_git_path(p) is None, p
        assert _is_item_shaped_git_path(p), p


def test_index_artifacts_are_not_item_shaped():
    """THE load-bearing case. all_page_N.json / pages.json live at depth 2, so
    they can never be counted as errors — if they were, every sync would carry
    a non-zero error count and the git baseline would pin FOREVER."""
    for p in ("movies/all_page_1.json",
              "movies/pages.json",
              "tv_shows/all_page_12.json",
              "movie_collections/pages.json",
              "movie_collections/all_page_1.json"):
        assert _classify_git_path(p) is None, p
        assert not _is_item_shaped_git_path(p), p


def test_unrelated_paths_are_not_item_shaped():
    for p in ("README.md",
              ".github/workflows/ci.yml",
              "docs/foo/bar.json",
              "movies/imdb/not_an_int.txt"):
        assert not _is_item_shaped_git_path(p), p


# ── the classifier refactor is behaviour-preserving ──────────────────────

def test_classification_unchanged_by_the_top_dir_lookup():
    """v0.51.235 replaced the if/elif top-dir chain with _GIT_ITEM_TOP_DIRS so
    the predicate above can't drift from it. Same answers as before."""
    assert _classify_git_path("movies/imdb/tt0000001.json") == ("movie", "tt0000001", None)
    assert _classify_git_path("movies/themoviedb/100.json") == ("movie", None, 100)
    assert _classify_git_path("tv_shows/imdb/tt0000002.json") == ("tv", "tt0000002", None)
    assert _classify_git_path("tv_shows/themoviedb/200.json") == ("tv", None, 200)
    assert _classify_git_path(
        "movie_collections/themoviedb/1241.json") == ("collection", None, 1241)
    assert _classify_git_path("other_top/themoviedb/1.json") is None


def test_top_dirs_constant_matches_the_media_types_it_yields():
    from app.core.sync import _GIT_ITEM_TOP_DIRS
    for top, media_type in _GIT_ITEM_TOP_DIRS.items():
        got = _classify_git_path(f"{top}/themoviedb/7.json")
        assert got == (media_type, None, 7), (top, got)


# ── the apply loop counts it ─────────────────────────────────────────────

def test_apply_loop_counts_an_unclassified_item_path_as_an_error():
    """Behavioural: the loop must bump stats.errors + failed_paths for an
    item-shaped miss, because that is what the run_sync baseline gate reads."""
    import inspect
    from app.core import sync

    src = inspect.getsource(sync._run_git_differential_upsert)
    i = src.index("classification = _classify_git_path(rel_path)")
    j = src.index("media_type, imdb_id, tmdb_id = classification", i)
    block = src[i:j]
    assert "_is_item_shaped_git_path(rel_path)" in block, (
        "the None branch must distinguish an item-shaped miss from a README")
    for counter in ("stats.errors += 1",
                    "failed_paths.append(rel_path)",
                    "unresolved_failures += 1"):
        assert counter in block, (
            f"{counter} missing — the baseline gate, the chronic escape and the "
            f"baseline-reset drop-detection skip each read one of these")
