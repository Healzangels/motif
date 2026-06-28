"""v1.18.0 Phase 3 — Sync absorbs movie_collections/themoviedb/*.

ThemerrDB publishes a collections tree alongside movies/ and
tv_shows/:

  /movie_collections/pages.json
  /movie_collections/all_page_<n>.json
  /movie_collections/themoviedb/<tmdb_collection_id>.json

There is NO imdb/ subtree — TMDB collection IDs are the only key
space ThemerrDB tracks (verified at runtime via curl against the
live database branch).

Per-record shape mirrors the movies tree:
  - `id` (TMDB collection ID — int)
  - `name` (used as title; falls back from `title` which is absent)
  - `original_name` (vs `original_title` for movies)
  - `youtube_theme_url`, `youtube_theme_added`, `youtube_theme_edited`
  - NO `release_date` / `first_air_date` (collections aren't dated)
  - NO `imdb_id`

Phase 3 wires three sites:

  1. `_classify_git_path` — `movie_collections/themoviedb/<n>.json`
     returns ('collection', None, n).

  2. `_run_git_differential_upsert` — collections counted in
     `SyncStats.collections_seen`; same upsert/dedupe plumbing.

  3. `_detect_and_stamp_drops_git` + `_detect_and_stamp_drops_full_walk`
     — collections in the survival probe + drop-cap loop.

Also: main run_sync iteration adds the third media tuple
(("collection", "movie_collections")) so the full-walk path
(remote/snapshot transports) sees collections too.

Also: snapshot extract whitelist adds `movie_collections/` so
the tarball path doesn't drop those entries.

Also: `_plex_supplies_theme` and `_enqueue_download` handle the
new themes ↔ plex_items 'collection' alignment (no aliasing —
both tables use the same string value).
"""

from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from app.core.sync import (  # noqa: E402
    SyncStats, _classify_git_path, _upsert_theme,
)


# ── _classify_git_path ────────────────────────────────────────


def test_classify_recognizes_movie_collections_themoviedb_path():
    """The new path shape must classify as media_type='collection'
    with the TMDB collection ID as tmdb_id (and imdb_id=None,
    since collections have no IMDB IDs)."""
    out = _classify_git_path("movie_collections/themoviedb/1241.json")
    assert out == ("collection", None, 1241)


def test_classify_collections_handles_big_ids():
    """Some TMDB collection IDs are 6+ digits — int() should
    parse them without issue."""
    out = _classify_git_path("movie_collections/themoviedb/521226.json")
    assert out == ("collection", None, 521226)


def test_classify_collections_rejects_non_int_stem():
    assert _classify_git_path("movie_collections/themoviedb/abc.json") is None


def test_classify_collections_pages_json_is_ignored():
    """pages.json + all_page_*.json under movie_collections/ are
    Phase A's index-walk artifacts; the differential upsert path
    skips them (mirrors the movies/tv_shows behavior)."""
    assert _classify_git_path("movie_collections/pages.json") is None
    assert _classify_git_path("movie_collections/all_page_1.json") is None


def test_classify_other_movies_paths_still_work():
    """Sanity: the v1.18.0 widening doesn't break the original
    movie/tv classification paths."""
    assert _classify_git_path("movies/themoviedb/100.json") == ("movie", None, 100)
    assert _classify_git_path("tv_shows/themoviedb/200.json") == ("tv", None, 200)
    assert _classify_git_path("movies/imdb/tt0000001.json") == ("movie", "tt0000001", None)


# ── SyncStats has collections_seen ────────────────────────────


def test_sync_stats_has_collections_seen_field():
    """v1.18.0 added a third counter for collection-tree activity."""
    s = SyncStats()
    assert s.collections_seen == 0
    s.collections_seen = 5
    assert s.collections_seen == 5


# ── _upsert_theme handles the collection record shape ─────────


def _init_v55_db(tmp_path: Path) -> Path:
    from app.core.db import init_db
    db_path = tmp_path / "motif.db"
    init_db(db_path)
    return db_path


def test_upsert_collection_inserts_themes_row(tmp_path: Path):
    """End-to-end: feed a collection JSON record into _upsert_theme
    and confirm a themes row with media_type='collection' lands."""
    db_path = _init_v55_db(tmp_path)
    record = {
        "id": 1241,
        "name": "Harry Potter Collection",
        "original_name": "Harry Potter Collection",
        "youtube_theme_url": "https://www.youtube.com/watch?v=yB-c85V8Zsg",
        "youtube_theme_added": 1702337610,
        "youtube_theme_edited": 1702337610,
    }
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        is_new, url_changed, old_vid, _ = _upsert_theme(
            conn,
            media_type="collection",
            tmdb_id=1241,
            record=record,
            upstream_source="themoviedb",
            sync_ts="2026-05-19T12:00:00",
        )
        assert is_new is True
        assert url_changed is False
        row = conn.execute(
            "SELECT title, youtube_url, upstream_source, imdb_id, year "
            "FROM themes WHERE media_type='collection' AND tmdb_id=1241"
        ).fetchone()
    assert row is not None
    assert row["title"] == "Harry Potter Collection"
    assert row["youtube_url"] == "https://www.youtube.com/watch?v=yB-c85V8Zsg"
    assert row["upstream_source"] == "themoviedb"
    # Collections have no IMDB or year — both must be NULL.
    assert row["imdb_id"] is None
    assert row["year"] is None


def test_upsert_collection_idempotent_fingerprint(tmp_path: Path):
    """Second sync of the same collection record must skip the
    full upsert (fingerprint match) and just bump last_seen_sync_at —
    same fast-path as movies."""
    db_path = _init_v55_db(tmp_path)
    record = {
        "id": 1241,
        "name": "Harry Potter Collection",
        "youtube_theme_url": "https://www.youtube.com/watch?v=yB-c85V8Zsg",
    }
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        # First call: insert.
        _upsert_theme(
            conn, media_type="collection", tmdb_id=1241,
            record=record, upstream_source="themoviedb",
            sync_ts="2026-05-19T12:00:00",
        )
        # Second call with same record: fast-path skip.
        is_new, url_changed, _, _ = _upsert_theme(
            conn, media_type="collection", tmdb_id=1241,
            record=record, upstream_source="themoviedb",
            sync_ts="2026-05-19T13:00:00",
        )
    assert is_new is False
    assert url_changed is False


# ── Source-level pins ─────────────────────────────────────────


def test_sync_iteration_includes_collection_tuple():
    """The main run_sync media_iter at the full-walk path must
    include the third ("collection", "movie_collections") tuple
    so remote/snapshot syncs walk the collections tree."""
    src = (REPO / "app" / "core" / "sync.py").read_text()
    # Both 3-tuple shape and the collection path must be present
    # together (the iteration is the only call site).
    assert '("collection", "movie_collections")' in src


def test_snapshot_whitelist_includes_movie_collections():
    """The snapshot tarball-extract whitelist must include
    `movie_collections` so the collection JSONs survive the
    paranoid filter."""
    src = (REPO / "app" / "core" / "sync.py").read_text()
    # The whitelist tuple is in the snapshot's _extract method.
    # Pin the exact 3-element shape so a regression to the old
    # 2-element ("movies", "tv_shows") drops collections silently.
    assert '("movies", "tv_shows", "movie_collections")' in src


def test_plex_supplies_theme_handles_collection_media_type():
    """_plex_supplies_theme must map media_type='collection' to
    plex_items.media_type='collection' (no aliasing — themes
    and plex_items align on this value post-v1.18.0)."""
    src = (REPO / "app" / "core" / "sync.py").read_text()
    # The function must branch on 'collection' explicitly.
    # We assert the alias dispatch is present after the function
    # definition by anchoring on a representative string in the
    # function's body.
    fn_start = src.index("def _plex_supplies_theme(")
    fn_end = src.index("\ndef ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    assert 'plex_type = "collection"' in fn_body, (
        "v1.18.0: _plex_supplies_theme must dispatch "
        "media_type='collection' to plex_type='collection' "
        "(themes ↔ plex_items align — no aliasing applied)."
    )


def test_enqueue_download_handles_collection_media_type():
    """_enqueue_download must dispatch 'collection' to plex_type
    'collection' so the section-aware lookup finds collection
    rows in plex_items (pi.media_type='collection')."""
    src = (REPO / "app" / "core" / "sync.py").read_text()
    fn_start = src.index("def _enqueue_download(")
    fn_end = src.index("\ndef ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    assert 'plex_type = "collection"' in fn_body


def test_run_sync_done_summary_items_includes_collections():
    """v1.21.26: the op_progress done_summary's `items` value must fold in
    collections (movies+tv+collections) so the ops drawer's Done headline
    reflects the full library — matching the sync_completed notification's
    total_seen. done_summary is now a [{l,v}] list (v1.21.25); pre-v1.21.25
    the drawer summed only movies+tv, dropping collections.

    NOTE: this replaces test_run_sync_done_summary_emits_collections_seen,
    which had become a phantom guard — its pinned string survived in an
    unrelated log_event detail at sync.py:3733, not the done_summary it
    claimed to protect."""
    src = (REPO / "app" / "core" / "sync.py").read_text()
    assert '{"l": "items", "v": stats.movies_seen + stats.tv_seen' in src
    assert "+ stats.collections_seen}" in src


def test_detect_drops_git_recognizes_collection_media_path():
    """_detect_and_stamp_drops_git's survival probe must look
    under movie_collections/themoviedb/ when checking whether a
    'collection' item still exists in the new tree."""
    src = (REPO / "app" / "core" / "sync.py").read_text()
    fn_start = src.index("def _detect_and_stamp_drops_git(")
    fn_end = src.index("\ndef ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    assert 'media_path = "movie_collections"' in fn_body, (
        "v1.18.0: drop detection must probe "
        "movie_collections/themoviedb/<id>.json for collection rows."
    )
