"""v1.23.27 — _GitMirror.read_json memoizes shared directory trees.

the user's report: a ThemerrDB sync with a huge git diff (10,420 changed paths
from an upstream mass re-process) was extremely slow — the apply phase crawled
at ~16 paths/sec. Cause: read_json re-resolved each changed path from the root
tree, and dulwich does NOT cache decompressed objects, so the big shared
directory tree (themoviedb/movie ≈ thousands of entries) was decompressed once
PER changed path. For N paths under one dir that's N re-decompresses of the same
tree — O(N × tree_size) instead of O(N + tree_size).

Fix: a per-run _tree_cache memoizes TREE objects by sha (blobs are leaf data
read once per path, so they're not cached). Heads are fixed after acquire(), so
no invalidation. A synthetic 4,000-entry tree benchmarked ~43× faster cached.
"""
from __future__ import annotations

import json

import pytest

from app.core.sync import _GitMirror


def _build_repo(tmp_path, n=40):
    Repo = pytest.importorskip("dulwich.repo").Repo
    from dulwich.objects import Blob, Tree, Commit
    repo = Repo.init_bare(str(tmp_path))
    store = repo.object_store
    movie = Tree()
    for i in range(n):
        b = Blob.from_string(
            json.dumps({"id": i, "youtube_theme_url": f"https://y/{i}"}).encode())
        store.add_object(b)
        movie.add(f"{i}.json".encode(), 0o100644, b.id)
    store.add_object(movie)
    tmdb = Tree(); tmdb.add(b"movie", 0o040000, movie.id); store.add_object(tmdb)
    root = Tree(); root.add(b"themoviedb", 0o040000, tmdb.id); store.add_object(root)
    c = Commit()
    c.tree = root.id
    c.author = c.committer = b"t <t@t>"
    c.author_time = c.commit_time = 1000
    c.author_timezone = c.commit_timezone = 0
    c.message = b"x"
    store.add_object(c)
    m = _GitMirror.__new__(_GitMirror)
    m._repo = repo
    m._new_head = c.id
    m._tree_cache = {}
    return m


def test_read_json_returns_correct_records(tmp_path):
    m = _build_repo(tmp_path, n=20)
    for i in range(20):
        rec = m.read_json(f"themoviedb/movie/{i}.json")
        assert rec is not None and rec["id"] == i


def test_missing_path_returns_none(tmp_path):
    m = _build_repo(tmp_path, n=5)
    assert m.read_json("themoviedb/movie/9999.json") is None
    assert m.read_json("nope/at/all.json") is None
    # a directory (not a blob) is not a record
    assert m.read_json("themoviedb/movie") is None


def test_trees_cached_blobs_not(tmp_path):
    m = _build_repo(tmp_path, n=10)
    for i in range(10):
        m.read_json(f"themoviedb/movie/{i}.json")
    # exactly the 3 shared trees (root, themoviedb, movie) are memoized —
    # NOT the 10 leaf blobs (those would bloat memory with zero reuse).
    assert len(m._tree_cache) == 3


def test_cache_reused_across_calls(tmp_path):
    # reading a second path under the same dir must not grow the cache —
    # the big movie tree is decompressed once, then served from cache.
    m = _build_repo(tmp_path, n=10)
    m.read_json("themoviedb/movie/0.json")
    after_first = dict(m._tree_cache)
    m.read_json("themoviedb/movie/1.json")
    assert m._tree_cache.keys() == after_first.keys()
