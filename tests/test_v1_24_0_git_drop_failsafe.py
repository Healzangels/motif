"""v1.24.0 (.99 line-close rollover) — fail-safe git drop detection + per-phase labels.

Two findings from the v1.23.x silent-failure sweep, fixed pre-cut.

Finding #1 (MED, data-loss class): _detect_and_stamp_drops_git probed survivorship
with `read_json(path) is not None`. read_json returns None for BOTH a missing tree
path AND a present-but-MALFORMED blob (the _GIT_MIRROR_READ_JSON_WARNED branch). So a
ThemerrDB entry whose themoviedb/<id>.json blob is momentarily corrupt read as "gone"
→ a false tdb_dropped_at stamp + pending_update deletion. commit_sync_ok advances the
git baseline immediately after, so the next run diffs from the NEW head and never
re-sees the removal — it does not self-correct. New path_exists_in_tree() probes blob
EXISTENCE regardless of JSON validity; a corrupt-but-present survivor now counts as
present and is never stamped dropped (fails SAFE).

Finding #4 (LOW, class-9 mislabel): the single broad `except: "drop detection failed"`
wrapped detection + commit_sync_ok (baseline advance) + _compact_if_oversized
(compaction) + log_event. A baseline-advance or compaction failure was mislabeled a
detection failure. Split into per-phase try/except with accurate labels; the v1.22.74
ordering (advance ONLY after detection succeeds) and the v1.23.80 "compact only after
the HEAD is retired" gate are preserved (the commit try's else-branch).
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from app.core.db import init_db
from app.core.sync import _ChangeSet, _GitMirror, _detect_and_stamp_drops_git

REPO = Path(__file__).resolve().parent.parent
SYNC_PY = (REPO / "app" / "core" / "sync.py").read_text()
SYNC_TS = "2026-06-19T00:00:00+00:00"


# ── fixture: a bare mirror whose new HEAD tree holds verbatim blobs ──

def _mirror_with_tree(tmp_path, blobs: dict[str, bytes]) -> _GitMirror:
    """Build a bare repo whose new HEAD tree holds `blobs` (top/kind/leaf.json →
    raw bytes, verbatim so a caller can plant a malformed-JSON blob), and an
    unparented _GitMirror pointed at it."""
    Repo = pytest.importorskip("dulwich.repo").Repo
    from dulwich.objects import Blob, Commit, Tree
    git_dir = tmp_path / "mirror.git"
    git_dir.mkdir(exist_ok=True)
    repo = Repo.init_bare(str(git_dir))
    store = repo.object_store
    kinds: dict[tuple[str, str], Tree] = {}
    for rel, raw in blobs.items():
        top, kind, leaf = rel.split("/")
        b = Blob.from_string(raw)
        store.add_object(b)
        kinds.setdefault((top, kind), Tree()).add(leaf.encode(), 0o100644, b.id)
    tops: dict[str, Tree] = {}
    for (top, kind), t in kinds.items():
        store.add_object(t)
        tops.setdefault(top, Tree()).add(kind.encode(), 0o040000, t.id)
    root = Tree()
    for top, t in tops.items():
        store.add_object(t)
        root.add(top.encode(), 0o040000, t.id)
    store.add_object(root)
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


def _seed_movie(db, *, tmdb_id, imdb_id):
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, imdb_id, title,"
            " upstream_source, last_seen_sync_at, first_seen_sync_at)"
            " VALUES ('movie', ?, ?, 'M', 'imdb', 't', 't')",
            (tmdb_id, imdb_id))
        conn.commit()


# ── Finding #1 — path_exists_in_tree: present (valid OR corrupt) vs absent ──

def test_path_exists_in_tree_present_corrupt_and_missing(tmp_path):
    m = _mirror_with_tree(tmp_path, {
        "movies/themoviedb/500.json": json.dumps({"id": 500}).encode(),
        "movies/themoviedb/501.json": b"{ this is NOT valid json",  # corrupt
    })
    assert m.path_exists_in_tree("movies/themoviedb/500.json") is True
    # The corrupt blob is STILL present — the whole point. read_json can't tell
    # it apart from missing (both None); path_exists_in_tree can.
    assert m.path_exists_in_tree("movies/themoviedb/501.json") is True
    assert m.read_json("movies/themoviedb/501.json") is None
    # genuinely missing leaf, missing dir, and a directory node: all absent.
    assert m.path_exists_in_tree("movies/themoviedb/999.json") is False
    assert m.path_exists_in_tree("nope/at/all.json") is False
    assert m.path_exists_in_tree("movies/themoviedb") is False


def test_corrupt_survivor_is_not_stamped_dropped(tmp_path):
    db = tmp_path / "motif.db"
    init_db(db)
    _seed_movie(db, tmdb_id=501, imdb_id="tt0000501")
    # The imdb path was removed upstream, but themoviedb/501.json still EXISTS —
    # just momentarily malformed. Pre-fix this read as "gone" → false drop.
    m = _mirror_with_tree(tmp_path, {
        "movies/themoviedb/501.json": b"{ corrupt-but-present",
    })
    m._changeset_cache = _ChangeSet(removed=["movies/imdb/tt0000501.json"])
    n = _detect_and_stamp_drops_git(db, m, sync_ts=SYNC_TS)
    assert n == 0, "a present-but-corrupt survivor must NOT be stamped dropped"
    with sqlite3.connect(db) as conn:
        dropped = conn.execute(
            "SELECT tdb_dropped_at FROM themes WHERE tmdb_id=501").fetchone()[0]
    assert dropped is None


def test_genuine_drop_still_stamps(tmp_path):
    # Guard against over-correction: a truly-gone item (no themoviedb blob,
    # imdb path removed) must STILL be stamped dropped.
    db = tmp_path / "motif.db"
    init_db(db)
    _seed_movie(db, tmdb_id=502, imdb_id="tt0000502")
    # new tree holds only an UNRELATED blob — 502 is genuinely absent.
    m = _mirror_with_tree(tmp_path, {
        "movies/themoviedb/777.json": json.dumps({"id": 777}).encode(),
    })
    m._changeset_cache = _ChangeSet(removed=["movies/imdb/tt0000502.json"])
    n = _detect_and_stamp_drops_git(db, m, sync_ts=SYNC_TS)
    assert n == 1
    with sqlite3.connect(db) as conn:
        dropped = conn.execute(
            "SELECT tdb_dropped_at FROM themes WHERE tmdb_id=502").fetchone()[0]
    assert dropped == SYNC_TS


def test_survivor_probe_uses_existence_not_read_json():
    # the probe must not silently regress back to read_json (which conflates
    # corrupt-with-missing — the exact data-loss bug v1.24.0 fixed).
    i = SYNC_PY.index("survives_tmdb = ")
    block = SYNC_PY[i:i + 400]
    assert "git_mirror.path_exists_in_tree(" in block
    assert "read_json" not in block, (
        "v1.24.0: survivor probe must use path_exists_in_tree, not read_json")


# ── Finding #4 — per-phase labels + preserved ordering gates ──

def test_drop_detection_phases_have_distinct_labels():
    i = SYNC_PY.index("n_dropped = _detect_and_stamp_drops_git(")
    # v1.24.14 widened 4200→5400: the read-failure baseline-hold breadcrumb
    # pushed the later phase labels down a few lines.
    block = SYNC_PY[i:i + 5400]
    assert 'log.warning("drop detection failed' in block
    assert 'log.warning("git baseline advance failed' in block
    assert 'log.warning("mirror compaction failed' in block
    assert 'log.warning("drop-event logging failed' in block


def test_compaction_gated_on_commit_success():
    # v1.23.80 safety: compaction only after commit_sync_ok retired the HEAD —
    # now the else-branch of the commit try, so a failed advance skips it.
    # v1.24.14: the gate gained `and stats.errors == 0` (read-failure hold).
    i = SYNC_PY.index("if ran_git_diff and detection_ok and stats.errors == 0:")
    block = SYNC_PY[i:i + 1400]
    commit = block.index("git_mirror.commit_sync_ok()")
    compact = block.index("git_mirror._compact_if_oversized()")
    assert commit < compact


def test_v1_24_0_rollover_marker_present():
    # Stable archaeology pin (not a current-version pin — that lives in
    # test_v1_13_79): proves the v1.24.0 rollover narrative shipped + survives
    # later bumps.
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "v1.24.0: rolled over at the v1.23.x .99 line-close" in init_py
