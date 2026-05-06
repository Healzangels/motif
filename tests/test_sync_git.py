"""Phase B regression tests — _GitMirror clone/fetch/diff against a
local fixture git repo, plus _classify_git_path, _ChangeSet, and
config-validation glue.

Strategy: build commits manually via the dulwich object-store API
(skips the worktree.commit() path that needs gpg/ssh-keygen vendor
detection, which fails in the minimal test env). The resulting
"remote" bare repo is byte-identical to one made by `git init --bare`
+ commits; dulwich.porcelain.clone / fetch operate on it the same way
they would against github.com.
"""
from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from dulwich.repo import Repo
from dulwich.objects import Tree, Blob, Commit


# ---------- helpers ----------------------------------------------------------

def _make_commit(repo: Repo, files: dict[bytes, bytes], message: bytes,
                 parents: list[bytes] | None = None) -> bytes:
    """Build trees + a commit by hand and add to repo's object store.
    Returns the commit SHA. Files keys are repo-relative paths."""
    tree_by_dir: dict[bytes, dict[bytes, tuple[str, bytes | bytes]]] = {}
    for path, content in files.items():
        parts = path.split(b"/")
        cur = b""
        for p in parts[:-1]:
            parent = cur
            cur = (cur + b"/" + p) if cur else p
            tree_by_dir.setdefault(parent, {}).setdefault(p, ("tree", cur))
        tree_by_dir.setdefault(b"/".join(parts[:-1]), {})[parts[-1]] = ("blob", content)

    def build_tree(prefix: bytes) -> Tree:
        t = Tree()
        for name, (kind, val) in sorted(tree_by_dir.get(prefix, {}).items()):
            if kind == "blob":
                blob = Blob.from_string(val)
                repo.object_store.add_object(blob)
                t.add(name, 0o100644, blob.id)
            else:
                child = build_tree(val)
                t.add(name, 0o040000, child.id)
        repo.object_store.add_object(t)
        return t

    root = build_tree(b"")
    c = Commit()
    c.tree = root.id
    c.author = c.committer = b"motif-test <test@motif>"
    c.author_time = c.commit_time = int(time.time())
    c.author_timezone = c.commit_timezone = 0
    c.message = message
    if parents:
        c.parents = parents
    repo.object_store.add_object(c)
    return c.id


_V1_FILES = {
    b"movies/pages.json": b'{"pages":1,"count":1}',
    b"movies/imdb/tt0000001.json": json.dumps(
        {"id": 100, "imdb_id": "tt0000001",
         "title": "Test Movie 1",
         "youtube_theme_url": "https://youtu.be/aaa"}
    ).encode(),
    b"movies/themoviedb/100.json": json.dumps(
        {"id": 100, "imdb_id": "tt0000001",
         "title": "Test Movie 1",
         "youtube_theme_url": "https://youtu.be/aaa"}
    ).encode(),
    b"tv_shows/pages.json": b'{"pages":1,"count":0}',
}

_V2_FILES = {
    b"movies/pages.json": b'{"pages":1,"count":2}',
    b"movies/imdb/tt0000001.json": json.dumps(
        {"id": 100, "imdb_id": "tt0000001",
         "title": "Test Movie 1",
         "youtube_theme_url": "https://youtu.be/aaa-CHANGED"}
    ).encode(),
    b"movies/themoviedb/100.json": json.dumps(
        {"id": 100, "imdb_id": "tt0000001",
         "title": "Test Movie 1",
         "youtube_theme_url": "https://youtu.be/aaa-CHANGED"}
    ).encode(),
    b"movies/imdb/tt0000002.json": json.dumps(
        {"id": 200, "imdb_id": "tt0000002",
         "title": "Test Movie 2",
         "youtube_theme_url": "https://youtu.be/bbb"}
    ).encode(),
    b"movies/themoviedb/200.json": json.dumps(
        {"id": 200, "imdb_id": "tt0000002",
         "title": "Test Movie 2",
         "youtube_theme_url": "https://youtu.be/bbb"}
    ).encode(),
    b"tv_shows/pages.json": b'{"pages":1,"count":0}',
}


def _init_remote_at_v1(tmpdir: Path) -> tuple[Path, Repo, bytes]:
    """Build a bare 'remote' repo with ONLY v1. Returns (path, repo, sha1).
    Real-world Phase B clones at the current HEAD; v2 lands later via
    _advance_remote_to_v2 and the next sync sees the diff."""
    remote = tmpdir / "remote.git"
    remote.mkdir()
    rrepo = Repo.init_bare(str(remote))
    sha1 = _make_commit(rrepo, _V1_FILES, message=b"v1")
    rrepo.refs[b"refs/heads/database"] = sha1
    rrepo.refs[b"HEAD"] = b"ref: refs/heads/database"
    return remote, rrepo, sha1


def _advance_remote_to_v2(rrepo: Repo, sha1: bytes) -> bytes:
    sha2 = _make_commit(rrepo, _V2_FILES, message=b"v2", parents=[sha1])
    rrepo.refs[b"refs/heads/database"] = sha2
    return sha2


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    """An init'd DB so op_progress.start_progress / update_progress
    don't blow up inside _GitMirror's progress emits."""
    from app.core.db import init_db
    path = tmp_path / "motif.db"
    init_db(path)
    return path


# ---------- happy path -------------------------------------------------------

def _start_op(db_path: Path):
    from app.core import progress as op_progress
    op_progress.start_progress(
        db_path, op_id="tdb_sync", kind="tdb_sync",
        stage="git_fetch", stage_label="…",
    )


def _new_mirror(db_path: Path, remote_path: Path):
    from app.core.sync import _GitMirror
    return _GitMirror(
        db_path, "tdb_sync",
        f"file://{remote_path}", "database",
        cancel_check=lambda: False,
    )


def test_git_mirror_first_run_clones_and_walks_tree(tmp_path: Path, db_path: Path):
    remote, _rrepo, sha1 = _init_remote_at_v1(tmp_path)
    _start_op(db_path)
    mirror = _new_mirror(db_path, remote)
    try:
        mirror.acquire(None)
        # First run: no MOTIF_LAST_SYNC, no diff baseline. is_unchanged()
        # is False (it's a fresh state, not "no new commits").
        assert mirror.is_unchanged() is False
        assert mirror._new_head == sha1
        assert mirror._old_head is None
        # list_changes on first run = the full tree.
        changes = mirror.list_changes()
        assert len(changes.added) >= 3   # movies/pages, imdb/tt001, themoviedb/100, tv_shows/pages
        assert any("movies/imdb/tt0000001.json" in p for p in changes.added)
        # read_json works
        rec = mirror.read_json("movies/imdb/tt0000001.json")
        assert rec is not None
        assert rec.get("youtube_theme_url") == "https://youtu.be/aaa"
    finally:
        mirror.release()


def test_git_mirror_subsequent_run_diffs_only(tmp_path: Path, db_path: Path):
    """Realistic timeline: clone at v1 (only sha1 is in the local
    object store at depth=1). Then v2 lands on the remote. Next
    sync fetches the delta and computes sha1..sha2."""
    remote, rrepo, sha1 = _init_remote_at_v1(tmp_path)
    # Stage 1: clone at v1 + stamp MOTIF_LAST_SYNC = sha1.
    _start_op(db_path)
    mirror = _new_mirror(db_path, remote)
    try:
        mirror.acquire(None)
        assert mirror._new_head == sha1
        mirror.commit_sync_ok()  # writes sha1 to MOTIF_LAST_SYNC
    finally:
        mirror.release()

    # Now v2 lands on the remote.
    sha2 = _advance_remote_to_v2(rrepo, sha1)

    # Stage 2: re-acquire. Should fetch, see old=sha1 → new=sha2, diff.
    _start_op(db_path)
    mirror = _new_mirror(db_path, remote)
    try:
        mirror.acquire(None)
        assert mirror.is_unchanged() is False
        assert mirror._old_head == sha1
        assert mirror._new_head == sha2
        changes = mirror.list_changes()
        added_paths = set(changes.added)
        modified_paths = set(changes.modified)
        # tt002 is genuinely new in v2.
        assert "movies/imdb/tt0000002.json" in added_paths
        # tt001's URL changed, so it should appear as modified.
        assert "movies/imdb/tt0000001.json" in modified_paths
        # changed = adds + mods (not removes).
        assert "movies/imdb/tt0000002.json" in changes.changed
        # New content reads from new HEAD.
        rec_new = mirror.read_json("movies/imdb/tt0000001.json")
        assert rec_new["youtube_theme_url"] == "https://youtu.be/aaa-CHANGED"
    finally:
        mirror.release()


def test_git_mirror_unchanged_when_no_new_commits(tmp_path: Path, db_path: Path):
    remote, _rrepo, sha1 = _init_remote_at_v1(tmp_path)
    # Clone + stamp MOTIF_LAST_SYNC = sha1. Then re-acquire without
    # advancing the remote: no new commits, should be unchanged.
    _start_op(db_path)
    mirror = _new_mirror(db_path, remote)
    try:
        mirror.acquire(None)
        mirror.commit_sync_ok()
    finally:
        mirror.release()

    _start_op(db_path)
    mirror = _new_mirror(db_path, remote)
    try:
        mirror.acquire(None)
        assert mirror.is_unchanged() is True
    finally:
        mirror.release()


def test_git_mirror_corrupt_last_sync_treats_as_first_run(tmp_path: Path,
                                                          db_path: Path):
    """A MOTIF_LAST_SYNC pointing to a SHA not in the repo's object
    store must NOT KeyError when list_changes runs — the helper should
    detect the corruption and treat the next acquire as a fresh
    baseline (or fall back to the local ref)."""
    remote, _rrepo, sha1 = _init_remote_at_v1(tmp_path)
    _start_op(db_path)
    mirror = _new_mirror(db_path, remote)
    try:
        mirror.acquire(None)
        # Write a SHA that isn't in the repo.
        mirror.last_sync_path.write_text("0" * 40)
    finally:
        mirror.release()

    _start_op(db_path)
    mirror = _new_mirror(db_path, remote)
    try:
        mirror.acquire(None)
        # list_changes shouldn't crash even though MOTIF_LAST_SYNC was
        # corrupt; the helper either treats as first-run (old=None) or
        # uses the local refs/heads/database fallback.
        changes = mirror.list_changes()
        assert isinstance(changes.added, list)
        assert isinstance(changes.modified, list)
    finally:
        mirror.release()


# ---------- _classify_git_path -----------------------------------------------

def test_classify_git_path_recognizes_per_item_jsons():
    from app.core.sync import _classify_git_path
    assert _classify_git_path("movies/imdb/tt0000001.json") == ("movie", "tt0000001", None)
    assert _classify_git_path("movies/themoviedb/100.json") == ("movie", None, 100)
    assert _classify_git_path("tv_shows/imdb/tt0000002.json") == ("tv", "tt0000002", None)
    assert _classify_git_path("tv_shows/themoviedb/200.json") == ("tv", None, 200)


def test_classify_git_path_skips_non_item_paths():
    from app.core.sync import _classify_git_path
    assert _classify_git_path("movies/pages.json") is None
    assert _classify_git_path("movies/all_page_1.json") is None
    assert _classify_git_path("README.md") is None
    assert _classify_git_path(".github/workflows/ci.yml") is None
    # Wrong-shape path shouldn't crash.
    assert _classify_git_path("movies/imdb/not_an_int.txt") is None
    assert _classify_git_path("movies/themoviedb/abc.json") is None


# ---------- _ChangeSet -------------------------------------------------------

def test_changeset_aggregations():
    from app.core.sync import _ChangeSet
    cs = _ChangeSet(added=["a", "b"], modified=["c"], removed=["d"])
    assert cs.changed == ["a", "b", "c"]  # adds + mods, not removes
    assert cs.total == 4
    assert cs.is_empty is False
    assert _ChangeSet().is_empty is True


# ---------- config validation ------------------------------------------------

def test_config_accepts_sync_source_git():
    from app.core.config_file import MotifConfig, validate
    cfg = MotifConfig()
    cfg.sync.source = "git"
    errors = [e for e in validate(cfg, require_themes_dir=False)
              if "themes_dir" not in e]
    assert errors == []


def test_config_defaults_for_git_settings():
    from app.core.config_file import MotifConfig
    cfg = MotifConfig()
    assert cfg.sync.git_url == "https://github.com/LizardByte/ThemerrDB.git"
    assert cfg.sync.git_branch == "database"
