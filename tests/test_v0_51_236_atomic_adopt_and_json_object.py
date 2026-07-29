"""v0.51.236 — autocommit multi-writes made atomic + read_json object guard.

get_conn is isolation_level=None (autocommit), so a bare `with get_conn(...)`
block commits every statement independently. Two adopt paths did several
dependent writes that way; a failure partway left the destructive half applied.

_GitMirror.read_json is typed dict|None but returned whatever json.loads gave —
a valid-JSON array/string came back as a list/str and every consumer calls
record.get(...), so an AttributeError escaped the apply loop (nothing catches
it there) and aborted the whole sync.
"""
from __future__ import annotations

import json
import sqlite3
import tempfile
from pathlib import Path

import pytest

from app.core.db import get_conn, init_db


# ── read_json must only ever yield a dict ────────────────────────────────

def _read_json_with(monkeypatch, raw: bytes):
    """Drive read_json over a REAL dulwich tree chain, so the walk actually
    reaches the blob. (A fake that isn't a dulwich Tree bails at the first
    isinstance check and returns None for the wrong reason — every non-object
    case would then pass vacuously.)"""
    from dulwich.objects import Blob, Tree
    from app.core.sync import _GitMirror

    blob = Blob.from_string(raw)
    leaf = Tree()
    leaf.add(b"603.json", 0o100644, blob.id)
    mid = Tree()
    mid.add(b"themoviedb", 0o040000, leaf.id)
    root = Tree()
    root.add(b"movies", 0o040000, mid.id)
    store = {root.id: root, mid.id: mid, leaf.id: leaf, blob.id: blob}

    m = _GitMirror.__new__(_GitMirror)
    m._repo = {"HEAD": type("C", (), {"tree": root.id})()}
    m._new_head = "HEAD"
    monkeypatch.setattr(_GitMirror, "_cached_obj",
                        lambda self, sha: store[sha], raising=True)
    return _GitMirror.read_json(m, "movies/themoviedb/603.json")


@pytest.mark.parametrize("raw", [
    b'[{"id": 603}]',        # a JSON array — .get() would AttributeError
    b'"just a string"',
    b'42',
    b'null',
    b'true',
])
def test_non_object_json_returns_none_instead_of_a_non_dict(monkeypatch, raw):
    got = _read_json_with(monkeypatch, raw)
    assert got is None, f"{raw!r} must not escape as {type(got).__name__}"


def test_a_real_object_still_parses(monkeypatch):
    got = _read_json_with(monkeypatch, b'{"id": 603, "title": "Dune"}')
    assert got == {"id": 603, "title": "Dune"}


# ── the adopt writes are all-or-nothing ──────────────────────────────────

def _seed(db: Path) -> None:
    init_db(db)
    TS = "2026-01-01T00:00:00+00:00"
    with get_conn(db) as c:
        c.execute("INSERT INTO themes (media_type, tmdb_id, title, youtube_url, "
                  "upstream_source, last_seen_sync_at, first_seen_sync_at) "
                  "VALUES ('movie', 603, 'Dune', 'https://youtu.be/tdb', "
                  "'themoviedb', ?, ?)", (TS, TS))
        c.execute("INSERT INTO plex_sections (section_id, title, type, "
                  "included, discovered_at, last_seen_at) "
                  "VALUES ('1', 'Movies', 'movie', 1, ?, ?)", (TS, TS))
        c.execute("INSERT INTO user_overrides (media_type, tmdb_id, section_id, "
                  "edition_key, youtube_url, set_at, set_by) VALUES "
                  "('movie', 603, '1', '', 'https://youtu.be/mine', ?, 'admin')",
                  (TS,))


def test_replace_with_themerrdb_rolls_back_the_override_delete_on_failure():
    """THE case: the override DELETE commits, then the download INSERT fails.
    Pre-fix the user lost their manual URL and got no replacement download."""
    from app.core import adopt

    db = Path(tempfile.mkdtemp()) / "m.db"
    _seed(db)

    # Fail ONLY the download-job INSERT, after the cancel/capture/delete have
    # run — a real SQLite abort, not a patched driver (Connection is immutable).
    with get_conn(db) as c:
        c.execute("CREATE TRIGGER boom BEFORE INSERT ON jobs "
                  "WHEN NEW.job_type = 'download' "
                  "BEGIN SELECT RAISE(ABORT, 'simulated write failure'); END")

    with pytest.raises(sqlite3.IntegrityError):
        adopt.replace_with_themerrdb(
            db, media_type="movie", tmdb_id=603,
            decided_by="admin", section_id="1")

    with get_conn(db) as c:
        c.execute("DROP TRIGGER boom")

    with get_conn(db) as c:
        ovr = c.execute(
            "SELECT youtube_url FROM user_overrides "
            "WHERE media_type='movie' AND tmdb_id=603").fetchone()
        prev = c.execute("SELECT COUNT(*) n FROM previous_urls").fetchone()["n"]
    assert ovr is not None and ovr["youtube_url"] == "https://youtu.be/mine", (
        "the override DELETE must roll back when the download INSERT fails")
    assert prev == 0, "the previous_urls capture must roll back too"


def test_replace_with_themerrdb_still_works_end_to_end():
    """The rollback must not have broken the happy path."""
    from app.core import adopt

    db = Path(tempfile.mkdtemp()) / "m.db"
    _seed(db)
    out = adopt.replace_with_themerrdb(
        db, media_type="movie", tmdb_id=603, decided_by="admin", section_id="1")

    assert out["job_ids"] and out["section_ids"] == ["1"]
    with get_conn(db) as c:
        assert c.execute(
            "SELECT COUNT(*) n FROM user_overrides").fetchone()["n"] == 0, \
            "the override is still dropped on success"
        assert c.execute(
            "SELECT youtube_url FROM previous_urls").fetchone()["youtube_url"] \
            == "https://youtu.be/mine", "and captured for REVERT"
        job = c.execute("SELECT job_type, status, payload FROM jobs").fetchone()
    assert job["job_type"] == "download" and job["status"] == "pending"
    assert json.loads(job["payload"])["force_place"] is True


def test_both_adopt_paths_hold_one_transaction():
    import inspect
    from app.core import adopt

    for fn in (adopt.replace_with_themerrdb, adopt._maybe_restore_url_history):
        src = inspect.getsource(fn)
        assert "with get_conn(db_path) as conn, transaction(conn):" in src, (
            f"{fn.__name__} does dependent multi-writes under autocommit")
