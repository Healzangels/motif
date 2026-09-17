"""v0.51.344 integration review, canonical writers: R1-F3, R1-F16, R1-F17 and R1-F19.

  F3  the incoming stamp and its write-back go through transaction()'s lock ladder like the sibling writers, and a
      write-back the ladder still loses is logged and skipped — the run goes on and a re-run lands the bytes.
  F16 link_failed / write_failed reasons word the errno, never the path (that stays in the log); a Plex-folder copy
      motif cannot read is refused the same way (the PB-048 decision, re-pinned beside its wording).
  F17 a read-back that fails after a successful publish stamps the length and sha of the bytes just published, not
      the pre-restore values back over PB-048's incoming stamp; the next CHECK lists nothing.
  F19 one local_files.file_path (or plex_items.folder_path) no OS call accepts skips only its own directory of the
      nightly temp sweep, at debug, and the job still reports its count.
"""
from __future__ import annotations

import errno
import logging
import os
import sqlite3
import threading
from pathlib import Path

import pytest

from app.core import canonical_health as ch
from app.core import db as dbmod
from app.core import plex_enum
from app.core import scheduler
from app.core.db import get_conn
from app.core.plex_enum import sweep_stale_placement_temps
from test_v0_51_339_canonical_health_restore import _NORM_COLS
from test_v0_51_344_canonical_temp_sweep import _db, _forward, _lf, _plex_item
from test_v0_51_344_torn_publish import (FAILED, OTHER_SIZE, RECORDED, SAME_SIZE, TMDB, Plex, _on_the_move, _row,
                                          _seed, _sha)

LEGS = pytest.mark.parametrize("leg", ["store", "sidecar"])


class _WriterLock:
    """Another connection holding the write lock — the sync / plex_enum writer db.transaction's docstring names."""

    def __init__(self, db):
        self.db = db
        self.held, self.release, self.released = threading.Event(), threading.Event(), threading.Event()
        self.t = threading.Thread(target=self._hold, daemon=True)

    def _hold(self):
        c = sqlite3.connect(self.db, isolation_level=None)
        try:
            c.execute("BEGIN IMMEDIATE")
            self.held.set()
            self.release.wait(30)
            c.execute("ROLLBACK")
        finally:
            c.close()
            self.released.set()

    def hold(self):
        self.t.start()
        assert self.held.wait(10), "premise: the holder never took the write lock"

    def let_go(self):
        self.release.set()
        if self.t.ident is not None:
            self.t.join(30)


class _NoWaitConn:
    """The run's connection with no busy wait; the first BEGIN IMMEDIATE the lock refuses lets the holder go."""

    def __init__(self, db, holder):
        self.real = sqlite3.connect(db, timeout=0, isolation_level=None)
        self.real.row_factory = sqlite3.Row
        self.real.execute("PRAGMA foreign_keys = ON")
        self.real.execute("PRAGMA busy_timeout = 0")
        self.holder, self.refused = holder, []

    def execute(self, sql, *a):
        try:
            return self.real.execute(sql, *a)
        except sqlite3.OperationalError as e:
            self.refused.append(sql)
            if sql == "BEGIN IMMEDIATE" and "locked" in str(e) and len(self.refused) == 1:
                self.holder.let_go()
                assert self.holder.released.wait(10), "premise: the holder never let the lock go"
            raise

    def __getattr__(self, name):
        return getattr(self.real, name)


def _candidate(db):
    with sqlite3.connect(db) as c:
        return c.execute("SELECT canonical_changed_candidate FROM local_files WHERE tmdb_id = ?", (TMDB,)).fetchone()[0]


# ── F3: the incoming stamp waits like its siblings; a lost write-back is a skip, not a failed run ──

def test_the_incoming_stamp_waits_through_the_lock_ladder_like_its_sibling_writers(tmp_path, monkeypatch):
    db, themes, canonical, _anchors = _seed(tmp_path, leg="sidecar", copy=SAME_SIZE)
    monkeypatch.setattr(dbmod, "LOCK_RETRY_DELAYS", (0.0,))  # the ladder's second rung, without its sleep
    with get_conn(db) as c:
        r = ch._broken_rows_with_placement(c)[0]
    holder = _WriterLock(db)
    holder.hold()
    conn = _NoWaitConn(db, holder)
    try:
        res = ch.restore_from_placement(db, themes, r, conn=conn)
    finally:
        conn.close()
        holder.let_go()
    assert conn.refused == ["BEGIN IMMEDIATE"], f"premise: the held lock refused the stamp's first BEGIN: {conn.refused}"
    assert res == {"ok": True, "kind": "hardlink"}
    row = _row(db)
    assert (row["canonical_present"], row["file_size"], row["file_sha256"]) == (1, len(SAME_SIZE), _sha(SAME_SIZE))


@LEGS
def test_the_write_back_after_a_failed_move_waits_through_the_lock_ladder_too(tmp_path, monkeypatch, leg):
    db, themes, canonical, _anchors = _seed(tmp_path, leg=leg, copy=SAME_SIZE)
    monkeypatch.setattr(dbmod, "LOCK_RETRY_DELAYS", (0.0,))
    before = _row(db)
    with get_conn(db) as c:
        r = ch._broken_rows_with_placement(c)[0]
    holder = _WriterLock(db)
    conn = _NoWaitConn(db, holder)

    def lock_then_no_space():
        holder.hold()  # the incoming stamp is already committed; the lock arrives with the failed move
        raise OSError(errno.ENOSPC, "No space left on device")
    try:
        with monkeypatch.context() as m:
            _on_the_move(m, canonical, instead=lock_then_no_space)
            if leg == "sidecar":
                res = ch.restore_from_placement(db, themes, r, conn=conn)
            else:
                res = ch._publish_store_bytes(db, themes, r, SAME_SIZE, "upload://themes/x", conn=conn)
    finally:
        conn.close()
        holder.let_go()
    assert conn.refused == ["BEGIN IMMEDIATE"], f"premise: the held lock refused the write-back's first BEGIN: {conn.refused}"
    assert res == {"ok": False, "reason": f"{FAILED[leg]}No space left on device"}
    assert _row(db) == before, "the write-back gave up after one refusal and left the incoming stamp on unmoved bytes"


@LEGS
def test_a_failed_move_whose_write_back_loses_the_lock_is_a_skip_and_the_rerun_lands_the_bytes(tmp_path, monkeypatch,
                                                                                               caplog, leg):
    db, themes, canonical, _anchors = _seed(tmp_path, leg=leg, copy=SAME_SIZE)
    # no busy wait and no ladder: the write-back's lock error is immediate, never a timing
    monkeypatch.setattr(dbmod, "LOCK_WAIT_S", 0.0)
    monkeypatch.setattr(dbmod, "LOCK_RETRY_DELAYS", ())
    holder = _WriterLock(db)

    def lock_then_no_space():
        holder.hold()
        raise OSError(errno.ENOSPC, "No space left on device")
    caplog.set_level(logging.WARNING, logger="motif.canonical_health")
    try:
        with monkeypatch.context() as m:
            _on_the_move(m, canonical, instead=lock_then_no_space)
            res = ch.restore_from_plex(db, themes, Plex(SAME_SIZE))
    finally:
        holder.let_go()
    assert ([s["reason"].startswith(FAILED[leg]) for s in res["skipped"]], res["restored"]) == ([True], 0), \
        f"the lost write-back ended the run instead of skipping the row: {res}"
    kept = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING and "stamp" in r.getMessage()]
    assert len(kept) == 1 and str(TMDB) in kept[0], f"no breadcrumb names the row whose write-back was lost: {kept}"
    assert not canonical.exists()
    assert ch.restore_from_plex(db, themes, Plex(SAME_SIZE))["restored"] == 1, "the re-run did not land the bytes"
    row = _row(db)
    assert (row["canonical_present"], row["file_size"], row["file_sha256"]) == (1, len(SAME_SIZE), _sha(SAME_SIZE))


# ── F16: the reason words the errno; the path stays in the log ──

@LEGS
@pytest.mark.parametrize("make, word", [(lambda p: OSError(errno.ENOSPC, "No space left on device", p), "No space left on device"),
                                        (lambda p: OSError(p), "OSError")], ids=["errno", "bare"])
def test_a_move_that_fails_words_its_reason_by_errno_and_keeps_the_path_for_the_log(tmp_path, monkeypatch, caplog, leg,
                                                                                    make, word):
    db, themes, canonical, _anchors = _seed(tmp_path, leg=leg, copy=SAME_SIZE)
    err = make(str(canonical))
    assert str(canonical) in str(err), "premise: the raised error names the path"

    def fail():
        raise err
    caplog.set_level(logging.WARNING, logger="motif.canonical_health")
    with monkeypatch.context() as m:
        _on_the_move(m, canonical, instead=fail)
        res = ch.restore_from_plex(db, themes, Plex(SAME_SIZE))
    assert [s["reason"] for s in res["skipped"]] == [f"{FAILED[leg]}{word}"], "the reason carries the media path"
    assert any(str(canonical) in r.getMessage() for r in caplog.records if r.levelno == logging.WARNING), \
        "the path left the log along with the reason"


@pytest.mark.skipif(hasattr(os, "geteuid") and os.geteuid() == 0, reason="root reads a 000 file")
def test_a_plex_folder_copy_motif_cannot_read_is_refused_by_its_errno_and_never_linked(tmp_path, caplog):
    db, themes, canonical, _anchors = _seed(tmp_path, leg="sidecar", copy=SAME_SIZE)
    before, src = _row(db), tmp_path / "plex" / str(TMDB) / "theme.mp3"
    src.chmod(0)
    caplog.set_level(logging.WARNING, logger="motif.canonical_health")
    try:
        res = ch.restore_from_plex(db, themes, None)
    finally:
        src.chmod(0o644)
    # the decision: a copy whose bytes motif cannot read is never linked in, however linkable the inode is
    assert (res["restored"], [s["reason"] for s in res["skipped"]]) == \
        (0, [f"{FAILED['sidecar']}{os.strerror(errno.EACCES)}"]), f"unreadable copy linked in, or path in reason: {res}"
    assert not canonical.exists() and _row(db) == before
    assert any(str(src) in r.getMessage() for r in caplog.records if r.levelno == logging.WARNING), \
        "the unreadable source is named nowhere the operator can read"


# ── F17: a failed read-back stamps the bytes just published ──

@pytest.mark.parametrize("copy", [SAME_SIZE, OTHER_SIZE], ids=["same-size", "other-size"])
def test_a_read_back_that_fails_after_the_publish_stamps_the_bytes_just_published(tmp_path, monkeypatch, copy):
    leg = "store"  # v0.51.344 (R2-F7): only the store leg reads its canonical back — the sidecar leg's read is below
    db, themes, canonical, _anchors = _seed(tmp_path, leg=leg, copy=copy)
    real_open, fired = Path.open, []

    def open_(self, mode="r", *a, **k):
        if self == canonical and mode == "rb":
            fired.append(1)
            raise OSError(errno.EIO, os.strerror(errno.EIO), str(self))
        return real_open(self, mode, *a, **k)
    with monkeypatch.context() as m:
        m.setattr(Path, "open", open_)
        res = ch.restore_from_plex(db, themes, Plex(copy))
    assert (res["restored"], fired) == (1, [1]), f"premise: the publish landed and its read-back raised: {res}"
    assert canonical.read_bytes() == copy, "premise: the published bytes are on disk"
    row = _row(db)
    assert (row["canonical_present"], row["file_size"], row["file_sha256"]) == (1, len(copy), _sha(copy)), \
        "the pre-restore size and sha were written back over the bytes just published"
    assert {k: row[k] for k in _NORM_COLS} == dict.fromkeys(_NORM_COLS)
    # a CHANGED candidate marks a size that moved — never a read-back the published bytes already answer
    assert bool(_candidate(db)) == (len(copy) != len(RECORDED)), _candidate(db)
    plex_enum.verify_canonical_health(db, themes)
    with get_conn(db) as c:
        assert ch.changed_canonicals(c, themes) == [], "the next CHECK lists the bytes motif just published as CHANGED"
    assert _row(db)["file_sha256"] == _sha(copy), "the check re-stamped the row away from the bytes on disk"


@pytest.mark.parametrize("copy", [SAME_SIZE, OTHER_SIZE], ids=["same-size", "other-size"])
def test_a_sidecar_copy_that_cannot_be_read_is_refused_before_it_moves(tmp_path, monkeypatch, copy):
    # v0.51.344 (R2-F7): the sidecar leg reads its staged inode once, before the move — that read is the one to fail
    db, themes, canonical, _anchors = _seed(tmp_path, leg="sidecar", copy=copy)
    before, real_open, fired = _row(db), Path.open, []

    def open_(self, mode="r", *a, **k):
        if self.parent == canonical.parent and self.name.endswith(".motif-tmp") and mode == "rb":
            fired.append(1)
            raise OSError(errno.EIO, os.strerror(errno.EIO), str(self))
        return real_open(self, mode, *a, **k)
    with monkeypatch.context() as m:
        m.setattr(Path, "open", open_)
        res = ch.restore_from_plex(db, themes, Plex(copy))
    assert (fired, res["restored"], [s["reason"] for s in res["skipped"]]) == \
        ([1], 0, [f"{FAILED['sidecar']}{os.strerror(errno.EIO)}"]), f"premise: the staged read raised: {res}"
    assert not canonical.exists() and list(canonical.parent.iterdir()) == [], "an unread copy moved, or its staging file stayed"
    assert _row(db) == before, "a copy that could not be read stamped the row"


# ── F19: one unstatable path skips its own directory; the sweep and its report go on ──

def test_one_unstatable_canonical_row_skips_only_its_directory_and_the_nightly_job_still_reports(tmp_path, monkeypatch,
                                                                                                 caplog):
    from app.config import Settings
    events: list[str] = []
    monkeypatch.setattr(scheduler, "log_event", lambda *a, **k: events.append(k.get("message")))
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    _db(s.db_path)
    themes = tmp_path / "themes"
    _lf(s.db_path, 1, "movies/Bad\x00 (2000)/theme.mp3")  # first, so its directory is queued before the good ones
    parts = []
    for n in (2, 3, 4):
        rel = f"movies/M{n}/theme.mp3"
        _lf(s.db_path, n, rel)
        part = themes / (rel + ".0123456789abcdef.part")  # v0.51.344: the store publish's own staging name (R2-F3)
        part.parent.mkdir(parents=True)
        part.write_bytes(b"stranded")
        parts.append(part)
    s._cfg.paths.themes_dir = str(themes)
    _forward(monkeypatch, 7200)
    caplog.set_level(logging.DEBUG, logger="app.core.plex_enum")
    scheduler._sweep_placement_temps_job(s)
    assert [p.exists() for p in parts] == [False] * 3, "good directories queued after the bad row were left unswept"
    assert len(events) == 1 and "3" in events[0], f"the job did not report its count: {events}"
    ours = [r for r in caplog.records if r.name in ("app.core.plex_enum", "app.core.scheduler")]
    assert [r.levelno for r in ours if "Bad" in r.getMessage()] == [logging.DEBUG], \
        "a malformed row is a debug breadcrumb, not a nightly warning (verify's precedent)"
    assert not [r.getMessage() for r in ours if r.levelno >= logging.WARNING]


def test_one_unstatable_media_folder_skips_only_its_folder(tmp_path, monkeypatch, caplog):
    db = _db(tmp_path / "m.db")
    _plex_item(db, rk="1", section="1", folder=str(tmp_path / "media" / "Bad\x00"))
    folder = tmp_path / "media" / "M2"
    folder.mkdir(parents=True)
    tmp = folder / "theme.mp3.motif-tmp"
    tmp.write_bytes(b"stranded")
    _plex_item(db, rk="2", section="1", folder=str(folder))
    _forward(monkeypatch, 7200)
    caplog.set_level(logging.DEBUG, logger="app.core.plex_enum")
    assert sweep_stale_placement_temps(db) == 1
    assert not tmp.exists()
    assert [r.levelno for r in caplog.records if "Bad" in r.getMessage()] == [logging.DEBUG]
