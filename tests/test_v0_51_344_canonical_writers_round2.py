"""v0.51.344 integration review round 2, canonical writers: R2-F2, F5, F7, F9, F10, F11 and R3-F6, F7.

  F2   exit's gate covers the sidecar restore too: closing waits for one mid-write and refuses the next, the bulk
       stops at the closed gate, and a raise while staging leaves the row untouched (the stamp follows the staging).
  F5   a plex_error: reason neither counts toward the stop rule nor resets it.
  F7   the sidecar leg reads each theme once by link and twice by copy: the staged inode is hashed, never read back.
  F9   the in-flight memo keys on the download backlog itself: its own connection's queueing is seen, an unrelated
       commit does not re-read the backlog, a queued download does, and no connection is kept.
  F10  a row an edition swap re-keyed mid-run is a row_moved skip: nothing lands at the stale path, nothing counts.
  F11  the recorded sha never runs ahead of bytes on disk: a census mid-restore names the prior sha until the staged
       file exists, and at the move the staged file holds the bytes the census names.
  R3-F6 a present canonical whose size was never recorded (NULL / 0) is CHANGED.
  R3-F7 a writer waiting on a path lock holds no publish gate: the close and unrelated publishes go on.
"""
from __future__ import annotations

import errno
import functools
import os
import re
import shutil
import sqlite3
import threading
from contextlib import closing
from pathlib import Path

import pytest

from app.core import canonical_health as ch
from app.core import placement, plex_enum
from app.core.bundle import themes_census
from app.core.db import get_conn
from test_v0_51_342_restore_from_plex_job import _ago
from test_v0_51_342_restore_pool import FakeClock, FakePlex, _lf_cols, _seed_store
from test_v0_51_344_shared_path_in_flight import _broken, _db, _download, _path, _theme, fresh_in_flight_paths  # noqa: F401 — an autouse fixture
from test_v0_51_344_torn_publish import FAILED, OTHER_SIZE, RECORDED, TMDB, Plex, _on_the_move, _row, _seed, _sha

REPO = Path(__file__).resolve().parents[1]
LEGS = pytest.mark.parametrize("leg", ["store", "sidecar"])


class Killed(BaseException):
    """motif killed mid-stage."""


@pytest.fixture(autouse=True)
def open_publishing(monkeypatch):
    monkeypatch.setattr(ch, "_PUBLISH_CLOSED", threading.Event())
    monkeypatch.setattr(ch, "_PUBLISH_LOCK", threading.Lock())


def _three_sidecar_rows(tmp_path):
    db, themes, plexdir = _db(tmp_path)
    for i, t in enumerate((2301, 2302, 2303)):
        _theme(db, t, title=f"Row {i}")
        _broken(db, plexdir, t, _path(title=f"Row {i}"))
    return db, themes, plexdir


# ── F2: the exit gate covers the sidecar restore ──────────────────────

def test_closing_publishing_waits_for_a_sidecar_restore_mid_write_and_refuses_the_next(tmp_path, monkeypatch):
    db, themes, _plexdir = _three_sidecar_rows(tmp_path)
    with get_conn(db) as c:
        rows = ch._broken_rows_with_placement(c)
    linked, go = threading.Event(), threading.Event()
    real_link = os.link

    def link(src, dst, *a, **k):
        real_link(src, dst, *a, **k)
        linked.set()
        assert go.wait(10), "premise: the held restore was released"
    monkeypatch.setattr(os, "link", link)
    out: dict = {}
    run = threading.Thread(target=lambda: out.update(res=ch.restore_from_placement(db, themes, rows[0])), daemon=True)
    run.start()
    try:
        assert linked.wait(10), "premise: the sidecar restore is mid-write — staged, not yet moved"
        assert ch.close_publishing(0.5) is False, "closing said no writer was mid-write while a sidecar restore was"
    finally:
        go.set()
    run.join(10)
    monkeypatch.setattr(os, "link", real_link)
    assert out["res"] == {"ok": True, "kind": "hardlink"}, "the restore closing waited for did not land whole"
    assert (themes / rows[0]["file_path"]).is_file() and _lf_cols(db, rows[0]["tmdb_id"], ("canonical_present",)) == (1,)
    assert ch.close_publishing(1.0) is True
    assert ch.restore_from_placement(db, themes, rows[1]) == {"ok": False, "reason": "motif_exiting"}, \
        "a sidecar restore started after exit closed publishing"
    assert not (themes / rows[1]["file_path"]).exists()
    assert _lf_cols(db, rows[1]["tmdb_id"], ("canonical_present",)) == (0,)


def test_the_bulk_stops_at_the_closed_gate_instead_of_refusing_every_remaining_row(tmp_path):
    db, themes, _plexdir = _three_sidecar_rows(tmp_path)
    closed: list[bool] = []

    def progress(done, total, counts):
        if done == 1 and not closed:
            closed.append(ch.close_publishing(1.0))  # exit's deadline lands between two rows
    res = ch.restore_from_plex(db, themes, None, progress_cb=progress)
    assert closed == [True], "premise: publishing closed between two rows, with nothing mid-write"
    assert (res["restored"], res["cancelled"], res["not_attempted"], res["skipped"]) == (1, True, 2, []), \
        "the bulk went on past the closed gate — restoring, or refusing row by row"
    assert len(list(themes.rglob("theme.mp3"))) == 1


@LEGS
def test_a_raise_while_staging_leaves_the_row_as_it_was_and_no_staging_file(tmp_path, monkeypatch, leg):
    db, themes, canonical, _anchors = _seed(tmp_path, leg=leg, copy=OTHER_SIZE)
    before = _row(db)
    assert before["file_sha256"] != _sha(OTHER_SIZE), "premise: the incoming bytes differ — the stamp would land"

    def killed(*a, **k):
        raise Killed()
    with monkeypatch.context() as m:
        if leg == "sidecar":
            m.setattr(os, "link", killed)
        else:
            real_open = Path.open
            m.setattr(Path, "open", lambda self, mode="r", *a, **k: killed() if self.name.endswith(".part")
                      else real_open(self, mode, *a, **k))
        with pytest.raises(Killed):
            ch.restore_from_plex(db, themes, Plex(OTHER_SIZE))
    assert not canonical.exists() and list(canonical.parent.iterdir()) == [], "a staging file was left behind"
    assert _row(db) == before, "a raise while staging left the incoming stamp on bytes that never landed"


# ── F5: plex_error: is neither an answer nor a no-answer ─────────────

def test_a_raise_reading_plexs_answer_neither_counts_nor_resets_the_stop_rule():
    c = FakeClock()
    gate = ch._PlexGate(clock=c, sleep=c.sleep)
    gate.after(None, 10.0)  # Plex answered at 10
    starts = [60.0 + 10 * i for i in range(ch._PLEX_TRIP_AFTER)]  # eight no-answers, sent 60..130
    c.t = 100.0
    gate.after("plex_error:TypeError", 95.0)  # a body motif could not read, answered at 100
    for sent in starts:  # the no-answers report after it — four of them were sent before 100 and hung until now
        gate.after("plex_themes:transport", sent)
    assert gate.tripped is True, "a raise reading Plex's answer reset the count, or threw away the no-answers in flight"
    only_raises = ch._PlexGate(clock=c, sleep=c.sleep)
    for i in range(ch._PLEX_TRIP_AFTER + 1):
        only_raises.after("plex_error:AttributeError", 60.0 + 10 * i)
    assert only_raises.tripped is False, "raises reading answers counted as no-answers"


class _MalformedEveryThird(FakePlex):
    """A Plex that gives no answer, except every third listing answers 200 with a body motif cannot read."""

    def get_themes(self, *, rating_key):
        if int(rating_key) % 3 == 0:
            self._call("themes", rating_key)
            return {"ok": True, "http_status": 200, "error": None, "body": {"MediaContainer": {"Metadata": 5}}}
        return super().get_themes(rating_key=rating_key)


def test_a_dead_plex_whose_every_third_answer_is_malformed_still_trips_the_stop_rule(tmp_path, monkeypatch):
    c = FakeClock()
    monkeypatch.setattr(ch, "_PlexGate", functools.partial(ch._PlexGate, clock=c, sleep=c.sleep))
    db, themes = _seed_store(tmp_path, n=40)
    plex = _MalformedEveryThird(no_answer=lambda: True)
    res = ch.restore_from_plex(db, themes, plex)
    reasons = [s["reason"] for s in res["skipped"]]
    assert any(r.startswith("plex_error:") for r in reasons), "premise: malformed bodies reached the gate"
    assert res["plex_unreachable"] is True and reasons.count("plex_unreachable") > 0, \
        "a malformed body every third row kept the run asking a dead Plex about every row"


# ── F7: one read per restored row ─────────────────────────────────────

@pytest.mark.parametrize("kind", ["hardlink", "copy"])
def test_the_sidecar_leg_reads_each_theme_once_by_link_and_twice_by_copy(tmp_path, monkeypatch, kind):
    db, themes, _plexdir = _three_sidecar_rows(tmp_path)
    if kind == "copy":
        monkeypatch.setattr(placement, "_can_hardlink", lambda src, dst_dir: False)
    real_open, real_copy2 = Path.open, shutil.copy2
    opened: list[str] = []
    copied: list[str] = []

    def open_(self, mode="r", *a, **k):
        if "r" in mode and "b" in mode and self.is_relative_to(tmp_path):
            opened.append(str(self.relative_to(tmp_path)))
        return real_open(self, mode, *a, **k)

    def copy2(src, dst, *a, **k):
        copied.append(str(Path(src).relative_to(tmp_path)))
        return real_copy2(src, dst, *a, **k)
    monkeypatch.setattr(Path, "open", open_)
    monkeypatch.setattr(shutil, "copy2", copy2)
    res = ch.restore_from_plex(db, themes, None)
    assert (res["restored_sidecar"], res["skipped"]) == (3, []), "premise: every row restored"
    reads = len(opened) + len(copied)
    assert reads == (3 if kind == "hardlink" else 6), f"{reads / 3:.2f} reads per restored row: {opened} {copied}"
    assert [p for p in opened if p.startswith("themes") and p.endswith("theme.mp3")] == [], \
        f"a canonical was read back after its staged inode was hashed: {opened}"


# ── F9: the in-flight memo keys on the backlog itself ────────────────

def test_the_in_flight_check_sees_a_download_its_own_connection_queued(tmp_path):
    db, _themes, plexdir = _db(tmp_path)
    _theme(db, 2101)
    _broken(db, plexdir, 2101, _path())
    with get_conn(db) as conn:
        row = conn.execute("SELECT * FROM local_files WHERE tmdb_id = 2101").fetchone()
        assert ch._download_in_flight(conn, row) is False, "premise: nothing queued yet"
        conn.execute("INSERT INTO jobs (job_type, media_type, tmdb_id, section_id, payload, status, created_at) "
                     "VALUES ('download', 'movie', 2101, '1', '{}', 'pending', ?)", (_ago(minutes=1),))
        assert ch._download_in_flight(conn, row) is True, \
            "the check answered from its memo, blind to the download its own connection just queued"


def test_an_unrelated_commit_keeps_the_backlog_memo_and_a_queued_download_moves_it(tmp_path):
    db, _themes, plexdir = _db(tmp_path)
    _theme(db, 2101)
    _theme(db, 2102)
    _broken(db, plexdir, 2101, _path())
    backlog_reads: list[str] = []
    with get_conn(db) as conn:
        conn.set_trace_callback(lambda sql: backlog_reads.append(sql) if "FROM jobs j" in sql else None)
        row = conn.execute("SELECT * FROM local_files WHERE tmdb_id = 2101").fetchone()
        assert ch._download_in_flight(conn, row) is False and len(backlog_reads) == 1, "premise: one read builds the memo"
        with closing(sqlite3.connect(db)) as other:  # a writer on its own connection, touching nothing of the backlog
            other.execute("UPDATE plex_sections SET last_seen_at = ? WHERE section_id = '1'", (_ago(),))
            other.commit()
        assert ch._download_in_flight(conn, row) is False and len(backlog_reads) == 1, \
            "an unrelated commit on another connection re-read the queued downloads"
        _download(db, 2102)  # a same-title download on this row's path, queued on yet another connection
        assert ch._download_in_flight(conn, row) is True and len(backlog_reads) == 2, \
            "a download queued on another connection was not seen"


def test_a_download_that_ends_between_two_rows_of_one_run_stops_holding_its_path(tmp_path):
    db, themes, plexdir = _db(tmp_path)
    _theme(db, 2100, title="Alpha Title")
    _theme(db, 2101)
    _theme(db, 2102)
    _broken(db, plexdir, 2100, _path(title="Alpha Title"))
    _broken(db, plexdir, 2101, _path())
    job = _download(db, 2102)
    ends: list[str] = []

    def progress(done, total, counts):
        if done == 1 and not ends:  # the first row is landed; the download ends the way every end-writer ends one
            with closing(sqlite3.connect(db)) as c:
                c.execute("UPDATE jobs SET status = 'done', finished_at = ? WHERE id = ?", (_ago(), job))
                c.commit()
            ends.append("done")
    res = ch.restore_from_plex(db, themes, None, progress_cb=progress)
    assert ends == ["done"], "premise: the download ended between the two rows"
    assert (res["restored"], res["skipped"]) == (2, []), "a download that had ended still held the row on its path"


_TERMINAL_WRITE = re.compile(r"UPDATE jobs SET status = '(?:done|failed|cancelled)'")


def test_every_end_writer_stamps_finished_at_which_the_in_flight_key_reads():
    """The key's contract: a job that ends moves MAX(finished_at) — so every terminal status write must set it."""
    bare = []
    for py in sorted((REPO / "app").rglob("*.py")):
        text = py.read_text()
        for m in _TERMINAL_WRITE.finditer(text):
            statement = text[m.start():text.index("WHERE", m.end())]  # this statement, up to its own WHERE
            if "finished_at" not in statement:
                bare.append(f"{py.relative_to(REPO)}:{text.count(chr(10), 0, m.start()) + 1}")
    assert bare == [], f"a job can end without moving the in-flight key: {bare}"


def test_the_in_flight_memo_keeps_no_connection(tmp_path):
    db, _themes, plexdir = _db(tmp_path)
    _theme(db, 2101)
    _broken(db, plexdir, 2101, _path())
    with get_conn(db) as conn:
        row = conn.execute("SELECT * FROM local_files WHERE tmdb_id = 2101").fetchone()
        ch._download_in_flight(conn, row)
    held = [x for memo in ch._IN_FLIGHT_DOWNLOADS.values() for x in memo if isinstance(x, sqlite3.Connection)]
    assert held == [], "the memo pins a closed connection for the life of the process"


def _backlog_shape(db):
    """What moves the in-flight key: the newest job, the newest ended download, the running count."""
    with closing(sqlite3.connect(db)) as c:
        return c.execute(
            "SELECT (SELECT MAX(id) FROM jobs), "
            "       (SELECT MAX(finished_at) FROM jobs WHERE job_type = 'download' AND status IN ('done', 'failed', 'cancelled')), "
            "       (SELECT COUNT(*) FROM jobs WHERE job_type = 'download' AND status = 'running')").fetchone()


def test_two_databases_on_one_backlog_shape_never_share_a_memo(tmp_path):
    """The memo names its database: two whose job tables collide on the backlog key each answer from their own."""
    a, b = tmp_path / "a", tmp_path / "b"
    a.mkdir()
    b.mkdir()
    db_a, _themes_a, plex_a = _db(a)
    _theme(db_a, 2101)
    _theme(db_a, 2109, title="Another Title")
    _broken(db_a, plex_a, 2101, _path())
    _download(db_a, 2109)  # A's one download writes another title's path
    db_b, _themes_b, plex_b = _db(b)
    _theme(db_b, 2101)
    _theme(db_b, 2102)
    _broken(db_b, plex_b, 2101, _path())
    _download(db_b, 2102)  # B's one download is a same-title job on 2101's path
    assert _backlog_shape(db_a) == _backlog_shape(db_b), "premise: the two job tables collide on the backlog key"
    with get_conn(db_a) as conn_a, get_conn(db_b) as conn_b:
        row_a = conn_a.execute("SELECT * FROM local_files WHERE tmdb_id = 2101").fetchone()
        row_b = conn_b.execute("SELECT * FROM local_files WHERE tmdb_id = 2101").fetchone()
        assert ch._download_in_flight(conn_a, row_a) is False, "premise: nothing queued in A writes 2101's path"
        assert ch._download_in_flight(conn_b, row_b) is True, \
            "B's row was answered from A's memo — two databases on one backlog shape shared it"
        assert ch._download_in_flight(conn_a, row_a) is False, "A's row was answered from B's memo"


# ── F10: a row an edition swap re-keyed mid-run ───────────────────────

def _move_row(db, *, new_edition="dc"):
    """What resolve_edition_swap does to the row: a new PK and file_path (its dead placement is its own DELETE)."""
    with closing(sqlite3.connect(db)) as c:
        c.execute("UPDATE local_files SET edition_key = ?, file_path = ? WHERE media_type = 'movie' AND tmdb_id = ? "
                  "AND section_id = '1' AND edition_key = ''",
                  (new_edition, f"movies/{TMDB} {{edition-{new_edition}}}/theme.mp3", TMDB))
        c.commit()


@LEGS
def test_a_row_an_edition_swap_moved_mid_run_is_a_row_moved_skip_with_no_stray_file(tmp_path, leg):
    # Plex holds the recorded bytes: no incoming stamp to notice the row is gone — only the re-read before staging can
    db, themes, canonical, _anchors = _seed(tmp_path, leg=leg, copy=RECORDED)
    canonical.parent.mkdir(parents=True)
    canonical.write_bytes(RECORDED)  # on disk with its flag stale — the shape a swap finds and moves
    moved = canonical.parent.parent / f"{TMDB} {{edition-dc}}" / "theme.mp3"
    swaps: list[int] = []

    def progress(done, total, counts):
        if not swaps:  # the run holds its row snapshot; the swap lands before the row is reached
            moved.parent.mkdir(parents=True)
            canonical.replace(moved)
            _move_row(db)
            swaps.append(1)
    before = _row(db)
    res = ch.restore_from_plex(db, themes, Plex(RECORDED), progress_cb=progress)
    assert swaps == [1], "premise: the swap landed mid-run"
    assert (res["restored"], [s["reason"] for s in res["skipped"]]) == (0, ["row_moved"]), \
        "a row the swap moved was counted restored"
    assert not canonical.exists(), "the restore re-created a theme.mp3 in the folder the swap emptied"
    assert moved.read_bytes() == RECORDED
    assert _row(db) == before, "the moved row's stamp changed"


@LEGS
def test_a_row_re_keyed_after_its_bytes_landed_is_not_counted_restored(tmp_path, monkeypatch, leg):
    db, themes, canonical, _anchors = _seed(tmp_path, leg=leg, copy=OTHER_SIZE)
    with monkeypatch.context() as m:
        _on_the_move(m, canonical, after=lambda: _move_row(db))
        res = ch.restore_from_plex(db, themes, Plex(OTHER_SIZE))
    assert (res["restored"], [s["reason"] for s in res["skipped"]]) == (0, ["row_moved"]), \
        "a stamp that reached no row was counted as a restore"
    assert _row(db)["canonical_present"] == 0, "the re-keyed row was stamped present by its old key"


@LEGS
def test_a_row_that_leaves_between_the_re_read_and_the_stamp_moves_nothing(tmp_path, monkeypatch, leg):
    db, themes, canonical, _anchors = _seed(tmp_path, leg=leg, copy=OTHER_SIZE)
    before, real = _row(db), ch._stamp_incoming

    def stamp(*a, **k):
        _move_row(db)
        return real(*a, **k)
    monkeypatch.setattr(ch, "_stamp_incoming", stamp)
    res = ch.restore_from_plex(db, themes, Plex(OTHER_SIZE))
    assert (res["restored"], [s["reason"] for s in res["skipped"]]) == (0, ["row_moved"])
    assert not canonical.exists() and list(canonical.parent.iterdir()) == [], "bytes or a staging file landed for a row that was gone"
    assert _row(db) == before


# ── F11: the recorded sha never runs ahead of bytes on disk ──────────

def _census_sha(db):
    return {r["tmdb_id"]: r["sha256"] for r in themes_census(db)}[TMDB]


@LEGS
def test_the_recorded_sha_never_runs_ahead_of_bytes_on_disk(tmp_path, monkeypatch, leg):
    db, themes, canonical, _anchors = _seed(tmp_path, leg=leg, copy=OTHER_SIZE)
    prior, incoming = _sha(RECORDED), _sha(OTHER_SIZE)
    seen: dict = {}
    if leg == "sidecar":
        real_link = os.link

        def link(src, dst, *a, **k):
            seen["before_staging"] = _census_sha(db)
            return real_link(src, dst, *a, **k)
        monkeypatch.setattr(os, "link", link)
    else:
        real_open = Path.open

        def open_(self, mode="r", *a, **k):
            if self.name.endswith(".part") and "w" in mode:
                seen["before_staging"] = _census_sha(db)
            return real_open(self, mode, *a, **k)
        monkeypatch.setattr(Path, "open", open_)

    def at_the_move():
        seen["at_the_move"] = _census_sha(db)
        seen["staged"] = [_sha(p.read_bytes()) for p in canonical.parent.iterdir() if p.name != canonical.name]
    _on_the_move(monkeypatch, canonical, instead=at_the_move)
    assert ch.restore_from_plex(db, themes, Plex(OTHER_SIZE))["restored"] == 1
    assert seen["before_staging"] == prior, "a bundle taken before any byte was staged would name the incoming sha"
    assert (seen["at_the_move"], seen["staged"]) == (incoming, [incoming]), \
        "at the move the recorded sha is the staged file's, and that file is on disk"
    assert _row(db)["file_sha256"] == incoming


# ── R3-F6: a size never recorded is CHANGED ───────────────────────────

def _present_unrecorded(tmp_path, sizes: dict):
    db, themes, _plexdir = _db(tmp_path)
    data = b"x" * 4099
    for tmdb, size in sizes.items():
        _theme(db, tmdb, title=f"T{tmdb}")
        with closing(sqlite3.connect(db)) as c:
            c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key, theme_id, file_path, "
                      "file_size, file_sha256, downloaded_at, source_video_id, provenance, source_kind, "
                      "canonical_present) VALUES ('movie', ?, '1', '', (SELECT id FROM themes WHERE tmdb_id = ?), ?, "
                      "?, NULL, ?, '', 'manual', 'upload', 0)", (tmdb, tmdb, f"movies/{tmdb}/theme.mp3", size, _ago()))
            c.commit()
        p = themes / f"movies/{tmdb}/theme.mp3"
        p.parent.mkdir(parents=True)
        p.write_bytes(data)
    return db, themes


def test_a_present_canonical_whose_size_was_never_recorded_is_changed(tmp_path):
    db, themes = _present_unrecorded(tmp_path, {2401: 0, 2402: None, 2403: 4099})
    assert plex_enum.verify_canonical_health(db, themes) == {"checked": 3, "missing": 0, "skipped": 0}
    with get_conn(db) as conn:
        changed = {r["tmdb_id"]: (r["recorded"], r["on_disk"]) for r in ch.changed_canonicals(conn, themes)}
        rep = ch.broken_canonical_report(conn, themes)
    assert changed == {2401: (0, 4099), 2402: (None, 4099)}, \
        "bytes on disk with no size on record were stamped present and never listed as CHANGED"
    # 2403 records the size on disk with no sha: nothing a size check can disagree with
    assert (rep["counts"]["broken"], rep["counts"]["changed"]) == (0, 2)


# ── R3-F7: a wait on a path lock holds no publish gate ───────────────

class _Contended:
    """A path lock whose contended acquire says so before it waits."""

    def __init__(self, real, waiting):
        self.real, self.waiting = real, waiting

    def __enter__(self):
        if not self.real.acquire(blocking=False):
            self.waiting.set()
            self.real.acquire()
        return self

    def __exit__(self, *exc):
        self.real.release()


def test_a_writer_waiting_on_a_path_lock_holds_no_publish_gate(tmp_path, monkeypatch):
    db, themes = _seed_store(tmp_path, n=2)
    with get_conn(db) as c:
        rows = {r["tmdb_id"]: r for r in ch._broken_rows_with_placement(c)}
    x, y = rows[601], rows[602]
    held = ch._canonical_write_lock(themes / x["file_path"])
    held.acquire()  # another writer — the sweep, an INFO restore — holds row 601's path
    waiting = threading.Event()
    real_lock = ch._canonical_write_lock
    monkeypatch.setattr(ch, "_canonical_write_lock", lambda p: _Contended(real_lock(p), waiting))
    out: dict = {}
    b = threading.Thread(target=lambda: out.update(b=ch._publish_store_bytes(db, themes, x, b"B" * 20, "u://b")),
                         daemon=True)
    b.start()
    try:
        assert waiting.wait(10), "premise: the publish of row 601 is waiting on its path lock"
        other = threading.Thread(target=lambda: out.update(c=ch._publish_store_bytes(db, themes, y, b"C" * 20, "u://c")),
                                 daemon=True)
        other.start()
        other.join(5)
        assert not other.is_alive() and out["c"]["ok"], \
            "a publish of an unrelated row queued behind the writer waiting on row 601's path"
        assert ch.close_publishing(0.5) is True, \
            "closing reported a write in progress while the writer only waited on a path lock"
    finally:
        held.release()
    b.join(10)
    assert out["b"] == {"ok": False, "reason": "motif_exiting"}, "the writer that waited through the close wrote after it"
    assert not (themes / x["file_path"]).exists() and (themes / y["file_path"]).is_file()
