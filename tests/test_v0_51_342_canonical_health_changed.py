"""v0.51.342: CANONICAL HEALTH — the CHANGED bucket and the CHECK walk.

  1. A page open stats the last check's CHANGED candidates, not the themes tree; each one's live size still decides.
  2. verify makes one stat per row with the same missing / skipped split, flags the candidates, and does not
     re-hash a genuine change until the file or its record moves — without ever blocking the v0.51.338 heal.
  3. The report's statements do not grow with broken rows: the override resolves in the row, placements come batched.
  4. Schema v80 and a staged database restore both clear the check results they cannot vouch for.
  5. The report says how fresh it is; RUN CHECK's status names no themes dir, a dead root and a partial run.
"""
from __future__ import annotations

import contextlib
import errno
import hashlib
import logging
import os
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core import adopt, db_backup, plex_enum
from app.core import canonical_health as ch
from app.core.db import get_conn, init_db
from app.core.events import now_iso
from tests.test_v0_51_339_canonical_health_restore import _NODE, _run_page
from tests.test_v0_51_339_canonical_health_restore import _report as _page_report

NOW = "2026-09-13T00:00:00+00:00"
AUTH = {"X-Authentik-Username": "testadmin"}
REPORT = "/api/admin/canonical-health/report"
CHECK = "/api/admin/canonical-health/check"


# ── seed helpers ──────────────────────────────────────────────────────

def _sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _rel(tmdb, edition=""):
    return f"movies/{tmdb}{'-' + edition if edition else ''}/theme.mp3"


@contextlib.contextmanager
def _conn(db):
    c = sqlite3.connect(db)
    try:
        yield c
        c.commit()
    finally:
        c.close()


def _exec(db, sql, params=()):
    with _conn(db) as c:
        c.execute(sql, params)


def _section(c, sid):
    c.execute("INSERT OR IGNORE INTO plex_sections (section_id, title, type, is_anime, is_4k, themes_subdir, "
              " included, discovered_at, last_seen_at) VALUES (?, 'M', 'movie', 0, 0, ?, 1, ?, ?)",
              (sid, f"movies{sid}", NOW, NOW))


def _lf(c, tmdb, *, size=None, sha=None, present=None, section="1", edition="", kind="themerrdb"):
    _section(c, section)
    c.execute("INSERT OR IGNORE INTO themes (id, media_type, tmdb_id, title, upstream_source, "
              " last_seen_sync_at, first_seen_sync_at, youtube_url) VALUES (?, 'movie', ?, ?, 'imdb', ?, ?, NULL)",
              (tmdb, tmdb, f"T{tmdb}", NOW, NOW))
    c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key, theme_id, file_path, "
              " file_size, file_sha256, downloaded_at, source_video_id, provenance, source_kind, canonical_present) "
              "VALUES ('movie', ?, ?, ?, ?, ?, ?, ?, ?, 'v', 'auto', ?, ?)",
              (tmdb, section, edition, tmdb, _rel(tmdb, edition), size, sha, NOW, kind, present))


def _write(themes, tmdb, data):
    p = themes / _rel(tmdb)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(data)


def _fill(db, themes, n, sections=("1",)):
    themes.mkdir(parents=True, exist_ok=True)
    with _conn(db) as c:
        for i in range(1, n + 1):
            data = b"x" * (100 + i)
            _write(themes, i, data)
            _lf(c, i, size=len(data), sha=_sha(data), section=sections[i % len(sections)])


def _mk(root: Path, n=6, sections=("1",), db_name="m.db"):
    root.mkdir(parents=True, exist_ok=True)
    db = root / db_name
    init_db(db)
    themes = root / "themes"
    _fill(db, themes, n, sections)
    return db, themes


def _broken(c, tmdb, *, folder=None, kind="hardlink", rk=None, tp=None, placed=NOW, section="1", edition=""):
    if not c.execute("SELECT 1 FROM local_files WHERE tmdb_id = ? AND section_id = ? AND edition_key = ?",
                     (tmdb, section, edition)).fetchone():
        _lf(c, tmdb, size=10, sha="0" * 64, present=0, section=section, edition=edition)
    if folder is not None:
        c.execute("INSERT INTO placements (media_type, tmdb_id, section_id, media_folder, placement_kind, "
                  " provenance, placed_at, plex_rating_key, theme_present, edition_key) "
                  "VALUES ('movie', ?, ?, ?, ?, 'manual', ?, ?, ?, ?)",
                  (tmdb, section, folder, kind, placed, rk, tp, edition))


def _override(c, tmdb, url, *, section, edition=""):
    c.execute("INSERT INTO user_overrides (media_type, tmdb_id, youtube_url, set_at, section_id, edition_key) "
              "VALUES ('movie', ?, ?, ?, ?, ?)", (tmdb, url, NOW, section, edition))


def _report(db, themes):
    with get_conn(db) as conn:
        return ch.broken_canonical_report(conn, themes, plex_available=True)


def _changed_ids(db, themes):
    return sorted(r["tmdb_id"] for r in _report(db, themes)["changed"])


def _flags(db):
    with _conn(db) as c:
        return {t: (p, f, s) for t, p, f, s in c.execute(
            "SELECT tmdb_id, canonical_present, canonical_changed_candidate, canonical_hash_miss_sig FROM local_files")}


def _size(db, tmdb):
    with _conn(db) as c:
        return c.execute("SELECT file_size FROM local_files WHERE tmdb_id = ?", (tmdb,)).fetchone()[0]


def _count_stats(monkeypatch, root, fn):
    """os.stat calls under root while fn runs (verify's pool threads included)."""
    real = os.stat
    hits: list[str] = []
    lock = threading.Lock()
    root_s = str(root)

    def counting(path, *a, **kw):
        if not isinstance(path, int) and os.fspath(path).startswith(root_s):
            with lock:
                hits.append(os.fspath(path))
        return real(path, *a, **kw)
    monkeypatch.setattr(os, "stat", counting)
    try:
        out = fn()
    finally:
        monkeypatch.setattr(os, "stat", real)
    return len(hits), out


def _counting_hash(monkeypatch, fail=None):
    calls: list[str] = []
    real = adopt._hash_file

    def h(path):
        calls.append(str(path))
        if fail is not None and fail[0]:
            raise OSError(errno.EIO, "Input/output error")
        return real(path)
    monkeypatch.setattr(adopt, "_hash_file", h)
    return calls


def _bump_mtime(p: Path):
    st = os.stat(p)
    os.utime(p, ns=(st.st_atime_ns, st.st_mtime_ns + 5_000_000_000))


# ── T1-T3: CHANGED is the last check's candidates, re-read live ──────

def test_a_page_open_stats_the_last_checks_candidates_not_the_tree(tmp_path, monkeypatch):
    db, themes = _mk(tmp_path, n=40)
    _write(themes, 3, b"y" * 999)
    _write(themes, 7, b"z" * 5)
    plex_enum.verify_canonical_health(db, themes)
    n, rep = _count_stats(monkeypatch, themes, lambda: _report(db, themes))
    assert sorted(r["tmdb_id"] for r in rep["changed"]) == [3, 7]
    assert n == 2, f"a page open statted {n} canonicals for 2 candidates"


def test_a_writer_restamp_clears_a_listed_row_at_once_even_at_the_same_values(tmp_path):
    db, themes = _mk(tmp_path, n=5)
    orig = (themes / _rel(1)).read_bytes()
    _write(themes, 1, b"t" * 50)  # an external truncation
    plex_enum.verify_canonical_health(db, themes)
    assert _changed_ids(db, themes) == [1]
    _write(themes, 1, orig)  # a writer puts the recorded bytes back and restamps the SAME size and sha
    _exec(db, "UPDATE local_files SET file_size = ?, file_sha256 = ? WHERE tmdb_id = 1", (len(orig), _sha(orig)))
    assert _changed_ids(db, themes) == [], "no check ran — the flagged row's live size decides"
    _write(themes, 2, b"q" * 77)
    plex_enum.verify_canonical_health(db, themes)
    assert _changed_ids(db, themes) == [2]
    new = b"n" * 333  # a re-download at another size, stamped with it
    _write(themes, 2, new)
    _exec(db, "UPDATE local_files SET file_size = ?, file_sha256 = ? WHERE tmdb_id = 2", (len(new), _sha(new)))
    assert _changed_ids(db, themes) == []


def test_an_external_change_waits_for_the_next_check_and_another_sections_check_keeps_it(tmp_path):
    db, themes = _mk(tmp_path, n=6, sections=("1", "2"))
    plex_enum.verify_canonical_health(db, themes)
    assert _changed_ids(db, themes) == []
    _write(themes, 4, b"w" * 55)  # tmdb 4 is in section "1"
    assert _changed_ids(db, themes) == [], "a page open reads the last check, by design"
    plex_enum.verify_canonical_health(db, themes)
    assert _changed_ids(db, themes) == [4]
    res = plex_enum.verify_canonical_health(db, themes, section_ids=["2"])
    assert res["checked"] == 3, "the scoped pass stamped its own section's rows"
    assert _changed_ids(db, themes) == [4], "a check of another section keeps this row's candidate"


# ── T4: one stat per row, the same missing / skipped split ───────────

def test_verify_makes_one_stat_per_row(tmp_path, monkeypatch):
    db, themes = _mk(tmp_path, n=10)
    with _conn(db) as c:
        _lf(c, 90)  # no file on disk
        _lf(c, 91)
    n, res = _count_stats(monkeypatch, themes, lambda: plex_enum.verify_canonical_health(db, themes))
    assert res == {"checked": 12, "missing": 2, "skipped": 0}
    assert n == 12 + 1, f"{n} stats for 12 rows + the root probe"


def test_one_stat_keeps_the_missing_and_skipped_split(tmp_path, monkeypatch):
    db, themes = _mk(tmp_path, n=8)
    with _conn(db) as c:
        _lf(c, 9)
        c.execute("UPDATE local_files SET file_path = ? WHERE tmdb_id = 9", ("movies/9/the\x00me.mp3",))  # stat: ValueError
    _exec(db, "UPDATE local_files SET canonical_present = 1, canonical_changed_candidate = 1, "
              "canonical_hash_miss_sig = 'prior'")
    (themes / _rel(3)).unlink()
    (themes / _rel(3)).mkdir()  # a directory where the file should be
    _write(themes, 4, b"")  # a 0-byte stub
    faults = {str(themes / _rel(1)): errno.EACCES, str(themes / _rel(2)): errno.EIO,
              str(themes / _rel(5)): errno.ENOENT, str(themes / _rel(6)): errno.ELOOP}
    real = os.stat

    def faulty(path, *a, **kw):
        err = None if isinstance(path, int) else faults.get(os.fspath(path))
        if err is not None:
            raise OSError(err, os.strerror(err), os.fspath(path))
        return real(path, *a, **kw)
    monkeypatch.setattr(os, "stat", faulty)
    try:
        res = plex_enum.verify_canonical_health(db, themes)
    finally:
        monkeypatch.setattr(os, "stat", real)
    assert res == {"checked": 7, "missing": 5, "skipped": 2}, res
    flags = _flags(db)
    assert flags[1] == flags[2] == (1, 1, "prior"), "an unreadable canonical keeps its last result whole"
    assert [flags[t] for t in (3, 4, 5, 6, 9)] == [(0, None, None)] * 5
    assert flags[7] == flags[8] == (1, None, None)


# ── T5-T7: the heal and the hash-miss memo ───────────────────────────

def test_a_healed_row_leaves_the_candidates(tmp_path, monkeypatch):
    db, themes = _mk(tmp_path, n=3)
    levelled = (themes / _rel(2)).read_bytes() + b"APETAGEX"
    _write(themes, 2, levelled)
    _exec(db, "UPDATE local_files SET file_sha256 = ? WHERE tmdb_id = 2", (_sha(levelled),))  # a pre-.338 level
    plex_enum.verify_canonical_health(db, themes)
    assert _size(db, 2) == len(levelled) and _flags(db)[2] == (1, None, None)
    n, rep = _count_stats(monkeypatch, themes, lambda: _report(db, themes))
    assert rep["changed"] == [] and n == 0


def test_a_missed_heal_hash_is_not_repeated_until_the_file_or_the_record_moves(tmp_path, monkeypatch):
    db, themes = _mk(tmp_path, n=3)
    _write(themes, 1, b"z" * 400)
    fail = [False]
    calls = _counting_hash(monkeypatch, fail)
    counts = []

    def check():
        plex_enum.verify_canonical_health(db, themes)
        counts.append(len(calls))
    check()  # the first miss
    first_sig = _flags(db)[1][2]
    check()  # remembered…
    check()  # …and still remembered: a memo hit keeps the sig it matched
    assert first_sig and _flags(db)[1][2] == first_sig, "a memo hit must not forget the miss it matched"
    _bump_mtime(themes / _rel(1))
    check()  # the file moved
    check()  # remembered
    check()  # remembered
    _exec(db, "UPDATE local_files SET file_sha256 = ? WHERE tmdb_id = 1", ("e" * 64,))
    check()  # the recorded sha moved
    fail[0] = True
    _write(themes, 1, b"w" * 401)
    check()  # a failed hash…
    fail[0] = False
    check()  # …is retried
    check()  # and that miss is remembered
    assert counts == [1, 1, 1, 2, 2, 2, 3, 4, 5, 5], counts
    _exec(db, "UPDATE local_files SET file_size = 399 WHERE tmdb_id = 1")
    check()  # the recorded size moved
    assert counts[-1] == 6, counts
    assert _changed_ids(db, themes) == [1]


def test_new_bytes_at_the_same_size_with_the_mtime_put_back_are_hashed_again(tmp_path, monkeypatch):
    db, themes = _mk(tmp_path, n=2)
    p = themes / _rel(1)
    fixed = b"b" * 300
    _exec(db, "UPDATE local_files SET file_sha256 = ? WHERE tmdb_id = 1", (_sha(fixed),))
    _write(themes, 1, b"a" * 300)  # the recorded sha is not these bytes: a miss
    calls = _counting_hash(monkeypatch)
    plex_enum.verify_canonical_health(db, themes)
    assert len(calls) == 1 and _changed_ids(db, themes) == [1]
    st = os.stat(p)
    p.write_bytes(fixed)  # the recorded bytes land at the same size…
    os.utime(p, ns=(st.st_atime_ns, st.st_mtime_ns))  # …and the mtime is put back
    now = os.stat(p)
    assert (now.st_size, now.st_mtime_ns) == (st.st_size, st.st_mtime_ns) and now.st_ctime_ns != st.st_ctime_ns
    plex_enum.verify_canonical_health(db, themes)
    assert len(calls) == 2, "only ctime says these bytes moved"
    assert _size(db, 1) == len(fixed) and _changed_ids(db, themes) == [], "the heal ran on the new bytes"


def test_the_hash_memo_never_blocks_the_338_heal(tmp_path, monkeypatch):
    db, themes = _mk(tmp_path, n=3)
    grown = (themes / _rel(1)).read_bytes() + b"APETAGEX"
    _write(themes, 1, grown)
    _exec(db, "UPDATE local_files SET file_sha256 = ? WHERE tmdb_id = 1", (_sha(grown),))
    calls = _counting_hash(monkeypatch)
    plex_enum.verify_canonical_health(db, themes)
    assert len(calls) == 1 and _changed_ids(db, themes) == []
    assert _size(db, 1) == len(grown) and _flags(db)[1] == (1, None, None), "healed on the first check"
    swapped = b"s" * 777
    _write(themes, 2, swapped)
    plex_enum.verify_canonical_health(db, themes)
    assert _flags(db)[2][1] == 1 and _flags(db)[2][2], "a genuine change: listed, and its miss remembered"
    # a writer then stamps the sha of these bytes but not their size — the remembered miss must not stop the heal
    _exec(db, "UPDATE local_files SET file_sha256 = ? WHERE tmdb_id = 2", (_sha(swapped),))
    plex_enum.verify_canonical_health(db, themes)
    assert _size(db, 2) == len(swapped) and _flags(db)[2] == (1, None, None)
    assert _changed_ids(db, themes) == []


# ── T8-T10: the report's statements, the batched pick, the override ──

def _traced(monkeypatch, fn) -> int:
    lines: list[str] = []
    real = sqlite3.connect

    def connect(*a, **kw):
        conn = real(*a, **kw)
        conn.set_trace_callback(lines.append)
        return conn
    monkeypatch.setattr(sqlite3, "connect", connect)
    try:
        fn()
    finally:
        monkeypatch.setattr(sqlite3, "connect", real)
    return len(lines)


def test_report_and_restore_list_statements_do_not_grow_with_broken_rows(tmp_path, monkeypatch):
    db, themes = _mk(tmp_path, n=2)
    plexdir = tmp_path / "plex"

    def add(lo, hi):
        with _conn(db) as c:
            for t in range(lo, hi):
                _broken(c, t, folder=str(plexdir / f"gone{t}"), tp=0)
                if t % 3 == 0:
                    _broken(c, t, folder="", kind="plex_upload", rk=str(9000 + t))
                _override(c, t, "https://y/o", section="1" if t % 2 else "")

    def counts():
        def helper():
            with get_conn(db) as conn:
                ch._broken_rows_with_placement(conn)
        return _traced(monkeypatch, lambda: _report(db, themes)), _traced(monkeypatch, helper)
    add(500, 503)
    small = counts()
    add(600, 640)
    assert counts() == small, "a SELECT per broken row is back"
    assert small == (7, 5), "3 PRAGMAs + the report's 4 SELECTs / the restore list's 2"


def test_the_batched_pick_is_the_per_row_pick(tmp_path):
    db, themes = _mk(tmp_path, n=1)
    plexdir = tmp_path / "plex"
    live = plexdir / "303-live"
    live.mkdir(parents=True)
    (live / "theme.mp3").write_bytes(b"sidecar")
    gone = {k: str(plexdir / k) for k in ("301-missing", "301-unverified", "302-dead", "303-dead", "Z-tie", "A-tie")}
    with _conn(db) as c:
        _broken(c, 301, folder=gone["301-missing"], tp=0, placed="2026-09-02T00:00:00")
        _broken(c, 301, folder=gone["301-unverified"], tp=None, placed="2026-09-01T00:00:00")
        _broken(c, 302, folder=gone["302-dead"], tp=1)
        _broken(c, 302, folder="", kind="plex_upload", rk="9302", tp=None)
        _broken(c, 303, folder=gone["303-dead"], tp=1, placed="2026-09-03T00:00:00")
        _broken(c, 303, folder=str(live), tp=None, placed="2026-09-01T00:00:00")
        _broken(c, 304)
        # a theme_present + placed_at tie, inserted Z first: only the media_folder tie-break orders them
        _broken(c, 305, folder=gone["Z-tie"], tp=1, placed=NOW)
        _broken(c, 305, folder=gone["A-tie"], tp=1, placed=NOW)
    picks = {}
    with get_conn(db) as conn:
        batch = ch._broken_placements(conn)
        for r in ch._broken_rows(conn):
            p1, s1 = ch._pick_placement(batch.get(ch._row_key(r), []))
            p2, s2 = ch._placement_for(conn, r)
            one = (p1["media_folder"] if p1 else None, s1)
            assert one == (p2["media_folder"] if p2 else None, s2), r["tmdb_id"]
            picks[r["tmdb_id"]] = one
    assert picks == {301: (gone["301-unverified"], False),  # unverified outranks verified-missing (v0.51.341)
                     302: ("", False),                      # the store over a dead folder
                     303: (str(live), True),                # the surviving sidecar over a dead folder
                     304: (None, False),
                     305: (gone["A-tie"], False)}
    # the PK index already scans media_folder in order — an unindexed placements (a TEMP table shadows the name)
    # scans Z first, so only the ORDER BY's own tie-break can pick A, in both queries
    with get_conn(db) as conn:
        conn.execute("CREATE TEMP TABLE placements AS SELECT * FROM main.placements ORDER BY media_folder DESC")
        r305 = next(r for r in ch._broken_rows(conn) if r["tmdb_id"] == 305)
        batched, _s = ch._pick_placement(ch._broken_placements(conn)[ch._row_key(r305)])
        per_row, _s = ch._placement_for(conn, r305)
    assert (batched["media_folder"], per_row["media_folder"]) == (gone["A-tie"], gone["A-tie"])


def test_the_override_resolves_section_first_then_global_edition_scoped(tmp_path):
    db, themes = _mk(tmp_path, n=1)
    with _conn(db) as c:
        for t, ed in ((201, ""), (202, ""), (203, ""), (204, "ext"), (205, "")):
            _broken(c, t, edition=ed)
        _override(c, 201, "https://y/section", section="1")
        _override(c, 201, "https://y/global", section="")
        _override(c, 202, "https://y/global", section="")
        _override(c, 203, "", section="1")  # a section row wins even with an empty URL, as fetchone() did
        _override(c, 203, "https://y/global", section="")
        _override(c, 204, "https://y/standard-cut", section="1", edition="")
        _override(c, 205, "https://y/other-section", section="2")
    with get_conn(db) as conn:
        rows = {r["tmdb_id"]: r for r in ch._broken_rows(conn)}
    assert {t: r["override_url"] for t, r in rows.items()} == {
        201: "https://y/section", 202: "https://y/global", 203: "", 204: None, 205: None}
    assert {t: ch.classify_repair(r) for t, r in rows.items()} == {
        201: "redownload", 202: "redownload", 203: "canonical_missing", 204: "canonical_missing",
        205: "canonical_missing"}


# ── T11-T12: the v80 migration and the restore reset ─────────────────

def test_v80_migration_adds_the_columns_and_clears_the_old_check_stamps(tmp_path, caplog):
    import app.core.db as dbm
    caplog.set_level(logging.INFO)
    db = tmp_path / "m.db"
    init_db(db)
    with _conn(db) as c:
        _lf(c, 1, size=3, sha="a" * 64, present=1)
        _lf(c, 2, size=4, sha="b" * 64, present=0)
        c.execute("UPDATE local_files SET canonical_health_checked_at = ?", (NOW,))
    with _conn(db) as c:  # the v79 shape
        c.execute("ALTER TABLE local_files DROP COLUMN canonical_changed_candidate")
        c.execute("ALTER TABLE local_files DROP COLUMN canonical_hash_miss_sig")
        c.execute("DELETE FROM schema_version")
        c.execute("INSERT INTO schema_version (version, applied_at) VALUES (79, ?)", (NOW,))
    init_db(db)
    init_db(db)
    with _conn(db) as c:
        cols = {r[1] for r in c.execute("PRAGMA table_info(local_files)")}
        assert {"canonical_changed_candidate", "canonical_hash_miss_sig"} <= cols
        assert c.execute("SELECT MAX(version) FROM schema_version").fetchone()[0] == dbm.CURRENT_SCHEMA_VERSION
        assert c.execute("SELECT tmdb_id, canonical_health_checked_at, canonical_present FROM local_files "
                         "ORDER BY tmdb_id").fetchall() == [(1, None, 1), (2, None, 0)], "BROKEN keeps its stamps"
    with get_conn(db) as conn:
        ck = ch.broken_canonical_report(conn)["checked"]
    assert ck["never"] == ck["tracked"] == 2
    assert sum("cleared 2 check stamp(s)" in r.getMessage() for r in caplog.records) == 1, "the cold path says what it did"
    c = sqlite3.connect(db)
    try:
        dbm._migrate_v79_to_v80(c)  # a crash between the column adds and the version stamp re-runs the step
        c.commit()
    finally:
        c.close()


class _Stop(Exception):
    pass


def _boot(monkeypatch, cd: Path):
    """main() through init_db and the canonical-health hook, stopped at _bootstrap_config_file."""
    from app import config as config_mod
    from app import main as main_mod
    monkeypatch.setattr(config_mod, "_DEFAULT_CONFIG_DIR", cd)
    monkeypatch.setattr(main_mod, "get_settings", lambda: config_mod.Settings(config_dir=cd, data_dir=cd / "data"))
    monkeypatch.setattr(main_mod, "configure_logging", lambda *a, **k: None)

    def stop(settings):
        raise _Stop()
    monkeypatch.setattr(main_mod, "_bootstrap_config_file", stop)
    with pytest.raises(_Stop):
        main_mod.main()
    return main_mod


def _live_library(tmp_path, monkeypatch):
    """A checked backup snapshot, then a motif write on the live library that the snapshot does not know about."""
    cd = tmp_path / "cfg"
    cd.mkdir()
    monkeypatch.setenv("MOTIF_COOKIES_FILE", str(cd / "cookies.txt"))
    src, themes = _mk(tmp_path / "src", n=3)
    plex_enum.verify_canonical_health(src, themes)
    _exec(src, "UPDATE local_files SET canonical_changed_candidate = 1, canonical_hash_miss_sig = 'backup-era' "
               "WHERE tmdb_id = 3")  # what that backup's own last check left behind
    snap = tmp_path / "snap.db"
    db_backup.vacuum_into(src, snap)
    live = cd / "motif.db"
    db_backup.vacuum_into(src, live)
    new = b"y" * 250
    _write(themes, 2, new)
    _exec(live, "UPDATE local_files SET file_size = ?, file_sha256 = ? WHERE tmdb_id = 2", (len(new), _sha(new)))
    return cd, live, snap, themes


def _main_lines(caplog, level, needle):
    return [r.getMessage() for r in caplog.records
            if r.name == "motif.main" and r.levelno == level and needle in r.getMessage()]


def test_a_staged_database_restore_forgets_the_backups_check_results_at_boot(tmp_path, monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    cd, live, snap, themes = _live_library(tmp_path, monkeypatch)
    assert _changed_ids(live, themes) == []
    db_backup.stage_restore(live, snap)
    _boot(monkeypatch, cd)
    rep = _report(live, themes)
    assert rep["checked"]["never"] == rep["checked"]["tracked"] == 3, "the backup's checks no longer read as current"
    assert rep["changed"] == []
    assert {t: f[1:] for t, f in _flags(live).items()} == dict.fromkeys((1, 2, 3), (None, None))
    assert [f[0] for f in _flags(live).values()] == [1, 1, 1], "canonical_present stays for BROKEN and the DL sort"
    assert len(_main_lines(caplog, logging.WARNING, "Canonical health: set aside 3 check result(s)")) == 1
    plex_enum.verify_canonical_health(live, themes)
    assert _changed_ids(live, themes) == [2], "the restored record no longer matches the disk"


def test_a_boot_without_a_restore_keeps_the_check_results(tmp_path, monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    cd, live, _snap, themes = _live_library(tmp_path, monkeypatch)
    plex_enum.verify_canonical_health(live, themes)
    _boot(monkeypatch, cd)
    assert _report(live, themes)["checked"]["never"] == 0
    assert not _main_lines(caplog, logging.WARNING, "Canonical health: set aside")


def test_a_boot_whose_clear_fails_logs_and_carries_on(tmp_path, monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    cd, live, snap, themes = _live_library(tmp_path, monkeypatch)
    db_backup.stage_restore(live, snap)
    from app import main as main_mod

    def broken(db_path):
        raise sqlite3.OperationalError("disk I/O error")
    monkeypatch.setattr(main_mod, "forget_canonical_checks", broken)
    _boot(monkeypatch, cd)  # reached _bootstrap_config_file
    assert len(_main_lines(caplog, logging.ERROR, "could not clear the restored database's check results")) == 1


# ── T13: the payloads ────────────────────────────────────────────────

@pytest.fixture
def app_env(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.web import api as api_mod
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="testadmin", password="testpassword")
    monkeypatch.setattr(api_mod, "log_event", lambda *a, **k: None)
    with api_mod._CANON_RESTORE_LOCK:
        api_mod._CANON_RESTORE_STATE.clear()
        api_mod._CANON_RESTORE_STATE["status"] = "idle"
    return api_mod, settings, tmp_path


def test_the_report_says_how_fresh_it_is_and_the_check_says_what_it_read(app_env):
    api_mod, settings, tmp_path = app_env
    themes = tmp_path / "themes"
    _fill(settings.db_path, themes, 4)
    settings._cfg.paths.themes_dir = str(themes)
    client = TestClient(api_mod.create_app(settings))
    rep = client.get(REPORT, headers=AUTH).json()
    assert rep["checked"] == {"tracked": 4, "never": 4, "oldest": None, "newest": None}
    before = now_iso()
    r = client.post(CHECK, headers=AUTH)
    after = now_iso()
    assert r.status_code == 200, r.text
    chk = r.json()
    assert chk["check"] == {"checked": 4, "missing": 0, "skipped": 0}
    ck = chk["checked"]
    assert (ck["tracked"], ck["never"]) == (4, 0)
    assert before <= ck["oldest"] <= ck["newest"] <= after
    rep = client.get(REPORT, headers=AUTH).json()
    assert rep["checked"] == ck and "check" not in rep


def test_a_check_with_no_themes_dir_says_nothing_was_checked(app_env):
    api_mod, settings, tmp_path = app_env
    client = TestClient(api_mod.create_app(settings))
    r = client.post(CHECK, headers=AUTH)
    assert r.status_code == 200, r.text
    assert "check" in r.json() and r.json()["check"] is None


# ── T14: the page, under node ────────────────────────────────────────

def _ago(**delta) -> str:
    return (datetime.now(timezone.utc) - timedelta(**delta)).isoformat(timespec="seconds")


def _page(checked, check="absent"):
    rep = _page_report()
    rep["checked"] = checked
    if check != "absent":
        rep["check"] = check
    return rep


@pytest.mark.skipif(not _NODE, reason="node not installed")
def test_the_freshness_line_and_the_all_clear_say_what_the_last_check_covered(tmp_path):
    never = {"tracked": 3, "never": 3, "oldest": None, "newest": None}
    partial = {"tracked": 5, "never": 2, "oldest": _ago(minutes=90), "newest": _ago(minutes=10)}
    full = dict(partial, never=0)
    stale = {"tracked": 5, "never": 0, "oldest": _ago(hours=30), "newest": _ago(minutes=10)}
    empty = {"tracked": 0, "never": 0, "oldest": None, "newest": None}
    s0, s1, s2, s3, s4 = _run_page(tmp_path, [_page(never), {"status": "idle"}, _page(partial), _page(full),
                                              _page(stale), _page(empty)], ["canon-check-btn"] * 4)
    f = s0["canon-freshness"]
    assert f["display"] == "" and f["className"] == "form-hint form-hint-warn"
    assert f["text"].startswith("Not checked yet — // RUN CHECK compares every theme on disk"), f["text"]
    assert s0["canon-clear-block"]["display"] == "none", "a never-checked library shows no ✓"
    f = s1["canon-freshness"]
    assert (f["text"], f["className"]) == ("As of the last check — the oldest result is 1h ago · 2 not checked yet. "
                                           "A file changed after its check shows at the next one.", "form-hint")
    assert f["title"].startswith("oldest ") and " · newest " in f["title"]
    assert s1["canon-clear-block"]["display"] == ""
    assert s1["canon-clear-text"]["text"] == "✓ Nothing missing or changed among the 3 checked. Nothing to repair."
    assert s2["canon-freshness"]["text"] == ("As of the last check — the oldest result is 1h ago. "
                                             "A file changed after its check shows at the next one.")
    assert s2["canon-clear-text"]["text"] == ("✓ Every tracked canonical was present at its recorded size when last "
                                              "checked. Nothing to repair.")
    f = s3["canon-freshness"]
    assert f["className"] == "form-hint form-hint-warn"
    assert f["text"].endswith(" · over a day old — // RUN CHECK re-reads every theme."), f["text"]
    assert s4["canon-freshness"]["display"] == "none" and s4["canon-clear-block"]["display"] == ""


@pytest.mark.skipif(not _NODE, reason="node not installed")
def test_run_check_names_no_themes_dir_a_dead_root_and_a_partial_run(tmp_path):
    ck = {"tracked": 5, "never": 0, "oldest": _ago(minutes=1), "newest": _ago(minutes=1)}
    snaps = _run_page(tmp_path, [
        _page(ck), {"status": "idle"},
        _page(ck, check=None),
        _page(ck, check={"checked": 0, "missing": 0, "skipped": 5}),
        _page(ck, check={"checked": 3, "missing": 0, "skipped": 2}),
        _page(ck, check={"checked": 5, "missing": 0, "skipped": 0}),
    ], ["canon-check-btn"] * 4)
    got = [(s["canon-check-status"]["text"], s["canon-check-status"]["className"]) for s in snaps[1:]]
    assert got == [
        ("✗ no themes directory configured — nothing was checked", "form-status form-status-fail"),
        ("✗ the themes directory did not answer — nothing was re-read; the results below are from the last check",
         "form-status form-status-fail"),
        ("checked 3 of 5 — the rest could not be read and keep their last result", "form-status warn"),
        ("✓ check complete", "form-status form-status-ok"),
    ]
