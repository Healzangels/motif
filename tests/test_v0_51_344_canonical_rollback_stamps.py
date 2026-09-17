"""v0.51.344: check stamps no v80-aware check vouched for are set aside at boot.

  1. A rollback to a build without CHANGED candidates (.341) stamps checks past the mark; the next boot sets those
     stamps aside so CANONICAL HEALTH stops reading all-clear over a changed file.
  2. The mark only moves forward, and with no mark every stamp goes once.
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timedelta, timezone

from app.core import canonical_health as ch
from app.core import plex_enum
from tests.test_v0_51_342_canonical_health_changed import (
    _boot, _changed_ids, _conn, _exec, _flags, _main_lines, _mk, _rel, _report, _write,
)


def _later(**delta) -> str:
    return (datetime.now(timezone.utc) + timedelta(**delta)).isoformat(timespec="seconds")


def _mark(db):
    with _conn(db) as c:
        row = c.execute("SELECT value FROM runtime_settings WHERE key = ?",
                        (plex_enum.CANONICAL_CHECK_MARK_KEY,)).fetchone()
    return row[0] if row else None


def _stamps(db):
    with _conn(db) as c:
        return dict(c.execute("SELECT tmdb_id, canonical_health_checked_at FROM local_files"))


def _library(tmp_path, monkeypatch, n=3, sections=("1",)):
    cd = tmp_path / "cfg"
    monkeypatch.setenv("MOTIF_COOKIES_FILE", str(cd / "cookies.txt"))
    db, themes = _mk(cd, n=n, sections=sections, db_name="motif.db")
    return cd, db, themes


def _sections(db):
    with _conn(db) as c:
        return dict(c.execute("SELECT tmdb_id, section_id FROM local_files"))


def _boot_keeps(monkeypatch, caplog, cd, db, themes, mark):
    """A boot after the check that wrote `mark`: every stamp stays and the boot names that mark."""
    kept = _stamps(db)
    _boot(monkeypatch, cd)
    assert _stamps(db) == kept, "a boot keeps every stamp a check this build ran wrote"
    assert _report(db, themes)["checked"]["never"] == 0
    assert _main_lines(caplog, logging.WARNING, "Canonical health: set aside") == []
    lines = _main_lines(caplog, logging.INFO, "no check stamp is newer")
    assert len(lines) == 1 and mark in lines[0], "the boot compared against the newest check's mark"
    assert ch.forget_unmarked_checks(db) == (0, mark)
    assert _stamps(db) == kept


def test_a_rolled_back_builds_stamps_are_set_aside_and_the_next_check_lists_the_change(tmp_path, monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    cd, db, themes = _library(tmp_path, monkeypatch)
    _write(themes, 3, b"z" * 90)  # a change this build's own check lists
    plex_enum.verify_canonical_health(db, themes)
    kept = _stamps(db)
    _write(themes, 2, b"x" * 40)  # the file changes while the rolled-back build runs
    # the .341 check's stamp: checked_at only, never canonical_changed_candidate
    _exec(db, "UPDATE local_files SET canonical_present = 1, canonical_health_checked_at = ? WHERE tmdb_id = 2",
          (_later(hours=1),))
    rep = _report(db, themes)
    assert (rep["checked"]["never"], _changed_ids(db, themes)) == (0, [3]), "premise: the false all-clear on 2"
    _boot(monkeypatch, cd)
    rep = _report(db, themes)
    assert (rep["checked"]["never"], _changed_ids(db, themes)) == (1, [3]), "a kept check keeps its CHANGED row"
    after = _stamps(db)
    assert after[2] is None and (after[1], after[3]) == (kept[1], kept[3]), "a check this build recorded keeps its stamp"
    assert len(_main_lines(caplog, logging.WARNING, "Canonical health: set aside 1 check stamp(s)")) == 1
    plex_enum.verify_canonical_health(db, themes)
    assert _changed_ids(db, themes) == [2, 3]


def test_the_mark_never_moves_back_when_an_earlier_check_commits_later(tmp_path, monkeypatch):
    db, themes = _mk(tmp_path, n=4, sections=("1", "2"))
    late, early = _later(hours=2), _later(hours=1)
    monkeypatch.setattr(plex_enum, "now_iso", lambda: late)
    plex_enum.verify_canonical_health(db, themes)
    monkeypatch.setattr(plex_enum, "now_iso", lambda: early)
    assert plex_enum.verify_canonical_health(db, themes, section_ids=["2"])["checked"] == 2
    assert _mark(db) == late
    assert ch.forget_unmarked_checks(db) == (0, late)
    assert all(_stamps(db).values())


def test_a_scoped_check_later_than_the_library_wide_one_moves_the_mark(tmp_path, monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    cd, db, themes = _library(tmp_path, monkeypatch, n=4, sections=("1", "2"))
    early, late = _later(hours=1), _later(hours=2)
    monkeypatch.setattr(plex_enum, "now_iso", lambda: early)
    plex_enum.verify_canonical_health(db, themes)
    monkeypatch.setattr(plex_enum, "now_iso", lambda: late)
    # the enum end pass's check: it always passes the walked sections
    assert plex_enum.verify_canonical_health(db, themes, section_ids=["2"])["checked"] == 2
    secs, by_section = _sections(db), {}
    for tmdb, stamp in _stamps(db).items():
        by_section.setdefault(secs[tmdb], set()).add(stamp)
    assert by_section == {"1": {early}, "2": {late}}, "premise: the scoped check stamped only its section, later"
    assert _mark(db) == late
    _boot_keeps(monkeypatch, caplog, cd, db, themes, late)


def test_a_check_that_finds_every_canonical_missing_still_moves_the_mark(tmp_path, monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    cd, db, themes = _library(tmp_path, monkeypatch)
    early, late = _later(hours=1), _later(hours=2)
    monkeypatch.setattr(plex_enum, "now_iso", lambda: early)
    plex_enum.verify_canonical_health(db, themes)
    (themes / _rel(1)).unlink()
    (themes / _rel(3)).unlink()
    _write(themes, 2, b"")  # a 0-byte download reads missing
    monkeypatch.setattr(plex_enum, "now_iso", lambda: late)
    res = plex_enum.verify_canonical_health(db, themes)
    assert res == {"checked": 3, "missing": 3, "skipped": 0}, "premise: every stamped row is missing, under the cap"
    assert _stamps(db) == dict.fromkeys((1, 2, 3), late)
    assert _mark(db) == late
    _boot_keeps(monkeypatch, caplog, cd, db, themes, late)
    assert _report(db, themes)["broken"] == 3, "the kept stamps keep the missing result"


def test_with_no_mark_every_stamp_is_set_aside_once(tmp_path, monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    cd, db, themes = _library(tmp_path, monkeypatch)
    _write(themes, 3, b"z" * 90)
    plex_enum.verify_canonical_health(db, themes)
    assert _changed_ids(db, themes) == [3], "premise: a live CHANGED candidate"
    _exec(db, "DELETE FROM runtime_settings WHERE key = ?", (plex_enum.CANONICAL_CHECK_MARK_KEY,))  # a pre-.344 check
    from app import main as main_mod
    events: list[dict] = []
    monkeypatch.setattr(main_mod, "log_event", lambda _db, **k: events.append(k))
    _boot(monkeypatch, cd)
    rep = _report(db, themes)
    assert rep["checked"]["never"] == rep["checked"]["tracked"] == 3
    # v0.51.344: 'Not checked yet' tells the page CHANGED is empty — a set-aside row keeps no check result.
    assert rep["changed"] == []
    assert {t: f[1:] for t, f in _flags(db).items()} == dict.fromkeys((1, 2, 3), (None, None))
    # v0.51.344: the first start on this build is not a rollback — an INFO event says so, no 'without CHANGED' WARNING
    assert _main_lines(caplog, logging.WARNING, "Canonical health: set aside") == []
    assert [(e["level"], e["component"]) for e in events] == [("INFO", "main")]
    assert "set aside 3 check result(s)" in events[0]["message"] and "first time" in events[0]["message"]
    assert ch.forget_unmarked_checks(db) == (0, None), "nothing left to set aside on the next boot"
    plex_enum.verify_canonical_health(db, themes)
    assert _mark(db) is not None and _report(db, themes)["checked"]["never"] == 0
    assert _changed_ids(db, themes) == [3], "the next check finds the change again"


def test_a_boot_whose_stamp_check_fails_logs_and_carries_on(tmp_path, monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    cd, _db, _themes = _library(tmp_path, monkeypatch)
    from app import main as main_mod

    def broken(db_path):
        raise sqlite3.OperationalError("disk I/O error")
    monkeypatch.setattr(main_mod, "forget_unmarked_checks", broken)
    _boot(monkeypatch, cd)  # reached _bootstrap_config_file
    assert len(_main_lines(caplog, logging.ERROR, "could not look for check stamps")) == 1
