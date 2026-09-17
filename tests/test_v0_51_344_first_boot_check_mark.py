"""v0.51.344 integration fix R1-F12: the first start on this build sets earlier check results aside in its own words.

  .342/.343 stamped checks WITH CHANGED candidates but recorded no check mark, so the first .344 start sets every stamp,
  CHANGED candidate and hash-miss memo aside (the accepted one-time trade). That boot now logs an INFO event that says so —
  the page and the event stream explain the empty CHANGED list — and the 'a build without CHANGED candidates wrote them'
  WARNING stays for the case it describes: a stamp past a mark this build recorded (a .341 rollback).
"""
from __future__ import annotations

import logging

from app.core import plex_enum
from tests.test_v0_51_342_canonical_health_changed import _boot, _changed_ids, _exec, _main_lines, _report, _write
from tests.test_v0_51_344_canonical_rollback_stamps import _later, _library, _mark


def _boot_events(monkeypatch):
    from app import main as main_mod
    events: list[dict] = []
    monkeypatch.setattr(main_mod, "log_event", lambda _db, **k: events.append(k))
    return events


def test_the_first_start_on_this_build_says_so_as_an_event_and_never_blames_a_rollback(tmp_path, monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    cd, db, themes = _library(tmp_path, monkeypatch)
    _write(themes, 3, b"z" * 90)
    plex_enum.verify_canonical_health(db, themes)
    assert _changed_ids(db, themes) == [3], "premise: a .343-style check listed a CHANGED candidate"
    _exec(db, "DELETE FROM runtime_settings WHERE key = ?", (plex_enum.CANONICAL_CHECK_MARK_KEY,))  # .342/.343 wrote no mark
    events = _boot_events(monkeypatch)
    _boot(monkeypatch, cd)
    rep = _report(db, themes)
    assert (rep["checked"]["never"], rep["changed"]) == (3, []), "premise: the one-time set-aside"
    assert _main_lines(caplog, logging.WARNING, "Canonical health") == [], \
        "a build WITH CHANGED candidates was blamed as one without"
    (ev,) = events
    assert (ev["level"], ev["component"]) == ("INFO", "main")
    assert "set aside 3 check result(s)" in ev["message"] and "first time" in ev["message"], ev["message"]
    assert "RUN CHECK" in ev["message"] and "CHANGED is empty" in ev["message"], ev["message"]
    assert "without CHANGED candidates" not in ev["message"]
    # the next boot has nothing to set aside: no event, the usual INFO line
    caplog.clear()
    events.clear()
    _boot(monkeypatch, cd)
    assert events == [] and _main_lines(caplog, logging.WARNING, "Canonical health") == []
    assert len(_main_lines(caplog, logging.INFO, "no check stamp is newer")) == 1


def test_a_stamp_past_a_mark_this_build_recorded_keeps_the_rollback_warning_as_a_log_line(tmp_path, monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    cd, db, themes = _library(tmp_path, monkeypatch)
    plex_enum.verify_canonical_health(db, themes)
    mark = _mark(db)
    assert mark, "premise: this build's check recorded its mark"
    # the .341 check's stamp: checked_at only, past the mark, never a CHANGED candidate
    _exec(db, "UPDATE local_files SET canonical_present = 1, canonical_health_checked_at = ? WHERE tmdb_id = 2",
          (_later(hours=1),))
    events = _boot_events(monkeypatch)
    _boot(monkeypatch, cd)
    (warned,) = _main_lines(caplog, logging.WARNING, "Canonical health: set aside 1 check stamp(s)")
    assert "a build without CHANGED candidates wrote them" in warned and mark in warned, warned
    assert events == [], "the rollback case is the WARNING log line, not the first-start event"
    assert _report(db, themes)["checked"]["never"] == 1
