"""v0.51.344: RESTORE FROM PLEX's pool under faults — the stop rule's window, a fetch that raises, a publish that raises.

  PB-025  the no-answer window runs between request starts, and a request sent before Plex last answered is not counted.
  PB-027  a fetch whose answer raises skips its row (plex_error:) and every other fetched body is still published.
  PB-030  a publish that raises wakes the pool's backoffs — the failure is not held up by a sleep, nor followed by fetches.
"""
from __future__ import annotations

import functools
import logging
import sqlite3
import threading

import pytest

from app.core import canonical_health as ch
from test_v0_51_342_restore_pool import SHARED, FakeClock, FakePlex, Meter, _seed_store


@pytest.fixture
def clock(monkeypatch):
    c = FakeClock()
    monkeypatch.setattr(ch, "_PlexGate", functools.partial(ch._PlexGate, clock=c, sleep=c.sleep))
    return c


# ── PB-025: the window runs between request starts ──────────────────

@pytest.mark.parametrize("caller", ["pool", "shared_tail"])
def test_a_refusal_then_a_hung_connect_inside_the_window_does_not_end_the_run(tmp_path, clock, caller):
    n = 20
    # v0.51.344: both callers of gate.after() — the pool's work() and the shared-path tail's fetch_now()
    db, themes = _seed_store(tmp_path, n=n, shared=tuple(range(601, 601 + n)) if caller == "shared_tail" else ())
    calls: list[tuple[float, float, bool]] = []

    def no_answer():
        sent = clock()
        if 30.0 <= sent < 40.0:
            clock.t += 30.0  # a connect that hangs until the client's timeout
        calls.append((sent, clock(), sent < 40.0))
        return sent < 40.0
    res = ch.restore_from_plex(db, themes, FakePlex(no_answer=no_answer))
    silent = [c for c in calls if c[2]]
    assert len(silent) >= ch._PLEX_TRIP_AFTER, "premise: more no-answers in a row than the count alone allows"
    assert silent[-1][1] - silent[0][1] >= ch._PLEX_TRIP_WINDOW_S, "premise: the last no-answer came back a window after the first"
    assert silent[-1][0] - silent[0][0] < ch._PLEX_TRIP_WINDOW_S, "premise: the outage itself was shorter than the window"
    reasons = [s["reason"] for s in res["skipped"]]
    assert res["plex_unreachable"] is False and "plex_unreachable" not in reasons, \
        "a 40 s outage whose last request hung to its timeout ended the run"
    if caller == "pool":
        assert res["restored_store"] == n - len(silent) > 0
    else:
        # one shared row is written; every later row on that path finds it present
        assert res["restored_store"] == 1
        assert reasons.count("canonical_already_present") == n - len(silent) - 1 > 0


def test_a_no_answer_sent_before_plex_last_answered_does_not_start_the_next_streak():
    c = FakeClock()
    gate = ch._PlexGate(clock=c, sleep=c.sleep)
    c.t = 50.0
    gate.after(None, 49.0)
    c.t = 80.0
    gate.after("plex_themes:transport", 20.0)  # sent at 20, before the answer at 50; hung until 80
    for k in range(ch._PLEX_TRIP_AFTER):
        sent = 85.0 + k * 10.0 / (ch._PLEX_TRIP_AFTER - 1)
        c.t = sent + 0.1
        gate.after("plex_themes:transport", sent)
    assert gate.tripped is False, "a request sent before Plex's last answer dated the streak 65 s early"
    c.t = 146.0
    gate.after("plex_fetch:transport", 85.0 + ch._PLEX_TRIP_WINDOW_S)
    assert gate.tripped is True, "premise: the streak itself still stops the run once it spans the window"


# ── PB-027: a fetch that raises ──────────────────────────────────────

class Malformed(FakePlex):
    """A Plex that answers one rating key with a body whose MediaContainer is not an object."""
    def __init__(self, meter=None, *, bad="9604", **kw):
        super().__init__(meter, **kw)
        self.bad = bad

    def get_themes(self, *, rating_key):
        out = super().get_themes(rating_key=rating_key)
        if rating_key == self.bad:
            return {"ok": True, "http_status": 200, "error": None, "body": {"MediaContainer": "not-a-dict"}}
        return out


@pytest.mark.parametrize("pooled", [False, True], ids=["serial", "pooled"])
def test_a_fetch_that_raises_skips_its_row_and_every_other_body_is_published(tmp_path, caplog, pooled):
    n = 12
    db, themes = _seed_store(tmp_path, n=n)
    meter = Meter()

    def make():
        return Malformed(meter, latency=0.02)
    with caplog.at_level(logging.WARNING, logger=ch.log.name):
        res = (ch.restore_from_plex(db, themes, None, workers=4, plex_client_factory=make) if pooled
               else ch.restore_from_plex(db, themes, make()))
    if pooled:
        assert meter.max_in_flight >= 2, "premise: the pooled run fetched side by side"
    assert [(s["tmdb_id"], s["reason"]) for s in res["skipped"]] == [(604, "plex_error:AttributeError")]
    written = sorted(p.parent.name for p in themes.rglob("theme.mp3"))
    assert meter.bodies == len(written) == res["restored_store"] == n - 1, "bodies fetched from Plex were never written"
    assert res["restored"] + len(res["skipped"]) + res["not_attempted"] == n
    assert any("raised AttributeError" in r.getMessage() and "604" in r.getMessage() for r in caplog.records), \
        "the skipped row left no warning naming what raised"


def test_a_shared_path_row_whose_fetch_raises_is_skipped_and_the_next_one_restores(tmp_path):
    db, themes = _seed_store(tmp_path, n=4, shared=(603, 604))
    res = ch.restore_from_plex(db, themes, None, workers=4, plex_client_factory=lambda: Malformed(bad="9603"))
    assert [(s["tmdb_id"], s["reason"]) for s in res["skipped"]] == [(603, "plex_error:AttributeError")]
    assert res["restored_store"] == 3 and (themes / SHARED).read_bytes() == b"store-9604"


# ── PB-030: a publish that raises ────────────────────────────────────

def test_a_publish_that_raises_wakes_the_backoffs_instead_of_sleeping_them_out(tmp_path, monkeypatch):
    workers, n = 4, 12
    db, themes = _seed_store(tmp_path, n=n)
    monkeypatch.setattr(ch, "_PLEX_BACKOFF_BASE_S", 30.0)
    monkeypatch.setattr(ch, "_PLEX_BACKOFF_CAP_S", 30.0)
    lock = threading.Lock()
    asleep = {"n": 0}
    others_asleep, all_asleep, raised = threading.Event(), threading.Event(), threading.Event()
    gates, late, premise = [], [], {}

    class Gate(ch._PlexGate):
        def __init__(self, *a, **k):
            super().__init__(*a, **k)
            gates.append(self)

        def _wait(self, s):
            with lock:
                asleep["n"] += 1
                if asleep["n"] >= workers - 1:
                    others_asleep.set()
                if asleep["n"] == workers:
                    all_asleep.set()
            try:
                super()._wait(s)
            finally:
                with lock:
                    asleep["n"] -= 1
    monkeypatch.setattr(ch, "_PlexGate", Gate)

    class Plex(FakePlex):
        def get_themes(self, *, rating_key):
            if raised.is_set():
                late.append(rating_key)
            if rating_key != "9601":
                return {"ok": False, "http_status": None, "error": "transport: ConnectError('refused')", "body": None}
            # the one answer lands once the other workers are backing off
            premise["others_asleep"] = others_asleep.wait(10)
            return super().get_themes(rating_key=rating_key)

    real = ch._stamp_restored

    def stamp(db_path, r, *a, **k):
        if r["tmdb_id"] == 601:
            premise["all_asleep"] = all_asleep.wait(10)
            raised.set()
            raise sqlite3.OperationalError("database is locked")
        return real(db_path, r, *a, **k)
    monkeypatch.setattr(ch, "_stamp_restored", stamp)
    out = {}

    def run():
        try:
            ch.restore_from_plex(db, themes, None, workers=workers, plex_client_factory=Plex)
        except sqlite3.OperationalError as e:
            out["err"] = e
    t = threading.Thread(target=run, daemon=True)
    t.start()
    try:
        assert raised.wait(15), "premise: the publish of the one answered row raised"
        t.join(5)
        ended = not t.is_alive()
    finally:
        for g in gates:
            g.cancelled.set()
        t.join(10)
    assert premise == {"others_asleep": True, "all_asleep": True}, "premise: every worker was backing off at the raise"
    assert ended, "the failed publish waited out the pool's backoffs before the run could fail"
    assert "locked" in str(out.get("err")), "the failed publish must fail the run"
    assert late == [], "Plex was asked for bytes after the publish failed"
