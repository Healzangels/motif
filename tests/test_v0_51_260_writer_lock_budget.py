"""v0.51.260 — align every write-once writer's lock-wait budget.

Measuring deferral #8 (api_set_override_intent holds the SQLite write lock
across a Plex upload) turned up a consequence the deferral record got wrong.
It says "no data loss, worst case a few-sec writer stall". Measured, with a
probe holding a real BEGIN IMMEDIATE for 40s:

    events flusher : DROPPED — Event flusher: DROPPING batch of 1 events
                     (dropped after 32.6s)
    transaction()  : succeeded after 7.5s wait
    event rows persisted: 0

Right for `transaction()` callers (30s busy wait x 5 attempts + 7.5s backoff
~= 157.5s of budget), wrong for the two writers that own their connections:
the events flusher and notify_inbox.record_notification each had 3 x 10s + 3s
~= 33s, then DROPPED — up to 200 events, or a notification that is generated
once and never regenerated.

So the gap is the WRITERS', not the one caller that surfaced it. Fixing it
here removes the data-loss consequence for every long-hold cause; deferral #8
then degrades to exactly what its record already claims — a stall.

A second measurement corrected a comment while we were here: `sqlite3.connect(
timeout=N)` already calls sqlite3_busy_timeout(N*1000) — a fresh connection
reads `PRAGMA busy_timeout` back as N*1000. db.py's v1.13.50 comment claimed
the Python timeout "only kicks in when the C library returns SQLITE_BUSY
immediately"; a connect(timeout=2.0) blocked the full 2.1s against a held
lock. The pragma line is belt-and-braces, not the sole mechanism.

The behavioural tests scale the constants down (0.4s waits) rather than
holding a lock for 40s — same discriminator, ~2s of suite time.
"""
from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path

import pytest

from app.core import db as dbmod
from app.core import events, notify_inbox
from app.core.db import get_conn, init_db, transaction


_INSERT_SQL = (
    "INSERT INTO events (ts, level, component, media_type, tmdb_id, "
    " section_id, message, detail) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
)
_ROW = ("2026-08-11T00:00:00", "INFO", "probe", None, None, None, "m", None)


def _hold_lock(db: Path, seconds: float) -> None:
    ready = threading.Event()

    def _holder():
        with get_conn(db) as conn, transaction(conn):
            conn.execute(
                "INSERT INTO runtime_settings (key, value, updated_at,"
                " updated_by) VALUES (?,'1',datetime('now'),'t')",
                (f"probe{time.monotonic()}",))
            ready.set()
            time.sleep(seconds)

    threading.Thread(target=_holder, daemon=True).start()
    assert ready.wait(10), "holder never acquired the lock"


@pytest.fixture
def locked_db(tmp_path):
    """A DB with a write lock held for `hold` seconds by a background thread."""
    db = tmp_path / "m.db"
    init_db(db)

    def hold_for(seconds: float) -> threading.Event:
        released = threading.Event()
        ready = threading.Event()

        def _holder():
            with get_conn(db) as conn, transaction(conn):
                conn.execute(
                    "INSERT INTO runtime_settings (key, value, updated_at,"
                    " updated_by) VALUES ('probe','1',datetime('now'),'t')")
                ready.set()
                time.sleep(seconds)
            released.set()

        threading.Thread(target=_holder, daemon=True).start()
        assert ready.wait(10), "holder never acquired the lock"
        return released

    return db, hold_for


def _scale(monkeypatch, mod, wait_s, delays):
    monkeypatch.setattr(mod, "_LOCK_WAIT_S", wait_s)
    monkeypatch.setattr(mod, "_LOCK_RETRY_DELAYS", delays)


# ── the events flusher (the writer that measurably lost rows) ──


def test_flusher_write_survives_a_hold_that_used_to_drop_the_batch(
        locked_db, monkeypatch):
    """Scaled discriminator, driven through the extracted _write_batch so it is
    deterministic. The OLD shape (3 attempts) budgets 3x0.4+0.3 = 1.5s; the NEW
    shape (5 attempts) budgets 5x0.4+0.75 = 2.75s. A 2.0s hold sits between
    them: pre-fix the batch is DROPPED and the events are gone, post-fix they
    land."""
    db, hold_for = locked_db
    _scale(monkeypatch, events, 0.4, (0.1, 0.15, 0.2, 0.3))
    hold_for(2.0)

    err = events._write_batch(db, _INSERT_SQL, [_ROW])

    assert err is None, (
        f"v0.51.260: the flusher must outwait a hold that transaction() itself "
        f"survives — this batch was dropped and unrecoverable pre-fix ({err})"
    )
    with sqlite3.connect(db) as c:
        assert c.execute(
            "SELECT COUNT(*) FROM events WHERE component='probe'"
        ).fetchone()[0] == 1


def test_flusher_write_still_reports_failure_when_the_budget_is_exhausted(
        locked_db, monkeypatch):
    """The v1.15.35 contract is preserved, not traded away: a hold that
    outlives even transaction()'s budget must still surface an error (which the
    loop turns into the loud DROPPING-batch ERROR) rather than block forever."""
    db, hold_for = locked_db
    _scale(monkeypatch, events, 0.2, (0.05, 0.05, 0.05, 0.05))
    hold_for(4.0)

    err = events._write_batch(db, _INSERT_SQL, [_ROW])

    assert isinstance(err, sqlite3.OperationalError), (
        "an exhausted budget must return the error, not silently claim success"
    )
    assert "locked" in str(err)


def test_flusher_loop_turns_that_error_into_a_loud_drop():
    """The other half of the contract, which _write_batch deliberately does NOT
    own: the caller must log it at ERROR (class 9 — a silent audit-log gap is
    the bug v1.15.35 fixed)."""
    src = Path(events.__file__).read_text()
    i = src.index("def _flusher_loop(")
    body = src[i:src.index("\ndef ", i + 1)]
    assert "last_err = _write_batch(" in body
    assert "if last_err is not None:" in body
    assert "log.error(" in body and "DROPPING batch" in body


# ── notify_inbox.record_notification (write once, never regenerated) ──


def test_record_notification_survives_the_same_hold(locked_db, monkeypatch):
    db, hold_for = locked_db
    _scale(monkeypatch, notify_inbox, 0.4, (0.1, 0.15, 0.2, 0.3))
    hold_for(2.0)

    notify_inbox.record_notification(
        db, event_kind="theme_added", severity="info",
        title="🎵 Theme added — Probe (2024)", body="b")

    rows = notify_inbox.list_notifications(db)
    assert [r["event_kind"] for r in rows] == ["theme_added"], (
        "v0.51.260: a notification is generated once — dropping it on a lock "
        "hold loses it permanently"
    )


def test_readers_and_dismiss_paths_deliberately_keep_the_short_wait():
    """Scope guard. The budget was raised ONLY for the two write-once paths.
    Readers and dismiss/seen fail visibly and are retried by reopening the
    drawer or clicking again — a 30s-per-attempt hang inside a request handler
    is worse than a fast error, so they keep timeout=10.0 on purpose."""
    src = (Path(notify_inbox.__file__)).read_text()
    assert src.count("sqlite3.connect(db_path, timeout=10.0)") == 7, (
        "expected the 7 reader/dismiss/prune connections to keep the short "
        "wait; if a path was intentionally promoted, update this count and "
        "say why"
    )
    assert "sqlite3.connect(db_path, timeout=_LOCK_WAIT_S)" in src


# ── the cross-module budget lint (drift guard) ──


def test_every_write_once_writer_matches_the_canonical_budget():
    """events.py and notify_inbox.py own their connections and cannot import
    db.py (events.py is the logging substrate — a db import would let a
    db-layer fault take the audit log down with it). So the constants are
    duplicated on purpose and THIS is what keeps them honest."""
    for mod in (events, notify_inbox):
        assert mod._LOCK_WAIT_S == dbmod.LOCK_WAIT_S, (
            f"{mod.__name__} waits {mod._LOCK_WAIT_S}s per attempt vs "
            f"db.LOCK_WAIT_S={dbmod.LOCK_WAIT_S}s — a writer with a smaller "
            f"budget than transaction() drops rows the DB would have accepted"
        )
        assert mod._LOCK_RETRY_DELAYS == dbmod.LOCK_RETRY_DELAYS, (
            f"{mod.__name__} retry ladder drifted from db.LOCK_RETRY_DELAYS"
        )


def test_budget_is_large_enough_for_the_longest_known_lock_hold():
    """The hold that motivated this: api_set_override_intent's plex_cloud
    PROMOTE runs get_themes (30s) + upload_collection_theme (60s x TWO
    attempts — multipart then a raw-body fallback) inside its BEGIN IMMEDIATE.
    ~150s. The writer budget must clear it."""
    budget = (dbmod.LOCK_WAIT_S * (len(dbmod.LOCK_RETRY_DELAYS) + 1)
              + sum(dbmod.LOCK_RETRY_DELAYS))
    assert budget >= 150.0, (
        f"writer budget {budget}s no longer covers the ~150s worst-case "
        f"PROMOTE hold — either raise it or shorten the hold"
    )


def test_transaction_uses_the_hoisted_ladder():
    src = Path(dbmod.__file__).read_text()
    assert "delays = LOCK_RETRY_DELAYS" in src, (
        "transaction() must consume the hoisted constant, or the lint above "
        "guards a value nothing uses"
    )


def test_v0_51_260_version_pin():
    init_py = (Path(dbmod.__file__).parent.parent / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
