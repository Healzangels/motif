"""v0.51.88 — CSS+login audit (login hardening): idle-session timeout.

Sessions previously died only at the 30-day ABSOLUTE TTL. This adds a shorter
ACTIVITY-based timeout: a session untouched for longer than
SESSION_IDLE_TIMEOUT_SECONDS is rejected, so a lingering/stolen cookie can't
stay live indefinitely on an idle account.

The careful part (v1.11.37): last_seen_at must NOT be written per request — that
UPDATE waited on the writer lock during long syncs and softlocked the UI. So the
refresh is COARSE (only when last_seen_at is already older than
SESSION_TOUCH_INTERVAL_SECONDS → ~99% of requests write nothing) AND fail-fast
best-effort (a short busy_timeout, lock swallowed).

Behavioral tests (real DB + real functions).
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path


def _auth_db(tmp_path: Path) -> Path:
    from app.core.auth import create_admin, init_auth_schema
    db = tmp_path / "auth.db"
    init_auth_schema(db)
    create_admin(db, username="admin", password="password123")
    return db


def _iso(dt: datetime) -> str:
    return dt.isoformat(timespec="seconds")


def _set_last_seen(db: Path, dt: datetime) -> None:
    # single session in these tests → update them all.
    with sqlite3.connect(db) as conn:
        conn.execute("UPDATE sessions SET last_seen_at = ?", (_iso(dt),))
        conn.commit()


def _get_last_seen(db: Path) -> str:
    with sqlite3.connect(db) as conn:
        return conn.execute("SELECT last_seen_at FROM sessions").fetchone()[0]


def test_idle_session_is_rejected(tmp_path):
    from app.core.auth import (
        SESSION_IDLE_TIMEOUT_SECONDS, create_session, lookup_session,
    )
    db = _auth_db(tmp_path)
    sid = create_session(db, username="admin", user_agent="x")
    assert lookup_session(db, sid) == "admin"
    # backdate last_seen past the idle window (expires_at is still 30d out).
    now = datetime.now(timezone.utc)
    _set_last_seen(db, now - timedelta(seconds=SESSION_IDLE_TIMEOUT_SECONDS + 3600))
    assert lookup_session(db, sid) is None, (
        "a session idle beyond the timeout must be rejected even within the "
        "absolute TTL")


def test_active_session_within_idle_window_survives(tmp_path):
    from app.core.auth import (
        SESSION_IDLE_TIMEOUT_SECONDS, create_session, lookup_session,
    )
    db = _auth_db(tmp_path)
    sid = create_session(db, username="admin", user_agent="x")
    now = datetime.now(timezone.utc)
    # inside the idle window (but older than the touch interval).
    _set_last_seen(db, now - timedelta(seconds=SESSION_IDLE_TIMEOUT_SECONDS - 3600))
    assert lookup_session(db, sid) == "admin"


def test_coarse_touch_refreshes_only_when_stale(tmp_path):
    from app.core.auth import (
        SESSION_TOUCH_INTERVAL_SECONDS, create_session, lookup_session,
    )
    db = _auth_db(tmp_path)
    sid = create_session(db, username="admin", user_agent="x")
    now = datetime.now(timezone.utc)

    # (a) FRESH (younger than the touch interval): lookup must NOT rewrite it.
    fresh = _iso(now - timedelta(seconds=60))
    _set_last_seen(db, now - timedelta(seconds=60))
    assert lookup_session(db, sid) == "admin"
    assert _get_last_seen(db) == fresh, (
        "a fresh session must not trigger a last_seen_at write (per-request "
        "writes are the v1.11.37 softlock)")

    # (b) STALE (older than the touch interval, still within idle): lookup
    #     refreshes last_seen_at forward.
    stale = _iso(now - timedelta(seconds=SESSION_TOUCH_INTERVAL_SECONDS + 120))
    _set_last_seen(db, now - timedelta(seconds=SESSION_TOUCH_INTERVAL_SECONDS + 120))
    assert lookup_session(db, sid) == "admin"
    assert _get_last_seen(db) > stale, (
        "a stale-but-valid session's last_seen_at must be coarsely refreshed")


def test_cleanup_reaps_idle_sessions(tmp_path):
    from app.core.auth import (
        SESSION_IDLE_TIMEOUT_SECONDS, cleanup_expired_sessions, create_session,
    )
    db = _auth_db(tmp_path)
    create_session(db, username="admin", user_agent="x")
    now = datetime.now(timezone.utc)
    _set_last_seen(db, now - timedelta(seconds=SESSION_IDLE_TIMEOUT_SECONDS + 3600))
    reaped = cleanup_expired_sessions(db)
    assert reaped >= 1
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0] == 0, (
            "cleanup must delete idle-timed-out sessions, not just absolute-"
            "expired ones")


def test_idle_timeout_is_shorter_than_absolute_ttl(tmp_path):
    # sanity: the activity timeout only adds value if it's below the hard cap.
    from app.core.auth import SESSION_IDLE_TIMEOUT_SECONDS, SESSION_TTL_SECONDS
    assert SESSION_IDLE_TIMEOUT_SECONDS < SESSION_TTL_SECONDS
