"""v0.51.152 — notifications inbox retention prune.

Phase 4, part 2. The `notifications` table (in-app inbox) previously grew forever —
record-only, no rotation. This adds a daily scheduler sweep, mirroring _prune_events:
`notify_inbox.prune_notifications` deletes dismissed rows older than 7 days (the
drawer only shows UNDISMISSED rows, so dismissed ones are pure DB weight) plus any
row older than 30 days (so an operator who never opens the inbox can't grow it
unbounded). Scheduled at 03:12 UTC, between events_prune (03:10) and prune_history
(03:15).

Tests: behavioral against a seeded DB (the four age/dismissed combinations) + source
pins for the scheduler wiring.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SCHEDULER_PY = (REPO / "app" / "core" / "scheduler.py").read_text()


def _iso(days_ago: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days_ago)).isoformat(
        timespec="seconds")


def _seed(conn, *, ts_days, dismissed_days=None):
    conn.execute(
        "INSERT INTO notifications (ts, event_kind, severity, title, dismissed_at) "
        "VALUES (?, 'theme_added', 'info', 'x', ?)",
        (_iso(ts_days), _iso(dismissed_days) if dismissed_days is not None else None),
    )


def _db(tmp_path):
    from app.core.db import init_db
    db = tmp_path / "motif.db"
    init_db(db)
    return db


def test_prune_deletes_old_dismissed_and_old_rows_keeps_the_rest(tmp_path):
    from app.core import notify_inbox
    db = _db(tmp_path)
    with sqlite3.connect(db) as conn:
        _seed(conn, ts_days=40, dismissed_days=30)   # A: dismissed >7d ago → delete
        _seed(conn, ts_days=5, dismissed_days=2)     # B: dismissed <7d ago → keep
        _seed(conn, ts_days=60)                       # C: undismissed >30d → delete
        _seed(conn, ts_days=1)                        # D: undismissed, recent → keep
        conn.commit()
    n = notify_inbox.prune_notifications(db)
    assert n == 2
    with sqlite3.connect(db) as conn:
        remaining = conn.execute("SELECT COUNT(*) FROM notifications").fetchone()[0]
    assert remaining == 2


def test_prune_keeps_recent_dismissed(tmp_path):
    """A row dismissed within the grace window stays (so a just-dismissed row
    isn't yanked out from under an undo/audit)."""
    from app.core import notify_inbox
    db = _db(tmp_path)
    with sqlite3.connect(db) as conn:
        _seed(conn, ts_days=3, dismissed_days=0)  # dismissed today, recent
        conn.commit()
    assert notify_inbox.prune_notifications(db) == 0


def test_prune_empty_is_zero(tmp_path):
    from app.core import notify_inbox
    db = _db(tmp_path)
    assert notify_inbox.prune_notifications(db) == 0


def test_scheduler_registers_notifications_prune():
    assert "def _prune_notifications(db_path: Path) -> None:" in SCHEDULER_PY
    assert "from .notify_inbox import prune_notifications" in SCHEDULER_PY
    # scheduled as its own cron job at 03:12.
    idx = SCHEDULER_PY.index('id="notifications_prune"')
    window = SCHEDULER_PY[idx - 300:idx]
    assert "_prune_notifications" in window
    assert 'minute="12", hour="3"' in window
