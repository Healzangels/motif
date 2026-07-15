"""In-app notification inbox — the store behind the topbar INBOX drawer.

A curated subset of notification-worthy events (INBOX_EVENT_KINDS) is recorded to
the `notifications` table at the notify dispatch chokepoint (notify.py), written
UNCONDITIONALLY of the per-event Apprise send-toggle — so the inbox surfaces
auto-added themes even when that kind's Discord/Apprise toggle is off. This module
owns every read/write of that table; the API layer calls the query helpers, notify
calls record_notification. Named notify_INBOX to stay clearly distinct from
notify.py (the Apprise dispatcher).

v0.51.147: first backend tag — store + writer + read/dismiss/seen helpers. dispatch()
carries only title/body, so media_type/tmdb_id/batch_id stay NULL for now; a later
tag threads item + batch identity for precise click-through and smart grouping.
"""
from __future__ import annotations

import logging
import sqlite3
import threading
from pathlib import Path

from .events import now_iso, _scrub_text

log = logging.getLogger("motif.notify_inbox")

# The event kinds surfaced in the in-app inbox — the "Additions + FYI" scope the
# user chose. Excludes operational kinds (sync_completed/failed, cookies_needed,
# disk_low, worker_restarted) which live on the topbar pills / LOGS, not here.
# `plex_item_arrived_themed` is listed for forward-readiness; it doesn't fire until
# a later tag adds that event at the plex_enum insert site.
INBOX_EVENT_KINDS: frozenset[str] = frozenset({
    "theme_added",
    "plex_item_arrived_themed",
    "theme_auto_restored",
    "new_tdb_theme_available",
    "backup_ready_to_deploy",
    "theme_lost_backup_ready",
    "theme_lost_sidecar_available",
    "plex_theme_lost",
})

_LIST_LIMIT_DEFAULT = 50
_LIST_LIMIT_MAX = 200


def record_notification(
    db_path: Path,
    *,
    event_kind: str,
    severity: str,
    title: str,
    body: str | None = None,
) -> None:
    """Insert one inbox row. Best-effort — never raises (a failed inbox write
    must not break notification dispatch). Scrubs title/body through the events
    credential scrubber (the same last-line-of-defense used for the events log).
    Mirrors the events flusher's lock-retry discipline (CLAUDE.md class 8: retry
    only the 'database is locked' string)."""
    try:
        safe_title = _scrub_text(title or "")
        safe_body = _scrub_text(body) if body else None
        sql = (
            "INSERT INTO notifications (ts, event_kind, severity, title, body) "
            "VALUES (?, ?, ?, ?, ?)"
        )
        params = (now_iso(), event_kind, severity or "info", safe_title, safe_body)
        last_err: Exception | None = None
        for attempt in range(3):
            try:
                conn = sqlite3.connect(db_path, timeout=10.0)
                try:
                    with conn:
                        conn.execute(sql, params)
                finally:
                    conn.close()
                return
            except sqlite3.OperationalError as e:
                last_err = e
                if "database is locked" in str(e) and attempt < 2:
                    threading.Event().wait(0.5 * (attempt + 1))
                    continue
                break
            except sqlite3.Error as e:
                last_err = e
                break
        # v0.51.147 (class 9): a dropped inbox write is a real gap — log loud, not silent.
        log.warning("record_notification: dropped %s inbox row: %s", event_kind, last_err)
    except Exception:
        log.exception("record_notification: unexpected error — continuing")


def list_notifications(db_path: Path, limit: int = _LIST_LIMIT_DEFAULT) -> list[dict]:
    """Undismissed notifications, newest first, capped. Each row carries `seen`
    (bool) so the UI can style already-seen rows."""
    limit = max(1, min(int(limit or _LIST_LIMIT_DEFAULT), _LIST_LIMIT_MAX))
    conn = sqlite3.connect(db_path, timeout=10.0)
    try:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT id, ts, event_kind, severity, title, body, seen_at "
            "FROM notifications WHERE dismissed_at IS NULL "
            "ORDER BY ts DESC, id DESC LIMIT ?",
            (limit,),
        ).fetchall()
    finally:
        conn.close()
    return [
        {
            "id": r["id"],
            "ts": r["ts"],
            "event_kind": r["event_kind"],
            "severity": r["severity"],
            "title": r["title"],
            "body": r["body"],
            "seen": r["seen_at"] is not None,
        }
        for r in rows
    ]


def count_unread(db_path: Path) -> int:
    """Undismissed AND unseen — the topbar INBOX badge count (a batch will count
    as 1 unit once grouping lands; today each row is 1)."""
    conn = sqlite3.connect(db_path, timeout=10.0)
    try:
        row = conn.execute(
            "SELECT COUNT(*) FROM notifications "
            "WHERE dismissed_at IS NULL AND seen_at IS NULL"
        ).fetchone()
    finally:
        conn.close()
    return int(row[0]) if row else 0


def dismiss_notification(db_path: Path, notification_id: int) -> int:
    """Dismiss one by id (idempotent — a re-dismiss touches nothing). Returns
    rows affected."""
    conn = sqlite3.connect(db_path, timeout=10.0)
    try:
        with conn:
            cur = conn.execute(
                "UPDATE notifications SET dismissed_at = ? "
                "WHERE id = ? AND dismissed_at IS NULL",
                (now_iso(), int(notification_id)),
            )
        return cur.rowcount
    finally:
        conn.close()


def dismiss_all(db_path: Path) -> int:
    """Clear-all: dismiss every currently-undismissed row. Returns rows affected."""
    conn = sqlite3.connect(db_path, timeout=10.0)
    try:
        with conn:
            cur = conn.execute(
                "UPDATE notifications SET dismissed_at = ? WHERE dismissed_at IS NULL",
                (now_iso(),),
            )
        return cur.rowcount
    finally:
        conn.close()


def mark_seen(db_path: Path) -> int:
    """Mark every undismissed, unseen row as seen (the drawer-open snapshot → the
    badge clears while rows stay until dismissed). Returns rows affected."""
    conn = sqlite3.connect(db_path, timeout=10.0)
    try:
        with conn:
            cur = conn.execute(
                "UPDATE notifications SET seen_at = ? "
                "WHERE dismissed_at IS NULL AND seen_at IS NULL",
                (now_iso(),),
            )
        return cur.rowcount
    finally:
        conn.close()
