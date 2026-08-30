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
# v0.51.256: `plex_item_arrived_themed` DOES fire — the note claiming it was
# "forward-readiness, doesn't fire until a later tag" outlived the tag that wired it
# (3 rows on the operator's install). Same misleading-comment class as the `rk=` log
# label: a stale claim costs a future reader a wrong assumption, not just tidiness.
# v0.51.259: theme_pushed + theme_backed_up join the set. They were the only
# theme-lifecycle kinds with no in-app trace at all, and .254/.255 sharpened the
# gap: Discord now COALESCES a bulk burst into one summary, so the operator's
# 72-item PUSH left exactly one Discord line and zero rows anywhere in motif.
# theme_added's siblings should be findable the same way it is — and because the
# inbox records before the Apprise gate, theme_pushed (Apprise-default-off) now
# leaves a trace even for operators who keep it muted on Discord.
INBOX_EVENT_KINDS: frozenset[str] = frozenset({
    "theme_added",
    "theme_pushed",
    "theme_backed_up",
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

# v0.51.260: record_notification's lock-wait budget, aligned with
# db.LOCK_WAIT_S / db.LOCK_RETRY_DELAYS (lint: test_v0_51_260). It was 3 x 10s
# + 3s ~= 31.5s against transaction()'s ~157.5s, so a long hold DROPPED the
# inbox row — and a notification is generated once and never regenerated.
# Deliberately NOT applied to the readers or to dismiss/seen below: those fail
# visibly and are retried by reopening the drawer or clicking again, and a
# 30s-per-attempt hang inside a request handler is worse than a fast error.
_LOCK_WAIT_S = 30.0
_LOCK_RETRY_DELAYS = (0.5, 1.0, 2.0, 4.0)


def record_notification(
    db_path: Path,
    *,
    event_kind: str,
    severity: str,
    title: str,
    body: str | None = None,
    media_type: str | None = None,
    tmdb_id: int | None = None,
    section_id: str | None = None,
    edition_key: str | None = None,
) -> None:
    """Insert one inbox row. Best-effort — never raises (a failed inbox write
    must not break notification dispatch). Scrubs title/body through the events
    credential scrubber (the same last-line-of-defense used for the events log).
    Mirrors the events flusher's lock-retry discipline (CLAUDE.md class 8: retry
    only the 'database is locked' string)."""
    try:
        safe_title = _scrub_text(title or "")
        safe_body = _scrub_text(body) if body else None
        # v0.51.151: media_type/tmdb_id/section_id enable the drawer's click-
        # through to the row's INFO card (via the info_open deep-link). Only
        # per-item dispatches carry them; batch digests store NULL (non-clickable,
        # since a batch has no single item to open). These are identity ids, not
        # secrets, so they skip the text scrubber.
        # v0.51.220: edition_key names the exact cut so the click-through opens THAT
        # edition's card, not the v0.51.218 picker. NULL = no single edition (batch
        # digest) → picker fallback, correct. An identity key, not a secret.
        sql = (
            "INSERT INTO notifications (ts, event_kind, severity, title, body,"
            " media_type, tmdb_id, section_id, edition_key) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        params = (now_iso(), event_kind, severity or "info", safe_title, safe_body,
                  media_type, tmdb_id, section_id, edition_key)
        last_err: Exception | None = None
        _last_attempt = len(_LOCK_RETRY_DELAYS)
        for attempt in range(_last_attempt + 1):
            try:
                conn = sqlite3.connect(db_path, timeout=_LOCK_WAIT_S)
                try:
                    with conn:
                        conn.execute(sql, params)
                finally:
                    conn.close()
                return
            except sqlite3.OperationalError as e:
                last_err = e
                if ("database is locked" in str(e)
                        and attempt < _last_attempt):
                    threading.Event().wait(_LOCK_RETRY_DELAYS[attempt])
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
        # v0.51.308: LEFT JOIN the section so the drawer can route anime
        # click-throughs to /anime — the v0.51.209 tab map defaulted every
        # show to /tv, landing anime cards on the wrong page (no poster, no
        # row behind the card). Joined at LIST time so it tracks the
        # section's CURRENT flag, not a stamp frozen at notify time.
        rows = conn.execute(
            "SELECT n.id, n.ts, n.event_kind, n.severity, n.title, n.body, "
            "       n.seen_at, n.media_type, n.tmdb_id, n.section_id, "
            "       n.edition_key, COALESCE(s.is_anime, 0) AS is_anime "
            "FROM notifications n "
            "LEFT JOIN plex_sections s ON s.section_id = n.section_id "
            "WHERE n.dismissed_at IS NULL "
            "ORDER BY n.ts DESC, n.id DESC LIMIT ?",
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
            # v0.51.151: identity for the drawer's click-through (NULL on batch
            # digests → the row renders non-clickable).
            "media_type": r["media_type"],
            "tmdb_id": r["tmdb_id"],
            "section_id": r["section_id"],
            # v0.51.220: the exact cut, so the click-through skips the picker. NULL on
            # digests → the drawer omits info_edition and the card falls back to it.
            "edition_key": r["edition_key"],
            # v0.51.308: anime rows deep-link to /anime, not /tv.
            "is_anime": bool(r["is_anime"]),
        }
        for r in rows
    ]


def count_undismissed(db_path: Path) -> int:
    """Every undismissed row — seen or not. This is the DENOMINATOR the drawer
    needs: list_notifications caps its result, and the drawer rendered that
    capped count as if it were the total. After a 77-item restore burst it read
    "50 themes restored" with 27 rows invisible and nothing saying so."""
    conn = sqlite3.connect(db_path, timeout=10.0)
    try:
        row = conn.execute(
            "SELECT COUNT(*) FROM notifications WHERE dismissed_at IS NULL"
        ).fetchone()
    finally:
        conn.close()
    return int(row[0]) if row else 0


def count_unread(db_path: Path) -> int:
    """Undismissed AND unseen — the topbar INBOX badge count. Counts individual rows:
    the v0.51.154 drawer grouping is client-side + display-only, so a bulk burst shows
    its true unread COUNT on the badge while collapsing to one expandable group in the
    drawer (intentional — the badge is 'how many new', the drawer tidies the view)."""
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


def mark_seen_one(db_path: Path, notification_id: int) -> int:
    """v0.51.266: mark ONE row seen (idempotent). Pre-tag the only seen-writer was
    the unconditional mark_seen() fired on drawer OPEN, so reading one notification
    cleared unread on every one — the operator's report. Same short wait as the
    dismiss path (see the .260 scope guard): a seen write fails visibly and the
    next click retries it."""
    conn = sqlite3.connect(db_path, timeout=10.0)
    try:
        with conn:
            cur = conn.execute(
                "UPDATE notifications SET seen_at = ? "
                "WHERE id = ? AND seen_at IS NULL",
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


# v0.51.152: retention prune for the in-app inbox. The drawer only ever shows
# UNDISMISSED rows (list_notifications filters dismissed_at IS NULL), so a
# dismissed row is pure DB weight — prune it after a short grace. Any row (even
# undismissed) older than the hard cap rotates out too, so an operator who never
# opens the inbox can't grow the table unbounded (mirrors the events 30-day cap).
_PRUNE_DISMISSED_DAYS = 7
_PRUNE_MAX_AGE_DAYS = 30


def prune_notifications(db_path: Path) -> int:
    """Delete dismissed rows older than 7 days + any row older than 30 days.
    Returns rows deleted. Called by the scheduler's daily maintenance sweep."""
    conn = sqlite3.connect(db_path, timeout=10.0)
    try:
        with conn:
            cur = conn.execute(
                "DELETE FROM notifications "
                "WHERE (dismissed_at IS NOT NULL "
                f"       AND dismissed_at < datetime('now', '-{_PRUNE_DISMISSED_DAYS} days')) "
                f"   OR ts < datetime('now', '-{_PRUNE_MAX_AGE_DAYS} days')"
            )
        return cur.rowcount or 0
    finally:
        conn.close()
