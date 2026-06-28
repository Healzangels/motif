"""Per-event dedupe state for notify.dispatch hook sites.

v1.17.4: phase 2 of the notifications system wired the remaining
7 event hooks. Several of those events (cookies_needed, disk_low,
release_available) can legitimately fire many times in quick
succession — every probe, every poll, every release-check tick —
so without dedupe the operator's notification channel would spam.

This module owns the small per-event state needed to gate
dispatches without spamming. Two primitives:

  * `should_fire(db, kind, rate_limit_seconds=...)` — true unless
    the last fire was within N seconds. Use for events that can
    re-fire on every tick (cookies_needed every probe, disk_low
    every job dispatch). Pair with `record_fire(db, kind)` after
    the dispatch.

  * `should_fire(db, kind, edge_value=...)` — true unless the
    edge_value equals the last-recorded value. Use for events
    with a meaningful sentinel (disk_low edge-transition `ok` ↔
    `low`; release_available dedupe by tag_name so each new
    version pings exactly once even across restarts).

State persists in `runtime_settings` keyed by
`notify_dedupe.<event_kind>` — same key/value table other motif
runtime state already lives in. No schema migration needed.

Both helpers are fail-open: if the read/write fails, they default
to "fire" rather than "skip" — losing a dedupe round is better
than swallowing a notification that the operator wanted to see.
"""
from __future__ import annotations

import json
import logging
import time
from pathlib import Path

from .db import get_conn, transaction
from .events import now_iso


log = logging.getLogger(__name__)


def _key(event_kind: str) -> str:
    return f"notify_dedupe.{event_kind}"


def _read_state(db_path: Path, event_kind: str) -> dict:
    """Read the dedupe state dict. Empty dict on missing row or
    parse failure (fail-open semantics)."""
    try:
        with get_conn(db_path) as conn:
            row = conn.execute(
                "SELECT value FROM runtime_settings WHERE key = ?",
                (_key(event_kind),),
            ).fetchone()
    except Exception as e:
        log.debug("notify_dedupe._read_state failed for %s: %s",
                  event_kind, e)
        return {}
    if row is None:
        return {}
    try:
        return json.loads(row["value"]) or {}
    except (json.JSONDecodeError, TypeError) as e:
        log.debug("notify_dedupe._read_state parse failed for %s: %s",
                  event_kind, e)
        return {}


def should_fire(
    db_path: Path,
    event_kind: str,
    *,
    rate_limit_seconds: int | None = None,
    edge_value: str | None = None,
) -> bool:
    """Return True if this dispatch should fire (not deduped).

    `rate_limit_seconds`: skip the fire if the last recorded fire
    was less than N seconds ago. Use for events that can legitimately
    re-fire on every tick.

    `edge_value`: skip the fire if `edge_value == last_value`. Use
    for events with a meaningful state sentinel — re-firing only
    happens when the sentinel changes (edge transition).

    Both can be combined: an event fires when it's outside the
    rate-limit AND the edge value changed (e.g. disk_low could be
    "edge to low" + "rate-limited to 1/24h").

    Fail-open: a state-read error returns True (the event fires)
    rather than False (the event silently drops). Losing a dedupe
    round is better than swallowing a notification."""
    state = _read_state(db_path, event_kind)
    if rate_limit_seconds is not None:
        last_at = state.get("last_at")
        if isinstance(last_at, (int, float)):
            age = time.time() - last_at
            if age < rate_limit_seconds:
                return False
    if edge_value is not None:
        last_value = state.get("last_value")
        if last_value == edge_value:
            return False
    return True


def record_fire(
    db_path: Path,
    event_kind: str,
    *,
    value: str | None = None,
) -> None:
    """Stamp this event's last_at (+ optional last_value) so
    subsequent should_fire calls can dedupe against it.

    Should be called AFTER the dispatch — that way a dispatch
    that itself raised won't leave a stale dedupe block in
    place. Fail-open: a write failure logs at debug and proceeds
    (the next tick will retry the dispatch + stamp)."""
    state = {"last_at": time.time()}
    if value is not None:
        state["last_value"] = value
    try:
        with get_conn(db_path) as conn, transaction(conn):
            conn.execute(
                "INSERT OR REPLACE INTO runtime_settings "
                "(key, value, updated_at, updated_by) "
                "VALUES (?, ?, ?, ?)",
                (_key(event_kind), json.dumps(state),
                 now_iso(), "notify_dedupe"),
            )
    except Exception as e:
        log.debug("notify_dedupe.record_fire failed for %s: %s",
                  event_kind, e)


def clear(db_path: Path, event_kind: str) -> None:
    """Drop the dedupe state for an event. Useful for tests + for
    "reset notification state" admin actions if we ever surface
    one. Fail-open."""
    try:
        with get_conn(db_path) as conn, transaction(conn):
            conn.execute(
                "DELETE FROM runtime_settings WHERE key = ?",
                (_key(event_kind),),
            )
    except Exception as e:
        log.debug("notify_dedupe.clear failed for %s: %s",
                  event_kind, e)
