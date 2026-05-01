"""
Event log helper. Writes structured events to the `events` table so the
WebGUI can render them without re-parsing log files.

v1.11.40: log_event no longer writes synchronously on the caller's
thread. A single background flusher thread pulls from an in-memory
Queue and batches inserts every ~250ms. The API request hot path
just enqueues — no DB lock contention, no WAL waits, no 5s
busy_timeout dragging request latency during a sync.
"""
from __future__ import annotations

import json
import logging
import queue
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# Bounded queue — overflows drop the oldest event rather than blocking
# a caller. Sized for a worst-case sync that fires ~thousands of
# events; even at 5K/queue we'd flush within a few seconds.
_EVENT_QUEUE: queue.Queue[tuple] = queue.Queue(maxsize=10000)
_FLUSHER_STARTED = False
_FLUSHER_LOCK = threading.Lock()


def _ensure_flusher_running(db_path: Path) -> None:
    """Lazily start the background flusher thread on first log_event."""
    global _FLUSHER_STARTED
    if _FLUSHER_STARTED:
        return
    with _FLUSHER_LOCK:
        if _FLUSHER_STARTED:
            return
        t = threading.Thread(
            target=_flusher_loop, args=(db_path,),
            name="motif-event-flusher", daemon=True,
        )
        t.start()
        _FLUSHER_STARTED = True


def _flusher_loop(db_path: Path) -> None:
    """Drain _EVENT_QUEUE and bulk-insert every ~250ms.

    The flusher owns its own sqlite3 connection for the lifetime of
    the process. Batch inserts in one transaction so the write lock
    is held for one short window per batch instead of N short windows
    per individual event.
    """
    while True:
        first = _EVENT_QUEUE.get()
        batch: list[tuple] = [first]
        # Drain whatever's piled up while we were idle
        try:
            while len(batch) < 200:
                batch.append(_EVENT_QUEUE.get_nowait())
        except queue.Empty:
            pass
        try:
            with sqlite3.connect(db_path, timeout=10.0) as conn:
                conn.executemany(
                    """INSERT INTO events
                         (ts, level, component, media_type, tmdb_id,
                          message, detail)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    batch,
                )
        except sqlite3.Error as e:
            log.warning("Event flusher: batch insert failed (%d events): %s",
                        len(batch), e)
        # Tiny sleep so we batch any back-to-back caller arrivals.
        threading.Event().wait(0.25)


def log_event(
    db_path: Path,
    *,
    level: str,
    component: str,
    message: str,
    media_type: str | None = None,
    tmdb_id: int | None = None,
    detail: dict[str, Any] | str | None = None,
) -> None:
    """Enqueue an event for the background flusher. Never raises —
    event logging must not break callers and must not hold the
    caller's thread for DB I/O."""
    detail_str: str | None
    if isinstance(detail, dict):
        scrubbed = _scrub(detail)
        detail_str = json.dumps(scrubbed, ensure_ascii=False, default=str)
    elif isinstance(detail, str):
        detail_str = detail
    else:
        detail_str = None

    py_logger = logging.getLogger(component)
    py_logger.log(
        getattr(logging, level.upper(), logging.INFO),
        "%s%s",
        message,
        f" | {detail_str}" if detail_str else "",
    )

    _ensure_flusher_running(db_path)
    try:
        _EVENT_QUEUE.put_nowait((
            now_iso(), level.upper(), component,
            media_type, tmdb_id, message, detail_str,
        ))
    except queue.Full:
        # Best-effort: drop on overflow. The python logger above
        # already has the message; only the DB-side render misses it.
        log.debug("event queue full — dropping event: %s", message)


def _scrub(d: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in d.items():
        kl = str(k).lower()
        if any(s in kl for s in ("token", "secret", "password", "cookie", "auth")):
            out[k] = "***REDACTED***"
        elif isinstance(v, dict):
            out[k] = _scrub(v)
        else:
            out[k] = v
    return out
