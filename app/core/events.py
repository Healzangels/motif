"""
Event log helper. Writes structured events to the `events` table so the
WebGUI can render them without re-parsing log files. Falls back gracefully
if the DB is locked (the in-process logger still gets the message).
"""
from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


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
    """Insert an event row. Never raises — event logging must not break callers."""
    detail_str: str | None
    if isinstance(detail, dict):
        # Best-effort scrub of secrets. Anything matching "token" or "key" gets redacted.
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

    try:
        with sqlite3.connect(db_path, timeout=5.0) as conn:
            conn.execute(
                """
                INSERT INTO events (ts, level, component, media_type, tmdb_id, message, detail)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (now_iso(), level.upper(), component, media_type, tmdb_id, message, detail_str),
            )
    except sqlite3.Error as e:
        log.warning("Failed to write event to DB: %s", e)


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
