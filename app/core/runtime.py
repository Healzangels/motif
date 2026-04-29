"""
Runtime settings — UI-toggleable options stored in the DB.

The pattern: env vars set the *initial* value when the DB row doesn't exist.
After that, the DB wins. This lets users change a setting in the UI and have
it persist across restarts, while still allowing initial bootstrapping via
env (which is the only way to get a value in before the UI exists).

Currently the only runtime setting is `dry_run`, but new ones can be added
trivially.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from .db import get_conn


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def get_runtime_bool(db_path: Path, key: str, default: bool) -> bool:
    """Read a boolean runtime setting. If the key doesn't exist yet, seed it
    with `default` so the value is stable from this moment onward."""
    with get_conn(db_path) as conn:
        row = conn.execute(
            "SELECT value FROM runtime_settings WHERE key = ?", (key,),
        ).fetchone()
        if row is not None:
            return row["value"].strip().lower() in ("1", "true", "yes", "on")
        # Seed with default
        conn.execute(
            """INSERT INTO runtime_settings (key, value, updated_at, updated_by)
               VALUES (?, ?, ?, 'system-default')""",
            (key, "true" if default else "false", _now()),
        )
        return default


def set_runtime_bool(db_path: Path, key: str, value: bool, *, updated_by: str) -> None:
    with get_conn(db_path) as conn:
        conn.execute(
            """INSERT INTO runtime_settings (key, value, updated_at, updated_by)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(key) DO UPDATE SET
                   value = excluded.value,
                   updated_at = excluded.updated_at,
                   updated_by = excluded.updated_by""",
            (key, "true" if value else "false", _now(), updated_by),
        )


def get_all_runtime(db_path: Path) -> dict[str, str]:
    with get_conn(db_path) as conn:
        rows = conn.execute(
            "SELECT key, value, updated_at, updated_by FROM runtime_settings"
        ).fetchall()
    return {r["key"]: dict(r) for r in rows}


# Convenience accessor for the only runtime bool we currently care about
def is_dry_run(db_path: Path, *, default: bool) -> bool:
    return get_runtime_bool(db_path, "dry_run", default)


def set_dry_run(db_path: Path, value: bool, *, updated_by: str) -> None:
    set_runtime_bool(db_path, "dry_run", value, updated_by=updated_by)
