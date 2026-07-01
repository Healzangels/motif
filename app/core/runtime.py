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
    with `default` so the value is stable from this moment onward.

    v0.50.89 (audit MEDIUM): the seed is now a single `ON CONFLICT DO
    NOTHING` INSERT instead of a separate SELECT-then-INSERT — pre-fix, two
    concurrent callers racing on a not-yet-seeded key (a worker thread and
    an API request both checking is_dry_run(), say) could both see `row is
    None` and both attempt the bare INSERT; the loser hit an uncaught
    sqlite3.IntegrityError (the PK has no ON CONFLICT clause to absorb it),
    unlike set_runtime_bool which already upserts safely. The follow-up
    SELECT is authoritative regardless of which caller's default (if any)
    actually won the race."""
    with get_conn(db_path) as conn:
        row = conn.execute(
            "SELECT value FROM runtime_settings WHERE key = ?", (key,),
        ).fetchone()
        if row is not None:
            return row["value"].strip().lower() in ("1", "true", "yes", "on")
        conn.execute(
            """INSERT INTO runtime_settings (key, value, updated_at, updated_by)
               VALUES (?, ?, ?, 'system-default')
               ON CONFLICT(key) DO NOTHING""",
            (key, "true" if default else "false", _now()),
        )
        row = conn.execute(
            "SELECT value FROM runtime_settings WHERE key = ?", (key,),
        ).fetchone()
        return row["value"].strip().lower() in ("1", "true", "yes", "on")


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


def get_runtime_text(db_path: Path, key: str, default: str = "") -> str:
    """v1.13.48: read a free-text runtime setting (e.g. JSON blob).
    Returns the default WITHOUT seeding the row when missing — text
    payloads are typically large enough that we'd rather skip a write
    on every read until the user actually saves something.
    """
    with get_conn(db_path) as conn:
        row = conn.execute(
            "SELECT value FROM runtime_settings WHERE key = ?", (key,),
        ).fetchone()
    return row["value"] if row is not None else default


def set_runtime_text(db_path: Path, key: str, value: str, *,
                     updated_by: str) -> None:
    """v1.13.48: write a free-text runtime setting."""
    with get_conn(db_path) as conn:
        conn.execute(
            """INSERT INTO runtime_settings (key, value, updated_at, updated_by)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(key) DO UPDATE SET
                   value = excluded.value,
                   updated_at = excluded.updated_at,
                   updated_by = excluded.updated_by""",
            (key, value, _now(), updated_by),
        )


# v1.17.9: get_all_runtime deleted — zero callers (hygiene audit).
# If a settings dump is ever needed, the table is small enough to
# query directly at the callsite.


# Convenience accessor for the only runtime bool we currently care about
def is_dry_run(db_path: Path, *, default: bool) -> bool:
    return get_runtime_bool(db_path, "dry_run", default)


def set_dry_run(db_path: Path, value: bool, *, updated_by: str) -> None:
    set_runtime_bool(db_path, "dry_run", value, updated_by=updated_by)
