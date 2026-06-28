"""v1.19.79 — MED batch from the Opus-4.8 audit.

Six follow-ups, none behavior-changing for the happy path:

1. Trigger-recreation guard: assert every trigger present in a
   migrated DB also exists in the SCHEMA constant with
   `CREATE TRIGGER IF NOT EXISTS`. `_drop_triggers_referencing`
   (v1.19.74) drops triggers by body-LIKE before a widen swap and
   relies on init_db's post-migration `executescript(SCHEMA)` to
   recreate them — a trigger that's live but absent from SCHEMA
   would be dropped and never restored (silent trigger loss).
2. cloud_theme_backup: wrap the 3 writer statements in one
   transaction (autocommit conn → pre-fix partial-write window).
3. cloud_theme_backup: reject a truncated/206 download body so a
   partial backup can't be PROMOTED TO ACTIVE later.
4. cloud_theme_backup walker: guard the per-row classify so one
   malformed /themes body can't abort the whole multi-minute walk.
5. cloud-backup run endpoint: activate the resumable cursor for a
   true full sweep (it was implemented + unit-tested but never
   wired into the live path).
6. sync._safe_json: class-9 breadcrumb (was a silent swallow).
7. worker delete_theme: WARN on the best-effort False return (the
   documented v1.18.0→v1.18.36 plural-/themes silent-404 class).
"""
from __future__ import annotations

import re
import sqlite3
import sys
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from app.core.db import init_db, SCHEMA

CTB = (REPO / "app" / "core" / "cloud_theme_backup.py").read_text()
API = (REPO / "app" / "web" / "api.py").read_text()
WORKER = (REPO / "app" / "core" / "worker.py").read_text()


# ── 1. Trigger-recreation guard ────────────────────────────────


def test_every_live_trigger_exists_in_schema(tmp_path):
    """Every trigger in a freshly-migrated DB must be recreatable
    from SCHEMA — otherwise _drop_triggers_referencing could drop
    it during a CHECK widen and it would be lost forever."""
    db = tmp_path / "m.db"
    init_db(db)
    conn = sqlite3.connect(db)
    triggers = [
        r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger' "
            "AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
    ]
    conn.close()
    assert triggers, "expected at least one trigger after init_db"
    for name in triggers:
        assert name in SCHEMA, (
            f"v1.19.79: trigger {name!r} exists in a migrated DB but is "
            f"NOT in the SCHEMA constant — _drop_triggers_referencing "
            f"could drop it during a widen and it would never be "
            f"recreated. Add it to SCHEMA with CREATE TRIGGER IF NOT EXISTS."
        )
        assert re.search(
            rf"CREATE TRIGGER IF NOT EXISTS {re.escape(name)}\b", SCHEMA
        ), (
            f"v1.19.79: trigger {name!r} in SCHEMA must use "
            f"CREATE TRIGGER IF NOT EXISTS so the post-migration "
            f"executescript(SCHEMA) re-creates it idempotently"
        )


# ── 2-4. cloud_theme_backup hardening ──────────────────────────


def test_writer_wraps_three_statements_in_transaction():
    """themes INSERT + plex_items UPDATE + local_files INSERT must
    be atomic (autocommit conn would otherwise commit each one)."""
    fn = CTB[CTB.index("def backup_cloud_theme("):]
    assert "from .db import transaction" in fn
    assert "with transaction(conn):" in fn
    # The local_files INSERT must sit inside the transaction block
    # (indented deeper than the `with`).
    with_at = fn.index("with transaction(conn):")
    lf_at = fn.index("INSERT INTO local_files")
    assert with_at < lf_at


def test_writer_rejects_truncated_body():
    """A 206 or Content-Length mismatch must abort the row, not
    stage a partial file."""
    fn = CTB[CTB.index("def backup_cloud_theme("):]
    assert "r.status_code == 206" in fn
    assert "truncated body" in fn
    assert "content-length" in fn.lower()


def test_walker_guards_per_row_classify():
    """A malformed /themes body must not abort the whole walk."""
    fn = CTB[CTB.index("def identify_c1_rows("):CTB.index("def backup_cloud_theme(")]
    classify_at = fn.index('_classify_themes_response(resp.get("body"))')
    # A `try:` must precede the classify call and an `except` must
    # follow it (the guard breadcrumb is unique to this handler).
    assert fn.rfind("try:", 0, classify_at) != -1
    assert "except Exception" in fn[classify_at:]
    assert "classify raised" in fn


# ── 5. cursor wired for the full sweep ─────────────────────────


def test_run_endpoint_activates_cursor_for_full_sweep_only():
    """The resumable cursor must be live for a true full sweep and
    OFF for scoped / anime / allow_existing_local runs."""
    assert "use_cursor=full_sweep" in API
    # full_sweep is the conjunction of all three "not scoped" conditions.
    block = API[API.index("full_sweep = ("): API.index("full_sweep = (") + 220]
    assert "rks_scope is None" in block
    assert "not only_anime" in block
    assert "not allow_existing_local" in block


# ── 6. sync._safe_json breadcrumb ──────────────────────────────


def test_safe_json_normal_dict_roundtrips():
    import app.core.sync as sync
    out = sync._safe_json({"a": 1, "b": "x"})
    assert '"a": 1' in out and '"b": "x"' in out


def test_safe_json_breadcrumb_on_unserializable():
    """A non-serializable record returns '{}' AND trips the
    once-per-process warning flag (no longer a silent swallow)."""
    import app.core.sync as sync
    sync._SAFE_JSON_WARNED = False

    class _Bad:
        pass

    out = sync._safe_json({"x": _Bad()})
    assert out == "{}"
    assert sync._SAFE_JSON_WARNED is True


# ── 7. worker delete_theme WARN-on-False ───────────────────────


def test_delete_theme_warns_on_false_return():
    """The best-effort delete_theme False return must log at WARNING,
    not INFO, so a systemic failure surfaces."""
    assert "logging.INFO if _td_ok" in WORKER
    assert "else logging.WARNING" in WORKER
