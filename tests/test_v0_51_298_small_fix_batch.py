"""v0.51.298 — holistic review wave 7: six small confirmed fixes.

  1. db: every rebuild migration's `finally: PRAGMA foreign_keys = ON` was
     a silent NO-OP — the pragma does nothing inside a transaction, and the
     rebuild's implicit txn was still open, so FK enforcement stayed OFF
     for the rest of init_db. All five sites now commit/rollback first.
  2. db: `synchronous` is per-connection; init_db's NORMAL applied only to
     the throwaway boot connection — every runtime get_conn ran at FULL.
  3. notify: a coalescer timer that expired while dispatch_coalesced held
     the lock flushed the just-appended item early and orphaned the
     re-armed timer. The flush now verifies its own identity.
  4. api: the interactive-login username reached logs unsanitized (the
     forward-auth path was hardened at v1.17.23; this one wasn't).
  5. notify: the attachment fetch declined silently (class 9) — now a
     warning with url/status/ctype.
  6. app.js: the edit-audio PREVIEW handler had no in-flight guard — two
     concurrent renders raced, last-response-wins orphaned a candidate.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DB_PY = (REPO / "app" / "core" / "db.py").read_text()
API = (REPO / "app" / "web" / "api.py").read_text()
NOTIFY = (REPO / "app" / "core" / "notify.py").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


# ── 1. FK restore actually applies ───────────────────────────


def test_pragma_foreign_keys_is_a_noop_inside_a_txn(tmp_path):
    # the mechanism demo: WHY the old finally was a silent no-op.
    conn = sqlite3.connect(tmp_path / "t.db")
    conn.execute("PRAGMA foreign_keys = OFF")
    conn.execute("CREATE TABLE t (x)")
    conn.execute("INSERT INTO t VALUES (1)")     # opens the implicit txn
    conn.execute("PRAGMA foreign_keys = ON")     # ignored mid-txn
    assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 0
    conn.commit()
    conn.execute("PRAGMA foreign_keys = ON")
    assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1
    conn.close()


def test_all_fk_restores_close_the_txn_first():
    # every finally-restore is guarded; the two remaining bare enables are
    # the boot pragma (init_db, pre-txn) and get_conn's fresh-connection
    # enable — neither can be mid-txn.
    finallys = DB_PY.count(
        'finally:\n        # v0.51.298')
    n_guarded = DB_PY.count("if conn.in_transaction:")
    assert finallys == n_guarded == 5, (
        f"every rebuild finally ({finallys}) must commit/rollback first "
        f"({n_guarded} guarded) — the pragma is a no-op mid-txn")
    assert "conn.rollback()" in DB_PY.split("if conn.in_transaction:")[1], (
        "the error path must roll back — a bare commit would persist a "
        "partial rebuild")


# ── 2. runtime connections pair WAL with NORMAL ──────────────


def test_get_conn_sets_synchronous_normal(tmp_path):
    from app.core.db import get_conn, init_db
    db = tmp_path / "t.db"
    init_db(db)
    with get_conn(db) as conn:
        assert conn.execute("PRAGMA synchronous").fetchone()[0] == 1, (
            "synchronous is per-connection — without the get_conn pragma "
            "every runtime connection ran at FULL (2)")


# ── 3. a stale coalescer timer cannot steal the window ───────


def test_stale_timer_flush_is_a_noop(tmp_path, monkeypatch):
    import threading
    from app.core import notify
    kind = "theme_added"
    sent: list = []
    monkeypatch.setattr(notify, "_dispatch_batch",
                        lambda *a, **k: sent.append(a[3]))
    monkeypatch.setitem(notify._COALESCE_BUF, kind, [{"label": "x"}])
    live = threading.Timer(9999, lambda: None)
    monkeypatch.setitem(notify._COALESCE_TIMERS, kind, live)
    stale = threading.Timer(9999, lambda: None)
    # the stale (superseded) timer fires: must NOT drain the buffer.
    notify._flush_coalesced(tmp_path / "db", None, kind, stale)
    assert notify._COALESCE_BUF.get(kind) == [{"label": "x"}]
    assert sent == []
    # the REGISTERED timer fires: drains.
    notify._flush_coalesced(tmp_path / "db", None, kind, live)
    assert kind not in notify._COALESCE_BUF
    assert sent and sent[0] == [{"label": "x"}]
    live.cancel(); stale.cancel()


def test_arm_hands_the_timer_its_own_identity():
    i = NOTIFY.index("def _arm_coalesce_timer")
    blk = NOTIFY[i:NOTIFY.index("\ndef ", i + 10)]
    assert "timer.args = (db_path, notifications, event_kind, timer)" in blk


# ── 4-6. wiring pins ─────────────────────────────────────────


def test_login_log_sites_sanitize_the_username():
    i = API.index("Failed login attempt for ")
    assert "_sanitize_forward_auth_username(username)" in API[i - 400:i + 200]
    j = API.index('" logged in"))')
    assert "_sanitize_forward_auth_username(username)" in API[j - 300:j]
    k = API.index('" created and logged in"))')
    assert "_sanitize_forward_auth_username(username)" in API[k - 300:k]
    assert "f\"Failed login attempt for '{username}'\"" not in API
    assert "f\"User '{username}' logged in\"" not in API


def test_attachment_decline_leaves_a_breadcrumb():
    i = NOTIFY.index("notify attachment fetch declined")
    blk = NOTIFY[max(0, i - 400):i]
    assert 'ctype.startswith("image/")' in blk, (
        "the breadcrumb must live on the decline branch itself")


def test_preview_handler_carries_the_inflight_guard():
    i = APP_JS.index("let _previewRendering = false;")
    blk = APP_JS[i:APP_JS.index("edit-audio-form", i)]
    assert "if (_previewRendering) return;" in blk
    assert "_previewRendering = true;" in blk
    assert "} finally {" in blk and "_previewRendering = false;" in blk, (
        "the guard must re-arm on every exit path or one failed render "
        "bricks PREVIEW until the dialog reopens")


def test_v0_51_298_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "0.51.298: " in init_py
