"""v0.51.246 — no unguarded per-request DB work on the auth path.

AuthMiddleware.dispatch is `async def` and calls `_resolve_principal` inline
(api.py, no offload), so anything that path does runs ON the event loop. Two
things did:

  1. authenticate_token ran an UNCONDITIONAL `UPDATE api_tokens SET
     last_used_at = ?` per request on get_conn's 30s busy_timeout. One token
     request arriving while sync / plex_enum / a bulk op held the SQLite writer
     lock blocked in sqlite3 C code for up to 30s — freezing the WHOLE app,
     since the middleware runs on the loop. A Homepage widget polling
     /api/public/stats is a live trigger.

  2. setup_complete opened a fresh connection and SELECTed on EVERY request.

The session path had already learned (1) the hard way: v1.11.37 removed a
per-request last_seen_at UPDATE that "softlocked the UI", and v0.51.88 shipped
the safe form — coarse interval + short busy_timeout + skip-on-lock. That
remedy was never applied to tokens. This tag extracts it as
`_best_effort_touch` so the next auth-path write inherits it instead of
re-deriving it, and the lint below fails if one doesn't.
"""
from __future__ import annotations

import ast
import inspect
import re
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from app.core import auth
from app.core.auth import (
    TOKEN_TOUCH_INTERVAL_SECONDS, authenticate_token, create_admin,
    create_api_token, init_auth_schema, setup_complete,
)
from app.core.db import get_conn, init_db

REPO = Path(__file__).resolve().parent.parent


@pytest.fixture
def db(tmp_path):
    d = tmp_path / "m.db"
    init_db(d)
    init_auth_schema(d)
    auth._SETUP_COMPLETE_CACHE.clear()
    auth._TOUCH_LOCKED_WARNED.clear()
    yield d
    auth._SETUP_COMPLETE_CACHE.clear()
    auth._TOUCH_LOCKED_WARNED.clear()


def _mk_token(db) -> str:
    _id, raw = create_api_token(db, name="probe", scope="read")
    return raw


def _last_used(db):
    with get_conn(db) as c:
        return c.execute("SELECT last_used_at FROM api_tokens").fetchone()[0]


# ── behaviour: the write is rate-limited ─────────────────────────────────

def test_repeat_auth_does_not_rewrite_last_used(db):
    """THE fix. Pre-fix every single request wrote. Now a second auth inside the
    interval leaves the stamp alone — that is what makes it rare enough to be
    safe on the event loop."""
    raw = _mk_token(db)
    assert authenticate_token(db, raw) is not None
    first = _last_used(db)
    assert first is not None, "the first auth should stamp"
    time.sleep(1.1)  # ensure a DIFFERENT iso second would be written
    assert authenticate_token(db, raw) is not None
    assert _last_used(db) == first, "a second auth inside the interval re-wrote"


def test_a_stale_stamp_is_refreshed(db):
    """The gate must not be a permanent off-switch — an old stamp still updates,
    or the tokens table's 'last used' column would freeze forever."""
    raw = _mk_token(db)
    stale = (datetime.now(timezone.utc)
             - timedelta(seconds=TOKEN_TOUCH_INTERVAL_SECONDS + 60)
             ).isoformat(timespec="seconds")
    with get_conn(db) as c:
        c.execute("UPDATE api_tokens SET last_used_at = ?", (stale,))
        c.commit()
    assert authenticate_token(db, raw) is not None
    assert _last_used(db) != stale, "a stale stamp must refresh"


def test_auth_still_succeeds_when_the_write_is_skipped(db, monkeypatch):
    """The whole point of best-effort: a DB failure on the stamp must not fail
    the request. Auth already succeeded on the READ; the stamp is bookkeeping.
    Exercises the REAL helper — the SELECT gets a live connection, the touch
    that follows it gets a raising one."""
    raw = _mk_token(db)
    real = auth.get_conn
    calls = {"n": 0}

    def _flaky(*a, **kw):
        calls["n"] += 1
        if calls["n"] == 1:          # the authenticating SELECT must work
            return real(*a, **kw)
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(auth, "get_conn", _flaky)
    assert authenticate_token(db, raw) is not None, (
        "a failing bookkeeping write must not break authentication")
    assert calls["n"] >= 2, "the touch never attempted — test proves nothing"


def test_a_locked_db_is_skipped_and_breadcrumbed(db, caplog):
    """class-9: the swallow needs a log line, and it must not drown the log."""
    with caplog.at_level("WARNING"):
        auth._best_effort_touch(db, "unit-probe",
                                "UPDATE api_tokens SET last_used_at = ?"
                                " WHERE id = (SELECT 1 FROM nope)", ("x",))
    # a malformed statement raises OperationalError the same way a lock does
    assert "unit-probe" in caplog.text or auth._TOUCH_LOCKED_WARNED, (
        "a skipped bookkeeping write must leave a breadcrumb")


# ── setup_complete caching ───────────────────────────────────────────────

def test_setup_complete_stops_hitting_the_db_once_true(db, monkeypatch):
    """Called on EVERY request from dispatch. Pre-fix that was a fresh
    sqlite3.connect + SELECT per request, on the event loop."""
    create_admin(db, username="admin", password="correct horse battery")
    assert setup_complete(db) is True
    opened = []
    real = auth.get_conn
    monkeypatch.setattr(auth, "get_conn",
                        lambda *a, **kw: (opened.append(1), real(*a, **kw))[1])
    for _ in range(5):
        assert setup_complete(db) is True
    assert not opened, f"cached True still opened the DB {len(opened)}x"


def test_setup_complete_false_is_never_cached(db):
    """False MUST stay live or the first-run /setup flow could never flip."""
    assert setup_complete(db) is False
    create_admin(db, username="admin", password="correct horse battery")
    assert setup_complete(db) is True, (
        "a cached False would strand the install on /setup forever")


# ── the class lint: every auth-path write goes through the helper ────────

def test_no_bare_update_on_an_auth_path_function():
    """The durable guard. Both softlocks came from a bookkeeping UPDATE written
    inline on a function the middleware calls per request. Any new one must go
    through _best_effort_touch (short busy_timeout + skip-on-lock) rather than
    re-deriving the safe form — which is exactly what the token path failed to
    do for three tags."""
    src = (REPO / "app" / "core" / "auth.py").read_text()
    tree = ast.parse(src)
    # functions reachable per-request from AuthMiddleware._resolve_principal
    AUTH_PATH = {"authenticate_token", "lookup_session", "setup_complete",
                 "_verify_token", "_maybe_touch_token_last_used",
                 "_touch_session_last_seen"}
    offenders = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef) or node.name not in AUTH_PATH:
            continue
        body = ast.get_source_segment(src, node) or ""
        for m in re.finditer(r'"\s*(UPDATE|INSERT|DELETE)\s', body, re.I):
            # allowed only as the sql ARGUMENT to _best_effort_touch
            before = body[:m.start()].rstrip().rstrip("(,").rstrip()
            if not before.endswith("_best_effort_touch") and \
               "_best_effort_touch(" not in body[max(0, m.start() - 200):m.start()]:
                offenders.append((node.name, m.group(1)))
    assert not offenders, (
        "auth-path function does a write outside _best_effort_touch — it will "
        f"block the event loop on the writer lock: {offenders}")


def test_the_helper_keeps_its_two_load_bearing_properties():
    """Both halves matter: the short timeout stops the 30s block, the except
    stops a locked DB from failing an otherwise-valid request."""
    # strip comments first: a bare `in src` check is satisfied by this tag's own
    # rationale comment, which is a mirror not a guard (the trap that produced
    # two phantom guards in the v0.51.233-242 run).
    src = "\n".join(ln for ln in inspect.getsource(auth._best_effort_touch).splitlines()
                    if not ln.lstrip().startswith("#"))
    assert "PRAGMA busy_timeout = 250" in src, "lost the short timeout"
    assert "except sqlite3.Error" in src, "lost the skip-on-lock"
    assert "except sqlite3.OperationalError" not in src, (
        "narrowed back to OperationalError — any other sqlite error would turn "
        "a valid auth into a failure")


def test_both_touch_sites_route_through_the_helper():
    """Named explicitly so a refactor that inlines one is caught by name, not
    only by the general lint above."""
    src = (REPO / "app" / "core" / "auth.py").read_text()
    for fn in ("_touch_session_last_seen", "_maybe_touch_token_last_used"):
        i = src.index(f"def {fn}")
        j = src.find("\ndef ", i + 1)
        assert "_best_effort_touch(" in src[i:j if j > 0 else len(src)], fn


# ── v0.51.248: end-to-end through the real middleware ────────────────────

def test_repeated_token_polls_write_once_through_the_real_app(tmp_path, monkeypatch):
    """The unit tests above call authenticate_token directly. This drives real
    HTTP through AuthMiddleware.dispatch — the path that actually froze — and
    pins the whole point of the tag: a Homepage widget polling forever must
    produce ONE write, not one per poll."""
    from starlette.testclient import TestClient
    from app.config import Settings
    from app.web.api import create_app
    monkeypatch.delenv("MOTIF_TRUST_FORWARD_AUTH", raising=False)
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    auth._SETUP_COMPLETE_CACHE.clear()
    create_admin(s.db_path, username="a", password="correct horse battery")
    _id, raw = create_api_token(s.db_path, name="homepage", scope="read")
    c = TestClient(create_app(s))
    hdr = {"Authorization": f"Bearer {raw}"}

    assert c.get("/healthz").status_code == 200
    assert c.get("/api/public/stats").status_code == 401, "unauth must stay 401"
    assert c.get("/api/public/stats", headers=hdr).status_code == 200

    def stamp():
        with get_conn(s.db_path) as cx:
            return cx.execute("SELECT last_used_at FROM api_tokens").fetchone()[0]

    first = stamp()
    assert first is not None
    for _ in range(15):
        assert c.get("/api/public/stats", headers=hdr).status_code == 200
    assert stamp() == first, "a poll inside the interval re-wrote — the freeze is back"
    # and auth is not weakened by the caching/gating
    assert c.get("/api/public/stats",
                 headers={"Authorization": "Bearer thmr_bogus"}).status_code == 401
