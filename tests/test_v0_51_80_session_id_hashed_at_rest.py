"""v0.51.80 — CSS+login audit (login half): session ids hashed at rest.

Pre-fix `sessions.id` stored the raw `motif_sess` cookie value. Anyone who
could read the DB — a SQL-injection foothold on some other endpoint, a leaked
`motif.db` backup, or plain filesystem access to `/config` — walked away with
live session tokens they could replay as the admin for the full 30-day TTL.

Fix: store `sha256(session_id)` in `sessions.id`; the raw token lives only in
the client cookie. On every request we hash the incoming cookie and match the
digest. A DB read now yields only the digest, which is useless for replay (the
raw token is 256 bits of `secrets.token_urlsafe` entropy — not reversible, not
brute-forceable). No schema change; pre-existing raw-id rows simply stop
matching and expire (one forced re-login for the admin).

These are behavioral tests (real DB, real functions) — the mechanism is proven
by asserting on the at-rest value directly, not by pinning source text.
"""
from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path


def _auth_db(tmp_path: Path) -> Path:
    from app.core.auth import create_admin, init_auth_schema
    db = tmp_path / "auth.db"
    init_auth_schema(db)
    create_admin(db, username="admin", password="old-password")
    return db


def _stored_ids(db: Path) -> list[str]:
    with sqlite3.connect(db) as conn:
        return [r[0] for r in conn.execute("SELECT id FROM sessions").fetchall()]


def test_session_id_stored_as_sha256_not_raw(tmp_path):
    """The row's id column is sha256(cookie), and the raw cookie value never
    appears in the table."""
    from app.core.auth import create_session
    db = _auth_db(tmp_path)
    sid = create_session(db, username="admin", user_agent="me")
    stored = _stored_ids(db)
    assert stored == [hashlib.sha256(sid.encode("utf-8")).hexdigest()], (
        "sessions.id must hold sha256(cookie), not the raw token")
    assert sid not in stored, "the raw cookie value must never be stored at rest"


def test_lookup_authenticates_the_raw_cookie(tmp_path):
    """The transparent round-trip still works: the raw cookie authenticates."""
    from app.core.auth import create_session, lookup_session
    db = _auth_db(tmp_path)
    sid = create_session(db, username="admin", user_agent="me")
    assert lookup_session(db, sid) == "admin"


def test_leaked_at_rest_digest_cannot_be_replayed(tmp_path):
    """The whole point: presenting the AT-REST value (what a DB thief steals)
    as if it were a cookie must NOT authenticate — the lookup hashes the
    incoming value, so the stored digest gets double-hashed and misses."""
    from app.core.auth import create_session, lookup_session
    db = _auth_db(tmp_path)
    sid = create_session(db, username="admin", user_agent="me")
    at_rest = hashlib.sha256(sid.encode("utf-8")).hexdigest()
    assert lookup_session(db, at_rest) is None, (
        "a DB-leaked digest replayed as a cookie must not authenticate")


def test_destroy_session_removes_the_hashed_row(tmp_path):
    """destroy_session takes the raw cookie and must delete the hashed row."""
    from app.core.auth import create_session, destroy_session, lookup_session
    db = _auth_db(tmp_path)
    sid = create_session(db, username="admin", user_agent="me")
    destroy_session(db, sid)
    assert lookup_session(db, sid) is None
    assert _stored_ids(db) == [], "the row must be gone after destroy_session"


def test_rotation_keep_session_hashes_the_raw_cookie(tmp_path):
    """change_admin_password keeps the caller's own (raw-cookie) session and
    revokes the rest — the keep id must be hashed for the WHERE id != ? match,
    or the rotation deletes the admin's OWN hashed row and logs them out."""
    from app.core.auth import (
        change_admin_password, create_session, lookup_session,
    )
    db = _auth_db(tmp_path)
    keep = create_session(db, username="admin", user_agent="me")
    other = create_session(db, username="admin", user_agent="thief")
    assert change_admin_password(
        db, current_password="old-password", new_password="new-password",
        keep_session_id=keep,
    ) is True
    assert lookup_session(db, keep) == "admin", (
        "the rotating admin's own session must survive the hashed keep-match")
    assert lookup_session(db, other) is None, "other sessions must be revoked"


def test_login_cookie_absent_from_db_end_to_end(tmp_path, monkeypatch):
    """Full TestClient path: POST /login sets a motif_sess cookie whose raw
    value is NOT present anywhere in the sessions table, and that same cookie
    still authenticates a follow-up request."""
    from fastapi.testclient import TestClient

    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import SESSION_COOKIE, create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="admin", password="password123")

    client = TestClient(create_app(s))
    r = client.post("/login", data={"username": "admin", "password": "password123",
                                    "next": "/"}, follow_redirects=False)
    assert r.status_code == 302, r.text
    cookie = r.cookies.get(SESSION_COOKIE)
    assert cookie, "login must set the session cookie"
    assert cookie not in _stored_ids(s.db_path), (
        "the raw login cookie must never be stored at rest")
    # and the cookie authenticates a follow-up request (not bounced to /login)
    r2 = client.get("/", follow_redirects=False)
    assert r2.status_code == 200, (
        f"the session cookie must authenticate; got {r2.status_code}")
