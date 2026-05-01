"""
Authentication.

Three modes are supported and they auto-coexist:

1. Session cookies — local username + bcrypt password. Set on /login, cleared
   on /logout. Cookie name 'motif_sess', 30-day expiry, HttpOnly, SameSite=Lax.
   On HTTPS deployments the Secure flag is set automatically based on the
   X-Forwarded-Proto header.

2. API tokens — opaque random strings, stored hashed (bcrypt). Sent as
   `Authorization: Bearer <token>` or `?api_key=<token>`. Each token has a
   scope of 'read' or 'admin'. Useful for Homepage / scripts.

3. Forward-auth header — when MOTIF_TRUST_FORWARD_AUTH is true, the
   X-Authentik-Username header is trusted and grants admin access. This is
   the path Connor uses when behind NPM + Authentik.

First-run: when the admin table is empty, every non-public route 302s to
/setup, which displays a one-time form to set the admin username + password.
After that route exits, the schema_version row is updated and /setup
becomes inaccessible.

A single admin account is supported. Multiple users were considered but
rejected as scope creep — a homelab tool with one user 99% of the time
doesn't need RBAC. API tokens cover the "different consumer, different
credential" need.
"""
from __future__ import annotations

import hashlib
import hmac
import logging
import secrets
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

import bcrypt

from .db import get_conn

log = logging.getLogger(__name__)

SESSION_COOKIE = "motif_sess"
SESSION_TTL_SECONDS = 30 * 24 * 3600  # 30 days

# Routes that bypass auth entirely. Healthcheck for Docker, public stats for
# Homepage, static assets, and the auth pages themselves.
PUBLIC_PATHS: set[str] = {
    "/healthz",
    "/api/public/stats",
    "/login",
    "/logout",
    "/setup",
}
PUBLIC_PREFIXES: tuple[str, ...] = ("/static/",)


Scope = Literal["read", "admin"]


@dataclass
class Principal:
    """Who is making this request."""
    username: str
    scope: Scope
    auth_method: Literal["session", "token", "forward_auth", "anonymous"]

    @property
    def is_admin(self) -> bool:
        return self.scope == "admin"

    @property
    def is_authenticated(self) -> bool:
        return self.auth_method != "anonymous"


ANONYMOUS = Principal(username="anonymous", scope="read", auth_method="anonymous")


# -------- Password hashing --------

def hash_password(password: str) -> str:
    if len(password) < 8:
        raise ValueError("Password must be at least 8 characters")
    if len(password) > 128:
        # bcrypt truncates at 72 bytes; reject anything weird before it gets
        # silently mangled
        raise ValueError("Password must be at most 128 characters")
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt(rounds=12)).decode("utf-8")


def verify_password(password: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(password.encode("utf-8"), hashed.encode("utf-8"))
    except (ValueError, TypeError):
        return False


# -------- Schema additions --------

ADDITIONAL_SCHEMA = """
CREATE TABLE IF NOT EXISTS admin (
    id              INTEGER PRIMARY KEY CHECK (id = 1),
    username        TEXT NOT NULL,
    password_hash   TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    last_login_at   TEXT
);

CREATE TABLE IF NOT EXISTS api_tokens (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT NOT NULL,
    token_prefix    TEXT NOT NULL,           -- first 8 chars of the raw token, for display
    token_hash      TEXT NOT NULL,           -- bcrypt of the full token
    scope           TEXT NOT NULL CHECK (scope IN ('read', 'admin')),
    created_at      TEXT NOT NULL,
    last_used_at    TEXT,
    revoked_at      TEXT
);

CREATE INDEX IF NOT EXISTS idx_api_tokens_active
    ON api_tokens (revoked_at) WHERE revoked_at IS NULL;

CREATE TABLE IF NOT EXISTS sessions (
    id              TEXT PRIMARY KEY,        -- random session id
    username        TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    expires_at      TEXT NOT NULL,
    last_seen_at    TEXT NOT NULL,
    user_agent      TEXT
);

CREATE INDEX IF NOT EXISTS idx_sessions_expires ON sessions (expires_at);
"""


def init_auth_schema(db_path: Path) -> None:
    with get_conn(db_path) as conn:
        conn.executescript(ADDITIONAL_SCHEMA)


# -------- Setup state --------

def setup_complete(db_path: Path) -> bool:
    """True once an admin account has been provisioned."""
    with get_conn(db_path) as conn:
        row = conn.execute("SELECT 1 FROM admin WHERE id = 1").fetchone()
    return row is not None


def create_admin(db_path: Path, *, username: str, password: str) -> None:
    """One-shot admin creation. Refuses if an admin already exists."""
    if not username or not username.strip():
        raise ValueError("Username is required")
    if len(username) > 64:
        raise ValueError("Username too long")
    pw_hash = hash_password(password)
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with get_conn(db_path) as conn:
        existing = conn.execute("SELECT 1 FROM admin WHERE id = 1").fetchone()
        if existing:
            raise RuntimeError("Admin already exists; use /settings to change the password")
        conn.execute(
            """INSERT INTO admin (id, username, password_hash, created_at)
               VALUES (1, ?, ?, ?)""",
            (username.strip(), pw_hash, now),
        )


def change_admin_password(db_path: Path, *, current_password: str, new_password: str) -> bool:
    """Verify current password, then update. Returns True on success."""
    with get_conn(db_path) as conn:
        row = conn.execute("SELECT password_hash FROM admin WHERE id = 1").fetchone()
        if row is None:
            return False
        if not verify_password(current_password, row["password_hash"]):
            return False
        new_hash = hash_password(new_password)
        conn.execute(
            "UPDATE admin SET password_hash = ? WHERE id = 1",
            (new_hash,),
        )
    return True


def authenticate_password(db_path: Path, *, username: str, password: str) -> bool:
    """Constant-time-ish credential check."""
    with get_conn(db_path) as conn:
        row = conn.execute(
            "SELECT username, password_hash FROM admin WHERE id = 1"
        ).fetchone()
    if row is None:
        # Run a dummy bcrypt to keep timing roughly constant
        bcrypt.checkpw(b"x", b"$2b$12$" + b"x" * 53)
        return False
    user_match = hmac.compare_digest(row["username"], username)
    pw_match = verify_password(password, row["password_hash"])
    if user_match and pw_match:
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        with get_conn(db_path) as conn:
            conn.execute(
                "UPDATE admin SET last_login_at = ? WHERE id = 1", (now,),
            )
        return True
    return False


# -------- Sessions --------

def create_session(db_path: Path, *, username: str, user_agent: str | None) -> str:
    sid = secrets.token_urlsafe(32)
    now = datetime.now(timezone.utc)
    expires = datetime.fromtimestamp(now.timestamp() + SESSION_TTL_SECONDS, tz=timezone.utc)
    with get_conn(db_path) as conn:
        conn.execute(
            """INSERT INTO sessions (id, username, created_at, expires_at,
                                     last_seen_at, user_agent)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (sid, username, now.isoformat(timespec="seconds"),
             expires.isoformat(timespec="seconds"),
             now.isoformat(timespec="seconds"),
             (user_agent or "")[:200]),
        )
    return sid


def lookup_session(db_path: Path, session_id: str) -> str | None:
    """Return the username if the session is valid, None otherwise."""
    if not session_id:
        return None
    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with get_conn(db_path) as conn:
        row = conn.execute(
            """SELECT username, expires_at FROM sessions
               WHERE id = ? AND expires_at > ?""",
            (session_id, now_iso),
        ).fetchone()
    if row is None:
        return None
    # v1.11.37: dropped the per-request UPDATE sessions SET last_seen_at
    # touch. Even with the v1.11.35 try/except, the UPDATE still
    # WAITED on the writer lock during a long sync — every
    # authenticated request blocked up to busy_timeout, making the
    # whole UI feel softlocked. last_seen_at is diagnostic-only
    # (no security or expiry logic depends on it); the SELECT above
    # is authoritative for authentication.
    return row["username"]


def destroy_session(db_path: Path, session_id: str) -> None:
    if not session_id:
        return
    with get_conn(db_path) as conn:
        conn.execute("DELETE FROM sessions WHERE id = ?", (session_id,))


def cleanup_expired_sessions(db_path: Path) -> int:
    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with get_conn(db_path) as conn:
        cur = conn.execute(
            "DELETE FROM sessions WHERE expires_at <= ?", (now_iso,),
        )
        return cur.rowcount


# -------- API tokens --------

def _hash_token(raw: str) -> str:
    # bcrypt has a 72-byte input cap. Tokens are 43 chars (urlsafe 32-byte),
    # comfortably under. We pre-sha256 anyway for defense-in-depth and to make
    # rotation simpler if we ever bump the algorithm.
    pre = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return bcrypt.hashpw(pre.encode("utf-8"), bcrypt.gensalt(rounds=10)).decode("utf-8")


def _verify_token(raw: str, hashed: str) -> bool:
    pre = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    try:
        return bcrypt.checkpw(pre.encode("utf-8"), hashed.encode("utf-8"))
    except (ValueError, TypeError):
        return False


def create_api_token(db_path: Path, *, name: str, scope: Scope) -> tuple[int, str]:
    """Create an API token. Returns (id, raw_token). The raw token is shown
    once at creation time and never stored — caller must hand it to the user
    immediately."""
    if scope not in ("read", "admin"):
        raise ValueError("Invalid scope")
    if not name.strip():
        raise ValueError("Name is required")
    raw = "thmr_" + secrets.token_urlsafe(32)
    prefix = raw[:12]  # "thmr_" + 7 chars, displayed as a hint
    hashed = _hash_token(raw)
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with get_conn(db_path) as conn:
        cur = conn.execute(
            """INSERT INTO api_tokens (name, token_prefix, token_hash, scope, created_at)
               VALUES (?, ?, ?, ?, ?)""",
            (name.strip()[:64], prefix, hashed, scope, now),
        )
        token_id = cur.lastrowid
    return token_id, raw


def list_api_tokens(db_path: Path) -> list[dict]:
    with get_conn(db_path) as conn:
        rows = conn.execute(
            """SELECT id, name, token_prefix, scope, created_at,
                      last_used_at, revoked_at
               FROM api_tokens ORDER BY id DESC"""
        ).fetchall()
    return [dict(r) for r in rows]


def revoke_api_token(db_path: Path, token_id: int) -> bool:
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with get_conn(db_path) as conn:
        cur = conn.execute(
            "UPDATE api_tokens SET revoked_at = ? WHERE id = ? AND revoked_at IS NULL",
            (now, token_id),
        )
    return cur.rowcount > 0


def authenticate_token(db_path: Path, raw_token: str) -> Principal | None:
    """Look up an API token and return a Principal if valid."""
    if not raw_token or not raw_token.startswith("thmr_"):
        return None
    prefix = raw_token[:12]
    with get_conn(db_path) as conn:
        # Narrow the candidate set by prefix to avoid bcrypt-checking every row
        rows = conn.execute(
            """SELECT id, name, token_hash, scope FROM api_tokens
               WHERE token_prefix = ? AND revoked_at IS NULL""",
            (prefix,),
        ).fetchall()
        for row in rows:
            if _verify_token(raw_token, row["token_hash"]):
                now = datetime.now(timezone.utc).isoformat(timespec="seconds")
                conn.execute(
                    "UPDATE api_tokens SET last_used_at = ? WHERE id = ?",
                    (now, row["id"]),
                )
                return Principal(
                    username=f"token:{row['name']}",
                    scope=row["scope"],
                    auth_method="token",
                )
    return None
