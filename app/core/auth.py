"""
Authentication.

Three modes are supported and they auto-coexist:

1. Session cookies — local username + bcrypt password. Set on /login, cleared
   on /logout. Cookie name 'motif_sess', 30-day expiry, HttpOnly, SameSite=Lax.
   The Secure flag follows web.cookie_secure (v1.21.17): "auto" (default)
   infers it from X-Forwarded-Proto / scheme, "on"/"off" force it. /login is
   rate-limited per-IP (v1.21.18) against brute force.

2. API tokens — opaque random strings, stored hashed (bcrypt). Sent as
   `Authorization: Bearer <token>` or `?api_key=<token>`. Each token has a
   scope of 'read' or 'admin'. Useful for Homepage / scripts.

3. Forward-auth header — when MOTIF_TRUST_FORWARD_AUTH is true, the
   X-Authentik-Username header is trusted and grants admin access. This is
   the path the user uses when behind NPM + Authentik.

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
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

import bcrypt

from .db import get_conn

log = logging.getLogger(__name__)

# v1.17.11: once-per-process flags for class-9 swallows in
# verify_password / _verify_token. A malformed hash in admin or
# api_tokens makes bcrypt raise ValueError/TypeError on EVERY
# auth attempt — without a breadcrumb the operator just sees a
# locked-out login screen with no clue that the stored hash is
# corrupt. Once-per-process flag means the first failed verify
# logs at warn so the operator can grep "corrupt hash" in logs;
# subsequent failures stay quiet (they can't auth anyway).
_VERIFY_PASSWORD_WARNED: bool = False
_VERIFY_TOKEN_WARNED: bool = False

SESSION_COOKIE = "motif_sess"
SESSION_TTL_SECONDS = 30 * 24 * 3600  # 30 days

# Routes that bypass auth entirely. Healthcheck for Docker, static assets, and
# the auth pages themselves. v1.11.41: /api/public/stats was demoted from this
# list — Homepage / external dashboards must now present a read-scope API
# token (Authorization: Bearer or ?api_key=). Exposing aggregate counters
# unauth'd was a leak of library size, queue health, and sync-status hints
# to anyone who could probe the URL.
PUBLIC_PATHS: set[str] = {
    "/healthz",
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
    except (ValueError, TypeError) as e:
        # v1.17.11: class-9 fix — bcrypt raises on a malformed hash
        # (admin row got mangled, hash field was truncated, etc.).
        # Fail-closed is correct, but pre-fix the operator saw a
        # locked-out login screen with no clue that the stored hash
        # is corrupt. Once-per-process warn so the breadcrumb lands
        # in logs; subsequent failures stay quiet.
        global _VERIFY_PASSWORD_WARNED
        if not _VERIFY_PASSWORD_WARNED:
            log.warning(
                "verify_password: bcrypt raised %s (%s) — the stored "
                "admin password hash appears corrupt. Login will "
                "fail until the hash is repaired or reset.",
                type(e).__name__, e,
            )
            _VERIFY_PASSWORD_WARNED = True
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


def change_admin_password(db_path: Path, *, current_password: str,
                          new_password: str,
                          keep_session_id: str | None = None) -> bool:
    """Verify current password, then update. Returns True on success.

    v0.51.16 (audit #19): rotating the password now revokes every OTHER
    session — pre-fix a stolen/lingering motif_sess cookie survived the
    rotation for the full 30-day TTL (lookup_session binds only on id +
    expiry, not the hash), defeating the point of changing it. The caller's
    own session (keep_session_id) stays alive so the admin isn't logged out
    by their own rotation. API tokens are deliberately NOT revoked — they
    have their own lifecycle + revocation UI at /settings#tokens."""
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
        if keep_session_id:
            conn.execute("DELETE FROM sessions WHERE id != ?",
                         (keep_session_id,))
        else:
            conn.execute("DELETE FROM sessions")
    return True


# v0.51.78 (security audit): a real rounds=12 bcrypt hash used ONLY to equalize the
# timing of the no-admin login branch below. Not a secret — its plaintext is
# irrelevant; it exists purely so bcrypt.checkpw runs a full ~235ms compare.
_DUMMY_PW_HASH = "$2b$12$n3/MPSAAl4c9WSEJfE2aR.wQ8UGoO6hTqZu802AAzGP/6tunsf42q"


def authenticate_password(db_path: Path, *, username: str, password: str) -> bool:
    """Constant-time-ish credential check."""
    with get_conn(db_path) as conn:
        row = conn.execute(
            "SELECT username, password_hash FROM admin WHERE id = 1"
        ).fetchone()
    if row is None:
        # v0.51.78 (security audit): burn ~the same time as a real verify so the
        # no-admin path doesn't leak (via timing) that no admin exists — WITHOUT
        # raising. The old b"$2b$12$"+b"x"*53 was a MALFORMED 60-byte salt →
        # bcrypt.checkpw raised ValueError in microseconds (defense inert + INVERTED:
        # no-admin returned FASTER), and the unhandled ValueError 500'd POST /login on
        # a fresh/wiped-admin install (exempting that state from the rate limit).
        # verify_password swallows any bcrypt error; _DUMMY_PW_HASH is a real hash.
        verify_password("x", _DUMMY_PW_HASH)
        return False
    # v1.22.77: compare UTF-8 bytes — compare_digest raises TypeError on
    # non-ASCII str inputs, so a non-ASCII admin username 500'd every
    # login (permanent lockout, no breadcrumb) and the 500 fired BEFORE
    # record_login_failure, exempting such probes from the rate limit.
    user_match = hmac.compare_digest(
        row["username"].encode("utf-8"), username.encode("utf-8"))
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
    except (ValueError, TypeError) as e:
        # v1.17.11: class-9 fix — same shape as verify_password
        # above. A corrupt api_tokens.token_hash makes every request
        # using that token 401 with no clue why. Once-per-process
        # warn captures the first occurrence.
        global _VERIFY_TOKEN_WARNED
        if not _VERIFY_TOKEN_WARNED:
            log.warning(
                "_verify_token: bcrypt raised %s (%s) — at least "
                "one api_tokens row has a corrupt hash. Affected "
                "tokens will 401 until rotated.",
                type(e).__name__, e,
            )
            _VERIFY_TOKEN_WARNED = True
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


# ── v1.21.18 (security audit): login rate-limiting ──────────────────────────
# Pre-fix the bcrypt /login endpoint had no throttle — unlimited attempts
# against the single admin account (bcrypt rounds=12 slows throughput but
# doesn't stop a patient online guess). This is a per-IP rolling-window
# failed-attempt counter: after LOGIN_MAX_FAILURES failures within
# LOGIN_FAILURE_WINDOW_S, the IP is refused with a 429 WITHOUT a password
# check until the failures age out. In-memory + monotonic-clock (resets on
# restart, which matches how a deploy fix lands). NOTE: the source IP comes
# from request.client.host, which is spoofable while uvicorn runs
# forwarded_allow_ips="*" (the deferred forward-auth item) — so this raises
# the bar against unsophisticated brute force but isn't airtight until that
# is resolved. Authentik fronts /login in production regardless.
LOGIN_MAX_FAILURES = 10
LOGIN_FAILURE_WINDOW_S = 900.0

_LOGIN_FAIL_LOCK = threading.Lock()
_LOGIN_FAILS: dict[str, list[float]] = {}


def _login_purge_locked(ip: str, now: float) -> list[float]:
    fresh = [t for t in _LOGIN_FAILS.get(ip, []) if now - t < LOGIN_FAILURE_WINDOW_S]
    if fresh:
        _LOGIN_FAILS[ip] = fresh
    else:
        _LOGIN_FAILS.pop(ip, None)
    return fresh


def login_rate_limited(ip: str) -> bool:
    """True if `ip` has reached LOGIN_MAX_FAILURES failed attempts within the
    rolling window — the caller should 429 without checking the password."""
    if not ip:
        return False
    now = time.monotonic()
    with _LOGIN_FAIL_LOCK:
        return len(_login_purge_locked(ip, now)) >= LOGIN_MAX_FAILURES


def record_login_failure(ip: str) -> None:
    """Stamp a failed login for `ip`."""
    if not ip:
        return
    now = time.monotonic()
    with _LOGIN_FAIL_LOCK:
        _login_purge_locked(ip, now)
        _LOGIN_FAILS.setdefault(ip, []).append(now)


def clear_login_failures(ip: str) -> None:
    """Reset an IP's failure history (called on a successful login)."""
    if not ip:
        return
    with _LOGIN_FAIL_LOCK:
        _LOGIN_FAILS.pop(ip, None)


def _reset_login_failures_for_test() -> None:
    with _LOGIN_FAIL_LOCK:
        _LOGIN_FAILS.clear()
