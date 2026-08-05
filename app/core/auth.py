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
import sqlite3
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
_TOUCH_LOCKED_WARNED: set[str] = set()  # v0.51.246: per-site, was a single bool
# v0.51.246: keyed by db_path so tests with temp DBs don't poison each other.
_SETUP_COMPLETE_CACHE: dict[str, bool] = {}

SESSION_COOKIE = "motif_sess"
SESSION_TTL_SECONDS = 30 * 24 * 3600  # 30 days
# v0.51.88: idle timeout — a session unused for longer than this is dead even
# within the 30-day absolute TTL, so a lingering/stolen cookie can't stay live
# indefinitely on an account nobody's touching. Activity-based: last_seen_at is
# refreshed COARSELY (see lookup_session), never per request.
SESSION_IDLE_TIMEOUT_SECONDS = 14 * 24 * 3600  # 14 days
# How stale last_seen_at must be before lookup_session bothers to refresh it.
# Coarse on purpose — at most one write per session per interval, NOT per
# request: v1.11.37 removed the per-request UPDATE because it WAITED up to the
# 30s busy_timeout on the writer lock during a long sync and softlocked the UI.
# The refresh is also fail-fast + best-effort (see _touch_session_last_seen).
SESSION_TOUCH_INTERVAL_SECONDS = 3600  # 1 hour
# v0.51.246: separate from the session interval on purpose — these are
# independent policies; last_used_at is display-only so it can be coarse.
TOKEN_TOUCH_INTERVAL_SECONDS = 3600  # 1 hour

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
    """True once an admin account has been provisioned.

    v0.51.246: memoized. AuthMiddleware.dispatch calls this on EVERY request,
    synchronously on the event loop, so pre-fix every request paid a fresh
    sqlite3.connect + SELECT. Only the True result is cached: nothing in motif
    deletes the admin row (no DELETE FROM admin exists), so True is permanent,
    while False must stay live for the first-run /setup flow to flip."""
    if _SETUP_COMPLETE_CACHE.get(str(db_path)):
        return True
    with get_conn(db_path) as conn:
        row = conn.execute("SELECT 1 FROM admin WHERE id = 1").fetchone()
    if row is not None:
        _SETUP_COMPLETE_CACHE[str(db_path)] = True
        return True
    return False


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
    have their own lifecycle + revocation UI at /settings#tokens.

    v0.51.80: keep_session_id is the caller's RAW cookie value; hash it to
    match the sha256-at-rest sessions.id (else `id != raw` matches every
    hashed row and the rotation deletes the admin's OWN session too)."""
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
                         (_hash_session_id(keep_session_id),))
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

def _hash_session_id(raw: str) -> str:
    # v0.51.80 (CSS+login audit): sessions.id stores sha256(cookie), never the raw
    # cookie value. A DB read (SQLi via some other endpoint, a leaked backup, disk
    # access) then yields only the digest — useless for replay, since the raw token
    # carries 256 bits of secrets.token_urlsafe entropy (not brute-forceable, so no
    # salt/stretch is needed). Deterministic + unsalted on purpose: the per-request
    # lookup stays a plain indexed WHERE with no writer-lock cost (cf. the v1.11.37
    # note in lookup_session). Same primitive as _hash_token's pre-sha256 step.
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def create_session(db_path: Path, *, username: str, user_agent: str | None) -> str:
    sid = secrets.token_urlsafe(32)
    now = datetime.now(timezone.utc)
    expires = datetime.fromtimestamp(now.timestamp() + SESSION_TTL_SECONDS, tz=timezone.utc)
    with get_conn(db_path) as conn:
        conn.execute(
            """INSERT INTO sessions (id, username, created_at, expires_at,
                                     last_seen_at, user_agent)
               VALUES (?, ?, ?, ?, ?, ?)""",
            # store the hash; the RAW sid is returned to the caller for the cookie.
            (_hash_session_id(sid), username, now.isoformat(timespec="seconds"),
             expires.isoformat(timespec="seconds"),
             now.isoformat(timespec="seconds"),
             (user_agent or "")[:200]),
        )
    return sid


def _parse_iso_utc(s: str | None) -> datetime | None:
    """Parse a stored isoformat timestamp to an aware-UTC datetime, or None if
    missing/unparseable (last_seen_at is written via isoformat, always with a
    +00:00 offset; the tzinfo guard covers any legacy naive row)."""
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s)
    except (ValueError, TypeError):
        return None
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)


def _best_effort_touch(db_path: Path, what: str, sql: str, params: tuple) -> None:
    """Fire-and-forget bookkeeping write on an AUTH path. Fail-fast: a short
    busy_timeout overrides get_conn's 30s so a locked DB (mid-sync) SKIPS the
    write instead of blocking the request — the read that just authenticated is
    authoritative, and the stamp refreshes on a later request.

    This is the safe form of the per-request write v1.11.37 removed, whose
    comment reads "NEVER touch last_seen_at per request (the UPDATE waited on
    the writer lock during long syncs and softlocked the UI)". Callers MUST
    also rate-limit (see the *_TOUCH_INTERVAL_SECONDS gates) — rare AND
    non-blocking are both load-bearing.

    v0.51.246: extracted so the token path can't miss it the way it did for
    three tags. Every auth-path bookkeeping write goes through here."""
    try:
        with get_conn(db_path) as conn:
            conn.execute("PRAGMA busy_timeout = 250")  # not the 30s default
            conn.execute(sql, params)
    except sqlite3.Error as e:
        # locked past 250ms → skip; auth already succeeded, the stamp retries
        # next request. sqlite3.Error not just OperationalError: this is pure
        # bookkeeping, so NO db failure of any kind may turn a valid auth into
        # a 401. Warn once per site (class-9: a swallowed write needs a
        # breadcrumb, but this fires per-request so it must not drown the log).
        if what not in _TOUCH_LOCKED_WARNED:
            log.warning("%s: DB locked, skipped bookkeeping write "
                        "(auth still valid; retries next request)", what)
            _TOUCH_LOCKED_WARNED.add(what)


def _touch_session_last_seen(db_path: Path, hashed_id: str, now_iso: str) -> None:
    """Coarse refresh of a session's last_seen_at (v0.51.88 idle timeout)."""
    _best_effort_touch(
        db_path, "_touch_session_last_seen",
        "UPDATE sessions SET last_seen_at = ? WHERE id = ?",
        (now_iso, hashed_id),
    )


def lookup_session(db_path: Path, session_id: str) -> str | None:
    """Return the username if the session is valid, None otherwise."""
    if not session_id:
        return None
    now = datetime.now(timezone.utc)
    now_iso = now.isoformat(timespec="seconds")
    hashed_id = _hash_session_id(session_id)
    with get_conn(db_path) as conn:
        row = conn.execute(
            """SELECT username, last_seen_at FROM sessions
               WHERE id = ? AND expires_at > ?""",
            (hashed_id, now_iso),
        ).fetchone()
    if row is None:
        return None
    # v0.51.88: idle timeout. The WHERE above enforces the 30-day ABSOLUTE TTL;
    # this enforces the shorter ACTIVITY-based one — a session untouched for
    # longer than SESSION_IDLE_TIMEOUT_SECONDS is dead, so a lingering/stolen
    # cookie can't stay live indefinitely on an idle account.
    last_seen = _parse_iso_utc(row["last_seen_at"])
    if last_seen is None:
        # unparseable/NULL — repair it (best-effort) rather than idle-expire on
        # a data glitch; a valid last_seen_at makes future checks meaningful.
        _touch_session_last_seen(db_path, hashed_id, now_iso)
        return row["username"]
    idle_s = (now - last_seen).total_seconds()
    if idle_s > SESSION_IDLE_TIMEOUT_SECONDS:
        return None
    # v1.11.37 lesson: NEVER touch last_seen_at per request (the UPDATE waited on
    # the writer lock during long syncs and softlocked the UI). Only refresh when
    # it's already gone stale — so ~99% of requests do zero writes — and even
    # then fail-fast + best-effort (see _touch_session_last_seen).
    if idle_s > SESSION_TOUCH_INTERVAL_SECONDS:
        _touch_session_last_seen(db_path, hashed_id, now_iso)
    return row["username"]


def destroy_session(db_path: Path, session_id: str) -> None:
    if not session_id:
        return
    with get_conn(db_path) as conn:
        conn.execute("DELETE FROM sessions WHERE id = ?", (_hash_session_id(session_id),))


def cleanup_expired_sessions(db_path: Path) -> int:
    now = datetime.now(timezone.utc)
    now_iso = now.isoformat(timespec="seconds")
    # v0.51.88: also reap idle-timed-out sessions here, OFF the hot path (this
    # runs at boot + on the scheduler sweep). lookup_session already rejects
    # them immediately; this just deletes the stale rows so they don't linger
    # to the 30-day absolute TTL. ISO-8601 sorts lexically == chronologically
    # (all timestamps share the same +00:00 isoformat), so a string <= works.
    idle_cutoff = datetime.fromtimestamp(
        now.timestamp() - SESSION_IDLE_TIMEOUT_SECONDS, tz=timezone.utc,
    ).isoformat(timespec="seconds")
    with get_conn(db_path) as conn:
        cur = conn.execute(
            "DELETE FROM sessions WHERE expires_at <= ? OR last_seen_at <= ?",
            (now_iso, idle_cutoff),
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
            """SELECT id, name, token_hash, scope, last_used_at FROM api_tokens
               WHERE token_prefix = ? AND revoked_at IS NULL""",
            (prefix,),
        ).fetchall()
        for row in rows:
            if _verify_token(raw_token, row["token_hash"]):
                _maybe_touch_token_last_used(db_path, row)
                return Principal(
                    username=f"token:{row['name']}",
                    scope=row["scope"],
                    auth_method="token",
                )
    return None


def _maybe_touch_token_last_used(db_path: Path, row) -> None:
    """v0.51.246: this was an UNCONDITIONAL per-request UPDATE on get_conn's
    30s busy_timeout, run from AuthMiddleware.dispatch. One token request
    arriving while sync/plex_enum/a bulk op held the writer lock blocked in
    sqlite3 for up to 30s — and because the middleware is on the event loop,
    that froze the WHOLE app, not just that request. Exactly the softlock
    v1.11.37 removed from the session path; the safe form shipped there in
    v0.51.88 and was never applied here.

    last_used_at is display-only (the tokens table's "last used" column), so
    hourly granularity costs nothing and the write becomes rare + skippable."""
    last = _parse_iso_utc(row["last_used_at"])
    if last is not None:
        age_s = (datetime.now(timezone.utc) - last).total_seconds()
        if age_s < TOKEN_TOUCH_INTERVAL_SECONDS:
            return
    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    _best_effort_touch(
        db_path, "_maybe_touch_token_last_used",
        "UPDATE api_tokens SET last_used_at = ? WHERE id = ?",
        (now_iso, row["id"]),
    )


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

# v0.51.81 (CSS+login audit): a GLOBAL failed-attempt ceiling on top of the
# per-IP counter. The per-IP limit is defeated by an attacker rotating source
# IPs — trivial, since request.client.host is spoofable while uvicorn runs
# forwarded_allow_ips="*" (the deferred forward-auth item). motif has exactly
# ONE admin account, so a high failure volume across ALL IPs is unambiguously
# an attack, not legitimate fat-fingering. Once the ceiling is reached the
# endpoint refuses EVERYONE (429, no password check) until the failures age
# out — a deliberate short lockout that caps both the guess rate and the
# bcrypt-CPU burn under a distributed attack. Set well above any human's
# mistyping (a real admin won't fail 50x in 15 min); a success does NOT clear
# it (only the legit admin knows the password, so wiping the attacker's counter
# on the admin's login would just hand back attempts). Authentik still fronts
# /login in production; this is defense-in-depth for a forward-auth bypass.
LOGIN_GLOBAL_MAX_FAILURES = 50
LOGIN_GLOBAL_WINDOW_S = 900.0

_LOGIN_FAIL_LOCK = threading.Lock()
_LOGIN_FAILS: dict[str, list[float]] = {}
_LOGIN_FAILS_GLOBAL: list[float] = []


def _login_purge_locked(ip: str, now: float) -> list[float]:
    fresh = [t for t in _LOGIN_FAILS.get(ip, []) if now - t < LOGIN_FAILURE_WINDOW_S]
    if fresh:
        _LOGIN_FAILS[ip] = fresh
    else:
        _LOGIN_FAILS.pop(ip, None)
    return fresh


def _login_global_purge_locked(now: float) -> list[float]:
    global _LOGIN_FAILS_GLOBAL
    _LOGIN_FAILS_GLOBAL = [t for t in _LOGIN_FAILS_GLOBAL
                          if now - t < LOGIN_GLOBAL_WINDOW_S]
    return _LOGIN_FAILS_GLOBAL


def login_rate_limited(ip: str) -> bool:
    """True if the GLOBAL across-all-IPs ceiling has been reached (v0.51.81) OR
    `ip` has hit its own LOGIN_MAX_FAILURES within the rolling window — either
    way the caller should 429 without checking the password. The global check
    applies even to an IP (or a missing IP) with no personal failure history."""
    now = time.monotonic()
    with _LOGIN_FAIL_LOCK:
        if len(_login_global_purge_locked(now)) >= LOGIN_GLOBAL_MAX_FAILURES:
            return True
        if not ip:
            return False
        return len(_login_purge_locked(ip, now)) >= LOGIN_MAX_FAILURES


def record_login_failure(ip: str) -> None:
    """Stamp a failed login against both the global ceiling and (if present)
    the per-IP bucket. A missing IP still counts toward the global ceiling —
    an attacker sending a blank client host can't dodge it (v0.51.81)."""
    now = time.monotonic()
    with _LOGIN_FAIL_LOCK:
        _login_global_purge_locked(now)
        _LOGIN_FAILS_GLOBAL.append(now)
        if ip:
            _login_purge_locked(ip, now)
            _LOGIN_FAILS.setdefault(ip, []).append(now)


def clear_login_failures(ip: str) -> None:
    """Reset an IP's failure history (called on a successful login)."""
    if not ip:
        return
    with _LOGIN_FAIL_LOCK:
        _LOGIN_FAILS.pop(ip, None)


def _reset_login_failures_for_test() -> None:
    global _LOGIN_FAILS_GLOBAL
    with _LOGIN_FAIL_LOCK:
        _LOGIN_FAILS.clear()
        _LOGIN_FAILS_GLOBAL = []
