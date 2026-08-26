"""
Event log helper. Writes structured events to the `events` table so the
WebGUI can render them without re-parsing log files.

v1.11.40: log_event no longer writes synchronously on the caller's
thread. A single background flusher thread pulls from an in-memory
Queue and batches inserts every ~250ms. The API request hot path
just enqueues — no DB lock contention, no WAL waits, no 5s
busy_timeout dragging request latency during a sync.
"""
from __future__ import annotations

import json
import logging
import queue
import re
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# Bounded queue — overflows drop the oldest event rather than blocking
# a caller. Sized for a worst-case sync that fires ~thousands of
# events; even at 5K/queue we'd flush within a few seconds.
_EVENT_QUEUE: queue.Queue[tuple] = queue.Queue(maxsize=10000)
_FLUSHER_STARTED = False
_FLUSHER_LOCK = threading.Lock()

# v0.51.260: MUST equal db.LOCK_WAIT_S / db.LOCK_RETRY_DELAYS — kept as literals
# because events.py is deliberately stdlib-only (it is the logging substrate; a
# db import would make a db-layer fault take the audit log down with it).
# test_v0_51_260 lints the two against each other, so drift is loud not silent.
_LOCK_WAIT_S = 30.0
_LOCK_RETRY_DELAYS = (0.5, 1.0, 2.0, 4.0)


def _ensure_flusher_running(db_path: Path) -> None:
    """Lazily start the background flusher thread on first log_event."""
    global _FLUSHER_STARTED
    if _FLUSHER_STARTED:
        return
    with _FLUSHER_LOCK:
        if _FLUSHER_STARTED:
            return
        t = threading.Thread(
            target=_flusher_loop, args=(db_path,),
            name="motif-event-flusher", daemon=True,
        )
        t.start()
        _FLUSHER_STARTED = True


def flush_events(timeout: float = 5.0) -> None:
    """v0.51.302 (holistic r2): bounded shutdown drain — the flusher is a
    daemon thread, so without this the final events of a shutdown could die
    in the queue. Waits for the queue to empty plus one batch cycle."""
    import time as _time
    deadline = _time.monotonic() + timeout
    while not _EVENT_QUEUE.empty() and _time.monotonic() < deadline:
        _time.sleep(0.05)
    _time.sleep(0.3)   # one flusher cycle (~250ms) for the in-flight batch


def _write_batch(db_path: Path, insert_sql: str, batch: list[tuple]):
    """Insert one batch, retrying ONLY `database is locked` (CLAUDE.md class 8).
    Returns None on success, else the last error — the caller decides how loudly
    to drop.

    v0.51.260: extracted from _flusher_loop so the retry budget is testable
    without a thread. The flusher is a process-global daemon bound to the FIRST
    db_path it ever sees, so a test that goes through log_event() writes to
    whichever DB some earlier test bound — which is exactly how the first cut of
    test_v0_51_260 passed alone and failed in the full suite."""
    last_err: Exception | None = None
    last_attempt = len(_LOCK_RETRY_DELAYS)
    for attempt in range(last_attempt + 1):
        try:
            # v1.20.16: explicit close. `with sqlite3.connect(...) as conn:`
            # commits/rolls-back but does NOT close the connection — a
            # per-batch connect would lean on refcounting/GC to reclaim the
            # handle. close() makes it deterministic (audit round 3 LOW).
            conn = sqlite3.connect(db_path, timeout=_LOCK_WAIT_S)
            try:
                with conn:
                    # v1.12.84: section_id flows through every event row so the
                    # INFO card's // HISTORY section can scope to the row's own
                    # section. Pre-fix the events table was title-global and
                    # HISTORY rendered every section's events on every card.
                    conn.executemany(insert_sql, batch)
            finally:
                conn.close()
            return None
        except sqlite3.OperationalError as e:
            last_err = e
            if "database is locked" in str(e) and attempt < last_attempt:
                threading.Event().wait(_LOCK_RETRY_DELAYS[attempt])
                continue
            break
        except sqlite3.Error as e:
            last_err = e
            break
    return last_err


def _flusher_loop(db_path: Path) -> None:
    """Drain _EVENT_QUEUE and bulk-insert every ~250ms.

    The flusher owns its own sqlite3 connection for the lifetime of
    the process. Batch inserts in one transaction so the write lock
    is held for one short window per batch instead of N short windows
    per individual event.

    v1.14.54: broad exception catch around the whole loop body. Pre-
    fix only sqlite3.Error was caught — any other exception (OSError
    on full disk during sqlite3.connect, MemoryError under load,
    arbitrary bug in batch construction) killed the thread, and
    _FLUSHER_STARTED=True blocked restart. The visible symptom:
    _EVENT_QUEUE silently filled to 10K then dropped every event
    with only a DEBUG log line; operators lost audit-log visibility
    and never knew. Now the loop body's exceptions are logged via
    log.exception (full traceback) and the loop continues draining.
    """
    while True:
        try:
            first = _EVENT_QUEUE.get()
            batch: list[tuple] = [first]
            try:
                while len(batch) < 200:
                    batch.append(_EVENT_QUEUE.get_nowait())
            except queue.Empty:
                pass
            # v1.15.35: bounded retry on `database is locked`. Pre-
            # fix a single lock-contention spike during a busy sync
            # dropped the whole batch with only a WARNING (audit-log
            # gaps invisible without log review). On persistent lock
            # OR any non-lock sqlite3 error, log at ERROR and drop.
            # Keeps the flusher draining (don't block the queue
            # forever) while making drops loud.
            # v0.51.260: the budget was 3 x 10s + 3s ~= 33s while every
            # transaction() writer gets ~157.5s, so a lock hold in that gap
            # dropped up to 200 events while the DB was demonstrably still
            # usable. MEASURED, not inferred: a probe holding a real
            # BEGIN IMMEDIATE for 40s logged "DROPPING batch" at 32.6s and
            # the event row was gone (0 persisted). The trigger is any long
            # hold — api_set_override_intent's plex_cloud PROMOTE can hold
            # ~150s (30s drift-check + 2 x 60s upload attempts) — but the
            # gap is the flusher's, so it is fixed here rather than in the
            # one caller that surfaced it.
            insert_sql = (
                "INSERT INTO events "
                " (ts, level, component, media_type, tmdb_id, "
                "  section_id, message, detail) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
            )
            last_err = _write_batch(db_path, insert_sql, batch)
            if last_err is not None:
                log.error(
                    "Event flusher: DROPPING batch of %d events after "
                    "%d attempts — last error: %s",
                    len(batch), len(_LOCK_RETRY_DELAYS) + 1, last_err,
                )
        except Exception:
            # v1.14.54: catch-all so a transient non-sqlite exception
            # doesn't kill the thread. log.exception logs the full
            # traceback so operators can diagnose the failure mode.
            log.exception("Event flusher: unexpected error — continuing")
        # Tiny sleep so we batch any back-to-back caller arrivals.
        threading.Event().wait(0.25)


def log_event(
    db_path: Path,
    *,
    level: str,
    component: str,
    message: str,
    media_type: str | None = None,
    tmdb_id: int | None = None,
    section_id: str | None = None,
    detail: dict[str, Any] | str | None = None,
) -> None:
    """Enqueue an event for the background flusher. Never raises —
    event logging must not break callers and must not hold the
    caller's thread for DB I/O.

    v1.12.84: section_id parameter so per-section actions can attribute
    their events. NULL section_id means "title-global" — the api_item
    HISTORY filter treats those as visible from every section's INFO
    card so cross-section context isn't lost. Existing callers that
    don't pass section_id continue to write NULL, matching the
    backfill semantics for pre-migration rows."""
    detail_str: str | None
    if isinstance(detail, dict):
        scrubbed = _scrub(detail)
        detail_str = json.dumps(scrubbed, ensure_ascii=False, default=str)
    elif isinstance(detail, str):
        detail_str = _scrub_text(detail)
    else:
        detail_str = None

    # v1.17.23 (Security MED 3): scrub URL-embedded credentials
    # from the message field too. Pre-fix `log_event(message=
    # f"Failed login for '{username}'")` and dozens of similar
    # f-strings interpolated user-controlled input into the
    # events table with no sanitization. The audit specifically
    # called out the URL-credential leak surface; the message
    # scrub addresses that without rejecting messages that
    # legitimately contain URLs (URLs get the user/pass
    # redaction; everything else stays).
    message = _scrub_text(message)

    py_logger = logging.getLogger(component)
    py_logger.log(
        getattr(logging, level.upper(), logging.INFO),
        "%s%s",
        message,
        f" | {detail_str}" if detail_str else "",
    )

    _ensure_flusher_running(db_path)
    try:
        _EVENT_QUEUE.put_nowait((
            now_iso(), level.upper(), component,
            media_type, tmdb_id, section_id, message, detail_str,
        ))
    except queue.Full:
        # Best-effort: drop on overflow. The python logger above
        # already has the message; only the DB-side render misses it.
        # v1.15.35: log at WARNING (was DEBUG) — audit-log losses
        # under peak load are worth surfacing without requiring
        # `LOG_LEVEL=DEBUG`. Mirrors the v1.15.34 surface-failures-
        # over-silence theme.
        log.warning("event queue full — dropping event: %s", message)


# v1.14.54: expanded substring set + list recursion.
# Pre-fix the set missed `api_key`, `apikey`, `bearer` (CLAUDE.md
# "never commit secrets" includes these); list-of-dict detail
# values silently passed through, so a future caller doing
# `detail={"keys": [{"token": "abc"}]}` would have leaked. Now any
# list element that's a dict is recursively scrubbed.
# v1.14.60 HOTFIX: removed bare `"key"` from the substring set —
# it matched legitimate non-secret fields (`rating_key`,
# `cache_key`, `changed_top_keys`, `theme_key`, `event_key`,
# `monkey_*`) and silently destroyed the audit log's value for
# diagnosing per-rk Plex-cache races (CLAUDE.md class 1 "phantom
# P after PURGE"). Real secret-bearing `*_key` fields like
# `private_key` / `signing_key` / `encryption_key` are caught by
# adding them as their own substrings; `api_key` / `apikey` were
# already covered. The `secret` substring catches `client_secret`
# and similar; `auth` catches `oauth_*`.
_SCRUB_SUBSTRINGS = (
    "token", "secret", "password", "cookie", "auth",
    "api_key", "apikey", "bearer",
    "private_key", "signing_key", "encryption_key", "master_key",
)


# v1.17.23 (Security MED 3): redact URL-embedded credentials
# regardless of dict key. Pre-fix `{"url": "https://user:pass@host"}`
# slipped through because `url` isn't a scrub substring. Same
# regex used by both _scrub (dict-value strings) and _scrub_text
# (message-string redaction).
#
# Match shape: <scheme>://[<user>[:<pass>]]@<host>
# We redact the userinfo segment (everything between `://` and
# the first `@`) — keeps the scheme + host visible so the
# event still names which service was involved.
_URL_CREDENTIALS_RE = re.compile(
    r"(?P<scheme>[a-zA-Z][a-zA-Z0-9+\-.]*://)"
    r"(?P<userinfo>[^/@\s]+)@",
)

# v1.21.17 (security audit LOW): the userinfo regex above only catches the
# `://user:pass@host` shape. Secrets also travel as query params —
# `?X-Plex-Token=<tok>`, `?api_key=<tok>`, `?access_token=<tok>`. No code
# path logs such a URL today (the Plex token rides an HTTP HEADER, never the
# query string), but the scrubber is the last line of defense if a future
# caller ever interpolates one into a message/detail — so redact the value
# while keeping the param name visible for diagnostics.
#
# v0.50.89 (audit MEDIUM): rebuilt from _SCRUB_SUBSTRINGS (the SAME list the
# dict-key scrubber uses) with the sensitive word allowed ANYWHERE inside the
# param name, not anchored as the WHOLE name. Pre-fix the alternation was
# exact-match-only right after `[?&]` — a compound name like `session_token=`,
# `refresh_token=`, or `client_secret=` passed through completely unredacted,
# an asymmetry with the dict-key scrubber (which already does substring
# containment). Sharing the word list also means a future addition to
# _SCRUB_SUBSTRINGS automatically covers this regex too.
_URL_QUERY_SECRET_RE = re.compile(
    r"(?i)([?&][A-Za-z0-9_\-]*(?:"
    + "|".join(re.escape(_s) for _s in _SCRUB_SUBSTRINGS)
    + r")[A-Za-z0-9_\-]*=)[^&\s#\"']+",
)

# v1.24.12 (security audit, defense-in-depth): webhook URLs carry their secret
# in the PATH — `…/webhooks/<id>/<token>` (Discord, apprise-generic) — which
# neither the userinfo nor the query-param regex catches. No code path logs an
# apprise / Discord sink URL today (notify dispatch logs only counts), but if a
# future caller ever interpolates one into a message/detail, redact the token
# segment while keeping the path + id visible for diagnostics.
#
# v0.50.89 (audit LOW): the comment above used to (incorrectly) claim
# "Slack-compatible" coverage too — Slack's real incoming-webhook shape,
# `hooks.slack.com/services/<team>/<bot>/<token>`, has no `/webhooks/`
# segment, so it passed through this pattern completely unredacted. Added a
# dedicated pattern for Slack's actual shape (same philosophy: keep the
# team/bot path segments visible, redact only the trailing token).
_URL_WEBHOOK_PATH_RE = re.compile(
    r"(?i)(/webhooks?/[^/\s]+/)[^/\s?#\"']+",
)
_URL_SLACK_WEBHOOK_RE = re.compile(
    r"(?i)(hooks\.slack\.com/services/[^/\s]+/[^/\s]+/)[^/\s?#\"']+",
)


def _redact_url_credentials(s: str) -> str:
    """Redact userinfo + sensitive query params in URL-ish text:
    `https://user:pass@host` → `https://***@host`, and
    `...?token=abc` → `...?token=***`. Preserves the scheme /
    host / param-name for diagnostic value."""
    s = _URL_CREDENTIALS_RE.sub(
        lambda m: f"{m.group('scheme')}***@", s)
    s = _URL_QUERY_SECRET_RE.sub(lambda m: f"{m.group(1)}***", s)
    s = _URL_WEBHOOK_PATH_RE.sub(lambda m: f"{m.group(1)}***", s)
    s = _URL_SLACK_WEBHOOK_RE.sub(lambda m: f"{m.group(1)}***", s)
    return s


def _scrub_text(s: str) -> str:
    """v1.17.23 (Security MED 3): scrub a free-text message
    string before logging. Pre-fix `log_event(message=
    f"Failed login for '{username}'")` interpolated user
    input straight into the events table; the v1.14.54 _scrub
    only covered dict keys, never the message field. This
    helper applies URL-credential redaction so anything
    looking like `<scheme>://<user>:<pass>@host` in the
    message gets neutralized.

    Length cap (2 KB) defends against pathological-input
    interpolations. Control-char strip lives in the caller
    where appropriate (we don't unconditionally strip here —
    multi-line messages are sometimes intentional)."""
    if not s:
        return s
    s = _redact_url_credentials(s)
    if len(s) > 2048:
        s = s[:2048] + "…"
    return s


# v1.22.41 (audit SECURITY): scrub one value of ANY shape. Pre-fix _scrub's
# list branch only recursed dict ELEMENTS — a string list-element
# (`{"urls": ["https://user:pass@host"]}`) passed through unredacted, and a
# NESTED list (`{"a": [["https://user:pass@host"]]}`) wasn't recursed at all,
# so URL credentials leaked into the events table one level deeper than the
# v1.14.54/v1.17.23 fixes reached. This helper handles every level uniformly:
# dict → key-aware _scrub; list/tuple → recurse each element; str → redact URL
# creds; everything else passes through. tuple is included because json.dumps
# serializes a tuple to a JSON array, so a tuple of strings is a real leak path.
def _scrub_value(v: Any) -> Any:
    if isinstance(v, dict):
        return _scrub(v)
    if isinstance(v, (list, tuple)):
        return [_scrub_value(item) for item in v]
    if isinstance(v, str):
        return _redact_url_credentials(v)
    if isinstance(v, (int, float, bool)) or v is None:
        return v
    # v0.51.302 (holistic r2): a non-JSON-native object (Path, exception,
    # httpx.URL...) used to pass through un-scrubbed and serialize via its
    # repr — a token-bearing URL object bypassed every redaction. Coerce to
    # a redacted string instead.
    return _redact_url_credentials(str(v))


def _scrub(d: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in d.items():
        kl = str(k).lower()
        if any(s in kl for s in _SCRUB_SUBSTRINGS):
            # Suspicious key → redact the whole value regardless of shape.
            out[k] = "***REDACTED***"
        else:
            # v1.22.41: route non-suspicious values through the shape-aware
            # scrubber so URL creds buried in lists / nested lists / list
            # strings get redacted (not just top-level + dict-in-list).
            out[k] = _scrub_value(v)
    return out
