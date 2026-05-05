"""
HTTP API and view layer.

Routes:
- GET  /                       — dashboard
- GET  /movies                 — browse + search
- GET  /tv                     — browse + search
- GET  /coverage               — Plex coverage report
- GET  /queue                  — queue + live logs
- GET  /api/stats              — dashboard JSON
- GET  /api/items              — paginated item list with filters
- GET  /api/items/{type}/{id}  — single item detail
- POST /api/items/{type}/{id}/redownload — enqueue re-download
- POST /api/items/{type}/{id}/override   — manual YouTube URL override
- DELETE /api/items/{type}/{id}/override — clear override
- GET  /api/jobs               — recent jobs
- GET  /api/events             — recent log events
- GET  /api/coverage/plex      — Plex coverage report data
- POST /api/sync/now           — kick off an immediate sync
- GET  /healthz                — liveness probe
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Annotated, Literal, Optional

import json
import sqlite3
from urllib.parse import quote

from fastapi import Depends, FastAPI, Form, HTTPException, Query, Request, status
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.concurrency import run_in_threadpool
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.base import BaseHTTPMiddleware

from ..config import Settings, get_settings
from ..core.auth import (
    ANONYMOUS, PUBLIC_PATHS, PUBLIC_PREFIXES, Principal, SESSION_COOKIE,
    SESSION_TTL_SECONDS, authenticate_password, authenticate_token,
    change_admin_password, create_admin, create_api_token, create_session,
    destroy_session, list_api_tokens, lookup_session, revoke_api_token,
    setup_complete,
)
from ..core.config_file import validate as validate_config
from ..core.db import get_conn, transaction
from ..core.events import log_event, now_iso
from ..core.plex import PlexClient, PlexConfig
from ..core.runtime import is_dry_run, set_dry_run
from ..core.sections import (
    get_managed_section_ids, list_sections, refresh_sections,
    set_section_inclusion,
)
from ..core.sync import extract_video_id

log = logging.getLogger(__name__)

MediaType = Literal["movie", "tv"]


# ---- Helpers for /api/config PATCH ----
# Allow partial updates: caller sends a JSON dict whose structure mirrors
# MotifConfig. Only the leaf keys present get updated. The plex.token field
# has special semantics (empty string = leave alone; null = clear).

_ALLOWED_TOP_LEVEL = {
    "paths", "plex", "downloads", "matching", "sync", "web", "runtime",
}


def _apply_partial_config(cfg, body: dict) -> None:
    """Mutate `cfg` in place from a partial JSON body. Raises ValueError
    on unknown sections or type mismatches."""
    for section_name, section_body in body.items():
        if section_name not in _ALLOWED_TOP_LEVEL:
            raise ValueError(f"unknown config section: {section_name}")
        if section_body is None:
            continue
        if not isinstance(section_body, dict):
            raise ValueError(f"section {section_name!r} must be an object")
        section = getattr(cfg, section_name)
        for k, v in section_body.items():
            if not hasattr(section, k):
                raise ValueError(f"unknown field: {section_name}.{k}")
            # Special: plex.token empty-string-or-mask = keep existing
            if section_name == "plex" and k == "token":
                if v == "" or v == "***":
                    continue
                if v is None:
                    setattr(section, k, "")
                    continue
            # Same for the optional API keys (legacy tvdb_api_key + new tmdb_api_key)
            if section_name == "plex" and k in ("tvdb_api_key", "tmdb_api_key"):
                if v == "" or v == "***":
                    continue
                if v is None:
                    setattr(section, k, "")
                    continue
            current = getattr(section, k)
            if isinstance(current, bool):
                if isinstance(v, str):
                    setattr(section, k, v.strip().lower() in ("1", "true", "yes", "on"))
                else:
                    setattr(section, k, bool(v))
            elif isinstance(current, int) and not isinstance(current, bool):
                setattr(section, k, int(v))
            elif isinstance(current, list):
                if not isinstance(v, list):
                    raise ValueError(f"{section_name}.{k} must be a list, got {type(v).__name__}")
                setattr(section, k, [str(x) for x in v])
            else:
                setattr(section, k, str(v) if v is not None else "")


# -------- Auth middleware --------

class AuthMiddleware(BaseHTTPMiddleware):
    """Resolve a Principal for every request and enforce auth on protected routes.

    Resolution order:
    1. Forward-auth header (when settings.trust_forward_auth) — admin scope
    2. API token (Authorization: Bearer or ?api_key=) — token's own scope
    3. Session cookie — admin scope (only one local user)
    4. Anonymous

    When admin setup hasn't run yet, every non-public request 302s to /setup.
    Otherwise, requests to protected routes without an authenticated Principal
    redirect to /login (for HTML) or return 401 (for JSON/API).
    """

    def __init__(self, app, *, settings: Settings):
        super().__init__(app)
        self.settings = settings

    def _is_public(self, path: str) -> bool:
        if path in PUBLIC_PATHS:
            return True
        return any(path.startswith(p) for p in PUBLIC_PREFIXES)

    def _resolve_principal(self, request: Request) -> Principal:
        # 1. Forward auth
        if self.settings.trust_forward_auth:
            user = (request.headers.get("X-Authentik-Username")
                    or request.headers.get("X-Forwarded-User"))
            if user:
                return Principal(username=user, scope="admin", auth_method="forward_auth")

        # 2. API token
        auth_header = request.headers.get("Authorization", "")
        token: str | None = None
        if auth_header.lower().startswith("bearer "):
            token = auth_header[7:].strip()
        elif "api_key" in request.query_params:
            token = request.query_params.get("api_key")
        if token:
            principal = authenticate_token(self.settings.db_path, token)
            if principal:
                return principal

        # 3. Session cookie
        sid = request.cookies.get(SESSION_COOKIE)
        if sid:
            username = lookup_session(self.settings.db_path, sid)
            if username:
                return Principal(username=username, scope="admin", auth_method="session")

        return ANONYMOUS

    async def dispatch(self, request: Request, call_next):
        path = request.url.path

        # Always resolve the principal first so handlers can read request.state.user
        request.state.principal = self._resolve_principal(request)
        request.state.user = request.state.principal.username

        # First-run gate: if no admin exists, force setup before anything else
        if not setup_complete(self.settings.db_path):
            if path == "/setup" or self._is_public(path):
                return await call_next(request)
            return RedirectResponse("/setup", status_code=302)

        if self._is_public(path):
            return await call_next(request)

        if request.state.principal.is_authenticated:
            return await call_next(request)

        # Not authenticated. JSON paths get 401, HTML paths get a redirect.
        wants_json = (
            path.startswith("/api/")
            or "application/json" in request.headers.get("accept", "")
        )
        if wants_json:
            return JSONResponse({"detail": "authentication required"}, status_code=401)
        # Preserve original URL so we can redirect back after login.
        # URL-encode so query strings with & or = don't bleed into our
        # own /login params (or open us up to redirect-injection via
        # crafted next= values).
        next_url = request.url.path
        if request.url.query:
            next_url += "?" + request.url.query
        return RedirectResponse(f"/login?next={quote(next_url, safe='/')}", status_code=302)


def _require_admin(request: Request) -> Principal:
    p: Principal = request.state.principal
    if not p.is_admin:
        raise HTTPException(status_code=403, detail="admin scope required")
    return p


def _capture_previous_url(
    conn, media_type: str, tmdb_id: int,
    *, section_id: str | None = None,
    on_purge: bool = False,
) -> None:
    """v1.12.37: snapshot the row's current canonical URL so REVERT
    can swap back one step. Called from any handler that's about to
    change the canonical URL (ACCEPT UPDATE, SET URL, UPLOAD MP3,
    REPLACE TDB, PURGE/UNMANAGE pre-drop).

    v1.12.86: writes to per-section previous_urls table (was the
    title-global themes.previous_youtube_url + previous_youtube_kind
    columns). Each section gets its own snapshot — pre-fix a PURGE
    in 4K then PURGE in standard overwrote 4K's slot, breaking
    RESTORE. Without section_id (legacy callers without row context)
    the snapshot lands at section_id='' which acts as a fallback for
    any section that doesn't have its own row.

    Resolution order for "current URL" (which we then snapshot):
      1. user_overrides.youtube_url for the given section_id (or
         '' global) — kind='user'.
      2. themes.youtube_url otherwise — kind='themerrdb'.
    A null URL produces a no-op (no INSERT). The library SQL hides
    REVERT/RESTORE on rows with no previous_urls entry.

    v1.12.102: on_purge=True triggers the 2-deep undo flow used by
    PURGE/UNMANAGE. Pre-fix the v1.12.99 user-kind preservation rule
    blocked these handlers from capturing the URL they were about to
    drop whenever a prior user-kind capture existed — RESTORE then
    brought back an older URL instead of the one just purged. New
    behavior: any existing previous_urls row is moved to the hidden_*
    slot (preserved for the post-RESTORE → REVERT step), then the
    just-dropped URL is written as the new previous_url regardless
    of kind. Non-purge captures (default) keep the old user-kind
    preservation rule and clear hidden_* on update — the hidden
    slot is only meaningful across a PURGE → RESTORE round-trip.
    """
    ovr = None
    if section_id is not None:
        ovr = conn.execute(
            "SELECT youtube_url FROM user_overrides "
            "WHERE media_type = ? AND tmdb_id = ? AND section_id = ?",
            (media_type, tmdb_id, section_id),
        ).fetchone()
        if ovr is None or not ovr["youtube_url"]:
            ovr = conn.execute(
                "SELECT youtube_url FROM user_overrides "
                "WHERE media_type = ? AND tmdb_id = ? AND section_id = ''",
                (media_type, tmdb_id),
            ).fetchone()
    else:
        ovr = conn.execute(
            "SELECT youtube_url FROM user_overrides "
            "WHERE media_type = ? AND tmdb_id = ? AND section_id = ''",
            (media_type, tmdb_id),
        ).fetchone()
        if ovr is None or not ovr["youtube_url"]:
            ovr = conn.execute(
                "SELECT youtube_url FROM user_overrides "
                "WHERE media_type = ? AND tmdb_id = ? "
                "ORDER BY section_id LIMIT 1",
                (media_type, tmdb_id),
            ).fetchone()
    if ovr and ovr["youtube_url"]:
        prev_url = ovr["youtube_url"]
        prev_kind = "user"
    else:
        t = conn.execute(
            "SELECT youtube_url FROM themes "
            "WHERE media_type = ? AND tmdb_id = ?",
            (media_type, tmdb_id),
        ).fetchone()
        prev_url = t["youtube_url"] if t else None
        prev_kind = "themerrdb" if prev_url else None
    if not prev_url or not prev_kind:
        return
    target_section = section_id if section_id is not None else ""
    existing = conn.execute(
        "SELECT youtube_url, kind, captured_at FROM previous_urls "
        "WHERE media_type = ? AND tmdb_id = ? AND section_id = ?",
        (media_type, tmdb_id, target_section),
    ).fetchone()
    if on_purge:
        # v1.12.102: PURGE/UNMANAGE always captures the URL being
        # dropped. The prior previous_url (if any) moves into hidden_*
        # so REVERT after RESTORE walks back to it. The v1.12.99
        # user-kind preservation rule does NOT apply here — losing the
        # just-dropped URL would defeat RESTORE's whole purpose.
        # v1.12.105: if the existing previous_url already matches what
        # we're about to capture (same URL + same kind), the capture
        # is a no-op — the visible chain wouldn't change. Don't move
        # it to hidden either: that would create a degenerate
        # hidden==previous pair that still trips the
        # "hidden IS NOT NULL" gate in the redundancy check, hiding
        # RESTORE's correctly-redundant state. Early-return so the
        # existing row (and any pre-existing hidden slot it carries
        # from an earlier PURGE) survives untouched.
        if (existing
                and existing["youtube_url"] == prev_url
                and existing["kind"] == prev_kind):
            return
        hidden_url = existing["youtube_url"] if existing else None
        hidden_kind = existing["kind"] if existing else None
        hidden_captured_at = existing["captured_at"] if existing else None
        conn.execute(
            """INSERT INTO previous_urls
                 (media_type, tmdb_id, section_id, youtube_url, kind,
                  captured_at, hidden_url, hidden_kind, hidden_captured_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(media_type, tmdb_id, section_id) DO UPDATE SET
                   youtube_url = excluded.youtube_url,
                   kind = excluded.kind,
                   captured_at = excluded.captured_at,
                   hidden_url = excluded.hidden_url,
                   hidden_kind = excluded.hidden_kind,
                   hidden_captured_at = excluded.hidden_captured_at""",
            (media_type, tmdb_id, target_section, prev_url, prev_kind,
             now_iso(), hidden_url, hidden_kind, hidden_captured_at),
        )
        return
    # v1.12.99: don't overwrite a user-kind capture with a themerrdb-kind
    # one. Pre-fix the M*A*S*H scenario lost the user's URL through this
    # cascade:
    #   1. SET URL    → captures (no-op; orphan themes.youtube_url=NULL)
    #   2. ACCEPT URL → captures user override (kind=user) ✓
    #   3. PURGE      → user_overrides gone; capture falls to
    #                   themes.youtube_url (kind=themerrdb), OVERWRITING
    #                   step-2's user capture
    # User then has no RESTORE option (kind=themerrdb is functionally
    # DOWNLOAD TDB; revert_redundant=true). The user-kind URL they
    # explicitly set is gone forever.
    # New rule: themerrdb-kind captures don't clobber existing
    # user-kind captures. User-kind URLs are meaningful for one-step
    # undo; themerrdb URLs are always reachable via DOWNLOAD TDB
    # regardless of whether previous_urls knows about them.
    if prev_kind == "themerrdb" and existing and existing["kind"] == "user":
        return
    # v1.12.102: non-purge captures (SET URL, ACCEPT UPDATE, REPLACE
    # TDB, etc.) clear hidden_*. The hidden slot is meant to survive a
    # single PURGE → RESTORE round-trip; any other URL change
    # invalidates the chain (the user's not undoing the purge anymore).
    conn.execute(
        """INSERT INTO previous_urls
             (media_type, tmdb_id, section_id, youtube_url, kind,
              captured_at, hidden_url, hidden_kind, hidden_captured_at)
           VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, NULL)
           ON CONFLICT(media_type, tmdb_id, section_id) DO UPDATE SET
               youtube_url = excluded.youtube_url,
               kind = excluded.kind,
               captured_at = excluded.captured_at,
               hidden_url = NULL,
               hidden_kind = NULL,
               hidden_captured_at = NULL""",
        (media_type, tmdb_id, target_section, prev_url, prev_kind, now_iso()),
    )


def _load_previous_url(
    conn, media_type: str, tmdb_id: int, *, section_id: str | None = None,
):
    """v1.12.86: read the one-step REVERT/RESTORE snapshot for a row.
    Returns the previous_urls row (youtube_url, kind, captured_at,
    plus the v1.12.102 hidden_* columns) or None when no snapshot
    exists. Resolution: section's own row first, then '' fallback —
    mirrors the user_overrides + worker URL-resolution pattern.
    """
    cols = ("youtube_url, kind, captured_at, "
            "hidden_url, hidden_kind, hidden_captured_at")
    if section_id is not None:
        row = conn.execute(
            f"SELECT {cols} FROM previous_urls "
            "WHERE media_type = ? AND tmdb_id = ? AND section_id = ?",
            (media_type, tmdb_id, section_id),
        ).fetchone()
        if row:
            return row
    row = conn.execute(
        f"SELECT {cols} FROM previous_urls "
        "WHERE media_type = ? AND tmdb_id = ? AND section_id = ''",
        (media_type, tmdb_id),
    ).fetchone()
    return row


def _clear_previous_url(
    conn, media_type: str, tmdb_id: int, *, section_id: str | None = None,
) -> int:
    """v1.12.86: delete the previous_urls snapshot for a row. When
    section_id is provided, drops only that section's row (the ''
    fallback survives so other sections relying on it stay intact).
    Without section_id, drops every row for the title (matches the
    legacy semantics of clearing themes.previous_youtube_url).
    Returns rowcount for callers that want to log the result."""
    if section_id is not None:
        cur = conn.execute(
            "DELETE FROM previous_urls "
            "WHERE media_type = ? AND tmdb_id = ? AND section_id = ?",
            (media_type, tmdb_id, section_id),
        )
    else:
        cur = conn.execute(
            "DELETE FROM previous_urls "
            "WHERE media_type = ? AND tmdb_id = ?",
            (media_type, tmdb_id),
        )
    return cur.rowcount


def _record_audit(
    conn, *, actor: str, action: str,
    media_type: str | None = None,
    tmdb_id: int | None = None,
    section_id: str | None = None,
    details: dict | None = None,
) -> None:
    """v1.12.80: append a row to audit_events — the long-lived
    provenance log for URL changes, override set/clear, accept/
    decline decisions, and destructive theme actions.

    Distinct from log_event (which uses the rolling `events`
    table). audit_events is intentionally not rotated: the goal
    is to be able to answer "who changed Willy Wonka's theme on
    2026-04-12, and what was it before?" months later.

    `details` is a free-form dict serialized to JSON. Each action
    documents its own keys; common ones include old_url, new_url,
    kind, scope.
    """
    payload = json.dumps(details, separators=(",", ":")) if details else None
    conn.execute(
        """INSERT INTO audit_events
             (occurred_at, actor, action, media_type, tmdb_id,
              section_id, details)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (now_iso(), actor, action, media_type, tmdb_id,
         section_id, payload),
    )


def _set_pending_update_decision(
    conn, *, media_type: str, tmdb_id: int,
    section_id: str | None, decision: str, decided_by: str,
) -> int:
    """v1.12.99: record a user's accept/decline on the per-section
    pending_updates row for a (media_type, tmdb_id, section_id) tuple.

    Schema v31 made pending_updates per-section. The user's decision
    lives at section_id-specific rows so KEEP CURRENT on standard
    doesn't bleed to 4K. The title-global '' row written by sync
    survives as the "default" any section without its own decision
    inherits — the library SQL's pu_sec / pu_global LEFT JOIN with
    COALESCE picks per-section first, '' second.

    Implementation:
      1. Read the effective pending update for the requested section
         (per-section first, '' fallback) — this provides the values
         to copy (new_youtube_url, kind, etc.).
      2. UPSERT into the section-specific row with the new decision.
         The '' row stays untouched so other sections still see
         their inherited pending state.

    Returns 1 when a row was written, 0 when no pending_update
    existed for the (mt, tmdb) tuple to act on.
    """
    section = section_id if section_id is not None else ""
    src = conn.execute(
        """SELECT new_video_id, new_youtube_url, old_youtube_url,
                  upstream_edited_at, detected_at, kind
             FROM pending_updates
            WHERE media_type = ? AND tmdb_id = ?
              AND section_id IN (?, '')
            ORDER BY (section_id = ?) DESC
            LIMIT 1""",
        (media_type, tmdb_id, section, section),
    ).fetchone()
    if src is None:
        return 0
    conn.execute(
        """INSERT INTO pending_updates (
              media_type, tmdb_id, section_id,
              new_video_id, new_youtube_url, old_youtube_url,
              upstream_edited_at, detected_at, decision,
              decision_at, decision_by, kind
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(media_type, tmdb_id, section_id) DO UPDATE SET
               decision = excluded.decision,
               decision_at = excluded.decision_at,
               decision_by = excluded.decision_by""",
        (media_type, tmdb_id, section,
         src["new_video_id"], src["new_youtube_url"], src["old_youtube_url"],
         src["upstream_edited_at"], src["detected_at"], decision,
         now_iso(), decided_by, src["kind"]),
    )
    return 1


def _drop_motif_tracking(conn, media_type: str, tmdb_id: int) -> None:
    """v1.12.57: drop motif's tracking metadata for a title — the
    rows that aren't tied to actual files on disk but persist
    motif's "what should we do with this title" decisions:
      - user_overrides (manual URL override)
      - pending_updates (any decision: pending / accepted / declined)

    Called from PURGE, UNMANAGE, and DEL so those actions truly
    leave a clean slate. Pre-fix the rows survived even after the
    user destroyed every on-disk file: the next download or sync
    re-applied the stale override URL or re-surfaced the stale
    pending update, undoing the user's intent.

    Safe to call BEFORE the themes row is dropped — the FK
    CASCADE handles pending_updates, but user_overrides has no FK
    and would otherwise survive.

    v1.12.64: themes.previous_youtube_url + previous_youtube_kind
    are NO LONGER cleared here. Per user feedback, the previous-URL
    snapshot should survive PURGE/UNMANAGE so the user retains a
    one-step RESTORE path (REVERT button stays surfaced when
    revert_redundant is false). The user can still wipe the
    snapshot explicitly via CLEAR URL before destruction if they
    want a true clean slate. DEL drops the themes row anyway, so
    previous_* go with it via row deletion.
    """
    conn.execute(
        "DELETE FROM user_overrides WHERE media_type = ? AND tmdb_id = ?",
        (media_type, tmdb_id),
    )
    conn.execute(
        "DELETE FROM pending_updates WHERE media_type = ? AND tmdb_id = ?",
        (media_type, tmdb_id),
    )


# v1.12.95: canonical SQL expression for the row's SRC letter. Mirrors
# JS computeSrcLetter exactly — same priority order, same fallthrough
# logic — so the pill filter, the SRC sort, and the row badge all
# agree on what SRC class a row belongs to. Pre-fix the pill SQL and
# sort SQL each had their own simpler heuristic (provenance-first)
# and could disagree with the JS render on rows where source_kind
# disagreed with placement.provenance.
#
# Ordering matches JS:
#   1. placed + source_kind=themerrdb → T
#   2. placed + source_kind=adopt     → A
#   3. placed + source_kind=url|upload → U
#   4. placed + provenance=auto        → T (legacy fallback for rows
#                                          without source_kind)
#   5. placed + provenance=manual:
#        non-orphan OR svid is empty OR svid length=11 → U
#                                          else        → A
#   6. !placed + plex_local_theme → M
#   7. !placed + has_theme        → P
#   8. else                       → '-'
#
# The length(svid)=11 check approximates the JS regex
# /^[A-Za-z0-9_-]{11}$/ — adopt's hash-based svids are not 11 chars,
# YouTube video IDs are exactly 11 chars. Empty svid handled
# separately via COALESCE.
_SRC_LETTER_SQL = (
    "CASE "
    "WHEN p.media_folder IS NOT NULL AND lf.source_kind = 'themerrdb' THEN 'T' "
    "WHEN p.media_folder IS NOT NULL AND lf.source_kind = 'adopt' THEN 'A' "
    "WHEN p.media_folder IS NOT NULL AND lf.source_kind IN ('url','upload') THEN 'U' "
    "WHEN p.media_folder IS NOT NULL AND p.provenance = 'auto' THEN 'T' "
    "WHEN p.media_folder IS NOT NULL AND p.provenance = 'manual' AND ("
        "t.upstream_source != 'plex_orphan' "
        "OR COALESCE(lf.source_video_id, '') = '' "
        "OR length(lf.source_video_id) = 11"
    ") THEN 'U' "
    "WHEN p.media_folder IS NOT NULL AND p.provenance = 'manual' THEN 'A' "
    "WHEN p.media_folder IS NULL AND pi.local_theme_file = 1 THEN 'M' "
    "WHEN p.media_folder IS NULL AND pi.has_theme = 1 THEN 'P' "
    "ELSE '-' END"
)

# v1.10.15: column-sort whitelist. Each entry maps the ?sort= value
# accepted by /api/library to a SQL ORDER BY snippet for the main row
# query. Wrapping in a whitelist keeps the param SQL-injection-safe.
# 'src' uses _SRC_LETTER_SQL with a numeric remap so sort-by-source
# groups rows visually the same way the SRC column does (T → U → A →
# M → P → —).
_LIBRARY_SORTS_MAIN = {
    "title": "pi.title COLLATE NOCASE",
    "year":  "pi.year",
    "imdb":  "pi.guid_imdb",
    "src": (
        f"CASE ({_SRC_LETTER_SQL}) "
        "WHEN 'T' THEN 1 "
        "WHEN 'U' THEN 2 "
        "WHEN 'A' THEN 3 "
        "WHEN 'M' THEN 4 "
        "WHEN 'P' THEN 5 "
        "ELSE 6 END"
    ),
    "dl":    "CASE WHEN lf.file_path IS NOT NULL THEN 0 ELSE 1 END",
    "pl":    "CASE WHEN p.media_folder IS NOT NULL THEN 0 ELSE 1 END",
    "link":  "CASE p.placement_kind WHEN 'hardlink' THEN 0 WHEN 'copy' THEN 1 ELSE 2 END",
    # v1.12.68: 'attention' sort orders rows by what needs the
    # user's attention most. Lower priority numbers sort first
    # (asc) so the rows demanding action float to the top of the
    # list — failures, then pending updates, then post-DEL await
    # placements, then mismatches, then the rest. Pairs with the
    # frontend's // NEEDS WORK sort button. Title is the
    # secondary key (added in the SQL builder via _LIBRARY_SORT_TIE)
    # so equal-priority rows stay alphabetized.
    "attention": (
        "CASE "
        # Active failure (red TDB ✗ pill, not yet acked)
        "WHEN t.failure_kind IS NOT NULL AND t.failure_acked_at IS NULL THEN 1 "
        # Pending upstream update (blue TDB ↑ pill, not yet decided)
        "WHEN EXISTS (SELECT 1 FROM pending_updates pu "
        "  WHERE pu.media_type = t.media_type AND pu.tmdb_id = t.tmdb_id "
        "    AND pu.decision = 'pending') THEN 2 "
        # Awaiting placement (canonical exists, no placement)
        "WHEN lf.file_path IS NOT NULL AND p.media_folder IS NULL THEN 3 "
        # Mismatch (canonical and placement file diverged)
        "WHEN lf.mismatch_state = 'pending' THEN 4 "
        # Acked failure or KEEP-CURRENTed update — a passive
        # "still broken" cue, lower urgency than the unhandled
        # cases above but higher than ordinary rows.
        "WHEN t.failure_kind IS NOT NULL THEN 5 "
        "WHEN EXISTS (SELECT 1 FROM pending_updates pu "
        "  WHERE pu.media_type = t.media_type AND pu.tmdb_id = t.tmdb_id "
        "    AND pu.decision = 'declined') THEN 6 "
        "ELSE 7 END"
    ),
}
_LIBRARY_SORTS_NOT_IN_PLEX = {
    "title": "t.title COLLATE NOCASE",
    "year":  "t.year",
    "imdb":  "t.imdb_id",
}


def _library_main_query(
    db: Path, *, tab: str, fourk: bool, q: str, status: str,
    page: int, per_page: int,
    sort: str = "title", sort_dir: str = "asc",
    tdb: str = "any",
    src_pills: set[str] | None = None,
    tdb_pills: set[str] | None = None,
    dl_pills: set[str] | None = None,
    pl_pills: set[str] | None = None,
    link_pills: set[str] | None = None,
    # v1.12.41: EDITION pill axis. Filters rows by whether the
    # Plex folder name carries an `{edition-...}` tag. 'has'
    # matches rows with an edition tag (Director's Cut,
    # Extended, IMAX, etc.); 'none' matches rows without one.
    # Lets the user scope themes to a specific edition family
    # without filtering by SRC/DL/PL.
    ed_pills: set[str] | None = None,
    cookies_present: bool = False,
    themes_dir: Path | None = None,
) -> dict:
    """Sync helper for /api/library — runs all SQL in one threadpool call
    so the FastAPI event loop isn't blocked. v1.10.2 perf pass:
      - consolidated 3 metadata queries (plex_enumerated, last_plex_enum_at,
        last_sync_at) into one round trip.
      - skip the heavy LEFT JOIN themes/lf/p in the COUNT when status='all'
        and q is empty/title-only — the tab predicate alone is sufficient
        to count rows there.
      - heavy joins still happen for the rows query (we need theme/lf/p
        columns to render rows) and for status filters that reference
        them in WHERE.
    """
    if tab == "movies":
        tab_where = "pi.media_type = 'movie' AND ps.is_anime = 0"
    elif tab == "tv":
        tab_where = "pi.media_type = 'show' AND ps.is_anime = 0"
    else:  # anime — both movie- and show-typed anime sections allowed
        tab_where = "ps.is_anime = 1"
    tab_where += " AND ps.is_4k = 1" if fourk else " AND ps.is_4k = 0"

    params: list = []
    where_extra = ""
    where_pi_only = ""
    if q:
        clause = " AND (pi.title LIKE ? OR pi.guid_imdb = ?)"
        where_extra += clause
        where_pi_only += clause
        params.extend([f"%{q}%", q])
    if status == "themed":
        where_extra += (" AND t.tmdb_id IS NOT NULL "
                        "AND t.upstream_source != 'plex_orphan'")
    elif status == "manual":
        where_extra += (
            " AND ("
            "  (lf.provenance = 'manual')"
            "  OR (pi.local_theme_file = 1 "
            "      AND lf.file_path IS NULL AND p.media_folder IS NULL)"
            ")"
        )
    elif status == "plex_agent":
        where_extra += (" AND pi.has_theme = 1 "
                        "AND pi.local_theme_file = 0 "
                        "AND lf.file_path IS NULL "
                        "AND p.media_folder IS NULL")
    elif status == "untracked":
        # v1.12.4: 'UNTHEMED' (renamed from UNTRACKED in v1.12.0) — no
        # theme content anywhere on this row. The TDB-coverage axis is
        # handled separately via the TDB MATCH chips so the user can
        # slice UNTHEMED into "TDB-tracked + no theme yet" (the
        # actionable list — motif can fetch from upstream) vs
        # "no TDB and no theme" (truly orphan). Pre-fix the t.* clause
        # was baked in here, which made TDB MATCH redundant on this
        # tab and the chip row was hidden.
        where_extra += (" AND pi.has_theme = 0 "
                        "AND pi.local_theme_file = 0 "
                        "AND lf.file_path IS NULL "
                        "AND p.media_folder IS NULL")
    elif status == "has_theme":
        # v1.10.54: any theme source — T (ThemerrDB-tracked + maybe
        # downloaded), U/A (motif-managed manual), M (sidecar), P
        # (Plex agent), or a downloaded canonical waiting for
        # placement. Inverse of UNTRACKED.
        where_extra += (" AND ("
                        "  (t.tmdb_id IS NOT NULL AND t.upstream_source != 'plex_orphan')"
                        "  OR pi.local_theme_file = 1"
                        "  OR pi.has_theme = 1"
                        "  OR p.media_folder IS NOT NULL"
                        "  OR lf.file_path IS NOT NULL"
                        ")")
    elif status == "downloaded":
        # motif has the theme file on disk (placed or not). Mirrors the
        # DL pill being lit in the row.
        where_extra += " AND lf.file_path IS NOT NULL"
    elif status == "placed":
        where_extra += " AND p.media_folder IS NOT NULL"
    elif status == "unplaced":
        where_extra += " AND lf.file_path IS NOT NULL AND p.media_folder IS NULL"
    elif status == "dl_missing":
        # v1.11.62: rows where motif HAS a local_files entry pointing
        # at a canonical that no longer exists on disk. The placement
        # in the Plex folder (hardlink / copy) survives, so the row
        # reads as "still in Plex, not downloaded". Filter is applied
        # post-SQL via the canonical_missing stat in _annotate_canonical_state
        # — SQL just narrows to candidate rows (must have lf.file_path
        # AND p.media_folder, otherwise canonical_missing is irrelevant).
        where_extra += (" AND lf.file_path IS NOT NULL "
                        "AND p.media_folder IS NOT NULL")
    elif status == "failures":
        # v1.10.50: only un-acknowledged failures land in this filter.
        # Acked rows keep their red TDB pill but don't show up here —
        # they're 'I know, dealt with it' state.
        where_extra += " AND t.failure_kind IS NOT NULL AND t.failure_acked_at IS NULL"
    elif status == "updates":
        # v1.11.74: items where ThemerrDB upstream changed the YouTube
        # URL since motif's last download. The pending_updates table
        # carries the diff with decision='pending'; ACCEPT replaces
        # motif's copy with a fresh download from the new URL, KEEP
        # leaves the current one alone (decision='declined'). Topbar
        # 'N UPD' badge counts these globally; this filter narrows
        # the library page to just the rows that are eligible.
        where_extra += (" AND EXISTS ("
                        "  SELECT 1 FROM pending_updates pu"
                        "   WHERE pu.media_type = t.media_type"
                        "     AND pu.tmdb_id = t.tmdb_id"
                        "     AND pu.decision = 'pending'"
                        ")")

    # v1.10.20: secondary 'TDB MATCH' filter, stacks on top of `status`.
    # 'tracked' means ThemerrDB has the title (the row's joined themes
    # record is a real upstream entry, not a plex_orphan). 'untracked'
    # is the inverse — Plex has the item but ThemerrDB doesn't, OR the
    # joined themes row is an orphan motif made up itself.
    # Useful for slicing MANUAL into 'I can REPLACE w/ TDB' vs 'no TDB
    # alternative exists'. No-op for status=themed (already tracked) /
    # status=untracked (already untracked) — frontend hides the
    # secondary toggle in those cases.
    if tdb == "tracked":
        where_extra += (" AND t.tmdb_id IS NOT NULL "
                        "AND t.upstream_source != 'plex_orphan'")
    elif tdb == "untracked":
        where_extra += " AND (t.tmdb_id IS NULL OR t.upstream_source = 'plex_orphan')"

    # v1.12.23: server-side pill filters. Pre-fix SRC and TDB pill
    # filters ran client-side after the server returned a page —
    # which made counts and pagination lie ("THEMED + M = 3,336"
    # whether or not M was actually selected, because the count
    # was the THEMED total). Now each pill axis composes with
    # `status` and `tdb` server-side, so total/pagination/sort all
    # honor the pill state.
    #
    # Each pill axis is multi-select. Selections within an axis OR
    # together; axes AND together. Empty set = no filter on that
    # axis. Mirrors the client-side computeSrcLetter / computeTdbPill
    # taxonomy exactly so the same row classification drives both
    # the row pill render and the filter set.
    src_pills = src_pills or set()
    tdb_pills = tdb_pills or set()
    dl_pills = dl_pills or set()
    pl_pills = pl_pills or set()
    link_pills = link_pills or set()
    ed_pills = ed_pills or set()
    if src_pills:
        # v1.12.95: route through _SRC_LETTER_SQL — single source of
        # truth for the pill, the SRC sort, and the row badge. Pre-fix
        # each pill had its own provenance-first predicate that
        # diverged from JS computeSrcLetter on rows where source_kind
        # disagreed with placement.provenance.
        valid_letters = {"T", "U", "A", "M", "P", "-"}
        wanted = sorted(p for p in src_pills if p in valid_letters)
        if wanted:
            placeholders = ",".join("?" for _ in wanted)
            where_extra += f" AND ({_SRC_LETTER_SQL}) IN ({placeholders})"
            params.extend(wanted)
    if tdb_pills:
        DEAD_KINDS = "('video_private','video_removed','video_age_restricted','geo_blocked')"
        # Subquery snippet reused below — "this row has an actionable
        # pending update". Mirrors the v1.12.5 row-pill priority:
        # update wins over the green-tdb pill in the row render, so
        # the filter excludes update rows from the green-tdb match
        # set (and includes them in the blue-update match set, gated
        # on src != '-' per JS computeTdbPill).
        PENDING_EXISTS = (
            "EXISTS (SELECT 1 FROM pending_updates pu "
            "  WHERE pu.media_type = t.media_type "
            "    AND pu.tmdb_id = t.tmdb_id "
            "    AND pu.decision = 'pending')"
        )
        # JS computeSrcLetter returns '-' when nothing's themed
        # anywhere on the row — no placement, no sidecar, no Plex
        # agent, no canonical. The blue-update pill is hidden on
        # those rows because there's nothing to "update from"; the
        # `update` filter mirrors the gate.
        SRC_NOT_DASH = (
            "(p.media_folder IS NOT NULL"
            " OR pi.local_theme_file = 1"
            " OR pi.has_theme = 1"
            " OR lf.file_path IS NOT NULL)"
        )
        branches = []
        for p in tdb_pills:
            if p == "tdb":
                # v1.12.95: green TDB pill render is the FALLTHROUGH
                # in JS computeTdbPill — wins only when none of
                # update / dead-failure / cookies-blocked match.
                # Pre-fix the SQL was `tracked AND (no failure OR
                # cookies_expired-with-cookies)`, which silently
                # included rows whose row-pill showed blue update
                # or red dead. Strict alignment now: also exclude
                # pending_update + dead-failure (and
                # cookies_expired when no cookies present).
                if cookies_present:
                    branches.append(
                        "(t.upstream_source IN ('imdb','themoviedb') "
                        "AND (t.failure_kind IS NULL OR t.failure_kind = 'cookies_expired') "
                        f"AND NOT {PENDING_EXISTS})"
                    )
                else:
                    branches.append(
                        "(t.upstream_source IN ('imdb','themoviedb') "
                        "AND t.failure_kind IS NULL "
                        f"AND NOT {PENDING_EXISTS})"
                    )
            elif p == "update":
                # v1.12.48: scoped to decision='pending' (was IN
                # ('pending','declined')) so the filter matches the
                # topbar UPD count exactly. Clicking the UPD badge
                # now lands the user on a list with the same
                # cardinality the badge advertised. KEEP CURRENT
                # decisions still light up the row's blue pill but
                # are excluded from this filter — the user already
                # said they don't want to act on them.
                # v1.12.95: also gate on src != '-' to mirror JS
                # computeTdbPill. Pre-fix unthemed rows with stale
                # pending_updates lit up the update filter even
                # though their row-pill rendered green/none.
                branches.append(f"({PENDING_EXISTS} AND {SRC_NOT_DASH})")
            elif p == "cookies":
                if not cookies_present:
                    branches.append("(t.failure_kind = 'cookies_expired')")
            elif p == "dead":
                branches.append(f"(t.failure_kind IN {DEAD_KINDS})")
            elif p == "none":
                branches.append("(t.tmdb_id IS NULL OR t.upstream_source = 'plex_orphan')")
        if branches:
            where_extra += " AND (" + " OR ".join(branches) + ")"
    # v1.12.81: when 'broken' is in dl_pills or pl_pills the filter
    # needs a post-SQL stat-check (canonical_missing / placement_missing
    # don't live in any column). Skip the SQL branch in that case and
    # let the post-filter step downstream apply the full dl_pills /
    # pl_pills selection. Without 'broken' the SQL branches stay so
    # the page query narrows efficiently.
    # v1.12.95: 'on' selections also need post-stat filtering because
    # `lf.file_path IS NOT NULL` (DL=on SQL) matches both intact and
    # missing-on-disk rows — same for `p.media_folder IS NOT NULL`
    # (PL=on). Without the post-stat narrowing, filtering by ON
    # silently includes rows whose row-pill renders BROKEN (red).
    # When `on` is selected we route the entire dl_pills / pl_pills
    # filter through the post-stat path (same trigger as `broken`)
    # so `_row_matches_dl` / `_row_matches_pl` apply the strict
    # canonical_missing / placement_missing exclusion.
    dl_needs_post_stat = ("on" in dl_pills) or ("broken" in dl_pills)
    pl_needs_post_stat = ("on" in pl_pills) or ("broken" in pl_pills)
    if dl_pills and not dl_needs_post_stat:
        branches = []
        for p in dl_pills:
            if p == "off":
                branches.append("(lf.file_path IS NULL)")
            # 'mismatch' was retired in v1.12.81 — content divergence
            # is a LINK fact (link_pills 'm') and a row-title `!`
            # glyph; DL no longer doubles up.
        if branches:
            where_extra += " AND (" + " OR ".join(branches) + ")"
    if pl_pills and not pl_needs_post_stat:
        branches = []
        for p in pl_pills:
            if p == "off":
                branches.append("(p.media_folder IS NULL)")
            # v1.12.70: 'await' — canonical exists but no placement
            # row (typically post-DEL: the user removed the file from
            # the Plex folder but motif's canonical still lives in
            # /themes). Mirrors the JS computeSrcLetter awaitingApproval
            # gate; lets users find every row that needs a PLACE →
            # PUSH TO PLEX (or REMOVE → PURGE) without scrolling.
            elif p == "await":
                branches.append(
                    "(p.media_folder IS NULL AND lf.file_path IS NOT NULL)"
                )
        if branches:
            where_extra += " AND (" + " OR ".join(branches) + ")"
    if link_pills:
        branches = []
        for p in link_pills:
            if p == "hl":
                branches.append("(p.placement_kind = 'hardlink' AND lf.mismatch_state IS NULL)")
            elif p == "c":
                branches.append("(p.placement_kind = 'copy' AND lf.mismatch_state IS NULL)")
            elif p == "m":
                # v1.12.95: gate on `placed` so the filter mirrors JS
                # `isMismatch && placed` — the M badge only paints
                # when there's a placement to be in mismatch with.
                # Pre-fix the SQL matched any mismatch_state row,
                # which could include orphan-placement edge cases.
                branches.append("(lf.mismatch_state IS NOT NULL AND p.media_folder IS NOT NULL)")
            elif p == "none":
                branches.append("(p.media_folder IS NULL)")
        if branches:
            where_extra += " AND (" + " OR ".join(branches) + ")"
    # v1.12.41: EDITION axis — match against the Plex folder
    # path's `{edition-...}` tag. SQL LIKE pattern is escape-safe
    # because the Plex folder convention is fixed (curly-brace
    # tags); no user-supplied substring lands in the pattern.
    if ed_pills:
        branches = []
        for p in ed_pills:
            if p == "has":
                branches.append("(pi.folder_path LIKE '%{edition-%')")
            elif p == "none":
                branches.append("(pi.folder_path IS NULL OR pi.folder_path NOT LIKE '%{edition-%')")
        if branches:
            where_extra += " AND (" + " OR ".join(branches) + ")"

    sql_select = """
        SELECT pi.rating_key, pi.section_id, pi.media_type AS plex_media_type,
               pi.title AS plex_title, pi.year, pi.guid_imdb, pi.guid_tmdb,
               pi.folder_path, pi.has_theme AS plex_has_theme,
               pi.local_theme_file AS plex_local_theme,
               ps.title AS section_title,
               t.tmdb_id AS theme_tmdb, t.media_type AS theme_media_type,
               t.title AS theme_title, t.youtube_url, t.youtube_video_id,
               -- v1.12.107: row's currently-applied URL — matches the
               -- worker's two-step fetch (per-section override → ''
               -- override → themes.youtube_url). Surfaces so the SET
               -- URL dialog can warn when the user is about to set
               -- the same URL that's already applied (no-op overlap
               -- with the existing v1.12.54 TDB-match warning, which
               -- only catches the U→T conversion case).
               COALESCE(
                 (SELECT youtube_url FROM user_overrides uo
                   WHERE uo.media_type = t.media_type
                     AND uo.tmdb_id = t.tmdb_id
                     AND uo.section_id = pi.section_id),
                 (SELECT youtube_url FROM user_overrides uo
                   WHERE uo.media_type = t.media_type
                     AND uo.tmdb_id = t.tmdb_id
                     AND uo.section_id = ''),
                 t.youtube_url
               ) AS applied_youtube_url,
               t.failure_kind, t.failure_message, t.failure_acked_at, t.upstream_source,
               lf.file_path, lf.source_video_id, lf.provenance, lf.source_kind,
               -- v1.11.99: mismatch state drives the DL=amber + LINK=≠
               -- visual on the row and the PUSH TO PLEX / ADOPT FROM
               -- PLEX / KEEP MISMATCH branches in the PLACE menu.
               lf.mismatch_state,
               p.media_folder, p.placement_kind, p.provenance AS placement_provenance,
               (SELECT j.job_type FROM jobs j
                WHERE j.media_type = t.media_type AND j.tmdb_id = t.tmdb_id
                  AND j.section_id = pi.section_id
                  AND j.job_type IN ('download', 'place')
                  AND j.status IN ('pending', 'running')
                ORDER BY CASE j.status WHEN 'running' THEN 0 ELSE 1 END,
                         j.id DESC
                LIMIT 1) AS job_in_flight,
               -- v1.11.74 / v1.12.5: pending_update drives the blue
               -- TDB ↑ pill on the row. v1.12.5 split this from
               -- 'actionable_update' below so KEEP CURRENT can
               -- decrement the topbar counter without removing the
               -- pill itself — declined updates stay visible for
               -- filtering and sorting but no longer prompt for
               -- action.
               -- v1.12.99: pending_update / actionable_update /
               -- pending_update_kind resolve via the per-section
               -- LEFT JOIN with '' fallback. COALESCE picks the
               -- section's own decision when present; otherwise
               -- inherits from the title-global '' row that sync
               -- writes. Pre-v1.12.99 the EXISTS subqueries hit
               -- pending_updates without section_id filter, so a
               -- KEEP CURRENT on standard cleared the blue pill on
               -- 4K and the topbar UPD count under-reported.
               (CASE WHEN COALESCE(pu_sec.decision, pu_global.decision)
                          IN ('pending', 'declined')
                     THEN 1 ELSE 0 END) AS pending_update,
               COALESCE(pu_sec.kind, pu_global.kind) AS pending_update_kind,
               (CASE WHEN COALESCE(pu_sec.decision, pu_global.decision) = 'pending'
                     THEN 1 ELSE 0 END) AS actionable_update,
               -- v1.12.86: previous-URL fields resolve from the
               -- previous_urls JOINs (pv_sec + pv_global). Each
               -- column's value comes from the section-specific row
               -- when present, falling back to the '' global row.
               -- Pre-v1.12.86 these came from title-global columns
               -- on the themes table; per-section keying eliminates
               -- cross-section bleed (PURGE in section A no longer
               -- overwrites the snapshot for section B).
               (CASE WHEN COALESCE(pv_sec.youtube_url, pv_global.youtube_url) IS NOT NULL
                     THEN 1 ELSE 0 END) AS has_previous_url,
               COALESCE(pv_sec.kind, pv_global.kind) AS previous_youtube_kind,
               -- v1.12.40: revert_redundant = the previous URL is
               -- a TDB URL AND it exactly matches the
               -- pending_updates.new_youtube_url that ACCEPT
               -- UPDATE would fetch. Reverting and accepting
               -- would download the same file, so REVERT is
               -- suppressed in the SOURCE menu to avoid two
               -- buttons that fire identical actions. Only
               -- triggers for previous_kind='themerrdb' — a
               -- user-kind previous URL stays revertible since
               -- it represents a meaningfully different state
               -- (the row would flip back to U).
               -- v1.12.53: also redundant when the previous URL
               -- equals the row's CURRENT canonical URL (the
               -- override URL when present, else themes.youtube_url).
               -- Covers the post-accept URL-match case: SET URL
               -- captured previous=TDB-URL, then ACCEPT UPDATE
               -- ran with url_match=True (override URL == TDB URL),
               -- decision flipped to 'accepted' so the original
               -- pending-updates clause no longer matches —
               -- but REVERT would just re-create the override
               -- pointing back at the current TDB URL, a no-op
               -- with extra UI churn. Suppress.
               -- v1.12.101: redundant only when there's a CURRENT
               -- canonical (lf.file_path IS NOT NULL). After PURGE
               -- or UNMANAGE the row has no canonical file but
               -- themes.youtube_url survives — the URL-equality
               -- check above would otherwise fire for the
               -- common case "previous_url captured at PURGE
               -- == themes.youtube_url" and hide RESTORE even
               -- though the row genuinely has no theme to
               -- play. Gating the redundancy check on
               -- "canonical exists" keeps RESTORE available
               -- whenever there's something to actually restore.
               -- v1.12.103: post-PURGE clean-overlap case —
               -- when the row has no canonical AND the captured
               -- URL is themerrdb-kind AND equals themes.youtube_url
               -- AND there's no hidden chain to preserve, RESTORE
               -- and DOWNLOAD TDB are functionally identical:
               -- both fetch the same URL into the canonical, no
               -- prior state to walk back to. Hide RESTORE in
               -- that exact case so the SOURCE menu doesn't
               -- carry two buttons with the same effect. The
               -- hidden_url IS NULL gate is load-bearing: a
               -- captured chain (PURGE on a row with a prior
               -- user URL) makes RESTORE → REVERT walk back to
               -- a URL DOWNLOAD TDB can't reach, so RESTORE is
               -- the unique way to start that walk. User-kind
               -- captures stay visible regardless (DOWNLOAD TDB
               -- can't reach a user URL). And if TDB has rolled
               -- the URL since the PURGE (themes.youtube_url
               -- changed), the URLs don't match and RESTORE
               -- locks to the captured URL — meaningfully
               -- different from DOWNLOAD TDB.
               (CASE WHEN COALESCE(pv_sec.youtube_url, pv_global.youtube_url) IS NOT NULL
                      AND (
                        -- v1.12.101 branch: row HAS a canonical
                        -- (REVERT competes with the existing-state
                        -- actions — pending update accept, no-op
                        -- swap to the same URL).
                        -- v1.12.105: also require hidden_url IS NULL.
                        -- Pre-fix the PURGE → DOWNLOAD TDB sequence
                        -- left a hidden user URL in the chain but
                        -- this branch fired (previous == current
                        -- post-download), hiding RESTORE — making
                        -- the hidden chain unreachable. The hidden
                        -- slot is the only path back to a URL that
                        -- DOWNLOAD TDB doesn't surface, so RESTORE
                        -- must stay visible whenever it's set.
                        (lf.file_path IS NOT NULL
                         AND COALESCE(pv_sec.hidden_url, pv_global.hidden_url) IS NULL
                         AND (
                          (COALESCE(pv_sec.kind, pv_global.kind) = 'themerrdb'
                           AND EXISTS (
                             SELECT 1 FROM pending_updates pu
                              WHERE pu.media_type = t.media_type
                                AND pu.tmdb_id = t.tmdb_id
                                AND pu.decision IN ('pending', 'declined')
                                AND pu.new_youtube_url = COALESCE(pv_sec.youtube_url, pv_global.youtube_url)
                           ))
                          OR
                          COALESCE(pv_sec.youtube_url, pv_global.youtube_url) = COALESCE(
                            -- v1.12.72: prefer section-specific
                            -- override; fall back to global ('') row;
                            -- final fallback to themes.youtube_url.
                            -- Mirrors the worker's two-step fetch so
                            -- the row's "currently applied" URL is
                            -- the same value the worker would use.
                            (SELECT youtube_url FROM user_overrides uo
                              WHERE uo.media_type = t.media_type
                                AND uo.tmdb_id = t.tmdb_id
                                AND uo.section_id = pi.section_id),
                            (SELECT youtube_url FROM user_overrides uo
                              WHERE uo.media_type = t.media_type
                                AND uo.tmdb_id = t.tmdb_id
                                AND uo.section_id = ''),
                            t.youtube_url
                          )
                        ))
                        OR
                        -- v1.12.103 branch: post-PURGE/UNMANAGE
                        -- clean overlap. RESTORE and DOWNLOAD TDB
                        -- would fetch the same URL with no chain
                        -- to preserve, so RESTORE adds nothing.
                        -- All four conditions matter: no canonical
                        -- to revert against, kind=themerrdb (user
                        -- URLs aren't reachable via DOWNLOAD TDB),
                        -- no hidden chain (RESTORE would otherwise
                        -- be the only way to start the chain walk),
                        -- and TDB hasn't rolled the URL since the
                        -- capture (else RESTORE locks to the older
                        -- URL — meaningfully different).
                        (lf.file_path IS NULL
                         AND COALESCE(pv_sec.kind, pv_global.kind) = 'themerrdb'
                         AND COALESCE(pv_sec.hidden_url, pv_global.hidden_url) IS NULL
                         AND COALESCE(pv_sec.youtube_url, pv_global.youtube_url) = t.youtube_url)
                      )
                     THEN 1 ELSE 0 END) AS revert_redundant,
               -- v1.12.35: accepted_update = the row's pending
               -- update was ACCEPTed previously. Suppresses
               -- REPLACE TDB in the SOURCE menu since the
               -- canonical was just downloaded from the current
               -- TDB URL — re-running would just download the
               -- same file.
               -- v1.12.99: per-section via the COALESCE-of-LEFT-JOIN
               -- pattern (pu_sec wins, '' fallback otherwise).
               (CASE WHEN COALESCE(pu_sec.decision, pu_global.decision) = 'accepted'
                     THEN 1 ELSE 0 END) AS accepted_update
    """
    # v1.11.0: every per-row JOIN to placements / local_files matches
    # by section_id = pi.section_id, so a row on the standard library
    # never sees the 4K library's placement / file (and vice versa).
    # The pre-v1.11.0 v1.10.48 / v1.10.52 gymnastics that simulated
    # per-section keying via media_folder + EXISTS subqueries are gone;
    # the schema now genuinely keys these rows per-section.
    sql_from = """
        FROM plex_items pi
        INNER JOIN plex_sections ps
          ON ps.section_id = pi.section_id AND ps.included = 1
        -- v1.11.26: themes match comes from the denormalized
        -- pi.theme_id column (populated by plex_enum + sync via
        -- resolve_theme_ids). Pre-fix this was a per-row correlated
        -- subquery with 3-OR clauses + NOT EXISTS + ORDER BY + LIMIT
        -- 1 that ran ×N for every page render, taking 20+ seconds on
        -- a 4K-item library. The lookup is now a single PK touch per
        -- row.
        LEFT JOIN themes t ON t.id = pi.theme_id
        LEFT JOIN placements p
          ON p.media_type = t.media_type
         AND p.tmdb_id = t.tmdb_id
         AND p.section_id = pi.section_id
        LEFT JOIN local_files lf
          ON lf.media_type = t.media_type
         AND lf.tmdb_id = t.tmdb_id
         AND lf.section_id = pi.section_id
        -- v1.12.86: per-section REVERT/RESTORE snapshot. Two
        -- LEFT JOINs (section-specific + '' fallback) so the SELECT
        -- can resolve the row's effective previous URL with one
        -- COALESCE rather than a per-row correlated subquery. The
        -- '' fallback row is populated by legacy callers that
        -- captured before per-section keying existed (or by future
        -- callers that genuinely want title-global semantics).
        LEFT JOIN previous_urls pv_sec
          ON pv_sec.media_type = t.media_type
         AND pv_sec.tmdb_id = t.tmdb_id
         AND pv_sec.section_id = pi.section_id
        LEFT JOIN previous_urls pv_global
          ON pv_global.media_type = t.media_type
         AND pv_global.tmdb_id = t.tmdb_id
         AND pv_global.section_id = ''
        -- v1.12.99: pending_updates is per-section as of schema v31.
        -- Two LEFT JOINs (section-specific + '' global fallback)
        -- mirror the previous_urls pattern. Library-row pending /
        -- actionable / kind columns resolve via COALESCE so each
        -- section's row reflects its own decision (KEEP CURRENT on
        -- standard doesn't bleed to 4K) and the '' row from sync
        -- still drives sections that haven't been independently
        -- accepted/declined.
        LEFT JOIN pending_updates pu_sec
          ON pu_sec.media_type = t.media_type
         AND pu_sec.tmdb_id = t.tmdb_id
         AND pu_sec.section_id = pi.section_id
        LEFT JOIN pending_updates pu_global
          ON pu_global.media_type = t.media_type
         AND pu_global.tmdb_id = t.tmdb_id
         AND pu_global.section_id = ''
    """
    sql_from_pi_only = """
        FROM plex_items pi
        INNER JOIN plex_sections ps
          ON ps.section_id = pi.section_id AND ps.included = 1
    """
    sql_where = f"WHERE {tab_where}{where_extra}"
    # v1.11.14: COUNT path now picks the minimum FROM for the requested
    # filter. The heavy LEFT JOIN themes correlated subquery (×N rows on
    # an N-item library) was firing for every filter chip click in
    # v1.11.0+, making "downloaded", "manual", "placed", etc. take
    # 30+ seconds on a 10K-item library. Filters that don't reference
    # t.* in the WHERE skip the themes JOIN; lf and p use pi.guid_tmdb
    # directly (loses orphan-adopted matches in COUNT only — the row
    # query keeps the full themes JOIN for accuracy).
    # v1.12.23: pill axes also need their relevant table JOINs in
    # the slim count path. tdb_pills references t; src_pills /
    # dl_pills / link_pills reference lf and p; pl_pills references
    # p. Without these flags the count query would 500 on missing
    # column references.
    needs_themes_for_count = (
        status in ("themed", "untracked", "has_theme", "failures", "updates")
        or tdb != "any"
        or bool(tdb_pills)
        # v1.12.100: src_pills now route through _SRC_LETTER_SQL which
        # references t.upstream_source for the orphan/svid branch. The
        # slim count path's lf+p joins aren't enough; we also need t.
        # Pre-fix selecting any SRC pill 500'd with
        # "no such column: t.upstream_source" because the count query
        # was assembled without the themes JOIN.
        or bool(src_pills)
    )
    needs_lf_for_count = (
        status in (
            "manual", "plex_agent", "untracked", "has_theme",
            "downloaded", "unplaced",
        )
        or bool(src_pills) or bool(dl_pills) or bool(link_pills)
    )
    needs_p_for_count = (
        status in (
            "manual", "plex_agent", "untracked", "has_theme",
            "placed", "unplaced",
        )
        or bool(src_pills) or bool(pl_pills) or bool(link_pills)
    )
    no_pills = not (src_pills or tdb_pills or dl_pills or pl_pills or link_pills or ed_pills)
    if status == "all" and tdb == "any" and no_pills:
        sql_count = (f"SELECT COUNT(*) {sql_from_pi_only} "
                     f"WHERE {tab_where}{where_pi_only}")
        count_params = params
    elif needs_themes_for_count:
        # Filter references t.* (themed, untracked, has_theme, failures,
        # or any tdb!='any') — fall back to the full FROM with the
        # correlated subquery.
        sql_count = f"SELECT COUNT(*) {sql_from} {sql_where}"
        count_params = params
    else:
        # Build a slim FROM with only the JOINs the WHERE references.
        # lf / p use pi.guid_tmdb directly (good enough for COUNT;
        # orphan-adopted items are rare and miscount slightly here).
        slim_parts = [sql_from_pi_only.strip()]
        if needs_p_for_count:
            slim_parts.append(
                "LEFT JOIN placements p\n"
                "  ON p.media_type = (CASE pi.media_type WHEN 'show' THEN 'tv' ELSE pi.media_type END)\n"
                " AND p.tmdb_id = pi.guid_tmdb\n"
                " AND p.section_id = pi.section_id"
            )
        if needs_lf_for_count:
            slim_parts.append(
                "LEFT JOIN local_files lf\n"
                "  ON lf.media_type = (CASE pi.media_type WHEN 'show' THEN 'tv' ELSE pi.media_type END)\n"
                " AND lf.tmdb_id = pi.guid_tmdb\n"
                " AND lf.section_id = pi.section_id"
            )
        slim_from = "\n        ".join(slim_parts)
        sql_count = f"SELECT COUNT(*) {slim_from} {sql_where}"
        count_params = params

    sql_missing_count = f"""
        SELECT COUNT(*)
        FROM plex_items pi
        INNER JOIN plex_sections ps
          ON ps.section_id = pi.section_id AND ps.included = 1
        INNER JOIN themes t
          ON t.tmdb_id = pi.guid_tmdb
         AND t.media_type = (CASE pi.media_type WHEN 'show' THEN 'tv' ELSE pi.media_type END)
        LEFT JOIN local_files lf
          ON lf.media_type = t.media_type AND lf.tmdb_id = t.tmdb_id
        WHERE {tab_where}
          AND lf.file_path IS NULL
          AND t.upstream_source != 'plex_orphan'
    """
    order_expr = _LIBRARY_SORTS_MAIN.get(sort, _LIBRARY_SORTS_MAIN["title"])
    direction = "DESC" if sort_dir == "desc" else "ASC"
    # Stable secondary sort by title so equal primary keys (lots of NULLs
    # in src/link) don't shuffle on every render.
    order_clause = f"ORDER BY {order_expr} {direction}, pi.title COLLATE NOCASE ASC"
    sql_rows = f"{sql_select} {sql_from} {sql_where} {order_clause} LIMIT ? OFFSET ?"
    # Single round-trip for the three banner-meta scalars added in v1.10.0.
    # Was three separate queries per request — now one. Each scalar is
    # already independently indexed; combining is purely a round-trip win.
    # v1.11.44: plex_enumerated is now scoped to the tab+fourk variant so
    # the "no Plex scan on record" banner stays visible per-library.
    # Pre-fix it asked "has *any* plex_items row ever been written?" —
    # so refreshing one library hid the prompt on every other untouched
    # library, leaving the user to wonder why their TV tab was blank.
    sql_meta = f"""
        SELECT
            EXISTS(SELECT 1 FROM plex_items pi
                    INNER JOIN plex_sections ps
                      ON ps.section_id = pi.section_id AND ps.included = 1
                    WHERE {tab_where})                               AS plex_enumerated,
            (SELECT MAX(finished_at) FROM jobs
              WHERE job_type = 'plex_enum' AND status = 'success') AS last_plex_enum_at,
            (SELECT MAX(finished_at) FROM sync_runs
              WHERE status = 'success')                            AS last_sync_at
    """
    offset = (page - 1) * per_page
    # v1.11.62: dl_missing requires post-SQL stat-check, which means
    # SQL pagination would lie about the total. For that filter,
    # fetch every candidate (no LIMIT) and paginate in Python below.
    # v1.12.81: DL=broken / PL=broken pills are also stat-driven —
    # SQL pagination would slice off broken candidates that happen
    # to fall outside the current page's window. Treat them like
    # status=dl_missing: load unbounded, post-filter, then paginate
    # in Python.
    needs_post_stat_pagination = (
        status == "dl_missing"
        or (dl_pills and ("broken" in dl_pills or "on" in dl_pills))
        or (pl_pills and ("broken" in pl_pills or "on" in pl_pills))
    )
    sql_rows_unbounded = f"{sql_select} {sql_from} {sql_where} {order_clause}"
    with get_conn(db) as conn:
        if needs_post_stat_pagination:
            total = -1  # filled in after stat-check
            missing_count = conn.execute(sql_missing_count).fetchone()[0]
            all_rows = conn.execute(sql_rows_unbounded, params).fetchall()
            rows = all_rows  # paginate later
        else:
            total = conn.execute(sql_count, count_params).fetchone()[0]
            missing_count = conn.execute(sql_missing_count).fetchone()[0]
            rows = conn.execute(sql_rows, params + [per_page, offset]).fetchall()
        meta = conn.execute(sql_meta).fetchone()
    plex_enumerated = bool(meta["plex_enumerated"])
    last_plex_enum_at = meta["last_plex_enum_at"]
    last_sync_at = meta["last_sync_at"]
    plex_scan_stale = bool(
        plex_enumerated
        and last_sync_at
        and (not last_plex_enum_at or last_sync_at > last_plex_enum_at)
    )
    items = [dict(r) for r in rows]
    # v1.11.62: stat the canonical for each row that has a local_files
    # entry, so the UI can render a 'DL broken' state when motif's
    # canonical was deleted out from under it but the placement
    # survives. Cheap (one stat per row, current page only); skipped
    # entirely when themes_dir isn't configured.
    items = _annotate_canonical_state(items, themes_dir=themes_dir)
    if status == "dl_missing":
        items = [it for it in items if it.get("canonical_missing")]
        total = len(items)
        items = items[offset:offset + per_page]

    # v1.12.81: post-SQL filter for DL=broken / PL=broken. Both
    # require a stat-check that doesn't live in any column. When
    # 'broken' is in dl_pills or pl_pills the SQL skipped the
    # corresponding branch (see the dl_pills / pl_pills processing
    # earlier); we apply the full pill selection here so the result
    # honors all selected states (broken alone, broken + on, etc.).
    def _row_matches_dl(it, pills):
        for p in pills:
            if p == "on" and it.get("file_path") and not it.get("canonical_missing"):
                return True
            if p == "off" and not it.get("file_path"):
                return True
            if p == "broken" and it.get("canonical_missing"):
                return True
        return False

    def _row_matches_pl(it, pills):
        for p in pills:
            if p == "on" and it.get("media_folder") and not it.get("placement_missing"):
                return True
            if p == "off" and not it.get("media_folder") and not it.get("file_path"):
                return True
            if p == "await" and not it.get("media_folder") and it.get("file_path"):
                return True
            if p == "broken" and it.get("placement_missing"):
                return True
        return False

    # v1.12.95: post-stat pagination triggers on either 'on' or
    # 'broken' in the pill set — `on` was leaking broken rows
    # because the SQL `lf.file_path IS NOT NULL` (or
    # `p.media_folder IS NOT NULL`) doesn't know about disk state.
    # _row_matches_dl / _row_matches_pl apply the strict
    # canonical_missing / placement_missing exclusion so the
    # filter result mirrors the row-pill display exactly.
    if dl_pills and ("broken" in dl_pills or "on" in dl_pills):
        items = [it for it in items if _row_matches_dl(it, dl_pills)]
        total = len(items)
        items = items[offset:offset + per_page]
    if pl_pills and ("broken" in pl_pills or "on" in pl_pills):
        items = [it for it in items if _row_matches_pl(it, pl_pills)]
        total = len(items)
        items = items[offset:offset + per_page]
    return {"total": total, "missing_count": missing_count,
            "page": page, "per_page": per_page,
            "tab": tab, "fourk": fourk, "items": items,
            "plex_enumerated": plex_enumerated,
            "plex_scan_stale": plex_scan_stale,
            "last_plex_enum_at": last_plex_enum_at,
            "last_sync_at": last_sync_at}


def _annotate_canonical_state(items: list[dict], *, themes_dir: Path | None) -> list[dict]:
    """Add canonical_missing=True to each row whose lf.file_path points
    at a file that no longer exists under themes_dir. Best-effort —
    OSError on stat (permissions, dead mount) treats as missing.

    v1.12.81: also annotates placement_missing — placements row has a
    media_folder but theme.mp3 isn't actually there. Parallel concept
    to canonical_missing on the Plex-folder side; surfaces as PL=broken
    in the library row UI so the user can spot a placement that fell
    out of sync (Plex deleted it, file moved manually, etc.) without
    motif noticing yet.
    """
    if not themes_dir:
        for it in items:
            it["canonical_missing"] = False
            it["placement_missing"] = False
        return items
    for it in items:
        rel = it.get("file_path")
        if not rel:
            it["canonical_missing"] = False
        else:
            try:
                it["canonical_missing"] = not (themes_dir / rel).is_file()
            except OSError:
                it["canonical_missing"] = True
        media_folder = it.get("media_folder")
        if not media_folder:
            it["placement_missing"] = False
        else:
            try:
                it["placement_missing"] = not (Path(media_folder) / "theme.mp3").is_file()
            except OSError:
                it["placement_missing"] = True
    return items


def _library_not_in_plex(
    db: Path, *, tab: str, fourk: bool,
    q: str, page: int, per_page: int,
    sort: str = "title", sort_dir: str = "asc",
) -> dict:
    """Return ThemerrDB rows whose tmdb_id has no matching plex_items
    in the requested tab. Synthesizes plex-shaped fields so the frontend
    renderer treats them as just-another-row, with not_in_plex=1 driving
    the distinct visual treatment.

    v1.12.10: anime tab now mirrors the TV path. ThemerrDB doesn't
    distinguish anime from regular TV (it tracks the same TVDB/TMDB
    show records), so the TDB-only browse for anime IS the TDB-only
    browse for TV. The "exists in any Plex section" check at the
    bottom naturally excludes shows the user already owns under
    their anime section, so the user sees a curated "anime/TV TDB
    has but I don't" list. Pre-fix the anime tab early-returned
    empty, which made the v1.12.6 faded-T pill useless on /anime.
    """
    media_type = "movie" if tab == "movies" else "tv"
    plex_media_type = "movie" if tab == "movies" else "show"
    # v1.10.55: drop the per-tab section-flag filter (is_anime/is_4k)
    # on the 'exists in Plex' check. Pre-1.10.55 a tab's not_in_plex
    # only counted plex_items in sections matching that tab's flags,
    # so an anime show registered as 'not in Plex' on the TV tab
    # even though the user has it in their anime section. Now we
    # check whether ANY managed plex_items row covers the title;
    # the media_type match stays since TMDB id namespaces are
    # disjoint per media_type (movie 999 ≠ show 999).
    params: list = [media_type, plex_media_type]
    q_where = ""
    if q:
        q_where = " AND (t.title LIKE ? OR t.imdb_id = ?)"
        params.extend([f"%{q}%", q])
    base_where = f"""
        WHERE t.media_type = ?
          AND t.upstream_source != 'plex_orphan'
          AND NOT EXISTS (
              SELECT 1 FROM plex_items pi2
              INNER JOIN plex_sections ps2
                ON ps2.section_id = pi2.section_id AND ps2.included = 1
              WHERE pi2.guid_tmdb = t.tmdb_id
                AND pi2.media_type = ?
          )
          {q_where}
    """
    sql_count = f"SELECT COUNT(*) FROM themes t {base_where}"
    order_expr = _LIBRARY_SORTS_NOT_IN_PLEX.get(
        sort, _LIBRARY_SORTS_NOT_IN_PLEX["title"],
    )
    direction = "DESC" if sort_dir == "desc" else "ASC"
    order_clause = f"ORDER BY {order_expr} {direction}, t.title COLLATE NOCASE ASC"
    sql_rows = f"""
        SELECT
            ('themerrdb-' || t.media_type || '-' || t.tmdb_id) AS rating_key,
            NULL AS section_id,
            (CASE t.media_type WHEN 'tv' THEN 'show' ELSE t.media_type END) AS plex_media_type,
            t.title AS plex_title,
            t.year, t.imdb_id AS guid_imdb, t.tmdb_id AS guid_tmdb,
            NULL AS folder_path,
            NULL AS plex_has_theme,
            NULL AS plex_local_theme,
            NULL AS section_title,
            t.tmdb_id AS theme_tmdb, t.media_type AS theme_media_type,
            t.title AS theme_title, t.youtube_url, t.youtube_video_id,
            t.failure_kind, t.failure_message, t.failure_acked_at, t.upstream_source,
            lf.file_path, lf.source_video_id, lf.provenance, lf.source_kind,
            lf.mismatch_state,
            p.media_folder, p.placement_kind, p.provenance AS placement_provenance,
            (SELECT j.job_type FROM jobs j
             WHERE j.media_type = t.media_type AND j.tmdb_id = t.tmdb_id
               AND j.job_type IN ('download', 'place')
               AND j.status IN ('pending', 'running')
             ORDER BY CASE j.status WHEN 'running' THEN 0 ELSE 1 END,
                      j.id DESC
             LIMIT 1) AS job_in_flight,
            1 AS not_in_plex
        FROM themes t
        LEFT JOIN local_files lf
          ON lf.media_type = t.media_type AND lf.tmdb_id = t.tmdb_id
        LEFT JOIN placements p
          ON p.media_type = t.media_type AND p.tmdb_id = t.tmdb_id
        {base_where}
        {order_clause}
        LIMIT ? OFFSET ?
    """
    offset = (page - 1) * per_page
    with get_conn(db) as conn:
        total = conn.execute(sql_count, params).fetchone()[0]
        rows = conn.execute(sql_rows, params + [per_page, offset]).fetchall()
        last_plex_enum_at = conn.execute(
            """SELECT MAX(finished_at) FROM jobs
               WHERE job_type = 'plex_enum' AND status = 'success'"""
        ).fetchone()[0]
        last_sync_at = conn.execute(
            """SELECT MAX(finished_at) FROM sync_runs
               WHERE status = 'success'"""
        ).fetchone()[0]
    items = [dict(r) for r in rows]
    return {"total": total, "missing_count": 0,
            "page": page, "per_page": per_page,
            "tab": tab, "fourk": fourk, "items": items,
            "plex_enumerated": True,
            "plex_scan_stale": False,
            "last_plex_enum_at": last_plex_enum_at,
            "last_sync_at": last_sync_at}


# -------- App factory --------

def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    app = FastAPI(title="Motif", docs_url=None, redoc_url=None)

    static_dir = Path(__file__).parent / "static"
    template_dir = Path(__file__).parent / "templates"
    static_dir.mkdir(parents=True, exist_ok=True)
    template_dir.mkdir(parents=True, exist_ok=True)
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
    templates = Jinja2Templates(directory=str(template_dir))

    # Make __version__ available to every template (rendered into the brand
    # badge at top-left). Centralising here means individual route handlers
    # don't need to remember to pass it.
    from .. import __version__ as motif_version
    templates.env.globals["motif_version"] = motif_version

    app.add_middleware(AuthMiddleware, settings=settings)

    def get_db_path() -> Path:
        return settings.db_path

    # --- HTML routes ---

    @app.get("/", response_class=HTMLResponse)
    async def dashboard(request: Request):
        return templates.TemplateResponse(request, "dashboard.html")

    @app.get("/movies", response_class=HTMLResponse)
    async def movies_page(request: Request):
        return templates.TemplateResponse(request, "library.html", {
            "tab": "movies", "title": "Movies",
        })

    @app.get("/tv", response_class=HTMLResponse)
    async def tv_page(request: Request):
        return templates.TemplateResponse(request, "library.html", {
            "tab": "tv", "title": "TV Shows",
        })

    @app.get("/anime", response_class=HTMLResponse)
    async def anime_page(request: Request):
        return templates.TemplateResponse(request, "library.html", {
            "tab": "anime", "title": "Anime",
        })

    @app.get("/coverage", response_class=HTMLResponse)
    async def coverage_page(request: Request):
        # /coverage is gone in v1.8.1 — its content was either duplicated on
        # the Dash (stat cards, copies/relink) or superseded by the missing-
        # themes banner on /movies, /tv, /anime. Redirect bookmarks to the
        # Dash where the relevant tools now live.
        return RedirectResponse("/", status_code=302)

    @app.get("/libraries", response_class=HTMLResponse)
    async def libraries_page(request: Request):
        # Libraries content lives under Settings → PLEX in v1.7+. Keep the
        # old route as a 302 redirect so bookmarks still land somewhere
        # useful.
        return RedirectResponse("/settings#plex", status_code=302)

    @app.get("/queue", response_class=HTMLResponse)
    async def queue_page(request: Request):
        return templates.TemplateResponse(request, "queue.html")

    # /scans page removed in v1.10.22 — adopt is now inline in the
    # library row actions (per the v1.10.9+ ADOPT button). The backend
    # /api/scans endpoints below stay for direct API callers and as a
    # fallback path for hash-match adoption from a real scan run, but
    # there's no UI surface and no /scans HTML route.

    # v1.12.41: /pending tab removed. Pending downloads are now
    # surfaced via the library tab's TDB ↑ pill filter instead;
    # the dedicated tab was redundant. No redirect — /pending
    # was never linked externally.

    @app.get("/settings", response_class=HTMLResponse)
    async def settings_page(request: Request):
        _require_admin(request)
        return templates.TemplateResponse(request, "settings.html")

    # --- Auth pages ---

    @app.get("/setup", response_class=HTMLResponse)
    async def setup_get(request: Request):
        if setup_complete(settings.db_path):
            return RedirectResponse("/", status_code=302)
        return templates.TemplateResponse(request, "setup.html", {"error": None})

    @app.post("/setup")
    async def setup_post(
        request: Request,
        username: Annotated[str, Form()],
        password: Annotated[str, Form()],
        password_confirm: Annotated[str, Form()],
    ):
        if setup_complete(settings.db_path):
            return RedirectResponse("/", status_code=302)
        try:
            if password != password_confirm:
                raise ValueError("Passwords do not match")
            create_admin(settings.db_path, username=username, password=password)
        except (ValueError, RuntimeError) as e:
            return templates.TemplateResponse(
                request, "setup.html", {"error": str(e)}, status_code=400,
            )
        # Auto-login
        sid = create_session(
            settings.db_path, username=username,
            user_agent=request.headers.get("User-Agent"),
        )
        log_event(settings.db_path, level="INFO", component="auth",
                  message=f"Initial admin '{username}' created and logged in")
        resp = RedirectResponse("/", status_code=302)
        _set_session_cookie(resp, sid, request)
        return resp

    @app.get("/login", response_class=HTMLResponse)
    async def login_get(request: Request, next: str = "/"):
        # If forward-auth is on, login page isn't useful; redirect home
        if settings.trust_forward_auth:
            return RedirectResponse(next or "/", status_code=302)
        # Already logged in?
        if request.state.principal.is_authenticated:
            return RedirectResponse(next or "/", status_code=302)
        return templates.TemplateResponse(
            request, "login.html",
            {"error": None, "next": next or "/"},
        )

    @app.post("/login")
    async def login_post(
        request: Request,
        username: Annotated[str, Form()],
        password: Annotated[str, Form()],
        next: Annotated[str, Form()] = "/",
    ):
        ok = authenticate_password(
            settings.db_path, username=username, password=password,
        )
        if not ok:
            log_event(settings.db_path, level="WARNING", component="auth",
                      message=f"Failed login attempt for '{username}'")
            return templates.TemplateResponse(
                request, "login.html",
                {"error": "Invalid username or password", "next": next or "/"},
                status_code=401,
            )
        sid = create_session(
            settings.db_path, username=username,
            user_agent=request.headers.get("User-Agent"),
        )
        log_event(settings.db_path, level="INFO", component="auth",
                  message=f"User '{username}' logged in")
        # Validate `next` to avoid open-redirect — must be a same-origin path
        target = next if (next and next.startswith("/") and not next.startswith("//")) else "/"
        resp = RedirectResponse(target, status_code=302)
        _set_session_cookie(resp, sid, request)
        return resp

    @app.post("/logout")
    @app.get("/logout")
    async def logout(request: Request):
        sid = request.cookies.get(SESSION_COOKIE)
        if sid:
            destroy_session(settings.db_path, sid)
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie(SESSION_COOKIE, path="/")
        return resp

    # --- API tokens (admin) ---

    @app.get("/api/tokens")
    async def api_list_tokens(request: Request, db: Path = Depends(get_db_path)):
        _require_admin(request)
        return {"tokens": list_api_tokens(db)}

    @app.post("/api/tokens")
    async def api_create_token(
        request: Request,
        name: Annotated[str, Form()],
        scope: Annotated[str, Form()] = "read",
        db: Path = Depends(get_db_path),
    ):
        _require_admin(request)
        if scope not in ("read", "admin"):
            raise HTTPException(status_code=400, detail="scope must be 'read' or 'admin'")
        try:
            token_id, raw = create_api_token(db, name=name, scope=scope)  # type: ignore[arg-type]
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        log_event(db, level="INFO", component="auth",
                  message=f"API token '{name}' (scope={scope}) created by "
                          f"{request.state.principal.username}")
        return {"id": token_id, "token": raw, "scope": scope, "name": name}

    @app.delete("/api/tokens/{token_id}")
    async def api_revoke_token(
        request: Request, token_id: int, db: Path = Depends(get_db_path),
    ):
        _require_admin(request)
        ok = revoke_api_token(db, token_id)
        if not ok:
            raise HTTPException(status_code=404, detail="token not found or already revoked")
        log_event(db, level="INFO", component="auth",
                  message=f"API token id={token_id} revoked by "
                          f"{request.state.principal.username}")
        return {"ok": True}

    @app.post("/api/admin/password")
    async def api_change_password(
        request: Request,
        current_password: Annotated[str, Form()],
        new_password: Annotated[str, Form()],
        db: Path = Depends(get_db_path),
    ):
        _require_admin(request)
        if request.state.principal.auth_method != "session":
            raise HTTPException(status_code=403,
                                detail="password change requires interactive session")
        try:
            ok = change_admin_password(
                db, current_password=current_password, new_password=new_password,
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        if not ok:
            raise HTTPException(status_code=401, detail="current password is incorrect")
        log_event(db, level="INFO", component="auth",
                  message=f"Admin password changed by {request.state.principal.username}")
        return {"ok": True}

    # --- JSON: stats shaped for Homepage's Custom API widget ---

    @app.get("/api/public/stats")
    async def api_public_stats(db: Path = Depends(get_db_path)):
        """Stats endpoint shaped for Homepage's Custom API widget. Exposes
        only counters — no titles, no paths, no sensitive data. Requires
        any authenticated principal; a read-scope API token from
        /settings#tokens is the intended caller."""
        with get_conn(db) as conn:
            stats = conn.execute("""
                SELECT
                  (SELECT COUNT(*) FROM themes WHERE media_type = 'movie') AS movies_total,
                  (SELECT COUNT(*) FROM themes WHERE media_type = 'tv') AS tv_total,
                  (SELECT COUNT(*) FROM local_files WHERE media_type = 'movie') AS movies_dl,
                  (SELECT COUNT(*) FROM local_files WHERE media_type = 'tv') AS tv_dl,
                  (SELECT COUNT(*) FROM placements WHERE media_type = 'movie') AS movies_placed,
                  (SELECT COUNT(*) FROM placements WHERE media_type = 'tv') AS tv_placed,
                  (SELECT COUNT(*) FROM placements WHERE placement_kind = 'hardlink') AS placements_hardlinks,
                  (SELECT COUNT(*) FROM placements WHERE placement_kind = 'copy') AS placements_copies,
                  (SELECT COALESCE(SUM(file_size), 0) FROM local_files) AS storage_bytes,
                  (SELECT COALESCE(SUM(lf.file_size), 0)
                   FROM placements p
                   JOIN local_files lf
                     ON lf.media_type = p.media_type AND lf.tmdb_id = p.tmdb_id
                   WHERE p.placement_kind = 'copy') AS storage_copies_bytes,
                  (SELECT COUNT(*) FROM jobs WHERE status = 'pending') AS pending,
                  (SELECT COUNT(*) FROM jobs WHERE status = 'running') AS running,
                  (SELECT COUNT(*) FROM jobs WHERE status = 'failed' AND acked_at IS NULL) AS failed,
                  -- v1.12.87: unacked_failures drives the topbar's red
                  -- IDLE dot — counts themes with an active alarm
                  -- (failure_kind set, failure_acked_at NULL). Distinct
                  -- from `failed` (jobs-based) which powers the /queue
                  -- FAILED filter chip and is purely an audit count.
                  -- ACK FAILURE clears failure_acked_at, so the topbar
                  -- dot reliably reflects user-acknowledged state
                  -- regardless of whether the underlying failed-status
                  -- job rows have been cancelled or not.
                  (SELECT COUNT(*) FROM themes
                   WHERE failure_kind IS NOT NULL
                     AND failure_acked_at IS NULL) AS unacked_failures,
                  -- v1.12.99: per-section count. Walks every plex_items
                  -- row whose effective pending_updates decision (per-
                  -- section first, '' fallback) is 'pending', so the
                  -- topbar UPD badge equals the number of (section ×
                  -- title) instances awaiting decision. Pre-fix the
                  -- count was titles-with-pending which under-reported
                  -- when the same title spanned multiple sections.
                  (SELECT COUNT(*) FROM plex_items pi2
                   INNER JOIN plex_sections ps2
                     ON ps2.section_id = pi2.section_id AND ps2.included = 1
                   INNER JOIN themes t2 ON t2.id = pi2.theme_id
                   WHERE COALESCE(
                     (SELECT pu.decision FROM pending_updates pu
                       WHERE pu.media_type = t2.media_type
                         AND pu.tmdb_id = t2.tmdb_id
                         AND pu.section_id = pi2.section_id),
                     (SELECT pu.decision FROM pending_updates pu
                       WHERE pu.media_type = t2.media_type
                         AND pu.tmdb_id = t2.tmdb_id
                         AND pu.section_id = '')
                   ) = 'pending'
                  ) AS updates_pending,
                  (SELECT COUNT(*) FROM themes WHERE failure_kind IS NOT NULL) AS failures_total
            """).fetchone()
            last_sync = conn.execute(
                "SELECT started_at, finished_at, status FROM sync_runs "
                "ORDER BY id DESC LIMIT 1"
            ).fetchone()

        return {
            "movies_total": stats["movies_total"],
            "movies_downloaded": stats["movies_dl"],
            "movies_placed": stats["movies_placed"],
            "movies_missing": max(0, stats["movies_total"] - stats["movies_dl"]),
            "tv_total": stats["tv_total"],
            "tv_downloaded": stats["tv_dl"],
            "tv_placed": stats["tv_placed"],
            "tv_missing": max(0, stats["tv_total"] - stats["tv_dl"]),
            "placements_hardlinks": stats["placements_hardlinks"],
            "placements_copies": stats["placements_copies"],
            "storage_bytes": stats["storage_bytes"],
            "storage_copies_bytes": stats["storage_copies_bytes"],
            "queue_pending": stats["pending"],
            "queue_running": stats["running"],
            "queue_failed": stats["failed"],
            "unacked_failures": stats["unacked_failures"],
            "updates_pending": stats["updates_pending"],
            "failures_total": stats["failures_total"],
            "last_sync_at": last_sync["finished_at"] if last_sync else None,
            "last_sync_status": last_sync["status"] if last_sync else None,
            "dry_run": is_dry_run(db, default=settings.dry_run_default),
        }

    def _set_session_cookie(resp: Response, sid: str, request: Request) -> None:
        # Detect HTTPS via X-Forwarded-Proto when behind a proxy, else by URL scheme
        proto = request.headers.get("x-forwarded-proto", "").lower() or request.url.scheme
        secure = proto == "https"
        resp.set_cookie(
            SESSION_COOKIE,
            sid,
            max_age=SESSION_TTL_SECONDS,
            httponly=True,
            secure=secure,
            samesite="lax",
            path="/",
        )

    # --- JSON: stats ---

    def _stats_sync(db: Path) -> tuple:
        """All SQL for /api/stats. Pulled out so the async endpoint can
        ship it to the threadpool — this is polled every 15s by the
        topbar and was blocking the event loop alongside library reads.
        """
        with get_conn(db) as conn:
            row = conn.execute("""
                SELECT
                  (SELECT COUNT(*) FROM themes WHERE media_type = 'movie') AS movies_total,
                  (SELECT COUNT(*) FROM themes WHERE media_type = 'tv') AS tv_total,
                  -- v1.11.97: TDB pill gate — only counts themes
                  -- whose upstream_source is a real ThemerrDB hit
                  -- ('imdb' / 'themoviedb'), excluding plex_orphan
                  -- (manual uploads, adoptions). The 'no TDB' pill
                  -- is a statement about ThemerrDB coverage, so an
                  -- uploaded theme shouldn't make us claim TDB has
                  -- been polled for that media_type.
                  (SELECT COUNT(*) FROM themes
                    WHERE media_type = 'movie'
                      AND upstream_source IN ('imdb', 'themoviedb'))
                                                AS movies_tdb_total,
                  (SELECT COUNT(*) FROM themes
                    WHERE media_type = 'tv'
                      AND upstream_source IN ('imdb', 'themoviedb'))
                                                AS tv_tdb_total,
                  (SELECT COUNT(*) FROM local_files WHERE media_type = 'movie') AS movies_dl,
                  (SELECT COUNT(*) FROM local_files WHERE media_type = 'tv') AS tv_dl,
                  (SELECT COUNT(*) FROM placements WHERE media_type = 'movie') AS movies_placed,
                  (SELECT COUNT(*) FROM placements WHERE media_type = 'tv') AS tv_placed,
                  (SELECT COUNT(*) FROM placements WHERE placement_kind = 'hardlink') AS hardlinks,
                  (SELECT COUNT(*) FROM placements WHERE placement_kind = 'copy') AS copies,
                  (SELECT COALESCE(SUM(file_size), 0) FROM local_files) AS storage_bytes,
                  (SELECT COALESCE(SUM(lf.file_size), 0)
                   FROM placements p
                   JOIN local_files lf
                     ON lf.media_type = p.media_type AND lf.tmdb_id = p.tmdb_id
                   WHERE p.placement_kind = 'copy') AS storage_copies_bytes,
                  (SELECT COUNT(*) FROM jobs WHERE status = 'pending') AS pending,
                  (SELECT COUNT(*) FROM jobs WHERE status = 'running') AS running,
                  (SELECT COUNT(*) FROM jobs WHERE status = 'failed' AND acked_at IS NULL) AS failed,
                  -- v1.12.87: unacked_failures drives the topbar's red
                  -- IDLE dot — counts themes with an active alarm
                  -- (failure_kind set, failure_acked_at NULL). Distinct
                  -- from `failed` (jobs-based) which powers the /queue
                  -- FAILED filter chip and is purely an audit count.
                  -- ACK FAILURE clears failure_acked_at, so the topbar
                  -- dot reliably reflects user-acknowledged state
                  -- regardless of whether the underlying failed-status
                  -- job rows have been cancelled or not.
                  (SELECT COUNT(*) FROM themes
                   WHERE failure_kind IS NOT NULL
                     AND failure_acked_at IS NULL) AS unacked_failures,
                  (SELECT COUNT(*) FROM jobs
                   WHERE job_type IN ('sync','plex_enum')
                     AND status IN ('pending','running')) AS sync_in_flight,
                  -- v1.10.6: split out so the UI can lock the per-tab
                  -- REFRESH button only when a Plex enum is active, and
                  -- the SYNC button only when a ThemerrDB sync is active.
                  (SELECT COUNT(*) FROM jobs
                   WHERE job_type = 'sync'
                     AND status IN ('pending','running')) AS themerrdb_sync_in_flight,
                  (SELECT COUNT(*) FROM jobs
                   WHERE job_type = 'plex_enum'
                     AND status IN ('pending','running')) AS plex_enum_in_flight,
                  -- v1.11.35: 'running' alone (no pending) so the topbar
                  -- banner only claims activity that's actually
                  -- happening right now. Pre-fix the banner said
                  -- 'SYNCING THEMERRDB + PLEX' when sync was running
                  -- and plex_enum was queued behind it — misleading
                  -- because the worker is single-threaded so plex_enum
                  -- doesn't start until sync finishes.
                  (SELECT COUNT(*) FROM jobs
                   WHERE job_type = 'sync'
                     AND status = 'running') AS themerrdb_sync_running,
                  (SELECT COUNT(*) FROM jobs
                   WHERE job_type = 'plex_enum'
                     AND status = 'running') AS plex_enum_running,
                  -- v1.11.7: per-job-type counts so the topbar status
                  -- can report what's actually being worked on. Cheaper
                  -- than per-type subqueries because the index on
                  -- (status, next_run_at) lets each one be a single seek.
                  (SELECT COUNT(*) FROM jobs
                   WHERE job_type = 'download'
                     AND status IN ('pending','running')) AS download_in_flight,
                  (SELECT COUNT(*) FROM jobs
                   WHERE job_type = 'place'
                     AND status IN ('pending','running')) AS place_in_flight,
                  (SELECT COUNT(*) FROM jobs
                   WHERE job_type = 'scan'
                     AND status IN ('pending','running')) AS scan_in_flight,
                  -- v1.12.15: refresh jobs (Plex metadata nudge enqueued
                  -- post-place with a 30s next_run_at delay) also count
                  -- toward "something is queued" — the topbar tooltip
                  -- breaks them out so the user sees what's actually
                  -- waiting instead of a generic amber dot.
                  (SELECT COUNT(*) FROM jobs
                   WHERE job_type = 'refresh'
                     AND status IN ('pending','running')) AS refresh_in_flight,
                  -- Local files with no placement = items waiting placement
                  -- approval (typically because a sidecar already exists at
                  -- the Plex folder; user must approve overwrite via /pending).
                  -- v1.11.86: exclude items already approved (pending/running
                  -- 'place' job exists). Without this, clicking PUSH TO PLEX
                  -- on a row left the /pending banner + nav dot lit until
                  -- the worker actually completed placement, even though the
                  -- user-facing question ("approve placement?") was already
                  -- answered. A failed placement re-becomes 'pending_placement'
                  -- because the job's status flips to 'failed' (no longer
                  -- counted by IN ('pending','running')) — correct behavior.
                  -- v1.11.99: also count mismatch_state='pending' rows
                  -- (canonical and placement diverged via SET URL /
                  -- UPLOAD MP3 over an existing placement). KEEP MISMATCH
                  -- flips the state to 'acked', which removes the row
                  -- from /pending while the library row still shows
                  -- DL=amber + LINK=≠ as a passive reminder.
                  (SELECT COUNT(*) FROM local_files lf
                   LEFT JOIN placements p
                     ON p.media_type = lf.media_type AND p.tmdb_id = lf.tmdb_id
                   WHERE (p.media_folder IS NULL
                          OR lf.mismatch_state = 'pending')
                     AND NOT EXISTS (
                       SELECT 1 FROM jobs j
                        WHERE j.job_type = 'place'
                          AND j.status IN ('pending','running')
                          AND j.media_type = lf.media_type
                          AND j.tmdb_id = lf.tmdb_id
                     )) AS pending_placements,
                  -- v1.12.99: per-section count. Walks every plex_items
                  -- row whose effective pending_updates decision (per-
                  -- section first, '' fallback) is 'pending', so the
                  -- topbar UPD badge equals the number of (section ×
                  -- title) instances awaiting decision. Pre-fix the
                  -- count was titles-with-pending which under-reported
                  -- when the same title spanned multiple sections.
                  (SELECT COUNT(*) FROM plex_items pi2
                   INNER JOIN plex_sections ps2
                     ON ps2.section_id = pi2.section_id AND ps2.included = 1
                   INNER JOIN themes t2 ON t2.id = pi2.theme_id
                   WHERE COALESCE(
                     (SELECT pu.decision FROM pending_updates pu
                       WHERE pu.media_type = t2.media_type
                         AND pu.tmdb_id = t2.tmdb_id
                         AND pu.section_id = pi2.section_id),
                     (SELECT pu.decision FROM pending_updates pu
                       WHERE pu.media_type = t2.media_type
                         AND pu.tmdb_id = t2.tmdb_id
                         AND pu.section_id = '')
                   ) = 'pending'
                  ) AS updates_pending,
                  -- v1.10.50: exclude acked rows from the topbar
                  -- failure counts. Acked = the user dealt with it
                  -- (manual URL/upload/adopt or explicit ACK); the
                  -- TDB pill keeps its red state but the row is no
                  -- longer 'needs attention'.
                  (SELECT COUNT(*) FROM themes
                   WHERE failure_kind IS NOT NULL
                     AND failure_acked_at IS NULL) AS failures_total,
                  (SELECT COUNT(*) FROM themes
                   WHERE failure_kind IN ('video_private','video_removed','video_age_restricted','geo_blocked')
                     AND failure_acked_at IS NULL
                  ) AS failures_unavailable,
                  (SELECT COUNT(*) FROM themes
                   WHERE failure_kind = 'cookies_expired'
                     AND failure_acked_at IS NULL) AS failures_cookies,
                  (SELECT COUNT(*) FROM themes WHERE upstream_source = 'plex_orphan') AS orphans_total,
                  -- Tab availability for adaptive nav/toggle rendering.
                  -- A "tab" is present if at least one *included* section
                  -- with the matching flag pair exists. The Movies and TV
                  -- tabs explicitly exclude anime-flagged sections.
                  EXISTS (SELECT 1 FROM plex_sections
                          WHERE included = 1 AND type = 'movie'
                            AND is_anime = 0 AND is_4k = 0) AS movies_std,
                  EXISTS (SELECT 1 FROM plex_sections
                          WHERE included = 1 AND type = 'movie'
                            AND is_anime = 0 AND is_4k = 1) AS movies_4k,
                  EXISTS (SELECT 1 FROM plex_sections
                          WHERE included = 1 AND type = 'show'
                            AND is_anime = 0 AND is_4k = 0) AS tv_std,
                  EXISTS (SELECT 1 FROM plex_sections
                          WHERE included = 1 AND type = 'show'
                            AND is_anime = 0 AND is_4k = 1) AS tv_4k,
                  EXISTS (SELECT 1 FROM plex_sections
                          WHERE included = 1
                            AND is_anime = 1 AND is_4k = 0) AS anime_std,
                  EXISTS (SELECT 1 FROM plex_sections
                          WHERE included = 1
                            AND is_anime = 1 AND is_4k = 1) AS anime_4k
            """).fetchone()
            last_sync = conn.execute("""
                SELECT * FROM sync_runs ORDER BY id DESC LIMIT 1
            """).fetchone()
            # v1.11.27: which (tab variant, section_id) pairs are
            # currently being enumerated. The library page REFRESH
            # button and the settings per-section REFRESH buttons
            # use these to lock per-button instead of globally so
            # the user can refresh one tab while another is still
            # running.
            enum_running_rows = conn.execute("""
                SELECT DISTINCT
                    ps.section_id, ps.type, ps.is_anime, ps.is_4k
                FROM jobs j
                INNER JOIN plex_sections ps
                  ON ps.section_id = json_extract(j.payload, '$.section_id')
                WHERE j.job_type = 'plex_enum'
                  AND j.status IN ('pending', 'running')
            """).fetchall()
            # v1.12.11: tab hint for the topbar FAIL badge link.
            # Pre-fix the badge hard-coded ?tab=movies regardless of
            # where the failure actually was, so a /anime failure
            # routed to /movies which had nothing to show. Pick a
            # tab from the first unacked dead-URL failure: the
            # section's flags determine which tab owns the failing
            # row (anime > 4k movies/tv > standard movies/tv).
            failure_tab_row = conn.execute("""
                SELECT ps.type, ps.is_anime
                FROM themes t
                JOIN plex_items pi
                  ON pi.guid_tmdb = t.tmdb_id
                 AND pi.media_type = (CASE t.media_type WHEN 'tv' THEN 'show' ELSE t.media_type END)
                JOIN plex_sections ps
                  ON ps.section_id = pi.section_id AND ps.included = 1
                WHERE t.failure_kind IN ('video_private','video_removed','video_age_restricted','geo_blocked')
                  AND t.failure_acked_at IS NULL
                LIMIT 1
            """).fetchone()
            # v1.12.48: matching tab hint for the topbar UPD badge so
            # clicking the badge lands the user on the tab that owns
            # the first row with a pending update. Mirrors the FAIL
            # query above. NULL when no pending updates exist.
            update_tab_row = conn.execute("""
                SELECT ps.type, ps.is_anime
                FROM pending_updates pu
                JOIN plex_items pi
                  ON pi.guid_tmdb = pu.tmdb_id
                 AND pi.media_type = (CASE pu.media_type WHEN 'tv' THEN 'show' ELSE pu.media_type END)
                JOIN plex_sections ps
                  ON ps.section_id = pi.section_id AND ps.included = 1
                WHERE pu.decision = 'pending'
                LIMIT 1
            """).fetchone()
            dry = is_dry_run(db, default=settings.dry_run_default)
        return row, last_sync, enum_running_rows, dry, failure_tab_row, update_tab_row

    # v1.11.37: short-TTL cache for /api/stats. Topbar polls every 15s
    # AND every page that wires sync/refresh fires it on click — under
    # an active sync this added up to multiple full-table-scan queries
    # per second, contending for the writer lock and dragging UI
    # navigation. A 1-second cache collapses overlapping calls into
    # one DB hit. The cache key is the db path so multi-tenant isn't
    # broken (single-tenant install has one key).
    _stats_cache: dict = {"key": None, "ts": 0.0, "value": None}
    _stats_cache_ttl = 1.0

    @app.get("/api/stats")
    async def api_stats(db: Path = Depends(get_db_path)):
        import time as _t
        now = _t.monotonic()
        cached = _stats_cache
        if (cached["key"] == str(db)
                and (now - cached["ts"]) < _stats_cache_ttl
                and cached["value"] is not None):
            return cached["value"]
        row, last_sync, enum_running_rows, dry, failure_tab_row, update_tab_row = await run_in_threadpool(_stats_sync, db)
        # v1.11.27: aggregate the per-section enum_running rows into a
        # tab-variant map and a section_id list so the UI can lock
        # buttons granularly.
        plex_enum_active = {
            "movies": {"standard": False, "fourk": False},
            "tv":     {"standard": False, "fourk": False},
            "anime":  {"standard": False, "fourk": False},
        }
        plex_enum_running_section_ids: list[str] = []
        for r in enum_running_rows:
            plex_enum_running_section_ids.append(r["section_id"])
            if r["is_anime"]:
                tab = "anime"
            elif r["type"] == "movie":
                tab = "movies"
            else:
                tab = "tv"
            variant = "fourk" if r["is_4k"] else "standard"
            plex_enum_active[tab][variant] = True
        response = {
            "movies": {
                "total": row["movies_total"],
                "downloaded": row["movies_dl"],
                "placed": row["movies_placed"],
                # v1.11.97: TDB-only theme count for the pill gate.
                "tdb_total": row["movies_tdb_total"],
            },
            "tv": {
                "total": row["tv_total"],
                "downloaded": row["tv_dl"],
                "placed": row["tv_placed"],
                "tdb_total": row["tv_tdb_total"],
            },
            "queue": {
                "pending": row["pending"],
                "running": row["running"],
                "failed": row["failed"],
                # v1.12.87: themes-based "active alarm" count for the
                # topbar's IDLE dot. Drops to zero on ACK FAILURE
                # regardless of whether the failed-status job rows
                # remain in the queue (they stay as audit trail).
                "unacked_failures": row["unacked_failures"],
                "sync_in_flight": row["sync_in_flight"],
                "themerrdb_sync_in_flight": row["themerrdb_sync_in_flight"],
                "plex_enum_in_flight": row["plex_enum_in_flight"],
                "themerrdb_sync_running": row["themerrdb_sync_running"],
                "plex_enum_running": row["plex_enum_running"],
                "download_in_flight": row["download_in_flight"],
                "place_in_flight": row["place_in_flight"],
                "scan_in_flight": row["scan_in_flight"],
                "refresh_in_flight": row["refresh_in_flight"],
                "pending_placements": row["pending_placements"],
                # v1.11.27: per-tab and per-section enum activity for
                # granular UI button locking.
                "plex_enum_active": plex_enum_active,
                "plex_enum_running_section_ids": plex_enum_running_section_ids,
            },
            "storage": {
                "hardlinks": row["hardlinks"],
                "copies": row["copies"],
                "total_bytes": row["storage_bytes"],
                "copies_bytes": row["storage_copies_bytes"],
                "orphans": row["orphans_total"],
            },
            "updates": {
                "pending": row["updates_pending"],
                # v1.12.48: tab hint mirrors failures.tab_hint so the
                # topbar UPD badge can route to whichever tab owns
                # the first pending update (anime / tv / movies).
                "tab_hint": (
                    "anime" if update_tab_row and update_tab_row["is_anime"]
                    else "movies" if update_tab_row and update_tab_row["type"] == "movie"
                    else "tv" if update_tab_row
                    else None
                ),
            },
            "failures": {
                "total": row["failures_total"],
                "unavailable": row["failures_unavailable"],
                "cookies_expired": row["failures_cookies"],
                # v1.12.11: tab hint for the topbar badge link.
                # NULL → no failures, JS keeps the default /movies link.
                "tab_hint": (
                    "anime" if failure_tab_row and failure_tab_row["is_anime"]
                    else "movies" if failure_tab_row and failure_tab_row["type"] == "movie"
                    else "tv" if failure_tab_row
                    else None
                ),
            },
            "config": {
                "paths_ready": settings.is_paths_ready(),
                "themes_dir": str(settings.themes_dir) if settings.is_paths_ready() else None,
                # v1.10.44: drives the cookies-aware TDB pill — green
                # when present (even if a previous probe set
                # cookies_expired), amber only when actually missing.
                "cookies_present": (settings.cookies_file is not None
                                    and settings.cookies_file.is_file()),
            },
            "tab_availability": {
                "movies": {"standard": bool(row["movies_std"]),
                           "fourk":   bool(row["movies_4k"])},
                "tv":     {"standard": bool(row["tv_std"]),
                           "fourk":   bool(row["tv_4k"])},
                "anime":  {"standard": bool(row["anime_std"]),
                           "fourk":   bool(row["anime_4k"])},
            },
            "dry_run": dry,
            "last_sync": dict(last_sync) if last_sync else None,
        }
        # Stash for the next caller within the TTL window.
        _stats_cache["key"] = str(db)
        _stats_cache["ts"] = now
        _stats_cache["value"] = response
        return response

    @app.get("/api/sections/coverage")
    async def api_sections_coverage(db: Path = Depends(get_db_path)):
        """v1.12.67: per-section coverage breakdown. Returns one row
        per managed plex_section with theme/unthemed/failure/pending
        counts. Powers the dashboard's PER-SECTION COVERAGE block —
        users with multi-section libraries (4K + standard, anime,
        etc.) can see at a glance which library has gaps without
        navigating into each tab.

        Themed = any of: motif placement exists, Plex sidecar (file
        in Plex folder), or Plex agent reports has_theme. Unthemed
        is the inverse on counted plex_items rows. Failures and
        pending_updates count titles whose theme record (themes
        row) has the corresponding flag — pending_updates is
        keyed on (media_type, tmdb_id) without section_id, so a
        single TDB-URL change for a multi-section title increments
        each owning section's count (each section will need its
        own ACCEPT UPDATE).

        Excluded sections (ps.included = 0) are filtered out so
        the dashboard shows what motif actually manages.
        """
        def _query():
            with get_conn(db) as conn:
                rows = conn.execute("""
                    SELECT
                      ps.section_id, ps.title, ps.type,
                      ps.is_4k, ps.is_anime,
                      COUNT(pi.rating_key) AS total,
                      COALESCE(SUM(CASE
                        WHEN p.media_folder IS NOT NULL THEN 1
                        WHEN pi.local_theme_file = 1 THEN 1
                        WHEN pi.has_theme = 1 THEN 1
                        ELSE 0
                      END), 0) AS themed,
                      COALESCE(SUM(CASE
                        WHEN t.failure_kind IS NOT NULL
                         AND t.failure_acked_at IS NULL THEN 1
                        ELSE 0
                      END), 0) AS failures,
                      COALESCE(SUM(CASE
                        WHEN EXISTS (
                          SELECT 1 FROM pending_updates pu
                          WHERE pu.media_type = (
                            CASE pi.media_type
                              WHEN 'show' THEN 'tv'
                              ELSE pi.media_type
                            END)
                            AND pu.tmdb_id = pi.guid_tmdb
                            AND pu.decision = 'pending'
                        ) THEN 1 ELSE 0
                      END), 0) AS pending_updates
                    FROM plex_sections ps
                    LEFT JOIN plex_items pi
                      ON pi.section_id = ps.section_id
                    LEFT JOIN themes t
                      ON t.id = pi.theme_id
                    LEFT JOIN placements p
                      ON p.media_type = t.media_type
                     AND p.tmdb_id = t.tmdb_id
                     AND p.section_id = pi.section_id
                    WHERE ps.included = 1
                    GROUP BY ps.section_id
                    ORDER BY ps.is_anime, ps.type, ps.is_4k, ps.title
                """).fetchall()
            return [dict(r) for r in rows]
        sections_raw = await run_in_threadpool(_query)
        sections = []
        for d in sections_raw:
            d["unthemed"] = max(0, (d["total"] or 0) - (d["themed"] or 0))
            # Map (type, is_anime) → tab so the frontend can route
            # the click-through to the right /movies / /tv / /anime
            # page. Mirrors the legacy applyTabAvailability logic.
            if d["is_anime"]:
                d["tab"] = "anime"
            elif d["type"] == "show":
                d["tab"] = "tv"
            else:
                d["tab"] = "movies"
            sections.append(d)
        return {"sections": sections}

    # --- Dry-run toggle ---

    @app.get("/api/dry-run")
    async def api_get_dry_run(db: Path = Depends(get_db_path)):
        """Read current dry-run state (read scope)."""
        return {"dry_run": is_dry_run(db, default=settings.dry_run_default)}

    @app.post("/api/dry-run")
    async def api_set_dry_run(
        request: Request,
        enabled: Annotated[str, Form()],   # "true" or "false"
        db: Path = Depends(get_db_path),
    ):
        _require_admin(request)
        val = enabled.strip().lower() in ("1", "true", "yes", "on")
        set_dry_run(db, val, updated_by=request.state.principal.username)
        log_event(db, level="WARNING" if not val else "INFO", component="api",
                  message=f"Dry-run {'ENABLED' if val else 'DISABLED'} by "
                          f"{request.state.principal.username}")
        return {"ok": True, "dry_run": val}

    # --- Config file: full settings round-trip ---

    @app.get("/api/config")
    async def api_get_config(request: Request):
        """Return the full config plus metadata about which fields are
        env-overridden (and therefore read-only from the UI).

        The Plex token is masked: the response shows only whether it's
        set or not, not the value itself."""
        _require_admin(request)
        cfg = settings.cfg
        from dataclasses import asdict
        payload = asdict(cfg)
        # Mask the token value (frontend just wants to know if it's set)
        if payload.get("plex", {}).get("token"):
            payload["plex"]["token"] = "***"
            payload["plex"]["token_set"] = True
        else:
            payload["plex"]["token_set"] = False
            payload["plex"]["token"] = ""
        # Same for the optional TMDB API key (and legacy tvdb_api_key)
        if payload.get("plex", {}).get("tvdb_api_key"):
            payload["plex"]["tvdb_api_key"] = "***"
            payload["plex"]["tvdb_api_key_set"] = True
        else:
            payload["plex"]["tvdb_api_key_set"] = False
            payload["plex"]["tvdb_api_key"] = ""
        if payload.get("plex", {}).get("tmdb_api_key"):
            payload["plex"]["tmdb_api_key"] = "***"
            payload["plex"]["tmdb_api_key_set"] = True
        else:
            payload["plex"]["tmdb_api_key_set"] = False
            payload["plex"]["tmdb_api_key"] = ""
        return {
            "config": payload,
            "env_overrides": settings.env_overrides(),
            "validation_errors": settings.validate_current(require_themes_dir=False),
            "is_paths_ready": settings.is_paths_ready(),
        }

    @app.patch("/api/config")
    async def api_patch_config(request: Request, db: Path = Depends(get_db_path)):
        """Apply a partial config update. Body is a JSON object whose
        structure mirrors MotifConfig — only the keys present are updated.

        The plex.token field is special: an empty string leaves the existing
        token untouched (so the masked UI display doesn't accidentally clear
        the token on save). To explicitly clear the token, send `null`.

        Returns the new config plus any validation errors. Validation
        errors that are NOT `themes_dir`-related still result in a 200 and
        a saved file — we don't block saves on warnings — but they're
        surfaced so the UI can render them inline."""
        _require_admin(request)

        try:
            body = await request.json()
        except Exception:
            raise HTTPException(status_code=400, detail="invalid JSON body")
        if not isinstance(body, dict):
            raise HTTPException(status_code=400, detail="body must be a JSON object")

        cfg = settings.cfg

        try:
            _apply_partial_config(cfg, body)
        except (ValueError, TypeError) as e:
            raise HTTPException(status_code=400, detail=str(e))

        # Validate strictly (require_themes_dir=False because partial saves
        # might happen before the user gets to that field)
        errors = validate_config(cfg, require_themes_dir=False)
        # Block saves only on truly malformed data — out-of-range numbers,
        # bad cron, invalid plus_mode, etc. Empty themes_dir is fine to save.
        critical = [e for e in errors if "themes_dir is not set" not in e]
        if critical:
            raise HTTPException(status_code=400,
                                detail={"errors": critical})

        settings.save(cfg, updated_by=request.state.principal.username)

        # Re-read so env overrides are re-applied
        settings.reload()

        # v1.11.0: only the themes_dir root is precreated. Per-section
        # subdirs land lazily as the worker / API write into them.
        if settings.is_paths_ready():
            try:
                settings.themes_dir.mkdir(parents=True, exist_ok=True)
            except OSError as e:
                log_event(db, level="WARNING", component="config",
                          message=f"Could not create themes subdirs: {e}")

        log_event(db, level="INFO", component="config",
                  message=f"Config saved by {request.state.principal.username}",
                  detail={"changed_top_keys": sorted(body.keys())})

        # Return the updated config with masking applied
        return await api_get_config(request)

    # --- Plex folder scanning ---

    @app.post("/api/scans")
    async def api_trigger_scan(request: Request, db: Path = Depends(get_db_path)):
        """Enqueue a scan job. Returns the job id; poll /api/scans for run state."""
        _require_admin(request)
        if not settings.is_paths_ready():
            raise HTTPException(
                status_code=400,
                detail="themes_dir not configured — set it on /settings before scanning",
            )
        # Reject if a scan is already running
        with get_conn(db) as conn:
            running = conn.execute(
                "SELECT id FROM scan_runs WHERE status = 'running' LIMIT 1"
            ).fetchone()
            if running:
                raise HTTPException(
                    status_code=409,
                    detail=f"a scan is already running (id={running['id']})",
                )
            payload = json.dumps({"initiated_by": request.state.principal.username})
            conn.execute(
                """INSERT INTO jobs (job_type, payload, status, created_at, next_run_at)
                   VALUES ('scan', ?, 'pending', datetime('now'), datetime('now'))""",
                (payload,),
            )
            job_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        log_event(db, level="INFO", component="scan",
                  message=f"Scan enqueued by {request.state.principal.username}",
                  detail={"job_id": job_id})
        return {"ok": True, "job_id": job_id}

    @app.get("/api/scans")
    async def api_list_scans(db: Path = Depends(get_db_path), limit: int = 20):
        with get_conn(db) as conn:
            rows = conn.execute(
                """SELECT * FROM scan_runs
                   ORDER BY started_at DESC LIMIT ?""",
                (max(1, min(100, limit)),),
            ).fetchall()
            running = conn.execute(
                "SELECT id FROM scan_runs WHERE status = 'running' LIMIT 1"
            ).fetchone()
        return {
            "runs": [dict(r) for r in rows],
            "running": dict(running) if running else None,
        }

    @app.get("/api/scans/{scan_id}")
    async def api_get_scan(scan_id: int, db: Path = Depends(get_db_path)):
        with get_conn(db) as conn:
            run = conn.execute(
                "SELECT * FROM scan_runs WHERE id = ?", (scan_id,)
            ).fetchone()
            if not run:
                raise HTTPException(status_code=404, detail="scan not found")
            counts = {}
            for kind in ("exact_match", "hash_match", "content_mismatch",
                         "orphan_resolvable", "orphan_unresolved"):
                row = conn.execute(
                    """SELECT COUNT(*) AS c FROM scan_findings
                       WHERE scan_run_id = ? AND finding_kind = ?""",
                    (scan_id, kind),
                ).fetchone()
                counts[kind] = row["c"]
            decisions = {}
            for d in ("pending", "adopt", "replace", "keep_existing", "ignore"):
                row = conn.execute(
                    """SELECT COUNT(*) AS c FROM scan_findings
                       WHERE scan_run_id = ? AND decision = ?""",
                    (scan_id, d),
                ).fetchone()
                decisions[d] = row["c"]
        return {
            "run": dict(run),
            "kind_counts": counts,
            "decision_counts": decisions,
        }

    @app.get("/api/scans/{scan_id}/findings")
    async def api_list_findings(scan_id: int, db: Path = Depends(get_db_path),
                                kind: Optional[str] = None,
                                decision: Optional[str] = None,
                                q: Optional[str] = None,
                                exclude_exact: bool = True,
                                offset: int = 0, limit: int = 100):
        sql = "SELECT * FROM scan_findings WHERE scan_run_id = ?"
        params: list = [scan_id]
        if kind:
            sql += " AND finding_kind = ?"
            params.append(kind)
        elif exclude_exact:
            # Default view hides exact_match findings — those are already
            # adopted+hardlinked, no action needed. The EXACT chip flips
            # exclude_exact=False to opt into seeing them.
            sql += " AND finding_kind != 'exact_match'"
        if decision:
            sql += " AND decision = ?"
            params.append(decision)
        if q:
            # Substring search over folder path + resolved metadata title.
            # The title lives inside resolved_metadata JSON for orphans, so
            # we LIKE-match the JSON blob too.
            sql += " AND (media_folder LIKE ? OR file_path LIKE ? OR resolved_metadata LIKE ?)"
            like = f"%{q}%"
            params.extend([like, like, like])
        sql += " ORDER BY id LIMIT ? OFFSET ?"
        params.extend([max(1, min(500, limit)), max(0, offset)])
        with get_conn(db) as conn:
            rows = conn.execute(sql, params).fetchall()
        return {"findings": [dict(r) for r in rows]}

    @app.post("/api/scans/findings/{finding_id}/decision")
    async def api_decide_finding(finding_id: int, request: Request,
                                  db: Path = Depends(get_db_path)):
        """Apply a decision to a single scan finding. Body: {"decision": "adopt"}."""
        _require_admin(request)
        body = await request.json()
        decision = body.get("decision")
        if decision not in ("adopt", "replace", "keep_existing"):
            raise HTTPException(status_code=400,
                                detail=f"invalid decision: {decision}")
        with get_conn(db) as conn:
            f = conn.execute(
                "SELECT id, decision, adopted_at FROM scan_findings WHERE id = ?",
                (finding_id,),
            ).fetchone()
            if not f:
                raise HTTPException(status_code=404, detail="finding not found")
            if f["adopted_at"]:
                return {"ok": True, "note": "already adopted (no-op)",
                        "finding_id": finding_id}
            payload = json.dumps({
                "finding_id": finding_id,
                "decision": decision,
                "decided_by": request.state.principal.username,
            })
            conn.execute(
                """INSERT INTO jobs (job_type, payload, status, created_at, next_run_at)
                   VALUES ('adopt', ?, 'pending', datetime('now'), datetime('now'))""",
                (payload,),
            )
        return {"ok": True, "finding_id": finding_id, "decision": decision}

    @app.post("/api/scans/findings/decisions/bulk")
    async def api_decide_findings_bulk(request: Request,
                                        db: Path = Depends(get_db_path)):
        """Apply a decision to many findings at once. Body:
            {"finding_ids": [1,2,3], "decision": "adopt"}"""
        _require_admin(request)
        body = await request.json()
        ids = body.get("finding_ids") or []
        decision = body.get("decision")
        if not isinstance(ids, list) or not ids:
            raise HTTPException(status_code=400, detail="finding_ids must be non-empty list")
        if decision not in ("adopt", "replace", "keep_existing"):
            raise HTTPException(status_code=400,
                                detail=f"invalid decision: {decision}")
        enqueued = 0
        with get_conn(db) as conn:
            for fid in ids:
                try:
                    fid_int = int(fid)
                except (TypeError, ValueError):
                    continue
                row = conn.execute(
                    "SELECT id, adopted_at FROM scan_findings WHERE id = ?",
                    (fid_int,),
                ).fetchone()
                if not row or row["adopted_at"]:
                    continue
                payload = json.dumps({
                    "finding_id": fid_int,
                    "decision": decision,
                    "decided_by": request.state.principal.username,
                })
                conn.execute(
                    """INSERT INTO jobs (job_type, payload, status, created_at, next_run_at)
                       VALUES ('adopt', ?, 'pending', datetime('now'), datetime('now'))""",
                    (payload,),
                )
                enqueued += 1
        return {"ok": True, "enqueued": enqueued}

    # --- TMDB credentials test ---

    @app.post("/api/tmdb/test")
    async def api_tmdb_test(request: Request, db: Path = Depends(get_db_path)):
        """Validate the configured TMDB API key. Body optional:
        {"api_key": "..."}; if absent, uses the saved key from motif.yaml."""
        _require_admin(request)
        from ..core.tmdb import TMDBClient

        try:
            body = await request.json() if (await request.body()) else {}
        except Exception:
            body = {}
        key = body.get("api_key") or settings.tmdb_api_key
        if not key:
            return {"ok": False, "message": "no API key configured"}

        client = TMDBClient(key, db)
        ok, msg = client.test_credentials()
        return {"ok": ok, "message": msg}

    # --- Storage waste / relink ---

    @app.get("/api/storage/copies")
    async def api_storage_copies(db: Path = Depends(get_db_path)):
        """List all placements that fell back to copy (cross-FS or no link).

        Useful for finding cases where reorganizing the User Share allocation
        would let us reclaim disk by re-linking.
        """
        with get_conn(db) as conn:
            rows = conn.execute("""
                SELECT p.media_type, p.tmdb_id, p.section_id, p.media_folder, p.placed_at,
                       p.placement_kind, t.title, t.year,
                       lf.file_path AS source_path, lf.file_size
                FROM placements p
                JOIN themes t
                  ON t.media_type = p.media_type AND t.tmdb_id = p.tmdb_id
                JOIN local_files lf
                  ON lf.media_type = p.media_type
                 AND lf.tmdb_id = p.tmdb_id
                 AND lf.section_id = p.section_id
                WHERE p.placement_kind = 'copy'
                ORDER BY t.title COLLATE NOCASE
            """).fetchall()
        items = [dict(r) for r in rows]
        total_bytes = sum(r["file_size"] or 0 for r in items)
        return {
            "items": items,
            "count": len(items),
            "total_bytes": total_bytes,
        }

    @app.post("/api/storage/relink")
    async def api_relink_all(request: Request, db: Path = Depends(get_db_path)):
        _require_admin(request)
        with get_conn(db) as conn:
            existing = conn.execute(
                """SELECT id FROM jobs WHERE job_type = 'relink'
                   AND media_type IS NULL AND tmdb_id IS NULL
                   AND status IN ('pending','running')"""
            ).fetchone()
            if existing:
                return {"ok": True, "job_id": existing["id"], "already_queued": True}
            cur = conn.execute(
                """INSERT INTO jobs (job_type, status, created_at, next_run_at)
                   VALUES ('relink', 'pending', ?, ?)""",
                (now_iso(), now_iso()),
            )
            job_id = cur.lastrowid
        log_event(db, level="INFO", component="api",
                  message=f"Bulk relink sweep queued by {request.state.principal.username}")
        return {"ok": True, "job_id": job_id}

    @app.post("/api/items/{media_type}/{tmdb_id}/relink")
    async def api_relink_item(
        request: Request, media_type: MediaType, tmdb_id: int,
        db: Path = Depends(get_db_path),
    ):
        _require_admin(request)
        with get_conn(db) as conn:
            conn.execute(
                """INSERT INTO jobs (job_type, media_type, tmdb_id, status,
                                     created_at, next_run_at, payload)
                   VALUES ('relink', ?, ?, 'pending', ?, ?, '{}')""",
                (media_type, tmdb_id, now_iso(), now_iso()),
            )
        return {"ok": True}

    @app.post("/api/items/{media_type}/{tmdb_id}/restore-canonical")
    async def api_restore_canonical(
        request: Request, media_type: MediaType, tmdb_id: int,
        db: Path = Depends(get_db_path),
    ):
        """v1.11.62: restore motif's canonical from a surviving placement.

        Use case: the file under <themes_dir>/<file_path> got deleted
        (manually, by an external sweep, etc.) but the placement copy
        in the Plex media folder is still there. Re-create the
        canonical from the placement so motif resumes managing the
        file — hardlink first, fall back to copy on cross-FS, and
        update local_files.file_size / file_sha256 if the surviving
        placement differs from what we recorded.

        Iterates every (section_id) where local_files exists for this
        item; restores each one independently. Sections whose
        canonical IS still on disk are skipped (no-op).
        """
        _require_admin(request)
        if not settings.is_paths_ready():
            raise HTTPException(status_code=409,
                                detail="themes_dir not configured")
        themes_dir = settings.themes_dir
        with get_conn(db) as conn:
            rows = conn.execute(
                """SELECT lf.section_id, lf.file_path, lf.file_size,
                          lf.file_sha256, p.media_folder, p.placement_kind
                   FROM local_files lf
                   LEFT JOIN placements p
                     ON p.media_type = lf.media_type
                    AND p.tmdb_id = lf.tmdb_id
                    AND p.section_id = lf.section_id
                   WHERE lf.media_type = ? AND lf.tmdb_id = ?""",
                (media_type, tmdb_id),
            ).fetchall()
        if not rows:
            raise HTTPException(status_code=404,
                                detail="no local_files row for this item")
        from ..core.plex_enum import _candidate_local_paths
        import hashlib
        import os
        import shutil
        restored = 0
        skipped: list[dict] = []
        for r in rows:
            canonical = themes_dir / r["file_path"]
            if canonical.is_file():
                skipped.append({"section_id": r["section_id"],
                                "reason": "canonical_already_present"})
                continue
            if not r["media_folder"]:
                skipped.append({"section_id": r["section_id"],
                                "reason": "no_placement"})
                continue
            # Find a readable placement file (try host→container
            # translations the same way plex_enum does)
            placement_src: Path | None = None
            for cand in _candidate_local_paths(r["media_folder"]):
                p = cand / "theme.mp3"
                try:
                    if p.is_file():
                        placement_src = p
                        break
                except OSError:
                    continue
            if placement_src is None:
                skipped.append({"section_id": r["section_id"],
                                "reason": "placement_file_missing"})
                continue
            try:
                canonical.parent.mkdir(parents=True, exist_ok=True)
                kind = "hardlink"
                try:
                    os.link(placement_src, canonical)
                except OSError as e:
                    if e.errno != 18:  # EXDEV (cross-device)
                        raise
                    shutil.copy2(placement_src, canonical)
                    kind = "copy"
            except OSError as e:
                log.warning("restore-canonical: %s/%s section=%s failed: %s",
                            media_type, tmdb_id, r["section_id"], e)
                skipped.append({"section_id": r["section_id"],
                                "reason": f"link_failed:{e}"})
                continue
            # Re-stat for accurate file_size + sha256
            try:
                size = canonical.stat().st_size
                h = hashlib.sha256()
                with canonical.open("rb") as f:
                    for chunk in iter(lambda: f.read(1024 * 1024), b""):
                        h.update(chunk)
                sha = h.hexdigest()
            except OSError:
                size, sha = r["file_size"], r["file_sha256"]
            with get_conn(db) as conn:
                conn.execute(
                    """UPDATE local_files SET file_size = ?, file_sha256 = ?,
                                              downloaded_at = ?
                       WHERE media_type = ? AND tmdb_id = ? AND section_id = ?""",
                    (size, sha, now_iso(),
                     media_type, tmdb_id, r["section_id"]),
                )
                # If a placement existed but its kind was 'copy' and
                # we just made a hardlink, update placements too.
                if r["placement_kind"] != kind:
                    conn.execute(
                        """UPDATE placements SET placement_kind = ?
                           WHERE media_type = ? AND tmdb_id = ?
                             AND section_id = ?""",
                        (kind, media_type, tmdb_id, r["section_id"]),
                    )
            restored += 1
        log_event(db, level="INFO", component="api",
                  media_type=media_type, tmdb_id=tmdb_id,
                  message=f"Canonical restored from placement by "
                          f"{request.state.principal.username}",
                  detail={"restored": restored, "skipped": skipped})
        return {"ok": True, "restored": restored, "skipped": skipped}

    # --- Libraries ---

    @app.get("/api/libraries")
    async def api_libraries(db: Path = Depends(get_db_path)):
        """List all known Plex sections with current managed status and
        per-section theme counts."""
        sections = list_sections(db)

        # Annotate with theme counts: for each managed section, count how many
        # items in our themes table are placed into folders that fall under
        # one of this section's location_paths.
        with get_conn(db) as conn:
            for s in sections:
                paths = s.get("location_paths") or []
                if not paths:
                    s["placed_count"] = 0
                    s["copies_count"] = 0
                    continue
                # Count placements whose media_folder starts with one of the section paths
                placed = 0
                copies = 0
                for path in paths:
                    p = path.rstrip("/") + "/"
                    rows = conn.execute(
                        """SELECT placement_kind FROM placements
                           WHERE media_folder LIKE ? || '%'""",
                        (p,),
                    ).fetchall()
                    for r in rows:
                        placed += 1
                        if r["placement_kind"] == "copy":
                            copies += 1
                s["placed_count"] = placed
                s["copies_count"] = copies
        return {"sections": sections}

    @app.post("/api/libraries/{section_id}/include")
    async def api_set_library_inclusion(
        request: Request, section_id: str,
        included: Annotated[str, Form()],
        db: Path = Depends(get_db_path),
    ):
        _require_admin(request)
        val = included.strip().lower() in ("1", "true", "yes", "on")
        ok = set_section_inclusion(db, section_id, val,
                                    updated_by=request.state.principal.username)
        if not ok:
            raise HTTPException(status_code=404, detail="section not found")
        log_event(db, level="INFO", component="api",
                  message=f"Library section {section_id} {'included' if val else 'excluded'} "
                          f"by {request.state.principal.username}")
        return {"ok": True, "included": val}

    @app.post("/api/libraries/{section_id}/flags")
    async def api_set_library_flags(
        request: Request, section_id: str,
        db: Path = Depends(get_db_path),
    ):
        """Toggle the user-applied flags (is_anime, is_4k) on a Plex section.
        Accepts either:
          - legacy {"is_anime": bool, "is_4k": bool} — either field optional.
          - v1.8.2 {"role": "standard"|"4k"|"anime"|"anime_4k"} — sets both
            flags atomically based on the role mapping.
        v1.11.9: every step (read-current, data-guard, UPDATE flags,
        reassign themes_subdir) now runs in one BEGIN IMMEDIATE
        transaction so an IntegrityError on the partial UNIQUE INDEX
        rolls back cleanly instead of leaving the flags committed but
        the slug stale (which surfaced as "x N of N failed: 500" on
        the SAVE button while the pill state still appeared updated).
        Exceptions also log server-side so 500s aren't opaque.
        """
        _require_admin(request)
        try:
            body = await request.json()
        except Exception:
            raise HTTPException(status_code=400, detail="invalid JSON body")
        if not isinstance(body, dict):
            raise HTTPException(status_code=400, detail="body must be a JSON object")
        sets = []
        params = []
        if "role" in body:
            role = str(body["role"])
            role_map = {
                "standard":  (0, 0),
                "4k":        (0, 1),
                "anime":     (1, 0),
                "anime_4k":  (1, 1),
            }
            if role not in role_map:
                raise HTTPException(status_code=400,
                                    detail=f"invalid role: {role}")
            anime_v, fourk_v = role_map[role]
            sets.append("is_anime = ?")
            params.append(anime_v)
            sets.append("is_4k = ?")
            params.append(fourk_v)
        else:
            if "is_anime" in body:
                sets.append("is_anime = ?")
                params.append(1 if bool(body["is_anime"]) else 0)
            if "is_4k" in body:
                sets.append("is_4k = ?")
                params.append(1 if bool(body["is_4k"]) else 0)
        if not sets:
            return {"ok": True, "no_op": True}
        from ..core.sections import _allocate_themes_subdir
        try:
            with get_conn(db) as conn, transaction(conn):
                current = conn.execute(
                    "SELECT title, type, is_anime, is_4k "
                    "FROM plex_sections WHERE section_id = ?",
                    (section_id,),
                ).fetchone()
                if current is None:
                    raise HTTPException(status_code=404, detail="section not found")
                requested = dict(zip([s.split(" = ")[0] for s in sets], params))
                unchanged = all(
                    int(current[col]) == int(val) for col, val in requested.items()
                )
                if unchanged:
                    return {"ok": True, "no_op": True,
                            "is_anime": bool(current["is_anime"]),
                            "is_4k": bool(current["is_4k"])}
                # Real change requested. Refuse if the section has staged
                # files or placements — the slug rewrite would orphan
                # their on-disk paths.
                existing = conn.execute(
                    "SELECT (SELECT COUNT(*) FROM local_files WHERE section_id = ?) "
                    "  AS lf_count, "
                    "  (SELECT COUNT(*) FROM placements WHERE section_id = ?) "
                    "  AS pl_count",
                    (section_id, section_id),
                ).fetchone()
                if existing and (existing["lf_count"] or existing["pl_count"]):
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            f"section has {existing['lf_count']} staged file(s) and "
                            f"{existing['pl_count']} placement(s) — UNMANAGE or FORGET "
                            "those rows before flipping is_4k / is_anime"
                        ),
                    )
                # Apply the flag flip first…
                conn.execute(
                    f"UPDATE plex_sections SET {', '.join(sets)} "
                    "WHERE section_id = ?",
                    (*params, section_id),
                )
                # …then derive the new themes_subdir from the *post-update*
                # row and apply it. Both writes are inside the same
                # BEGIN IMMEDIATE so either both land or neither does.
                row = conn.execute(
                    "SELECT title, type, is_anime, is_4k "
                    "FROM plex_sections WHERE section_id = ?",
                    (section_id,),
                ).fetchone()
                new_subdir = _allocate_themes_subdir(
                    conn, title=row["title"], type_=row["type"],
                    is_anime=bool(row["is_anime"]), is_4k=bool(row["is_4k"]),
                    own_section_id=section_id,
                )
                conn.execute(
                    "UPDATE plex_sections SET themes_subdir = ? "
                    "WHERE section_id = ?",
                    (new_subdir, section_id),
                )
        except HTTPException:
            raise
        except sqlite3.IntegrityError as e:
            log.warning("section %s flags update hit IntegrityError: %s",
                        section_id, e)
            raise HTTPException(
                status_code=409,
                detail=f"themes_subdir collision while updating section: {e}",
            )
        except Exception as e:
            log.exception("section %s flags update failed", section_id)
            raise HTTPException(status_code=500, detail=f"flags update: {e}")
        log_event(db, level="INFO", component="api",
                  message=f"Library section {section_id} flags updated by "
                          f"{request.state.principal.username}",
                  detail={"is_anime": bool(row["is_anime"]),
                          "is_4k": bool(row["is_4k"]),
                          "themes_subdir": new_subdir})
        return {"ok": True, "is_anime": bool(row["is_anime"]),
                "is_4k": bool(row["is_4k"]),
                "themes_subdir": new_subdir}

    @app.post("/api/libraries/refresh")
    async def api_refresh_libraries(
        request: Request, db: Path = Depends(get_db_path),
    ):
        """Manually trigger Plex section discovery, then enqueue a plex_enum
        job for every included (selected) section so their unified browse
        cache reloads. v1.10.58: previously only re-discovered sections — the
        user expects the top-right REFRESH FROM PLEX in /settings to refresh
        the contents of all selected libraries too."""
        _require_admin(request)
        if not (settings.plex_enabled and settings.plex_token):
            raise HTTPException(status_code=400, detail="Plex not configured")
        cfg = PlexConfig(
            url=settings.plex_url, token=settings.plex_token,
            movie_section=settings.plex_movie_section,
            tv_section=settings.plex_tv_section, enabled=True,
        )
        try:
            with PlexClient(cfg, plus_mode=settings.plus_equiv_mode) as plex:  # type: ignore[arg-type]
                sections = refresh_sections(
                    db, plex,
                    excluded_titles=settings.plex_excluded_titles,
                    included_titles=settings.plex_included_titles,
                )
            enqueued = 0
            with get_conn(db) as conn:
                included = conn.execute(
                    "SELECT section_id FROM plex_sections WHERE included = 1"
                ).fetchall()
                pending_section_ids = {
                    r["section_id"] for r in conn.execute(
                        "SELECT json_extract(payload, '$.section_id') AS section_id "
                        "FROM jobs WHERE job_type = 'plex_enum' "
                        "AND status IN ('pending','running')"
                    ).fetchall() if r["section_id"] is not None
                }
                for s in included:
                    sid = s["section_id"]
                    if sid in pending_section_ids:
                        continue
                    conn.execute(
                        "INSERT INTO jobs (job_type, payload, status, created_at, next_run_at) "
                        "VALUES ('plex_enum', ?, 'pending', ?, ?)",
                        (json.dumps({"section_id": sid}), now_iso(), now_iso()),
                    )
                    enqueued += 1
            log_event(db, level="INFO", component="api",
                      message=f"Manual library refresh by {request.state.principal.username}: "
                              f"{len(sections)} sections discovered, "
                              f"{enqueued} plex_enum jobs enqueued")
            return {"ok": True, "sections_count": len(sections),
                    "enqueued": enqueued}
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Plex unreachable: {e}")

    # --- Pending updates ---

    @app.get("/api/pending")
    async def api_pending(db: Path = Depends(get_db_path)):
        """List items downloaded into the staging area but not yet placed
        into a Plex media folder. These are awaiting user approval when the
        global placement.auto_place is False, or when a sync was kicked off
        with download_only=true.

        v1.10.47: each item now carries a `reason` text explaining why it
        landed on /pending instead of auto-placing. Computed from row
        state (plex_local_theme + plex_has_theme + auto_place_default)
        so the user knows up front what approval will actually do.
        """
        with get_conn(db) as conn:
            # v1.11.0: pending = local_files row exists for (item, section)
            # but no placement for that (item, section). Each section is
            # its own pending entry — Matilda 4K can be pending while
            # Matilda standard is already placed, and vice versa.
            # v1.11.99: include mismatch_state='pending' rows. These have
            # both a local_files canonical AND a placements row but the
            # files diverged (user did SET URL / UPLOAD MP3 over an
            # existing placement). Surface them on /pending alongside
            # the classic no-placement case.
            rows = conn.execute("""
                SELECT t.media_type, t.tmdb_id, t.imdb_id, t.title, t.year,
                       t.youtube_url, t.youtube_video_id, t.upstream_source,
                       lf.section_id, lf.file_path, lf.file_size, lf.downloaded_at,
                       lf.source_video_id, lf.provenance, lf.source_kind,
                       lf.last_place_attempt_at, lf.last_place_attempt_reason,
                       lf.mismatch_state,
                       ps.title AS section_title,
                       COALESCE(MAX(pi.local_theme_file), 0) AS plex_local_theme,
                       COALESCE(MAX(pi.has_theme), 0) AS plex_has_theme,
                       (SELECT 1 FROM jobs j
                         WHERE j.media_type = lf.media_type
                           AND j.tmdb_id = lf.tmdb_id
                           AND j.section_id = lf.section_id
                           AND j.job_type = 'place'
                           AND j.status IN ('pending','running')
                         LIMIT 1) AS place_pending
                FROM local_files lf
                JOIN themes t
                  ON t.media_type = lf.media_type AND t.tmdb_id = lf.tmdb_id
                LEFT JOIN plex_sections ps
                  ON ps.section_id = lf.section_id
                LEFT JOIN placements p
                  ON p.media_type = lf.media_type
                 AND p.tmdb_id = lf.tmdb_id
                 AND p.section_id = lf.section_id
                LEFT JOIN plex_items pi
                  ON pi.guid_tmdb = t.tmdb_id
                 AND pi.section_id = lf.section_id
                 AND pi.media_type = (CASE t.media_type WHEN 'tv' THEN 'show' ELSE t.media_type END)
                WHERE p.media_folder IS NULL
                   OR lf.mismatch_state = 'pending'
                GROUP BY t.media_type, t.tmdb_id, lf.section_id
                ORDER BY MAX(lf.downloaded_at) DESC
            """).fetchall()
        auto_place = settings.auto_place_default
        items: list[dict] = []
        for r in rows:
            d = dict(r)
            # v1.11.24: reason precedence reads the actual place worker
            # outcome stamped on local_files first, falling back to the
            # pre-attempt heuristics. Pre-fix the worker would log
            # 'Skipped placement: existing_theme:theme.mp3' but /pending
            # still showed 'Awaiting placement — the worker should pick
            # it up shortly' because the reason logic checked
            # plex_items.local_theme_file (often stale) instead of the
            # actual outcome.
            attempt_reason = (d.get("last_place_attempt_reason") or "")
            place_pending = bool(d.get("place_pending"))
            # v1.11.99: mismatch state takes precedence on the reason —
            # "you uploaded new content; placement still has the old"
            # is the most actionable explanation when both apply.
            if d.get("mismatch_state") == "pending":
                d["reason"] = ("New canonical (upload / SET URL) doesn't match "
                               "the file currently at the Plex folder — "
                               "PUSH TO PLEX to overwrite, ADOPT FROM PLEX "
                               "to discard the new download, or KEEP MISMATCH "
                               "to dismiss this prompt and resolve later")
                d["reason_kind"] = "mismatch"
            elif attempt_reason.startswith("existing_theme:"):
                fname = attempt_reason.split(":", 1)[1] or "theme.mp3"
                d["reason"] = (f"Existing {fname} at the Plex folder — "
                               f"approval will overwrite it")
                d["reason_kind"] = "overwrites_sidecar"
            elif attempt_reason == "plex_has_theme":
                d["reason"] = ("Plex's agent already supplies a theme — "
                               "approval will replace it")
                d["reason_kind"] = "overwrites_plex_agent"
            elif attempt_reason.startswith("placement_error:"):
                d["reason"] = ("Last place attempt failed — "
                               + attempt_reason.split(":", 1)[1].strip())
                d["reason_kind"] = "placement_error"
            elif attempt_reason == "no_match":
                d["reason"] = ("No Plex folder matched this title — "
                               "the section may not be scanned yet")
                d["reason_kind"] = "no_match"
            elif d["plex_local_theme"]:
                d["reason"] = "Existing theme.mp3 at the Plex folder — approval will overwrite it"
                d["reason_kind"] = "overwrites_sidecar"
            elif d["plex_has_theme"]:
                d["reason"] = "Plex's agent already supplies a theme — approval will replace it"
                d["reason_kind"] = "overwrites_plex_agent"
            elif not auto_place:
                d["reason"] = "Auto-place is disabled in Settings — every download awaits approval"
                d["reason_kind"] = "auto_place_off"
            elif place_pending:
                d["reason"] = "Awaiting placement — the worker should pick it up shortly"
                d["reason_kind"] = "queued"
            else:
                d["reason"] = "No place job queued — click APPROVE to place now"
                d["reason_kind"] = "no_job"
            items.append(d)
        return {"items": items}

    @app.get("/api/pending/count")
    async def api_pending_count(db: Path = Depends(get_db_path)):
        with get_conn(db) as conn:
            row = conn.execute("""
                SELECT COUNT(*) FROM local_files lf
                LEFT JOIN placements p
                  ON p.media_type = lf.media_type
                 AND p.tmdb_id = lf.tmdb_id
                 AND p.section_id = lf.section_id
                WHERE p.media_folder IS NULL
            """).fetchone()
        return {"count": row[0]}

    @app.post("/api/pending/place")
    async def api_pending_place(request: Request, db: Path = Depends(get_db_path)):
        """Approve placement for one, many, or all staged items. Body:
        {"items": [{media_type, tmdb_id}, ...]} or {"all": true}."""
        _require_admin(request)
        try:
            body = await request.json()
        except Exception:
            body = {}
        # v1.11.0: targets are (media_type, tmdb_id, section_id) triples.
        # When the body identifies items by (mt, tmdb) only, we expand to
        # every section that has a pending local_files row for that item.
        targets: list[tuple[str, int, str]] = []
        with get_conn(db) as conn:
            if isinstance(body, dict) and body.get("all"):
                rows = conn.execute("""
                    SELECT lf.media_type, lf.tmdb_id, lf.section_id
                    FROM local_files lf
                    LEFT JOIN placements p
                      ON p.media_type = lf.media_type
                     AND p.tmdb_id = lf.tmdb_id
                     AND p.section_id = lf.section_id
                    WHERE p.media_folder IS NULL
                """).fetchall()
                targets = [(r["media_type"], r["tmdb_id"], r["section_id"])
                           for r in rows]
            elif isinstance(body, dict) and isinstance(body.get("items"), list):
                for it in body["items"]:
                    if not (isinstance(it, dict)
                            and "media_type" in it and "tmdb_id" in it):
                        continue
                    mt, tid = str(it["media_type"]), int(it["tmdb_id"])
                    if "section_id" in it:
                        targets.append((mt, tid, str(it["section_id"])))
                    else:
                        for r in conn.execute(
                            """SELECT lf.section_id FROM local_files lf
                               LEFT JOIN placements p
                                 ON p.media_type = lf.media_type
                                AND p.tmdb_id = lf.tmdb_id
                                AND p.section_id = lf.section_id
                               WHERE lf.media_type = ? AND lf.tmdb_id = ?
                                 AND p.media_folder IS NULL""",
                            (mt, tid),
                        ).fetchall():
                            targets.append((mt, tid, r["section_id"]))
            enqueued = 0
            for media_type, tmdb_id, section_id in targets:
                existing = conn.execute(
                    """SELECT id FROM jobs WHERE job_type = 'place'
                       AND media_type = ? AND tmdb_id = ? AND section_id = ?
                       AND status IN ('pending','running')""",
                    (media_type, tmdb_id, section_id),
                ).fetchone()
                if existing:
                    continue
                conn.execute(
                    """INSERT INTO jobs (job_type, media_type, tmdb_id, section_id,
                                         payload, status, created_at, next_run_at)
                       VALUES ('place', ?, ?, ?,
                               '{"force":true,"reason":"approved_from_pending"}',
                               'pending', ?, ?)""",
                    (media_type, tmdb_id, section_id, now_iso(), now_iso()),
                )
                enqueued += 1
        log_event(db, level="INFO", component="api",
                  message=f"Bulk place approved by {request.state.user}",
                  detail={"requested": len(targets), "enqueued": enqueued})
        return {"ok": True, "enqueued": enqueued, "requested": len(targets)}

    @app.post("/api/pending/discard")
    async def api_pending_discard(request: Request, db: Path = Depends(get_db_path)):
        """Decline a staged download: delete the file and its local_files row.
        Theme record is preserved so a future sync can re-detect and re-download."""
        _require_admin(request)
        try:
            body = await request.json()
        except Exception:
            body = {}
        # v1.11.0: discard targets are (mt, tmdb, section_id?) — when
        # section_id is omitted, drop every per-section local_files row
        # for the item that's not already placed in that section.
        targets: list[tuple[str, int, str | None]] = []
        if isinstance(body, dict) and isinstance(body.get("items"), list):
            for it in body["items"]:
                if not (isinstance(it, dict)
                        and "media_type" in it and "tmdb_id" in it):
                    continue
                mt, tid = str(it["media_type"]), int(it["tmdb_id"])
                sid = str(it["section_id"]) if "section_id" in it else None
                targets.append((mt, tid, sid))
        if not targets:
            return {"ok": True, "discarded": 0}
        themes_dir = settings.themes_dir
        discarded = 0
        with get_conn(db) as conn:
            for media_type, tmdb_id, section_id in targets:
                if section_id is None:
                    candidates = conn.execute(
                        """SELECT lf.section_id, lf.file_path
                           FROM local_files lf
                           LEFT JOIN placements p
                             ON p.media_type = lf.media_type
                            AND p.tmdb_id = lf.tmdb_id
                            AND p.section_id = lf.section_id
                           WHERE lf.media_type = ? AND lf.tmdb_id = ?
                             AND p.media_folder IS NULL""",
                        (media_type, tmdb_id),
                    ).fetchall()
                else:
                    candidates = conn.execute(
                        """SELECT lf.section_id, lf.file_path
                           FROM local_files lf
                           LEFT JOIN placements p
                             ON p.media_type = lf.media_type
                            AND p.tmdb_id = lf.tmdb_id
                            AND p.section_id = lf.section_id
                           WHERE lf.media_type = ? AND lf.tmdb_id = ?
                             AND lf.section_id = ?
                             AND p.media_folder IS NULL""",
                        (media_type, tmdb_id, section_id),
                    ).fetchall()
                for c in candidates:
                    if themes_dir:
                        abs_path = themes_dir / c["file_path"]
                        try:
                            if abs_path.is_file():
                                abs_path.unlink()
                        except OSError as e:
                            log.warning("Discard failed to unlink %s: %s", abs_path, e)
                    conn.execute(
                        "DELETE FROM local_files WHERE media_type = ? "
                        "AND tmdb_id = ? AND section_id = ?",
                        (media_type, tmdb_id, c["section_id"]),
                    )
                    discarded += 1
        log_event(db, level="INFO", component="api",
                  message=f"Bulk discard by {request.state.user}",
                  detail={"requested": len(targets), "discarded": discarded})
        return {"ok": True, "discarded": discarded, "requested": len(targets)}

    @app.get("/api/updates")
    async def api_list_updates(
        decision: str = Query("pending", pattern="^(pending|accepted|declined|all)$"),
        db: Path = Depends(get_db_path),
    ):
        """List pending updates: items where ThemerrDB has a different YouTube
        URL than what we already downloaded. The user can accept (re-download)
        or decline (keep current)."""
        with get_conn(db) as conn:
            if decision == "all":
                rows = conn.execute("""
                    SELECT pu.*, t.title, t.year, t.imdb_id,
                           lf.provenance AS local_provenance,
                           lf.source_video_id AS current_video_id
                    FROM pending_updates pu
                    JOIN themes t
                      ON t.media_type = pu.media_type AND t.tmdb_id = pu.tmdb_id
                    LEFT JOIN local_files lf
                      ON lf.media_type = pu.media_type AND lf.tmdb_id = pu.tmdb_id
                    ORDER BY pu.detected_at DESC
                """).fetchall()
            else:
                rows = conn.execute("""
                    SELECT pu.*, t.title, t.year, t.imdb_id,
                           lf.provenance AS local_provenance,
                           lf.source_video_id AS current_video_id
                    FROM pending_updates pu
                    JOIN themes t
                      ON t.media_type = pu.media_type AND t.tmdb_id = pu.tmdb_id
                    LEFT JOIN local_files lf
                      ON lf.media_type = pu.media_type AND lf.tmdb_id = pu.tmdb_id
                    WHERE pu.decision = ?
                    ORDER BY pu.detected_at DESC
                """, (decision,)).fetchall()
        return {"updates": [dict(r) for r in rows]}

    @app.get("/api/updates/count")
    async def api_updates_count(db: Path = Depends(get_db_path)):
        with get_conn(db) as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS n FROM pending_updates WHERE decision = 'pending'"
            ).fetchone()
        return {"pending": row["n"]}

    @app.post("/api/updates/{media_type}/{tmdb_id}/accept")
    async def api_accept_update(
        request: Request, media_type: MediaType, tmdb_id: int,
        # v1.12.46: optional section_id scopes the download +
        # placement to one section only. When omitted, falls back
        # to the v1.12.34 behavior of fanning out to every
        # section that owns the title (used by callers without
        # row-level context; the library row UI now always passes
        # section_id so the user gets per-section behavior).
        section_id: str | None = Query(None),
        db: Path = Depends(get_db_path),
    ):
        """Accept an upstream theme update: enqueue a re-download with the new URL.

        v1.12.34 / v1.12.35:
        When the row currently has a user_overrides URL (U source),
        this handler:
          1. Compares the user's URL to the new TDB URL. If they
             match, no REVERT will be possible (would just put the
             row back where it is) — replaced_user_url stays NULL.
             If they differ, the user's URL is captured into
             pending_updates.replaced_user_url for REVERT.
          2. DELETEs user_overrides so the worker uses the TDB
             URL on re-download (worker prefers user_overrides URL
             when present — pre-1.12.34, leaving the override in
             place meant ACCEPT UPDATE silently kept downloading
             the user's URL).
          3. Flips provenance='auto' / source_kind='themerrdb' on
             every local_files + placements row for this title.
             v1.12.35: pre-fix, the worker's sibling-hardlink path
             inherited the sibling's existing 'manual'/'url'
             values when no override was present — so even after
             ACCEPT UPDATE deleted user_overrides, the row kept
             classifying as U because every section's hardlink
             carried the original U-upload's provenance forward.
             Explicit flip here resolves the classification
             before the worker touches anything.
        """
        _require_admin(request)
        with get_conn(db) as conn:
            update = conn.execute(
                "SELECT new_youtube_url FROM pending_updates "
                "WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            ).fetchone()
            if update is None:
                raise HTTPException(status_code=404, detail="no pending update")
            new_tdb_url = update["new_youtube_url"]
            # v1.12.72: section-aware override fetch. Try the row's
            # section first, then fall back to the legacy '' global
            # row. ACCEPT UPDATE on a 4K row queries the 4K-specific
            # override (so the url_match check + delete + flip
            # affect only that section's override). Without
            # section_id (legacy callers / orphan paths) we fall
            # through to the global row.
            ovr_section_for_accept = section_id or ""
            override = conn.execute(
                "SELECT youtube_url FROM user_overrides "
                "WHERE media_type = ? AND tmdb_id = ? AND section_id = ?",
                (media_type, tmdb_id, ovr_section_for_accept),
            ).fetchone()
            if override is None and ovr_section_for_accept:
                # Try global fallback when no per-section override
                override = conn.execute(
                    "SELECT youtube_url FROM user_overrides "
                    "WHERE media_type = ? AND tmdb_id = ? AND section_id = ''",
                    (media_type, tmdb_id),
                ).fetchone()
                if override is not None:
                    # The override that applied was the global one,
                    # so deleting it on accept means dropping it
                    # globally too. Track which section_id the
                    # delete should target.
                    ovr_section_for_accept = ""

            # v1.12.37: capture the row's current URL into
            # themes.previous_youtube_url so REVERT can round-trip
            # this accept later.
            # v1.12.61: previous capture is now unconditional. The
            # original code skipped capture when override URL ==
            # new TDB URL, on the theory that REVERT would be a
            # no-op. That suppressed REVERT correctly but also lied
            # about the row's "previous state" in the INFO card —
            # for a SET URL → ACCEPT UPDATE flow with matching
            # URLs, the previous_youtube_url stayed at the
            # pre-SET-URL value (themerrdb-kind), so the card
            # showed "previous url: X themerrdb" when the URL was
            # actually a user-sourced override moments before.
            # v1.12.53's smarter revert_redundant SQL hides the
            # REVERT button independently (suppresses when
            # previous_url == current canonical URL), so the
            # skip-capture optimization is no longer needed.
            # Capturing always means the kind reflects the actual
            # source: if override exists at this moment, kind=user;
            # otherwise kind=themerrdb.
            url_match = bool(
                override
                and new_tdb_url
                and override["youtube_url"]
                and override["youtube_url"].strip() == new_tdb_url.strip()
            )
            # v1.12.105: scope the capture to the row's section so
            # the section-specific previous_urls slot tracks the URL
            # being replaced. Pre-fix the call wrote to section_id=''
            # while the user's ACCEPT UPDATE actually changed the
            # section-scoped override — the per-section previous_url
            # stayed stale (mirrors the SET URL bug). For bulk
            # accept-all the section_id is None by design (the bulk
            # endpoint fans out, no per-row context).
            _capture_previous_url(conn, media_type, tmdb_id,
                                  section_id=section_id)

            if override:
                # v1.12.72: scoped delete using the section_id we
                # tracked during the override fetch above. If a
                # per-section override existed for this section,
                # only it is deleted (sibling sections keep theirs);
                # if we fell through to the global row, the global
                # row gets dropped instead.
                conn.execute(
                    "DELETE FROM user_overrides "
                    "WHERE media_type = ? AND tmdb_id = ? AND section_id = ?",
                    (media_type, tmdb_id, ovr_section_for_accept),
                )
                # v1.12.37: provenance flip happens lazily, on
                # download completion (worker.py writes fresh
                # provenance/source_kind based on the current
                # override presence). Pre-fix the API flipped
                # provenance eagerly here so the row classified
                # as T immediately, but the user wants the T
                # badge to appear only AFTER the new canonical is
                # in place — pre-download the row should still
                # show its current source state.
                #
                # v1.12.53: BUT when url_match=True the new
                # canonical IS already in place — override URL
                # == new TDB URL means the file already on disk
                # was downloaded from the same YouTube video TDB
                # would now download. So flip eagerly only for
                # the url_match case. Closes a race where a
                # follow-on SET URL re-creates the override
                # before the worker re-runs, leaving local_files
                # stale at source_kind='url'. Scoped to
                # section_id when provided (mirrors the
                # _enqueue_download scope below); otherwise
                # covers every section the global override
                # affected.
                if url_match:
                    if section_id:
                        conn.execute(
                            """UPDATE local_files
                                  SET provenance = 'auto',
                                      source_kind = 'themerrdb'
                                WHERE media_type = ?
                                  AND tmdb_id = ?
                                  AND section_id = ?""",
                            (media_type, tmdb_id, section_id),
                        )
                    else:
                        conn.execute(
                            """UPDATE local_files
                                  SET provenance = 'auto',
                                      source_kind = 'themerrdb'
                                WHERE media_type = ?
                                  AND tmdb_id = ?""",
                            (media_type, tmdb_id),
                        )

            # v1.12.99: per-section decision write. Schema v31 keys
            # pending_updates by (mt, tmdb, section_id); the helper
            # inserts a section-specific row leaving the '' fallback
            # untouched, so other sections continue to see pending.
            _set_pending_update_decision(
                conn, media_type=media_type, tmdb_id=tmdb_id,
                section_id=section_id, decision="accepted",
                decided_by=request.state.principal.username,
            )
            _record_audit(
                conn, actor=request.state.user, action="accept_update",
                media_type=media_type, tmdb_id=tmdb_id,
                section_id=section_id,
                details={
                    "old_url": override["youtube_url"] if override else None,
                    "new_url": new_tdb_url,
                    "url_match": url_match,
                },
            )
            from ..core.sync import _enqueue_download
            # v1.12.41: ACCEPT UPDATE forces auto_place=True so
            # the download chains directly into a place job
            # instead of deferring to the global setting.
            # v1.12.43: also pass force_place=True so the place
            # job overwrites whatever theme.mp3 is currently in
            # the Plex folder (typical case: a user-source
            # theme that the user is now updating to TDB's URL).
            # Pre-fix the place job ran but skipped with status
            # "plex_has_theme" because Plex's metadata cache
            # still showed has_theme=true from the existing
            # user file — motif downloaded the new canonical
            # but the placement file never got replaced, so
            # Plex kept playing the old U theme.
            # v1.12.46: section_id (if provided) scopes the
            # enqueue to only one section. Without it, the
            # download fans out to every section that owns the
            # title — wrong when the same title lives in
            # multiple libraries with separately-themed editions
            # (e.g., standard + 4K). The library row UI passes
            # section_id; sync/scheduled callers omit it.
            _enqueue_download(
                conn, media_type=media_type, tmdb_id=tmdb_id,
                reason="upstream_update_accepted",
                auto_place=True,
                force_place=True,
                only_section_id=section_id,
            )
        # v1.12.43: log message references url_match (computed
        # above) instead of the v1.12.35-era replaced_user_url
        # variable which was removed when the previous-URL state
        # moved to themes.previous_youtube_url. The stale
        # reference NameError'd accept-update for U-source rows,
        # 500ing the response even though the DB transaction +
        # download enqueue succeeded.
        log_event(db, level="INFO", component="api",
                  media_type=media_type, tmdb_id=tmdb_id,
                  message=(f"Update accepted by {request.state.principal.username}"
                           + (" (user URL captured for revert)"
                              if (override and not url_match)
                              else " (user URL matched TDB; no revert)"
                              if override else "")))
        return {"ok": True}

    @app.post("/api/updates/{media_type}/{tmdb_id}/decline")
    async def api_decline_update(
        request: Request, media_type: MediaType, tmdb_id: int,
        section_id: str | None = Query(None),
        db: Path = Depends(get_db_path),
    ):
        """Decline an upstream update: keep the current theme. The decline
        is sticky — won't re-prompt unless ThemerrDB updates again.

        v1.12.99: per-section. Pre-fix declining on standard wrote a
        title-global decision that 4K's row inherited too. Now writes
        a section-specific row; sibling sections continue to see the
        '' fallback (pending) until they make their own decision.
        """
        _require_admin(request)
        with get_conn(db) as conn:
            written = _set_pending_update_decision(
                conn, media_type=media_type, tmdb_id=tmdb_id,
                section_id=section_id, decision="declined",
                decided_by=request.state.principal.username,
            )
            if written == 0:
                raise HTTPException(status_code=404, detail="no pending update")
            _record_audit(
                conn, actor=request.state.principal.username,
                action="decline_update",
                media_type=media_type, tmdb_id=tmdb_id,
                section_id=section_id,
            )
        log_event(db, level="INFO", component="api",
                  media_type=media_type, tmdb_id=tmdb_id,
                  section_id=section_id,
                  message=f"Update declined by {request.state.principal.username}")
        return {"ok": True}

    # v1.12.56: server-side YouTube oEmbed proxy. Used by the INFO
    # card's pending-update diff view to fetch human-readable video
    # titles for side-by-side current/proposed tiles. YouTube's
    # oEmbed endpoint doesn't reliably set CORS headers, so a
    # browser-side fetch fails intermittently — proxying server-side
    # avoids that, and the LRU cache amortizes repeat lookups
    # across rows that share videos (e.g. franchise themes).
    _OEMBED_CACHE: dict[str, dict] = {}
    _OEMBED_CACHE_MAX = 512

    def _fetch_youtube_oembed(yt_url: str) -> dict | None:
        cached = _OEMBED_CACHE.get(yt_url)
        if cached is not None:
            return cached
        try:
            import httpx
            with httpx.Client(timeout=8.0, follow_redirects=True) as c:
                resp = c.get(
                    "https://www.youtube.com/oembed",
                    params={"url": yt_url, "format": "json"},
                )
                if resp.status_code != 200:
                    return None
                data = resp.json()
        except Exception:
            return None
        # Trim to fields the UI uses (avoid leaking unbounded
        # response shapes through the cache).
        slim = {
            "title": data.get("title"),
            "author_name": data.get("author_name"),
            "author_url": data.get("author_url"),
            "thumbnail_url": data.get("thumbnail_url"),
        }
        # Naive LRU eviction — drop the oldest entry when full.
        # dict iteration order is insertion-ordered (Python 3.7+).
        if len(_OEMBED_CACHE) >= _OEMBED_CACHE_MAX:
            try:
                first_key = next(iter(_OEMBED_CACHE))
                _OEMBED_CACHE.pop(first_key, None)
            except StopIteration:
                pass
        _OEMBED_CACHE[yt_url] = slim
        return slim

    @app.get("/api/youtube/oembed")
    async def api_youtube_oembed(url: str = Query(...)):
        """Proxy + cache YouTube oEmbed lookups. Returns
        {title, author_name, author_url, thumbnail_url} or 404 when
        the URL is not a recognizable YouTube video / oEmbed
        rejected it (private, removed, geo-blocked). Surface the
        404 cleanly so the UI can fall back to thumbnail-only +
        bare URL display."""
        if not url or "youtube.com" not in url and "youtu.be" not in url:
            raise HTTPException(status_code=400, detail="not a YouTube URL")
        data = await run_in_threadpool(_fetch_youtube_oembed, url)
        if data is None:
            raise HTTPException(status_code=404, detail="oEmbed lookup failed")
        return data

    @app.post("/api/updates/accept-all")
    async def api_accept_all_updates(
        request: Request, db: Path = Depends(get_db_path),
    ):
        """v1.12.55: bulk-accept every pending_updates row with
        decision='pending'. Iterates server-side applying the same
        logic as the single-row api_accept_update: capture previous
        URL, delete user_overrides, eager-flip local_files for
        url_match rows (v1.12.53), mark decision='accepted', enqueue
        a download. No per-section scoping — bulk fans out to every
        section owning each title (the v1.12.34 fallback). Users who
        need per-edition control should use the per-row action.

        Returns counts so the UI can show breakdown
        (e.g. "23 accepted · 5 eager-flipped · 18 downloads queued").
        """
        _require_admin(request)
        accepted = 0
        eager_flipped = 0
        enqueued = 0
        with get_conn(db) as conn:
            rows = conn.execute(
                """SELECT media_type, tmdb_id, new_youtube_url, kind
                     FROM pending_updates
                    WHERE decision = 'pending'"""
            ).fetchall()
            from ..core.sync import _enqueue_download
            for row in rows:
                media_type = row["media_type"]
                tmdb_id = row["tmdb_id"]
                new_tdb_url = row["new_youtube_url"]
                override = conn.execute(
                    "SELECT youtube_url FROM user_overrides "
                    "WHERE media_type = ? AND tmdb_id = ?",
                    (media_type, tmdb_id),
                ).fetchone()
                url_match = bool(
                    override
                    and new_tdb_url
                    and override["youtube_url"]
                    and override["youtube_url"].strip() == new_tdb_url.strip()
                )
                if not url_match:
                    _capture_previous_url(conn, media_type, tmdb_id)
                if override:
                    conn.execute(
                        "DELETE FROM user_overrides "
                        "WHERE media_type = ? AND tmdb_id = ?",
                        (media_type, tmdb_id),
                    )
                    if url_match:
                        conn.execute(
                            """UPDATE local_files
                                  SET provenance = 'auto',
                                      source_kind = 'themerrdb'
                                WHERE media_type = ?
                                  AND tmdb_id = ?""",
                            (media_type, tmdb_id),
                        )
                        eager_flipped += 1
                conn.execute(
                    """UPDATE pending_updates SET decision = 'accepted',
                           decision_at = ?, decision_by = ?
                       WHERE media_type = ? AND tmdb_id = ?""",
                    (now_iso(), request.state.principal.username,
                     media_type, tmdb_id),
                )
                _enqueue_download(
                    conn, media_type=media_type, tmdb_id=tmdb_id,
                    reason="bulk_update_accepted",
                    auto_place=True,
                    force_place=True,
                    only_section_id=None,
                )
                enqueued += 1
                accepted += 1
        log_event(
            db, level="INFO", component="api",
            message=(
                f"Bulk-accepted {accepted} pending update"
                f"{'s' if accepted != 1 else ''} by "
                f"{request.state.principal.username} "
                f"({eager_flipped} eager-flipped, "
                f"{enqueued} downloads queued)"
            ),
        )
        return {
            "ok": True,
            "accepted": accepted,
            "eager_flipped": eager_flipped,
            "downloads_queued": enqueued,
        }

    @app.post("/api/updates/decline-all")
    async def api_decline_all_updates(
        request: Request, db: Path = Depends(get_db_path),
    ):
        """v1.12.55: bulk-decline every pending update. Flips
        decision='declined' for every row currently at 'pending'.
        The blue ↑ pill stays for filter/sort but the topbar UPD
        count drops to 0. Sticky — won't re-prompt unless
        ThemerrDB updates again."""
        _require_admin(request)
        with get_conn(db) as conn:
            cur = conn.execute(
                """UPDATE pending_updates SET decision = 'declined',
                       decision_at = ?, decision_by = ?
                   WHERE decision = 'pending'""",
                (now_iso(), request.state.principal.username),
            )
            declined = cur.rowcount
        log_event(
            db, level="INFO", component="api",
            message=(
                f"Bulk-declined {declined} pending update"
                f"{'s' if declined != 1 else ''} by "
                f"{request.state.principal.username}"
            ),
        )
        return {"ok": True, "declined": declined}

    # --- JSON: items ---

    @app.get("/api/library")
    async def api_library(
        tab: str = Query(..., pattern="^(movies|tv|anime)$"),
        fourk: bool = Query(False),
        q: str = Query(""),
        status: str = Query("all", pattern="^(all|has_theme|themed|manual|plex_agent|untracked|downloaded|placed|unplaced|dl_missing|failures|updates|not_in_plex)$"),
        tdb: str = Query("any", pattern="^(any|tracked|untracked)$"),
        # v1.12.23: server-side multi-select pill filters. Each axis
        # accepts a comma-separated list. Empty = no filter on that
        # axis. Selections within an axis OR; axes AND.
        src_pills: str = Query(""),
        tdb_pills: str = Query(""),
        dl_pills: str = Query(""),
        pl_pills: str = Query(""),
        link_pills: str = Query(""),
        # v1.12.41: EDITION axis. Empty = no filter; 'has' /
        # 'none' / 'has,none' control whether to surface only
        # rows with / without an `{edition-...}` Plex folder tag.
        ed_pills: str = Query(""),
        page: int = Query(1, ge=1),
        per_page: int = Query(50, ge=1, le=200),
        sort: str = Query("title", pattern="^(title|year|src|dl|pl|link|imdb|attention)$"),
        sort_dir: str = Query("asc", pattern="^(asc|desc)$"),
        db: Path = Depends(get_db_path),
    ):
        """Unified browse: every item Plex sees in the requested tab/sub-tab,
        joined to themes / local_files / placements so the UI can show the
        full theme-status picture in one row.

        v1.10.1: status=not_in_plex switches to a different query shape that
        returns ThemerrDB rows whose tmdb_id has no matching plex_items in
        this tab. Rows are synthesized into the plex-shaped schema so the
        frontend renderer can handle them uniformly (with not_in_plex=1
        distinguishing them visually). Skipped on anime tab — themes table
        has no anime signal.
        """
        if status == "not_in_plex":
            # tdb sub-filter is redundant for not_in_plex (everything in
            # that view is tracked), so it's not threaded through.
            return await run_in_threadpool(
                _library_not_in_plex, db, tab=tab, fourk=fourk,
                q=q, page=page, per_page=per_page,
                sort=sort, sort_dir=sort_dir,
            )
        # v1.12.23: parse the comma-separated pill axes once.
        def _pset(s: str, valid: set[str]) -> set[str]:
            if not s:
                return set()
            return {p for p in (x.strip() for x in s.split(",")) if p in valid}
        src_set = _pset(src_pills, {"T", "U", "A", "M", "P", "-"})
        tdb_set = _pset(tdb_pills, {"tdb", "update", "cookies", "dead", "none"})
        dl_set = _pset(dl_pills, {"on", "off", "broken"})
        pl_set = _pset(pl_pills, {"on", "await", "off", "broken"})
        link_set = _pset(link_pills, {"hl", "c", "m", "none"})
        ed_set = _pset(ed_pills, {"has", "none"})
        # v1.12.23: 'broken' DL pill alone routes through the existing
        # dl_missing path (post-SQL stat-check). Combined with on/off
        # selections it's ignored for now — would require refactoring
        # the post-SQL annotator to OR with the SQL pre-filter.
        effective_status = status
        if dl_set == {"broken"} and status == "all":
            effective_status = "dl_missing"
            dl_set = set()  # consumed by status routing
        cookies_present = bool(settings.cookies_file
                               and settings.cookies_file.exists())
        return await run_in_threadpool(
            _library_main_query, db, tab=tab, fourk=fourk,
            q=q, status=effective_status, tdb=tdb,
            page=page, per_page=per_page,
            sort=sort, sort_dir=sort_dir,
            src_pills=src_set, tdb_pills=tdb_set,
            dl_pills=dl_set, pl_pills=pl_set, link_pills=link_set,
            ed_pills=ed_set,
            cookies_present=cookies_present,
            themes_dir=settings.themes_dir if settings.is_paths_ready() else None,
        )

    @app.post("/api/library/download-batch")
    async def api_library_download_batch(
        request: Request, db: Path = Depends(get_db_path),
    ):
        """Enqueue download jobs for an explicit list of items. Body:
        {"items": [{"media_type": "...", "tmdb_id": ...}, ...]}.
        Skips items already downloaded or already enqueued. Returns
        {ok, enqueued, skipped}.
        """
        _require_admin(request)
        try:
            body = await request.json()
        except Exception:
            body = {}
        items = body.get("items") if isinstance(body, dict) else None
        if not isinstance(items, list) or not items:
            raise HTTPException(status_code=400, detail="items must be a non-empty list")
        enqueued = 0
        skipped = 0
        from ..core.sync import _enqueue_download
        with get_conn(db) as conn:
            for it in items:
                if not isinstance(it, dict):
                    skipped += 1
                    continue
                mt = str(it.get("media_type", ""))
                tid = it.get("tmdb_id")
                if mt not in ("movie", "tv") or not isinstance(tid, int):
                    skipped += 1
                    continue
                # Verify the theme actually exists
                theme = conn.execute(
                    "SELECT 1 FROM themes WHERE media_type = ? AND tmdb_id = ?",
                    (mt, tid),
                ).fetchone()
                if not theme:
                    skipped += 1
                    continue
                # _enqueue_download fans out per-section + dedupes per-section
                n = _enqueue_download(
                    conn, media_type=mt, tmdb_id=tid, reason="bulk_select",
                )
                if n == 0:
                    skipped += 1
                else:
                    enqueued += n
        log_event(db, level="INFO", component="api",
                  message=f"Bulk download-batch by {request.state.user}",
                  detail={"enqueued": enqueued, "skipped": skipped,
                          "requested": len(items)})
        return {"ok": True, "enqueued": enqueued, "skipped": skipped}

    @app.post("/api/library/download-missing")
    async def api_library_download_missing(
        request: Request, db: Path = Depends(get_db_path),
    ):
        """Enqueue a download job for every plex_item in the requested tab
        whose ThemerrDB theme exists but motif hasn't downloaded yet.
        Body: {tab, fourk}."""
        _require_admin(request)
        try:
            body = await request.json()
        except Exception:
            body = {}
        tab = body.get("tab", "movies")
        fourk = bool(body.get("fourk", False))
        if tab not in ("movies", "tv", "anime"):
            raise HTTPException(status_code=400, detail="invalid tab")
        if tab == "movies":
            tab_where = "pi.media_type = 'movie' AND ps.is_anime = 0"
        elif tab == "tv":
            tab_where = "pi.media_type = 'show' AND ps.is_anime = 0"
        else:
            tab_where = "ps.is_anime = 1"
        tab_where += " AND ps.is_4k = 1" if fourk else " AND ps.is_4k = 0"

        enqueued = 0
        from ..core.sync import _enqueue_download
        with get_conn(db) as conn:
            # v1.11.0: missing = (theme exists) AND (this row's section
            # has no local_files row). Per-section local_files match
            # cleanly via lf.section_id = pi.section_id, so the LEFT JOIN
            # nulls out exactly the (item, section) pairs we still need
            # to download.
            rows = conn.execute(f"""
                SELECT DISTINCT t.media_type, t.tmdb_id
                FROM plex_items pi
                INNER JOIN plex_sections ps
                  ON ps.section_id = pi.section_id AND ps.included = 1
                INNER JOIN themes t
                  ON t.tmdb_id = pi.guid_tmdb
                 AND t.media_type = (CASE pi.media_type WHEN 'show' THEN 'tv' ELSE pi.media_type END)
                LEFT JOIN local_files lf
                  ON lf.media_type = t.media_type
                 AND lf.tmdb_id = t.tmdb_id
                 AND lf.section_id = pi.section_id
                WHERE {tab_where}
                  AND lf.file_path IS NULL
                  AND t.upstream_source != 'plex_orphan'
            """).fetchall()
            for r in rows:
                # _enqueue_download dedupes per-section internally
                enqueued += _enqueue_download(
                    conn, media_type=r["media_type"], tmdb_id=r["tmdb_id"],
                    reason="bulk_missing",
                )
        log_event(db, level="INFO", component="api",
                  message=f"Bulk download-missing by {request.state.user}",
                  detail={"tab": tab, "fourk": fourk, "enqueued": enqueued})
        return {"ok": True, "enqueued": enqueued, "tab": tab, "fourk": fourk}

    @app.post("/api/plex_items/{rating_key}/adopt-sidecar")
    async def api_adopt_sidecar(
        request: Request, rating_key: str,
        db: Path = Depends(get_db_path),
    ):
        """v1.10.9: take ownership of an existing theme.mp3 sidecar at a
        Plex item's folder. Hardlinks the file into motif's themes_dir
        and records local_files + placement rows so the row flips from
        unmanaged-sidecar (M) to motif-adopted (A).

        Uses adopt_folder() which reuses _do_adopt for the actual file
        and DB work (same path the /scans page uses for orphan_resolvable
        findings) — but no scan_findings row is required.
        """
        _require_admin(request)
        if not settings.is_paths_ready():
            raise HTTPException(status_code=409,
                                detail="themes_dir not configured; visit /settings")
        with get_conn(db) as conn:
            pi = conn.execute(
                "SELECT * FROM plex_items WHERE rating_key = ?",
                (rating_key,),
            ).fetchone()
            if pi is None:
                raise HTTPException(status_code=404,
                                    detail="plex_items row not found")
            if not pi["folder_path"]:
                raise HTTPException(status_code=409,
                                    detail="Plex item has no folder path")
            if not pi["local_theme_file"]:
                raise HTTPException(status_code=409,
                                    detail="no theme.mp3 sidecar at this folder")
        from ..core.adopt import adopt_folder, AdoptError
        media_type = "tv" if pi["media_type"] == "show" else "movie"
        try:
            outcome = await run_in_threadpool(
                adopt_folder, db,
                settings=settings,
                folder_path=pi["folder_path"],
                media_type=media_type,
                title=pi["title"],
                year=str(pi["year"] or ""),
                section_id=pi["section_id"],
                imdb_id=pi["guid_imdb"],
                tmdb_id=int(pi["guid_tmdb"]) if pi["guid_tmdb"] else None,
                decided_by=request.state.principal.username,
            )
        except AdoptError as e:
            raise HTTPException(status_code=409, detail=str(e))
        return {"ok": True, "outcome": outcome}

    @app.post("/api/plex_items/{rating_key}/replace-with-themerrdb")
    async def api_replace_with_themerrdb(
        request: Request, rating_key: str,
        db: Path = Depends(get_db_path),
    ):
        """v1.10.9: sidecar-only Plex row whose tmdb_id is tracked by
        ThemerrDB — fetch motif's authoritative download and overwrite
        the existing sidecar. Cancels any in-flight jobs for the item
        and enqueues a fresh download with force_place=true so the
        place worker overwrites without the plex_has_theme guard.
        """
        _require_admin(request)
        with get_conn(db) as conn:
            pi = conn.execute(
                "SELECT * FROM plex_items WHERE rating_key = ?",
                (rating_key,),
            ).fetchone()
            if pi is None:
                raise HTTPException(status_code=404,
                                    detail="plex_items row not found")
            if not pi["guid_tmdb"]:
                raise HTTPException(status_code=409,
                                    detail="Plex item has no tmdb GUID")
        from ..core.adopt import replace_with_themerrdb, AdoptError
        media_type = "tv" if pi["media_type"] == "show" else "movie"
        try:
            outcome = await run_in_threadpool(
                replace_with_themerrdb, db,
                media_type=media_type,
                tmdb_id=int(pi["guid_tmdb"]),
                decided_by=request.state.principal.username,
                section_id=pi["section_id"],
            )
        except AdoptError as e:
            raise HTTPException(status_code=409, detail=str(e))
        return {"ok": True, "outcome": outcome}

    @app.post("/api/plex_items/{rating_key}/upload-theme")
    async def api_upload_theme(
        request: Request, rating_key: str,
        db: Path = Depends(get_db_path),
    ):
        """Manual MP3 upload for a Plex item that ThemerrDB doesn't cover.
        Creates a plex_orphan themes row if one doesn't exist for this Plex
        item, writes the file to the canonical layout, and enqueues placement.
        """
        _require_admin(request)
        if not settings.is_paths_ready():
            raise HTTPException(status_code=409, detail="themes_dir not configured; visit /settings")

        form = await request.form()
        upload = form.get("file")
        if upload is None or not hasattr(upload, "read"):
            raise HTTPException(status_code=400, detail="multipart 'file' required")
        # Read the upload up-front (simpler than streaming for typical theme sizes)
        data = await upload.read()
        if not data:
            raise HTTPException(status_code=400, detail="empty file")
        if len(data) > 50 * 1024 * 1024:
            raise HTTPException(status_code=413, detail="file > 50 MiB")

        # Look up the Plex item
        with get_conn(db) as conn:
            pi = conn.execute(
                "SELECT * FROM plex_items WHERE rating_key = ?",
                (rating_key,),
            ).fetchone()
            if pi is None:
                raise HTTPException(status_code=404,
                                    detail="plex_items row not found; refresh /libraries")
            theme_media_type = "tv" if pi["media_type"] == "show" else "movie"
            # Match an existing themes row by tmdb GUID, else allocate orphan
            theme = None
            if pi["guid_tmdb"]:
                theme = conn.execute(
                    "SELECT * FROM themes WHERE media_type = ? AND tmdb_id = ?",
                    (theme_media_type, pi["guid_tmdb"]),
                ).fetchone()
            tmdb_id: int
            theme_id_pk: int
            if theme is None:
                # Allocate synthetic negative tmdb_id (consistent with orphan adopt)
                row = conn.execute(
                    "SELECT MIN(tmdb_id) AS lo FROM themes "
                    "WHERE media_type = ? AND tmdb_id < 0",
                    (theme_media_type,),
                ).fetchone()
                min_tmdb = row["lo"] if row and row["lo"] is not None else 0
                tmdb_id = min(min_tmdb, 0) - 1
                cur = conn.execute(
                    """INSERT INTO themes
                         (media_type, tmdb_id, imdb_id, title, year,
                          upstream_source, last_seen_sync_at, first_seen_sync_at)
                       VALUES (?, ?, ?, ?, ?, 'plex_orphan', ?, ?)""",
                    (theme_media_type, tmdb_id, pi["guid_imdb"],
                     pi["title"], pi["year"], now_iso(), now_iso()),
                )
                theme_id_pk = cur.lastrowid
            else:
                tmdb_id = theme["tmdb_id"]
                theme_id_pk = theme["id"]
            # v1.11.64: stamp pi.theme_id immediately. See manual-url
            # endpoint for the long-form rationale; without this the
            # library JOIN doesn't pick up the new orphan + uploaded
            # file until the next plex_enum runs resolve_theme_ids.
            conn.execute(
                "UPDATE plex_items SET theme_id = ? WHERE rating_key = ?",
                (theme_id_pk, rating_key),
            )

        # v1.11.0: write the upload under the section's themes_subdir.
        from ..core.canonical import canonical_theme_subdir
        section_id = pi["section_id"]
        with get_conn(db) as conn:
            sec = conn.execute(
                "SELECT themes_subdir FROM plex_sections WHERE section_id = ?",
                (section_id,),
            ).fetchone()
        if sec is None or not sec["themes_subdir"]:
            raise HTTPException(status_code=409,
                                detail=f"section {section_id} has no themes_subdir")
        media_root = settings.section_themes_dir_by_subdir(sec["themes_subdir"])
        out_dir = media_root / canonical_theme_subdir(pi["title"], pi["year"])
        out_dir.mkdir(parents=True, exist_ok=True)
        target = out_dir / "theme.mp3"

        # v1.11.99: detect existing placement → mismatch flow.
        # If a placement row exists for this (mt, tmdb, section) the
        # canonical and the placement file are likely hardlinked to
        # the same inode. Writing new bytes via target.write_bytes()
        # would mutate that shared inode and silently overwrite the
        # placement too — defeating the user-visible distinction
        # between "user uploaded new content" and "Plex is now
        # playing the new content". Break the hardlink first
        # (target.unlink()) so the placement file keeps its original
        # inode, then write the new canonical content into a fresh
        # inode at the same path.
        with get_conn(db) as conn:
            existing_placement = conn.execute(
                """SELECT 1 FROM placements
                    WHERE media_type = ? AND tmdb_id = ? AND section_id = ?""",
                (theme_media_type, tmdb_id, section_id),
            ).fetchone()
        is_mismatch = bool(existing_placement)
        if is_mismatch:
            try:
                if target.exists():
                    target.unlink()
            except OSError as e:
                log.warning("upload: could not unlink canonical for mismatch: %s", e)
        target.write_bytes(data)

        # Compute sha256 + insert local_files (per-section)
        import hashlib
        sha = hashlib.sha256(data).hexdigest()
        rel_path = str(target.relative_to(settings.themes_dir))
        mismatch_value = "pending" if is_mismatch else None

        with get_conn(db) as conn:
            conn.execute(
                """INSERT INTO local_files
                     (media_type, tmdb_id, section_id, file_path, file_sha256,
                      file_size, downloaded_at, source_video_id, provenance,
                      source_kind, mismatch_state)
                   VALUES (?, ?, ?, ?, ?, ?, ?, '', 'manual', 'upload', ?)
                   ON CONFLICT(media_type, tmdb_id, section_id) DO UPDATE SET
                       file_path = excluded.file_path,
                       file_sha256 = excluded.file_sha256,
                       file_size = excluded.file_size,
                       downloaded_at = excluded.downloaded_at,
                       provenance = excluded.provenance,
                       source_kind = excluded.source_kind,
                       mismatch_state = excluded.mismatch_state""",
                (theme_media_type, tmdb_id, section_id, rel_path, sha, len(data),
                 now_iso(), mismatch_value),
            )
            # v1.10.50: implicit ack on a manual upload — the user is
            # routing around the broken TDB URL. Stamp acked_at so the
            # ! glyph + FAILURES filter exclude the row, but keep
            # failure_kind so the TDB pill stays red.
            conn.execute(
                "UPDATE themes SET failure_acked_at = ? "
                "WHERE media_type = ? AND tmdb_id = ? AND failure_kind IS NOT NULL",
                (now_iso(), theme_media_type, tmdb_id),
            )
            # v1.11.99: only enqueue placement on the no-mismatch path.
            # Mismatch rows wait for explicit PUSH TO PLEX / ADOPT FROM
            # PLEX / KEEP MISMATCH from /pending or the row's PLACE menu.
            if not is_mismatch:
                conn.execute(
                    "INSERT INTO jobs (job_type, media_type, tmdb_id, section_id, "
                    "                  payload, status, created_at, next_run_at) "
                    "VALUES ('place', ?, ?, ?, '{}', 'pending', ?, ?)",
                    (theme_media_type, tmdb_id, section_id, now_iso(), now_iso()),
                )

        log_event(db, level="INFO", component="api",
                  media_type=theme_media_type, tmdb_id=tmdb_id,
                  section_id=section_id,
                  message=f"Manual upload by {request.state.user}: {len(data)} bytes",
                  detail={"rating_key": rating_key, "title": pi["title"]})
        return {"ok": True, "media_type": theme_media_type, "tmdb_id": tmdb_id,
                "file_path": rel_path, "size": len(data)}

    @app.post("/api/plex_items/{rating_key}/manual-url")
    async def api_manual_url(
        request: Request, rating_key: str,
        db: Path = Depends(get_db_path),
    ):
        """Assign a YouTube URL to a Plex item as a manual theme source.

        Creates a plex_orphan themes row if no themes row exists for this
        Plex item (matched via guid_tmdb), writes a user_overrides row, and
        enqueues a download. The URL becomes the authoritative source for
        the item — ThemerrDB syncs won't overwrite it (per v1.5.4 sync
        guard).
        """
        _require_admin(request)
        try:
            body = await request.json()
        except Exception:
            raise HTTPException(status_code=400, detail="invalid JSON body")
        url = (body or {}).get("youtube_url", "")
        if not url:
            raise HTTPException(status_code=400, detail="youtube_url required")
        vid = extract_video_id(url)
        if not vid:
            raise HTTPException(status_code=400, detail="invalid YouTube URL")
        canonical_url = f"https://www.youtube.com/watch?v={vid}"

        with get_conn(db) as conn:
            pi = conn.execute(
                "SELECT * FROM plex_items WHERE rating_key = ?",
                (rating_key,),
            ).fetchone()
            if pi is None:
                raise HTTPException(status_code=404,
                                    detail="plex_items row not found; refresh from plex")
            theme_media_type = "tv" if pi["media_type"] == "show" else "movie"
            theme = None
            if pi["guid_tmdb"]:
                theme = conn.execute(
                    "SELECT * FROM themes WHERE media_type = ? AND tmdb_id = ?",
                    (theme_media_type, pi["guid_tmdb"]),
                ).fetchone()
            if theme is None:
                row = conn.execute(
                    "SELECT MIN(tmdb_id) AS lo FROM themes "
                    "WHERE media_type = ? AND tmdb_id < 0",
                    (theme_media_type,),
                ).fetchone()
                min_tmdb = row["lo"] if row and row["lo"] is not None else 0
                tmdb_id = min(min_tmdb, 0) - 1
                cur = conn.execute(
                    """INSERT INTO themes
                         (media_type, tmdb_id, imdb_id, title, year,
                          upstream_source, last_seen_sync_at, first_seen_sync_at)
                       VALUES (?, ?, ?, ?, ?, 'plex_orphan', ?, ?)""",
                    (theme_media_type, tmdb_id, pi["guid_imdb"],
                     pi["title"], pi["year"], now_iso(), now_iso()),
                )
                theme_id_pk = cur.lastrowid
            else:
                tmdb_id = theme["tmdb_id"]
                theme_id_pk = theme["id"]

            # v1.11.64: stamp pi.theme_id immediately so the library JOIN
            # picks up the new orphan on the very next /api/library
            # refresh — the lib query joins through pi.theme_id (the
            # v1.11.26 denormalized cache) and won't see local_files /
            # placements rows under a synthetic-negative tmdb_id until
            # the next plex_enum's resolve_theme_ids re-stamps the
            # column. Same fix v1.11.43 added for adopt; missed here
            # for SET URL / UPLOAD MP3, so manual URL + upload on
            # P-source rows looked like the action did nothing.
            conn.execute(
                "UPDATE plex_items SET theme_id = ? WHERE rating_key = ?",
                (theme_id_pk, rating_key),
            )

            # v1.12.37: snapshot the prior canonical URL into
            # themes.previous_youtube_url BEFORE the override
            # upsert overwrites the current state. Lets REVERT
            # round-trip back to the URL that was active before
            # this SET URL call.
            # v1.12.105: scope the capture to the row's section.
            # Pre-fix the call passed no section_id so the capture
            # read user_overrides via ORDER BY section_id LIMIT 1
            # (right URL by accident if only one section owned the
            # title) but always WROTE to section_id='' — the
            # section-scoped previous_urls row stayed stale forever.
            # User saw "previous=oldest" instead of "previous=URL
            # I just replaced". Library SQL prefers pv_sec over
            # pv_global so the global-fallback write was invisible.
            section_id_for_override = pi["section_id"] or ""
            _capture_previous_url(conn, theme_media_type, tmdb_id,
                                  section_id=section_id_for_override)

            # v1.12.72: SET URL writes a per-section override scoped
            # to the rating_key's plex_items.section_id. Pre-fix, all
            # SET URL writes overwrote the same global (media_type,
            # tmdb_id) row across every section that owned the title
            # — users with separately-themed editions (4K + standard)
            # couldn't pin different sources per edition. PK is now
            # (media_type, tmdb_id, section_id) so different sections
            # coexist; ON CONFLICT clause updates only the matching
            # section's row, leaving sibling-section overrides
            # untouched.
            # section_id_for_override defined above (v1.12.105).
            prior_override = conn.execute(
                "SELECT youtube_url FROM user_overrides "
                "WHERE media_type = ? AND tmdb_id = ? AND section_id = ?",
                (theme_media_type, tmdb_id, section_id_for_override),
            ).fetchone()
            conn.execute(
                """INSERT INTO user_overrides (media_type, tmdb_id, youtube_url,
                                               set_at, set_by, note,
                                               section_id)
                   VALUES (?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(media_type, tmdb_id, section_id) DO UPDATE SET
                       youtube_url = excluded.youtube_url,
                       set_at = excluded.set_at,
                       set_by = excluded.set_by,
                       note = excluded.note""",
                (theme_media_type, tmdb_id, canonical_url, now_iso(),
                 request.state.user, f"manual url for plex rk={rating_key}",
                 section_id_for_override),
            )
            _record_audit(
                conn, actor=request.state.user, action="set_url",
                media_type=theme_media_type, tmdb_id=tmdb_id,
                section_id=section_id_for_override,
                details={
                    "old_url": prior_override["youtube_url"] if prior_override else None,
                    "new_url": canonical_url,
                    "rating_key": rating_key,
                },
            )
            # v1.12.62: eager synthetic urls_match detection. If the
            # URL the user just set exactly matches the row's current
            # TDB URL, surface the "convert U → T" prompt immediately
            # — pre-fix the user had to wait until the next sync for
            # the v1.12.53 sweep to write the synthetic pending_update,
            # which felt like the action menu had no path to swap to T.
            # v1.12.81: drop stale pending_updates (any decision) for
            # this title BEFORE re-inserting. Pre-fix the original
            # NOT EXISTS guard blocked insertion whenever ANY row
            # existed, so the SET URL → ACCEPT (urls_match) → SET URL
            # again with the same URL flow silently produced a U row
            # with no re-prompt — the prior decision='accepted' row
            # vetoed the synthetic insert. Treat each fresh SET URL
            # match as a clean prompt: clear prior decisions and
            # insert a new pending row. Still idempotent because the
            # SET URL writes are user-initiated, not automatic.
            existing_theme_yt = (theme["youtube_url"]
                                 if theme and "youtube_url" in theme.keys()
                                 else None)
            if (existing_theme_yt
                    and canonical_url
                    and existing_theme_yt.strip() == canonical_url.strip()):
                # v1.12.99: schema v31 added section_id to the PK.
                # SET URL is per-section (rk-driven), so the eager
                # urls_match insert lands on the row's section_id —
                # not the title-global '' row. Other sections see
                # whatever was already there (or nothing).
                conn.execute(
                    "DELETE FROM pending_updates "
                    "WHERE media_type = ? AND tmdb_id = ? AND section_id = ?",
                    (theme_media_type, tmdb_id, section_id_for_override),
                )
                conn.execute(
                    """
                    INSERT INTO pending_updates (
                        media_type, tmdb_id, section_id,
                        old_video_id, new_video_id,
                        old_youtube_url, new_youtube_url,
                        upstream_edited_at, detected_at, decision, kind
                    )
                    SELECT t.media_type, t.tmdb_id, ?,
                           t.youtube_video_id, t.youtube_video_id,
                           NULL, t.youtube_url,
                           t.youtube_edited_at, ?, 'pending', 'urls_match'
                      FROM themes t
                     WHERE t.media_type = ?
                       AND t.tmdb_id = ?
                    """,
                    (section_id_for_override, now_iso(), theme_media_type, tmdb_id),
                )
            # v1.10.50: implicit ack on manual URL — same reasoning as
            # upload-theme. The user has provided a working source, so
            # stop alerting; keep failure_kind so the TDB pill stays red.
            conn.execute(
                "UPDATE themes SET failure_acked_at = ? "
                "WHERE media_type = ? AND tmdb_id = ? AND failure_kind IS NOT NULL",
                (now_iso(), theme_media_type, tmdb_id),
            )
            # v1.11.0: cancel any in-flight downloads across sections.
            conn.execute(
                "UPDATE jobs SET status = 'cancelled', finished_at = ? "
                "WHERE job_type = 'download' AND media_type = ? AND tmdb_id = ? "
                "  AND status IN ('pending','failed')",
                (now_iso(), theme_media_type, tmdb_id),
            )
            # v1.11.15: enqueue the download directly with the rating_key's
            # section. _enqueue_download looks up sections via plex_items.
            # guid_tmdb, which never matches for plex_orphan rows (tmdb_id
            # is a synthetic negative). Pre-fix the manual-url flow on an
            # orphan logged 'no included Plex section owns (...)' and
            # silently skipped — the URL persisted but no download fired.
            section_id = pi["section_id"]
            # v1.12.81: SET URL now auto-places over the existing
            # placement (force_place=True in the payload, no
            # user_initiated_mismatch). Pre-fix the v1.11.99 mismatch
            # flow preserved the old placement file and marked
            # mismatch_state='pending' so the user could choose
            # between PUSH / ADOPT FROM PLEX / KEEP MISMATCH — but
            # per user feedback, clicking SET URL is already an
            # explicit "use this URL" decision; landing in a yellow
            # mismatch state and needing a follow-up PLACE action
            # felt like the action didn't take. Force-place mirrors
            # the /override endpoint's behavior so the two SET URL
            # paths produce the same outcome. UPLOAD MP3 still uses
            # the mismatch flow since uploaded content is locally
            # crafted — there the "preserve until user confirms
            # overwrite" gate matches user intent better.
            #
            # Direct INSERT (rather than _enqueue_download) is
            # preserved per v1.11.15 — _enqueue_download looks up
            # sections via plex_items.guid_tmdb which doesn't match
            # for plex_orphan rows (synthetic-negative tmdb_id),
            # so manual-url on an orphan would silently skip.
            payload = {"reason": "manual_url", "force_place": True}
            conn.execute(
                "INSERT INTO jobs (job_type, media_type, tmdb_id, section_id, "
                "                  payload, status, created_at, next_run_at) "
                "VALUES ('download', ?, ?, ?, ?, 'pending', ?, ?)",
                (theme_media_type, tmdb_id, section_id,
                 json.dumps(payload),
                 now_iso(), now_iso()),
            )

        log_event(db, level="INFO", component="api",
                  media_type=theme_media_type, tmdb_id=tmdb_id,
                  section_id=section_id,
                  message=f"Manual URL set by {request.state.user}: {canonical_url}",
                  detail={"rating_key": rating_key, "title": pi["title"]})
        return {"ok": True, "media_type": theme_media_type, "tmdb_id": tmdb_id,
                "youtube_url": canonical_url}

    @app.post("/api/libraries/{section_id}/refresh")
    async def api_libraries_section_refresh(
        request: Request, section_id: str,
        db: Path = Depends(get_db_path),
    ):
        """Re-enumerate a single Plex section. Lighter than the global plex_enum
        (which walks every managed section). Useful when a particular library
        had recent additions and the user wants the unified view to reflect
        them without waiting for the next sync."""
        _require_admin(request)
        with get_conn(db) as conn:
            row = conn.execute(
                "SELECT type, included FROM plex_sections WHERE section_id = ?",
                (section_id,),
            ).fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail="section not found")
            if not row["included"]:
                raise HTTPException(status_code=409,
                                    detail="section is not managed (toggle MGD first)")
            existing = conn.execute(
                "SELECT id FROM jobs WHERE job_type = 'plex_enum' "
                "AND status IN ('pending','running')"
            ).fetchone()
            if existing:
                return {"ok": True, "job_id": existing["id"], "already_queued": True}
            cur = conn.execute(
                "INSERT INTO jobs (job_type, payload, status, created_at, next_run_at) "
                "VALUES ('plex_enum', ?, 'pending', ?, ?)",
                (json.dumps({"section_id": section_id}), now_iso(), now_iso()),
            )
        log_event(db, level="INFO", component="api",
                  message=f"Per-section refresh ({section_id}) by {request.state.user}")
        return {"ok": True, "job_id": cur.lastrowid}

    @app.post("/api/library/refresh")
    async def api_library_refresh(request: Request, db: Path = Depends(get_db_path)):
        """Manually enqueue a plex_enum job (refresh the unified browse cache).

        v1.10.6: optional body {"tab": "movies|tv|anime", "fourk": bool}
        narrows the refresh to sections that back the requested tab so the
        REFRESH FROM PLEX button on /movies?fourk=1 only re-enumerates
        movie+4k sections. Body absent → full refresh (legacy behavior).
        """
        _require_admin(request)
        try:
            body = await request.json() if (await request.body()) else {}
        except Exception:
            body = {}
        tab = body.get("tab")
        fourk = bool(body.get("fourk"))

        with get_conn(db) as conn:
            existing = conn.execute(
                "SELECT id FROM jobs WHERE job_type = 'plex_enum' "
                "AND status IN ('pending','running')"
            ).fetchone()
            if existing:
                return {"ok": True, "job_id": existing["id"], "already_queued": True}

            if tab in ("movies", "tv", "anime"):
                # Map tab → section predicate, mirroring the /api/library
                # tab_where logic. Anime is purely flag-driven; Movies/TV
                # exclude anime-flagged sections.
                if tab == "movies":
                    sec_where = "type = 'movie' AND is_anime = 0"
                elif tab == "tv":
                    sec_where = "type = 'show' AND is_anime = 0"
                else:
                    sec_where = "is_anime = 1"
                # v1.12.1: try the requested variant first, then fall
                # back to the other one. Pre-fix a /movies tab with
                # only 4K Movies enabled (or only standard) returned
                # 'no managed sections' silently when REFRESH FROM
                # PLEX shipped fourk=false, leaving the user staring
                # at an empty page with a button that did nothing.
                # Now we route to whichever variant actually exists.
                requested_4k = 1 if fourk else 0
                fallback_4k = 0 if fourk else 1
                sections = conn.execute(
                    f"SELECT section_id FROM plex_sections "
                    f"WHERE included = 1 AND {sec_where} "
                    f"  AND is_4k = {requested_4k}"
                ).fetchall()
                used_fallback = False
                if not sections:
                    sections = conn.execute(
                        f"SELECT section_id FROM plex_sections "
                        f"WHERE included = 1 AND {sec_where} "
                        f"  AND is_4k = {fallback_4k}"
                    ).fetchall()
                    used_fallback = bool(sections)
                if not sections:
                    return {"ok": True, "enqueued": 0,
                            "note": f"no managed Plex sections match the {tab} tab"}
                effective_fourk = (fallback_4k == 1) if used_fallback else fourk
                ids: list[int] = []
                for s in sections:
                    cur = conn.execute(
                        "INSERT INTO jobs (job_type, payload, status, created_at, next_run_at) "
                        "VALUES ('plex_enum', ?, 'pending', ?, ?)",
                        (json.dumps({"section_id": s["section_id"],
                                     "scope": f"{tab}{'-4k' if effective_fourk else ''}"}),
                         now_iso(), now_iso()),
                    )
                    ids.append(cur.lastrowid)
                return {"ok": True, "enqueued": len(ids), "job_ids": ids,
                        "scope": f"{tab}{'-4k' if effective_fourk else ''}",
                        "fallback_used": used_fallback}

            # Legacy global refresh (no tab specified).
            cur = conn.execute(
                "INSERT INTO jobs (job_type, payload, status, created_at, next_run_at) "
                "VALUES ('plex_enum', '{}', 'pending', ?, ?)",
                (now_iso(), now_iso()),
            )
            return {"ok": True, "job_id": cur.lastrowid}

    @app.get("/api/items")
    async def api_items(
        media_type: MediaType = Query(...),
        q: str = Query(""),
        status: str = Query("all", pattern="^(all|downloaded|missing|placed|unplaced|failed|failures|updates|manual)$"),
        page: int = Query(1, ge=1),
        per_page: int = Query(50, ge=1, le=200),
        db: Path = Depends(get_db_path),
    ):
        offset = (page - 1) * per_page
        params: list = [media_type]
        where = ["t.media_type = ?"]
        if q:
            where.append("(t.title LIKE ? OR t.original_title LIKE ? OR t.imdb_id = ?)")
            params.extend([f"%{q}%", f"%{q}%", q])

        join = ""
        if status == "downloaded":
            join = "INNER JOIN local_files lf ON lf.media_type = t.media_type AND lf.tmdb_id = t.tmdb_id"
        elif status == "missing":
            join = "LEFT JOIN local_files lf ON lf.media_type = t.media_type AND lf.tmdb_id = t.tmdb_id"
            where.append("lf.tmdb_id IS NULL")
        elif status == "placed":
            join = "INNER JOIN placements p ON p.media_type = t.media_type AND p.tmdb_id = t.tmdb_id"
        elif status == "unplaced":
            join = ("LEFT JOIN placements p ON p.media_type = t.media_type AND p.tmdb_id = t.tmdb_id "
                    "INNER JOIN local_files lf2 ON lf2.media_type = t.media_type AND lf2.tmdb_id = t.tmdb_id")
            where.append("p.media_folder IS NULL")
        elif status == "failed":
            # Legacy: failed JOB. We keep this but recommend "failures" below.
            join = ("INNER JOIN jobs j ON j.media_type = t.media_type AND j.tmdb_id = t.tmdb_id "
                    "AND j.status = 'failed'")
        elif status == "failures":
            # Items where the last download attempt failed; failure_kind tells us why
            where.append("t.failure_kind IS NOT NULL")
        elif status == "updates":
            # Items with a pending upstream update awaiting decision
            join = ("INNER JOIN pending_updates pu "
                    "ON pu.media_type = t.media_type AND pu.tmdb_id = t.tmdb_id "
                    "AND pu.decision = 'pending'")
        elif status == "manual":
            # Items whose downloaded file came from a manual override
            join = ("INNER JOIN local_files lf ON lf.media_type = t.media_type "
                    "AND lf.tmdb_id = t.tmdb_id AND lf.provenance = 'manual'")

        where_clause = " AND ".join(where)
        sql = f"""
            SELECT DISTINCT t.media_type, t.tmdb_id, t.imdb_id, t.title, t.year,
                   t.youtube_url, t.youtube_video_id, t.failure_kind, t.failure_message,
                   t.upstream_source
            FROM themes t {join}
            WHERE {where_clause}
            ORDER BY t.title COLLATE NOCASE
            LIMIT ? OFFSET ?
        """
        count_sql = f"SELECT COUNT(DISTINCT t.tmdb_id) FROM themes t {join} WHERE {where_clause}"

        with get_conn(db) as conn:
            total = conn.execute(count_sql, params).fetchone()[0]
            params2 = params + [per_page, offset]
            rows = conn.execute(sql, params2).fetchall()
            items = []
            for r in rows:
                lf = conn.execute(
                    "SELECT downloaded_at, source_video_id, provenance FROM local_files "
                    "WHERE media_type = ? AND tmdb_id = ?",
                    (r["media_type"], r["tmdb_id"]),
                ).fetchone()
                pl = conn.execute(
                    "SELECT media_folder, placed_at, placement_kind, provenance "
                    "FROM placements "
                    "WHERE media_type = ? AND tmdb_id = ? LIMIT 1",
                    (r["media_type"], r["tmdb_id"]),
                ).fetchone()
                ovr = conn.execute(
                    "SELECT youtube_url, set_at FROM user_overrides "
                    "WHERE media_type = ? AND tmdb_id = ?",
                    (r["media_type"], r["tmdb_id"]),
                ).fetchone()
                pending_update = conn.execute(
                    "SELECT new_youtube_url, new_video_id, detected_at "
                    "FROM pending_updates "
                    "WHERE media_type = ? AND tmdb_id = ? AND decision = 'pending'",
                    (r["media_type"], r["tmdb_id"]),
                ).fetchone()
                items.append({
                    **dict(r),
                    "downloaded": dict(lf) if lf else None,
                    "placed": dict(pl) if pl else None,
                    "override": dict(ovr) if ovr else None,
                    "pending_update": dict(pending_update) if pending_update else None,
                })

        return {"total": total, "page": page, "per_page": per_page, "items": items}

    @app.get("/api/items/{media_type}/{tmdb_id}")
    async def api_item(
        media_type: MediaType, tmdb_id: int,
        # v1.12.72: optional section_id picks which section's
        # override to surface in the legacy `override` field.
        # Without it, the response uses the per-title query that
        # may return any matching row when multiple per-section
        # overrides exist. Pass section_id from row clicks so the
        # INFO card's "currently applied" matches what the worker
        # would use for that section.
        section_id: str | None = Query(None),
        db: Path = Depends(get_db_path),
    ):
        with get_conn(db) as conn:
            t = conn.execute(
                "SELECT * FROM themes WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            ).fetchone()
            if t is None:
                raise HTTPException(status_code=404, detail="not found")
            # v1.11.0: per-section local_files — return the list so the
            # UI can render one row per section. legacy `local_file`
            # field still emitted (first row, or null) for older callers.
            # v1.12.81: scope local_files + placements to the requested
            # section_id when provided. Pre-fix opening INFO on a
            # standard-section row whose 4K sibling still had a
            # canonical / placement showed the 4K paths in the
            # "downloaded" / "placed in" rows because the queries
            # returned all sections' files and the legacy `local_file`
            # field grabbed local_files[0]. Filtering here keeps the
            # card honest — the user sees exactly what's at this row's
            # section. Without section_id (legacy callers) the
            # unfiltered query stays.
            if section_id:
                local_files = conn.execute(
                    "SELECT * FROM local_files "
                    "WHERE media_type = ? AND tmdb_id = ? AND section_id = ?",
                    (media_type, tmdb_id, section_id),
                ).fetchall()
                placements = conn.execute(
                    "SELECT * FROM placements "
                    "WHERE media_type = ? AND tmdb_id = ? AND section_id = ? "
                    "ORDER BY media_folder",
                    (media_type, tmdb_id, section_id),
                ).fetchall()
            else:
                local_files = conn.execute(
                    "SELECT * FROM local_files WHERE media_type = ? AND tmdb_id = ? "
                    "ORDER BY section_id",
                    (media_type, tmdb_id),
                ).fetchall()
                placements = conn.execute(
                    "SELECT * FROM placements WHERE media_type = ? AND tmdb_id = ? "
                    "ORDER BY section_id, media_folder",
                    (media_type, tmdb_id),
                ).fetchall()
            # v1.12.72: prefer the section-specific override row when
            # section_id is provided; fall back to global ('') row.
            # v1.12.81: dropped the "any-section LIMIT 1" final
            # fallback when section_id is specified. Pre-fix, opening
            # INFO on a 4K row that has no override of its own surfaced
            # the standard section's override (or any sibling's),
            # making "currently applied" render in violet on a row
            # whose canonical was sourced from TDB. The worker never
            # cross-pollinates either — it only reads the row's own
            # section_id and the '' global. Mirror that here so
            # "currently applied" reflects what's actually applied.
            # The any-section fallback survives only when section_id
            # is NULL (legacy callers without row context).
            ovr = None
            if section_id:
                ovr = conn.execute(
                    "SELECT * FROM user_overrides "
                    "WHERE media_type = ? AND tmdb_id = ? AND section_id = ?",
                    (media_type, tmdb_id, section_id),
                ).fetchone()
                if ovr is None:
                    ovr = conn.execute(
                        "SELECT * FROM user_overrides "
                        "WHERE media_type = ? AND tmdb_id = ? AND section_id = ''",
                        (media_type, tmdb_id),
                    ).fetchone()
            else:
                ovr = conn.execute(
                    "SELECT * FROM user_overrides "
                    "WHERE media_type = ? AND tmdb_id = ? AND section_id = ''",
                    (media_type, tmdb_id),
                ).fetchone()
                if ovr is None:
                    ovr = conn.execute(
                        "SELECT * FROM user_overrides "
                        "WHERE media_type = ? AND tmdb_id = ? "
                        "ORDER BY section_id LIMIT 1",
                        (media_type, tmdb_id),
                    ).fetchone()
            # Also surface the full per-section override list so the
            # frontend can show all overrides at once if it wants.
            all_overrides = conn.execute(
                "SELECT * FROM user_overrides "
                "WHERE media_type = ? AND tmdb_id = ? "
                "ORDER BY section_id",
                (media_type, tmdb_id),
            ).fetchall()
            # v1.12.34 / v1.12.37: include the pending_updates row
            # so the INFO dialog can render the upstream-update
            # state. The "previous URL" lives on themes.previous_*
            # (not pending_updates) since v1.12.37 generalized
            # REVERT beyond the post-accept-on-U flow.
            pu = conn.execute(
                "SELECT old_youtube_url, new_youtube_url, decision, "
                "       decision_at, decision_by, detected_at "
                "FROM pending_updates WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            ).fetchone()
            # v1.12.84: scope events by section_id when provided, like
            # audit_events. Title-global entries (section_id IS NULL —
            # legacy pre-migration rows + sync/scan events that span
            # every section) stay visible so cross-section context
            # isn't lost. Pre-fix the unfiltered query rendered
            # standard's worker events on the 4K card and vice versa.
            if section_id is not None:
                recent_events = conn.execute(
                    """SELECT * FROM events
                        WHERE media_type = ? AND tmdb_id = ?
                          AND (section_id = ? OR section_id IS NULL)
                        ORDER BY ts DESC LIMIT 25""",
                    (media_type, tmdb_id, section_id),
                ).fetchall()
            else:
                recent_events = conn.execute(
                    "SELECT * FROM events WHERE media_type = ? AND tmdb_id = ? "
                    "ORDER BY ts DESC LIMIT 25",
                    (media_type, tmdb_id),
                ).fetchall()
        local_payloads: list[dict] = []
        for lf in local_files:
            d = dict(lf)
            if settings.is_paths_ready() and d.get("file_path"):
                d["abs_path"] = str(settings.themes_dir / d["file_path"])
            local_payloads.append(d)
        # v1.12.86: surface per-section previous URL (replaces the
        # title-global themes.previous_youtube_url + previous_youtube_kind
        # columns). The INFO card reads previous_url.youtube_url +
        # previous_url.kind to render the "previous url" row with the
        # correct color + suffix.
        with get_conn(db) as conn:
            prev_row = _load_previous_url(
                conn, media_type, tmdb_id, section_id=section_id,
            )
            # v1.12.93: per-row "motif touched this on" timestamps for
            # the INFO card. Distinct from themes.youtube_added_at /
            # youtube_edited_at which track ThemerrDB's record.
            #   motif_added_at  = oldest evidence motif acted on this
            #     row. audit_events covers post-v1.12.80 actions; falls
            #     back to local_files.downloaded_at for older installs.
            #   motif_edited_at = most recent evidence. Same fallback.
            # NULL when motif has never acted (e.g. M sidecar that
            # was never adopted, P-agent never replaced) — frontend
            # renders an em-dash.
            motif_added = None
            motif_edited = None
            if section_id is not None:
                ae = conn.execute(
                    """SELECT MIN(occurred_at) AS first_at,
                              MAX(occurred_at) AS last_at
                         FROM audit_events
                        WHERE media_type = ? AND tmdb_id = ?
                          AND (section_id = ? OR section_id IS NULL)""",
                    (media_type, tmdb_id, section_id),
                ).fetchone()
            else:
                ae = conn.execute(
                    """SELECT MIN(occurred_at) AS first_at,
                              MAX(occurred_at) AS last_at
                         FROM audit_events
                        WHERE media_type = ? AND tmdb_id = ?""",
                    (media_type, tmdb_id),
                ).fetchone()
            if ae:
                motif_added = ae["first_at"]
                motif_edited = ae["last_at"]
            # local_files.downloaded_at fallback when no audit row
            # exists (pre-v1.12.80 installs, or pure-T rows whose
            # initial sync-driven download never wrote an audit
            # entry). Per-section when section_id is provided so
            # the timestamp matches the row the user clicked.
            if not motif_added:
                if section_id is not None:
                    lf_dl = conn.execute(
                        """SELECT MIN(downloaded_at) AS first_at,
                                  MAX(downloaded_at) AS last_at
                             FROM local_files
                            WHERE media_type = ? AND tmdb_id = ?
                              AND section_id = ?""",
                        (media_type, tmdb_id, section_id),
                    ).fetchone()
                else:
                    lf_dl = conn.execute(
                        """SELECT MIN(downloaded_at) AS first_at,
                                  MAX(downloaded_at) AS last_at
                             FROM local_files
                            WHERE media_type = ? AND tmdb_id = ?""",
                        (media_type, tmdb_id),
                    ).fetchone()
                if lf_dl and lf_dl["first_at"]:
                    motif_added = lf_dl["first_at"]
                    motif_edited = lf_dl["last_at"]
        previous_url_payload = (
            {"youtube_url": prev_row["youtube_url"],
             "kind": prev_row["kind"],
             "captured_at": prev_row["captured_at"]}
            if prev_row else None
        )
        return {
            "theme": dict(t),
            "local_file": local_payloads[0] if local_payloads else None,
            "local_files": local_payloads,
            "placements": [dict(p) for p in placements],
            "override": dict(ovr) if ovr else None,
            # v1.12.72: full per-section override list. Frontend
            # can render one row per section in the INFO card if
            # multiple overrides exist; legacy `override` field
            # still surfaces a single representative row for
            # back-compat with consumers that don't read this list.
            "overrides": [dict(o) for o in all_overrides],
            "pending_update": dict(pu) if pu else None,
            "previous_url": previous_url_payload,
            # v1.12.93: motif-side timestamps so the INFO card can
            # show when *motif* first/last acted on the row, distinct
            # from ThemerrDB's record on the theme.
            "motif_added_at": motif_added,
            "motif_edited_at": motif_edited,
            "events": [dict(e) for e in recent_events],
        }

    def _trigger_plex_item_refresh(
        media_type: str, tmdb_id: int,
    ) -> int:
        """v1.10.28: ask Plex to re-run its metadata agent on each
        rating_key tracking this item, so the server-side has_theme
        cache reflects motif having just deleted the file. Returns
        the count of refresh requests fired.

        Without this, after PURGE/DEL on a T row our optimistic
        clear of plex_items.has_theme=0 survived only until the
        next plex_enum, which re-fetches from Plex's API. Plex
        was still reporting theme=true (its metadata cache hadn't
        re-scanned the now-empty folder), so the row came back as
        a phantom P (Plex agent / cloud). A targeted refresh kicks
        Plex to re-evaluate immediately.

        Best-effort — failures (Plex offline, auth issue) log and
        return without raising. The fallback path is the user
        clicking REFRESH FROM PLEX manually.
        """
        if not (settings.plex_enabled and settings.plex_token):
            return 0
        plex_mt = "show" if media_type == "tv" else "movie"
        with get_conn(settings.db_path) as conn:
            rks = conn.execute(
                "SELECT rating_key FROM plex_items "
                "WHERE guid_tmdb = ? AND media_type = ?",
                (tmdb_id, plex_mt),
            ).fetchall()
        if not rks:
            return 0
        cfg = PlexConfig(
            url=settings.plex_url, token=settings.plex_token,
            movie_section=settings.plex_movie_section,
            tv_section=settings.plex_tv_section, enabled=True,
        )
        n = 0
        try:
            with PlexClient(cfg, plus_mode=settings.plus_equiv_mode) as plex:  # type: ignore[arg-type]
                for r in rks:
                    try:
                        if plex.refresh(r["rating_key"]):
                            n += 1
                    except Exception as e:
                        log.debug("plex refresh failed for rk=%s: %s",
                                  r["rating_key"], e)
        except Exception as e:
            log.warning("plex client init failed for refresh: %s", e)
        return n

    # v1.11.47: removed _enqueue_section_refresh_for_item. Each
    # destructive endpoint already optimistically updates
    # plex_items.local_theme_file from the post-mutation disk state,
    # so the confirmatory full-section plex_enum was scanning 10K+
    # items just to re-affirm one row's flag. The single remaining
    # caller (unmanage) was the only user.

    @app.post("/api/items/{media_type}/{tmdb_id}/unplace")
    async def api_unplace_item(
        request: Request, media_type: MediaType, tmdb_id: int,
        # v1.12.77: optional section_id scopes DEL (unplace) to one
        # section. Without it, every section's placement file gets
        # unlinked (legacy fan-out). Mirrors the v1.12.46/.47/.73
        # scoping for ACCEPT UPDATE / REVERT / RE-DOWNLOAD /
        # UNMANAGE.
        section_id: str | None = Query(None),
        db: Path = Depends(get_db_path),
    ):
        """Remove the placement of a theme (theme.mp3 in Plex's folder) but
        keep motif's canonical so the user can REPLACE it back later. The
        Plex side stops playing the motif theme; the file in themes_dir
        stays, allowing one-click re-placement without re-download.

        v1.12.77: when section_id is provided, only that section's
        placement gets unlinked. Sibling sections continue to play
        their motif theme.
        """
        _require_admin(request)
        with get_conn(db) as conn:
            theme = conn.execute(
                "SELECT title FROM themes WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            ).fetchone()
            if theme is None:
                raise HTTPException(status_code=404, detail="theme not found")
            if section_id:
                placements = conn.execute(
                    "SELECT media_folder FROM placements "
                    "WHERE media_type = ? AND tmdb_id = ? AND section_id = ?",
                    (media_type, tmdb_id, section_id),
                ).fetchall()
            else:
                placements = conn.execute(
                    "SELECT media_folder FROM placements "
                    "WHERE media_type = ? AND tmdb_id = ?",
                    (media_type, tmdb_id),
                ).fetchall()
        # Unlink each placement's theme.mp3
        unlinked = 0
        affected_folders: list[str] = []
        for pr in placements:
            try:
                p = Path(pr["media_folder"]) / "theme.mp3"
                if p.is_file():
                    p.unlink()
                    unlinked += 1
                affected_folders.append(pr["media_folder"])
            except OSError as e:
                log.warning("unplace: could not unlink %s: %s", pr["media_folder"], e)
        with get_conn(db) as conn:
            if section_id:
                conn.execute(
                    "DELETE FROM placements "
                    "WHERE media_type = ? AND tmdb_id = ? AND section_id = ?",
                    (media_type, tmdb_id, section_id),
                )
            else:
                conn.execute(
                    "DELETE FROM placements WHERE media_type = ? AND tmdb_id = ?",
                    (media_type, tmdb_id),
                )
            # Optimistically clear plex_items flags so the SRC badge
            # immediately reflects "no theme" instead of going stale-P
            # (Plex's metadata still says theme=present for some time
            # after a file delete; a manual REFRESH FROM PLEX will
            # re-confirm against the live Plex API).
            for folder in affected_folders:
                conn.execute(
                    "UPDATE plex_items SET local_theme_file = 0, has_theme = 0 "
                    "WHERE folder_path = ?",
                    (folder,),
                )
            # v1.10.28: skip the section enum here — see the matching
            # comment in api_forget_item. DEL deletes the Plex-folder
            # file; running plex_enum would re-fetch has_theme=1 from
            # Plex's cached metadata and bring back a phantom P badge.
            # Trigger a per-item Plex refresh below instead so Plex's
            # cache catches up.
        try:
            _trigger_plex_item_refresh(media_type, tmdb_id)
        except Exception as e:
            log.debug("plex item refresh skipped: %s", e)
        with get_conn(db) as conn:
            _record_audit(
                conn, actor=request.state.user, action="unplace",
                media_type=media_type, tmdb_id=tmdb_id,
                section_id=section_id,
                details={"placements_unlinked": unlinked,
                         "placements_total": len(placements)},
            )
        log_event(db, level="INFO", component="api",
                  media_type=media_type, tmdb_id=tmdb_id,
                  section_id=section_id,
                  message=f"Unplaced by {request.state.user}",
                  detail={"title": theme["title"],
                          "placements_unlinked": unlinked,
                          "placements_total": len(placements)})
        return {"ok": True, "placements_unlinked": unlinked}

    @app.post("/api/items/{media_type}/{tmdb_id}/replace")
    async def api_replace_item(
        request: Request, media_type: MediaType, tmdb_id: int,
        db: Path = Depends(get_db_path),
    ):
        """Re-place motif's existing canonical into the Plex media folder.
        Used after a DEL (unplace) when the user wants to push the
        downloaded theme back without re-fetching from YouTube. Force-
        overwrites any sidecar that has appeared in the meantime.
        """
        _require_admin(request)
        with get_conn(db) as conn:
            # v1.11.0: enqueue one place job per section that has a
            # local_files row for this item. Sections without staged
            # content are skipped — re-download is a separate action.
            sections = conn.execute(
                "SELECT section_id FROM local_files "
                "WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            ).fetchall()
            if not sections:
                raise HTTPException(
                    status_code=409,
                    detail="no local file to replace from — re-download first",
                )
            conn.execute(
                """UPDATE jobs SET status = 'cancelled', finished_at = ?
                   WHERE job_type = 'place' AND media_type = ? AND tmdb_id = ?
                     AND status IN ('pending','running')""",
                (now_iso(), media_type, tmdb_id),
            )
            for s in sections:
                conn.execute(
                    """INSERT INTO jobs (job_type, media_type, tmdb_id, section_id,
                                         payload, status, created_at, next_run_at)
                       VALUES ('place', ?, ?, ?,
                               '{"force":true,"reason":"user_replace"}',
                               'pending', ?, ?)""",
                    (media_type, tmdb_id, s["section_id"], now_iso(), now_iso()),
                )
            _record_audit(
                conn, actor=request.state.user,
                action="replace_with_themerrdb",
                media_type=media_type, tmdb_id=tmdb_id,
            )
        log_event(db, level="INFO", component="api",
                  media_type=media_type, tmdb_id=tmdb_id,
                  message=f"Replace requested by {request.state.user}")
        return {"ok": True}

    # v1.11.99: mismatch-resolution endpoints. These complement the
    # existing /replace (PUSH TO PLEX) endpoint above:
    #
    #   PUSH TO PLEX     → /replace            — write canonical to placement
    #   ADOPT FROM PLEX  → /adopt-from-plex    — replace canonical with placement
    #   KEEP MISMATCH    → /keep-mismatch      — flip 'pending' → 'acked';
    #                                            stays in library as DL=amber
    #                                            + LINK=≠ but exits /pending
    #
    # The first two clear mismatch_state on success (place worker
    # handles PUSH; adopt-from-plex sets it directly). KEEP MISMATCH
    # is a state-only flip — no file mutation.

    @app.post("/api/items/{media_type}/{tmdb_id}/adopt-from-plex")
    async def api_adopt_from_plex(
        request: Request, media_type: MediaType, tmdb_id: int,
        db: Path = Depends(get_db_path),
    ):
        """v1.11.99: replace motif's canonical with the file currently at
        the placement (Plex folder) — the inverse of PUSH TO PLEX.

        Use case: user uploaded / set URL on a row that already had a
        placement, decides on review they want to keep what Plex is
        actually playing instead of the new download. We hardlink the
        placement file back into canonical (new inode for canonical,
        same inode as placement → matched again), discarding the
        rejected new download.
        """
        _require_admin(request)
        if not settings.is_paths_ready():
            raise HTTPException(
                status_code=409,
                detail="themes_dir not configured; visit /settings",
            )
        with get_conn(db) as conn:
            rows = conn.execute(
                """SELECT lf.section_id, lf.file_path AS canon_rel,
                          p.media_folder
                     FROM local_files lf
                     JOIN placements p
                       ON p.media_type = lf.media_type
                      AND p.tmdb_id = lf.tmdb_id
                      AND p.section_id = lf.section_id
                    WHERE lf.media_type = ? AND lf.tmdb_id = ?
                      AND lf.mismatch_state IS NOT NULL""",
                (media_type, tmdb_id),
            ).fetchall()
        if not rows:
            raise HTTPException(
                status_code=409,
                detail="no mismatch state to resolve",
            )
        import hashlib
        adopted = 0
        for r in rows:
            placement_file = Path(r["media_folder"]) / "theme.mp3"
            if not placement_file.is_file():
                log.warning(
                    "adopt-from-plex: placement file missing at %s — skipping",
                    placement_file,
                )
                continue
            canon_path = settings.themes_dir / r["canon_rel"]
            try:
                # Break the canonical inode (the rejected new content),
                # then hardlink placement → canonical so they share an
                # inode again (matching state).
                if canon_path.exists():
                    canon_path.unlink()
                canon_path.parent.mkdir(parents=True, exist_ok=True)
                try:
                    canon_path.hardlink_to(placement_file)
                    placement_kind = "hardlink"
                except OSError:
                    # Cross-filesystem — fall back to copy.
                    import shutil
                    shutil.copy2(placement_file, canon_path)
                    placement_kind = "copy"
            except OSError as e:
                log.warning(
                    "adopt-from-plex: failed for %s: %s", placement_file, e,
                )
                continue
            # Re-read the now-canonical file to refresh sha + size,
            # clear mismatch_state, and align placement.placement_kind.
            sha = hashlib.sha256(canon_path.read_bytes()).hexdigest()
            size = canon_path.stat().st_size
            with get_conn(db) as conn:
                conn.execute(
                    """UPDATE local_files SET file_sha256 = ?, file_size = ?,
                                              downloaded_at = ?,
                                              mismatch_state = NULL
                       WHERE media_type = ? AND tmdb_id = ? AND section_id = ?""",
                    (sha, size, now_iso(),
                     media_type, tmdb_id, r["section_id"]),
                )
                conn.execute(
                    """UPDATE placements SET placement_kind = ?
                       WHERE media_type = ? AND tmdb_id = ? AND section_id = ?""",
                    (placement_kind, media_type, tmdb_id, r["section_id"]),
                )
            adopted += 1
        with get_conn(db) as conn:
            _record_audit(
                conn, actor=request.state.user, action="adopt",
                media_type=media_type, tmdb_id=tmdb_id,
                details={"sections_adopted": adopted},
            )
        log_event(db, level="INFO", component="api",
                  media_type=media_type, tmdb_id=tmdb_id,
                  message=f"Adopt from Plex by {request.state.user}",
                  detail={"sections_adopted": adopted})
        return {"ok": True, "sections_adopted": adopted}

    @app.post("/api/items/{media_type}/{tmdb_id}/keep-mismatch")
    async def api_keep_mismatch(
        request: Request, media_type: MediaType, tmdb_id: int,
        db: Path = Depends(get_db_path),
    ):
        """v1.11.99: ack the mismatch — drop it out of /pending but
        keep the canonical / placement divergence intact. Library row
        continues to render DL=amber + LINK=≠ as a passive reminder
        that motif holds a download Plex isn't playing.
        """
        _require_admin(request)
        with get_conn(db) as conn:
            cur = conn.execute(
                """UPDATE local_files SET mismatch_state = 'acked'
                    WHERE media_type = ? AND tmdb_id = ?
                      AND mismatch_state = 'pending'""",
                (media_type, tmdb_id),
            )
            if cur.rowcount == 0:
                raise HTTPException(
                    status_code=409,
                    detail="no pending mismatch to ack",
                )
        log_event(db, level="INFO", component="api",
                  media_type=media_type, tmdb_id=tmdb_id,
                  message=f"Keep mismatch by {request.state.user}",
                  detail={"sections_acked": cur.rowcount})
        return {"ok": True, "sections_acked": cur.rowcount}

    @app.post("/api/items/{media_type}/{tmdb_id}/unmanage")
    async def api_unmanage_item(
        request: Request, media_type: MediaType, tmdb_id: int,
        # v1.12.73: optional section_id scopes UNMANAGE to one
        # section. Without it, every section that motif manages
        # for this title gets unmanaged at once (legacy fan-out).
        # Mirrors the v1.12.46 ACCEPT UPDATE / v1.12.47 REVERT
        # scoping; per-edition theming (v1.12.72) requires this
        # so unmanaging the 4K edition doesn't yank motif's
        # tracking from the standard edition too.
        section_id: str | None = Query(None),
        db: Path = Depends(get_db_path),
    ):
        """v1.10.18: drop motif's management of a theme without deleting
        the file at the Plex folder.

        Workflow:
          - Delete motif's canonical (/themes/<movies|tv>/<Title (Year)>/theme.mp3).
            For hardlink placements the file at the Plex folder shares the
            inode and survives; for copy placements the Plex copy is
            independent and likewise survives.
          - Drop local_files + placements rows.
          - For plex_orphan rows: drop the themes row too (FK CASCADE
            cleans children) so the orphan doesn't linger with no
            local_files. Real ThemerrDB rows keep the themes row so the
            row reappears as themed-but-unmanaged.

        Result: row flips back to M (unmanaged sidecar) and can be
        re-adopted, replaced with TDB, or otherwise re-managed via the
        normal row actions. Use case: testing, or temporarily
        disconnecting motif from a title without nuking the actual
        theme file.

        v1.12.73: when section_id is provided the action targets just
        that section's local_files + placement; sibling sections'
        management survives. The orphan-themes drop only fires when
        the unmanaged section is the LAST one for the title.
        """
        _require_admin(request)
        with get_conn(db) as conn:
            theme = conn.execute(
                "SELECT upstream_source, title FROM themes "
                "WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            ).fetchone()
            if theme is None:
                raise HTTPException(status_code=404, detail="theme not found")
            # v1.11.0: walk every per-section local_files row for this
            # item — UNMANAGE drops motif from all of them at once.
            # v1.12.73: when section_id is provided, scope to that
            # section only.
            if section_id:
                locals_rows = conn.execute(
                    "SELECT section_id, file_path, file_sha256, source_kind, "
                    "       source_video_id "
                    "FROM local_files "
                    "WHERE media_type = ? AND tmdb_id = ? AND section_id = ?",
                    (media_type, tmdb_id, section_id),
                ).fetchall()
            else:
                locals_rows = conn.execute(
                    "SELECT section_id, file_path, file_sha256, source_kind, "
                    "       source_video_id "
                    "FROM local_files "
                    "WHERE media_type = ? AND tmdb_id = ?",
                    (media_type, tmdb_id),
                ).fetchall()
            if not locals_rows:
                raise HTTPException(
                    status_code=409,
                    detail="not motif-managed (no local_files row)",
                )
            # v1.10.57: snapshot the youtube_url so a future re-adopt of
            # the same file content can restore the URL. user_overrides /
            # themes are section-agnostic, so we read once and stamp one
            # history row per section snapshot.
            override_row = conn.execute(
                "SELECT youtube_url FROM user_overrides "
                "WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            ).fetchone()
            theme_yt = conn.execute(
                "SELECT youtube_url FROM themes "
                "WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            ).fetchone()
            saved_youtube_url = (
                (override_row and override_row["youtube_url"])
                or (theme_yt and theme_yt["youtube_url"])
            )
            for lr in locals_rows:
                if lr["file_sha256"]:
                    conn.execute(
                        """INSERT INTO local_files_history
                             (file_sha256, media_type, tmdb_id, source_kind,
                              source_video_id, youtube_url, saved_at, saved_reason)
                           VALUES (?, ?, ?, ?, ?, ?, ?, 'unmanage')""",
                        (lr["file_sha256"], media_type, tmdb_id,
                         lr["source_kind"], lr["source_video_id"],
                         saved_youtube_url, now_iso()),
                    )
            placements = conn.execute(
                "SELECT media_folder FROM placements "
                "WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            ).fetchall()
            # v1.11.85: snapshot the plex_items rows we'll need to
            # re-stat AFTER the DELETE chain, before any of it runs.
            # Pre-fix the post-DELETE block looked up plex_items via
            # `themes WHERE media_type=? AND tmdb_id=?` to derive a
            # theme_id_pk and then matched pi.theme_id = ?. For the
            # orphan branch (adopted manual sidecars; upstream_source=
            # 'plex_orphan') this lookup fired AFTER `DELETE FROM themes`
            # — so theme_id_pk came back None, the loop skipped entirely,
            # and pi.local_theme_file stayed at whatever value it held
            # before unmanage. Result: row dropped to '—' instead of
            # falling back to M, and only a follow-up REFRESH FROM PLEX
            # would correct it. Capture the pi list here while themes
            # (and pi.theme_id) still exist.
            plex_type_for_restat = "show" if media_type == "tv" else "movie"
            theme_id_pk_pre_row = conn.execute(
                "SELECT id FROM themes WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            ).fetchone()
            theme_id_pk_pre = (
                theme_id_pk_pre_row["id"] if theme_id_pk_pre_row else None
            )
            pi_rows_to_restat = []
            if theme_id_pk_pre is not None:
                pi_rows_to_restat = [
                    dict(r) for r in conn.execute(
                        "SELECT rating_key, folder_path FROM plex_items "
                        "WHERE theme_id = ? AND media_type = ?",
                        (theme_id_pk_pre, plex_type_for_restat),
                    ).fetchall()
                ]

        # Delete the per-section canonicals — leave Plex-folder placements intact.
        themes_dir = settings.themes_dir
        if themes_dir:
            for lr in locals_rows:
                try:
                    p = themes_dir / lr["file_path"]
                    if p.is_file():
                        p.unlink()
                    try:
                        parent = p.parent
                        if parent.is_dir() and not any(parent.iterdir()):
                            parent.rmdir()
                    except OSError:
                        pass
                except OSError as e:
                    log.warning("unmanage: could not unlink canonical %s: %s",
                                lr["file_path"], e)

        is_orphan = theme["upstream_source"] == "plex_orphan"
        with get_conn(db) as conn:
            # v1.12.73: when section_id is provided, scope the
            # delete to that section. The orphan-themes drop only
            # fires when this is the LAST section for the title
            # (otherwise we'd nuke the themes row and orphan the
            # other sections' local_files). _drop_motif_tracking
            # also runs only at the last-section step so per-edition
            # overrides survive partial unmanages.
            if section_id:
                # v1.12.79: capture the section's override URL into
                # themes.previous_youtube_url before the override
                # delete so the row keeps a one-step RESTORE path
                # post-UNMANAGE. Mirrors the PURGE capture; the
                # snapshot survives _drop_motif_tracking via v1.12.64.
                # v1.12.102: on_purge=True so any prior previous_url
                # moves to the hidden slot — RESTORE brings the
                # just-dropped URL back, REVERT after RESTORE walks
                # the chain to whatever was previous before this.
                _capture_previous_url(conn, media_type, tmdb_id,
                                      section_id=section_id,
                                      on_purge=True)
                conn.execute(
                    "DELETE FROM placements "
                    "WHERE media_type = ? AND tmdb_id = ? AND section_id = ?",
                    (media_type, tmdb_id, section_id),
                )
                conn.execute(
                    "DELETE FROM local_files "
                    "WHERE media_type = ? AND tmdb_id = ? AND section_id = ?",
                    (media_type, tmdb_id, section_id),
                )
                # Drop the row's section-specific override (matches
                # the v1.12.72 per-section semantic). The global ''
                # override and sibling sections' overrides survive.
                conn.execute(
                    "DELETE FROM user_overrides "
                    "WHERE media_type = ? AND tmdb_id = ? AND section_id = ?",
                    (media_type, tmdb_id, section_id),
                )
                # Last-section detection — drop themes (orphan only)
                # and tracking metadata only when this was the
                # final section.
                remaining = conn.execute(
                    "SELECT COUNT(*) AS n FROM local_files "
                    "WHERE media_type = ? AND tmdb_id = ?",
                    (media_type, tmdb_id),
                ).fetchone()
                last_section = remaining and remaining["n"] == 0
                if last_section:
                    _drop_motif_tracking(conn, media_type, tmdb_id)
                    if is_orphan:
                        conn.execute(
                            "DELETE FROM themes "
                            "WHERE media_type = ? AND tmdb_id = ?",
                            (media_type, tmdb_id),
                        )
            else:
                # Legacy global UNMANAGE — drops every section at once.
                # v1.12.57: drop motif's tracking metadata (override URL,
                # pending_updates, previous-URL snapshot). UNMANAGE means
                # motif walks away — the file stays at the Plex folder
                # but the metadata that drove motif's tracking should
                # leave too. Pre-fix the override URL lingered, so a
                # later re-adopt or re-download silently re-applied the
                # user's old URL.
                _drop_motif_tracking(conn, media_type, tmdb_id)
                if is_orphan:
                    conn.execute(
                        "DELETE FROM themes WHERE media_type = ? AND tmdb_id = ?",
                        (media_type, tmdb_id),
                    )
                else:
                    conn.execute(
                        "DELETE FROM placements WHERE media_type = ? AND tmdb_id = ?",
                        (media_type, tmdb_id),
                    )
                    conn.execute(
                        "DELETE FROM local_files WHERE media_type = ? AND tmdb_id = ?",
                        (media_type, tmdb_id),
                    )
            # v1.10.31 / v1.11.62 / v1.11.78 / v1.11.85: re-stat the
            # sidecar at every Plex folder that pointed at this theme.
            # The sidecar still exists (we deleted only the canonical),
            # so local_theme_file should be 1 and the row should render
            # as M immediately. folder_has_theme_sidecar handles
            # host→container path translation (Unraid /mnt/user/data
            # → /data) and matches any theme.<audio-ext>.
            # v1.11.85: pi_rows_to_restat was captured BEFORE the
            # DELETE chain ran. Pre-fix this block re-queried via
            # `themes WHERE media_type=? AND tmdb_id=?` after the
            # delete; for the orphan branch (upstream_source=
            # 'plex_orphan' — i.e. adopted manual sidecars) the themes
            # row had already been cascade-removed, so the lookup
            # returned no theme_id_pk and the loop quietly skipped.
            # Result: pi.local_theme_file kept its pre-unmanage value
            # (often 0, because plex_enum hadn't re-stat'd post-adopt)
            # and the row dropped to '—' until the next REFRESH FROM
            # PLEX corrected it.
            from ..core.plex_enum import folder_has_theme_sidecar
            for pi_row in pi_rows_to_restat:
                sidecar_present = folder_has_theme_sidecar(
                    pi_row["folder_path"] or "",
                )
                conn.execute(
                    "UPDATE plex_items SET local_theme_file = ? "
                    "WHERE rating_key = ?",
                    (1 if sidecar_present else 0, pi_row["rating_key"]),
                )
            # v1.11.47: dropped the post-unmanage _enqueue_section_refresh_for_item
            # call. The optimistic re-stat above already updates
            # plex_items.local_theme_file correctly per (folder, media_type),
            # so the confirmatory plex_enum was scanning all 10K+ items in
            # the section just to re-affirm one row. Net gain: one less
            # full-section enum per unmanage; pi.local_theme_file is
            # already truthful before this block exits.

        with get_conn(db) as conn:
            _record_audit(
                conn, actor=request.state.principal.username, action="unmanage",
                media_type=media_type, tmdb_id=tmdb_id,
                section_id=section_id,
                details={"orphan": is_orphan},
            )
        log_event(db, level="INFO", component="api",
                  media_type=media_type, tmdb_id=tmdb_id,
                  section_id=section_id,
                  message=f"Unmanage theme '{theme['title']}' "
                          f"by {request.state.principal.username}",
                  detail={"orphan": is_orphan})
        return {"ok": True, "title": theme["title"], "orphan": is_orphan}

    @app.post("/api/items/{media_type}/{tmdb_id}/forget")
    async def api_forget_item(
        request: Request, media_type: MediaType, tmdb_id: int,
        # v1.12.77: optional section_id scopes PURGE to one section.
        # Without it, every section that owns the title gets purged
        # at once (legacy fan-out). Mirrors the v1.12.46/.47/.73
        # scoping for ACCEPT UPDATE / REVERT / RE-DOWNLOAD /
        # UNMANAGE — per-edition theming requires this so PURGE
        # on the 4K row doesn't yank the standard edition's
        # theme too.
        section_id: str | None = Query(None),
        db: Path = Depends(get_db_path),
    ):
        """PURGE: full destruction of motif's theme presence for an item.

        Deletes BOTH the canonical at /themes/<media>/<Title (Year)>/theme.mp3
        AND the placement file(s) at every Plex media folder. Drops
        local_files + placements rows. For plex_orphan rows, also drops
        the themes row (FK CASCADE clears children).

        After PURGE the row's source flips to '—' (no theme anywhere).
        - If ThemerrDB still has the title, DOWNLOAD becomes available
          via the SOURCE menu.
        - If the row was user-provided (URL / upload / adopt), the file
          is entirely gone — the user must SET URL / UPLOAD MP3 / drop
          a sidecar back at the Plex folder before ADOPT becomes
          available.

        v1.12.77: when section_id is provided the action targets just
        that section. Sibling sections' files + tracking survive.
        Per-section override drops, themes-row drop (orphan-only),
        and tracking-metadata drop only fire when this is the LAST
        section for the title.

        v1.10.38: reverted v1.10.27's source_kind=URL/upload/adopt
        preservation. Per user feedback, PURGE should always be the
        full-destruction action; UNMANAGE is the keep-the-file action.
        """
        _require_admin(request)
        with get_conn(db) as conn:
            theme = conn.execute(
                "SELECT upstream_source, title FROM themes "
                "WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            ).fetchone()
            if theme is None:
                raise HTTPException(status_code=404, detail="theme not found")
            # v1.12.77: scope placements + locals_rows to the
            # requested section when one was provided.
            if section_id:
                placements = conn.execute(
                    "SELECT media_folder FROM placements "
                    "WHERE media_type = ? AND tmdb_id = ? AND section_id = ?",
                    (media_type, tmdb_id, section_id),
                ).fetchall()
            else:
                placements = conn.execute(
                    "SELECT media_folder FROM placements "
                    "WHERE media_type = ? AND tmdb_id = ?",
                    (media_type, tmdb_id),
                ).fetchall()
            if section_id:
                locals_rows = conn.execute(
                    "SELECT section_id, file_path, file_sha256, source_kind, "
                    "       source_video_id "
                    "FROM local_files "
                    "WHERE media_type = ? AND tmdb_id = ? AND section_id = ?",
                    (media_type, tmdb_id, section_id),
                ).fetchall()
            else:
                locals_rows = conn.execute(
                    "SELECT section_id, file_path, file_sha256, source_kind, "
                    "       source_video_id "
                    "FROM local_files "
                    "WHERE media_type = ? AND tmdb_id = ?",
                    (media_type, tmdb_id),
                ).fetchall()
            # v1.12.1: collect every plex_items.rating_key pointing at
            # this theme BEFORE the delete cascade fires. The post-
            # delete flag clear at the bottom of this handler clears
            # by rating_key — that's the only stable identifier that
            # survives the delete and reliably covers all three
            # 'how the row reaches this theme' paths:
            #   1. pi.theme_id stamp (set by adopt/SET URL/UPLOAD MP3
            #      via v1.11.43 / v1.11.64; ON DELETE SET NULL would
            #      strip this once we drop the themes row, so we
            #      memo-ize before delete)
            #   2. pi.folder_path matches a placement we're about to
            #      unlink
            #   3. (pi.guid_tmdb, media_type) matches the theme's id
            #      (the only path that worked in v1.11.60 — broken
            #      for synthetic-negative orphan tmdb_ids)
            # Pre-fix: PURGE on an upload/URL/adopt row left
            # pi.local_theme_file=1 forever because the orphan's
            # synthetic tmdb_id never matched pi.guid_tmdb (which
            # holds Plex's real id), so the row kept rendering as
            # L (or M pre-v1.12.0) with ADOPT shown — confusing
            # since the user had just said 'destroy this'.
            theme_id_pk_for_clear = conn.execute(
                "SELECT id FROM themes WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            ).fetchone()
            theme_id_pk_for_clear = (
                theme_id_pk_for_clear["id"] if theme_id_pk_for_clear else None
            )
            plex_mt = "show" if media_type == "tv" else "movie"
            placement_folders = [pr["media_folder"] for pr in placements]
            rk_clear: set[str] = set()
            # v1.12.77: when section_id is provided, scope the
            # rating_key collection to that section so plex_items
            # in sibling sections keep their local_theme_file +
            # has_theme flags. Otherwise the section-scoped PURGE
            # would delete the 4K placement file but ALSO clear
            # the standard section's plex_items has_theme flag,
            # which would silently flip the standard row to '-'
            # at the next render.
            section_filter_sql = (
                "AND section_id = ?" if section_id else ""
            )
            section_filter_args = ((section_id,) if section_id else ())
            # Path 1: theme_id stamp
            if theme_id_pk_for_clear is not None:
                for r in conn.execute(
                    f"SELECT rating_key FROM plex_items "
                    f"WHERE theme_id = ? {section_filter_sql}",
                    (theme_id_pk_for_clear, *section_filter_args),
                ).fetchall():
                    rk_clear.add(r["rating_key"])
            # Path 2: placement folder_path match
            if placement_folders:
                qmarks = ",".join("?" for _ in placement_folders)
                for r in conn.execute(
                    f"SELECT rating_key FROM plex_items "
                    f"WHERE folder_path IN ({qmarks}) AND media_type = ? "
                    f"{section_filter_sql}",
                    (*placement_folders, plex_mt, *section_filter_args),
                ).fetchall():
                    rk_clear.add(r["rating_key"])
            # Path 3: real-tmdb match (covers the non-orphan case
            # where motif never had to allocate a synthetic id)
            for r in conn.execute(
                f"SELECT rating_key FROM plex_items "
                f"WHERE guid_tmdb = ? AND media_type = ? "
                f"{section_filter_sql}",
                (tmdb_id, plex_mt, *section_filter_args),
            ).fetchall():
                rk_clear.add(r["rating_key"])
            # v1.10.57: snapshot content-hash → URL mapping per section
            # so a future ADOPT (any section) can restore the URL.
            if locals_rows:
                override_row = conn.execute(
                    "SELECT youtube_url FROM user_overrides "
                    "WHERE media_type = ? AND tmdb_id = ?",
                    (media_type, tmdb_id),
                ).fetchone()
                theme_yt = conn.execute(
                    "SELECT youtube_url FROM themes "
                    "WHERE media_type = ? AND tmdb_id = ?",
                    (media_type, tmdb_id),
                ).fetchone()
                saved_youtube_url = (
                    (override_row and override_row["youtube_url"])
                    or (theme_yt and theme_yt["youtube_url"])
                )
                for lr in locals_rows:
                    if lr["file_sha256"]:
                        conn.execute(
                            """INSERT INTO local_files_history
                                 (file_sha256, media_type, tmdb_id, source_kind,
                                  source_video_id, youtube_url, saved_at, saved_reason)
                               VALUES (?, ?, ?, ?, ?, ?, ?, 'forget')""",
                            (lr["file_sha256"], media_type, tmdb_id,
                             lr["source_kind"], lr["source_video_id"],
                             saved_youtube_url, now_iso()),
                        )

        # Unlink every placement file across all sections.
        unlinked = 0
        affected_folders: list[str] = []
        for pr in placements:
            try:
                p = Path(pr["media_folder"]) / "theme.mp3"
                if p.is_file():
                    p.unlink()
                    unlinked += 1
                affected_folders.append(pr["media_folder"])
            except OSError as e:
                log.warning("forget: could not unlink %s: %s",
                            pr["media_folder"], e)
        themes_dir = settings.themes_dir
        if themes_dir:
            for lr in locals_rows:
                try:
                    p = themes_dir / lr["file_path"]
                    if p.is_file():
                        p.unlink()
                    try:
                        parent = p.parent
                        if parent.is_dir() and not any(parent.iterdir()):
                            parent.rmdir()
                    except OSError:
                        pass
                except OSError as e:
                    log.warning("forget: could not unlink canonical %s: %s",
                                lr["file_path"], e)

        is_orphan = theme["upstream_source"] == "plex_orphan"
        with get_conn(db) as conn:
            if section_id:
                # v1.12.77: scope DELETE to the row's section.
                # Sibling sections' placements + local_files
                # survive. Per-section override drops too. The
                # tracking-metadata drop and orphan-themes delete
                # only fire when this is the LAST section for the
                # title (no other local_files remaining), mirroring
                # the v1.12.73 UNMANAGE last-section logic.
                # v1.12.79: snapshot the URL ABOUT to be dropped so
                # the post-PURGE RESTORE button has something to
                # bring back. Reads the section's override (or
                # falls back to '' / themes); writes title-global
                # previous_youtube_url + previous_youtube_kind.
                # Last-section path below also keeps this snapshot
                # intact — _drop_motif_tracking deliberately
                # preserves previous_* per v1.12.64.
                # v1.12.102: on_purge=True so any prior previous_url
                # moves to the hidden slot — RESTORE brings the
                # just-purged URL back, REVERT after RESTORE walks
                # the chain to whatever was previous before this.
                _capture_previous_url(conn, media_type, tmdb_id,
                                      section_id=section_id,
                                      on_purge=True)
                conn.execute(
                    "DELETE FROM placements "
                    "WHERE media_type = ? AND tmdb_id = ? AND section_id = ?",
                    (media_type, tmdb_id, section_id),
                )
                conn.execute(
                    "DELETE FROM local_files "
                    "WHERE media_type = ? AND tmdb_id = ? AND section_id = ?",
                    (media_type, tmdb_id, section_id),
                )
                conn.execute(
                    "DELETE FROM user_overrides "
                    "WHERE media_type = ? AND tmdb_id = ? AND section_id = ?",
                    (media_type, tmdb_id, section_id),
                )
                remaining = conn.execute(
                    "SELECT COUNT(*) AS n FROM local_files "
                    "WHERE media_type = ? AND tmdb_id = ?",
                    (media_type, tmdb_id),
                ).fetchone()
                last_section = remaining and remaining["n"] == 0
                if last_section:
                    _drop_motif_tracking(conn, media_type, tmdb_id)
                    if is_orphan:
                        conn.execute(
                            "DELETE FROM themes "
                            "WHERE media_type = ? AND tmdb_id = ?",
                            (media_type, tmdb_id),
                        )
            else:
                # Legacy global PURGE path — destroys every section.
                # v1.12.57: PURGE = full destruction. Drop motif's
                # tracking metadata (override URL, pending_updates,
                # previous-URL snapshot) so the row is a true clean
                # slate. Pre-fix a "CLEAR URL → PURGE" sequence left
                # the user_overrides row alive — the row showed src='-'
                # but the next download silently used the stale
                # override URL, classifying the row as U again.
                _drop_motif_tracking(conn, media_type, tmdb_id)
                if is_orphan:
                    conn.execute(
                        "DELETE FROM themes WHERE media_type = ? AND tmdb_id = ?",
                        (media_type, tmdb_id),
                    )
                else:
                    conn.execute(
                        "DELETE FROM placements WHERE media_type = ? AND tmdb_id = ?",
                        (media_type, tmdb_id),
                    )
                    conn.execute(
                        "DELETE FROM local_files WHERE media_type = ? AND tmdb_id = ?",
                        (media_type, tmdb_id),
                    )
            # v1.12.1: clear pi.local_theme_file + has_theme by the
            # rating_key set we memo-ized BEFORE delete (see the
            # theme_id_pk_for_clear / rk_clear collection above).
            # Pre-fix the WHERE guid_tmdb=? clause silently missed
            # every orphan-backed row because the orphan's synthetic
            # negative tmdb_id never matches pi.guid_tmdb (which
            # holds Plex's real id, or NULL).
            if rk_clear:
                qmarks = ",".join("?" for _ in rk_clear)
                conn.execute(
                    f"UPDATE plex_items SET local_theme_file = 0, "
                    f"                      has_theme = 0 "
                    f"WHERE rating_key IN ({qmarks})",
                    tuple(rk_clear),
                )
        # v1.10.28: skip the section enum (it would re-fetch has_theme
        # from Plex's stale cache and bring back a phantom P badge);
        # trigger a per-item refresh instead so Plex updates its cache.
        try:
            _trigger_plex_item_refresh(media_type, tmdb_id)
        except Exception as e:
            log.debug("plex item refresh skipped: %s", e)

        with get_conn(db) as conn:
            _record_audit(
                conn, actor=request.state.user, action="purge",
                media_type=media_type, tmdb_id=tmdb_id,
                section_id=section_id,
                details={"orphan_dropped": is_orphan,
                         "placements_unlinked": unlinked,
                         "placements_total": len(placements)},
            )
        log_event(db, level="INFO", component="api",
                  media_type=media_type, tmdb_id=tmdb_id,
                  section_id=section_id,
                  message=f"Forget by {request.state.user}",
                  detail={"title": theme["title"],
                          "orphan_dropped": is_orphan,
                          "placements_unlinked": unlinked,
                          "placements_total": len(placements)})
        return {"ok": True, "orphan_dropped": is_orphan,
                "placements_unlinked": unlinked}

    @app.delete("/api/items/{media_type}/{tmdb_id}", status_code=204)
    async def api_delete_item(
        request: Request, media_type: MediaType, tmdb_id: int,
        db: Path = Depends(get_db_path),
    ):
        """Delete a theme row + all FK'd children + on-disk files. Restricted
        to plex_orphan rows: real ThemerrDB rows would just come back on the
        next sync, so deleting them is a footgun."""
        _require_admin(request)
        with get_conn(db) as conn:
            theme = conn.execute(
                "SELECT upstream_source, title FROM themes "
                "WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            ).fetchone()
            if theme is None:
                raise HTTPException(status_code=404, detail="theme not found")
            if theme["upstream_source"] != "plex_orphan":
                raise HTTPException(
                    status_code=400,
                    detail="only plex_orphan rows can be deleted; real ThemerrDB rows "
                           "would be re-created on the next sync",
                )
            # Collect files to unlink BEFORE we drop the rows
            placement_rows = conn.execute(
                "SELECT media_folder, placement_kind FROM placements "
                "WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            ).fetchall()
            # v1.11.0: per-section local_files — fetch all so we unlink
            # every staging copy. Pre-fix only fetched the first row,
            # leaking on-disk files in sibling sections after FK CASCADE
            # dropped the DB rows.
            local_rows = conn.execute(
                "SELECT file_path FROM local_files "
                "WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            ).fetchall()

        # Unlink placement files (hardlinks have independent inodes; deleting
        # the canonical doesn't remove these). Best-effort; missing files are OK.
        unlinked = 0
        for pr in placement_rows:
            try:
                p = Path(pr["media_folder"]) / "theme.mp3"
                if p.is_file():
                    p.unlink()
                    unlinked += 1
            except OSError as e:
                log.warning("Could not unlink placement %s: %s", pr["media_folder"], e)
        # Unlink every per-section canonical.
        themes_dir = settings.themes_dir
        if themes_dir:
            for lr in local_rows:
                try:
                    p = themes_dir / lr["file_path"]
                    if p.is_file():
                        p.unlink()
                    try:
                        parent = p.parent
                        if parent.is_dir() and not any(parent.iterdir()):
                            parent.rmdir()
                    except OSError:
                        pass
                except OSError as e:
                    log.warning("Could not unlink canonical %s: %s",
                                lr["file_path"], e)

        # Drop the theme row; FK ON DELETE CASCADE handles local_files,
        # placements, pending_updates, user_overrides in one transaction.
        # v1.12.1: collect every plex_items.rating_key pointing at
        # this theme BEFORE the delete cascade fires, then clear the
        # post-delete flags by rating_key. Same reasoning as the
        # forget endpoint fix — guid_tmdb-keyed clears miss
        # synthetic-negative orphan tmdb_ids entirely.
        plex_mt = "show" if media_type == "tv" else "movie"
        placement_folders = [pr["media_folder"] for pr in placement_rows]
        rk_clear: set[str] = set()
        with get_conn(db) as conn:
            theme_id_pk_for_clear = conn.execute(
                "SELECT id FROM themes WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            ).fetchone()
            theme_id_pk_for_clear = (
                theme_id_pk_for_clear["id"] if theme_id_pk_for_clear else None
            )
            if theme_id_pk_for_clear is not None:
                for r in conn.execute(
                    "SELECT rating_key FROM plex_items WHERE theme_id = ?",
                    (theme_id_pk_for_clear,),
                ).fetchall():
                    rk_clear.add(r["rating_key"])
            if placement_folders:
                qmarks = ",".join("?" for _ in placement_folders)
                for r in conn.execute(
                    f"SELECT rating_key FROM plex_items "
                    f"WHERE folder_path IN ({qmarks}) AND media_type = ?",
                    (*placement_folders, plex_mt),
                ).fetchall():
                    rk_clear.add(r["rating_key"])
            for r in conn.execute(
                "SELECT rating_key FROM plex_items "
                "WHERE guid_tmdb = ? AND media_type = ?",
                (tmdb_id, plex_mt),
            ).fetchall():
                rk_clear.add(r["rating_key"])

            # v1.12.57: drop motif's tracking metadata before
            # killing the themes row. FK CASCADE on themes handles
            # placements / local_files / pending_updates, but
            # user_overrides has no FK so without this explicit
            # delete the override survives the orphan-themes
            # destruction and silently re-applies on next adopt.
            _drop_motif_tracking(conn, media_type, tmdb_id)
            conn.execute(
                "DELETE FROM themes WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            )
            if rk_clear:
                qmarks = ",".join("?" for _ in rk_clear)
                conn.execute(
                    f"UPDATE plex_items SET local_theme_file = 0, "
                    f"                      has_theme = 0 "
                    f"WHERE rating_key IN ({qmarks})",
                    tuple(rk_clear),
                )

        log_event(db, level="INFO", component="api",
                  media_type=media_type, tmdb_id=tmdb_id,
                  message=f"Orphan deleted by {request.state.user}",
                  detail={"title": theme["title"],
                          "placements_unlinked": unlinked,
                          "placements_total": len(placement_rows)})
        return Response(status_code=204)

    @app.post("/api/items/{media_type}/{tmdb_id}/clear-url")
    async def api_clear_url_override(
        request: Request, media_type: MediaType, tmdb_id: int,
        section_id: str | None = Query(None),
        db: Path = Depends(get_db_path),
    ):
        """v1.12.37 (revised per user feedback): drop the captured
        PREVIOUS URL on a row so REVERT/RESTORE becomes unavailable.
        The current canonical and user_overrides are untouched — the
        playing theme keeps playing.

        v1.12.86: per-section. With section_id provided, drops only
        that section's previous_urls row (the '' fallback row, if any,
        survives so other sections relying on it stay intact). Without
        section_id, drops every previous_urls row for the title — same
        as the legacy "clear themes.previous_youtube_url" semantic.

        Returns 409 if no matching previous URL is captured.
        """
        _require_admin(request)
        with get_conn(db) as conn:
            existing = _load_previous_url(
                conn, media_type, tmdb_id, section_id=section_id,
            )
            if existing is None:
                raise HTTPException(
                    status_code=409,
                    detail="no captured previous URL to clear",
                )
            old_url = existing["youtube_url"]
            _clear_previous_url(
                conn, media_type, tmdb_id, section_id=section_id,
            )
            _record_audit(
                conn, actor=request.state.user, action="clear_previous_url",
                media_type=media_type, tmdb_id=tmdb_id,
                section_id=section_id,
                details={"old_url": old_url},
            )
        log_event(db, level="INFO", component="api",
                  media_type=media_type, tmdb_id=tmdb_id,
                  message=f"Previous URL cleared by {request.state.user}")
        return {"ok": True}

    @app.post("/api/items/{media_type}/{tmdb_id}/revert")
    async def api_revert_to_themerrdb(
        request: Request, media_type: MediaType, tmdb_id: int,
        # v1.12.47: optional section_id scopes the re-download +
        # placement to one section (mirrors the v1.12.46
        # ACCEPT UPDATE flow). Without it, the download fans
        # out to every section that owns the title — wrong
        # when only one library/edition is being reverted.
        section_id: str | None = Query(None),
        db: Path = Depends(get_db_path),
    ):
        """REVERT the row's canonical URL one step back to the
        captured previous URL for this section.

        v1.12.37: generalized one-step undo. The snapshot is
        populated by any handler that changes the canonical URL
        (ACCEPT UPDATE, SET URL, UPLOAD MP3, REPLACE TDB,
        PURGE/UNMANAGE pre-drop).

        Resolves which restoration path to apply based on the
        snapshot's kind:
          'user'      → INSERT user_overrides with previous URL
                        (row goes back to U, blue TDB ↑ pill
                        returns if there's a pending update).
          'themerrdb' → DELETE user_overrides (worker uses
                        themes.youtube_url which is the upstream
                        URL; row goes to T).

        Either way, swaps the new "current → previous" URL pair
        atomically so REVERT is fully round-trippable: clicking
        REVERT twice returns the row to its pre-first-revert
        state. Returns 409 if no previous URL is captured.

        v1.12.86: reads/writes the per-section previous_urls table
        instead of the title-global themes.previous_youtube_url
        columns. RESTORE on section A no longer disturbs section B.
        """
        _require_admin(request)
        with get_conn(db) as conn:
            theme_row = conn.execute(
                "SELECT youtube_url FROM themes "
                "WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            ).fetchone()
            if theme_row is None:
                raise HTTPException(status_code=404, detail="not in database")
            snapshot = _load_previous_url(
                conn, media_type, tmdb_id, section_id=section_id,
            )
            if snapshot is None:
                raise HTTPException(
                    status_code=409,
                    detail="nothing to revert — no previous URL captured for this row",
                )
            prev_url = snapshot["youtube_url"]
            prev_kind = snapshot["kind"]

            # Snapshot what's currently in the canonical so REVERT
            # is a swap, not a one-way pop. Capturing here also
            # means a second REVERT call cleanly round-trips.
            # v1.12.79: prefer the section-specific override row
            # so a per-section revert round-trips against the
            # section's URL rather than picking up a sibling
            # section's override by accident. Falls back to the
            # '' global row, then to themes.youtube_url.
            ovr_section = section_id or ""
            ovr = conn.execute(
                "SELECT youtube_url FROM user_overrides "
                "WHERE media_type = ? AND tmdb_id = ? AND section_id = ?",
                (media_type, tmdb_id, ovr_section),
            ).fetchone()
            if ovr is None or not ovr["youtube_url"]:
                ovr = conn.execute(
                    "SELECT youtube_url FROM user_overrides "
                    "WHERE media_type = ? AND tmdb_id = ? AND section_id = ''",
                    (media_type, tmdb_id),
                ).fetchone()
            if ovr and ovr["youtube_url"]:
                new_prev_url = ovr["youtube_url"]
                new_prev_kind = "user"
            else:
                new_prev_url = theme_row["youtube_url"]
                new_prev_kind = "themerrdb" if new_prev_url else None

            # Apply the previous URL based on its kind.
            # v1.12.39: provenance flip happens lazily, on the
            # worker's download completion (worker.py writes
            # provenance + source_kind from the CURRENT presence
            # of a user override). Pre-fix this handler eagerly
            # flipped local_files + placements provenance, so the
            # SRC badge transitioned to its target value the
            # instant REVERT was clicked — even though the
            # canonical was still the OLD theme until the
            # download landed. Now the SRC badge stays on its
            # current value (T after a previous accept; U after a
            # previous SET URL) until the new canonical is in
            # place, matching the user's mental model: "the row's
            # SRC describes what's actually playing".
            if prev_kind == "user":
                # v1.12.72: scope to the row's section_id when
                # provided. Without a section the legacy '' global
                # row gets restored, matching pre-migration behavior.
                ovr_section = section_id or ""
                conn.execute(
                    """INSERT INTO user_overrides
                         (media_type, tmdb_id, youtube_url,
                          set_at, set_by, note, section_id)
                       VALUES (?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT(media_type, tmdb_id, section_id) DO UPDATE SET
                           youtube_url = excluded.youtube_url,
                           set_at = excluded.set_at,
                           set_by = excluded.set_by,
                           note = excluded.note""",
                    (media_type, tmdb_id, prev_url, now_iso(),
                     request.state.principal.username,
                     "restored via REVERT", ovr_section),
                )
                # If a pending_update was previously accepted on
                # this row, reset it so the blue TDB ↑ pill
                # returns — user is back in the pre-accept state.
                conn.execute(
                    """UPDATE pending_updates SET
                         decision = 'pending',
                         decision_at = NULL,
                         decision_by = NULL,
                         replaced_user_url = NULL
                       WHERE media_type = ? AND tmdb_id = ?
                         AND decision = 'accepted'""",
                    (media_type, tmdb_id),
                )
            else:  # 'themerrdb'
                # Drop user_overrides so the worker uses
                # themes.youtube_url on download. If the previous
                # URL no longer matches themes.youtube_url (TDB
                # has changed since the snapshot), the worker
                # will download the current TDB URL — close
                # enough for a one-step undo.
                # v1.12.72: scope to the row's section. Sibling
                # sections' overrides survive (they may have their
                # own per-edition URL the user wants to keep).
                # When called without section_id (legacy / orphan
                # paths), drops only the '' global row.
                ovr_section = section_id or ""
                conn.execute(
                    "DELETE FROM user_overrides "
                    "WHERE media_type = ? AND tmdb_id = ? AND section_id = ?",
                    (media_type, tmdb_id, ovr_section),
                )

            # Swap the previous_urls snapshot to point at what was
            # current before this revert — REVERT becomes
            # round-trippable. v1.12.86: per-section write (was a
            # title-global update on themes columns).
            # v1.12.102: when the snapshot we just consumed has a
            # hidden_* slot (PURGE/UNMANAGE pre-loaded it with the
            # URL that was previous BEFORE the destructive action),
            # promote hidden_* to previous_url instead of swapping.
            # This is the post-RESTORE step: previous_url goes back
            # to whatever was previous before the user purged, so a
            # subsequent REVERT continues the chain. The hidden slot
            # is consumed (cleared) since the round-trip is complete.
            target_section = section_id if section_id is not None else ""
            hidden_url = snapshot["hidden_url"] if snapshot else None
            hidden_kind = snapshot["hidden_kind"] if snapshot else None
            if hidden_url and hidden_kind:
                conn.execute(
                    """INSERT INTO previous_urls
                         (media_type, tmdb_id, section_id, youtube_url,
                          kind, captured_at,
                          hidden_url, hidden_kind, hidden_captured_at)
                       VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, NULL)
                       ON CONFLICT(media_type, tmdb_id, section_id) DO UPDATE SET
                           youtube_url = excluded.youtube_url,
                           kind = excluded.kind,
                           captured_at = excluded.captured_at,
                           hidden_url = NULL,
                           hidden_kind = NULL,
                           hidden_captured_at = NULL""",
                    (media_type, tmdb_id, target_section,
                     hidden_url, hidden_kind, now_iso()),
                )
            elif new_prev_url and new_prev_kind:
                conn.execute(
                    """INSERT INTO previous_urls
                         (media_type, tmdb_id, section_id, youtube_url,
                          kind, captured_at,
                          hidden_url, hidden_kind, hidden_captured_at)
                       VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, NULL)
                       ON CONFLICT(media_type, tmdb_id, section_id) DO UPDATE SET
                           youtube_url = excluded.youtube_url,
                           kind = excluded.kind,
                           captured_at = excluded.captured_at,
                           hidden_url = NULL,
                           hidden_kind = NULL,
                           hidden_captured_at = NULL""",
                    (media_type, tmdb_id, target_section,
                     new_prev_url, new_prev_kind, now_iso()),
                )
            else:
                _clear_previous_url(
                    conn, media_type, tmdb_id, section_id=section_id,
                )
            # Failure flags clear regardless — REVERT's intent is
            # "I'm fixing this row".
            conn.execute(
                """UPDATE themes SET failure_kind = NULL,
                                     failure_message = NULL,
                                     failure_at = NULL
                   WHERE media_type = ? AND tmdb_id = ?""",
                (media_type, tmdb_id),
            )
            # Cancel any in-flight download (would otherwise pick
            # up the stale URL), then enqueue a fresh one.
            conn.execute(
                """UPDATE jobs SET status = 'cancelled', finished_at = ?
                   WHERE job_type = 'download' AND media_type = ? AND tmdb_id = ?
                     AND status IN ('pending', 'failed')""",
                (now_iso(), media_type, tmdb_id),
            )
            # v1.12.47: REVERT now passes force_place=True +
            # only_section_id (when provided) — mirrors the
            # v1.12.43 ACCEPT UPDATE fix. Pre-fix the place job
            # ran but skipped with status "plex_has_theme"
            # because the existing post-accept theme.mp3 was
            # there; the new (reverted-to) URL got downloaded
            # into canonical but never replaced the placement,
            # so Plex kept playing the post-accept theme.
            from ..core.sync import _enqueue_download
            _enqueue_download(
                conn, media_type=media_type, tmdb_id=tmdb_id,
                reason=f"revert_to_{prev_kind}_url",
                auto_place=True,
                force_place=True,
                only_section_id=section_id,
            )
            _record_audit(
                conn, actor=request.state.user, action="revert",
                media_type=media_type, tmdb_id=tmdb_id,
                section_id=section_id,
                details={
                    "restored_url": prev_url,
                    "restored_kind": prev_kind,
                    "new_previous_url": new_prev_url,
                    "new_previous_kind": new_prev_kind,
                },
            )
        log_event(db, level="INFO", component="api",
                  media_type=media_type, tmdb_id=tmdb_id,
                  section_id=section_id,
                  message=f"REVERT to {prev_kind} URL by {request.state.user}")
        return {"ok": True}

    @app.get("/api/items/{media_type}/{tmdb_id}/audit")
    async def api_item_audit(
        media_type: MediaType, tmdb_id: int,
        section_id: str | None = Query(None),
        limit: int = Query(50, ge=1, le=500),
        db: Path = Depends(get_db_path),
    ):
        """v1.12.80: audit_events for a single row, newest first.
        Powers the INFO card's // PROVENANCE section so users can
        see "who changed this row, when, and what was the URL
        before" months after the fact — distinct from the rolling
        events log which gets rotated.

        v1.12.81: section_id filter. When the INFO card is opened
        on a 4K row the user expects PROVENANCE to show actions
        taken against that section — pre-fix the unfiltered query
        returned every audit row for the title, so the standard
        and 4K cards rendered identical timelines and the user
        couldn't tell which section's history they were looking
        at. The filter keeps title-global rows (section_id IS NULL
        — e.g., ADOPT which spans every section) visible alongside
        the section-specific ones, so cross-section context isn't
        lost."""
        with get_conn(db) as conn:
            if section_id is not None:
                rows = conn.execute(
                    """SELECT id, occurred_at, actor, action, section_id, details
                         FROM audit_events
                        WHERE media_type = ? AND tmdb_id = ?
                          AND (section_id = ? OR section_id IS NULL)
                        ORDER BY occurred_at DESC, id DESC
                        LIMIT ?""",
                    (media_type, tmdb_id, section_id, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """SELECT id, occurred_at, actor, action, section_id, details
                         FROM audit_events
                        WHERE media_type = ? AND tmdb_id = ?
                        ORDER BY occurred_at DESC, id DESC
                        LIMIT ?""",
                    (media_type, tmdb_id, limit),
                ).fetchall()
        out = []
        for r in rows:
            details = None
            if r["details"]:
                try:
                    details = json.loads(r["details"])
                except (ValueError, TypeError):
                    details = None
            out.append({
                "id": r["id"],
                "occurred_at": r["occurred_at"],
                "actor": r["actor"],
                "action": r["action"],
                "section_id": r["section_id"],
                "details": details,
            })
        return {"events": out}

    @app.delete("/api/items/{media_type}/{tmdb_id}/audit")
    async def api_item_audit_clear(
        request: Request, media_type: MediaType, tmdb_id: int,
        section_id: str | None = Query(None),
        db: Path = Depends(get_db_path),
    ):
        """v1.12.83: clear audit_events for a single row. Useful while
        testing — repeated SET URL / ACCEPT UPDATE / PURGE cycles can
        bloat PROVENANCE to dozens of entries that are only meaningful
        to the testing session. Admin-only.

        Honors the same section_id filter as the GET so the user can
        clear just one section's row without nuking title-global
        entries (NULL section_id, e.g. ADOPT). Without section_id
        every audit row for the (media_type, tmdb_id) tuple is
        deleted.
        """
        _require_admin(request)
        with get_conn(db) as conn:
            if section_id is not None:
                cur = conn.execute(
                    """DELETE FROM audit_events
                        WHERE media_type = ? AND tmdb_id = ?
                          AND (section_id = ? OR section_id IS NULL)""",
                    (media_type, tmdb_id, section_id),
                )
            else:
                cur = conn.execute(
                    "DELETE FROM audit_events "
                    "WHERE media_type = ? AND tmdb_id = ?",
                    (media_type, tmdb_id),
                )
        return {"ok": True, "deleted": cur.rowcount}

    @app.delete("/api/items/{media_type}/{tmdb_id}/events")
    async def api_item_events_clear(
        request: Request, media_type: MediaType, tmdb_id: int,
        db: Path = Depends(get_db_path),
    ):
        """v1.12.83: clear the rolling events log entries for a row
        (drives the INFO card's // HISTORY section). Title-global —
        events table has no section_id column, so this clears across
        all sections that own the title. Admin-only."""
        _require_admin(request)
        with get_conn(db) as conn:
            cur = conn.execute(
                "DELETE FROM events "
                "WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            )
        return {"ok": True, "deleted": cur.rowcount}

    @app.get("/api/items/{media_type}/{tmdb_id}/theme.mp3")
    async def api_item_theme_audio(
        request: Request, media_type: MediaType, tmdb_id: int,
        section_id: str | None = Query(None),
        db: Path = Depends(get_db_path),
    ):
        """v1.12.90: stream the canonical theme.mp3 for in-browser
        audio playback from the INFO card. Resolves the file via
        local_files (per-section when section_id is provided, falling
        back to any-section). FastAPI's FileResponse handles range
        requests so the <audio> element can seek without re-fetching
        the whole file.

        Auth-required like every other /api/items/* endpoint. Returns
        404 when the row has no canonical recorded, and 410 when the
        DB has a file_path but the file isn't actually on disk
        (canonical_missing / dlBroken state).
        """
        _require_admin(request)
        with get_conn(db) as conn:
            if section_id is not None:
                row = conn.execute(
                    "SELECT file_path FROM local_files "
                    "WHERE media_type = ? AND tmdb_id = ? AND section_id = ?",
                    (media_type, tmdb_id, section_id),
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT file_path FROM local_files "
                    "WHERE media_type = ? AND tmdb_id = ? "
                    "ORDER BY section_id LIMIT 1",
                    (media_type, tmdb_id),
                ).fetchone()
        if row is None or not row["file_path"]:
            raise HTTPException(status_code=404, detail="no canonical recorded for this row")
        if not settings.is_paths_ready():
            raise HTTPException(status_code=503, detail="themes_dir not configured")
        full = settings.themes_dir / row["file_path"]
        try:
            full = full.resolve()
            themes_root = settings.themes_dir.resolve()
        except OSError as e:
            raise HTTPException(status_code=500, detail=f"path resolution failed: {e}")
        # Reject path-traversal attempts — full must live under themes_dir.
        if themes_root not in full.parents and full != themes_root:
            raise HTTPException(status_code=400, detail="resolved path escapes themes_dir")
        if not full.is_file():
            raise HTTPException(
                status_code=410,
                detail="canonical recorded but missing on disk (dlBroken)",
            )
        return FileResponse(full, media_type="audio/mpeg",
                            headers={"Cache-Control": "no-store"})

    @app.get("/api/items/{media_type}/{tmdb_id}/recovery-options")
    async def api_recovery_options(
        media_type: MediaType, tmdb_id: int,
        db: Path = Depends(get_db_path),
    ):
        """v1.12.71: structured recovery checklist for a failed row.

        Returns a list of options ranked by likelihood of fixing the
        specific failure_kind, each with a label, tooltip, action
        keyword that the frontend can map to existing handlers, and
        a tone color to match motif's source palette. The ordering
        reflects "try this first" — e.g., for VIDEO_PRIVATE the
        most-likely fix is SET URL (find a different YouTube source);
        ACK FAILURE is always last because it dismisses without
        fixing.

        Powers the INFO card's // TRY THIS NEXT section so the user
        gets concrete next steps for each failure kind instead of
        the generic "Recover via SET URL, UPLOAD MP3, or drop a
        sidecar and ADOPT" copy that previously lived in the row's
        tdb-pill tooltip.
        """
        with get_conn(db) as conn:
            row = conn.execute(
                "SELECT failure_kind, failure_message, failure_acked_at, "
                "       upstream_source FROM themes "
                "WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            ).fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail="theme not found")
            # v1.12.71: resolve a representative rating_key so the
            # recovery buttons that need one (SET URL, UPLOAD MP3)
            # can wire up via the existing per-rating_key endpoints.
            # Picks the first plex_items row matching this title;
            # NULL if the row isn't currently in any Plex section
            # (e.g., after the Plex section was removed).
            plex_mt = "show" if media_type == "tv" else "movie"
            rk_row = conn.execute(
                "SELECT rating_key FROM plex_items "
                "WHERE guid_tmdb = ? AND media_type = ? "
                "LIMIT 1",
                (tmdb_id, plex_mt),
            ).fetchone()
            rating_key = rk_row["rating_key"] if rk_row else None
        kind = row["failure_kind"]
        if not kind:
            return {"failure_kind": None, "options": [],
                    "human": None, "needs_manual_override": False}
        # Static recipe per FailureKind. Each entry is (action,
        # label, tooltip, priority, tone). Frontend renders these
        # as buttons that call into the existing per-action
        # handlers — `action` is the same string as the SOURCE menu
        # data-act attribute so the click dispatch is shared.
        cookies_present = bool(
            settings.cookies_file and settings.cookies_file.is_file()
        )
        is_orphan = row["upstream_source"] == "plex_orphan"
        recipes: dict[str, list[dict]] = {
            "cookies_expired": [
                {"action": "info", "label": "DROP cookies.txt",
                 "tooltip": "Place a fresh cookies.txt at the path configured in /settings. The next download attempt will retry with the new credentials.",
                 "priority": 1, "tone": "info",
                 "interactive": False},
                {"action": "redl", "label": "RE-DOWNLOAD",
                 "tooltip": "Retry the download with the current cookies file. Useful if you just dropped a fresh one.",
                 "priority": 2, "tone": "themerrdb",
                 "interactive": True,
                 "disabled": not cookies_present,
                 "disabled_reason": "no cookies.txt at the configured path"},
                {"action": "manual-url", "label": "SET URL",
                 "tooltip": "Provide a non-cookie-walled YouTube URL.",
                 "priority": 3, "tone": "user",
                 "interactive": True},
                {"action": "upload-theme", "label": "UPLOAD MP3",
                 "tooltip": "Skip YouTube — upload your own MP3 file.",
                 "priority": 4, "tone": "user",
                 "interactive": True},
            ],
            "video_private": [
                {"action": "manual-url", "label": "SET URL",
                 "tooltip": "Provide a different YouTube URL — the original is private.",
                 "priority": 1, "tone": "user",
                 "interactive": True},
                {"action": "upload-theme", "label": "UPLOAD MP3",
                 "tooltip": "Upload your own MP3 file.",
                 "priority": 2, "tone": "user",
                 "interactive": True},
                {"action": "clear-failure", "label": "ACK FAILURE",
                 "tooltip": "Drop this row from the topbar FAIL count. The red TDB ✗ pill stays.",
                 "priority": 9, "tone": "info",
                 "interactive": True},
            ],
            "video_removed": [
                {"action": "manual-url", "label": "SET URL",
                 "tooltip": "Provide a different YouTube URL — the original was removed.",
                 "priority": 1, "tone": "user",
                 "interactive": True},
                {"action": "upload-theme", "label": "UPLOAD MP3",
                 "tooltip": "Upload your own MP3 file.",
                 "priority": 2, "tone": "user",
                 "interactive": True},
                {"action": "clear-failure", "label": "ACK FAILURE",
                 "tooltip": "Drop this row from the topbar FAIL count. The red TDB ✗ pill stays.",
                 "priority": 9, "tone": "info",
                 "interactive": True},
            ],
            "video_age_restricted": [
                {"action": "info", "label": "DROP cookies.txt",
                 "tooltip": "Drop an age-verified YouTube cookies.txt at the configured path so yt-dlp can authenticate as a logged-in user.",
                 "priority": 1, "tone": "info",
                 "interactive": False},
                {"action": "manual-url", "label": "SET URL",
                 "tooltip": "Provide a non-age-restricted YouTube alternative.",
                 "priority": 2, "tone": "user",
                 "interactive": True},
                {"action": "upload-theme", "label": "UPLOAD MP3",
                 "tooltip": "Skip YouTube — upload your own MP3 file.",
                 "priority": 3, "tone": "user",
                 "interactive": True},
            ],
            "geo_blocked": [
                {"action": "manual-url", "label": "SET URL",
                 "tooltip": "Provide a YouTube URL that's available in your region.",
                 "priority": 1, "tone": "user",
                 "interactive": True},
                {"action": "upload-theme", "label": "UPLOAD MP3",
                 "tooltip": "Skip YouTube — upload your own MP3 file.",
                 "priority": 2, "tone": "user",
                 "interactive": True},
                {"action": "clear-failure", "label": "ACK FAILURE",
                 "tooltip": "Drop this row from the topbar FAIL count. The red TDB ✗ pill stays.",
                 "priority": 9, "tone": "info",
                 "interactive": True},
            ],
            "network_error": [
                {"action": "redl", "label": "RE-DOWNLOAD",
                 "tooltip": "Retry — network errors are usually transient.",
                 "priority": 1, "tone": "themerrdb",
                 "interactive": True},
                {"action": "manual-url", "label": "SET URL",
                 "tooltip": "If retries keep failing, the original URL may be the problem — try a different one.",
                 "priority": 2, "tone": "user",
                 "interactive": True},
                {"action": "clear-failure", "label": "ACK FAILURE",
                 "tooltip": "Drop this row from the topbar FAIL count. Re-fires on the next failed attempt.",
                 "priority": 9, "tone": "info",
                 "interactive": True},
            ],
            "unknown": [
                {"action": "redl", "label": "RE-DOWNLOAD",
                 "tooltip": "Retry the download — unknown errors are sometimes transient.",
                 "priority": 1, "tone": "themerrdb",
                 "interactive": True},
                {"action": "manual-url", "label": "SET URL",
                 "tooltip": "Provide a different YouTube URL.",
                 "priority": 2, "tone": "user",
                 "interactive": True},
                {"action": "upload-theme", "label": "UPLOAD MP3",
                 "tooltip": "Upload your own MP3 file.",
                 "priority": 3, "tone": "user",
                 "interactive": True},
                {"action": "info", "label": "CHECK LOGS",
                 "tooltip": "Open the LOGS tab to see the raw yt-dlp error message — sometimes the cause is identifiable from the trace.",
                 "priority": 4, "tone": "info",
                 "interactive": False},
                {"action": "clear-failure", "label": "ACK FAILURE",
                 "tooltip": "Drop this row from the topbar FAIL count.",
                 "priority": 9, "tone": "info",
                 "interactive": True},
            ],
        }
        humans = {
            "cookies_expired": "YouTube cookies missing or expired",
            "video_private": "Video is private",
            "video_removed": "Video was removed or deleted",
            "video_age_restricted": "Video is age-restricted",
            "geo_blocked": "Video is geo-blocked from your region",
            "network_error": "Network error reaching YouTube",
            "unknown": "Unknown error",
        }
        options = sorted(recipes.get(kind, recipes["unknown"]),
                         key=lambda o: o["priority"])
        # Orphans don't have a TDB record so RE-DOWNLOAD against
        # themes.youtube_url is meaningless. Filter it out for
        # plex_orphan rows so the user isn't pointed at a dead path.
        if is_orphan:
            options = [o for o in options if o["action"] != "redl"]
        # v1.12.88: filter ACK FAILURE out of the options when the
        # row is already acked. The user explicitly wants the recovery
        # context (failure kind + resolution paths like SET URL /
        # UPLOAD MP3) to remain visible after acking, but the ACK
        # button itself becomes redundant — the alarm is already
        # dismissed. Pre-fix clicking ACK closed the dialog and
        # required reopening to see the same recovery options.
        if row["failure_acked_at"]:
            options = [o for o in options if o["action"] != "clear-failure"]
        return {
            "failure_kind": kind,
            "failure_message": row["failure_message"],
            "human": humans.get(kind, kind),
            "needs_manual_override": kind in (
                "video_private", "video_removed",
                "video_age_restricted", "geo_blocked",
            ),
            "acked": bool(row["failure_acked_at"]),
            "rating_key": rating_key,
            "options": options,
        }

    @app.post("/api/items/{media_type}/{tmdb_id}/clear-failure")
    async def api_clear_failure(
        request: Request, media_type: MediaType, tmdb_id: int,
        db: Path = Depends(get_db_path),
    ):
        """v1.10.50: acknowledge a failure without pretending it's gone.

        Sets themes.failure_acked_at = now() — the ! glyph and the
        FAILURES filter exclude the row, but the TDB pill keeps its
        red 'TDB ✗' (or amber 'TDB ⚠') so the user knows the
        upstream URL is still broken. failure_kind is preserved.

        The underlying URL stays dead until the next sync probe
        finds an updated one or the user provides a manual override.
        Future probes / downloads that produce a *different*
        failure_kind clear failure_acked_at so the user gets
        re-alerted on the new condition.
        """
        _require_admin(request)
        with get_conn(db) as conn:
            row = conn.execute(
                "SELECT failure_kind, failure_acked_at, title FROM themes "
                "WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            ).fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail="theme not found")
            if row["failure_kind"] is None:
                return {"ok": True, "no_op": True,
                        "note": "no failure to acknowledge"}
            # v1.12.87: idempotent over the failure_acked_at flag —
            # always run the cancel sweep below even if the user has
            # ACKed before. Pre-fix the early no-op meant a second
            # ACK click after a stale-pending retry cycle produced
            # new failed jobs did NOT cancel them, so the topbar
            # stayed red. With the topbar now driven by themes (not
            # jobs) this is mostly belt-and-suspenders, but the
            # cleanup keeps the queue tidy and matches user intent
            # ("dismiss this and stop trying").
            conn.execute(
                "UPDATE themes SET failure_acked_at = ? "
                "WHERE media_type = ? AND tmdb_id = ? AND failure_acked_at IS NULL",
                (now_iso(), media_type, tmdb_id),
            )
            # v1.12.74: also cancel the underlying failed download
            # job in the queue. Pre-fix the topbar dot stayed red
            # after ACK FAILURE because q.failed counted the
            # 'failed'-status job in jobs even though the row's
            # failure was dismissed. The user's intent in clicking
            # ACK is "I'm done with this failure" — that should
            # include the queued failed job that produced the
            # failure in the first place. Marking 'cancelled' (not
            # deleting) preserves the audit trail in LOGS.
            # v1.12.85: extend the cancel to status='pending' too.
            # Pre-fix a misclassified-as-transient failure (e.g.
            # "This video is not available" before .85 added the
            # pattern) cycled between pending → failed → pending via
            # the worker's backoff retry; ACK cleared the failed row
            # but the next retry put another in pending → failed,
            # so the topbar dot kept going red and the row's `!`
            # glyph kept reappearing. Cancelling pending too breaks
            # the cycle. Running jobs are left alone (cancel_requested
            # via /jobs/{id}/cancel is the cooperative path for those).
            conn.execute(
                """UPDATE jobs SET status = 'cancelled', finished_at = ?
                   WHERE job_type = 'download'
                     AND media_type = ? AND tmdb_id = ?
                     AND status IN ('pending', 'failed')""",
                (now_iso(), media_type, tmdb_id),
            )
            _record_audit(
                conn, actor=request.state.principal.username,
                action="ack_failure",
                media_type=media_type, tmdb_id=tmdb_id,
                details={"failure_kind": row["failure_kind"]},
            )
        log_event(db, level="INFO", component="api",
                  media_type=media_type, tmdb_id=tmdb_id,
                  message=f"Failure acknowledged on '{row['title']}' "
                          f"by {request.state.principal.username}",
                  detail={"failure_kind": row["failure_kind"]})
        return {"ok": True, "title": row["title"],
                "acked_kind": row["failure_kind"]}

    @app.post("/api/items/{media_type}/{tmdb_id}/redownload")
    async def api_redownload(
        request: Request, media_type: MediaType, tmdb_id: int,
        # v1.12.73: optional section_id scopes the re-download +
        # placement to one section. Without it, the download fans
        # out to every section that owns the title — wrong when
        # the user clicks DOWNLOAD/RE-DOWNLOAD TDB on a specific
        # row (4K vs standard) and only wants that one section
        # affected. Mirrors the v1.12.46 ACCEPT UPDATE / v1.12.47
        # REVERT scoping; per-section overrides (v1.12.72) need
        # this too so a per-edition theme isn't accidentally
        # re-downloaded into sibling sections that have their own
        # different URLs.
        section_id: str | None = Query(None),
        db: Path = Depends(get_db_path),
    ):
        _require_admin(request)
        with get_conn(db) as conn:
            row = conn.execute(
                "SELECT title FROM themes WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            ).fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail="not in database")
            # Cancel any pending download for this item, then enqueue a fresh one.
            # v1.12.73: when section_id is provided, only cancel
            # jobs in that section so sibling-section downloads
            # the user isn't replacing keep running.
            if section_id:
                conn.execute(
                    """UPDATE jobs SET status = 'cancelled', finished_at = ?
                       WHERE job_type = 'download' AND media_type = ? AND tmdb_id = ?
                         AND section_id = ?
                         AND status IN ('pending', 'failed')""",
                    (now_iso(), media_type, tmdb_id, section_id),
                )
            else:
                conn.execute(
                    """UPDATE jobs SET status = 'cancelled', finished_at = ?
                       WHERE job_type = 'download' AND media_type = ? AND tmdb_id = ?
                         AND status IN ('pending', 'failed')""",
                    (now_iso(), media_type, tmdb_id),
                )
            # v1.12.47: RE-DOWNLOAD TDB / DOWNLOAD TDB (this
            # endpoint backs both labels) needs force_place=True
            # so the place job overwrites the existing
            # placement file. Pre-fix the place worker hit
            # skip_if_plex_has_theme and the new canonical
            # never reached Plex's folder — same Plex-cache bug
            # the v1.12.43 ACCEPT UPDATE fix solved.
            from ..core.sync import _enqueue_download
            n = _enqueue_download(
                conn, media_type=media_type, tmdb_id=tmdb_id,
                reason="manual",
                auto_place=True,
                force_place=True,
                only_section_id=section_id,
            )
            _record_audit(
                conn, actor=request.state.user, action="redownload",
                media_type=media_type, tmdb_id=tmdb_id,
                section_id=section_id,
                details={"sections_enqueued": n},
            )
        log_event(db, level="INFO", component="api",
                  media_type=media_type, tmdb_id=tmdb_id,
                  section_id=section_id,
                  message=f"Manual re-download requested by {request.state.user} "
                          f"({n} sections{', scoped to ' + section_id if section_id else ''})")
        return {"ok": True, "enqueued_sections": n}

    @app.post("/api/items/{media_type}/{tmdb_id}/override")
    async def api_override(
        request: Request, media_type: MediaType, tmdb_id: int,
        db: Path = Depends(get_db_path),
    ):
        """Set or replace a manual YouTube URL override for one theme.

        Accepts either JSON body {"youtube_url": "...", "note": "..."} or
        legacy form-encoded fields (youtube_url, note). Side effects:
          - upsert into user_overrides
          - clear failure_kind / failure_message / failure_at on themes
            (we're overriding the broken upstream)
          - enqueue a download job; worker prefers user_overrides
        """
        _require_admin(request)
        # Accept JSON or form
        youtube_url: str = ""
        note: str = ""
        ctype = (request.headers.get("content-type") or "").lower()
        if "application/json" in ctype:
            try:
                body = await request.json()
            except Exception:
                raise HTTPException(status_code=400, detail="invalid JSON body")
            if not isinstance(body, dict):
                raise HTTPException(status_code=400, detail="body must be a JSON object")
            youtube_url = str(body.get("youtube_url") or "")
            note = str(body.get("note") or "")
        else:
            form = await request.form()
            youtube_url = str(form.get("youtube_url") or "")
            note = str(form.get("note") or "")
        if not youtube_url:
            raise HTTPException(status_code=400, detail="youtube_url is required")
        vid = extract_video_id(youtube_url)
        if not vid:
            raise HTTPException(status_code=400, detail="invalid YouTube URL")
        canonical = f"https://www.youtube.com/watch?v={vid}"
        with get_conn(db) as conn:
            row = conn.execute(
                "SELECT 1 FROM themes WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            ).fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail="theme not in database")
            # v1.12.72: api_override has no rating_key/section_id
            # context — it's the global "override URL for this title"
            # path used by the failure dialog. Writes the '' (global)
            # row so the worker uses this URL for any section that
            # doesn't have its own per-section override.
            conn.execute(
                """INSERT INTO user_overrides (media_type, tmdb_id, youtube_url,
                                               set_at, set_by, note,
                                               section_id)
                   VALUES (?, ?, ?, ?, ?, ?, '')
                   ON CONFLICT(media_type, tmdb_id, section_id) DO UPDATE SET
                       youtube_url = excluded.youtube_url,
                       set_at = excluded.set_at,
                       set_by = excluded.set_by,
                       note = excluded.note""",
                (media_type, tmdb_id, canonical, now_iso(), request.state.user, note),
            )
            # Clear any prior failure — the override should retry from scratch
            conn.execute(
                """UPDATE themes SET failure_kind = NULL, failure_message = NULL,
                                     failure_at = NULL
                   WHERE media_type = ? AND tmdb_id = ?""",
                (media_type, tmdb_id),
            )
            # Cancel any in-flight download for this item, then enqueue
            # one fresh per-section download via _enqueue_download.
            conn.execute(
                """UPDATE jobs SET status = 'cancelled', finished_at = ?
                   WHERE job_type = 'download' AND media_type = ? AND tmdb_id = ?
                     AND status IN ('pending', 'failed')""",
                (now_iso(), media_type, tmdb_id),
            )
            # v1.12.47: SET URL via the /override dialog needs
            # force_place=True so the new URL's download
            # overwrites whatever theme.mp3 is currently in the
            # Plex folder. User intent is "use this URL" — the
            # placement should reflect it. Pre-fix the place
            # worker hit skip_if_plex_has_theme on rows that
            # already had a theme cached, so the override URL
            # downloaded into canonical but Plex kept playing
            # the old theme.
            from ..core.sync import _enqueue_download
            _enqueue_download(
                conn, media_type=media_type, tmdb_id=tmdb_id,
                reason="override",
                auto_place=True,
                force_place=True,
            )
            _record_audit(
                conn, actor=request.state.user, action="set_url",
                media_type=media_type, tmdb_id=tmdb_id,
                section_id="",
                details={"new_url": canonical, "note": note,
                         "via": "override_dialog"},
            )
        log_event(db, level="INFO", component="api",
                  media_type=media_type, tmdb_id=tmdb_id,
                  message=f"Override set by {request.state.user}: {canonical}",
                  detail={"note": note} if note else None)
        return {"ok": True, "youtube_url": canonical}

    @app.delete("/api/items/{media_type}/{tmdb_id}/override")
    async def api_clear_override(
        request: Request, media_type: MediaType, tmdb_id: int,
        db: Path = Depends(get_db_path),
    ):
        _require_admin(request)
        with get_conn(db) as conn:
            prior = conn.execute(
                "SELECT youtube_url, section_id FROM user_overrides "
                "WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            ).fetchall()
            conn.execute(
                "DELETE FROM user_overrides WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            )
            for r in prior:
                _record_audit(
                    conn, actor=request.state.user, action="clear_override",
                    media_type=media_type, tmdb_id=tmdb_id,
                    section_id=r["section_id"],
                    details={"old_url": r["youtube_url"]},
                )
        log_event(db, level="INFO", component="api",
                  media_type=media_type, tmdb_id=tmdb_id,
                  message=f"Override cleared by {request.state.user}")
        return {"ok": True}

    # --- JSON: jobs / events ---

    @app.get("/api/jobs")
    async def api_jobs(
        status: str = Query("all"),
        limit: int = Query(100, ge=1, le=500),
        db: Path = Depends(get_db_path),
    ):
        with get_conn(db) as conn:
            if status == "all":
                rows = conn.execute(
                    "SELECT * FROM jobs ORDER BY id DESC LIMIT ?", (limit,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM jobs WHERE status = ? ORDER BY id DESC LIMIT ?",
                    (status, limit),
                ).fetchall()
        return {"jobs": [dict(r) for r in rows]}

    @app.post("/api/jobs/{job_id}/cancel")
    async def api_cancel_job(
        request: Request, job_id: int,
        force: bool = Query(False),
        db: Path = Depends(get_db_path),
    ):
        """v1.11.36: cooperative job cancellation.

        - Pending jobs flip to status='cancelled' immediately (the
          worker's pre-dispatch cancel-check or this endpoint catches
          them before they ever run).
        - Running jobs get cancel_requested=1; the worker handler
          notices on the next safe yield (between sync batches /
          plex_enum sections / scan iterations / etc.) and bails out.
        - Already-finished jobs (done / failed / cancelled) return 409.

        v1.11.53: ?force=true escalates to a hard cancel — the row
        flips straight to status='cancelled' regardless of the
        cooperative path. Used when the worker thread has wedged
        (DB lock contention, stuck on a Plex API call, etc.) and
        the cancel_requested flag never gets read. The UI surfaces
        a × FORCE button on rows already marked cancelling so the
        user has a one-click escape from a stuck-cancelling job.
        """
        _require_admin(request)
        with get_conn(db) as conn:
            row = conn.execute(
                "SELECT status, cancel_requested FROM jobs WHERE id = ?",
                (job_id,),
            ).fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail="job not found")
            status_now = row["status"]
            if status_now == "pending":
                conn.execute(
                    "UPDATE jobs SET status = 'cancelled', "
                    "                finished_at = ?, "
                    "                last_error = 'cancelled by user', "
                    "                cancel_requested = 0 "
                    "WHERE id = ?",
                    (now_iso(), job_id),
                )
                outcome = "cancelled"
            elif status_now == "running":
                if force:
                    conn.execute(
                        "UPDATE jobs SET status = 'cancelled', "
                        "                finished_at = ?, "
                        "                last_error = 'force-cancelled by user', "
                        "                cancel_requested = 0 "
                        "WHERE id = ?",
                        (now_iso(), job_id),
                    )
                    outcome = "force_cancelled"
                else:
                    conn.execute(
                        "UPDATE jobs SET cancel_requested = 1 WHERE id = ?",
                        (job_id,),
                    )
                    outcome = "cancel_requested"
            else:
                raise HTTPException(
                    status_code=409,
                    detail=f"job is already in terminal state '{status_now}'",
                )
        log_event(db, level="INFO", component="api",
                  message=f"Job {job_id} cancellation requested by "
                          f"{request.state.principal.username}",
                  detail={"prev_status": status_now, "outcome": outcome,
                          "force": bool(force)})
        return {"ok": True, "outcome": outcome, "prev_status": status_now}

    @app.post("/api/jobs/clear-failed")
    async def api_clear_failed_jobs(
        request: Request, db: Path = Depends(get_db_path),
    ):
        """v1.11.73 / v1.12.12: acknowledge every unacked failed
        job. Pre-1.12.12 this hard-deleted the rows, throwing away
        the historical record of what failed. Now it stamps
        jobs.acked_at so the row stays put — renders in green
        ('ACKNOWLEDGED'), drops out of the topbar failure count,
        but stays filterable / sortable for later review.
        """
        _require_admin(request)
        with get_conn(db) as conn:
            cur = conn.execute(
                "UPDATE jobs SET acked_at = ? "
                "WHERE status = 'failed' AND acked_at IS NULL",
                (now_iso(),),
            )
            cleared = cur.rowcount or 0
        log_event(db, level="INFO", component="api",
                  message=f"Acknowledged {cleared} failed job(s) by "
                          f"{request.state.principal.username}",
                  detail={"acknowledged": cleared})
        return {"ok": True, "acknowledged": cleared}

    @app.get("/api/events")
    async def api_events(
        limit: int = Query(200, ge=1, le=1000),
        component: str = Query(""),
        level: str = Query(""),
        db: Path = Depends(get_db_path),
    ):
        sql = "SELECT * FROM events"
        clauses = []
        params: list = []
        if component:
            clauses.append("component = ?"); params.append(component)
        if level:
            clauses.append("level = ?"); params.append(level.upper())
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(limit)
        with get_conn(db) as conn:
            rows = conn.execute(sql, params).fetchall()
        return {"events": [dict(r) for r in rows]}

    # v1.11.76: diagnostic endpoint for per-folder stat behavior. The
    # M-vs-P false-positive on Unraid setups (Plex says theme=present
    # via its metadata cache, motif's stat reads 0) is hard to debug
    # without seeing exactly what Python's iterdir() actually returns
    # for the problematic path. Dump everything the sidecar logic
    # observes so the user can paste the JSON back when they hit the
    # bug.
    @app.get("/api/debug/stat-folder")
    async def api_debug_stat_folder(
        request: Request,
        path: str = Query(..., description="absolute folder path"),
    ):
        _require_admin(request)
        from ..core.plex_enum import (
            _candidate_local_paths, stat_theme_sidecar,
            SIDECAR_AUDIO_EXTS,
        )
        out = {
            "input_path": path,
            "candidates": [],
            "sidecar_result": None,
        }
        for cand in _candidate_local_paths(path):
            entry = {
                "candidate": str(cand),
                "exists": False,
                "is_dir": False,
                "iterdir_error": None,
                "entries": [],
            }
            try:
                entry["exists"] = cand.exists()
            except OSError as e:
                entry["iterdir_error"] = f"exists() raised: {e}"
            try:
                entry["is_dir"] = cand.is_dir()
            except OSError as e:
                entry["iterdir_error"] = f"is_dir() raised: {e}"
            if entry["is_dir"]:
                try:
                    for ent in cand.iterdir():
                        try:
                            is_file = ent.is_file()
                        except OSError as e:
                            entry["entries"].append({
                                "name": ent.name,
                                "is_file": None,
                                "stat_error": str(e),
                            })
                            continue
                        item = {"name": ent.name, "is_file": is_file}
                        nm = ent.name.lower()
                        if is_file and nm.startswith("theme."):
                            ext = nm[len("theme"):]
                            item["theme_ext"] = ext
                            item["matches_audio_ext"] = (
                                ext in SIDECAR_AUDIO_EXTS
                            )
                        entry["entries"].append(item)
                except OSError as e:
                    entry["iterdir_error"] = f"iterdir() raised: {e}"
            out["candidates"].append(entry)
            # Stop at the first reachable candidate (mirrors
            # stat_theme_sidecar's actual logic).
            if entry["is_dir"]:
                break
        out["sidecar_result"] = stat_theme_sidecar(path)
        return out

    # --- JSON: coverage ---

    @app.get("/api/coverage/plex")
    async def api_coverage_plex(db: Path = Depends(get_db_path)):
        """v1.11.38: serve from plex_items (populated by plex_enum) instead
        of hitting Plex's /library/sections/{id}/all live on every
        dashboard load. The previous live call used the client-wide
        10s timeout — on a large section it'd ReadTimeout, the request
        would hang ~10s, and dashboard → library navigation felt
        sluggish even when the worker wasn't doing anything heavy.
        plex_items is a DB-side mirror of the same data; refreshing
        the cache is what plex_enum is for.
        """
        if not settings.plex_enabled or not settings.plex_token:
            return {"enabled": False, "items": []}
        with get_conn(db) as conn:
            # Cross-reference plex_items + themes via the cached
            # theme_id (v1.11.26). 'motif_available' is True iff the
            # row resolves to a real (non-orphan) themes record.
            movies_rows = conn.execute("""
                SELECT pi.rating_key, pi.title, pi.year, pi.has_theme,
                       t.tmdb_id, t.upstream_source
                FROM plex_items pi
                INNER JOIN plex_sections ps
                  ON ps.section_id = pi.section_id AND ps.included = 1
                LEFT JOIN themes t ON t.id = pi.theme_id
                WHERE pi.media_type = 'movie'
                ORDER BY pi.title COLLATE NOCASE
            """).fetchall()
            tv_rows = conn.execute("""
                SELECT pi.rating_key, pi.title, pi.year, pi.has_theme,
                       t.tmdb_id, t.upstream_source
                FROM plex_items pi
                INNER JOIN plex_sections ps
                  ON ps.section_id = pi.section_id AND ps.included = 1
                LEFT JOIN themes t ON t.id = pi.theme_id
                WHERE pi.media_type = 'show'
                ORDER BY pi.title COLLATE NOCASE
            """).fetchall()

        def to_item(r):
            return {
                "rating_key": r["rating_key"],
                "title": r["title"],
                "year": r["year"],
                "has_theme": bool(r["has_theme"]),
                "motif_available": bool(
                    r["tmdb_id"] is not None
                    and r["upstream_source"] != "plex_orphan"
                ),
                "tmdb_id": r["tmdb_id"],
            }

        return {
            "enabled": True,
            "movies": [to_item(r) for r in movies_rows],
            "tv": [to_item(r) for r in tv_rows],
        }

    # --- JSON: sync ---

    @app.post("/api/sync/now")
    async def api_sync_now(request: Request, db: Path = Depends(get_db_path)):
        _require_admin(request)
        # Optional body:
        #   {"metadata_only": true}  — pull ThemerrDB updates only;
        #                              do NOT enqueue any download jobs.
        #   {"download_only": true}  — sync + download, but stage in /pending
        #                              instead of auto-placing.
        #   {"download_only": false} — sync + download + auto-place.
        # Default (no body): metadata_only behaviour, matching the v1.7+
        # dashboard's single SYNC button.
        try:
            body = await request.json()
        except Exception:
            body = {}
        payload: dict = {}
        if isinstance(body, dict):
            if body.get("metadata_only"):
                payload["enqueue_downloads"] = False
            elif "download_only" in body:
                payload["auto_place"] = not bool(body["download_only"])
        payload_json = json.dumps(payload) if payload else "{}"
        with get_conn(db) as conn:
            existing = conn.execute(
                "SELECT id FROM jobs WHERE job_type = 'sync' AND status IN ('pending','running')"
            ).fetchone()
            if existing:
                return {"ok": True, "job_id": existing["id"], "already_queued": True}
            cur = conn.execute(
                """INSERT INTO jobs (job_type, payload, status, created_at, next_run_at)
                   VALUES ('sync', ?, 'pending', ?, ?)""",
                (payload_json, now_iso(), now_iso()),
            )
            job_id = cur.lastrowid
        log_event(db, level="INFO", component="api",
                  message=f"Manual sync queued by {request.state.user}",
                  detail={"download_only": bool(body.get("download_only", False))} if isinstance(body, dict) else None)
        return {"ok": True, "job_id": job_id}

    @app.get("/api/progress")
    async def api_progress(db: Path = Depends(get_db_path)):
        """v1.12.106: live progress feed for the ops side-drawer.

        Returns currently-running ops + a tail of recently-finished
        ops so the drawer can show "last completion" summaries when
        nothing is active. Polled at 1s by the UI when any op is
        running, 10s when idle.

        Each row carries: stage label, stage_current/total,
        processed_total, error_count, throughput history (drives
        the items/sec sparkline + ETA), and the rolling activity
        feed. The drawer renders one card per row.
        """
        from ..core import progress as op_progress
        rows = await run_in_threadpool(op_progress.load_active, db)
        # Cheap on the read path so we don't need a dedicated sweeper.
        await run_in_threadpool(op_progress.prune_finished, db)
        return {"ops": rows, "now": now_iso()}

    @app.post("/api/progress/{op_id}/cancel")
    async def api_progress_cancel(
        op_id: str, request: Request, db: Path = Depends(get_db_path),
    ):
        """v1.12.106: signal cooperative cancellation. Sets
        op_progress.status='cancelling'; the worker checks at its
        next checkpoint and bails (raises _JobCancelled), then the
        finally path stamps status='cancelled' + finished_at.

        Returns 409 if no running op matches op_id (already finished
        or never existed) so the UI can refresh its state.
        """
        _require_admin(request)
        from ..core import progress as op_progress
        ok = await run_in_threadpool(op_progress.request_cancel, db, op_id)
        if not ok:
            raise HTTPException(
                status_code=409,
                detail="op not running (already finished or unknown id)",
            )
        log_event(db, level="INFO", component="api",
                  message=f"Cancel requested for op {op_id} by {request.state.user}")
        return {"ok": True}

    @app.get("/healthz")
    async def healthz():
        return JSONResponse({"status": "ok"})

    return app
