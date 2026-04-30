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

from fastapi import Depends, FastAPI, Form, HTTPException, Query, Request, status
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
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
from ..core.db import get_conn
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
        # Preserve original URL so we can redirect back after login
        next_url = request.url.path
        if request.url.query:
            next_url += "?" + request.url.query
        return RedirectResponse(f"/login?next={next_url}", status_code=302)


def _require_admin(request: Request) -> Principal:
    p: Principal = request.state.principal
    if not p.is_admin:
        raise HTTPException(status_code=403, detail="admin scope required")
    return p


# v1.10.15: column-sort whitelist. Each entry maps the ?sort= value
# accepted by /api/library to a SQL ORDER BY snippet for the main row
# query. Wrapping in a whitelist keeps the param SQL-injection-safe.
# 'src' uses a CASE expression that mirrors the badge precedence:
# T → U → A → M → P → — so sort-by-source groups rows visually the same
# way the SRC column does.
_LIBRARY_SORTS_MAIN = {
    "title": "pi.title COLLATE NOCASE",
    "year":  "pi.year",
    "imdb":  "pi.guid_imdb",
    "src": (
        "CASE "
        "WHEN p.provenance = 'auto' THEN 1 "
        "WHEN p.provenance = 'manual' AND lf.source_kind = 'adopt' THEN 2 "
        "WHEN p.provenance = 'manual' THEN 3 "
        "WHEN pi.local_theme_file = 1 THEN 4 "
        "WHEN pi.has_theme = 1 THEN 5 "
        "ELSE 6 END"
    ),
    "dl":    "CASE WHEN lf.file_path IS NOT NULL THEN 0 ELSE 1 END",
    "pl":    "CASE WHEN p.media_folder IS NOT NULL THEN 0 ELSE 1 END",
    "link":  "CASE p.placement_kind WHEN 'hardlink' THEN 0 WHEN 'copy' THEN 1 ELSE 2 END",
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
        where_extra += (" AND (t.tmdb_id IS NULL OR t.upstream_source = 'plex_orphan') "
                        "AND pi.has_theme = 0 "
                        "AND pi.local_theme_file = 0 "
                        "AND lf.file_path IS NULL "
                        "AND p.media_folder IS NULL")
    elif status == "downloaded":
        # motif has the theme file on disk (placed or not). Mirrors the
        # DL pill being lit in the row.
        where_extra += " AND lf.file_path IS NOT NULL"
    elif status == "placed":
        where_extra += " AND p.media_folder IS NOT NULL"
    elif status == "unplaced":
        where_extra += " AND lf.file_path IS NOT NULL AND p.media_folder IS NULL"
    elif status == "failures":
        where_extra += " AND t.failure_kind IS NOT NULL"

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

    sql_select = """
        SELECT pi.rating_key, pi.section_id, pi.media_type AS plex_media_type,
               pi.title AS plex_title, pi.year, pi.guid_imdb, pi.guid_tmdb,
               pi.folder_path, pi.has_theme AS plex_has_theme,
               pi.local_theme_file AS plex_local_theme,
               ps.title AS section_title,
               t.tmdb_id AS theme_tmdb, t.media_type AS theme_media_type,
               t.title AS theme_title, t.youtube_url, t.youtube_video_id,
               t.failure_kind, t.failure_message, t.upstream_source,
               lf.file_path, lf.source_video_id, lf.provenance, lf.source_kind,
               p.media_folder, p.placement_kind, p.provenance AS placement_provenance,
               (SELECT j.job_type FROM jobs j
                WHERE j.media_type = t.media_type AND j.tmdb_id = t.tmdb_id
                  AND j.job_type IN ('download', 'place')
                  AND j.status IN ('pending', 'running')
                ORDER BY CASE j.status WHEN 'running' THEN 0 ELSE 1 END,
                         j.id DESC
                LIMIT 1) AS job_in_flight
    """
    sql_from = """
        FROM plex_items pi
        INNER JOIN plex_sections ps
          ON ps.section_id = pi.section_id AND ps.included = 1
        LEFT JOIN themes t ON t.id = (
            SELECT t2.id FROM themes t2
            WHERE t2.media_type = (CASE pi.media_type WHEN 'show' THEN 'tv' ELSE pi.media_type END)
              AND (
                (t2.tmdb_id = pi.guid_tmdb AND pi.guid_tmdb IS NOT NULL
                 AND t2.upstream_source != 'plex_orphan')
                OR (t2.imdb_id = pi.guid_imdb AND pi.guid_imdb IS NOT NULL
                    AND t2.upstream_source = 'plex_orphan')
                -- v1.10.32: title+year fallback. Fires only when Plex's
                -- guid_tmdb didn't match anything in themes (e.g.
                -- 'Twelve Monkeys' in TDB but Plex has '12 Monkeys'
                -- with a stale tmdb pointing elsewhere). Constrained
                -- to ThemerrDB-real rows so we don't accidentally
                -- claim an orphan row by title.
                OR (
                  t2.upstream_source != 'plex_orphan'
                  AND pi.title_norm IS NOT NULL
                  AND t2.title_norm = pi.title_norm
                  AND pi.year IS NOT NULL
                  AND t2.year = pi.year
                  AND NOT EXISTS (
                    SELECT 1 FROM themes t3
                    WHERE t3.media_type = t2.media_type
                      AND ((t3.tmdb_id = pi.guid_tmdb AND pi.guid_tmdb IS NOT NULL
                            AND t3.upstream_source != 'plex_orphan')
                           OR (t3.imdb_id = pi.guid_imdb AND pi.guid_imdb IS NOT NULL))
                  )
                )
              )
            ORDER BY CASE WHEN t2.upstream_source = 'plex_orphan' THEN 1 ELSE 0 END,
                     t2.id DESC
            LIMIT 1
        )
        LEFT JOIN local_files lf
          ON lf.media_type = t.media_type AND lf.tmdb_id = t.tmdb_id
        LEFT JOIN placements p
          ON p.media_type = t.media_type AND p.tmdb_id = t.tmdb_id
    """
    sql_from_pi_only = """
        FROM plex_items pi
        INNER JOIN plex_sections ps
          ON ps.section_id = pi.section_id AND ps.included = 1
    """
    sql_where = f"WHERE {tab_where}{where_extra}"
    # Fast-path COUNT: status='all' AND tdb='any' (the default tab load)
    # only references plex_items/plex_sections columns, so we can skip
    # the heavy LEFT JOIN themes correlated subquery + lf/p joins. The
    # tdb filter requires the themes join, so it falls through to the
    # full-join path when set.
    if status == "all" and tdb == "any":
        sql_count = (f"SELECT COUNT(*) {sql_from_pi_only} "
                     f"WHERE {tab_where}{where_pi_only}")
        count_params = params
    else:
        sql_count = f"SELECT COUNT(*) {sql_from} {sql_where}"
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
    sql_meta = """
        SELECT
            EXISTS(SELECT 1 FROM plex_items LIMIT 1)             AS plex_enumerated,
            (SELECT MAX(finished_at) FROM jobs
              WHERE job_type = 'plex_enum' AND status = 'success') AS last_plex_enum_at,
            (SELECT MAX(finished_at) FROM sync_runs
              WHERE status = 'success')                            AS last_sync_at
    """
    offset = (page - 1) * per_page
    with get_conn(db) as conn:
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
    return {"total": total, "missing_count": missing_count,
            "page": page, "per_page": per_page,
            "tab": tab, "fourk": fourk, "items": items,
            "plex_enumerated": plex_enumerated,
            "plex_scan_stale": plex_scan_stale,
            "last_plex_enum_at": last_plex_enum_at,
            "last_sync_at": last_sync_at}


def _library_not_in_plex(
    db: Path, *, tab: str, fourk: bool,
    q: str, page: int, per_page: int,
    sort: str = "title", sort_dir: str = "asc",
) -> dict:
    """Return ThemerrDB rows whose tmdb_id has no matching plex_items
    in the requested tab. Synthesizes plex-shaped fields so the frontend
    renderer treats them as just-another-row, with not_in_plex=1 driving
    the distinct visual treatment.

    Anime tab returns empty — themes table has no anime signal so we
    can't tell which ThemerrDB rows would belong there.
    """
    if tab == "anime":
        return {"total": 0, "missing_count": 0,
                "page": page, "per_page": per_page,
                "tab": tab, "fourk": fourk, "items": [],
                "plex_enumerated": True, "plex_scan_stale": False,
                "last_plex_enum_at": None, "last_sync_at": None}
    media_type = "movie" if tab == "movies" else "tv"
    plex_media_type = "movie" if tab == "movies" else "show"
    params: list = [media_type, plex_media_type, 1 if fourk else 0]
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
                AND ps2.is_anime = 0
                AND ps2.is_4k = ?
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
            t.failure_kind, t.failure_message, t.upstream_source,
            lf.file_path, lf.source_video_id, lf.provenance, lf.source_kind,
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
            "tab": "tv", "title": "TV",
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

    @app.get("/pending", response_class=HTMLResponse)
    async def pending_page(request: Request):
        return templates.TemplateResponse(request, "pending.html")

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

    # --- JSON: public stats (no auth) ---

    @app.get("/api/public/stats")
    async def api_public_stats(db: Path = Depends(get_db_path)):
        """Public, unauthenticated stats endpoint shaped for Homepage's
        Custom API widget. Exposes only counters — no titles, no paths,
        no sensitive data."""
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
                  (SELECT COUNT(*) FROM jobs WHERE status = 'failed') AS failed,
                  (SELECT COUNT(*) FROM pending_updates WHERE decision = 'pending') AS updates_pending,
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
                  (SELECT COUNT(*) FROM jobs WHERE status = 'failed') AS failed,
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
                  -- Local files with no placement = items waiting placement
                  -- approval (typically because a sidecar already exists at
                  -- the Plex folder; user must approve overwrite via /pending).
                  (SELECT COUNT(*) FROM local_files lf
                   LEFT JOIN placements p
                     ON p.media_type = lf.media_type AND p.tmdb_id = lf.tmdb_id
                   WHERE p.media_folder IS NULL) AS pending_placements,
                  (SELECT COUNT(*) FROM pending_updates WHERE decision = 'pending') AS updates_pending,
                  (SELECT COUNT(*) FROM themes WHERE failure_kind IS NOT NULL) AS failures_total,
                  (SELECT COUNT(*) FROM themes
                   WHERE failure_kind IN ('video_private','video_removed','video_age_restricted','geo_blocked')
                  ) AS failures_unavailable,
                  (SELECT COUNT(*) FROM themes WHERE failure_kind = 'cookies_expired') AS failures_cookies,
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
            dry = is_dry_run(db, default=settings.dry_run_default)
        return row, last_sync, dry

    @app.get("/api/stats")
    async def api_stats(db: Path = Depends(get_db_path)):
        row, last_sync, dry = await run_in_threadpool(_stats_sync, db)
        return {
            "movies": {
                "total": row["movies_total"],
                "downloaded": row["movies_dl"],
                "placed": row["movies_placed"],
            },
            "tv": {
                "total": row["tv_total"],
                "downloaded": row["tv_dl"],
                "placed": row["tv_placed"],
            },
            "queue": {
                "pending": row["pending"],
                "running": row["running"],
                "failed": row["failed"],
                "sync_in_flight": row["sync_in_flight"],
                "themerrdb_sync_in_flight": row["themerrdb_sync_in_flight"],
                "plex_enum_in_flight": row["plex_enum_in_flight"],
                "pending_placements": row["pending_placements"],
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
            },
            "failures": {
                "total": row["failures_total"],
                "unavailable": row["failures_unavailable"],
                "cookies_expired": row["failures_cookies"],
            },
            "config": {
                "paths_ready": settings.is_paths_ready(),
                "themes_dir": str(settings.themes_dir) if settings.is_paths_ready() else None,
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

        # If themes_dir is now set, create subdirs
        if settings.is_paths_ready():
            try:
                settings.themes_dir.mkdir(parents=True, exist_ok=True)
                settings.movies_themes_dir.mkdir(parents=True, exist_ok=True)
                settings.tv_themes_dir.mkdir(parents=True, exist_ok=True)
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
                SELECT p.media_type, p.tmdb_id, p.media_folder, p.placed_at,
                       p.placement_kind, t.title, t.year,
                       lf.file_path AS source_path, lf.file_size
                FROM placements p
                JOIN themes t
                  ON t.media_type = p.media_type AND t.tmdb_id = p.tmdb_id
                JOIN local_files lf
                  ON lf.media_type = p.media_type AND lf.tmdb_id = p.tmdb_id
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
        params.append(section_id)
        with get_conn(db) as conn:
            cur = conn.execute(
                f"UPDATE plex_sections SET {', '.join(sets)} WHERE section_id = ?",
                params,
            )
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="section not found")
            row = conn.execute(
                "SELECT is_anime, is_4k FROM plex_sections WHERE section_id = ?",
                (section_id,),
            ).fetchone()
        log_event(db, level="INFO", component="api",
                  message=f"Library section {section_id} flags updated by "
                          f"{request.state.principal.username}",
                  detail={"is_anime": bool(row["is_anime"]),
                          "is_4k": bool(row["is_4k"])})
        return {"ok": True, "is_anime": bool(row["is_anime"]),
                "is_4k": bool(row["is_4k"])}

    @app.post("/api/libraries/refresh")
    async def api_refresh_libraries(
        request: Request, db: Path = Depends(get_db_path),
    ):
        """Manually trigger Plex section discovery."""
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
            log_event(db, level="INFO", component="api",
                      message=f"Manual library refresh by {request.state.principal.username}: "
                              f"{len(sections)} sections")
            return {"ok": True, "sections_count": len(sections)}
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Plex unreachable: {e}")

    # --- Pending updates ---

    @app.get("/api/pending")
    async def api_pending(db: Path = Depends(get_db_path)):
        """List items downloaded into the staging area but not yet placed
        into a Plex media folder. These are awaiting user approval when the
        global placement.auto_place is False, or when a sync was kicked off
        with download_only=true."""
        with get_conn(db) as conn:
            # Aggregate plex_items so we can flag rows whose Plex folder
            # already has a sidecar theme.mp3 — approval would overwrite it.
            # MAX over plex_items.local_theme_file in case a theme covers
            # multiple folders (rare, but defensive).
            rows = conn.execute("""
                SELECT t.media_type, t.tmdb_id, t.imdb_id, t.title, t.year,
                       t.youtube_url, t.youtube_video_id, t.upstream_source,
                       lf.file_path, lf.file_size, lf.downloaded_at,
                       lf.source_video_id, lf.provenance, lf.source_kind,
                       COALESCE(MAX(pi.local_theme_file), 0) AS plex_local_theme,
                       COALESCE(MAX(pi.has_theme), 0) AS plex_has_theme
                FROM local_files lf
                JOIN themes t
                  ON t.media_type = lf.media_type AND t.tmdb_id = lf.tmdb_id
                LEFT JOIN placements p
                  ON p.media_type = lf.media_type AND p.tmdb_id = lf.tmdb_id
                LEFT JOIN plex_items pi
                  ON pi.guid_tmdb = t.tmdb_id
                 AND pi.media_type = (CASE t.media_type WHEN 'tv' THEN 'show' ELSE t.media_type END)
                WHERE p.media_folder IS NULL
                GROUP BY t.media_type, t.tmdb_id
                ORDER BY MAX(lf.downloaded_at) DESC
            """).fetchall()
        return {"items": [dict(r) for r in rows]}

    @app.get("/api/pending/count")
    async def api_pending_count(db: Path = Depends(get_db_path)):
        with get_conn(db) as conn:
            row = conn.execute("""
                SELECT COUNT(*) FROM local_files lf
                LEFT JOIN placements p
                  ON p.media_type = lf.media_type AND p.tmdb_id = lf.tmdb_id
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
        targets: list[tuple[str, int]] = []
        with get_conn(db) as conn:
            if isinstance(body, dict) and body.get("all"):
                rows = conn.execute("""
                    SELECT lf.media_type, lf.tmdb_id FROM local_files lf
                    LEFT JOIN placements p
                      ON p.media_type = lf.media_type AND p.tmdb_id = lf.tmdb_id
                    WHERE p.media_folder IS NULL
                """).fetchall()
                targets = [(r["media_type"], r["tmdb_id"]) for r in rows]
            elif isinstance(body, dict) and isinstance(body.get("items"), list):
                for it in body["items"]:
                    if isinstance(it, dict) and "media_type" in it and "tmdb_id" in it:
                        targets.append((str(it["media_type"]), int(it["tmdb_id"])))
            enqueued = 0
            for media_type, tmdb_id in targets:
                # Skip if a place job is already pending/running for this item
                existing = conn.execute(
                    """SELECT id FROM jobs WHERE job_type = 'place'
                       AND media_type = ? AND tmdb_id = ?
                       AND status IN ('pending','running')""",
                    (media_type, tmdb_id),
                ).fetchone()
                if existing:
                    continue
                # User-approved placements bypass the plex_has_theme guard:
                # the user explicitly chose to use motif's file, even if a
                # sidecar exists. force=true makes the worker pass
                # skip_if_plex_has_theme=False to placement.
                conn.execute(
                    """INSERT INTO jobs (job_type, media_type, tmdb_id, payload, status,
                                         created_at, next_run_at)
                       VALUES ('place', ?, ?, '{"force":true,"reason":"approved_from_pending"}',
                               'pending', ?, ?)""",
                    (media_type, tmdb_id, now_iso(), now_iso()),
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
        targets: list[tuple[str, int]] = []
        if isinstance(body, dict) and isinstance(body.get("items"), list):
            for it in body["items"]:
                if isinstance(it, dict) and "media_type" in it and "tmdb_id" in it:
                    targets.append((str(it["media_type"]), int(it["tmdb_id"])))
        if not targets:
            return {"ok": True, "discarded": 0}
        themes_dir = settings.themes_dir
        discarded = 0
        with get_conn(db) as conn:
            for media_type, tmdb_id in targets:
                # Don't discard if already placed — that's a different operation
                placed = conn.execute(
                    "SELECT 1 FROM placements WHERE media_type = ? AND tmdb_id = ?",
                    (media_type, tmdb_id),
                ).fetchone()
                if placed:
                    continue
                row = conn.execute(
                    "SELECT file_path FROM local_files WHERE media_type = ? AND tmdb_id = ?",
                    (media_type, tmdb_id),
                ).fetchone()
                if row and themes_dir:
                    abs_path = themes_dir / row["file_path"]
                    try:
                        if abs_path.is_file():
                            abs_path.unlink()
                    except OSError as e:
                        log.warning("Discard failed to unlink %s: %s", abs_path, e)
                conn.execute(
                    "DELETE FROM local_files WHERE media_type = ? AND tmdb_id = ?",
                    (media_type, tmdb_id),
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
        db: Path = Depends(get_db_path),
    ):
        """Accept an upstream theme update: enqueue a re-download with the new URL.
        Works for both auto and manual provenance — accepting overrides whatever
        was there. Note: a manual override URL (in user_overrides) takes precedence
        in the worker, so to actually replace a manual theme with the upstream
        ThemerrDB version, the user must also clear their override."""
        _require_admin(request)
        with get_conn(db) as conn:
            update = conn.execute(
                "SELECT * FROM pending_updates WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            ).fetchone()
            if update is None:
                raise HTTPException(status_code=404, detail="no pending update")
            # Mark accepted
            conn.execute(
                """UPDATE pending_updates SET decision = 'accepted',
                       decision_at = ?, decision_by = ?
                   WHERE media_type = ? AND tmdb_id = ?""",
                (now_iso(), request.state.principal.username, media_type, tmdb_id),
            )
            # Enqueue download
            conn.execute(
                """INSERT INTO jobs (job_type, media_type, tmdb_id, payload, status,
                                     created_at, next_run_at)
                   VALUES ('download', ?, ?, ?, 'pending', ?, ?)""",
                (media_type, tmdb_id,
                 '{"reason":"upstream_update_accepted"}', now_iso(), now_iso()),
            )
        log_event(db, level="INFO", component="api",
                  media_type=media_type, tmdb_id=tmdb_id,
                  message=f"Update accepted by {request.state.principal.username}")
        return {"ok": True}

    @app.post("/api/updates/{media_type}/{tmdb_id}/decline")
    async def api_decline_update(
        request: Request, media_type: MediaType, tmdb_id: int,
        db: Path = Depends(get_db_path),
    ):
        """Decline an upstream update: keep the current theme. The decline is
        sticky — won't re-prompt unless ThemerrDB updates again."""
        _require_admin(request)
        with get_conn(db) as conn:
            cur = conn.execute(
                """UPDATE pending_updates SET decision = 'declined',
                       decision_at = ?, decision_by = ?
                   WHERE media_type = ? AND tmdb_id = ? AND decision = 'pending'""",
                (now_iso(), request.state.principal.username, media_type, tmdb_id),
            )
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="no pending update")
        log_event(db, level="INFO", component="api",
                  media_type=media_type, tmdb_id=tmdb_id,
                  message=f"Update declined by {request.state.principal.username}")
        return {"ok": True}

    # --- JSON: items ---

    @app.get("/api/library")
    async def api_library(
        tab: str = Query(..., pattern="^(movies|tv|anime)$"),
        fourk: bool = Query(False),
        q: str = Query(""),
        status: str = Query("all", pattern="^(all|themed|manual|plex_agent|untracked|downloaded|placed|unplaced|failures|not_in_plex)$"),
        tdb: str = Query("any", pattern="^(any|tracked|untracked)$"),
        page: int = Query(1, ge=1),
        per_page: int = Query(50, ge=1, le=200),
        sort: str = Query("title", pattern="^(title|year|src|dl|pl|link|imdb)$"),
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
        return await run_in_threadpool(
            _library_main_query, db, tab=tab, fourk=fourk,
            q=q, status=status, tdb=tdb, page=page, per_page=per_page,
            sort=sort, sort_dir=sort_dir,
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
                # Skip if there's already a pending/running download
                existing = conn.execute(
                    "SELECT 1 FROM jobs WHERE job_type = 'download' "
                    "AND media_type = ? AND tmdb_id = ? "
                    "AND status IN ('pending','running')",
                    (mt, tid),
                ).fetchone()
                if existing:
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
                conn.execute(
                    "INSERT INTO jobs (job_type, media_type, tmdb_id, payload, status, "
                    "                  created_at, next_run_at) "
                    "VALUES ('download', ?, ?, '{\"reason\":\"bulk_select\"}', 'pending', ?, ?)",
                    (mt, tid, now_iso(), now_iso()),
                )
                enqueued += 1
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
        with get_conn(db) as conn:
            rows = conn.execute(f"""
                SELECT t.media_type, t.tmdb_id
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
            """).fetchall()
            for r in rows:
                # Dedupe: skip if a download is already pending/running
                existing = conn.execute(
                    "SELECT 1 FROM jobs WHERE job_type = 'download' "
                    "AND media_type = ? AND tmdb_id = ? "
                    "AND status IN ('pending','running')",
                    (r["media_type"], r["tmdb_id"]),
                ).fetchone()
                if existing:
                    continue
                conn.execute(
                    "INSERT INTO jobs (job_type, media_type, tmdb_id, payload, status, "
                    "                  created_at, next_run_at) "
                    "VALUES ('download', ?, ?, '{\"reason\":\"bulk_missing\"}', 'pending', ?, ?)",
                    (r["media_type"], r["tmdb_id"], now_iso(), now_iso()),
                )
                enqueued += 1
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
            if theme is None:
                # Allocate synthetic negative tmdb_id (consistent with orphan adopt)
                row = conn.execute(
                    "SELECT MIN(tmdb_id) AS lo FROM themes "
                    "WHERE media_type = ? AND tmdb_id < 0",
                    (theme_media_type,),
                ).fetchone()
                min_tmdb = row["lo"] if row and row["lo"] is not None else 0
                tmdb_id = min(min_tmdb, 0) - 1
                conn.execute(
                    """INSERT INTO themes
                         (media_type, tmdb_id, imdb_id, title, year,
                          upstream_source, last_seen_sync_at, first_seen_sync_at)
                       VALUES (?, ?, ?, ?, ?, 'plex_orphan', ?, ?)""",
                    (theme_media_type, tmdb_id, pi["guid_imdb"],
                     pi["title"], pi["year"], now_iso(), now_iso()),
                )
            else:
                tmdb_id = theme["tmdb_id"]

        # Write the file under the canonical layout
        from ..core.canonical import canonical_theme_subdir
        media_root = (settings.movies_themes_dir if theme_media_type == "movie"
                      else settings.tv_themes_dir)
        out_dir = media_root / canonical_theme_subdir(pi["title"], pi["year"])
        out_dir.mkdir(parents=True, exist_ok=True)
        target = out_dir / "theme.mp3"
        target.write_bytes(data)

        # Compute sha256 + insert local_files
        import hashlib
        sha = hashlib.sha256(data).hexdigest()
        rel_path = str(target.relative_to(settings.themes_dir))

        with get_conn(db) as conn:
            conn.execute(
                """INSERT INTO local_files
                     (media_type, tmdb_id, file_path, file_sha256, file_size,
                      downloaded_at, source_video_id, provenance, source_kind)
                   VALUES (?, ?, ?, ?, ?, ?, '', 'manual', 'upload')
                   ON CONFLICT(media_type, tmdb_id) DO UPDATE SET
                       file_path = excluded.file_path,
                       file_sha256 = excluded.file_sha256,
                       file_size = excluded.file_size,
                       downloaded_at = excluded.downloaded_at,
                       provenance = excluded.provenance,
                       source_kind = excluded.source_kind""",
                (theme_media_type, tmdb_id, rel_path, sha, len(data), now_iso()),
            )
            # Clear any prior failure on the row (a manual upload obviates it)
            conn.execute(
                "UPDATE themes SET failure_kind = NULL, failure_message = NULL, "
                "failure_at = NULL WHERE media_type = ? AND tmdb_id = ?",
                (theme_media_type, tmdb_id),
            )
            # Enqueue placement
            conn.execute(
                "INSERT INTO jobs (job_type, media_type, tmdb_id, payload, status, "
                "                  created_at, next_run_at) "
                "VALUES ('place', ?, ?, '{}', 'pending', ?, ?)",
                (theme_media_type, tmdb_id, now_iso(), now_iso()),
            )

        log_event(db, level="INFO", component="api",
                  media_type=theme_media_type, tmdb_id=tmdb_id,
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
                conn.execute(
                    """INSERT INTO themes
                         (media_type, tmdb_id, imdb_id, title, year,
                          upstream_source, last_seen_sync_at, first_seen_sync_at)
                       VALUES (?, ?, ?, ?, ?, 'plex_orphan', ?, ?)""",
                    (theme_media_type, tmdb_id, pi["guid_imdb"],
                     pi["title"], pi["year"], now_iso(), now_iso()),
                )
            else:
                tmdb_id = theme["tmdb_id"]

            conn.execute(
                """INSERT INTO user_overrides (media_type, tmdb_id, youtube_url,
                                               set_at, set_by, note)
                   VALUES (?, ?, ?, ?, ?, ?)
                   ON CONFLICT(media_type, tmdb_id) DO UPDATE SET
                       youtube_url = excluded.youtube_url,
                       set_at = excluded.set_at,
                       set_by = excluded.set_by,
                       note = excluded.note""",
                (theme_media_type, tmdb_id, canonical_url, now_iso(),
                 request.state.user, f"manual url for plex rk={rating_key}"),
            )
            conn.execute(
                "UPDATE themes SET failure_kind = NULL, failure_message = NULL, "
                "failure_at = NULL WHERE media_type = ? AND tmdb_id = ?",
                (theme_media_type, tmdb_id),
            )
            # Cancel any in-flight download, enqueue a fresh one
            conn.execute(
                "UPDATE jobs SET status = 'cancelled', finished_at = ? "
                "WHERE job_type = 'download' AND media_type = ? AND tmdb_id = ? "
                "  AND status IN ('pending','failed')",
                (now_iso(), theme_media_type, tmdb_id),
            )
            conn.execute(
                "INSERT INTO jobs (job_type, media_type, tmdb_id, payload, status, "
                "                  created_at, next_run_at) "
                "VALUES ('download', ?, ?, '{\"reason\":\"manual_url\"}', 'pending', ?, ?)",
                (theme_media_type, tmdb_id, now_iso(), now_iso()),
            )

        log_event(db, level="INFO", component="api",
                  media_type=theme_media_type, tmdb_id=tmdb_id,
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
                sec_where += f" AND is_4k = {1 if fourk else 0}"
                sections = conn.execute(
                    f"SELECT section_id FROM plex_sections "
                    f"WHERE included = 1 AND {sec_where}"
                ).fetchall()
                if not sections:
                    return {"ok": True, "enqueued": 0,
                            "note": "no managed sections match this tab"}
                ids: list[int] = []
                for s in sections:
                    cur = conn.execute(
                        "INSERT INTO jobs (job_type, payload, status, created_at, next_run_at) "
                        "VALUES ('plex_enum', ?, 'pending', ?, ?)",
                        (json.dumps({"section_id": s["section_id"],
                                     "scope": f"{tab}{'-4k' if fourk else ''}"}),
                         now_iso(), now_iso()),
                    )
                    ids.append(cur.lastrowid)
                return {"ok": True, "enqueued": len(ids), "job_ids": ids,
                        "scope": f"{tab}{'-4k' if fourk else ''}"}

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
    async def api_item(media_type: MediaType, tmdb_id: int, db: Path = Depends(get_db_path)):
        with get_conn(db) as conn:
            t = conn.execute(
                "SELECT * FROM themes WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            ).fetchone()
            if t is None:
                raise HTTPException(status_code=404, detail="not found")
            lf = conn.execute(
                "SELECT * FROM local_files WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            ).fetchone()
            placements = conn.execute(
                "SELECT * FROM placements WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            ).fetchall()
            ovr = conn.execute(
                "SELECT * FROM user_overrides WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            ).fetchone()
            recent_events = conn.execute(
                "SELECT * FROM events WHERE media_type = ? AND tmdb_id = ? "
                "ORDER BY ts DESC LIMIT 25",
                (media_type, tmdb_id),
            ).fetchall()
        # Absolute file_path for the UI: local_files.file_path is stored
        # relative to themes_dir. Compose it once on the server so the
        # dialog can display the real on-disk path.
        local_payload = None
        if lf:
            d = dict(lf)
            if settings.is_paths_ready() and d.get("file_path"):
                d["abs_path"] = str(settings.themes_dir / d["file_path"])
            local_payload = d
        return {
            "theme": dict(t),
            "local_file": local_payload,
            "placements": [dict(p) for p in placements],
            "override": dict(ovr) if ovr else None,
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

    def _enqueue_section_refresh_for_item(
        conn, media_type: str, tmdb_id: int,
    ) -> int:
        """v1.10.21: enqueue a per-section plex_enum for every Plex section
        that holds this item, so cached plex_items.local_theme_file +
        has_theme reflect post-destructive reality. Returns the number
        of jobs enqueued (0 when there's already an enum in flight or
        the item isn't in any tracked section).

        Why: motif's place / unplace / unmanage / forget mutate state
        on disk that plex_items doesn't know about until the next enum.
        Without a refresh, an UNMANAGE row would render as untracked
        instead of M (sidecar) because plex_items.local_theme_file is
        stale. A scoped enum is cheap (200-row batches per v1.9.6).
        Dedupes against any pending/running plex_enum globally.
        """
        plex_mt = "show" if media_type == "tv" else "movie"
        existing = conn.execute(
            "SELECT id FROM jobs WHERE job_type = 'plex_enum' "
            "AND status IN ('pending','running')"
        ).fetchone()
        if existing:
            return 0
        sections = conn.execute(
            "SELECT DISTINCT section_id FROM plex_items "
            "WHERE guid_tmdb = ? AND media_type = ?",
            (tmdb_id, plex_mt),
        ).fetchall()
        enq = 0
        for s in sections:
            conn.execute(
                "INSERT INTO jobs (job_type, payload, status, created_at, next_run_at) "
                "VALUES ('plex_enum', ?, 'pending', ?, ?)",
                (json.dumps({"section_id": s["section_id"],
                             "scope": "post_destructive"}),
                 now_iso(), now_iso()),
            )
            enq += 1
        return enq

    @app.post("/api/items/{media_type}/{tmdb_id}/unplace")
    async def api_unplace_item(
        request: Request, media_type: MediaType, tmdb_id: int,
        db: Path = Depends(get_db_path),
    ):
        """Remove the placement of a theme (theme.mp3 in Plex's folder) but
        keep motif's canonical so the user can REPLACE it back later. The
        Plex side stops playing the motif theme; the file in themes_dir
        stays, allowing one-click re-placement without re-download.
        """
        _require_admin(request)
        with get_conn(db) as conn:
            theme = conn.execute(
                "SELECT title FROM themes WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            ).fetchone()
            if theme is None:
                raise HTTPException(status_code=404, detail="theme not found")
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
        log_event(db, level="INFO", component="api",
                  media_type=media_type, tmdb_id=tmdb_id,
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
            local = conn.execute(
                "SELECT 1 FROM local_files WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            ).fetchone()
            if local is None:
                raise HTTPException(
                    status_code=409,
                    detail="no local file to replace from — re-download first",
                )
            # Cancel in-flight place jobs to avoid double placement
            conn.execute(
                """UPDATE jobs SET status = 'cancelled', finished_at = ?
                   WHERE job_type = 'place' AND media_type = ? AND tmdb_id = ?
                     AND status IN ('pending','running')""",
                (now_iso(), media_type, tmdb_id),
            )
            conn.execute(
                """INSERT INTO jobs (job_type, media_type, tmdb_id, payload, status,
                                     created_at, next_run_at)
                   VALUES ('place', ?, ?, '{"force":true,"reason":"user_replace"}',
                           'pending', ?, ?)""",
                (media_type, tmdb_id, now_iso(), now_iso()),
            )
        log_event(db, level="INFO", component="api",
                  media_type=media_type, tmdb_id=tmdb_id,
                  message=f"Replace requested by {request.state.user}")
        return {"ok": True}

    @app.post("/api/items/{media_type}/{tmdb_id}/unmanage")
    async def api_unmanage_item(
        request: Request, media_type: MediaType, tmdb_id: int,
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
            local = conn.execute(
                "SELECT file_path FROM local_files "
                "WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            ).fetchone()
            if local is None:
                raise HTTPException(
                    status_code=409,
                    detail="not motif-managed (no local_files row)",
                )
            # v1.10.31: snapshot the placement folders before deletion so
            # we can optimistically flip plex_items.local_theme_file=1 on
            # them. The hardlink at /movies/Foo/theme.mp3 survives motif
            # deleting the canonical (different inode reference), but
            # plex_items.local_theme_file may have been 0 — the placement
            # was made by motif post-enum and we haven't re-stat'd. The
            # async section enum will re-confirm later, but the row needs
            # to render as M *now*.
            placements = conn.execute(
                "SELECT media_folder FROM placements "
                "WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            ).fetchall()

        # Delete the canonical only — leave Plex-folder placements intact.
        themes_dir = settings.themes_dir
        if themes_dir:
            try:
                p = themes_dir / local["file_path"]
                if p.is_file():
                    p.unlink()
                # Best-effort cleanup of empty parent dir (Title (Year)/).
                # Don't recurse upward; only the immediate parent matters.
                try:
                    parent = p.parent
                    if parent.is_dir() and not any(parent.iterdir()):
                        parent.rmdir()
                except OSError:
                    pass
            except OSError as e:
                log.warning("unmanage: could not unlink canonical %s: %s",
                            local["file_path"], e)

        is_orphan = theme["upstream_source"] == "plex_orphan"
        with get_conn(db) as conn:
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
            # v1.10.31: optimistically set plex_items.local_theme_file=1
            # for every folder we just unmanaged from. The sidecar at
            # those paths still exists (we deleted only the canonical),
            # so the row should render as M immediately. Without this,
            # if local_theme_file was 0 from a stale enum the row would
            # appear as untracked until the section enum below caught
            # up — which left users thinking UNMANAGE behaved like
            # PURGE.
            for pr in placements:
                conn.execute(
                    "UPDATE plex_items SET local_theme_file = 1 "
                    "WHERE folder_path = ?",
                    (pr["media_folder"],),
                )
            # Section enum still queued so the live state confirms our
            # optimistic flip (preserve_plex_file=True path — the file
            # is on disk, enum re-stat's and agrees).
            _enqueue_section_refresh_for_item(conn, media_type, tmdb_id)

        log_event(db, level="INFO", component="api",
                  media_type=media_type, tmdb_id=tmdb_id,
                  message=f"Unmanage theme '{theme['title']}' "
                          f"by {request.state.principal.username}",
                  detail={"orphan": is_orphan})
        return {"ok": True, "title": theme["title"], "orphan": is_orphan}

    @app.post("/api/items/{media_type}/{tmdb_id}/forget")
    async def api_forget_item(
        request: Request, media_type: MediaType, tmdb_id: int,
        db: Path = Depends(get_db_path),
    ):
        """Remove motif's tracking of a theme.

        - For ThemerrDB-sourced rows (T): unlinks placement files
          (theme.mp3 in each Plex folder), unlinks the canonical,
          drops local_files + placements rows. Keeps the themes row
          so the next sync can re-detect it.
        - For plex_orphan rows: same file cleanup + drop themes row.
        - v1.10.27: for user-provided / adopted rows (U / A — i.e.
          local_files.source_kind in {url, upload, adopt}), the
          file at the Plex folder is preserved. The user owns/
          provided that file, and PURGE was deleting their content.
          Now PURGE on U/A drops the canonical + tracking only,
          leaving the Plex-folder theme.mp3 alone — the row flips
          to M (unmanaged sidecar) and ADOPT becomes available
          again. Effectively the same shape as UNMANAGE, but the
          UI has both buttons since users reach for PURGE
          intuitively for 'remove from motif'.
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
            placements = conn.execute(
                "SELECT media_folder FROM placements "
                "WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            ).fetchall()
            local = conn.execute(
                "SELECT file_path, source_kind FROM local_files "
                "WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            ).fetchone()

        preserve_plex_file = (local is not None
                              and local["source_kind"] in ("url", "upload", "adopt"))

        unlinked = 0
        affected_folders: list[str] = []
        if preserve_plex_file:
            # User-provided/adopted file — keep the Plex-folder copy
            # so the row can flip to M. We still record the folder
            # paths for the post-destructive enum below.
            for pr in placements:
                affected_folders.append(pr["media_folder"])
        else:
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
        # Always drop motif's canonical — the user-owned file at the
        # Plex folder (if any) lives at a different path.
        themes_dir = settings.themes_dir
        if local and themes_dir:
            try:
                p = themes_dir / local["file_path"]
                if p.is_file():
                    p.unlink()
            except OSError as e:
                log.warning("forget: could not unlink canonical %s: %s",
                            local["file_path"], e)

        is_orphan = theme["upstream_source"] == "plex_orphan"
        with get_conn(db) as conn:
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
            # Only clear plex_items flags when we actually deleted the
            # Plex-folder file. For preserve_plex_file=True the file is
            # still there — leave local_theme_file=1 so the row renders
            # as M after the post-destructive enum.
            if not preserve_plex_file:
                for folder in affected_folders:
                    conn.execute(
                        "UPDATE plex_items SET local_theme_file = 0, has_theme = 0 "
                        "WHERE folder_path = ?",
                        (folder,),
                    )
            # v1.10.28: only run the section enum when the file is
            # still on disk (UNMANAGE / U/A PURGE). When motif just
            # deleted the file, an enum would re-fetch has_theme=1
            # from Plex's stale cache and bring back a phantom P
            # badge. In that case our optimistic flag clear above
            # is the correct UI; a per-item Plex refresh below kicks
            # Plex to re-evaluate so its cache catches up over time.
            if preserve_plex_file:
                _enqueue_section_refresh_for_item(conn, media_type, tmdb_id)
        if not preserve_plex_file:
            # Best-effort outside the txn so a slow Plex doesn't hold
            # the writer lock.
            try:
                _trigger_plex_item_refresh(media_type, tmdb_id)
            except Exception as e:
                log.debug("plex item refresh skipped: %s", e)

        log_event(db, level="INFO", component="api",
                  media_type=media_type, tmdb_id=tmdb_id,
                  message=f"Forget by {request.state.user}",
                  detail={"title": theme["title"],
                          "orphan_dropped": is_orphan,
                          "placements_unlinked": unlinked,
                          "placements_total": len(placements),
                          "preserved_plex_file": preserve_plex_file})
        return {"ok": True, "orphan_dropped": is_orphan,
                "placements_unlinked": unlinked,
                "preserved_plex_file": preserve_plex_file}

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
            local_row = conn.execute(
                "SELECT file_path FROM local_files "
                "WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            ).fetchone()

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
        # Unlink canonical (only safe to delete if no other theme references
        # the same file_sha256 — but for orphans this is always unique by
        # construction)
        themes_dir = settings.themes_dir
        if local_row and themes_dir:
            try:
                p = themes_dir / local_row["file_path"]
                if p.is_file():
                    p.unlink()
            except OSError as e:
                log.warning("Could not unlink canonical %s: %s", local_row["file_path"], e)

        # Drop the theme row; FK ON DELETE CASCADE handles local_files,
        # placements, pending_updates, user_overrides in one transaction.
        with get_conn(db) as conn:
            conn.execute(
                "DELETE FROM themes WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            )

        log_event(db, level="INFO", component="api",
                  media_type=media_type, tmdb_id=tmdb_id,
                  message=f"Orphan deleted by {request.state.user}",
                  detail={"title": theme["title"],
                          "placements_unlinked": unlinked,
                          "placements_total": len(placement_rows)})
        return Response(status_code=204)

    @app.post("/api/items/{media_type}/{tmdb_id}/revert")
    async def api_revert_to_themerrdb(
        request: Request, media_type: MediaType, tmdb_id: int,
        db: Path = Depends(get_db_path),
    ):
        """Drop the user_override on a theme and re-download from
        ThemerrDB's upstream URL. Used by the // DOWNLOAD button on
        U-tagged rows in the library view to flip a manual override
        back to T (auto). Refuses if the theme is a plex_orphan
        (no upstream to revert to).
        """
        _require_admin(request)
        with get_conn(db) as conn:
            row = conn.execute(
                "SELECT upstream_source FROM themes WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            ).fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail="not in database")
            if row["upstream_source"] == "plex_orphan":
                raise HTTPException(
                    status_code=409,
                    detail="this theme has no ThemerrDB upstream to revert to",
                )
            conn.execute(
                "DELETE FROM user_overrides WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            )
            conn.execute(
                """UPDATE themes SET failure_kind = NULL, failure_message = NULL,
                                     failure_at = NULL
                   WHERE media_type = ? AND tmdb_id = ?""",
                (media_type, tmdb_id),
            )
            # Cancel any in-flight download (would otherwise pick up the
            # override URL we just deleted), then enqueue a fresh one.
            conn.execute(
                """UPDATE jobs SET status = 'cancelled', finished_at = ?
                   WHERE job_type = 'download' AND media_type = ? AND tmdb_id = ?
                     AND status IN ('pending', 'failed')""",
                (now_iso(), media_type, tmdb_id),
            )
            conn.execute(
                """INSERT INTO jobs (job_type, media_type, tmdb_id, payload, status,
                                     created_at, next_run_at)
                   VALUES ('download', ?, ?, '{"reason":"revert_to_themerrdb"}', 'pending', ?, ?)""",
                (media_type, tmdb_id, now_iso(), now_iso()),
            )
        log_event(db, level="INFO", component="api",
                  media_type=media_type, tmdb_id=tmdb_id,
                  message=f"Override cleared + ThemerrDB re-download requested by {request.state.user}")
        return {"ok": True}

    @app.post("/api/items/{media_type}/{tmdb_id}/redownload")
    async def api_redownload(
        request: Request, media_type: MediaType, tmdb_id: int,
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
            # Cancel any pending download for this item, then enqueue a fresh one
            conn.execute(
                """UPDATE jobs SET status = 'cancelled', finished_at = ?
                   WHERE job_type = 'download' AND media_type = ? AND tmdb_id = ?
                     AND status IN ('pending', 'failed')""",
                (now_iso(), media_type, tmdb_id),
            )
            conn.execute(
                """INSERT INTO jobs (job_type, media_type, tmdb_id, payload, status,
                                     created_at, next_run_at)
                   VALUES ('download', ?, ?, '{"reason":"manual"}', 'pending', ?, ?)""",
                (media_type, tmdb_id, now_iso(), now_iso()),
            )
        log_event(db, level="INFO", component="api",
                  media_type=media_type, tmdb_id=tmdb_id,
                  message=f"Manual re-download requested by {request.state.user}")
        return {"ok": True}

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
            conn.execute(
                """INSERT INTO user_overrides (media_type, tmdb_id, youtube_url,
                                               set_at, set_by, note)
                   VALUES (?, ?, ?, ?, ?, ?)
                   ON CONFLICT(media_type, tmdb_id) DO UPDATE SET
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
            # Cancel any in-flight download for this item, then enqueue fresh
            conn.execute(
                """UPDATE jobs SET status = 'cancelled', finished_at = ?
                   WHERE job_type = 'download' AND media_type = ? AND tmdb_id = ?
                     AND status IN ('pending', 'failed')""",
                (now_iso(), media_type, tmdb_id),
            )
            conn.execute(
                """INSERT INTO jobs (job_type, media_type, tmdb_id, payload, status,
                                     created_at, next_run_at)
                   VALUES ('download', ?, ?, '{"reason":"override"}', 'pending', ?, ?)""",
                (media_type, tmdb_id, now_iso(), now_iso()),
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
            conn.execute(
                "DELETE FROM user_overrides WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
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

    # --- JSON: coverage ---

    @app.get("/api/coverage/plex")
    async def api_coverage_plex(db: Path = Depends(get_db_path)):
        if not settings.plex_enabled or not settings.plex_token:
            return {"enabled": False, "items": []}
        cfg = PlexConfig(
            url=settings.plex_url, token=settings.plex_token,
            movie_section=settings.plex_movie_section,
            tv_section=settings.plex_tv_section, enabled=True,
        )
        movies_in_plex: list = []
        tv_in_plex: list = []
        try:
            with PlexClient(cfg) as plex:
                for c in plex.list_section(media_type="movie"):
                    movies_in_plex.append({
                        "rating_key": c.rating_key, "title": c.title,
                        "year": c.year, "has_theme": c.has_theme,
                    })
                for c in plex.list_section(media_type="tv"):
                    tv_in_plex.append({
                        "rating_key": c.rating_key, "title": c.title,
                        "year": c.year, "has_theme": c.has_theme,
                    })
        except Exception as e:
            return {"enabled": True, "error": str(e)}

        # Cross-reference: which Plex items have themes available in ThemerrDB
        # but aren't yet placed locally?
        with get_conn(db) as conn:
            available_movies = {
                (r["title"].lower(), r["year"]): dict(r)
                for r in conn.execute(
                    "SELECT title, year, tmdb_id FROM themes WHERE media_type='movie'"
                ).fetchall()
            }
            available_tv = {
                (r["title"].lower(), r["year"]): dict(r)
                for r in conn.execute(
                    "SELECT title, year, tmdb_id FROM themes WHERE media_type='tv'"
                ).fetchall()
            }

        def annotate(items, available):
            for it in items:
                key = (it["title"].lower(), it["year"])
                match = available.get(key)
                it["motif_available"] = bool(match)
                it["tmdb_id"] = match["tmdb_id"] if match else None
            return items

        return {
            "enabled": True,
            "movies": annotate(movies_in_plex, available_movies),
            "tv": annotate(tv_in_plex, available_tv),
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

    @app.get("/healthz")
    async def healthz():
        return JSONResponse({"status": "ok"})

    return app
