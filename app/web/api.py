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
from typing import Annotated, Literal

from fastapi import Depends, FastAPI, Form, HTTPException, Query, Request, status
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
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

    app.add_middleware(AuthMiddleware, settings=settings)

    def get_db_path() -> Path:
        return settings.db_path

    # --- HTML routes ---

    @app.get("/", response_class=HTMLResponse)
    async def dashboard(request: Request):
        return templates.TemplateResponse(request, "dashboard.html")

    @app.get("/movies", response_class=HTMLResponse)
    async def movies_page(request: Request):
        return templates.TemplateResponse(request, "browse.html", {
            "media_type": "movie", "title": "Movies",
        })

    @app.get("/tv", response_class=HTMLResponse)
    async def tv_page(request: Request):
        return templates.TemplateResponse(request, "browse.html", {
            "media_type": "tv", "title": "TV Shows",
        })

    @app.get("/coverage", response_class=HTMLResponse)
    async def coverage_page(request: Request):
        return templates.TemplateResponse(request, "coverage.html")

    @app.get("/libraries", response_class=HTMLResponse)
    async def libraries_page(request: Request):
        return templates.TemplateResponse(request, "libraries.html")

    @app.get("/queue", response_class=HTMLResponse)
    async def queue_page(request: Request):
        return templates.TemplateResponse(request, "queue.html")

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

    @app.get("/api/stats")
    async def api_stats(db: Path = Depends(get_db_path)):
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
                  (SELECT COUNT(*) FROM pending_updates WHERE decision = 'pending') AS updates_pending,
                  (SELECT COUNT(*) FROM themes WHERE failure_kind IS NOT NULL) AS failures_total,
                  (SELECT COUNT(*) FROM themes
                   WHERE failure_kind IN ('video_private','video_removed','video_age_restricted','geo_blocked')
                  ) AS failures_unavailable,
                  (SELECT COUNT(*) FROM themes WHERE failure_kind = 'cookies_expired') AS failures_cookies
            """).fetchone()
            last_sync = conn.execute("""
                SELECT * FROM sync_runs ORDER BY id DESC LIMIT 1
            """).fetchone()
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
            },
            "storage": {
                "hardlinks": row["hardlinks"],
                "copies": row["copies"],
                "total_bytes": row["storage_bytes"],
                "copies_bytes": row["storage_copies_bytes"],
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
            "dry_run": is_dry_run(db, default=settings.dry_run_default),
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
                   t.youtube_url, t.youtube_video_id, t.failure_kind, t.failure_message
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
        return {
            "theme": dict(t),
            "local_file": dict(lf) if lf else None,
            "placements": [dict(p) for p in placements],
            "override": dict(ovr) if ovr else None,
            "events": [dict(e) for e in recent_events],
        }

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
        youtube_url: Annotated[str, Form()],
        note: Annotated[str, Form()] = "",
        db: Path = Depends(get_db_path),
    ):
        _require_admin(request)
        vid = extract_video_id(youtube_url)
        if not vid:
            raise HTTPException(status_code=400, detail="invalid YouTube URL")
        # Normalize to canonical form
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
            conn.execute(
                """INSERT INTO jobs (job_type, media_type, tmdb_id, payload, status,
                                     created_at, next_run_at)
                   VALUES ('download', ?, ?, '{"reason":"override"}', 'pending', ?, ?)""",
                (media_type, tmdb_id, now_iso(), now_iso()),
            )
        log_event(db, level="INFO", component="api",
                  media_type=media_type, tmdb_id=tmdb_id,
                  message=f"Override set by {request.state.user}: {canonical}")
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
        with get_conn(db) as conn:
            existing = conn.execute(
                "SELECT id FROM jobs WHERE job_type = 'sync' AND status IN ('pending','running')"
            ).fetchone()
            if existing:
                return {"ok": True, "job_id": existing["id"], "already_queued": True}
            cur = conn.execute(
                """INSERT INTO jobs (job_type, status, created_at, next_run_at)
                   VALUES ('sync', 'pending', ?, ?)""",
                (now_iso(), now_iso()),
            )
            job_id = cur.lastrowid
        log_event(db, level="INFO", component="api",
                  message=f"Manual sync queued by {request.state.user}")
        return {"ok": True, "job_id": job_id}

    @app.get("/healthz")
    async def healthz():
        return JSONResponse({"status": "ok"})

    return app
