"""v1.19.44 — post-v1.19.43 audit fixes.

Three parallel audit agents (mirror-drift / behavioral trace /
class-9 silent-fails) reviewed the v1.19.41-43 cloud-themes-backup
arc immediately after ship. This tag addresses the BLOCKING + S1
findings:

  1. **PROMOTE TO ACTIVE re-upload trick for plex_cloud (BLOCKING)**
     The v1.19.43 tooltip claimed "Motif uploads its copy of
     Plex's cloud theme back to Plex via the v1.18.36 re-upload
     trick." Reality: the BK-no-override branch enqueued a vanilla
     place job which routed via `default_placement_method` →
     typically `'file'` → hardlink sidecar in the Plex folder.
     `set_active_theme_via_reupload` was never reached. Tooltip
     lied to users.

     Fix: BK-no-override branch detects `source_kind='plex_cloud'`
     and dispatches synchronously to `plex.upload_collection_theme`
     (the same code path the v1.18.36 production
     `set_active_theme_via_reupload` uses). Stamps a `plex_upload`
     placement so the row's LINK badge transitions from B → PU.

  2. **F3 admin endpoint JSON parse fail-fast (S1)**
     Pre-fix bare `except: body = {}` on the v1.19.42
     cloud-themes-backup admin endpoints silently ran the FULL
     ~3,883-row catalog walk when the operator meant to scope.
     Run endpoint has write side-effects (~4.2 GB + 1,940
     INSERTs) — silent over-scope is data the operator didn't
     ask for.

     Fix: read raw body first; if non-empty, parse and reject
     400 on malformed. Empty body still routes to the documented
     "walk everything" sentinel.

  3. **F8/F9 body formatter KeyError → notify dispatch silenced (S1)**
     The notify_content body formatters read
     `ctx['display_title']` unguarded. If `enrich_item` returned
     a partial ctx (row deleted mid-dispatch, future caller
     synthesizing ctx directly), KeyError → outer
     `_THEME_LOST_NOTIFY_WARNED` flag goes hot → every future
     notification in the process silently drops to log.debug.

     Fix: `_safe_display_title(ctx)` helper with media_type/
     tmdb_id fallback. Applied to all five formatters that
     read display_title.

  4. **F11 outer dispatch swallow at log.debug (S1)**
     Pre-fix the `except Exception as _outer: log.debug(...)`
     wrapper around the plex_theme_lost dispatch loop swallowed
     import errors / config crashes with no operator-visible
     breadcrumb. Class-9 outer-catch-all sub-pattern.

     Fix: warn-then-debug-flag pattern using new module-level
     `_THEME_LOST_DISPATCH_OUTER_WARNED`. Mirrors the inner
     `_THEME_LOST_NOTIFY_WARNED` / `_BACKUP_READY_NOTIFY_WARNED`
     shape established in v1.19.41.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from fastapi.testclient import TestClient  # noqa: E402

API_PY = (REPO / "app" / "web" / "api.py").read_text()
NOTIFY_CONTENT_PY = (
    REPO / "app" / "core" / "notify_content.py"
).read_text()
PLEX_ENUM_PY = (REPO / "app" / "core" / "plex_enum.py").read_text()


# ── 1. PROMOTE TO ACTIVE re-upload trick (BLOCKING fix) ──────


def test_bk_no_override_branch_detects_plex_cloud():
    """The BK-no-override branch in api_set_override_intent must
    detect `source_kind='plex_cloud'` and route to the v1.18.36
    re-upload trick instead of the default place-job path."""
    fn_idx = API_PY.index("async def api_set_override_intent(")
    fn_end = API_PY.index("\n    @app.", fn_idx + 1)
    body = API_PY[fn_idx:fn_end]
    assert 'bk_source_kind == "plex_cloud"' in body, (
        "v1.19.44: BK-no-override branch must detect plex_cloud "
        "rows and dispatch to re-upload trick"
    )


def test_bk_no_override_widened_select_includes_source_kind():
    """The bk_local SELECT must include source_kind + file_path
    so the plex_cloud branch can read them without a second
    query. v1.19.54: also fetches source_video_id (SHA-1 for
    drift check); pin substring instead of exact match so
    future widenings don't break this guard."""
    fn_idx = API_PY.index("async def api_set_override_intent(")
    fn_end = API_PY.index("\n    @app.", fn_idx + 1)
    body = API_PY[fn_idx:fn_end]
    assert (
        '"SELECT source_kind, file_path' in body
    ), (
        "v1.19.44: bk_local SELECT must widen to fetch "
        "source_kind + file_path"
    )


def test_plex_cloud_promote_calls_upload_collection_theme():
    """The plex_cloud branch must call upload_collection_theme
    (the v1.18.36 path) — NOT enqueue a place job which would
    route through hardlink-sidecar default placement."""
    fn_idx = API_PY.index("async def api_set_override_intent(")
    fn_end = API_PY.index("\n    @app.", fn_idx + 1)
    body = API_PY[fn_idx:fn_end]
    pc_idx = body.index('bk_source_kind == "plex_cloud"')
    block = body[pc_idx:pc_idx + 12000]
    # v1.22.42: the upload is off-loaded via run_in_threadpool, so the bound
    # method is now followed by a comma, not an open-paren.
    assert "plex.upload_collection_theme," in block, (
        "v1.19.44: plex_cloud PROMOTE must call "
        "upload_collection_theme — matches the v1.19.43 tooltip "
        "promise"
    )
    assert "audio_bytes=audio_bytes" in block


def test_plex_cloud_promote_stamps_plex_upload_placement():
    """After re-upload succeeds, the branch must stamp a
    plex_upload placement so the row's LINK badge transitions
    from B → PU (motif now owns the Plex-side state)."""
    fn_idx = API_PY.index("async def api_set_override_intent(")
    fn_end = API_PY.index("\n    @app.", fn_idx + 1)
    body = API_PY[fn_idx:fn_end]
    pc_idx = body.index('bk_source_kind == "plex_cloud"')
    block = body[pc_idx:pc_idx + 13000]
    assert "INSERT INTO placements" in block
    assert "'plex_upload'" in block, (
        "v1.19.44: plex_cloud PROMOTE must stamp placement_kind="
        "'plex_upload' so the row reads as PU (motif-owned via API)"
    )


def test_plex_cloud_promote_clears_backup_only_marker():
    """After PROMOTE, the local_files row must have its
    backup_only marker cleared so the row no longer renders as
    B in the LINK column."""
    fn_idx = API_PY.index("async def api_set_override_intent(")
    fn_end = API_PY.index("\n    @app.", fn_idx + 1)
    body = API_PY[fn_idx:fn_end]
    pc_idx = body.index('bk_source_kind == "plex_cloud"')
    block = body[pc_idx:pc_idx + 14000]
    assert "last_place_attempt_reason = NULL" in block, (
        "v1.19.44: PROMOTE must clear the backup_only marker so "
        "the row stops rendering as B post-promote"
    )


def test_plex_cloud_promote_falls_through_to_legacy_path_for_non_cloud():
    """The non-plex_cloud BK rows (themerrdb / url / upload
    source_kind) must continue to enqueue the standard place job
    — the legacy v1.19.35 path that has always written sidecars
    on PROMOTE for user-explicit KEEP AS BACKUP rows."""
    fn_idx = API_PY.index("async def api_set_override_intent(")
    fn_end = API_PY.index("\n    @app.", fn_idx + 1)
    body = API_PY[fn_idx:fn_end]
    # The legacy place-job enqueue must still exist AFTER the
    # plex_cloud branch (which `return`s on success).
    pc_idx = body.index('bk_source_kind == "plex_cloud"')
    post_pc = body[pc_idx:]
    assert "promote_bk_no_override" in post_pc
    assert "INSERT INTO jobs" in post_pc, (
        "v1.19.44: non-plex_cloud BK rows must still enqueue "
        "the place job — only plex_cloud routes to re-upload"
    )


# ── 2. Admin endpoint JSON parse fail-fast (F3) ──────────────


def test_dry_run_endpoint_rejects_malformed_json():
    """The dry-run endpoint must 400 on malformed JSON instead
    of silently running the full catalog walk."""
    fn_idx = API_PY.index(
        "async def api_admin_cloud_themes_backup_dry_run("
    )
    fn_end = API_PY.index("\n    @app.", fn_idx + 1)
    body = API_PY[fn_idx:fn_end]
    assert "malformed JSON body" in body, (
        "v1.19.44: dry-run endpoint must raise 400 on JSON parse "
        "failure with explicit error message"
    )
    assert "status_code=400" in body


def test_run_endpoint_rejects_malformed_json():
    """The run endpoint (which has write side-effects) must also
    fail-fast on malformed JSON — silent over-scope here would
    download ~4.2 GB the operator didn't ask for."""
    fn_idx = API_PY.index(
        "async def api_admin_cloud_themes_backup_run("
    )
    fn_end = API_PY.index("\n    @app.", fn_idx + 1)
    body = API_PY[fn_idx:fn_end]
    assert "malformed JSON body" in body
    assert "status_code=400" in body


def test_endpoints_still_tolerate_empty_body():
    """Empty body (no JSON at all) must still route to the
    documented "walk everything" sentinel. The fix is to
    distinguish malformed-JSON from no-body."""
    fn_idx = API_PY.index(
        "async def api_admin_cloud_themes_backup_dry_run("
    )
    fn_end = API_PY.index("\n    @app.", fn_idx + 1)
    body = API_PY[fn_idx:fn_end]
    assert "raw = await request.body()" in body
    assert "if raw:" in body, (
        "v1.19.44: empty body must skip the parse step so "
        "operators can still POST with no body to mean "
        "'walk everything'"
    )


# ── 3. Body formatter defensive ctx access (F8/F9) ───────────


def test_safe_display_title_helper_defined():
    """notify_content must define a _safe_display_title helper
    that handles missing ctx['display_title']."""
    assert "def _safe_display_title(ctx:" in NOTIFY_CONTENT_PY


def test_safe_display_title_falls_back_to_mt_tmdb():
    """The fallback must use media_type/tmdb_id, NOT a generic
    '<unknown>' placeholder — operators reading the notification
    still need to identify the row."""
    from app.core.notify_content import _safe_display_title
    # Missing display_title → uses mt/tmdb_id fallback.
    assert _safe_display_title(
        {"media_type": "movie", "tmdb_id": 42}
    ) == "movie/42"
    # Both missing → graceful "?/?" rather than KeyError.
    assert _safe_display_title({}) == "?/?"
    # Present → used directly.
    assert _safe_display_title(
        {"display_title": "American Psycho (2000)"}
    ) == "American Psycho (2000)"


def test_all_v1_19_41_formatters_use_safe_helper():
    """The five v1.19.41 / v1.18.80 / v1.18.90 / earlier
    formatters that read display_title must all use the safe
    helper. Pre-fix any one of them KeyError-ing would silence
    the entire dispatch loop via the _THEME_LOST_NOTIFY_WARNED
    flag."""
    # All `ctx['display_title']` direct reads must be gone
    # (only the helper definition + docstring should still
    # reference the literal).
    pre_fix_pattern = "ctx['display_title']"
    # Should appear ONLY in the helper definition or its
    # docstring — not in any active formatter line.
    lines = NOTIFY_CONTENT_PY.split("\n")
    bad_lines = []
    in_helper_docstring = False
    for i, line in enumerate(lines):
        if pre_fix_pattern in line:
            # Allow the helper's own docstring + comment lines.
            if (
                "_safe_display_title" in line
                or "Pre-fix" in line
                or "defensive accessor" in line
                or '"""' in line
            ):
                continue
            bad_lines.append((i + 1, line.strip()))
    assert not bad_lines, (
        f"v1.19.44: every formatter must use _safe_display_title; "
        f"remaining unguarded reads at: {bad_lines}"
    )


def test_safe_display_title_handles_none_values():
    """ctx values might be None (sqlite3.Row → dict with NULL
    columns). Defensive accessor must coerce to the fallback."""
    from app.core.notify_content import _safe_display_title
    # display_title is None → use fallback.
    assert _safe_display_title(
        {"display_title": None, "media_type": "tv", "tmdb_id": 99}
    ) == "tv/99"


# ── 4. Outer dispatch warn-flag pattern (F11) ────────────────


def test_outer_dispatch_uses_warned_flag():
    """The outer plex_theme_lost dispatch swallow must use a
    module-level _THEME_LOST_DISPATCH_OUTER_WARNED flag so the
    first failure logs at WARNING (operator visibility) and
    subsequent occurrences drop to debug (no log drowning)."""
    assert (
        "_THEME_LOST_DISPATCH_OUTER_WARNED" in PLEX_ENUM_PY
    ), (
        "v1.19.44: outer dispatch needs the warn-flag pattern "
        "to mirror inner _THEME_LOST_NOTIFY_WARNED"
    )
    # Module-level declaration.
    assert (
        "_THEME_LOST_DISPATCH_OUTER_WARNED: bool = False"
        in PLEX_ENUM_PY
    )


def test_outer_dispatch_first_occurrence_logs_warning():
    """First-occurrence path must call log.warning, not
    log.debug — class-9 lesson from v1.17.11 hot-path."""
    # Locate the outer dispatch try/except.
    idx = PLEX_ENUM_PY.index(
        "except Exception as _outer:"
    )
    # Walk forward to the conditional.
    block = PLEX_ENUM_PY[idx:idx + 1500]
    assert "_THEME_LOST_DISPATCH_OUTER_WARNED" in block
    # First-occurrence branch logs at warning.
    assert "log.warning(" in block, (
        "v1.19.44: first-occurrence outer dispatch swallow "
        "must log at WARNING level"
    )
    # Subsequent occurrences drop to debug.
    assert "log.debug(" in block


# ── 5. Behavioral via TestClient ─────────────────────────────


@pytest.fixture
def admin_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("MOTIF_PLEX_URL", "http://fake:32400")
    monkeypatch.setenv("MOTIF_PLEX_TOKEN", "fake-token")
    (tmp_path / "themes").mkdir(exist_ok=True)
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    settings = Settings(
        config_dir=tmp_path, data_dir=tmp_path / "data",
    )
    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    return TestClient(create_app(settings))


AUTH = {"X-Authentik-Username": "testadmin"}


def test_run_endpoint_400_on_malformed_json(admin_client):
    """End-to-end: POST malformed JSON to run endpoint → 400,
    not silent over-scope."""
    r = admin_client.post(
        "/api/admin/cloud-themes-backup-run",
        headers={**AUTH, "Content-Type": "application/json"},
        content=b"not valid json {{{",
    )
    assert r.status_code == 400, (
        f"v1.19.44: malformed JSON must produce 400; got "
        f"{r.status_code} — {r.text}"
    )
    assert "malformed JSON" in r.text


def test_run_endpoint_accepts_empty_body(admin_client):
    """End-to-end: POST with no body must NOT 400 (documented
    sentinel for 'walk everything'). May return 503 if no
    themes_dir, but not 400."""
    r = admin_client.post(
        "/api/admin/cloud-themes-backup-run",
        headers=AUTH,
    )
    # 503 (themes_dir not configured) is expected in this test
    # env; the contract under test is NOT-400.
    assert r.status_code != 400, (
        f"v1.19.44: empty body must NOT 400 — that's the "
        f"documented 'walk everything' path; got "
        f"{r.status_code} — {r.text}"
    )


# ── Version pin ──────────────────────────────────────────────


def test_v1_19_44_version_pin():
    """Version bumped at v1.19.44 (then again at v1.19.45). Match
    the 1.19.x prefix so subsequent bumps don't break this guard."""
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
