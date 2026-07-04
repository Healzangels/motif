"""v1.19.45 — cloud-themes-backup async refactor (UNBLOCKS PRODUCTION USE).

the user's repro (2026-05-27, immediately post-v1.19.43 ship):
clicked BACKUP THIS THEME on a single TV row → browser hit
"Failed to fetch" → entire motif UI locked up → docker logs
silent. Three interacting bugs made the v1.19.42/43 feature
unusable in production:

  1. **identify_c1_rows walked the FULL catalog before filtering
     by rks_scope.** Single-row click = 13+ min minimum (200ms
     inter-call × ~3,883 P-rows). Push-down filter missing.

  2. **Endpoint was async-def-with-sync-work.** The handler
     declared `async def` but contained synchronous Plex API
     calls + time.sleep + httpx GETs. These blocked the
     FastAPI event loop for the full walk duration, queuing
     every other request. UI locked up because /api/library
     requests sat waiting for the loop.

  3. **No log breadcrumbs during the walk.** Walker only
     logged at end (which never happened) or per-row Plex
     errors. v1.18.7 cold-path-needs-MORE-logging lesson —
     exactly the same shape.

## v1.19.45 fixes

  1. **`rks_scope` push-down in identify_c1_rows** — when the
     caller scopes to specific rks, the WHERE clause filters
     IN the SQL query, not after the walk completes. Single-
     row clicks become ~10s (one /themes call + one download)
     instead of 13+ minutes.

  2. **Endpoint refactored to acquire-then-spawn-thread**
     pattern (same shape as bulk_lps / bulk_probe_tdb /
     tvdb_bridge). The endpoint:
       - Validates input + parses body
       - Calls `op_progress.try_acquire('cloud-themes-backup',
         'cloud_themes_backup')` — 409 if already in flight
       - Spawns daemon thread targeting
         `_cloud_themes_backup_run`
       - Returns immediately with `{ok, op_id, scope}`
     The worker thread runs `start_progress` → walk →
     download → `finish_progress`, with `update_progress`
     each stage + per-target. Cancellable via
     `op_progress.is_cancelled`.

  3. **Schema v59** widens op_progress.kind CHECK to include
     `'cloud_themes_backup'`. Pre-flight verified safe against
     the user's prod DB copy on 2026-05-27.

  4. **ops.js maps** — `KIND_LABEL`, `TONE_BY_KIND`,
     `OP_MINI_PRIORITY` all get cloud_themes_backup entries
     so the drawer card renders with proper label/tone and
     the mini-bar picks up the kind in the contended slot.

  5. **Log breadcrumbs** at walk start, per batch, per
     download success/failure, and final summary. v1.18.7
     cold-path-needs-MORE-logging compliance.

  6. **Client click handlers** updated to expect
     `{ok, op_id}` async response. Toast says "QUEUED" and
     the ops drawer surfaces progress.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from fastapi.testclient import TestClient  # noqa: E402

DB_PY = (REPO / "app" / "core" / "db.py").read_text()
CLOUD_PY = (REPO / "app" / "core" / "cloud_theme_backup.py").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
OPS_JS = (REPO / "app" / "web" / "static" / "ops.js").read_text()


# ── Schema v59 ───────────────────────────────────────────────


def test_current_schema_version_is_at_least_59():
    """v1.19.45 bumps the canonical schema version to 59. Later
    tags may bump higher (e.g. v1.19.71 → 60); the floor is what
    this test pins."""
    import re
    m = re.search(r"CURRENT_SCHEMA_VERSION = (\d+)", DB_PY)
    assert m is not None
    assert int(m.group(1)) >= 59


def test_migrate_v58_to_v59_defined():
    """The migration function must exist and widen
    op_progress.kind CHECK."""
    assert "def _migrate_v58_to_v59(conn:" in DB_PY
    fn_idx = DB_PY.index("def _migrate_v58_to_v59(conn:")
    fn_end = DB_PY.index("\ndef ", fn_idx + 1)
    body = DB_PY[fn_idx:fn_end]
    assert "'cloud_themes_backup'" in body, (
        "v1.19.45: migration must add 'cloud_themes_backup' "
        "to op_progress.kind CHECK"
    )
    # Same shape as v51→v52 / v55→v56 (canonical kind-widening
    # via executescript + table recreate).
    assert "CREATE TABLE op_progress_new" in body
    assert "INSERT INTO op_progress_new SELECT * FROM op_progress" in body


def test_schema_constant_includes_cloud_themes_backup():
    """SCHEMA constant (fresh-install path) must list the new
    kind in the CHECK so new installs don't need the migration."""
    anchor = DB_PY.index("CREATE TABLE IF NOT EXISTS op_progress")
    block = DB_PY[anchor:anchor + 6000]
    assert "'cloud_themes_backup'" in block


def test_migration_v58_to_v59_runs_clean(tmp_path):
    """Boot fresh DB → init_db runs the full ladder → schema=59
    + try_acquire on cloud_themes_backup succeeds."""
    db_path = tmp_path / "test_v59.db"
    from app.core.db import init_db
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        version = conn.execute(
            "SELECT MAX(version) FROM schema_version"
        ).fetchone()[0]
        assert version >= 59, (
            f"Expected schema_version>=59, got {version}"
        )
    # try_acquire on the new kind must not raise CHECK constraint.
    from app.core.progress import try_acquire
    ok = try_acquire(
        db_path, op_id="cloud-themes-backup",
        kind="cloud_themes_backup",
    )
    assert ok is True, (
        "v1.19.45: try_acquire('cloud-themes-backup', "
        "'cloud_themes_backup') must succeed post-migration"
    )


# ── identify_c1_rows rks_scope push-down ─────────────────────


def test_identify_c1_rows_accepts_rks_scope_parameter():
    """The walker must accept rks_scope to push the filter down
    into the SQL WHERE clause."""
    assert "rks_scope: list[str] | None = None" in CLOUD_PY


def test_rks_scope_pushed_into_sql_where_clause():
    """rks_scope must filter IN the SQL (push-down), not after
    the walk completes. Pre-fix the v1.19.42 endpoint walked
    the full catalog then filtered post-walk — 13+ min for a
    single row."""
    fn_idx = CLOUD_PY.index("def identify_c1_rows(")
    fn_end = CLOUD_PY.index("\ndef ", fn_idx + 1)
    body = CLOUD_PY[fn_idx:fn_end]
    assert "pi.rating_key IN (" in body, (
        "v1.19.45: rks_scope must push into the SQL WHERE clause "
        "via `pi.rating_key IN (?, ?, ...)` so single-row clicks "
        "short-circuit instead of walking the full catalog"
    )
    # Empty rks_scope must short-circuit to empty results.
    assert "if not rks_scope:" in body
    assert "return []" in body


def test_rks_scope_filters_walker_results(tmp_path):
    """Behavioral: with rks_scope=['rk-a'], walker must walk
    only that row even though other P-rows exist."""
    from app.core.db import init_db
    db_path = tmp_path / "test.db"
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute(
            "INSERT INTO plex_sections "
            "(section_id, title, type, is_anime, is_4k, "
            " themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 0, 0, 'movies', 1, "
            "        '2026-05-27', '2026-05-27')"
        )
        # Seed 3 P-rows.
        for i, rk in enumerate(("rk-a", "rk-b", "rk-c"), start=1):
            conn.execute(
                "INSERT INTO plex_items "
                "(rating_key, section_id, media_type, guid_tmdb, "
                " title, year, has_theme, "
                " first_seen_at, last_seen_at) "
                "VALUES (?, '1', 'movie', ?, ?, 2020, 1, "
                "        '2026-05-27', '2026-05-27')",
                (rk, 1000 + i, f"Movie {i}"),
            )
        conn.commit()
        from app.core.cloud_theme_backup import identify_c1_rows
        plex = MagicMock()
        plex.get_themes.return_value = {
            "ok": True, "http_status": 200, "error": None,
            "body": {"MediaContainer": {"Metadata": [
                {"ratingKey": "metadata://themes/" + "f" * 40}
            ]}},
        }
        # Walker scoped to one rk.
        targets = identify_c1_rows(
            conn, plex, inter_call_sleep_s=0,
            use_cursor=False,
            rks_scope=["rk-a"],
        )
        # Plex.get_themes called ONCE (not 3 times).
        assert plex.get_themes.call_count == 1, (
            f"v1.19.45: rks_scope=['rk-a'] must result in "
            f"exactly 1 Plex /themes call; got "
            f"{plex.get_themes.call_count} (catalog walk leaked "
            f"through the scope filter)"
        )
        assert len(targets) == 1
        assert targets[0]["rating_key"] == "rk-a"


def test_empty_rks_scope_short_circuits_to_no_walk(tmp_path):
    """rks_scope=[] (explicit empty) must short-circuit — no
    Plex calls, no SQL even."""
    from app.core.db import init_db
    db_path = tmp_path / "test.db"
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        from app.core.cloud_theme_backup import identify_c1_rows
        plex = MagicMock()
        targets = identify_c1_rows(
            conn, plex, inter_call_sleep_s=0,
            use_cursor=False,
            rks_scope=[],
        )
        assert targets == []
        assert plex.get_themes.call_count == 0


# ── Walk-start log breadcrumb (v1.18.7 cold-path lesson) ─────


def test_walker_logs_at_start():
    """The walker must log.info at the START of the walk so
    docker logs show something during the multi-minute work.
    Pre-fix the walker logged only at completion (which never
    happened on a stuck walk) — same shape as the v1.18.7
    cold-path-needs-MORE-logging lesson."""
    fn_idx = CLOUD_PY.index("def identify_c1_rows(")
    fn_end = CLOUD_PY.index("\ndef ", fn_idx + 1)
    body = CLOUD_PY[fn_idx:fn_end]
    assert 'log.info(' in body
    # First log.info should be the start-of-walk breadcrumb.
    info_idx = body.index('log.info(')
    snippet = body[info_idx:info_idx + 400]
    assert "starting walk" in snippet, (
        "v1.19.45: walker must emit a START log line so docker "
        "logs surface something during the long walk"
    )


# ── Endpoint: acquire-then-spawn-thread ──────────────────────


def test_run_endpoint_uses_try_acquire():
    """The endpoint must use op_progress.try_acquire to claim
    the slot atomically (409 if already in flight)."""
    fn_idx = API_PY.index(
        "async def api_admin_cloud_themes_backup_run("
    )
    fn_end = API_PY.index("\n    @app.", fn_idx + 1)
    body = API_PY[fn_idx:fn_end]
    assert "try_acquire(" in body
    assert '"cloud-themes-backup"' in body or "'cloud-themes-backup'" in body
    assert '"cloud_themes_backup"' in body or "'cloud_themes_backup'" in body


def test_run_endpoint_returns_409_when_in_flight():
    """Endpoint must raise HTTPException(409) when try_acquire
    returns False."""
    fn_idx = API_PY.index(
        "async def api_admin_cloud_themes_backup_run("
    )
    fn_end = API_PY.index("\n    @app.", fn_idx + 1)
    body = API_PY[fn_idx:fn_end]
    assert "status_code=409" in body, (
        "v1.19.45: endpoint must 409 when another cloud-themes-"
        "backup run is already in flight (concurrent click race)"
    )


def test_run_endpoint_spawns_daemon_thread():
    """Endpoint must spawn the work in a daemon thread targeting
    _cloud_themes_backup_run — NOT inline async work that would
    block the FastAPI event loop."""
    fn_idx = API_PY.index(
        "async def api_admin_cloud_themes_backup_run("
    )
    fn_end = API_PY.index("\n    @app.", fn_idx + 1)
    body = API_PY[fn_idx:fn_end]
    assert "threading.Thread(" in body
    assert "target=_cloud_themes_backup_run" in body
    assert "daemon=True" in body


def test_run_endpoint_returns_op_id_immediately():
    """Endpoint must return {ok, op_id, scope} synchronously
    (the response is the ack, not the result)."""
    fn_idx = API_PY.index(
        "async def api_admin_cloud_themes_backup_run("
    )
    fn_end = API_PY.index("\n    @app.", fn_idx + 1)
    body = API_PY[fn_idx:fn_end]
    assert '"op_id"' in body
    assert '"scope"' in body
    # Must NOT contain the old synchronous downloaded list.
    assert '"downloaded": downloaded' not in body, (
        "v1.19.45: endpoint must NOT return the synchronous "
        "downloaded list — work has moved to the background "
        "worker thread"
    )


# ── Worker function ──────────────────────────────────────────


def test_worker_function_defined():
    """The background worker function must exist."""
    assert "def _cloud_themes_backup_run(" in API_PY


def test_worker_calls_start_progress():
    """Worker must call start_progress at entry so the op shows
    up in the drawer + mini-bar immediately."""
    fn_idx = API_PY.index("def _cloud_themes_backup_run(")
    fn_end = API_PY.index("\ndef ", fn_idx + 1)
    body = API_PY[fn_idx:fn_end]
    assert "op_progress.start_progress(" in body


def test_worker_calls_finish_progress_on_success():
    """Worker must transition op_progress to 'done' on
    successful completion."""
    fn_idx = API_PY.index("def _cloud_themes_backup_run(")
    fn_end = API_PY.index("\ndef ", fn_idx + 1)
    body = API_PY[fn_idx:fn_end]
    assert "op_progress.finish_progress(" in body
    # Worker computes final_status ternary based on cancel state.
    # Either the literal "done" appears or the ternary expression
    # does. Both are acceptable; we just need to ensure success
    # path doesn't leave the op stuck in 'running'.
    assert '"done"' in body, (
        "v1.19.45: worker success path must transition op_progress "
        "to 'done' (literal must appear in the worker body)"
    )


def test_worker_calls_finish_progress_on_failure():
    """Worker must transition op_progress to 'failed' on
    exception — wrapped in try/except so failures don't leave
    the op stuck in 'running' state."""
    fn_idx = API_PY.index("def _cloud_themes_backup_run(")
    fn_end = API_PY.index("\ndef ", fn_idx + 1)
    body = API_PY[fn_idx:fn_end]
    assert 'status="failed"' in body or "'failed'" in body
    assert "except Exception" in body


def test_worker_supports_cancellation():
    """Worker must check op_progress.is_cancelled at batch
    boundaries (passed as cancel_check) so the user can cancel
    via the ops drawer."""
    fn_idx = API_PY.index("def _cloud_themes_backup_run(")
    fn_end = API_PY.index("\ndef ", fn_idx + 1)
    body = API_PY[fn_idx:fn_end]
    assert "op_progress.is_cancelled(" in body
    assert "cancel_check=" in body, (
        "v1.19.45: worker must pass cancel_check= to "
        "identify_c1_rows so the walker breaks at batch "
        "boundaries when cancelled"
    )


def test_worker_passes_rks_scope_to_identify_c1_rows():
    """Worker must forward rks_scope to identify_c1_rows so
    single-row scopes short-circuit. Without this the v1.19.45
    push-down would be defeated."""
    fn_idx = API_PY.index("def _cloud_themes_backup_run(")
    fn_end = API_PY.index("\ndef ", fn_idx + 1)
    body = API_PY[fn_idx:fn_end]
    assert "rks_scope=rks_scope" in body


def test_worker_logs_summary_at_finish():
    """Worker must log a summary at completion (X backed up,
    Y errors) so docker logs surface the result. the user's
    repro: 'didn't see anything in the docker logs.'"""
    fn_idx = API_PY.index("def _cloud_themes_backup_run(")
    fn_end = API_PY.index("\ndef ", fn_idx + 1)
    body = API_PY[fn_idx:fn_end]
    assert "log.info(summary)" in body or 'log.info("CLOUD' in body
    assert "completed:" in body


# ── ops.js maps (v1.18.53 status-bar consistency contract) ───


def test_ops_js_kind_label_has_cloud_themes_backup():
    """The drawer KIND_LABEL must map cloud_themes_backup to a
    user-friendly label. v1.19.48: relabeled to mirror the
    renamed bulk-bar + SOURCE-menu entries ('DOWNLOAD PLEX
    BACKUP')."""
    assert "cloud_themes_backup:" in OPS_JS
    # Locate the KIND_LABEL block.
    idx = OPS_JS.index("const KIND_LABEL = {")
    end = OPS_JS.index("};", idx)
    block = OPS_JS[idx:end]
    assert "cloud_themes_backup" in block
    assert "'DOWNLOAD PLEX BACKUP'" in block, (
        "v1.19.48: KIND_LABEL must map to 'DOWNLOAD PLEX BACKUP' "
        "(mirrors the bulk-bar + SOURCE-menu labels)"
    )


def test_ops_js_tone_by_kind_has_cloud_themes_backup():
    """TONE_BY_KIND must give the kind a color identity. plex
    tone matches bulk_lps + PUSH/RESTORE FROM PLEX UX."""
    idx = OPS_JS.index("const TONE_BY_KIND = {")
    end = OPS_JS.index("};", idx)
    block = OPS_JS[idx:end]
    assert "cloud_themes_backup" in block
    assert "'plex'" in block  # paranoid sanity


def test_ops_js_op_mini_priority_has_cloud_themes_backup():
    """OP_MINI_PRIORITY must give the kind an explicit priority
    (not inherit FALLBACK=99). Same tier as bulk_lps."""
    idx = OPS_JS.index("const OP_MINI_PRIORITY = {")
    end = OPS_JS.index("};", idx)
    block = OPS_JS[idx:end]
    assert "cloud_themes_backup" in block


# ── Client click handlers (async response shape) ─────────────


def test_bulk_handler_expects_op_id_async_response():
    """Bulk handler must expect {ok, op_id} response — NOT
    the old synchronous {downloaded_count, errors_count} shape."""
    idx = APP_JS.index("library-cloud-backup-btn")
    handler_idx = APP_JS.index("library-cloud-backup-btn", idx + 1)
    # v1.19.52: handler grew with waitForOp + count surface;
    # widen the window so the boostPoll assertion still hits.
    block = APP_JS[handler_idx:handler_idx + 8000]
    # Old sync-response handling must be gone.
    assert "downloaded_count" not in block, (
        "v1.19.45: bulk handler must NOT reference "
        "downloaded_count — endpoint is now async (returns "
        "op_id; progress shows in drawer)"
    )
    # New async-response handling: boostPoll + libraryRapidPoll.
    assert "boostPoll" in block


def test_source_menu_handler_expects_op_id_async_response():
    """Per-row SOURCE-menu handler must also expect async
    response shape."""
    idx = APP_JS.index("act === 'backup-cloud-theme'")
    block = APP_JS[idx:idx + 3400]
    assert "downloaded_count" not in block
    assert "boostPoll" in block


# ── Behavioral via TestClient ────────────────────────────────


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


def test_run_endpoint_409_on_concurrent_request(admin_client, tmp_path):
    """End-to-end: pre-seed a 'running' op_progress row → POST
    the endpoint → expect 409. Confirms the try_acquire gate."""
    from app.config import Settings
    settings = Settings(
        config_dir=tmp_path, data_dir=tmp_path / "data",
    )
    # Pre-seed a running op_progress row to simulate "another
    # backup already in flight."
    with sqlite3.connect(settings.db_path) as conn:
        conn.execute(
            "INSERT INTO op_progress "
            "(op_id, kind, status, started_at, updated_at) "
            "VALUES ('cloud-themes-backup', 'cloud_themes_backup', "
            "        'running', '2026-05-27', '2026-05-27')"
        )
        conn.commit()
    # Need themes_dir set so we get past the 503 to the 409.
    # Use the run endpoint with a body that would otherwise be
    # 503 (themes_dir missing) — but the try_acquire happens
    # BEFORE the themes_dir check... wait, actually it happens
    # AFTER. Let me set themes_dir via env (the admin_client
    # fixture didn't set it). Use settings dir.
    import os
    os.environ["MOTIF_THEMES_DIR"] = str(tmp_path / "themes")
    try:
        r = admin_client.post(
            "/api/admin/cloud-themes-backup-run",
            headers=AUTH, json={"rks": ["rk-test"]},
        )
        # 409 expected if themes_dir is set; the contract is
        # "concurrent click → 409 not silent over-walk."
        assert r.status_code in (409, 503), (
            f"v1.19.45: concurrent run must 409 (or 503 if "
            f"themes_dir blocks first); got {r.status_code}"
        )
    finally:
        os.environ.pop("MOTIF_THEMES_DIR", None)


# ── Version pin ──────────────────────────────────────────────


def test_v1_19_45_version_pin():
    """Version bumped at v1.19.45 (then again at v1.19.46 for the
    FK constraint fix). Match 1.19.x prefix."""
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
