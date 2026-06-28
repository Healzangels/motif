"""v1.15.10 — // REPROBE FAILURES button (cooldown bypass for failed rows).

the user: "I'm seeing currently 1842 fails, it looks like from a
previous probe gone bad, I've tried reprobing a few of these
manually from the info card and they come back alive and clear
the failure but I have no way to bulk reprobe from settings
because of the 24hr cooldown when I try it just says done 0 -
processed"

## Pre-fix

The PROBE TDB URLS bulk button (v1.14.29) skipped any row
probed within the last 24h via this SQL filter:

    AND (t.last_probed_at IS NULL
         OR julianday(t.last_probed_at)
            < julianday('now', '-24 hours'))

That filter exists to prevent back-to-back full-library sweeps
from DOSing YouTube. But it also blocks the recovery case the user
hit: a previous probe ran with bad config (missing cookies, rate-
limit hammered, network blip) → 1842 rows red-pilled → operator
fixes the config → no way to bulk-recheck them until the cooldown
expires. The single-row reprobe (info card) has no cooldown, so
manual recovery works one row at a time but is impractical at
1842 rows.

## Fix

New `// REPROBE FAILURES` button next to `// PROBE TDB URLS` in
Settings → DOWNLOADS → PROBE TDB URLS section. Calls a new
`POST /api/admin/reprobe-tdb-failures` endpoint that re-uses
`_bulk_probe_tdb_run` with a new `scope_failures_only=True`
kwarg. The kwarg swaps the cooldown filter for
`AND t.failure_kind IS NOT NULL`, so the probe targets only
rows currently marked failed and ignores when they were last
probed.

Shares the bulk-probe-tdb op_id (server + JS watcher) so the
running-lock + LIVE OPS drawer just work — only one yt-dlp
probe loop can run at a time, and the completion watcher
handles both buttons identically.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"
APP_JS = REPO / "app" / "web" / "static" / "app.js"
SETTINGS_HTML = REPO / "app" / "web" / "templates" / "settings.html"


# ── Backend: _bulk_probe_tdb_run kwarg + SQL branch ────────────


def test_bulk_probe_run_accepts_scope_failures_only_kwarg():
    """The shared run function must accept scope_failures_only.
    The endpoint relies on the kwarg name being stable."""
    src = API_PY.read_text()
    fn_start = src.index("def _bulk_probe_tdb_run(")
    fn_signature_end = src.index(") -> None:", fn_start)
    sig = src[fn_start:fn_signature_end]
    assert "scope_failures_only: bool = False" in sig, (
        "_bulk_probe_tdb_run must accept scope_failures_only=False"
    )


def test_sql_branches_on_scope_failures_only():
    """The SELECT must use `failure_kind IS NOT NULL` when the
    kwarg is True (cooldown bypassed) and the existing cooldown
    filter otherwise. Pin both branches so a future refactor
    can't silently merge them and reintroduce the cooldown for
    the recovery path."""
    src = API_PY.read_text()
    fn_start = src.index("def _bulk_probe_tdb_run(")
    fn_end = src.index("\ndef ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    # Failure-only branch.
    assert "if scope_failures_only:" in fn_body
    assert "AND t.failure_kind IS NOT NULL" in fn_body, (
        "scope_failures_only branch must filter on failure_kind"
    )
    # Cooldown branch must still mention BULK_PROBE_COOLDOWN_HOURS
    # (preserved for the global PROBE TDB URLS button).
    assert "BULK_PROBE_COOLDOWN_HOURS" in fn_body
    assert "last_probed_at" in fn_body


def test_activity_label_distinguishes_recovery_from_global():
    """Operator should be able to tell from the LIVE OPS activity
    line whether this is a global sweep or a failures-only recovery.

    v1.15.15 refined the failures-only label from "(failures only,
    cooldown bypassed)" → "(visible failures only, cooldown
    bypassed)". v1.15.33 broadened the scope back to all red-pill
    failures (including acked rows) and updated the label to "(all
    red-pill failures, cooldown bypassed)" — the cooldown-bypass
    + failures-recovery framing survives, just with the new
    qualifier."""
    src = API_PY.read_text()
    fn_start = src.index("def _bulk_probe_tdb_run(")
    fn_end = src.index("\ndef ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    assert "failures, cooldown bypassed)" in fn_body, (
        "Failures-only activity must mention cooldown bypass"
    )
    assert "(24h cooldown applied)" in fn_body
    assert '"(failures only)"' in fn_body, (
        "stage_label should mark failures-only runs in the drawer"
    )


# ── Backend: POST /api/admin/reprobe-tdb-failures endpoint ─────


def test_reprobe_failures_endpoint_route_defined():
    """New endpoint with admin auth + same op-id running-lock as
    the global bulk probe (only one yt-dlp probe loop at a time).
    v1.15.37: the op-id gate is now atomic via
    `op_progress.try_acquire(...)` — the route still claims the
    same `bulk-probe-tdb` op_id so it can't race the global PROBE
    TDB URLS sweep."""
    src = API_PY.read_text()
    assert '@app.post("/api/admin/reprobe-tdb-failures")' in src
    assert "async def api_admin_reprobe_tdb_failures(" in src
    fn_start = src.index("async def api_admin_reprobe_tdb_failures(")
    fn_end = src.index("\n    @app.", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    assert "_require_admin(request)" in fn_body
    # Shared running-lock on the bulk-probe-tdb op_id (v1.15.37
    # via try_acquire, was load_active + any() pre-fix).
    assert '"bulk-probe-tdb"' in fn_body
    assert "try_acquire" in fn_body
    # Calls _bulk_probe_tdb_run with the kwarg set.
    assert "scope_failures_only" in fn_body
    assert "True" in fn_body  # scope_failures_only=True


def test_reprobe_failures_endpoint_passes_kwarg_to_thread():
    """Pin the kwarg dict shape so a future refactor doesn't drop
    the failures-only flag and silently turn the button into a
    duplicate of the global probe."""
    src = API_PY.read_text()
    fn_start = src.index("async def api_admin_reprobe_tdb_failures(")
    fn_end = src.index("\n    @app.", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    assert 'kwargs={"scope_failures_only": True}' in fn_body
    assert "target=_bulk_probe_tdb_run" in fn_body


def test_reprobe_failures_logs_intent_on_start():
    """The activity log must distinguish a failures-only run from
    a global sweep so the events feed reads cleanly when both
    buttons see use in the same day."""
    src = API_PY.read_text()
    fn_start = src.index("async def api_admin_reprobe_tdb_failures(")
    fn_end = src.index("\n    @app.", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    assert "REPROBE TDB FAILURES" in fn_body
    assert "cooldown bypassed" in fn_body


# ── Frontend: settings.html button ─────────────────────────────


def test_settings_template_has_reprobe_failures_button():
    """The button + status span must live in the same PROBE TDB
    URLS section. JS reads the IDs directly."""
    html = SETTINGS_HTML.read_text()
    section_start = html.index('<h2 class="block-title">// PROBE TDB URLS</h2>')
    section = html[section_start:section_start + 3000]
    assert 'id="reprobe-tdb-failures-btn"' in section
    assert 'id="reprobe-tdb-failures-status"' in section
    assert "// REPROBE FAILURES" in section


def test_settings_template_explains_recovery_intent():
    """Hint text must explain when to use this vs the global
    PROBE TDB URLS — the user read the cooldown copy and didn't
    have a way out before v1.15.10. Pin the intent phrase so
    a future copy edit doesn't drop the recovery framing."""
    html = SETTINGS_HTML.read_text()
    section_start = html.index('<h2 class="block-title">// PROBE TDB URLS</h2>')
    section = html[section_start:section_start + 3000]
    assert "REPROBE FAILURES" in section
    assert "bypasses" in section.lower(), (
        "Hint must say it bypasses the cooldown"
    )


# ── Frontend: app.js handler ───────────────────────────────────


def test_js_handler_function_defined_and_wired():
    """bindReprobeTdbFailures must exist + be wired into the page-
    load init alongside bindBulkProbeTdb."""
    src = APP_JS.read_text()
    assert "function bindReprobeTdbFailures()" in src
    assert "bindReprobeTdbFailures();" in src


def test_js_handler_posts_to_failures_endpoint():
    """Click handler must POST to the new failures endpoint —
    pinning the URL guards against a copy-paste regression that
    accidentally hits the global probe endpoint instead."""
    src = APP_JS.read_text()
    fn_start = src.index("function bindReprobeTdbFailures()")
    fn_end = src.index("\n  function ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    assert "/api/admin/reprobe-tdb-failures" in fn_body
    assert "api('POST'" in fn_body
    # Reuses the bulk_probe_tdb completion watcher (shared op_id).
    assert "_watchOpForCompletion('bulk_probe_tdb'" in fn_body


def test_js_handler_confirm_explains_cooldown_bypass():
    """The confirm() copy must explain that the 24h cooldown is
    bypassed — the user's confusion was specifically that the
    existing button silently no-ops because of the cooldown."""
    src = APP_JS.read_text()
    fn_start = src.index("function bindReprobeTdbFailures()")
    fn_end = src.index("\n  function ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    assert "confirm(" in fn_body
    assert "24 h cooldown" in fn_body or "Bypasses" in fn_body
