"""v1.15.28 — bulk LET PLEX SERVE: one click, server-side
probe-then-unplace, fresh-target skip.

## Why

the user's v1.15.25 feedback (item still open after v1.15.27):

> "attempting a bulk action of let plex server doesn't do
>  anything. it indicates it will reprobe the selected items
>  but nothing ends up happening or is seen in the status bar."

Plus, after v1.15.27 dropped:

> "lets go with recommended as it reduces clicks as if an item
>  was probed in the last 24hrs then no need to probe again."

## Pre-fix flow (v1.14.27)

1. Click LPS → confirm "Run probe first?"
2. JS POSTs /api/admin/bulk-probe-tdb with explicit items —
   v1.15.20 made explicit-items BYPASS the 24h cooldown so even
   freshly-probed rows got re-probed.
3. Probe runs in background; the topbar mini-bar misses brief
   sub-poll-cadence ops, so the user typically sees no visible
   state change → gives up.
4. If the user IS persistent, they re-click LPS, choose "Cancel"
   at the probe prompt, then confirm the destructive action,
   then JS fires per-row /unplace calls.

The flow had three failure modes the user hit: invisible probe,
unnecessary re-probe of fresh data, and two-stage confirm dance.

## Post-fix flow

1. Click LPS → single confirm describing the full sequence.
2. JS POSTs /api/admin/bulk-let-plex-serve {items}.
3. Server spawns _bulk_lps_run thread:
   - PROBE stage: skips targets with last_probed_at within
     BULK_PROBE_COOLDOWN_HOURS (the user's "no need to probe
     again" requirement); probes the rest using the same
     ThreadPoolExecutor + concurrency + batched-write pattern
     as _bulk_probe_tdb_run.
   - UNPLACE stage: re-queries failure_kind for all targets;
     for each row where failure_kind IS NULL (alive — fresh
     OR confirmed by the probe stage), unlinks theme.mp3 from
     the per-target section's Plex folder + deletes the
     placement row. Targets where the probe revealed dead get
     skipped (the safety gate v1.14.29 was designed to
     enforce).
4. Single op_progress entry visible in LIVE OPS, kind=bulk_lps,
   stage transitions probe → unplace.
5. JS button shows "// PROBING + UNPLACING…" and watches the
   op via motifOps.state() until it exits the active set.
   On terminal transition: button resets, library reloads.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"
APP_JS = REPO / "app" / "web" / "static" / "app.js"


# ── Server: _bulk_lps_run thread function ─────────────────────


def test_bulk_lps_run_function_exists():
    """_bulk_lps_run is the new thread function backing the
    bulk-let-plex-serve endpoint. Must live alongside the
    other bulk thread functions in api.py."""
    src = API_PY.read_text()
    assert "def _bulk_lps_run(" in src, (
        "v1.15.28: _bulk_lps_run thread function must be defined "
        "in api.py"
    )


def test_bulk_lps_uses_distinct_op_id_and_kind():
    """The bulk LPS op uses op_id='bulk-lps' + kind='bulk_lps' so
    it doesn't conflict with the existing bulk-probe-tdb op gate.
    Pin both — the JS watcher (function bindBulk* and the
    finishWatcher in the LPS handler) anchors on the kind, the
    409 already-running guard anchors on the op_id."""
    src = API_PY.read_text()
    fn_anchor = src.index("def _bulk_lps_run(")
    fn_end = src.index("\ndef _", fn_anchor + 1)
    fn_body = src[fn_anchor:fn_end]
    assert 'OP_ID = "bulk-lps"' in fn_body
    assert 'kind="bulk_lps"' in fn_body


def test_bulk_lps_skips_targets_probed_within_cooldown():
    """the user: "if an item was probed in the last 24hrs then no
    need to probe again." The probe-stage SQL must filter by
    last_probed_at against the BULK_PROBE_COOLDOWN_HOURS window
    (using julianday math like the existing global probe path)
    and only probe targets that are stale OR never probed."""
    src = API_PY.read_text()
    fn_anchor = src.index("def _bulk_lps_run(")
    fn_end = src.index("\ndef _", fn_anchor + 1)
    fn_body = src[fn_anchor:fn_end]
    assert "last_probed_at IS NULL" in fn_body, (
        "v1.15.28: bulk LPS probe stage must skip targets probed "
        "alive within the cooldown window — pin the SQL filter so "
        "a future refactor can't silently re-introduce the v1.15.20 "
        "explicit-items cooldown bypass for this flow"
    )
    assert "BULK_PROBE_COOLDOWN_HOURS" in fn_body
    assert "julianday" in fn_body


def test_bulk_lps_unplace_stage_gates_on_alive_after_probe():
    """The unplace stage must re-query failure_kind post-probe
    and only act on rows where failure_kind IS NULL. This is the
    safety gate the v1.14.29 probe-first prompt was designed to
    enforce — without it, the bulk LPS would silently destroy
    the recovery file on rows the probe revealed as dead."""
    src = API_PY.read_text()
    fn_anchor = src.index("def _bulk_lps_run(")
    fn_end = src.index("\ndef _", fn_anchor + 1)
    fn_body = src[fn_anchor:fn_end]
    # Locate the unplace-stage anchor.
    unplace_anchor = fn_body.index("UNPLACE stage")
    unplace_block = fn_body[unplace_anchor:]
    # Re-query alive-after-probe.
    assert "failure_kind IS NULL" in unplace_block, (
        "v1.15.28: unplace stage must gate on failure_kind IS NULL "
        "(post-probe alive predicate) — without it, dead rows get "
        "their recovery file destroyed"
    )
    # Per-target unlink + placements row delete.
    assert 'theme.mp3' in unplace_block
    assert "DELETE FROM placements" in unplace_block


def test_bulk_lps_uses_op_progress_with_stage_transition():
    """Single op_progress lifecycle, with stage transitioning
    from "probe" to "unplace" so the LIVE OPS card reflects the
    sequence the user can watch."""
    src = API_PY.read_text()
    fn_anchor = src.index("def _bulk_lps_run(")
    fn_end = src.index("\ndef _", fn_anchor + 1)
    fn_body = src[fn_anchor:fn_end]
    # Single start_progress at the top.
    assert fn_body.count("op_progress.start_progress(") == 1
    # Stage transition — probe → unplace.
    assert 'stage="probe"' in fn_body
    assert 'stage="unplace"' in fn_body
    # finish_progress called on done / failed / cancelled paths.
    assert 'status="done"' in fn_body
    assert 'status="cancelled"' in fn_body
    assert 'status="failed"' in fn_body


def test_bulk_lps_run_supports_per_target_section_id():
    """LPS must scope unplace to the per-target section_id (the
    row's source library) so that unplacing a row in 4K Movies
    doesn't accidentally unplace the standard Movies sibling.
    Mirrors the v1.12.77 section_id scoping in /unplace."""
    src = API_PY.read_text()
    fn_anchor = src.index("def _bulk_lps_run(")
    fn_end = src.index("\ndef _", fn_anchor + 1)
    fn_body = src[fn_anchor:fn_end]
    # Per-target section_id read.
    assert 't.get("section_id")' in fn_body
    # SQL placeholders include section_id.
    assert "AND section_id = ?" in fn_body


# ── Server: /api/admin/bulk-let-plex-serve route ──────────────


def test_bulk_lps_endpoint_registered():
    """The new endpoint must be wired up via @app.post."""
    src = API_PY.read_text()
    assert '@app.post("/api/admin/bulk-let-plex-serve")' in src
    assert "async def api_admin_bulk_let_plex_serve(" in src


def test_bulk_lps_endpoint_requires_admin_and_validates_items():
    """The route handler must require admin + reject empty
    item lists with 400 (not silently spawn a no-op thread)."""
    src = API_PY.read_text()
    fn_anchor = src.index("async def api_admin_bulk_let_plex_serve(")
    fn_end = src.index("\n    # ", fn_anchor + 1)
    fn_body = src[fn_anchor:fn_end]
    # Admin check.
    assert "_require_admin(request)" in fn_body
    # Empty-items rejection.
    assert "status_code=400" in fn_body
    assert "non-empty list" in fn_body


def test_bulk_lps_endpoint_409s_on_already_running():
    """Concurrent bulk LPS attempts must be refused (409) — only
    one bulk LPS op can run at a time. Anchors on the op_id +
    409 so the gate matches the canonical pattern shared with
    reprobe-plex-themes / bulk-probe-tdb.
    v1.15.37: gate moved to atomic `op_progress.try_acquire`
    (closes the TOCTOU race the v1.15.28 load_active +
    any() pattern had)."""
    src = API_PY.read_text()
    fn_anchor = src.index("async def api_admin_bulk_let_plex_serve(")
    fn_end = src.index("\n    # ", fn_anchor + 1)
    fn_body = src[fn_anchor:fn_end]
    assert '"bulk-lps"' in fn_body
    assert "try_acquire" in fn_body
    assert "status_code=409" in fn_body


def test_bulk_lps_endpoint_spawns_thread_and_returns_op_id():
    """The handler must spawn _bulk_lps_run as a daemon thread
    and return {ok, op_id, target_count} so the JS can wire up
    the completion watcher."""
    src = API_PY.read_text()
    fn_anchor = src.index("async def api_admin_bulk_let_plex_serve(")
    fn_end = src.index("\n    # ", fn_anchor + 1)
    fn_body = src[fn_anchor:fn_end]
    assert "target=_bulk_lps_run" in fn_body
    assert "daemon=True" in fn_body
    assert '"op_id": "bulk-lps"' in fn_body
    assert '"target_count"' in fn_body


# ── Client: library LPS handler ───────────────────────────────


def test_js_lps_handler_uses_new_bulk_endpoint():
    """The library bulk LPS handler must POST to the new
    /api/admin/bulk-let-plex-serve endpoint. Pre-fix it called
    /api/admin/bulk-probe-tdb (probe-only) + per-row /unplace
    in a manual two-stage flow."""
    src = APP_JS.read_text()
    # Anchor on the addEventListener line (the click handler),
    # not the earlier visibility-toggle reference to the same id.
    handler_anchor = src.index(
        "document.getElementById('library-let-plex-serve-btn')?.addEventListener"
    )
    block = src[handler_anchor:handler_anchor + 8000]
    assert "/api/admin/bulk-let-plex-serve" in block, (
        "v1.15.28: bulk LPS handler must POST to the new server-"
        "side composite endpoint, not the legacy probe-only one"
    )


def test_js_lps_handler_sends_section_id_per_target():
    """Each item in the POST body must carry section_id so the
    server can scope the unplace correctly. media_type +
    tmdb_id are the existing per-target identifiers."""
    src = APP_JS.read_text()
    # Anchor on the addEventListener line (the click handler),
    # not the earlier visibility-toggle reference to the same id.
    handler_anchor = src.index(
        "document.getElementById('library-let-plex-serve-btn')?.addEventListener"
    )
    block = src[handler_anchor:handler_anchor + 8000]
    # Locate the items.map call inside the POST body.
    items_map_anchor = block.index("items: targets.map(")
    items_map_block = block[items_map_anchor:items_map_anchor + 400]
    assert "media_type: t.mt" in items_map_block
    assert "tmdb_id: t.id" in items_map_block
    assert "section_id: t.section_id" in items_map_block


def test_js_lps_handler_drops_two_stage_prompt():
    """The pre-fix handler had a two-stage confirm: "Run probe
    first?" then (on cancel) "Proceed?". The post-fix flow
    collapses to one confirm describing the full sequence."""
    src = APP_JS.read_text()
    # Anchor on the addEventListener line (the click handler),
    # not the earlier visibility-toggle reference to the same id.
    handler_anchor = src.index(
        "document.getElementById('library-let-plex-serve-btn')?.addEventListener"
    )
    block = src[handler_anchor:handler_anchor + 8000]
    # The pre-fix prompt-1 phrasing must be gone.
    assert "Run // PROBE TDB URLS first?" not in block, (
        "v1.15.28: the two-stage probe-first prompt was replaced "
        "by a single confirm — the pre-fix phrasing must not be "
        "re-introduced"
    )


def test_js_lps_handler_drops_per_row_unplace_loop():
    """The pre-fix handler iterated targets and called per-row
    /unplace. The new flow lets the server-side _bulk_lps_run
    handle that internally — no per-row /unplace from this
    handler."""
    src = APP_JS.read_text()
    # Anchor on the addEventListener line (the click handler),
    # not the earlier visibility-toggle reference to the same id.
    handler_anchor = src.index(
        "document.getElementById('library-let-plex-serve-btn')?.addEventListener"
    )
    block = src[handler_anchor:handler_anchor + 8000]
    # Per-row unplace URL pattern from the pre-fix flow.
    assert "/unplace?section_id=" not in block, (
        "v1.15.28: per-row /unplace calls moved server-side; "
        "the JS handler should only POST to "
        "/api/admin/bulk-let-plex-serve"
    )


def test_js_lps_handler_watches_op_completion():
    """After kicking the bulk LPS, the handler must watch the
    op via motifOps.state() (matching the pattern used by other
    bulk ops) and reset the button + reload library only when
    the op exits the active set. Pre-fix used a fixed 4s timeout
    that frequently fired before the per-row /unplace loop
    completed."""
    src = APP_JS.read_text()
    # Anchor on the addEventListener line (the click handler),
    # not the earlier visibility-toggle reference to the same id.
    handler_anchor = src.index(
        "document.getElementById('library-let-plex-serve-btn')?.addEventListener"
    )
    block = src[handler_anchor:handler_anchor + 8000]
    # Watcher anchors on the new op kind.
    assert "'bulk_lps'" in block
    # Watcher uses motifOps.state() like the shared
    # _watchOpForCompletion helper.
    assert "motifOps.state" in block
    # On terminal transition, library reloads.
    assert "loadLibrary()" in block
