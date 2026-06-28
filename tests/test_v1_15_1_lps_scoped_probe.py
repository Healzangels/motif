"""v1.15.1 — LET PLEX SERVE pre-flight probe scoped to selection.

the user: "when doing a bulk let server it starts a probe
themerdb which probes every themerrdb url not just the url of
the bulk let plex serve action is this intended vehavior?"

## Pre-fix

The LPS pre-flight prompt's "OK" path called
`POST /api/admin/bulk-probe-tdb` with no body. The endpoint
walked EVERY themes row with a TDB URL (modulo the 24h cooldown
skip). Net effect: clicking LPS on 5 selected rows kicked a
global probe of potentially thousands. Not a correctness bug —
the 5 selected rows did get probed eventually — but:

  - wasted time (waiting for thousands of probes for 5 we care
    about)
  - burned 24h cooldown on rows the user didn't intend to LPS
  - larger rate-limit footprint with YouTube

## Fix

Extend the bulk-probe-tdb endpoint to accept an optional
`items` body. When present, the SQL adds an
`AND (t.media_type, t.tmdb_id) IN (...)` clause scoping the
probe to just those rows. 24h cooldown is still respected.

The settings PROBE TDB URLS button stays unchanged (no body =
global probe). The LPS pre-flight passes the user's selection
so the probe stays scoped.

Limit cap: scope_items > 499 rows falls back to a global probe
(SQLite's default 999-param cap ≈ 499 pairs). Selections that
big are already a "probably want global anyway" signal.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"
APP_JS = REPO / "app" / "web" / "static" / "app.js"


# ── Backend: scope_items kwarg + SQL narrowing ─────────────────


def test_bulk_probe_tdb_run_accepts_scope_items_kwarg():
    """The worker function signature must include the optional
    `scope_items` kwarg so the route handler can pass through
    the parsed selection."""
    src = API_PY.read_text()
    # Anchor on the function def — must include scope_items.
    fn_start = src.index("def _bulk_probe_tdb_run(")
    # Walk forward to the closing `:` of the def header.
    fn_header = src[fn_start:fn_start + 400]
    assert "scope_items" in fn_header, (
        "_bulk_probe_tdb_run must accept scope_items kwarg"
    )
    assert "list[tuple[str, int]] | None" in fn_header, (
        "scope_items must be typed list[(media_type, tmdb_id)] | None"
    )


def test_bulk_probe_sql_narrows_when_scope_items_provided():
    """When scope_items is provided, the SQL must add an
    `IN (...)` clause restricting to the selected rows.
    Pre-fix the SQL only filtered by url-non-null + cooldown."""
    src = API_PY.read_text()
    # Anchor on the v1.15.1 marker comment in the SQL block.
    anchor = src.index(
        "v1.15.1: when scope_items is provided"
    )
    block = src[anchor:anchor + 2500]
    assert "(t.media_type, t.tmdb_id) IN" in block, (
        "Scoped probe must use IN-tuples filter on (media_type, tmdb_id)"
    )
    # The placeholder pair list must respect SQLite's param cap.
    assert "499" in block, (
        "Param-cap fallback must be enforced (499 pairs ≈ 998 params)"
    )


def test_route_handler_parses_items_body():
    """The /api/admin/bulk-probe-tdb route must parse the
    optional `items` body, validate each {media_type, tmdb_id}
    pair, and pass the parsed list as `scope_items` to the
    worker."""
    src = API_PY.read_text()
    route_start = src.index("async def api_admin_bulk_probe_tdb(")
    route_end = src.index("\n    @app.", route_start + 1)
    route_block = src[route_start:route_end]
    # Body parsing. v1.18.96 swapped `await request.json()` for
    # `await request.body()` + `json.loads(...)` so the handler
    # can distinguish empty body (intentional all-rows) from
    # non-empty malformed body (400). Either shape satisfies the
    # contract — the test just pins that the body IS being parsed
    # and the items field IS being read.
    assert ("request.json()" in route_block
            or "json.loads(raw_body)" in route_block), (
        "v1.15.1 contract: handler must parse the request body "
        "(via request.json() OR raw_body + json.loads — v1.18.96 "
        "switched to the raw-body shape for empty-vs-malformed "
        "distinction)"
    )
    assert 'body.get("items")' in route_block
    # Per-pair validation. v1.20.25: collection included so bulk PROBE
    # TDB SELECTED covers collections (they have a TDB URL to probe).
    assert 'mt in ("movie", "tv", "collection")' in route_block
    assert "isinstance(tid, int)" in route_block
    # Pass-through to the worker.
    assert 'kwargs={"scope_items": scope_items}' in route_block


def test_route_handler_logs_scope_in_event():
    """The log_event must include the scope label so /queue's
    events list distinguishes scoped probes from global ones.
    Helps diagnosis when a probe runs longer/shorter than
    expected."""
    src = API_PY.read_text()
    route_start = src.index("async def api_admin_bulk_probe_tdb(")
    route_end = src.index("\n    @app.", route_start + 1)
    route_block = src[route_start:route_end]
    assert "scope_label" in route_block
    # Both branches of the label.
    assert '(all rows)' in route_block
    assert 'selected)' in route_block


def test_route_handler_returns_scope_count_in_response():
    """The response must include scope_count so the JS can
    surface "probing N selected rows" in the alert text."""
    src = API_PY.read_text()
    route_start = src.index("async def api_admin_bulk_probe_tdb(")
    route_end = src.index("\n    @app.", route_start + 1)
    route_block = src[route_start:route_end]
    assert '"scope_count":' in route_block


# ── Frontend: LPS handler passes selection ─────────────────────


def test_lps_handler_scopes_probe_to_selection():
    """v1.15.1's scope-aware probing intent — don't burn the 24h
    cooldown by re-probing every theme row when the user only
    wants to LPS a few — survives v1.15.28's server-side rework.
    The new POST /api/admin/bulk-let-plex-serve receives `items`
    and only probes that subset internally (in _bulk_lps_run).
    Pin the per-target shape so the JS still passes media_type
    + tmdb_id (with section_id added for the v1.15.28 unplace
    scoping)."""
    src = APP_JS.read_text()
    handler_anchor = src.index(
        "document.getElementById('library-let-plex-serve-btn')?.addEventListener"
    )
    block = src[handler_anchor:handler_anchor + 8000]
    # New endpoint name.
    assert "/api/admin/bulk-let-plex-serve" in block
    # Per-target items shape.
    assert "items: targets.map" in block
    assert "media_type: t.mt" in block
    assert "tmdb_id: t.id" in block


def test_lps_prompt_describes_cooldown_skip_for_fresh_targets():
    """v1.15.1's prompt explicitly told the user the probe was
    scoped + that 24h-fresh rows would be skipped. v1.15.28's
    single confirm carries that same guarantee in the new
    sequence description — the user specifically asked for the
    skip-fresh behavior: "if an item was probed in the last
    24hrs then no need to probe again." Pin the 24-h skip
    phrasing so it stays surfaced to the user."""
    src = APP_JS.read_text()
    handler_anchor = src.index(
        "document.getElementById('library-let-plex-serve-btn')?.addEventListener"
    )
    block = src[handler_anchor:handler_anchor + 8000]
    # Locate the confirm() call inside the handler.
    confirm_anchor = block.index("confirm(")
    confirm_block = block[confirm_anchor:confirm_anchor + 2500]
    # The skip-when-fresh guarantee must be in the prompt.
    assert "last 24" in confirm_block, (
        "v1.15.28: LPS prompt must surface the 24h cooldown skip "
        "behavior (the user's no-need-to-re-probe-fresh request)"
    )


def test_lps_handler_kicks_server_side_op_visible_in_live_ops():
    """v1.15.1's success alert showed "{N} selected row(s)" so
    the user could verify the scope. v1.15.28 replaced the alert
    with a server-side op_progress entry visible in LIVE OPS —
    the same scope information is in the op's
    activity/stage_label messages. Pin the new flow: handler
    POSTs once, then watches the bulk_lps op via motifOps.state()
    instead of relying on a one-shot alert."""
    src = APP_JS.read_text()
    handler_anchor = src.index(
        "document.getElementById('library-let-plex-serve-btn')?.addEventListener"
    )
    block = src[handler_anchor:handler_anchor + 8000]
    # POST happens once (single api() call to the composite
    # endpoint).
    assert block.count("api('POST', '/api/admin/bulk-let-plex-serve'") == 1
    # The handler watches bulk_lps op completion.
    assert "'bulk_lps'" in block


# ── Regression guards ──────────────────────────────────────────


def test_settings_probe_button_unchanged_no_items_body():
    """The settings PROBE TDB URLS button (bindBulkProbeTdb)
    must NOT pass an items body — that endpoint is for global
    probes. The route's body parser tolerates a missing body
    (parsed as `None` → global), but verify the JS still posts
    without one to keep the call sites distinct."""
    src = APP_JS.read_text()
    fn_start = src.index("function bindBulkProbeTdb()")
    fn_end = src.index("function ", fn_start + 1)
    fn_block = src[fn_start:fn_end]
    # The settings handler's POST is bare (no second arg).
    assert "api('POST', '/api/admin/bulk-probe-tdb');" in fn_block, (
        "Settings PROBE TDB URLS button must call without an items "
        "body (global probe path)"
    )


def test_v1_15_1_marker_explains_the_scoping():
    """A v1.15.1 marker on the worker function explains the user's
    repro so a future "simplify the kwarg" refactor sees the
    rationale."""
    src = API_PY.read_text()
    fn_start = src.index("def _bulk_probe_tdb_run(")
    # The docstring sits inside the function body.
    fn_block = src[fn_start:fn_start + 3500]
    assert "v1.15.1" in fn_block
    assert "LET PLEX SERVE pre-flight" in fn_block or \
        "scope_items" in fn_block
