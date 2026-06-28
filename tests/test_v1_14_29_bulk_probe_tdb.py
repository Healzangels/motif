"""v1.14.29 — bulk-probe TDB URLs as a background job.

Builds on v1.14.28's single-row /probe-tdb endpoint. The single-row
button is fine for ad-hoc spot checks; before any destructive bulk
action (// LET PLEX SERVE on N rows), the user needs a way to verify
the recovery URLs for the WHOLE selection are alive in one click.

## Scope

1. Background runner `_bulk_probe_tdb_run(db_path, settings)` that:
   - Uses op_progress lifecycle (start/update/finish) so the live-
     ops drawer renders it for free
   - Honours a 24h cooldown via the SQL filter (skip rows probed in
     the window so back-to-back runs are no-ops)
   - Picks the row's CURRENT URL — override.youtube_url if present,
     else themes.youtube_url
   - Stamps last_probed_at on every probed row
   - Writes failure_kind PREEMPTIVELY for needs_manual_override
     results — same shape as the single-row /probe-tdb endpoint
   - cookies_expired stays indeterminate (don't change row state)
   - Cancellable mid-run via op_progress.is_cancelled
   - Batched verdict writes (BATCH_SIZE=50) — one txn per flush
   - Worker pool size capped at 3 (yt-dlp simulate is heavy +
     YouTube rate-limits aggressively)

2. POST /api/admin/bulk-probe-tdb — admin-only entrypoint that:
   - Refuses to start a second sweep while one is running (409)
   - Spawns the runner as a daemon thread
   - Returns {ok, op_id} matching REPROBE PLEX THEMES shape

3. Settings tile + JS handler (`bindBulkProbeTdb`).

4. Bulk LET PLEX SERVE confirm: opt-in probe-first prompt that, on
   accept, kicks off the bulk probe + aborts the LPS so the user
   retries after the sweep finishes (dead URLs will have moved out
   of the +P filter, their TDB pill turning red).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── Background runner ────────────────────────────────────────


def test_bulk_probe_tdb_run_function_defined():
    """The runner must exist at module scope so the route can
    `threading.Thread(target=_bulk_probe_tdb_run, ...)`.

    Whitespace-tolerant — v1.15.1 split the signature across
    multiple lines to add the optional `scope_items` kwarg."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    flat = " ".join(src.split())
    # Required positional args still match.
    assert "def _bulk_probe_tdb_run( db_path: Path, settings," in flat \
        or "def _bulk_probe_tdb_run(db_path: Path, settings" in flat


def test_bulk_probe_uses_op_progress_lifecycle():
    """Runner must drive op_progress so the live-ops drawer picks
    it up automatically — no extra wiring needed in the UI."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("def _bulk_probe_tdb_run(")
    body = src[fn_anchor:fn_anchor + 28000]
    assert 'OP_ID = "bulk-probe-tdb"' in body
    assert "op_progress.start_progress(" in body
    assert "op_progress.update_progress(" in body
    assert "op_progress.finish_progress(" in body


def test_bulk_probe_honours_24h_cooldown_in_sql():
    """24h cooldown must be SQL-level (filter clause) — Python-
    side checks race a concurrent admin trigger. Pin the SQL
    shape so a refactor can't accidentally drop the cooldown."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    assert "BULK_PROBE_COOLDOWN_HOURS = 24" in src
    fn_anchor = src.index("def _bulk_probe_tdb_run(")
    body = src[fn_anchor:fn_anchor + 28000]
    assert "t.last_probed_at IS NULL" in body
    assert "julianday('now'" in body
    assert "BULK_PROBE_COOLDOWN_HOURS" in body


def test_bulk_probe_targets_themes_youtube_url_only():
    """v1.14.33: bulk runner targets the TDB URL specifically
    (themes.youtube_url) — NOT the override. Pre-v1.14.33 the SQL
    did COALESCE(override_url, tdb_url) which was wrong for the
    same reason as the single-row endpoint: probe-says-alive on
    a working override masks a dead TDB URL.

    Pin both: the new shape (t.youtube_url AS target_url, NULLIF
    filter) AND the absence of the pre-fix LEFT JOIN on
    user_overrides."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("def _bulk_probe_tdb_run(")
    body = src[fn_anchor:fn_anchor + 28000]
    # New shape.
    assert "t.youtube_url AS target_url" in body
    assert "NULLIF(t.youtube_url, '') IS NOT NULL" in body
    # Pre-fix shape must be gone.
    assert "COALESCE(NULLIF(ovr.youtube_url, ''),"  not in body
    assert "user_overrides" not in body


def test_bulk_probe_stamps_last_probed_at_always():
    """Every probed row (alive, dead, indeterminate, error) must
    stamp last_probed_at so the cooldown filter on the NEXT run
    skips them. Batched via executemany."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("def _bulk_probe_tdb_run(")
    body = src[fn_anchor:fn_anchor + 28000]
    assert "pending_stamps.append" in body
    assert "UPDATE themes SET last_probed_at = ?" in body
    assert "executemany" in body


def test_bulk_probe_writes_failure_kind_preemptively_for_dead_urls():
    """Same write the single-row /probe-tdb endpoint does for
    needs_manual_override results. the user's option B — the dead-
    URL safety net for the LET PLEX SERVE flow."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("def _bulk_probe_tdb_run(")
    body = src[fn_anchor:fn_anchor + 28000]
    assert "result.needs_manual_override" in body
    assert "UPDATE themes SET failure_kind = ?" in body
    assert "failure_acked_at = NULL" in body
    assert "pending_dead" in body


def test_bulk_probe_cookies_expired_stays_indeterminate():
    """cookies_expired must NOT write failure_kind — without
    cookies the probe can't distinguish "actually dead" from
    "needs cookies". Pin the indeterminate branch."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("def _bulk_probe_tdb_run(")
    body = src[fn_anchor:fn_anchor + 28000]
    assert "FailureKind.COOKIES_EXPIRED" in body
    assert "n_indet" in body


def test_bulk_probe_uses_probe_youtube_url_helper():
    """Reuse the v1.10.43 helper, don't hand-roll yt-dlp. Imported
    at function scope so test fixtures that monkeypatch the
    module-level symbol still work."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("def _bulk_probe_tdb_run(")
    body = src[fn_anchor:fn_anchor + 28000]
    assert "from ..core.downloader import probe_youtube_url, FailureKind" in body
    # v1.15.11: cookies_file is now a per-call snapshot path
    # (per_call_cookies) instead of the shared `cookies` var, to
    # dodge yt-dlp's writeback race when 3 workers share
    # /config/cookies.txt. The shape "probe_youtube_url(url,
    # cookies_file=...)" is the load-bearing invariant — the
    # specific argument name moved.
    assert "probe_youtube_url(url, cookies_file=per_call_cookies)" in body


def test_bulk_probe_cancellable_mid_run():
    """The is_cancelled poll must run before each result is
    processed so a long sweep can be aborted from the drawer."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("def _bulk_probe_tdb_run(")
    body = src[fn_anchor:fn_anchor + 28000]
    assert "op_progress.is_cancelled(db_path, OP_ID)" in body
    assert 'status="cancelled"' in body
    # Flush pending writes BEFORE finishing on cancel so partial
    # progress isn't lost.
    cancel_anchor = body.index("if _cancel():")
    cancel_block = body[cancel_anchor:cancel_anchor + 1000]
    assert "_flush_batch()" in cancel_block


def test_bulk_probe_worker_pool_capped_modestly():
    """YouTube rate-limits aggressively; cap workers low.
    REPROBE uses 6 but that's HTTP Range-GETs to Plex — far
    lighter than yt-dlp --simulate.

    v1.15.12: dropped the cap 3 → 2 (the user's deployment was
    averaging 2.9 probes/s on 3 workers, over YouTube's
    threshold). v1.15.14: dropped 2 → 1 because 2 workers STILL
    triggered the throttle (1534 of 2127 probes returned
    transient in the v1.15.12 verify run). Definitive pinning
    lives in the v1.15.14 test
    (test_bulk_probe_max_workers_dropped_to_one); this test
    keeps the broader "must be a low single-digit cap"
    invariant."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    assert "BULK_PROBE_MAX_WORKERS = 1" in src
    # Defensive: anything > 1 risks rate-limit retrigger.
    for n in (2, 3, 4, 5, 6):
        assert f"BULK_PROBE_MAX_WORKERS = {n}" not in src


# ── Route handler ───────────────────────────────────────────


def test_bulk_probe_route_defined():
    """The POST endpoint must exist at the exact admin path."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    assert '@app.post("/api/admin/bulk-probe-tdb")' in src
    assert "async def api_admin_bulk_probe_tdb(" in src


def test_bulk_probe_route_admin_only_and_single_instance():
    """_require_admin gate + 409 on double-start. Mirror of
    REPROBE PLEX THEMES route.
    v1.15.37: the gate moved from a non-atomic
    `load_active(...) + any(... op_id ...)` check to an atomic
    `op_progress.try_acquire(db, "bulk-probe-tdb", ...)` call
    (closes the TOCTOU race where two near-simultaneous
    requests could both pass the check). Same op_id, same 409
    on conflict — just atomic now."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("async def api_admin_bulk_probe_tdb(")
    # v1.18.96: window widened 2000 → 5000 — the body-parse
    # fail-fast block added ~70 lines BEFORE try_acquire,
    # pushing the "bulk-probe-tdb" string past the old window.
    body = src[fn_anchor:fn_anchor + 5000]
    assert "_require_admin(request)" in body
    # v1.15.37: try_acquire takes the op_id as a positional arg.
    assert '"bulk-probe-tdb"' in body
    assert "try_acquire" in body
    assert "status_code=409" in body
    assert "bulk probe already running" in body


def test_bulk_probe_route_spawns_daemon_thread():
    """Must spawn a daemon thread so the HTTP response returns
    immediately and the runner survives across requests.

    Slice widened in v1.15.1 — the route grew to parse an
    optional `items` body for scoped probes (LPS pre-flight)."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("async def api_admin_bulk_probe_tdb(")
    # v1.18.96: window widened 4500 → 7000 — body-parse
    # fail-fast block + slot-acquire reorder added ~80 lines
    # ahead of the threading.Thread spawn.
    body = src[fn_anchor:fn_anchor + 7000]
    assert "import threading" in body
    assert "target=_bulk_probe_tdb_run" in body
    assert "daemon=True" in body
    # v1.15.1 added scope_count to the response. Relax the literal
    # match to just the {"ok": True} prefix.
    assert '"ok": True' in body and '"op_id": "bulk-probe-tdb"' in body


# ── Settings tile + JS ──────────────────────────────────────


def test_settings_template_has_bulk_probe_tile():
    """settings.html must include the // PROBE TDB URLS tile in
    the DOWNLOADS tab panel."""
    html = (REPO / "app" / "web" / "templates" / "settings.html").read_text()
    assert '// PROBE TDB URLS' in html
    assert 'id="bulk-probe-tdb-btn"' in html
    assert 'id="bulk-probe-tdb-status"' in html
    # Tucked inside the DOWNLOADS tab panel (same panel as
    # download tuning lives in) since it's adjacent to that flow.
    anchor = html.index('id="bulk-probe-tdb-btn"')
    # Walk back to find the nearest data-panel attribute.
    panel_idx = html.rfind('data-panel="', 0, anchor)
    panel_chunk = html[panel_idx:panel_idx + 80]
    assert 'data-panel="downloads"' in panel_chunk


def test_settings_template_explains_cooldown_and_concurrency():
    """The tile must explain the 24h cooldown + concurrency cap
    so the user understands a re-click was a no-op or a slow
    sweep is expected."""
    html = (REPO / "app" / "web" / "templates" / "settings.html").read_text()
    anchor = html.index('// PROBE TDB URLS')
    block = html[anchor:anchor + 3000]
    assert "24" in block  # cooldown hours referenced
    assert "Cookies-aware" in block or "cookies.txt" in block


def test_js_bulk_probe_handler_defined():
    """bindBulkProbeTdb wires the click → POST → status message
    flow. Mirrors bindReprobePlexThemes' shape."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert "function bindBulkProbeTdb()" in js
    fn_anchor = js.index("function bindBulkProbeTdb()")
    body = js[fn_anchor:fn_anchor + 2500]
    assert "/api/admin/bulk-probe-tdb" in body
    # Confirms before kicking off — yt-dlp simulate is heavy +
    # YouTube rate-limits, surface the cost in the prompt.
    assert "confirm(" in body
    # Live-ops drawer pointer.
    assert "// LIVE OPS" in body


def test_js_bulk_probe_handler_invoked_on_load():
    """bindBulkProbeTdb must be called from the page-load init
    block so the button is actually wired."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert "bindBulkProbeTdb();" in js


# ── Bulk LET PLEX SERVE: opt-in probe-first prompt ──────────


def test_bulk_let_plex_serve_uses_server_side_composite_endpoint():
    """v1.14.29 introduced the opt-in probe-first prompt as a
    safety gate: probe → abort LPS, user re-clicks. v1.15.28
    folded the gate server-side: one click → /api/admin/bulk-
    let-plex-serve which probes-then-unplaces in a single op,
    skipping targets probed alive within the cooldown window
    AND skipping unplace on rows the probe revealed dead.
    the user: "attempting a bulk action of let plex server doesn't
    do anything ... if an item was probed in the last 24hrs then
    no need to probe again." The safety gate from v1.14.29 still
    fires — it just lives server-side now."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    handler_anchor = js.index("library-let-plex-serve-btn")
    listener_anchor = js.index(
        "library-let-plex-serve-btn')?.addEventListener", handler_anchor,
    )
    body = js[listener_anchor:listener_anchor + 6000]
    assert "/api/admin/bulk-let-plex-serve" in body, (
        "v1.15.28: bulk LPS handler must use the new server-side "
        "composite endpoint"
    )


def test_bulk_let_plex_serve_describes_probe_then_unplace_sequence():
    """The single confirm must describe the full probe-then-
    unplace sequence so the user knows what's about to run.
    v1.14.29 split the description across two prompts; v1.15.28
    bundles it into one — the destructive-action description
    still carries the same key clauses about what's deleted vs
    kept."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    handler_anchor = js.index("library-let-plex-serve-btn")
    listener_anchor = js.index(
        "library-let-plex-serve-btn')?.addEventListener", handler_anchor,
    )
    body = js[listener_anchor:listener_anchor + 6000]
    assert "DELETE motif\\'s theme.mp3" in body
    assert "Proceed?" in body


# ── Reuse: schema v45 + last_probed_at column unchanged ─────


def test_schema_v45_still_in_force():
    """v1.14.29 reuses v1.14.28's themes.last_probed_at column.
    Nothing in v1.14.29 should bump the schema (no new tables,
    no new columns).
    v1.14.74: relaxed `== 45` → `>= 45`. The v45 column shape
    is intact; v46 (added in v1.14.74) only ALTERs plex_sections
    and doesn't touch themes.last_probed_at, so the v1.14.29
    contract still holds. Future migrations only need to update
    this assertion if they remove v45's themes.last_probed_at."""
    from app.core.db import CURRENT_SCHEMA_VERSION
    assert CURRENT_SCHEMA_VERSION >= 45


def test_probe_youtube_url_signature_unchanged():
    """The bulk runner calls probe_youtube_url with
    (url, cookies_file=cookies) — pin the helper's signature so
    a refactor doesn't accidentally break the bulk runner."""
    src = (REPO / "app" / "core" / "downloader.py").read_text()
    assert "def probe_youtube_url(" in src
    assert "cookies_file: Path | None = None" in src
