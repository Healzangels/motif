"""v1.14.49 — probe-alive clears stale stored failure state.

the user repro on v1.14.48: clicked PROBE TDB URL on 1408 (acked
network_error failure on a TDB-tracked row). The probe came back
✓ alive but the INFO card still read "✓ ACKED — TDB UNAVAILABLE"
and the row's M chip still wore the amber acked-failure dot.

Root cause: the v1.14.28 probe endpoint asymmetrically wrote
state on dead probes (preemptive failure_kind) but did NOTHING
on alive probes (just stamped last_probed_at). A stale failure
contradicting the probe result is the natural consequence.

Fix: when probe returns alive (`result is None`), mirror the
worker's success-clear pattern (worker.py:1217-1235) verbatim:
  - clear themes.failure_kind / failure_message / failure_at /
    failure_acked_at
  - DELETE matching section_failure_acks rows so per-section
    sfa rows don't keep the FAIL count + ATTN axis lying

Response shape gains `failure_cleared: bool` so the JS handler
can surface "(stale failure cleared)" inline + reload the library
+ topbar (mirroring the dead-branch reload). No reload fires when
nothing was cleared — keeps the alive-on-clean-row case silent.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── Server: alive-probe clears stored failure state ──────────


def test_probe_alive_clears_failure_kind():
    """The probe endpoint must clear failure_kind / message / at /
    acked_at when result is None (alive). Mirror of the worker's
    themerrdb-success clear at worker.py:1217."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("async def api_probe_tdb(")
    body = src[fn_anchor:fn_anchor + 15000]
    # The v1.14.49 marker explains the symmetry rationale.
    assert "v1.14.49: alive-probe symmetric to dead-probe write" in body
    # The clear UPDATE itself.
    assert "if result is None:" in body
    assert "SET failure_kind = NULL" in body
    assert "failure_message = NULL" in body
    assert "failure_at = NULL" in body
    assert "failure_acked_at = NULL" in body
    # The guard prevents wasted writes when nothing's stored.
    assert "AND failure_kind IS NOT NULL" in body


def test_probe_alive_drops_section_failure_acks():
    """The clear must also DELETE matching section_failure_acks
    rows. Without the sfa drop, per-section ack state survives
    even though the title-global flags went NULL — the FAIL pill
    breakdown + library SQL filter on `sfa.acked_at IS NULL` would
    silently exclude affected sections (same audit-P0 #1 bug class
    that motivated worker.py:1231-1235)."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("async def api_probe_tdb(")
    body = src[fn_anchor:fn_anchor + 15000]
    # The DELETE sits inside the alive branch.
    branch_anchor = body.index("if result is None:")
    branch_block = body[branch_anchor:branch_anchor + 2000]
    assert "DELETE FROM section_failure_acks" in branch_block
    assert "WHERE media_type = ? AND tmdb_id = ?" in branch_block


def test_probe_alive_clear_is_conditional_on_actual_failure_present():
    """The DELETE FROM section_failure_acks must only fire when
    the themes-level UPDATE actually changed a row (stored failure
    existed). Avoids a wasted DELETE on every clean-row probe.

    Pin via cursor.rowcount check + the conditional DELETE shape."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("async def api_probe_tdb(")
    body = src[fn_anchor:fn_anchor + 15000]
    branch_anchor = body.index("if result is None:")
    branch_block = body[branch_anchor:branch_anchor + 2000]
    assert "failure_cleared = cur.rowcount > 0" in branch_block
    assert "if failure_cleared:" in branch_block


def test_probe_alive_response_shape_includes_failure_cleared():
    """The alive-probe response (`{ok: True, kind: None, ...}`)
    must include `failure_cleared: bool`. The JS handler reads
    this to decide whether to surface the "(stale failure
    cleared)" hint + reload the library."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("async def api_probe_tdb(")
    body = src[fn_anchor:fn_anchor + 15000]
    # The alive return statement carries failure_cleared.
    return_anchor = body.index('"ok": True, "kind": None, "message": None,')
    return_block = body[return_anchor:return_anchor + 400]
    assert '"failure_cleared": failure_cleared' in return_block


def test_probe_dead_branch_unchanged():
    """Sanity: the v1.14.28 dead-URL preemptive write must still
    fire (gated on `result.needs_manual_override`). v1.14.49 is
    purely additive on the alive branch."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("async def api_probe_tdb(")
    body = src[fn_anchor:fn_anchor + 15000]
    assert "if result is not None and result.needs_manual_override:" in body
    # The dead-write SQL stays as-is.
    assert 'UPDATE themes SET failure_kind = ?,' in body


# ── Client: hint + reload on alive-with-clear ────────────────


def test_js_probe_handler_renders_failure_cleared_hint():
    """When res.failure_cleared is true, the inline result text
    must surface "✓ alive (stale failure cleared)" so the user
    knows why the row's red ! / amber acked-failure dot just
    disappeared. False/missing → falls back to plain "✓ alive"."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    handler_anchor = js.index('body.querySelector(\'button[data-act="probe-tdb"]\')')
    body = js[handler_anchor:handler_anchor + 4000]
    assert "res.failure_cleared" in body
    assert "'✓ alive (stale failure cleared)'" in body
    # The fallback (no clear) keeps the original text.
    assert "'✓ alive'" in body


def test_js_probe_handler_reloads_library_on_alive_with_clear():
    """When res.failure_cleared is true, the JS must reload the
    library + refresh the topbar so the row's pill + count update
    immediately. Without the reload the user has to wait ~30s for
    the auto-poll to catch the new clean state. Mirrors the
    dead-branch reload pattern."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    handler_anchor = js.index('body.querySelector(\'button[data-act="probe-tdb"]\')')
    body = js[handler_anchor:handler_anchor + 4000]
    # The alive-with-clear branch fires both reloads.
    branch_anchor = body.index("if (res.failure_cleared) {")
    branch_block = body[branch_anchor:branch_anchor + 800]
    assert "loadLibrary().catch" in branch_block
    assert "refreshTopbarStatus" in branch_block


def test_js_probe_handler_silent_on_alive_no_clear():
    """When res.failure_cleared is false (or omitted), the JS
    must NOT trigger the library reload — keeps the alive-on-
    clean-row case silent (no UI churn for a no-op probe).

    Pin by checking the reload calls live INSIDE the
    if-failure_cleared block."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    handler_anchor = js.index('body.querySelector(\'button[data-act="probe-tdb"]\')')
    body = js[handler_anchor:handler_anchor + 4000]
    # Bound the alive branch to the "} else if (res.indeterminate)" line
    # that immediately follows it.
    alive_anchor = body.index("if (res.ok) {")
    indeterminate_anchor = body.index("} else if (res.indeterminate)")
    alive_block = body[alive_anchor:indeterminate_anchor]
    # loadLibrary must appear ONLY inside the failure_cleared
    # nested if — not at the top of the alive branch.
    assert alive_block.count("loadLibrary") == 1
    # And it must sit after the failure_cleared gate.
    assert (alive_block.index("if (res.failure_cleared)")
            < alive_block.index("loadLibrary"))
