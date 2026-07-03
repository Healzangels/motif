"""v1.14.61 — dead-code sweep + 1 v1.14.56 over-deletion fix.

Audit follow-up to v1.14.60. Two parallel agents reviewed the
v1.14.47-59 surface; the dead-code agent found three downstream
orphans + one regression I introduced in v1.14.56.

## Fixed: v1.14.56 over-deletion

`.coverage-bar-seg-no-tdb` (CSS) was deleted on the assumption
only the green themed segment was rendered. The libraries-page
per-section coverage bar at app.js:2899 still emits the class
for the unthemed remainder — without the rule, the unthemed
wedge rendered with no background color → no visual
differentiation. Restored with the original styling
(`var(--fg-mute)` + 50% opacity).

The other v1.14.56 deletions (`.coverage-bar-seg-available`,
`.coverage-row-legend`, `.coverage-legend-swatch-*`) ARE
genuinely dead — verified zero JS / template references via
the existing v1.14.56 sweep test.

## Deleted: pending-page surface (1 template + ~170 JS + 4 endpoints + 1 test file)

The /pending route was removed in v1.12.41. Layered cleanup:
  - v1.14.56 deleted `templates/pending.html`
  - v1.14.61 deletes:
    - JS: `loadPending`, `pendingState`, `pendingKey`,
      `pendingItemsForKeys`, `pendingApprove`, `pendingDiscard`,
      `bindPending`, `updatePendingBulkBar` (~170 lines in app.js)
    - JS DOMContentLoaded: `bindPending()` call, `loadPending()`
      kick, `/pending` poll-interval entry
    - JS highlightNav: `/pending` route map entry
    - api.py: `api_pending`, `api_pending_count`,
      `api_pending_place`, `api_pending_discard` (~280 lines)
    - tests/test_v1_14_31_pending_count_mismatch.py: pinned the
      SQL of api_pending_count which is now gone

## Deleted: dead `is_lps` + `motif_has_placement` derivation in api_recovery_options

v1.14.42/v1.14.44 added the `placements` SELECT + both
derivations to power the v1.14.40 LPS-aware no-fail TRY THIS
NEXT branch. v1.14.47 emptied that branch but left the
producer in place → SQL roundtrip per /recovery-options call
producing 2 unused booleans. v1.14.55 M5 dropped finding
verified the no-consumption side (`is_lps` and
`motif_has_placement` had zero downstream uses inside
api_recovery_options) without deleting the dead producer;
v1.14.61 finishes the cleanup.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── v0.51.31: .coverage-bar-seg-no-tdb co-removed with its feature ──
# The v1.14.56 regression was "CSS rule deleted but JS still emits the
# class → blank unthemed wedge." v0.51.31 removed the whole
# // COVERAGE COMPARISON block (renderCoverageComparison), so BOTH the
# CSS rule and its sole JS emitter are gone together — the
# CSS-vs-JS-consistency invariant this section guards holds trivially
# now, and can never recur (no emitter left to strand).


def test_coverage_bar_seg_no_tdb_css_rule_removed():
    """v0.51.31: the .coverage-bar-seg rules were removed with the
    // COVERAGE COMPARISON block they styled. Scan outside comments."""
    import re
    css_raw = (REPO / "app" / "web" / "static" / "app.css").read_text()
    css = re.sub(r"/\*.*?\*/", "", css_raw, flags=re.DOTALL)
    rule_pattern = re.compile(
        r"\.coverage-bar-seg-no-tdb\s*\{([^}]+)\}", flags=re.DOTALL
    )
    assert not rule_pattern.search(css), (
        "v0.51.31: `.coverage-bar-seg-no-tdb` rule must be gone — its only "
        "emitter (renderCoverageComparison) was removed")


def test_coverage_bar_seg_no_tdb_js_emit_removed():
    """v0.51.31: the sole JS emitter (renderCoverageComparison) is gone, so
    the class must no longer appear in app.js — no stranded emitter."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert 'class="coverage-bar-seg coverage-bar-seg-no-tdb"' not in js


# ── Deleted: pending-page surface (JS) ──────────────────────


def test_pending_page_js_surface_deleted():
    """The pending-page JS functions must all be gone. Strip
    line comments first so the v1.14.61 marker mentioning the
    deleted symbols doesn't trip the check."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    js_no_comments = "\n".join(
        line for line in js.splitlines()
        if not line.lstrip().startswith("//")
    )
    for symbol in (
        "function loadPending(",
        "function bindPending(",
        "function pendingApprove(",
        "function pendingDiscard(",
        "function pendingItemsForKeys(",
        "function updatePendingBulkBar(",
        "function pendingKey(",
        "const pendingState =",
    ):
        assert symbol not in js_no_comments, (
            f"pending-page JS symbol {symbol!r} survived the "
            "v1.14.61 deletion. Either re-delete it or add a "
            "v1.14.61.X marker explaining why it came back."
        )


def test_pending_page_dom_event_wires_deleted():
    """The DOMContentLoaded calls to bindPending() / loadPending()
    + the /pending poll-interval line + the highlightNav map entry
    must all be gone."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    js_no_comments = "\n".join(
        line for line in js.splitlines()
        if not line.lstrip().startswith("//")
    )
    assert "bindPending()" not in js_no_comments
    assert "loadPending()" not in js_no_comments
    # /pending no longer in the highlightNav route map.
    assert "'/pending':" not in js_no_comments


# ── Deleted: pending-page surface (server endpoints) ────────


def test_pending_endpoints_deleted_from_api():
    """The 4 server endpoints (/api/pending, /api/pending/count,
    /api/pending/place, /api/pending/discard) must all be gone."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    src_no_comments = "\n".join(
        line for line in src.splitlines()
        if not line.lstrip().startswith("#")
    )
    assert "async def api_pending(" not in src_no_comments
    assert "async def api_pending_count(" not in src_no_comments
    assert "async def api_pending_place(" not in src_no_comments
    assert "async def api_pending_discard(" not in src_no_comments
    # Also verify the route decorators are gone.
    for route in ('"/api/pending"', '"/api/pending/count"',
                  '"/api/pending/place"', '"/api/pending/discard"'):
        assert route not in src_no_comments, (
            f"pending route {route} survived"
        )


def test_pending_count_test_file_deleted():
    """The pinning test file for the now-deleted api_pending_count
    SQL shape must be gone."""
    assert not (REPO / "tests" / "test_v1_14_31_pending_count_mismatch.py").exists()


# ── Deleted: is_lps + motif_has_placement dead provision ────


def test_is_lps_derivation_deleted_from_api_recovery_options():
    """The v1.14.42/v1.14.44 `is_lps` + `motif_has_placement`
    derivation in api_recovery_options is gone — both variables
    had zero downstream consumers post-v1.14.47 (verified by
    v1.14.55 M5 dropped finding) and the SQL roundtrip was
    pure dead provision."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("async def api_recovery_options(")
    body = src[fn_anchor:fn_anchor + 25000]
    src_no_comments = "\n".join(
        line for line in body.splitlines()
        if not line.lstrip().startswith("#")
    )
    # The placement SELECT specifically scoped to is_lps detection.
    assert "section_placement = conn.execute(" not in src_no_comments
    # The two assignments.
    assert "motif_has_placement = section_placement is not None" not in src_no_comments
    assert "is_lps = bool(local and p_available" not in src_no_comments
    # v1.14.61 marker explains the deletion rationale.
    assert "v1.14.61: deleted the v1.14.42/v1.14.44 `is_lps`" in body


def test_api_recovery_options_still_uses_local_for_locally_resolved():
    """Sanity: the v1.13.33/35 locally-resolved derivation that
    DOES consume `local` must survive — only the LPS-specific
    placement query was dead, not the local_files lookup it
    sat alongside."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("async def api_recovery_options(")
    body = src[fn_anchor:fn_anchor + 25000]
    # local_files lookup + locally_resolved derivation still present.
    assert "local_source = local[\"source_kind\"] if local else None" in body
    assert "locally_resolved = bool(" in body
