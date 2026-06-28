"""v1.15.61 — header checkbox stuck-indeterminate fix + topbar
FAIL count includes orphan failures.

the user:
* "can we fix the selection selector so it doesn't get stuck
  with the x when nothing is selected"
* "there is a mismatch between the N Fail and the actual
  filtered red ! we can see there are 17 selected and 17
  displayed when it says there are only 16"

## Bug A — header checkbox indeterminate stuck

The header `library-select-all` checkbox uses tri-state
(checked / indeterminate / unchecked). updateLibrarySelectionUi
sets the state based on libraryState selection + visible-page
overlap, with a defensive belt forcing unchecked when
libraryState.selected.size === 0.

The belt is correct, but Safari/Firefox can ignore
`indeterminate = false` writes if the browser's native click
handler already set `indeterminate = true` that same tick — a
race between the native event and our async state update.

Fix: in the click handler, explicitly set
`headerCb.indeterminate = false` + `headerCb.checked = turnOn`
BEFORE calling updateLibrarySelectionUi. The synchronous writes
inside the click handler (post e.preventDefault) sequence
correctly with the browser's native click event.

## Bug B — topbar FAIL count undercounts orphan failures

`_FAILURES_SFA_FROM_SQL` joins via `pi.guid_tmdb = t.tmdb_id`.
For orphan rows (synthetic-negative tmdb_id, no Plex-supplied
TMDB id), pi.guid_tmdb is NULL → the join misses them → the
topbar FAIL count undercounts by exactly the number of orphan
failures.

The library main query joins via `pi.theme_id = t.id` (the
v1.11.26 cached pointer set by resolve_theme_ids) which DOES
catch orphans → the attn_pills=fail filter showed 17 rows
when the topbar said 16.

Fix: OR the join paths in `_FAILURES_SFA_FROM_SQL`:
  `pi.guid_tmdb = t.tmdb_id (+ media_type match) OR pi.theme_id = t.id`
TDB-tracked rows match via guid_tmdb (works pre-
resolve_theme_ids), orphans match via theme_id. Each (pi, t)
pair is unique regardless of which join path matched, so no
double-counting.

Static-text guards consistent with v1.14.30 / v1.15.38 SFA-
predicate test patterns.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"
APP_JS = REPO / "app" / "web" / "static" / "app.js"


# ── Bug A: header checkbox sets state in click handler ──────


def test_header_checkbox_click_handler_sets_state_explicitly():
    """The library-select-all change handler (v1.22.95: was click
    + preventDefault, whose post-handler revert clobbered these
    writes) must explicitly set
    `headerCb.indeterminate = false` + `headerCb.checked = turnOn`
    BEFORE calling updateLibrarySelectionUi. Pre-fix the handler
    relied on updateLibrarySelectionUi alone — but Safari /
    Firefox can ignore `indeterminate = false` writes if the
    native click handler set it to true that same tick."""
    js = APP_JS.read_text()
    handler_anchor = js.index(
        "library-select-all')?.addEventListener('change'"
    )
    # v1.16.11: handler body grew (master-toggle if/else) — widen
    # the span so the two explicit writes near the bottom stay in
    # scope.
    body = js[handler_anchor:handler_anchor + 5000]
    assert "headerCb.indeterminate = false;" in body, (
        "v1.15.61: click handler must explicitly clear "
        "headerCb.indeterminate inside the click context — "
        "synchronous reset survives the Safari/Firefox race"
    )
    assert "headerCb.checked = turnOn;" in body, (
        "v1.15.61: click handler must explicitly set headerCb.checked "
        "inside the click context"
    )
    # Both writes must happen BEFORE updateLibrarySelectionUi().
    indet_idx = body.index("headerCb.indeterminate = false;")
    update_idx = body.index("updateLibrarySelectionUi();")
    assert indet_idx < update_idx, (
        "v1.15.61: explicit indeterminate=false must come BEFORE "
        "updateLibrarySelectionUi() so the synchronous write lands "
        "in the click context (post preventDefault)"
    )


def test_update_selection_ui_keeps_defensive_belt():
    """Regression guard: updateLibrarySelectionUi's defensive belt
    (force unchecked when selected.size === 0) must stay in place
    as the second layer of defense. The click-handler fix above
    is the first layer (immediate); the belt covers
    loadLibrary() re-render polls + non-click paths."""
    js = APP_JS.read_text()
    fn_anchor = js.index("function updateLibrarySelectionUi()")
    body = js[fn_anchor:fn_anchor + 8000]
    assert "if (libraryState.selected.size === 0) {" in body, (
        "v1.15.61: defensive belt must remain in updateLibrarySelectionUi"
    )
    # The belt must force both checked=false AND indeterminate=false.
    belt_anchor = body.index("if (libraryState.selected.size === 0) {")
    belt = body[belt_anchor:belt_anchor + 200]
    assert "all.checked = false;" in belt
    assert "all.indeterminate = false;" in belt


# ── Bug B: topbar FAIL JOIN catches orphans via theme_id ────


def test_failures_sfa_from_sql_includes_theme_id_join_path():
    """v1.15.62 simplified v1.15.61's OR-join → single
    `pi.theme_id = t.id` join. theme_id covers BOTH orphans AND
    TDB-tracked rows (both have theme_id populated post-
    resolve_theme_ids). v1.15.61 used an OR-join which catered
    to pre-resolve robustness, but SQLite's optimizer bailed and
    perf collapsed — v1.15.62 trades the pre-resolve edge case
    for fast queries that still match the library filter
    behavior."""
    src = API_PY.read_text()
    sql_anchor = src.index("_FAILURES_SFA_FROM_SQL = (")
    sql_end = src.index("\n)", sql_anchor) + 2
    sql_block = src[sql_anchor:sql_end]
    flat = " ".join(sql_block.split())
    assert "ON pi.theme_id = t.id" in flat, (
        "v1.15.62: single theme_id join required for fast queries"
    )
    assert "pi.guid_tmdb = t.tmdb_id" not in flat, (
        "v1.15.62: guid_tmdb join clause must be gone — theme_id "
        "alone covers both TDB rows AND orphans (v1.15.61's OR-join "
        "was reverted for perf)"
    )


def test_failures_sfa_where_sql_unchanged():
    """Regression guard: the WHERE predicate stays the same — the
    fix is purely in the JOIN. The predicate is still
    `failure_kind NOT NULL AND not acked AND sfa not acked`."""
    src = API_PY.read_text()
    sql_anchor = src.index("_FAILURES_SFA_WHERE_SQL = (")
    sql_end = src.index("\n)", sql_anchor) + 2
    sql_block = src[sql_anchor:sql_end]
    flat = " ".join(sql_block.split())
    assert "t.failure_kind IS NOT NULL" in flat
    assert "AND t.failure_acked_at IS NULL" in flat
    assert "AND sfa.acked_at IS NULL" in flat, (
        "v1.15.61: WHERE predicate must remain ack-aware — the "
        "fix is in the JOIN, not the WHERE"
    )
