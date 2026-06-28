"""v1.16.10 — selection-wide row-data cache for bulk actions.

Pre-fix: SELECT ALL FILTERED added N keys to libraryState.selected
(survives pagination) but the bulk-bar count badges + click handlers
walked libraryState.items (visible page only). On a 484-row selection
with page size 50 the operator saw `(50)` on every bulk button and the
handlers silently dropped the 434 off-page rows.

the user:
- "doing a select all is not reflecting correctly on the bulk actions"
- "let plex server isn't displaying a number at all anytime"

Fix (v1.16.10):
- New libraryState.selectedRows Map (libKey → row data), parallel to
  the .selected key Set.
- Populated by SELECT ALL FILTERED pagination, per-row checkbox
  toggles, and a syncSelectedRowsFromItems() pass on every render.
- Bulk-bar bucket counts + the effectiveCount() helper walk
  selectedRows.values() instead of libraryState.items, so badges
  reflect the full selection regardless of page.
- Each bulk handler's loop walks `useSelection ?
  libraryState.selectedRows.values() : (libraryState.items || [])`
  so the action ACTUALLY covers every selected row (not just the
  visible page).
- The v1.13.35 / v1.15.60 off-page-selection warnings (PUSH/ACK/LPS/
  ADOPT+LPS) and the v1.15.60 onPlusPFilter LPS safety-net are gone
  — both were workarounds for the visible-page undercount the cache
  eliminates.

Tests pin the cache + the cleanup.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"


# ── 1. Cache field + sync helper exist ────────────────────────


def test_library_state_has_selected_rows_cache():
    """libraryState must declare a selectedRows Map alongside the
    selected Set."""
    js = APP_JS.read_text()
    assert "selected: new Set()," in js, "the parallel selected Set must stay"
    assert "selectedRows: new Map()," in js, (
        "v1.16.10: libraryState.selectedRows Map must hold the "
        "selection-wide row-data cache"
    )


def test_sync_helper_exists_and_refreshes_from_items():
    """syncSelectedRowsFromItems() walks libraryState.items and
    refreshes selectedRows entries for any currently-selected key —
    keeps the cache fresh for visible rows on every render."""
    js = APP_JS.read_text()
    fn_anchor = js.index("function syncSelectedRowsFromItems()")
    body = js[fn_anchor:fn_anchor + 600]
    assert "libraryState.items" in body
    assert "libraryState.selected.has(k)" in body
    assert "libraryState.selectedRows.set(k, it)" in body


def test_items_render_calls_sync_helper():
    """The library-render path (where libraryState.items is set)
    must invoke syncSelectedRowsFromItems() so the cache picks up
    the latest row data for visible selections."""
    js = APP_JS.read_text()
    set_anchor = js.index("libraryState.items = dedupedItems;")
    after = js[set_anchor:set_anchor + 500]
    assert "syncSelectedRowsFromItems();" in after, (
        "v1.16.10: items render must refresh the selectedRows cache "
        "so visible-page row data stays current"
    )


# ── 2. Selection-add paths populate selectedRows ──────────────


def test_select_all_filtered_populates_selected_rows():
    """The SELECT ALL FILTERED handler paginates /api/library and
    adds each row's key to selected. v1.16.10 also stashes the
    full row in selectedRows so off-page rows survive without a
    re-fetch on bulk-action click."""
    js = APP_JS.read_text()
    handler_anchor = js.index("library-select-all-filtered-btn')?.addEventListener")
    body = js[handler_anchor:handler_anchor + 1500]
    assert "libraryState.selected.add(k);" in body
    assert "libraryState.selectedRows.set(k, it);" in body, (
        "v1.16.10: SELECT ALL FILTERED must stash row data in "
        "selectedRows so off-page rows are actionable post-paginate"
    )


def test_per_row_checkbox_populates_selected_rows():
    """The per-row checkbox change handler must add the row's data
    to selectedRows on check, and remove it on uncheck."""
    js = APP_JS.read_text()
    handler_anchor = js.index("// Per-row select checkbox toggle")
    body = js[handler_anchor:handler_anchor + 1500]
    assert "libraryState.selectedRows.set(k, row)" in body
    assert "libraryState.selectedRows.delete(k)" in body


def test_select_all_visible_populates_selected_rows():
    """The header (select-all-visible) checkbox handler must mirror
    its rows into selectedRows on select.

    v1.16.11: deselect branch swapped for a full-clear (the header
    is now a master toggle — any selection → clear everything,
    nothing selected → select visible). The per-row delete is gone;
    selectedRows.clear() takes over."""
    js = APP_JS.read_text()
    handler_anchor = js.index("library-select-all')?.addEventListener")
    body = js[handler_anchor:handler_anchor + 3000]
    assert "libraryState.selectedRows.set(k, row)" in body
    assert "libraryState.selectedRows.clear();" in body


# ── 3. Clear paths drop the cache too ─────────────────────────


def test_every_selected_clear_is_paired_with_rows_clear():
    """Every libraryState.selected.clear() must be paired with a
    libraryState.selectedRows.clear() — otherwise the cache leaks
    stale rows across bulk-action lifecycles and the next selection
    sees a polluted starting state."""
    js = APP_JS.read_text()
    clear_count = js.count("libraryState.selected.clear();")
    rows_clear_count = js.count("libraryState.selectedRows.clear();")
    assert clear_count == rows_clear_count, (
        f"v1.16.10: {clear_count} selected.clear() vs "
        f"{rows_clear_count} selectedRows.clear() — must be paired so "
        f"the cache never outlives the Set"
    )


# ── 4. Bulk counts walk the cache ─────────────────────────────


def test_bucket_count_walker_uses_selected_rows():
    """The bulk-bar bucket count loop (pureP_count, adoptOnlyCount,
    lpsOnlyCount, etc.) must walk libraryState.selectedRows.values()
    when n > 0 so counts reflect the full selection, not just the
    visible page."""
    js = APP_JS.read_text()
    # Anchor on the bucket-init block and check the loop after it.
    anchor = js.index("let adoptOnlyCount = 0;")
    body = js[anchor:anchor + 2500]
    assert "for (const it of libraryState.selectedRows.values())" in body, (
        "v1.16.10: bucket count walker must iterate selectedRows, "
        "not (libraryState.items || []) with a selected.has() filter"
    )


def test_effective_count_walks_selected_rows_when_selection_active():
    """effectiveCount(n>0 branch) must iterate selectedRows so badges
    like `// ACCEPT ALL UPDATES (N)` show the full selection count
    (not the visible-page subset)."""
    js = APP_JS.read_text()
    fn_anchor = js.index("const effectiveCount = (predicate) =>")
    body = js[fn_anchor:fn_anchor + 800]
    assert "for (const it of libraryState.selectedRows.values())" in body
    # No-selection mode still walks visible-page items.
    assert "(libraryState.items || []).filter(predicate).length" in body


# ── 5. Bulk handlers walk the cache ───────────────────────────


def _handler_body(js: str, btn_id: str, span: int = 4000) -> str:
    anchor = js.index(f"getElementById('{btn_id}')?.addEventListener")
    return js[anchor:anchor + span]


def test_download_handler_walks_selected_rows():
    js = APP_JS.read_text()
    body = _handler_body(js, "library-download-selected-btn")
    assert "for (const it of libraryState.selectedRows.values())" in body, (
        "v1.16.10: DOWNLOAD handler must walk selectedRows so the "
        "POST /api/library/download-batch payload covers every "
        "selected row, not just the visible page"
    )


def test_tdb_backup_handler_walks_selected_rows():
    js = APP_JS.read_text()
    body = _handler_body(js, "library-tdb-backup-btn")
    assert "for (const it of libraryState.selectedRows.values())" in body


def test_push_handler_walks_selected_rows_in_selection_mode():
    """PUSH TO PLEX has a dual-mode source (selection vs visible
    page fallback). With selection, it must walk selectedRows."""
    js = APP_JS.read_text()
    body = _handler_body(js, "library-push-selected-btn", span=5000)
    assert "const source = useSelection" in body
    assert "libraryState.selectedRows.values()" in body
    assert "(libraryState.items || [])" in body


def test_revert_handler_walks_selected_rows_in_selection_mode():
    js = APP_JS.read_text()
    body = _handler_body(js, "library-revert-mismatch-btn")
    assert "const source = useSelection" in body
    assert "libraryState.selectedRows.values()" in body


def test_restore_handler_walks_selected_rows_in_selection_mode():
    js = APP_JS.read_text()
    body = _handler_body(js, "library-restore-from-plex-btn")
    assert "const source = useSelection" in body
    assert "libraryState.selectedRows.values()" in body


def test_lps_handler_walks_selected_rows_in_selection_mode():
    js = APP_JS.read_text()
    body = _handler_body(js, "library-let-plex-serve-btn", span=5000)
    assert "const source = useSelection" in body
    assert "libraryState.selectedRows.values()" in body


def test_probe_tdb_handler_walks_selected_rows_in_selection_mode():
    js = APP_JS.read_text()
    body = _handler_body(js, "library-bulk-probe-tdb-btn")
    assert "const source = useSelection" in body
    assert "libraryState.selectedRows.values()" in body


def test_adopt_and_lps_handler_walks_selected_rows():
    """ADOPT + LET PLEX SERVE is selection-only (no visible-page
    fallback), so the iteration is direct over the cache."""
    js = APP_JS.read_text()
    body = _handler_body(js, "library-adopt-and-lps-btn", span=3000)
    assert "Array.from(libraryState.selectedRows.values())" in body, (
        "v1.16.10: ADOPT + LPS handler must source candidates from "
        "the selectedRows cache so the action covers every selected "
        "M+P composite (not just the ones on the visible page)"
    )


def test_ack_handler_walks_selected_rows_in_selection_mode():
    js = APP_JS.read_text()
    body = _handler_body(js, "library-ack-selected-btn")
    assert "const source = useSelection" in body
    assert "libraryState.selectedRows.values()" in body


# ── 6. Workaround warnings are gone ───────────────────────────


def test_no_offpage_warning_on_push_handler():
    """The v1.13.35 off-page warning workaround for PUSH (and
    others) is moot now that the action covers off-page rows.
    Pin the absence of the user-visible warning text."""
    js = APP_JS.read_text()
    body = _handler_body(js, "library-push-selected-btn", span=5000)
    assert "bulk PUSH only operates on the current page" not in body
    assert "offPageCount" not in body


def test_no_offpage_warning_on_ack_handler():
    js = APP_JS.read_text()
    body = _handler_body(js, "library-ack-selected-btn")
    assert "aren't on the visible page" not in body
    assert "offPageSelected" not in body


def test_no_offpage_warning_on_adopt_lps_handler():
    js = APP_JS.read_text()
    body = _handler_body(js, "library-adopt-and-lps-btn", span=3000)
    assert "aren't on the visible page" not in body
    assert "offPageSelected" not in body


# ── 7. LPS bare-label-no-count branch is gone ─────────────────


def test_lps_button_always_has_count_when_visible():
    """the user's original v1.16.10 complaint: "let plex server isn't
    displaying a number at all anytime." The v1.15.60 safety-net
    branch surfaced LPS without a count when lpsOnlyCount=0 but
    +P filter was active. v1.16.10 drops that branch — every
    visible LPS button now carries a count badge."""
    js = APP_JS.read_text()
    marker = "v1.14.27 / v1.15.49: bulk LET PLEX SERVE"
    anchor = js.index(marker)
    block_end = js.index("// v1.15.60: bulk PROBE TDB SELECTED", anchor)
    block = js[anchor:block_end]
    # The bare-label branch is gone — i.e. no textContent =
    # '// LET PLEX SERVE' assignment outside the withCount() call.
    # withCount() is allowed (that's the count-bearing branch).
    assert "letPlexServeBtn.textContent = '// LET PLEX SERVE';" not in block, (
        "v1.16.10: the bare-label-no-count LPS branch must be removed "
        "— every visible LPS button carries a count badge"
    )
    # And the count-bearing branch remains:
    assert "withCount('// LET PLEX SERVE', lpsOnlyCount)" in block


# ── 8. Selection-wide pending-update count uses the cache ─────


def test_selected_eligible_updates_walks_selected_rows():
    """selectedEligibleUpdates drives showBarForUpdates — must walk
    the cache so SELECT ALL FILTERED → ACCEPT-ALL/KEEP-ALL surfaces
    even when the eligible rows are off the visible page."""
    js = APP_JS.read_text()
    decl_anchor = js.index("let selectedEligibleUpdates = 0;")
    body = js[decl_anchor:decl_anchor + 500]
    assert "for (const it of libraryState.selectedRows.values())" in body
    # v1.20.0: predicate consolidated into pendingUpdateActionable()
    # (this was the v1.19.98-missed site the rollover audit caught —
    # it had used the bare SRC≠'-' form without the new_theme exception).
    assert "pendingUpdateActionable(it)" in body
