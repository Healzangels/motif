"""v1.16.11 — header checkbox becomes a master toggle.

Pre-fix: clicking the header (#library-select-all) toggled the
visible-page selection only. With SELECT ALL FILTERED of 484 rows
(only 50 visible), one header click deselected the visible 50
leaving 434 stuck in the bulk bar — the bar stayed open, the
header appeared "broken" (eventually unchecks but bulk actions
remain). the user: "when trying to clear the select all by clicking
the check box next to title it gets into a weird stuck state."

Fix: header click is a master toggle across the full selection.
- libraryState.selected.size === 0 → select all visible rows
- libraryState.selected.size  >  0 → CLEAR EVERYTHING (mirrors
  the // CLEAR button semantics; the bulk bar collapses on a
  single click regardless of how many off-page rows were in the
  selection).

The visible-page tri-state RENDER (checked / indeterminate /
unchecked based on visibleSelected vs visibleCount) is unchanged
— it still tells the user at a glance which visible rows are
selected. Only the CLICK action grew.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"


def _handler_body() -> str:
    js = APP_JS.read_text()
    anchor = js.index("library-select-all')?.addEventListener('change'")
    return js[anchor:anchor + 5000]


def test_header_click_gate_uses_full_selection_size():
    """The gate that decides select-vs-clear must check
    libraryState.selected.size, not the visible-page subset."""
    body = _handler_body()
    assert "const anySelected = libraryState.selected.size > 0;" in body, (
        "v1.16.11: header click must gate on FULL selection size — "
        "visible-only gates (visibleKeys.some / .every) left off-page "
        "rows orphaned after a click"
    )
    assert "const turnOn = !anySelected;" in body


def test_header_click_deselect_branch_is_full_clear():
    """The deselect branch must mirror the // CLEAR button
    semantics — full clear of both selected + selectedRows, plus
    unchecking every visible row checkbox."""
    body = _handler_body()
    # The else (deselect) branch contains the full-clear calls.
    else_idx = body.index("} else {")
    else_block = body[else_idx:else_idx + 800]
    assert "libraryState.selected.clear();" in else_block
    assert "libraryState.selectedRows.clear();" in else_block
    assert "cb.checked = false" in else_block, (
        "v1.16.11: deselect branch must walk visible checkboxes "
        "and set cb.checked=false so the DOM matches the cleared "
        "state instantly (no flicker waiting on the render pass)"
    )


def test_header_click_select_branch_only_runs_when_nothing_selected():
    """The if (turnOn) branch (selects visible rows + populates
    selectedRows) must only fire when nothing is currently
    selected. Pre-v1.16.11 it also fired in the indeterminate /
    partial-visible case — which was the pre-v1.14.26 footgun."""
    body = _handler_body()
    # The if branch should come BEFORE the else.
    if_idx = body.index("if (turnOn) {")
    else_idx = body.index("} else {")
    assert if_idx < else_idx
    if_block = body[if_idx:else_idx]
    # Sanity: the select-add calls live inside the if branch.
    assert "libraryState.selected.add(k);" in if_block
    assert "libraryState.selectedRows.set(k, row)" in if_block


def test_header_click_writes_state_explicitly_before_render():
    """v1.15.61 contract preserved: explicit headerCb.indeterminate
    = false + headerCb.checked = turnOn writes happen before the
    updateLibrarySelectionUi() call, so the Safari/Firefox
    indeterminate-clear race is still defended."""
    body = _handler_body()
    indet_idx = body.index("headerCb.indeterminate = false;")
    checked_idx = body.index("headerCb.checked = turnOn;")
    update_idx = body.index("updateLibrarySelectionUi();")
    assert indet_idx < update_idx
    assert checked_idx < update_idx


def test_render_pass_still_uses_visible_tri_state():
    """The header RENDER (in updateLibrarySelectionUi) is
    unchanged — it still drives checked/indeterminate from
    visibleSelected vs visibleCount so the user gets at-a-glance
    feedback for the visible page. Only the CLICK action grew
    to act on the full selection."""
    js = APP_JS.read_text()
    fn_anchor = js.index("function updateLibrarySelectionUi()")
    fn_body = js[fn_anchor:fn_anchor + 8000]
    # The tri-state render anchors:
    assert "const visibleSelected = Array.from(rowBoxes).filter(" in fn_body
    assert "if (visibleSelected === visibleCount)" in fn_body
    # And the v1.14.26 defensive belt (force unchecked on empty
    # selection) still in place.
    assert "if (libraryState.selected.size === 0)" in fn_body
