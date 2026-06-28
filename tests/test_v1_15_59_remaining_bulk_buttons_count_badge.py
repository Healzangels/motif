"""v1.15.59 — count badges on ACCEPT ALL UPDATES, KEEP ALL
CURRENT, EXPORT CSV.

the user: "Let's tackle all of these especially Bulk-action
consistency, remaining buttons, want them to all by consistent."

v1.15.49 + v1.15.51 standardized count badges on PUSH / REVERT
/ RESTORE / ACK / DOWNLOAD + ADOPT / ADOPT + LET PLEX SERVE.
The three remaining bulk buttons (ACCEPT ALL UPDATES, KEEP ALL
CURRENT, EXPORT CSV) didn't get the same treatment. v1.15.59
finishes the sweep.

## Standardized convention (across all bulk buttons)

  idle (N=1):    "// LABEL"
  idle (N>1):    "// LABEL (N)"

* ACCEPT ALL UPDATES + KEEP ALL CURRENT use `updateCount` (rows
  with pending_update where SRC !== '-' — the v1.12.95 canonical
  predicate). Both share the same eligible row set.
* EXPORT CSV uses raw `n` (selection size) since the handler is
  selection-only (no attn-pill no-selection fallback).

Static-text guards consistent with v1.15.49 + v1.15.51 patterns.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"


def test_update_count_helper_defined():
    """`updateCount` helper must be declared in updateLibrarySelectionUi
    using the existing effectiveCount + predicate (pending_update
    + src !== '-'). Drives the (N) badge on the two update-axis
    buttons."""
    js = APP_JS.read_text()
    fn_anchor = js.index("function updateLibrarySelectionUi()")
    # v1.18.24: widen window — comment additions in
    # updateLibrarySelectionUi pushed `// ADOPT + LET PLEX SERVE`
    # past the prior 30000 boundary.
    fn_body = js[fn_anchor:fn_anchor + 36000]
    assert "const updateCount = effectiveCount(" in fn_body, (
        "v1.15.59: updateCount must be declared via effectiveCount "
        "for ACCEPT ALL / KEEP ALL count-badge support"
    )
    # Predicate must match the canonical v1.12.95 rule.
    update_anchor = fn_body.index("const updateCount = effectiveCount(")
    block = fn_body[update_anchor:update_anchor + 400]
    # v1.20.0: the pending-update predicate was consolidated into the
    # pendingUpdateActionable() helper (ends the v1.18/19 SRC-axis
    # mirror-drift class). updateCount now calls it.
    assert "pendingUpdateActionable(it)" in block, (
        "v1.15.59/v1.20.0: updateCount must gate on the consolidated "
        "pendingUpdateActionable() helper"
    )


def test_accept_all_button_uses_with_count():
    """v1.20.1: ACCEPT button is now selection-aware (the user) — it
    DROPPED withCount. Nothing selected → '// ACCEPT ALL UPDATES'
    (no count, presumed all); a selection → singular/plural + the
    selection count ('// ACCEPT UPDATE (1)' / '// ACCEPT UPDATES (N)'),
    matching the PROBE/EXPORT selection buttons."""
    js = APP_JS.read_text()
    fn_anchor = js.index("function updateLibrarySelectionUi()")
    fn_body = js[fn_anchor:fn_anchor + 36000]
    assert "'// ACCEPT ALL UPDATES'" in fn_body, (
        "v1.20.1: no-selection label is the bare '// ACCEPT ALL UPDATES'"
    )
    assert "// ACCEPT UPDATE${updateCount === 1 ? '' : 'S'} (${updateCount})" in fn_body, (
        "v1.20.1: selection label = singular/plural + (N)"
    )


def test_keep_all_current_button_uses_with_count():
    """v1.20.1: KEEP button selection-aware too — no-selection
    '// KEEP ALL CURRENT'; a selection → '// KEEP CURRENT (N)'."""
    js = APP_JS.read_text()
    fn_anchor = js.index("function updateLibrarySelectionUi()")
    fn_body = js[fn_anchor:fn_anchor + 36000]
    assert "'// KEEP ALL CURRENT'" in fn_body
    assert "// KEEP CURRENT (${updateCount})" in fn_body


def test_export_csv_button_uses_with_count():
    """EXPORT CSV uses raw selection size `n` (not effectiveCount)
    because the handler is selection-only — no attn-pill no-
    selection fallback. The (N) badge tells the operator how
    many rows will be in the export."""
    js = APP_JS.read_text()
    fn_anchor = js.index("function updateLibrarySelectionUi()")
    # v1.18.24: widen window — comment additions in
    # updateLibrarySelectionUi pushed `// ADOPT + LET PLEX SERVE`
    # past the prior 30000 boundary.
    fn_body = js[fn_anchor:fn_anchor + 36000]
    assert "withCount('// EXPORT CSV', n)" in fn_body, (
        "v1.15.59: EXPORT CSV must use withCount('// EXPORT CSV', n) "
        "— scope = selection size since handler alerts when n === 0"
    )


def test_all_bulk_buttons_share_with_count_convention():
    """Cross-button consistency: ALL bulk buttons that have a
    meaningful eligible-row count use withCount(). Pin the full
    list so a future button addition that skips withCount() trips
    the test."""
    js = APP_JS.read_text()
    fn_anchor = js.index("function updateLibrarySelectionUi()")
    # v1.18.24: widen window — comment additions in
    # updateLibrarySelectionUi pushed `// ADOPT + LET PLEX SERVE`
    # past the prior 30000 boundary.
    fn_body = js[fn_anchor:fn_anchor + 36000]
    expected_withcount_labels = [
        "// PUSH TO PLEX",
        "// REVERT MISMATCH",
        "// RESTORE FROM PLEX",
        "// ACK FAILURES",
        "// DOWNLOAD FROM TDB",
        "// DOWNLOAD & REPLACE FROM TDB",
        "// ADOPT SELECTED",
        "// LET PLEX SERVE",
        "// ADOPT + LET PLEX SERVE",
        # v1.15.59 added EXPORT CSV. v1.20.1 dropped ACCEPT ALL
        # UPDATES + KEEP ALL CURRENT from the withCount convention —
        # they're now selection-aware template labels (no count when
        # nothing is selected; singular/plural + count when selected).
        # See test_accept_all_button_uses_with_count.
        "// EXPORT CSV",
    ]
    missing = [
        lbl for lbl in expected_withcount_labels
        if f"withCount('{lbl}'," not in fn_body
    ]
    assert not missing, (
        f"v1.15.59: bulk buttons missing withCount() call(s): "
        f"{missing} — the (N) badge convention must apply to "
        "every bulk button for consistency"
    )
