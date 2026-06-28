"""v1.14.16 — REVERT MISMATCH moves between KEEP ALL CURRENT and PUSH TO PLEX.

the user: "can we move the revert mismatch button to between
keep all current and push all to plex when multiple filters are
selected like in the screenshot".

Pre-fix the bulk-action button row ordered as:

    SELECT ALL FILTERED · CLEAR · DOWNLOAD · ADOPT
      · REVERT MISMATCH                               ← here
      · ACCEPT ALL UPDATES · KEEP ALL CURRENT
      · PUSH TO PLEX · RESTORE FROM PLEX
      · ACK FAILURES · EXPORT CSV

When multiple ATTN chips were active simultaneously (mismatch +
update + await), REVERT MISMATCH appeared BEFORE the
update-pair which broke the natural priority scan order. Moving
it after KEEP ALL CURRENT puts the bar in:

    SELECT ALL FILTERED · CLEAR · DOWNLOAD · ADOPT
      · ACCEPT ALL UPDATES · KEEP ALL CURRENT
      · REVERT MISMATCH                               ← here now
      · PUSH TO PLEX · RESTORE FROM PLEX
      · ACK FAILURES · EXPORT CSV

Reading order now flows: UPDATE pair → MISMATCH → PLEX-touching
→ ACK → EXPORT — matching the row-glyph priority hierarchy
(failure > update > mismatch > await > broken).

Tests pin the new DOM order via index-of comparisons.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── DOM order pinned ──────────────────────────────────────────


def _bulk_bar_block():
    """Return the bulk-action bar block of library.html."""
    html = (REPO / "app" / "web" / "templates" / "library.html").read_text()
    bar_start = html.index('id="library-bulk-bar"')
    # The row of buttons sits inside the bulk bar; cap at the closing
    # </div> after EXPORT CSV.
    bar_end = html.index('id="library-export-csv-btn"', bar_start)
    bar_end = html.index("</button>", bar_end) + len("</button>")
    return html[bar_start:bar_end]


def test_revert_mismatch_after_keep_all_current():
    """REVERT MISMATCH must sit AFTER the KEEP ALL CURRENT
    button in DOM order — that's the v1.14.16 reorder."""
    bar = _bulk_bar_block()
    keep_pos = bar.index('id="library-decline-all-updates-btn"')
    revert_pos = bar.index('id="library-revert-mismatch-btn"')
    assert keep_pos < revert_pos, (
        "v1.14.16: REVERT MISMATCH must follow KEEP ALL CURRENT "
        "in DOM order"
    )


def test_revert_mismatch_before_push_to_plex():
    """REVERT MISMATCH must sit BEFORE PUSH TO PLEX — closes
    the "between" sandwich the user described."""
    bar = _bulk_bar_block()
    revert_pos = bar.index('id="library-revert-mismatch-btn"')
    push_pos = bar.index('id="library-push-selected-btn"')
    assert revert_pos < push_pos, (
        "v1.14.16: REVERT MISMATCH must precede PUSH TO PLEX in DOM order"
    )


def test_full_button_order_matches_priority_hierarchy():
    """End-to-end DOM order — pin the entire sequence so any
    future button-position refactor reads against this baseline."""
    bar = _bulk_bar_block()
    expected_order = [
        'id="library-select-all-filtered-btn"',
        'id="library-clear-selection-btn"',
        'id="library-download-selected-btn"',
        'id="library-adopt-selected-btn"',
        'id="library-accept-all-updates-btn"',
        'id="library-decline-all-updates-btn"',
        'id="library-revert-mismatch-btn"',          # ← v1.14.16 moved here
        'id="library-push-selected-btn"',
        'id="library-restore-from-plex-btn"',
        'id="library-ack-selected-btn"',
        'id="library-export-csv-btn"',
    ]
    positions = [bar.index(marker) for marker in expected_order]
    # Strictly increasing → DOM order matches the expected sequence.
    for i in range(1, len(positions)):
        assert positions[i - 1] < positions[i], (
            f"v1.14.16: button {expected_order[i]!r} must follow "
            f"{expected_order[i - 1]!r} in DOM order"
        )


def test_revert_mismatch_is_no_longer_pre_update_pair():
    """Regression guard: the pre-fix position (REVERT MISMATCH
    before ACCEPT ALL UPDATES) must not return. A revert that
    moves the button back would violate the priority-order
    reading flow."""
    bar = _bulk_bar_block()
    accept_pos = bar.index('id="library-accept-all-updates-btn"')
    revert_pos = bar.index('id="library-revert-mismatch-btn"')
    assert accept_pos < revert_pos, (
        "v1.14.16: REVERT MISMATCH must NOT precede ACCEPT ALL "
        "UPDATES in DOM order — regression of the pre-fix layout"
    )


# ── EXPORT CSV is still re-anchored to the end (unaffected) ───


def test_export_csv_still_re_anchored_to_end_via_js():
    """v1.12.101 forces EXPORT CSV to the end via appendChild
    so it stays rightmost regardless of which other buttons
    render. v1.14.16 doesn't touch this; pin so it stays."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert "exportBtn.parentNode.appendChild(exportBtn)" in js


# ── No JS re-orders REVERT MISMATCH (pure DOM order wins) ─────


def test_no_js_re_anchors_revert_mismatch():
    """The v1.14.16 reorder relies purely on DOM source order —
    no appendChild trick like EXPORT CSV uses. Pin that no JS
    code path moves the REVERT button after the bar renders,
    so the v1.14.16 ordering can't drift via a runtime
    re-anchor."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # Search for any appendChild on the revert button id.
    forbidden = "revertBtn.parentNode.appendChild(revertBtn)"
    assert forbidden not in js
