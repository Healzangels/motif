"""v1.14.15 — bulk actions act on visible page when no row is selected.

the user: "for keep current, ack failure and all other bulk actions
when nothing has been selected it becomes a bulk action on
everything".

Status quo before v1.14.15:
- ACCEPT ALL UPDATES + KEEP ALL CURRENT — already act with no
  selection (server-side global accept-all / decline-all).
- REVERT MISMATCH + RESTORE FROM PLEX — already iterate the
  visible page when nothing selected (v1.13.68).
- ACK FAILURES + PUSH TO PLEX — required selection. Bug.

v1.14.15 brings ACK FAILURES + PUSH TO PLEX onto the
no-selection-targets-visible-page pattern. Visibility was
already there (v1.13.68 exposed both buttons on the matching
ATTN chip with no selection); the click handlers catch up.

v1.16.10 reshaped the handler bodies: the "with selection"
iteration now walks `libraryState.selectedRows.values()` (the
selection-wide row-data cache) instead of filtering
`libraryState.items` by selection membership. The cache lets a
SELECT ALL FILTERED of N rows actually act on all N, not just the
50 on the visible page. Off-page-selected warnings became
unnecessary and were removed. The v1.14.15 contract — selection
mode acts on selected, no-selection mode acts on visible page —
is preserved, the wiring just changed shape.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── ACK FAILURES handler ──────────────────────────────────────


def test_ack_failures_handler_supports_no_selection():
    """The ACK SELECTED click handler must check
    `useSelection = libraryState.selected.size > 0`. v1.16.10:
    selection mode walks the selectedRows cache; no-selection
    mode walks the visible page (libraryState.items)."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    handler_anchor = js.index("getElementById('library-ack-selected-btn')?.addEventListener")
    body = js[handler_anchor:handler_anchor + 3000]
    assert "const useSelection = libraryState.selected.size > 0;" in body
    # v1.16.10: dual-mode source selector.
    assert "const source = useSelection" in body
    assert "libraryState.selectedRows.values()" in body
    assert "(libraryState.items || [])" in body


def test_ack_failures_alert_message_distinguishes_modes():
    """The "no candidates" alert must say "in selection" when
    useSelection, "on this page" otherwise — so the user reads
    accurate context for what was searched."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    handler_anchor = js.index("getElementById('library-ack-selected-btn')?.addEventListener")
    body = js[handler_anchor:handler_anchor + 3000]
    assert "const where = useSelection ? 'in selection' : 'on this page';" in body


def test_ack_failures_confirm_uses_scope_word():
    """The confirm prompt should use 'selected' or 'visible' to
    describe what's being acked."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    handler_anchor = js.index("getElementById('library-ack-selected-btn')?.addEventListener")
    body = js[handler_anchor:handler_anchor + 3000]
    assert "const scope = useSelection ? 'selected' : 'visible';" in body
    assert "Acknowledge ${candidates.length} ${scope} failure" in body


def test_ack_failures_no_offpage_warning_in_v1_16_10():
    """v1.16.10: with the selectedRows cache, the ACK handler now
    acts on every selected row regardless of page, so the pre-
    v1.16.10 "X selected rows aren't on the visible page" warning
    is gone. Pin its absence — its presence would mean the
    selection-wide cache path got reverted."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    handler_anchor = js.index("getElementById('library-ack-selected-btn')?.addEventListener")
    body = js[handler_anchor:handler_anchor + 3000]
    assert "aren't on the visible page" not in body
    assert "offPageSelected" not in body


# ── PUSH TO PLEX handler ──────────────────────────────────────


def test_push_to_plex_handler_supports_no_selection():
    """The PUSH SELECTED click handler must check useSelection
    and route between the selection-wide cache (v1.16.10) and the
    visible page accordingly. v1.13.68 added the no-selection
    visibility path (label becomes "PUSH ALL TO PLEX"); the
    handler matches via the dual-mode source selector."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    handler_anchor = js.index("getElementById('library-push-selected-btn')?.addEventListener")
    body = js[handler_anchor:handler_anchor + 4000]
    assert "const useSelection = libraryState.selected.size > 0;" in body
    # v1.16.10: dual-mode source selector.
    assert "const source = useSelection" in body
    assert "libraryState.selectedRows.values()" in body
    assert "(libraryState.items || [])" in body


def test_push_to_plex_skipped_only_counts_selection_misses():
    """In no-selection mode, the visible page contains rows that
    aren't awaiting placement (already placed, broken, etc.).
    Counting them as "skipped" would inflate the skip notice
    misleadingly. Pin that the `else if (useSelection)` gate
    only counts skips when a selection exists."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    handler_anchor = js.index("getElementById('library-push-selected-btn')?.addEventListener")
    body = js[handler_anchor:handler_anchor + 4000]
    assert "} else if (useSelection) {" in body


def test_push_to_plex_no_offpage_warning_in_v1_16_10():
    """v1.16.10: PUSH now operates on every selected row via the
    selectedRows cache (not just the visible page), so the pre-
    v1.16.10 "X rows not on this page" warning is gone. Anchor on
    the user-visible warning ("only operates on the current page")
    rather than a generic substring — comments referencing the
    fix still mention pagination, that's fine."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    handler_anchor = js.index("getElementById('library-push-selected-btn')?.addEventListener")
    body = js[handler_anchor:handler_anchor + 4000]
    assert "bulk PUSH only operates on the current page" not in body
    assert "offPageCount" not in body


def test_push_to_plex_alert_distinguishes_modes():
    """Empty-candidates alert must read different copy in
    selection vs no-selection mode."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    handler_anchor = js.index("getElementById('library-push-selected-btn')?.addEventListener")
    body = js[handler_anchor:handler_anchor + 4000]
    assert "every selected row is already placed" in body
    assert "no awaiting-placement rows on this page" in body


# ── REVERT/RESTORE/ACCEPT-ALL/KEEP-ALL still work ─────────────


def test_revert_mismatch_handler_still_supports_no_selection():
    """v1.13.68 added the no-selection path; v1.16.10 reshaped it
    onto the selectedRows-cache pattern. Pin both."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    handler_anchor = js.index("getElementById('library-revert-mismatch-btn')?.addEventListener")
    body = js[handler_anchor:handler_anchor + 2500]
    assert "const useSelection = libraryState.selected.size > 0;" in body
    assert "libraryState.selectedRows.values()" in body


def test_restore_from_plex_handler_still_supports_no_selection():
    """Same dual-mode shape as REVERT MISMATCH."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    handler_anchor = js.index("getElementById('library-restore-from-plex-btn')?.addEventListener")
    body = js[handler_anchor:handler_anchor + 2500]
    assert "const useSelection = libraryState.selected.size > 0;" in body
    assert "libraryState.selectedRows.values()" in body


def test_accept_all_updates_still_has_global_path():
    """ACCEPT ALL UPDATES uses a server-side global endpoint
    (/api/updates/accept-all) when no selection. v1.14.15
    doesn't touch this; pin the global path."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    handler_anchor = js.index("getElementById('library-accept-all-updates-btn')?.addEventListener")
    # v1.21.81: widened from 5000 — the selected-rows path gained the
    # per-edition rating_key param builder, pushing the no-selection
    # global-path branch a few lines further down the handler body.
    body = js[handler_anchor:handler_anchor + 9000]
    assert "'/api/updates/accept-all'" in body
    # The "No selection — global accept-all path" branch comment
    # marks where the no-selection mode kicks in.
    assert "No selection" in body


# ── Visibility / no-selection-button surfacing unchanged ──────


def test_ack_failures_button_still_visible_on_attn_fail_with_no_selection():
    """Pin the v1.13.68 visibility gate: ACK FAILURES button
    shows when on attn_pills=fail filter regardless of
    selection. Without this, the new no-selection handler
    would never fire.

    v1.15.51: the one-liner `if (ackBtn) ackBtn.style.display = ...`
    became a multi-line block (now wraps visibility + label-with-
    count). Visibility statement form unchanged — anchor on
    `ackBtn.style.display = onAttnFail ? '' : 'none'` directly."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert "ackBtn.style.display = onAttnFail ? '' : 'none';" in js
    assert "const onAttnFail = libraryState.attnPills.has('fail');" in js


def test_push_to_plex_button_still_visible_on_attn_await_with_no_selection():
    """Pin the v1.13.68 visibility gate for PUSH TO PLEX on
    attn_pills=await."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert "const onAttnAwait = libraryState.attnPills.has('await');" in js
    assert "if (pushBtn && onAttnAwait && pushableCount === 0)" in js
    # And the label flip.
    # v0.51.252: label writes route through setBulkLabel (ping-pong guard).
    assert "setBulkLabel(pushBtn, '// PUSH ALL TO PLEX');" in js
