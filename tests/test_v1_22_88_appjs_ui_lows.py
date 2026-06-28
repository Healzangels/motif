"""v1.22.88 (audit round 2, Batch D #5) — app.js/UI LOWs.

(1) ACK DROP and CONVERT TO MANUAL skipped the catalogued class-8
/api/stats TTL kick — both success paths called only loadLibrary(),
so the topbar N DROP pill kept its stale count for up to 10s after
the action ("the ACK didn't take"), unlike every sibling action.
Both now schedule setTimeout(refreshTopbarStatus, 1100).

(2) SYNC THEMERRDB's enqueue-failure catch never cleared the
optimistic 'tdb_sync' placeholder it had just set, leaving
// SYNCING THEMERRDB in the ops mini-bar for the placeholder TTL
after the alert. The catch now clears it like every sibling action.

(3) The SECTIONS SAVE handler wiped librariesDirty unconditionally
after the per-section loop, so FAILED sections' staged changes were
dropped — toggles visually reverted and SAVE disabled, forcing the
user to re-toggle to retry. Saves now delete only the saved
section's entry; failures stay dirty and retryable.

(4) Dead code removal: the v1.12.69 enum-count badge block anchored
on '#topbar-status .dot', which v1.12.118 retired — the badge never
rendered again. The JS block and its .topbar-dot-badge CSS rule are
gone; enumSectionIds stays (live consumers remain).
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def _handler(anchor: str, span: int = 1200) -> str:
    i = APP_JS.index(anchor)
    return APP_JS[i:i + span]


# ── (1) class-8 stats kick on ACK DROP / CONVERT ─────────────


def test_ack_drop_kicks_topbar_stats():
    block = _handler("} else if (act === 'ack-drop') {")
    assert "/ack-drop`);" in block
    assert "setTimeout(refreshTopbarStatus, 1100);" in block


def test_convert_to_manual_kicks_topbar_stats():
    block = _handler("} else if (act === 'convert-to-manual') {")
    assert "/convert-to-manual${params}`);" in block
    assert "setTimeout(refreshTopbarStatus, 1100);" in block


# ── (2) sync-failure clears the optimistic placeholder ───────


def test_sync_failure_clears_tdb_sync_placeholder():
    i = APP_JS.index("alert('Sync failed: ' + e.message);")
    block = APP_JS[i:i + 800]
    assert "clearOptimisticPlaceholder('tdb_sync')" in block, (
        "v1.22.88: the enqueue-failure catch must clear the "
        "optimistic placeholder it set"
    )
    assert "setSyncButtonState('idle');" in block


# ── (3) failed sections stay dirty/retryable ─────────────────


def test_sections_save_keeps_failed_sections_dirty():
    i = APP_JS.index("libraries-save-btn')?.addEventListener")
    block = APP_JS[i:APP_JS.index("\n    });", i)]
    assert "delete librariesDirty[sid];" in block
    assert "librariesDirty = {};" not in block, (
        "v1.22.88: the unconditional wipe dropped FAILED sections' "
        "staged changes — only saved sections may leave the map"
    )


# ── (4) dead enum-count badge removed ────────────────────────


def test_dead_enum_badge_block_removed():
    assert "enumDotEl" not in APP_JS
    assert ".topbar-dot-badge {" not in APP_CSS


def test_enum_section_ids_still_live():
    assert APP_JS.count("enumSectionIds") >= 3, (
        "enumSectionIds has live consumers (anyEnumRunning + the "
        "per-library inFlight check) — only the badge was dead"
    )
