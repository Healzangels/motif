"""v1.22.84 (audit round 2, Batch D #1) — orphans page LOWs.

(1) RE-PUSH races the worker: /replace only ENQUEUES a place job (the
worker claims it on its ~5s loop), but the post-action reprobe fired
at 1.5s — it always saw pre-job Plex state, re-rendered the same
drift row, and the action read as broken (it succeeded ~5-10s later,
invisibly). Now: '✓ QUEUED' (the accurate verb the library page
already uses) + a 12s reprobe for repush only; the synchronous
actions (LPS/PURGE/DELETE-SIDECAR) keep 1.5s.

(2) activeDriftFilter dead-ended when its drift type vanished from
the data (last filtered row dropped via placement_gone / fresh scan):
the chip row rebuilt WITHOUT that type's chip while the filter stayed
set — no chip active, table stuck on "No findings for filter X".
renderChips now resets the filter when its type left the summary.

(3) A failed post-action reprobe left the ✓/NO-OP glyph DISABLED on a
row whose displayed state was stale — recovery required spotting the
small status line and using the separate PROBE button. Both reprobe
catches now re-arm the acted-on button.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
ORPHANS_HTML = (REPO / "app" / "web" / "templates" /
                "orphans.html").read_text()


def test_repush_shows_queued_and_delays_reprobe():
    i = ORPHANS_HTML.index("const _isAsyncAct = act === 'repush'")
    block = ORPHANS_HTML[i:i + 1200]
    assert "'✓ QUEUED'" in block, (
        "v1.22.84: RE-PUSH only enqueues — the verb must say so"
    )
    assert "_isAsyncAct ? 12000 : 1500" in block, (
        "the reprobe must wait past the worker's claim-and-upload "
        "window for the async action only"
    )


def test_vanished_drift_filter_resets():
    i = ORPHANS_HTML.index("function renderChips(summary)")
    head = ORPHANS_HTML[i:i + 800]
    assert "!(activeDriftFilter in summary)" in head
    assert "activeDriftFilter = null;" in head


def test_failed_reprobe_rearms_the_button():
    # Both setTimeout catches restore the label + enable (indent
    # differs between the NO-OP and main paths — match loosely).
    import re
    count = len(re.findall(
        r"btn\.textContent = origText;\s*\n\s*btn\.disabled = false;\s*\n"
        r"\s*\}\), ", ORPHANS_HTML))
    assert count >= 2, (
        "v1.22.84: both post-action reprobe catches must re-arm the "
        f"button (found {count})"
    )
