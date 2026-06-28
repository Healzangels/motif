"""v1.14.65 — mini-bar prefers running over pending + dash button lock-reason tooltips.

Two the user reproes after the v1.14.64 logger-quieting ship:

## Issue 1: REFRESH PLEX button "looks running" during a TDB sync

The v1.14.62 fix split disable-from-label so the REFRESH PLEX
button correctly shows the IDLE label "// REFRESH PLEX" (not
"// REFRESHING PLEX…") during a TDB sync. But the disabled
state's only visual cue is `opacity: 0.4` — the user still read
the locked button as "running too" because there was no
explanation of WHY it was locked.

Fix: `title` tooltip on the disabled button explaining the
lock reason. When dashSyncPlexBtn is disabled SOLELY due to
themerrdbBusy (writer-lock prevention), the title reads
"Locked while THEMERRDB SYNC runs (avoid SQLite writer-lock
contention). Will re-enable when sync finishes." Mirror
tooltip on the SYNC THEMERRDB button when it's locked due to
cascadeInFlight (post-sync auto-enum cascade).

## Issue 2: mini-bar flips between "1 LIBRARY REFRESH QUEUED" and "MOVIES (FETCH)"

When the user queues multiple plex_enum jobs (e.g., REFRESH
MOVIES then REFRESH TV in quick succession), the synth
`plex_enum_pending` card (api.py: refreshes its updated_at
every /api/progress poll) competed with the actually-running
plex_enum op for the most-recently-updated mini-bar slot.
the user repro: topbar visibly pinged between the queued count
label and the running progress label every poll.

Fix: in ops.js renderTopbar, prefer `status === 'running'`
ops over `status === 'pending'` when picking the mini-bar
candidate. Pending ops still appear in the drawer (where they
have room to coexist); only the contended single mini-bar
slot strictly favors the running op.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
JS = REPO / "app" / "web" / "static" / "app.js"
OPS_JS = REPO / "app" / "web" / "static" / "ops.js"


# ── Issue 1: dash-button lock-reason tooltips ────────────────


def test_refresh_plex_btn_keeps_in_progress_tooltip():
    """The dash REFRESH PLEX button must still surface a `title`
    when actively refreshing Plex. v1.14.66 retired the
    "writer-lock contention" branch (and the themerrdbBusy lock
    it explained) — see test_v1_14_66 for the supersede — but the
    "Plex refresh in progress" hover affordance survives.

    Pre-v1.14.65 the disabled button had no explanation at all,
    which the user read as "running too" even when the label was
    correct (idle "// REFRESH PLEX")."""
    js = JS.read_text()
    # The v1.14.65 marker text was rewritten by v1.14.66 — anchor on
    # the v1.14.62 marker (still load-bearing for the split-disable-
    # from-label rationale) and walk forward to the dashSyncPlexBtn
    # block.
    anchor = js.index("v1.14.62: split disabled-state from label")
    block = js[anchor:anchor + 3000]
    # In-progress branch + clear branch survive v1.14.66.
    assert "dashSyncPlexBtn.title = 'Plex refresh in progress" in block
    assert "dashSyncPlexBtn.removeAttribute('title')" in block


def test_sync_themerrdb_btn_has_cascade_tooltip():
    """Mirror tooltip on the SYNC THEMERRDB button — when locked
    due to cascadeInFlight (post-sync auto-enum still draining),
    the user should see why the button is busy even though the
    TDB phase finished."""
    js = JS.read_text()
    anchor = js.index("v1.14.65: tooltip explains lock reason on the SYNC")
    block = js[anchor:anchor + 1500]
    assert "syncBtn.title = 'TDB sync in progress" in block
    assert "Post-sync Plex refresh cascade still draining" in block
    assert "syncBtn.removeAttribute('title')" in block


# ── Issue 2: mini-bar prefers running over pending ───────────


def test_mini_bar_prefers_running_over_pending():
    """When picking the candidate op for the topbar mini-bar,
    `status === 'running'` ops must win over `status === 'pending'`
    ops. Pre-fix the synth `plex_enum_pending` card (refreshes its
    updated_at every poll) competed with the running plex_enum op
    for the most-recently-updated slot; the mini-bar visibly
    flipped between the queued count and the running progress
    label each tick."""
    js = OPS_JS.read_text()
    # Anchor on the v1.14.65 marker.
    anchor = js.index("v1.14.65: prefer status='running' over status='pending'")
    block = js[anchor:anchor + 2000]
    # The runningOnly filter + fallback to running.
    assert "const runningOnly = running.filter(" in block
    assert (
        "(o) => o.status === 'running' || o.status === 'cancelling'"
        in block
    )
    # The candidates fallback: prefer running, fall back to pending
    # when no running ops are in flight (e.g., synth card alone).
    assert "const candidates = runningOnly.length > 0 ? runningOnly : running;" in block


def test_mini_bar_candidate_sort_uses_candidates_not_running():
    """The .sort()[0] picker must use the new `candidates` array,
    not the original `running` (which would re-include pending
    ops)."""
    js = OPS_JS.read_text()
    anchor = js.index("v1.14.65: prefer status='running' over status='pending'")
    block = js[anchor:anchor + 2500]
    # Sort uses candidates.
    assert (
        "const op = candidates.slice().sort((a, b) =>"
        in block
    )
