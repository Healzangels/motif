"""v1.15.7 — two probe-related UX fixes from the user's v1.15.6 retest.

## A. Probe button watcher false-cancelled on stale op_progress row

the user: "right away after clicking probe tdb url from the
settings to do a bulk prob it will say cancelled right away,
then a few seconds later see the status bar change and it
begin the probe"

The op_progress table retains rows for 24h post-finish. When
the user clicks PROBE TDB URLS shortly after a previous probe
was cancelled (or finished), the OLD row with status='cancelled'
is still in motifOps.state().ops with the same kind. The
watcher's first poll (within ~2s) lands in the gap between
threading.Thread.start() and the spawned thread's
start_progress call (a few hundred ms) — sees the OLD row,
reports "× cancelled" immediately. Then a few seconds later
the new probe actually starts.

Fix: filter ops by started_at >= watcher's startTime
(with 5s skew tolerance). Stale terminal-status rows are
ignored until the new run's start_progress stamps a fresh
started_at.

## B. Probe-running indicator when probe is hidden by mini-bar

the user: "when a refresh it started over the probe it swaps
to the refresh status which is good but you can't tell the
probe is still going without looking in the drawer or
waiting for the refresh to finish, would be good to have a
similar indicator when a sync themerdb is queued or going
when a refresh is going."

Pre-fix the topbar mini-bar shows the most-recently-updated
running op. When bulk_probe_tdb (or reprobe_plex_themes)
runs concurrently with a plex_enum / sync, the probe is
hidden because the other op's progress updates more
frequently. Drawer has the full picture; topbar doesn't.

Fix: extend the overflow pill chain to surface a
"PROBING TDB" / "REPROBING PLEX" indicator when a probe op
is running but NOT the one currently in the mini-bar. Same
visual treatment as the v1.14.90 SYNC QUEUED indicator —
tdb tone, simple label, click-through to drawer for full
progress.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"
OPS_JS = REPO / "app" / "web" / "static" / "ops.js"


# ── A. Watcher stale-op filter ─────────────────────────────────


def test_watcher_filters_ops_by_started_at():
    """The completion watcher must filter the ops list to only
    rows whose started_at is >= the watcher's own startTime
    (with skew tolerance). Pre-fix it matched any op with the
    matching kind and reported terminal-status immediately on
    the first poll, even if that row was stale history from a
    previous run."""
    src = APP_JS.read_text()
    fn_start = src.index("function _watchOpForCompletion(")
    fn_end = src.index("function ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    # The watcher must reference startTime when filtering.
    assert "opStarted >=" in fn_body, (
        "Watcher must compare op.started_at against startTime"
    )
    # Skew tolerance for clock disagreement.
    assert "startTime - 5000" in fn_body, (
        "Watcher must allow 5s negative skew to handle clock "
        "differences between server + client"
    )


def test_watcher_handles_missing_started_at():
    """Defensive: if the op has no started_at (or it's
    unparseable), the filter must accept it rather than
    silently skip. Stale-row protection is the secondary
    concern; correctness when the timestamp is missing
    matters more."""
    src = APP_JS.read_text()
    fn_start = src.index("function _watchOpForCompletion(")
    fn_end = src.index("function ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    assert "if (!o.started_at) return true" in fn_body
    assert "Number.isNaN(opStarted)" in fn_body


def test_watcher_v1_15_7_marker_explains_intent():
    """v1.15.7 marker references the stale-row repro so a
    future watcher refactor sees the rationale."""
    src = APP_JS.read_text()
    fn_start = src.index("function _watchOpForCompletion(")
    fn_end = src.index("function ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    assert "v1.15.7" in fn_body
    # Reference to the 24h retention or the threading gap.
    assert "24h" in fn_body or "stale" in fn_body.lower()


# ── B. Probe-running indicator ─────────────────────────────────


def test_overflow_pill_surfaces_probe_when_hidden_by_minibar():
    """The overflow pill chain must include a probe-running
    branch that fires when bulk_probe_tdb or reprobe_plex_themes
    is running but NOT the op shown in the mini-bar.
    v1.15.30 lifted the probe detection ABOVE the cascade so
    every branch can compose a probe suffix into its label —
    the detection logic moved to the v1.15.30-anchored block,
    while the v1.15.7 marker now describes the standalone
    branch (no other queue/sync signals → probe pill stands
    alone)."""
    src = OPS_JS.read_text()
    # v1.15.30: detection logic anchored on the new marker.
    detection_anchor = src.index(
        "v1.15.30: lift live-probe detection ABOVE the cascade"
    )
    cascade_anchor = src.index(
        "if (queueDepth > 0 && hasSyncPending)", detection_anchor,
    )
    detection_block = src[detection_anchor:cascade_anchor]
    # Must check for both probe kinds (plus v1.15.28's bulk_lps).
    assert "bulk_probe_tdb" in detection_block
    assert "reprobe_plex_themes" in detection_block
    # Must check status is live (running / cancelling).
    assert "'running'" in detection_block
    assert "'cancelling'" in detection_block
    # Must skip when the probe is the same as the mini-bar's op.
    assert "o.kind !== op.kind" in detection_block, (
        "Probe-running branch must exclude the op currently in "
        "the mini-bar (would double-render)"
    )


def test_probe_badge_uses_tdb_tone_with_named_label():
    """Badge tone must be tdb (orange) — distinct from the
    mini-bar's likely plex / warn tone. Label names the probe
    kind for clarity. v1.15.30 split detection (above cascade)
    from rendering (in the standalone branch); both tone +
    label live in the standalone branch's render block."""
    src = OPS_JS.read_text()
    # The standalone probe-render branch (no other queue signals).
    anchor = src.index("v1.15.7: probe running but hidden")
    block = src[anchor:anchor + 3000]
    assert "op-tone-tdb" in block
    # The two label variants — defined in the v1.15.30
    # detection block above the cascade.
    detection_anchor = src.index(
        "v1.15.30: lift live-probe detection ABOVE the cascade"
    )
    detection_block = src[detection_anchor:detection_anchor + 1500]
    assert "PROBING TDB" in detection_block, (
        "bulk_probe_tdb badge must label as PROBING TDB"
    )
    assert "REPROBING PLEX" in detection_block, (
        "reprobe_plex_themes badge must label as REPROBING PLEX"
    )


def test_probe_branch_only_fires_when_no_other_badge_active():
    """The probe-running branch must be in the FINAL else of
    the overflow pill chain. Pre-existing branches (plex/sync/
    download queues) take priority — those signal queued WORK,
    while probe-running is just a "you have another thing
    running" hint."""
    src = OPS_JS.read_text()
    # Find the if/else-if chain in the overflow logic.
    chain_anchor = src.index("if (queueDepth > 0 && hasSyncPending)")
    chain_block = src[chain_anchor:chain_anchor + 5000]
    # Probe check must come after dlQueueDepth (priority order).
    dl_idx = chain_block.index("else if (dlQueueDepth > 0)")
    probe_idx = chain_block.index(
        "v1.15.7: probe running but hidden",
    )
    assert dl_idx < probe_idx, (
        "Probe-running branch must come AFTER the queued-* "
        "branches (queued work is higher priority signal)"
    )


def test_overflow_hidden_when_no_signal_at_all():
    """Regression guard: when no queue is queued AND no probe
    is running, the overflow pill must hide. Pre-fix this was
    a bare `else { overflow.hidden = true; }`; the v1.15.7
    addition put the probe check + a nested else inside that
    block — verify the hidden=true path is still reachable."""
    src = OPS_JS.read_text()
    anchor = src.index("v1.15.7: probe running but hidden")
    block = src[anchor:anchor + 3000]
    # The new structure has if (liveProbe) ... else { overflow.hidden = true; }
    assert "overflow.hidden = true" in block, (
        "Hidden=true path must remain reachable when no probe "
        "is running"
    )
