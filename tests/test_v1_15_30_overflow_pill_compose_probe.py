"""v1.15.30 — overflow pill composes live-probe info into every
queue/sync branch (not just the standalone branch).

## Why

the user's repro after testing v1.15.28:

> "while a probe of themerrdb urls is going and a refresh is
>  queued it properly shows the refresh status bar and lets you
>  know the probe is still going I like that, however when a
>  sync of themerrdb is queued on top of that it just says sync
>  pending and you lose the information on the probe. Can we
>  treat it similar to if there are multiple refresh but sync
>  queued — include both in the status bar pill for the queued
>  items."

## Pre-fix (v1.15.7 + v1.14.90)

The overflow-pill render in `renderTopbar` was a strict
if/else cascade with the live-probe branch as the FINAL else
clause:

  1. queueDepth > 0 && hasSyncPending → "+N QUEUED · SYNC"
  2. queueDepth > 0                    → "+N QUEUED"
  3. hasSyncPending                    → "SYNC QUEUED"
  4. dlQueueDepth > 0                  → "+N QUEUED" (warn tone)
  5. liveProbe                         → "PROBING TDB" / "REPROBING PLEX"
  6. else                              → hidden

the user's case: plex_enum running (mini-bar) + bulk_probe_tdb
running + tdb_sync_pending queued. The cascade hit branch 3
("SYNC QUEUED") and exited — so the probe info that would
normally appear in branch 5 was silently dropped. The user
saw "SYNC QUEUED" and lost track that the probe was still
running.

## Post-fix

Lift the live-probe detection ABOVE the cascade and append
the probe label as a suffix to every queue/sync branch's
label string. Same composition shape as v1.14.90's existing
"+N QUEUED · SYNC" branch — extends that pattern uniformly.
The standalone branch (5) keeps its current shape (no
suffix to append; it IS the probe).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
OPS_JS = REPO / "app" / "web" / "static" / "ops.js"


def _overflow_block() -> str:
    """Return the renderTopbar overflow-pill cascade body for
    anchored substring assertions. Anchored on the v1.15.30
    composition marker so test windows resize naturally as the
    surrounding code grows."""
    src = OPS_JS.read_text()
    anchor = src.index(
        "v1.15.30: lift live-probe detection ABOVE the cascade"
    )
    end = src.index("} else {\n        overflow.hidden = true;", anchor)
    return src[anchor:end + 200]


# ── liveProbe lifted above the cascade ────────────────────────


def test_live_probe_detected_outside_else_branch():
    """Pre-fix the liveProbe detection sat inside the else
    branch of the cascade (after branches 1-4 fell through).
    Post-fix it must be detected at the cascade's TOP so every
    branch can append the suffix."""
    src = OPS_JS.read_text()
    # Detection happens above the if-cascade.
    detection_anchor = src.index(
        "v1.15.30: lift live-probe detection ABOVE the cascade"
    )
    cascade_anchor = src.index(
        "if (queueDepth > 0 && hasSyncPending)", detection_anchor,
    )
    # Detection must come BEFORE the cascade entry.
    block_before_cascade = src[detection_anchor:cascade_anchor]
    assert "const liveProbe" in block_before_cascade, (
        "v1.15.30: liveProbe must be detected above the cascade "
        "so every branch can read it"
    )
    assert "const probeLabel" in block_before_cascade
    assert "const probeSuffix" in block_before_cascade


def test_probe_kinds_include_bulk_probe_reprobe_and_lps():
    """The live-probe detection must consider all bulk ops that
    a user would want to keep visible while another op runs:
    bulk_probe_tdb, reprobe_plex_themes, and bulk_lps (v1.15.28).
    Pin the set so a future bulk-op kind (added without
    updating this set) silently falls out of the overflow
    surface."""
    block = _overflow_block()
    assert "'bulk_probe_tdb'" in block
    assert "'reprobe_plex_themes'" in block
    assert "'bulk_lps'" in block, (
        "v1.15.30: bulk_lps (v1.15.28's bulk LPS op) must also "
        "qualify as a probe-kind for overflow-pill composition"
    )


def test_probe_label_maps_kind_to_human_readout():
    """Each probe kind must map to a distinct human-readable
    label for the pill text. Pre-fix only bulk_probe_tdb +
    reprobe_plex_themes mapped; v1.15.30 adds bulk_lps."""
    block = _overflow_block()
    assert "'PROBING TDB'" in block
    assert "'REPROBING PLEX'" in block
    assert "'BULK LPS'" in block


# ── Suffix appended to every queue/sync branch ────────────────


def test_branch_queue_plus_sync_appends_probe_suffix():
    """Branch 1 (queue + sync queued) must append `${probeSuffix}`
    to its label so probe info doesn't get lost when both
    queues are active."""
    block = _overflow_block()
    # Locate branch 1.
    branch_anchor = block.index("if (queueDepth > 0 && hasSyncPending)")
    # The next branch starts with `else if (queueDepth > 0)`.
    branch_end = block.index("} else if (queueDepth > 0)", branch_anchor)
    branch_body = block[branch_anchor:branch_end]
    assert "${probeSuffix}" in branch_body, (
        "v1.15.30: queue+sync branch must append probeSuffix to "
        "the pill label so a concurrent probe stays visible"
    )


def test_branch_queue_only_appends_probe_suffix():
    """Branch 2 (queue depth only)."""
    block = _overflow_block()
    branch_anchor = block.index("} else if (queueDepth > 0)")
    branch_end = block.index("} else if (hasSyncPending)", branch_anchor)
    branch_body = block[branch_anchor:branch_end]
    assert "${probeSuffix}" in branch_body


def test_branch_sync_pending_only_appends_probe_suffix():
    """Branch 3 (sync queued only) — the user's exact repro.
    Pre-fix this branch dropped the probe info entirely."""
    block = _overflow_block()
    branch_anchor = block.index("} else if (hasSyncPending)")
    branch_end = block.index("} else if (dlQueueDepth > 0)", branch_anchor)
    branch_body = block[branch_anchor:branch_end]
    assert "${probeSuffix}" in branch_body, (
        "v1.15.30: sync-queued-only branch must append probeSuffix "
        "(the user's exact repro — probe was being dropped here)"
    )


def test_branch_dl_queue_appends_probe_suffix():
    """Branch 4 (download queue only)."""
    block = _overflow_block()
    branch_anchor = block.index("} else if (dlQueueDepth > 0)")
    branch_end = block.index("} else if (liveProbe)", branch_anchor)
    branch_body = block[branch_anchor:branch_end]
    assert "${probeSuffix}" in branch_body


# ── Standalone live-probe branch preserved ────────────────────


def test_standalone_live_probe_branch_still_renders():
    """When NO queue/sync signals are present and only a probe
    is running, the v1.15.7 standalone branch must still
    render the probe pill on its own (no suffix needed — the
    probe IS the pill content)."""
    block = _overflow_block()
    branch_anchor = block.index("} else if (liveProbe)")
    branch_end = block.index("} else {", branch_anchor)
    branch_body = block[branch_anchor:branch_end]
    # Renders the probe label as the pill content directly.
    assert "${probeLabel}" in branch_body
    # tdb tone (orange — TDB-side metadata check).
    assert "op-tone-tdb" in branch_body


# ── v1.15.7 + v1.14.90 anchors preserved (regression guard) ──


def test_v1_14_90_queued_sync_combination_label_preserved():
    """The v1.14.90 "+N QUEUED · SYNC" composition (queue +
    sync queued) must still render. v1.15.30 only ADDED a
    suffix; the base label must stay intact."""
    block = _overflow_block()
    assert "QUEUED · SYNC" in block, (
        "v1.15.30 must preserve the v1.14.90 '+N QUEUED · SYNC' "
        "composition — the probe suffix appends to it, doesn't "
        "replace it"
    )


def test_v1_15_7_probe_labels_preserved():
    """v1.15.7 introduced the standalone PROBING TDB /
    REPROBING PLEX badge. v1.15.30 generalizes the labels to
    a suffix; the original literal labels must still be in
    the source so the standalone branch still uses them."""
    block = _overflow_block()
    assert "PROBING TDB" in block
    assert "REPROBING PLEX" in block
