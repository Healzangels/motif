"""v1.15.128 — event-stream empty-state fallbacks.

Design audit surfaced two `<ul>` event-stream surfaces that
rendered as VISUALLY EMPTY when the API returned zero events:

  - `#event-stream` on the dashboard (recent 20 events)
  - `#event-stream-full` on /queue (events under the current
    SINCE filter)

Pre-fix both did `events.map(...).join('')` with no fallback,
so zero events produced an empty string → `<ul>` rendered with
no `<li>` children. The user saw a section header ("EVENT
STREAM") with nothing below it. On a fresh install (dashboard)
or a tight SINCE filter (queue) this read as "is the section
broken?" instead of "no events yet."

## Fix

Both renders now chain `|| '<li class="muted small">...</li>'`
to the join, mirroring the empty-state pattern used by:

  - library tbody (v1.13.58 contextual messages)
  - queue jobs tbody (v1.11.x "no jobs in the queue")
  - libraries movie/TV tbodys ("no movie sections discovered")
  - tokens-body ("no tokens yet")

Messages are contextual:

  - dashboard: "no events yet — actions and worker activity
    (sync / download / place) appear here" (tells the user
    HOW to make events appear)
  - queue: "no events match the current SINCE filter — try
    widening to ALL or wait for worker activity" (tells the
    user the most likely fix)

## Tests
"""

from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"


def test_dashboard_event_stream_has_empty_state():
    """The `#event-stream` render must fall back to a `<li>` with
    a muted explanatory message when the events list is empty."""
    src = APP_JS.read_text()
    # Find the dashboard event-stream render block.
    anchor = src.index("$('#event-stream');")
    block = src[anchor:anchor + 1500]
    assert "no events yet" in block, (
        "v1.15.128: dashboard #event-stream must show 'no events "
        "yet' fallback when the API returns 0 events. Without it "
        "the section reads as broken on fresh installs."
    )
    # Empty state should be styled with the `muted small`
    # primitive pair so it visually de-emphasizes against
    # populated rows.
    assert 'class="muted small"' in block


def test_queue_event_stream_has_empty_state():
    """The `#event-stream-full` render must fall back when no
    events match the SINCE filter — most likely fix is widening
    the filter so call that out in the message."""
    src = APP_JS.read_text()
    anchor = src.index("const _evHtml = evs.events.map(")
    # Wider window — the queue-event render block is ~7-8KB
    # because of the per-event reprobe action button branch.
    block = src[anchor:anchor + 9000]
    assert "no events match the current SINCE filter" in block, (
        "v1.15.128: queue #event-stream-full must show "
        "filter-aware fallback when no events match. Pre-fix the "
        "tight-SINCE case showed an empty <ul>."
    )


def test_empty_state_styling_uses_muted_small():
    """Both event-stream empty-state rendering must use the
    `muted small` primitive pair so they style consistently
    with other empty-state messages (tbody, op-drawer, etc.)."""
    src = APP_JS.read_text()
    # Both should reference the muted+small class combo on the
    # empty-state <li>.
    dash_anchor = src.index("no events yet")
    dash_window = src[max(0, dash_anchor - 200):dash_anchor]
    assert 'muted small' in dash_window, (
        "v1.15.128: dashboard empty-state must use `muted small`"
    )
    queue_anchor = src.index("no events match the current SINCE filter")
    queue_window = src[max(0, queue_anchor - 200):queue_anchor]
    assert 'muted small' in queue_window, (
        "v1.15.128: queue empty-state must use `muted small`"
    )
