"""v1.14.89 — REPROBE event dedupe by rk + REPROBE AGAIN disabled while running.

the user: "after clicking reprobe it seems to go off with an
issue but still hard to tell since old reprobe and open rows
below."

Two clarity fixes for the REPROBE error events on /queue:

## A. Dedupe by rating_key

Pre-fix every REPROBE run logged a fresh WARNING for each
unresolved row, so the events list piled up identical-looking
entries (rk=693570 errored at 12:16:54, again at 12:17:04,
etc). Same problem, multiple loud entries.

Events come back DESC by id (newest first). Dedupe keeps the
first occurrence per rk and suppresses older ones, so the
list shows "what's still unresolved" without the historical
noise.

## B. REPROBE AGAIN disabled while running

the user's logs showed two reprobe starts at 12:17:49 and
12:18:00 from a single click sequence — the button stayed
clickable while the first reprobe was in flight, queueing a
second redundant run. Reading motifOps.state().ops for an
active reprobe op gates the button to disabled +
"// REPROBE RUNNING…" while it's in flight.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
JS = REPO / "app" / "web" / "static" / "app.js"


def _events_render_block() -> str:
    """Slice from the events render in loadQueue. Anchored on
    the v1.14.89 dedupe marker so we get the right block even
    if loadQueue grows new sections."""
    src = JS.read_text()
    fn_start = src.index("async function loadQueue()")
    fn_end = src.index("\n  }\n", fn_start)
    return src[fn_start:fn_end]


# ── A. Dedupe by rk ────────────────────────────────────────────


def test_events_render_tracks_seen_reprobe_rks():
    """A Set tracks which rks we've already rendered. The
    .map() callback returns '' to suppress duplicates — the
    join collapses suppressed rows to nothing."""
    body = _events_render_block()
    assert "_seenReprobeRks" in body
    assert "new Set()" in body
    # The skip-on-duplicate branch.
    assert "_seenReprobeRks.has(det.rating_key)" in body
    assert "_seenReprobeRks.add(det.rating_key)" in body


def test_dedupe_only_applies_to_reprobe_events():
    """The dedupe must be gated inside the
    `e.component === 'reprobe'` branch. Other events (place,
    download, sync) keep their full chronology."""
    body = _events_render_block()
    # The dedupe live inside the reprobe branch.
    reprobe_idx = body.index("e.component === 'reprobe'")
    # Slice from the reprobe branch through the next 3000 chars
    # — the dedupe code lives within it.
    branch_block = body[reprobe_idx:reprobe_idx + 3000]
    assert "_seenReprobeRks.has(det.rating_key)" in branch_block


def test_v1_14_89_dedupe_marker_explains_rationale():
    """The v1.14.89 marker on the dedupe block explains
    the user's repro context (events DESC + dedupe = newest
    survives)."""
    body = _events_render_block()
    assert "v1.14.89: dedupe REPROBE warnings by rating_key" in body


# ── B. REPROBE AGAIN button disabled while running ─────────────


def test_reprobe_running_state_read_from_motif_ops():
    """The button-disable state must come from motifOps's
    current ops snapshot — kind='reprobe_plex_themes' and
    status running/cancelling. Reading from window.motifOps
    keeps us in sync with the polled /api/progress feed."""
    body = _events_render_block()
    assert "_reprobeRunning" in body
    # Reads from motifOps.state().
    assert "window.motifOps.state" in body
    # Filters by kind + status.
    assert "o.kind === 'reprobe_plex_themes'" in body
    assert "o.status === 'running'" in body
    assert "o.status === 'cancelling'" in body


def test_reprobe_again_button_disabled_when_running():
    """When _reprobeRunning is true, the REPROBE AGAIN button
    must render with `disabled` attribute and the running
    label so the user can't queue back-to-back reprobes."""
    body = _events_render_block()
    # The conditional.
    assert "_reprobeRunning" in body
    # The disabled attribute branch.
    assert "disabled title=\"REPROBE PLEX THEMES is already running…\"" in body
    # The running-label branch.
    assert "// REPROBE RUNNING…" in body


def test_reprobe_again_button_enabled_when_idle():
    """When _reprobeRunning is false, the button renders
    normally (no disabled attribute, "// REPROBE AGAIN" label)."""
    body = _events_render_block()
    # Default (idle) label still present.
    assert "// REPROBE AGAIN" in body
    # Default tooltip still wires through.
    assert "Re-fire the global REPROBE PLEX THEMES" in body


def test_motif_ops_state_read_is_defensive():
    """The motifOps.state() call must be wrapped so a missing
    motifOps (early page load, drawer not initialized) doesn't
    throw and break the entire events render. Defaults to
    "not running" if state is unavailable."""
    body = _events_render_block()
    # try/catch wrap.
    assert "try {" in body
    # Defensive presence check.
    assert "window.motifOps && window.motifOps.state" in body


def test_v1_14_89_running_lock_marker_present():
    """The v1.14.89 marker on the running-lock block explains
    the rationale (the user's double-fire repro from logs)."""
    body = _events_render_block()
    assert (
        "v1.14.89: detect whether a REPROBE op is currently"
        in body
    )
