"""v1.14.73 — restore per-tab pending-aware lock without re-triggering cross-tab cascade.

the user v1.14.72 follow-up:

> "while jumping between libraries while a refresh is going on
>  and starting another refresh in another library seems to
>  make the library still refreshing button become clickable
>  again when it should be locked or a section not being
>  scanned or queued as locked"

## Diagnosis

v1.14.66 narrowed `enum_running_rows` SQL from
`status IN ('pending','running')` to `status = 'running'` to
fix a bug where queueing a 2nd library scan locked EVERY
library tab (because `enumTabsActive > 1` triggered
`globalEnumPipeline`).

That fix was correct for the cross-tab signal but went too far:
the per-tab `myTabBusy` (drives the per-library REFRESH lock)
was reading the SAME map. Result: a tab that had a PENDING
plex_enum job for itself stopped reading as "busy" and its
REFRESH button became clickable mid-burst. the user's repro:

  1. Start REFRESH on STD MOVIES → STD enum running.
  2. Click //4K → navigate to 4K MOVIES view.
  3. Click REFRESH 4K MOVIES → queues a 4K enum (pending).
  4. Click //STANDARD → back to STD MOVIES.
  5. STD button shows clickable even though STD might still be
     mid-run (or 4K is now running while STD just finished —
     either way the just-queued 4K button on its own page
     would also become clickable, also wrong).

## Fix

Split the SQL into TWO queries:
  - `enum_running_rows` (status='running') — feeds the existing
    `plex_enum_active` map + section-id list.
  - `enum_pending_rows` (status='pending') — feeds a NEW
    parallel `plex_enum_pending` map + pending section-id list.

JS combines BOTH for the per-tab `myTabBusy` and per-section
REFRESH lock (so a queued tab/section stays locked). But the
cross-tab `enumTabsActive` (driving `globalEnumPipeline`) reads
ONLY the running map — so queueing across tabs doesn't re-
trigger the v1.14.66-fixed ALL-tabs-locking bug.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
JS = REPO / "app" / "web" / "static" / "app.js"
API = REPO / "app" / "web" / "api.py"


# ── Server-side: parallel pending query ───────────────────────


def test_enum_pending_rows_sql_filters_to_pending_only():
    """The new `enum_pending_rows` query must filter on
    `status = 'pending'` only — the parallel of the running
    query, not a re-merging of both states."""
    py = API.read_text()
    anchor = py.index("enum_pending_rows = conn.execute(")
    block = py[anchor:anchor + 800]
    assert "AND j.status = 'pending'" in block
    # Must NOT include the running predicate or the combined IN.
    assert "AND j.status = 'running'" not in block
    assert "AND j.status IN" not in block


def test_stats_sync_returns_enum_pending_rows():
    """The internal `_stats_sync` tuple return must include the
    new pending rows so the async caller can build the
    plex_enum_pending map."""
    py = API.read_text()
    fn_start = py.index("def _stats_sync(db: Path) -> tuple:")
    # Slice through the return statement.
    ret_idx = py.index("return (row, last_sync, enum_running_rows", fn_start)
    # v1.24.41: +repush_tab_row (and its comment) in the tuple widened the slice.
    block = py[ret_idx:ret_idx + 900]
    # The pending rows are part of the tuple.
    assert "enum_pending_rows" in block


def test_api_stats_response_exposes_plex_enum_pending_map():
    """The /api/stats response payload must surface
    `plex_enum_pending` (the {tab: {variant: bool}} map) +
    `plex_enum_pending_section_ids` so the JS can read them."""
    py = API.read_text()
    # Both new keys are in the response dict.
    assert '"plex_enum_pending": plex_enum_pending,' in py
    assert (
        '"plex_enum_pending_section_ids": plex_enum_pending_section_ids,'
        in py
    )


def test_plex_enum_pending_map_initial_shape_matches_active():
    """The pending map must have the same {movies/tv/anime ×
    standard/fourk} shape as plex_enum_active so the JS can
    treat them symmetrically without per-tab guards."""
    py = API.read_text()
    # The initial pending dict literal mirrors plex_enum_active.
    pattern_idx = py.index("plex_enum_pending = {")
    block = py[pattern_idx:pattern_idx + 400]
    assert '"movies": {"standard": False, "fourk": False}' in block
    assert '"tv":     {"standard": False, "fourk": False}' in block
    assert '"anime":  {"standard": False, "fourk": False}' in block


# ── Client-side: per-tab + per-section locks ──────────────────


def test_js_reads_plex_enum_pending_from_stats():
    """The JS must pull the new map + section-id list from the
    /api/stats payload (q.plex_enum_pending,
    q.plex_enum_pending_section_ids)."""
    js = JS.read_text()
    assert "q.plex_enum_pending || {}" in js
    assert "q.plex_enum_pending_section_ids || []" in js


def test_my_tab_busy_combines_active_and_pending():
    """The myTabBusy expression must lock the per-tab REFRESH
    button when the tab+variant is RUNNING OR PENDING. This is
    the fix for the user's "queued tab becomes clickable" repro."""
    js = JS.read_text()
    # Anchor on the v1.14.73 marker.
    anchor = js.index(
        "v1.14.73: include pending too. Pre-fix v1.14.66"
    )
    block = js[anchor:anchor + 2000]
    # Both maps participate in the disjunction.
    assert "enumActive[tabKey] && enumActive[tabKey][variantKey]" in block
    assert "enumPending[tabKey] && enumPending[tabKey][variantKey]" in block
    # The combiner is `||` (either signal locks).
    assert "||" in block


def test_per_section_refresh_lock_combines_running_and_pending():
    """The per-section REFRESH button (on /settings) must lock
    when the section is in EITHER the running set OR the
    pending set."""
    js = JS.read_text()
    # Anchor on the per-section refresh callsite.
    anchor = js.index(
        "Per-section REFRESH — lock only if THIS section is enumerating."
    )
    block = js[anchor:anchor + 1000]
    # Both sets participate.
    assert "enumSectionIds.has(sid)" in block
    assert "enumPendingSectionIds.has(sid)" in block


def test_enum_tabs_active_stays_running_only():
    """The cross-tab `enumTabsActive` count (which drives
    `globalEnumPipeline > 1`) must STILL read only the running
    map. Otherwise a single pending job on a 2nd tab would re-
    trigger the v1.14.66-fixed ALL-tabs-locking bug."""
    js = JS.read_text()
    # The enumTabsActive expression filters tabs by enumActive
    # (running) only — never by enumPending.
    fn_idx = js.index("const enumTabsActive = ['movies', 'tv', 'anime']")
    expr = js[fn_idx:fn_idx + 400]
    assert "enumActive[t]" in expr
    # The pending map MUST NOT appear in this expression.
    assert "enumPending" not in expr
