"""v1.15.20 — explicit scope_items bypasses cooldown + total=0 logs event.

the user: "attempted a bulk let plex server and it stated it would
probe 2 urls but and would put status in the status bar but never
saw any probe or change in the status bar"

## Pre-fix

The LET PLEX SERVE bulk pre-flight (v1.15.1) posts the user's
selection as `scope_items` to /api/admin/bulk-probe-tdb. The SQL
applied BOTH the IN-tuples filter AND the 24h cooldown filter.
When the selected rows had been probed within the last 24h
(common for a routine selection of clean rows), the intersection
produced total=0 → the early-return at line 2511 fired silently
→ no log_event, no follow-up activity → the LIVE OPS card
flashed "done" for ~50ms and disappeared. Operator saw no
signal at all that the probe ran.

## Fix (two parts)

1. SCOPE-ITEMS BYPASSES COOLDOWN. The 24h cooldown is designed
   to prevent back-to-back full-library sweeps from DOSing
   YouTube. An explicit user-selected probe (scope_items set,
   scope_failures_only False) is a deliberate operator action
   and should override the cooldown. New `elif scope_items:`
   branch sets `row_filter = ""` and `activity_note =
   "(selected rows, cooldown bypassed)"`.
   v1.22.68 re-keyed the bypass on scope_CLAUSE (selection
   actually applied to the SQL): a >499 selection degrades to a
   global probe and keeps the cooldown in its own
   `elif scope_items:` branch — see
   test_v1_22_68_bulk_probe_scope_degradation.py.
2. TOTAL=0 LOGS AN INFO EVENT. The empty early-return now fires
   a log_event with the scope label + activity note + "nothing
   to do" tag so the operator sees the result in /queue events
   and docker logs even when no probes ran.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"


def test_scope_items_bypasses_cooldown_branch_present():
    """The new `elif scope_items:` branch must sit between the
    failures-only branch and the cooldown branch. Pin its
    presence + the row_filter shape (empty string = no
    cooldown filter applied) + the distinctive activity_note
    so operator-visible logs reflect the bypass."""
    src = API_PY.read_text()
    fn_start = src.index("def _bulk_probe_tdb_run(")
    fn_end = src.index("\ndef ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    # The bypass branch. v1.22.68: keyed on scope_CLAUSE (selection
    # actually applied) so a >499 degraded global probe keeps the cooldown.
    assert "elif scope_clause:" in fn_body, (
        "v1.15.20/v1.22.68: an APPLIED selection must have its own SQL "
        "branch that bypasses the cooldown filter"
    )
    branch_anchor = fn_body.index(
        "elif scope_clause:\n                # v1.15.20")
    branch_block = fn_body[branch_anchor:branch_anchor + 2500]
    assert 'row_filter = ""' in branch_block, (
        "v1.15.20: scope_items branch must set row_filter to "
        "the empty string (no cooldown clause)"
    )
    assert "(selected rows, cooldown bypassed)" in branch_block


def test_scope_items_branch_sits_between_failures_and_cooldown():
    """The branch ORDER matters: scope_failures_only check first
    (failures-only is its own predicate that already bypasses
    cooldown), THEN scope_clause (applied selection, bypass), THEN
    scope_items (v1.22.68 degraded >499 path, cooldown kept), THEN
    the default cooldown branch."""
    src = API_PY.read_text()
    fn_start = src.index("def _bulk_probe_tdb_run(")
    fn_end = src.index("\ndef ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    # The SQL filter section (not the scope_label section at the
    # top) is identifiable by the v1.15.10 marker comment.
    sql_section_anchor = fn_body.index(
        "# v1.15.10: scope_failures_only swaps the cooldown filter")
    sql_section = fn_body[sql_section_anchor:sql_section_anchor + 8000]
    failures_idx = sql_section.index("if scope_failures_only:")
    scope_clause_idx = sql_section.index("elif scope_clause:")
    scope_items_idx = sql_section.index("elif scope_items:")
    cooldown_idx = sql_section.index("else:", scope_items_idx)
    assert failures_idx < scope_clause_idx < scope_items_idx < cooldown_idx, (
        "v1.15.20/v1.22.68: branch order must be failures-only → "
        "scope-clause (bypass) → scope-items (degraded, cooldown "
        "kept) → cooldown (default)"
    )


def test_total_zero_early_return_logs_event():
    """The total=0 early-return must log an INFO event so the
    operator sees the result in /queue events + docker logs.
    Pre-fix the early-return was silent — no signal that the
    run completed."""
    src = API_PY.read_text()
    fn_start = src.index("def _bulk_probe_tdb_run(")
    fn_end = src.index("\ndef ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    early_return_anchor = fn_body.index("if total == 0:")
    block = fn_body[early_return_anchor:early_return_anchor + 1500]
    # Must call log_event before finish_progress.
    assert "log_event(" in block, (
        "v1.15.20: total=0 early-return must log an INFO event"
    )
    assert 'level="INFO"' in block
    assert "nothing to do" in block, (
        "v1.15.20: log message should signal the no-op clearly"
    )


def test_total_zero_log_event_precedes_finish_progress():
    """Order: log first, then finish_progress. A finish_progress
    that fires before the log_event would close the LIVE OPS
    card before the activity update lands."""
    src = API_PY.read_text()
    fn_start = src.index("def _bulk_probe_tdb_run(")
    fn_end = src.index("\ndef ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    early_return_anchor = fn_body.index("if total == 0:")
    block = fn_body[early_return_anchor:early_return_anchor + 1500]
    log_idx = block.index("log_event(")
    finish_idx = block.index("op_progress.finish_progress(")
    assert log_idx < finish_idx, (
        "v1.15.20: log_event must precede finish_progress in "
        "the early-return block"
    )


def test_cooldown_branch_unchanged_for_unscoped_default():
    """The default (no scope_items, no scope_failures_only)
    branch must STILL apply the cooldown filter. This is the
    PROBE TDB URLS button's normal flow — periodic verification
    sweeps should keep their guard against re-probing recently-
    probed rows."""
    src = API_PY.read_text()
    fn_start = src.index("def _bulk_probe_tdb_run(")
    fn_end = src.index("\ndef ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    # Find the default else branch.
    sql_section_anchor = fn_body.index(
        "# v1.15.10: scope_failures_only swaps the cooldown filter")
    sql_section = fn_body[sql_section_anchor:sql_section_anchor + 8000]
    cooldown_else_idx = sql_section.index(
        "else:", sql_section.index("elif scope_items:"))
    cooldown_block = sql_section[cooldown_else_idx:cooldown_else_idx + 800]
    assert "BULK_PROBE_COOLDOWN_HOURS" in cooldown_block
    assert "last_probed_at" in cooldown_block
    assert "(24h cooldown applied)" in cooldown_block
