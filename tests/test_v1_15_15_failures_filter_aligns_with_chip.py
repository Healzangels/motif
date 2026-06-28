"""v1.15.15 — REPROBE FAILURES filter aligns with the FAIL chip predicate.

the user v1.15.14 repro: FAIL chip showed 1301 but REPROBE FAILURES
LIVE OPS card said "Found 1772 URL(s) to probe (failures only,
cooldown bypassed)". 471 row gap — the bulk probe was targeting
rows the user couldn't see counted as failures.

## Root cause

The bulk-probe failures-only filter was just:

    AND t.failure_kind IS NOT NULL

The FAIL chip uses _FAILURES_SFA_FROM_SQL +
_FAILURES_SFA_WHERE_SQL (api.py:660+) which adds:

  - JOIN plex_items (excludes themes orphaned by Plex item
    deletions — motif's themes table outlives the source item)
  - JOIN plex_sections.included = 1 (excludes themes whose
    only sections are in the user's exclude list)
  - LEFT JOIN sfa + WHERE sfa.acked_at IS NULL (excludes per-
    section ack'd failures)
  - WHERE failure_acked_at IS NULL (excludes title-globally
    ack'd failures)

So `failure_kind IS NOT NULL` includes orphans, excluded
sections, and acked rows; the FAIL chip excludes them all.

## Fix

Add an EXISTS subquery + the title-ack filter to the
scope_failures_only branch. The EXISTS form keeps the SELECT
per-theme (one probe per URL even if it fails in multiple
sections) — using the canonical JOIN form would multiply
themes by their visible-section count, double-probing the
same URL.

After the fix, the probe count should be ≤ FAIL chip count
(slightly less because the FAIL chip counts (theme, section)
pairs while the probe loop dedupes per theme).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"


def test_failures_only_filter_excludes_orphans():
    """The failures-only branch must JOIN-filter on plex_items
    so themes orphaned by Plex item deletions don't get
    re-probed (their results are wasted — clearing failure_kind
    on an orphan theme doesn't help anyone)."""
    src = API_PY.read_text()
    fn_start = src.index("def _bulk_probe_tdb_run(")
    fn_end = src.index("\ndef ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    # The failures-only SQL must contain a plex_items reference.
    # Anchor on the SQL-side failures-only branch (the second
    # `if scope_failures_only:` inside this function — the first
    # is the scope_label setup at the top).
    sql_anchor = fn_body.index("# v1.15.15: align the failures-only filter")
    failures_branch_anchor = fn_body.index(
        "if scope_failures_only:", sql_anchor)
    branch_block = fn_body[failures_branch_anchor:failures_branch_anchor + 2500]
    assert "plex_items" in branch_block, (
        "v1.15.15: failures-only filter must JOIN/EXISTS plex_items "
        "to exclude orphans (matches the FAIL chip predicate)"
    )


def test_failures_only_filter_excludes_disabled_sections():
    """The failures-only branch must filter via plex_sections.
    included = 1 so themes in excluded sections don't get
    re-probed (the user opted them out via section-exclude
    config)."""
    src = API_PY.read_text()
    fn_start = src.index("def _bulk_probe_tdb_run(")
    fn_end = src.index("\ndef ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    # Anchor on the SQL-side failures-only branch (the second
    # `if scope_failures_only:` inside this function — the first
    # is the scope_label setup at the top).
    sql_anchor = fn_body.index("# v1.15.15: align the failures-only filter")
    failures_branch_anchor = fn_body.index(
        "if scope_failures_only:", sql_anchor)
    branch_block = fn_body[failures_branch_anchor:failures_branch_anchor + 2500]
    assert "ps.included = 1" in branch_block, (
        "v1.15.15: failures-only filter must restrict to included "
        "sections (matches the FAIL chip predicate)"
    )


def test_failures_only_filter_no_longer_excludes_acked_rows():
    """v1.15.15 narrowed REPROBE FAILURES to match the FAIL chip
    count (excluding title-globally-acked + per-section-acked
    rows). v1.15.33 BROADENED back: the user wants every red-pill
    TDB row re-probed regardless of ack state, because an acked
    failure may be stale and re-probing might clear it. The
    title-ack + sfa-ack predicates must be GONE from the
    failures-only branch."""
    src = API_PY.read_text()
    fn_start = src.index("def _bulk_probe_tdb_run(")
    fn_end = src.index("\ndef ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    sql_anchor = fn_body.index("# v1.15.15: align the failures-only filter")
    failures_branch_anchor = fn_body.index(
        "if scope_failures_only:", sql_anchor)
    branch_block = fn_body[failures_branch_anchor:failures_branch_anchor + 2500]
    # The visibility filter (plex_items + included sections) is
    # preserved — see the other v1.15.15 tests in this file.
    # Only the ack predicates were dropped.
    assert "t.failure_acked_at IS NULL" not in branch_block, (
        "v1.15.33: title-ack predicate must be dropped — the user "
        "wants acked failures re-probed too"
    )
    assert "sfa.acked_at IS NULL" not in branch_block, (
        "v1.15.33: per-section-ack predicate must be dropped"
    )


def test_failures_only_uses_exists_to_stay_per_theme():
    """The visibility/ack filters must use an EXISTS subquery
    (not a top-level JOIN) so the SELECT stays per-theme. A
    JOIN form would multiply themes by their visible-section
    count, causing the probe loop to hit the same YouTube URL
    multiple times — exactly the rate-limit hammering v1.15.14
    is trying to avoid."""
    src = API_PY.read_text()
    fn_start = src.index("def _bulk_probe_tdb_run(")
    fn_end = src.index("\ndef ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    # Anchor on the SQL-side failures-only branch (the second
    # `if scope_failures_only:` inside this function — the first
    # is the scope_label setup at the top).
    sql_anchor = fn_body.index("# v1.15.15: align the failures-only filter")
    failures_branch_anchor = fn_body.index(
        "if scope_failures_only:", sql_anchor)
    branch_block = fn_body[failures_branch_anchor:failures_branch_anchor + 2500]
    assert "EXISTS (" in branch_block, (
        "v1.15.15: visibility/ack predicates must be wrapped in "
        "EXISTS so the SELECT stays per-theme (one probe per URL)"
    )


def test_canonical_fail_predicate_constants_still_present():
    """Sanity guard: the canonical _FAILURES_SFA_FROM_SQL +
    _FAILURES_SFA_WHERE_SQL constants must still exist so any
    future surface that needs the FAIL predicate uses the same
    shape. The bulk-probe v1.15.15 fix mirrors them in EXISTS
    form rather than re-using them directly (the bulk SELECT
    needs per-theme dedup; the canonical predicates are
    per-(theme, section) for COUNT use)."""
    src = API_PY.read_text()
    assert "_FAILURES_SFA_FROM_SQL = " in src
    assert "_FAILURES_SFA_WHERE_SQL = " in src
    # And they must still be used by the per-section stats path —
    # the v1.14.30 mirror-principle fix that the bulk-probe
    # filter is now aligned with.
    assert "_FAILURES_SFA_FROM_SQL}" in src or "_FAILURES_SFA_FROM_SQL )" in src


def test_unscoped_branch_unchanged():
    """The default (cooldown-applied) bulk PROBE TDB URLS branch
    must NOT pick up the new visibility/ack filters — its
    semantics are "probe everything that needs a refresh", not
    "probe currently-visible failures". Pin the cooldown-only
    SQL shape so a future refactor can't accidentally collapse
    the two branches."""
    src = API_PY.read_text()
    fn_start = src.index("def _bulk_probe_tdb_run(")
    fn_end = src.index("\ndef ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    # Find the else branch (cooldown).
    # Anchor on the SQL-side failures-only branch (the second
    # `if scope_failures_only:` inside this function — the first
    # is the scope_label setup at the top).
    sql_anchor = fn_body.index("# v1.15.15: align the failures-only filter")
    failures_branch_anchor = fn_body.index(
        "if scope_failures_only:", sql_anchor)
    else_anchor = fn_body.index("else:", failures_branch_anchor)
    cooldown_block = fn_body[else_anchor:else_anchor + 800]
    # The cooldown branch references BULK_PROBE_COOLDOWN_HOURS +
    # last_probed_at; should NOT mention plex_items or sfa.
    assert "BULK_PROBE_COOLDOWN_HOURS" in cooldown_block
    assert "last_probed_at" in cooldown_block
    assert "plex_items" not in cooldown_block, (
        "Cooldown branch must NOT inherit the failures-only "
        "visibility filter"
    )


def test_activity_label_reflects_v1_15_33_broadening():
    """v1.15.15 used "(visible failures only, cooldown bypassed)"
    to convey the FAIL-chip alignment. v1.15.33 broadened the
    scope to all red-pill failures (acked + unacked); the label
    must now reflect that — pin the new "all red-pill failures"
    phrasing so a docker-log post-mortem can tell the v1.15.33+
    runs apart from older ones."""
    src = API_PY.read_text()
    fn_start = src.index("def _bulk_probe_tdb_run(")
    fn_end = src.index("\ndef ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    assert "(visible failures only" not in fn_body, (
        "v1.15.33: the v1.15.15 'visible failures only' label "
        "must be dropped — semantics changed"
    )
    assert "(all red-pill failures" in fn_body, (
        "v1.15.33: activity label must convey that all "
        "failure_kind-bearing rows are re-probed (acked + "
        "unacked)"
    )
