"""v1.15.33 — REPROBE FAILURES re-probes all red-pill TDB rows
(including acked failures), not just unacked failure rows.

## Why

the user:

> "also can we make it so reprobe failure reprobes all red pill
>  TDB not just unack failure rows"

## Pre-fix (v1.15.15)

REPROBE FAILURES filtered on:
  - failure_kind IS NOT NULL
  - failure_acked_at IS NULL  (excludes title-globally-acked)
  - sfa.acked_at IS NULL      (excludes per-section-acked)
  - EXISTS plex_items + included sections (visibility)

This matched the FAIL chip count exactly. But the per-row TDB
pill turns red on `failure_kind IS NOT NULL` regardless of ack
state — so the REPROBE button was missing a slice of red-pill
rows that the user could SEE on the dashboard.

## Post-fix (v1.15.33)

Drop the title-ack + per-section-ack predicates. Keep:
  - failure_kind IS NOT NULL  (canonical "this row is failed")
  - EXISTS plex_items + included sections (don't burn yt-dlp
    calls on themes for items no longer in any tracked library)

Net effect: an acked failure that's actually still broken stays
acked (the probe confirms dead → no state change other than
last_probed_at). An acked failure whose URL has come back alive
gets failure_kind cleared (and any stale sfa rows deleted by
the existing alive-clear pattern), which naturally drops it out
of the red-pill set. Either way, the user gets ground-truth.

The activity label changes from "(visible failures only,
cooldown bypassed)" to "(all red-pill failures, cooldown
bypassed)" so docker-log post-mortems can distinguish v1.15.33+
runs from older ones.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"
APP_JS = REPO / "app" / "web" / "static" / "app.js"


def _failures_branch_block() -> str:
    """Return the SQL-side `if scope_failures_only:` branch body
    inside `_bulk_probe_tdb_run`. Anchored on the v1.15.15
    marker comment that introduced the v1.15.15 → v1.15.33
    history of this filter."""
    src = API_PY.read_text()
    fn_start = src.index("def _bulk_probe_tdb_run(")
    fn_end = src.index("\ndef ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    sql_anchor = fn_body.index("# v1.15.15: align the failures-only filter")
    branch_anchor = fn_body.index("if scope_failures_only:", sql_anchor)
    elif_anchor = fn_body.index("elif scope_items:", branch_anchor)
    return fn_body[branch_anchor:elif_anchor]


# ── 1. SQL filter: ack predicates dropped ─────────────────────


def test_failures_branch_no_longer_filters_on_failure_acked_at():
    """The title-ack predicate `t.failure_acked_at IS NULL` from
    v1.15.15 must be GONE. the user wants acked failures re-probed
    too (a stale ack might be hiding a row whose URL has come
    back alive)."""
    block = _failures_branch_block()
    assert "t.failure_acked_at IS NULL" not in block, (
        "v1.15.33: the v1.15.15 title-ack filter was dropped — "
        "acked failures should be re-probed too"
    )


def test_failures_branch_no_longer_filters_on_sfa_ack():
    """The per-section-ack predicate `sfa.acked_at IS NULL` from
    v1.15.15 must be GONE. The LEFT JOIN section_failure_acks
    inside the EXISTS clause was dropped alongside (no longer
    needed)."""
    block = _failures_branch_block()
    assert "sfa.acked_at IS NULL" not in block, (
        "v1.15.33: the v1.15.15 per-section-ack filter was dropped"
    )
    # The LEFT JOIN sfa is no longer required either.
    assert "section_failure_acks" not in block, (
        "v1.15.33: the LEFT JOIN section_failure_acks inside the "
        "EXISTS clause is no longer needed (the only consumer "
        "was the dropped sfa.acked_at predicate)"
    )


def test_failures_branch_still_filters_on_failure_kind():
    """The canonical "this row is failed" predicate
    (failure_kind IS NOT NULL) must stay — that's THE definition
    of a red-pill TDB row."""
    block = _failures_branch_block()
    assert "t.failure_kind IS NOT NULL" in block


def test_failures_branch_still_filters_on_managed_plex_section():
    """Visibility filter preserved: the EXISTS clause must still
    require the row to be in a managed (included) Plex section
    so we don't burn yt-dlp calls on themes for items no longer
    in any tracked library. Pin both joins."""
    block = _failures_branch_block()
    assert "EXISTS (" in block
    assert "FROM plex_items pi" in block
    assert "JOIN plex_sections ps" in block
    assert "ps.included = 1" in block
    assert "pi.guid_tmdb = t.tmdb_id" in block


def test_failures_branch_still_uses_per_theme_select():
    """The per-theme dedup (one probe per URL even if the failure
    spans multiple sections) must survive. The EXISTS form
    naturally provides this — pin the EXISTS shape so a future
    refactor can't accidentally collapse to a JOIN that
    multiplies themes by section count."""
    block = _failures_branch_block()
    # No top-level JOIN — the visibility check sits inside an
    # EXISTS subquery. Double-check by counting EXISTS markers.
    assert block.count("EXISTS (") == 1


# ── 2. Activity label updated ─────────────────────────────────


def test_failures_only_activity_label_changed_to_all_red_pill():
    """The LIVE OPS activity label must convey the new scope
    ("all red-pill failures"), not the v1.15.15 "(visible
    failures only)" wording."""
    src = API_PY.read_text()
    fn_start = src.index("def _bulk_probe_tdb_run(")
    fn_end = src.index("\ndef ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    assert "(visible failures only" not in fn_body
    assert "(all red-pill failures, cooldown bypassed)" in fn_body


# ── 3. JS confirm prompt mentions the broadened scope ─────────


def test_js_reprobe_prompt_mentions_acked_inclusion():
    """the user's confirm dialog needs to communicate that this
    button now also re-probes acked failures — surprising users
    by silently broadening the scope is bad. Pin the prompt
    text so a future copy edit doesn't accidentally drop the
    'acked failures included' note."""
    src = APP_JS.read_text()
    fn_anchor = src.index("function bindReprobeTdbFailures()")
    fn_end = src.index("function ", fn_anchor + 1)
    fn_body = src[fn_anchor:fn_end]
    # The new prompt opens with "Reprobe every row with a red
    # TDB ✗ pill?" — anchored on the per-row visual signal,
    # not the chip count.
    assert "red TDB" in fn_body, (
        "v1.15.33: prompt must reference the per-row TDB ✗ pill "
        "(the canonical red-pill signal)"
    )
    # And explicitly call out acked-failure inclusion so the
    # user isn't surprised.
    assert "ACKED" in fn_body or "acked" in fn_body
    assert "v1.15.33" in fn_body, (
        "v1.15.33: prompt should carry the version marker so a "
        "future copy edit knows when the scope-change happened"
    )
