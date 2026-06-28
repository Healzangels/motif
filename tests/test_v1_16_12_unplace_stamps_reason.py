"""v1.16.12 — unplace stamps last_place_attempt_reason='plex_has_theme'.

the user did a bulk ADOPT + LET PLEX SERVE on 121 M+P rows and then,
on the next hourly retry sweep, saw 121 `Skipped placement:
plex_has_theme` INFO lines in /queue logs. The lines themselves
weren't wrong — Plex did have its own theme; the place worker
correctly skipped — but the user reading the log just performed
the ACTION that puts those rows into Plex-serves state. Seeing
the system "rediscover" that fact 60 minutes later, 121 times, is
confusing noise.

Root cause: the unplace path (per-row `api_unplace_item` and the
bulk `_bulk_lps_run` UNPLACE stage) deleted the placement row and
updated plex_items, but never touched
`local_files.last_place_attempt_reason`. The retry sweep's
v1.13.76 filter at scheduler.py:107-109 skips rows where that
field equals `'plex_has_theme'` or LIKE `'existing_theme:%'`, but
post-LPS rows carried the prior `'placed'` value (set by the
successful placement before LPS). The sweep re-enqueued, the
place worker ran, hit Plex's theme, stamped `'plex_has_theme'`
itself, then logged the INFO line.

Fix: stamp `last_place_attempt_reason = 'backup_only'` at
unplace time so the sweep skips the row on its next pass. A
subsequent PUSH TO PLEX (the legit re-place path) writes
`'placed'` via the worker's existing outcome-stamp logic, so the
fix doesn't permanently lock rows out of placement.

Two sites need the stamp:

* `api_unplace_item` (api.py around line 10851) — per-row UNPLACE
  endpoint, also the target of the per-row INFO card flow.
* `_bulk_lps_run` UNPLACE stage (api.py around line 3380) —
  server-side bulk LPS handler.

Both stamps live inside the same `transaction(conn)` block that
deletes the placements row so the field never lags behind the
delete.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"


# ── 1. api_unplace_item stamps the reason ─────────────────────


def test_api_unplace_item_stamps_last_place_attempt_reason():
    """The per-row UNPLACE endpoint must update
    `local_files.last_place_attempt_reason = 'backup_only'`
    inside the same transaction that deletes the placements row.
    Without it the v1.13.76 retry sweep filter doesn't skip the
    row on its next pass."""
    src = API_PY.read_text()
    fn_anchor = src.index("async def api_unplace_item(")
    fn_end = src.index("\n    @app.", fn_anchor + 1)
    body = src[fn_anchor:fn_end]
    # The stamp UPDATEs are scoped two ways (section-scoped +
    # title-global fallback). Both must exist + both must set the
    # reason to 'plex_has_theme'.
    assert "UPDATE local_files SET last_place_attempt_at = ?, " in body, (
        "v1.16.12: api_unplace_item must stamp last_place_attempt_at + "
        "last_place_attempt_reason. Section-scoped + title-global "
        "branches both need the UPDATE."
    )
    assert "last_place_attempt_reason = 'backup_only'" in body, (
        "v1.16.12: the stamp must write the literal 'plex_has_theme' "
        "string — the v1.13.76 retry-sweep filter compares against "
        "this exact value (scheduler.py:108)."
    )


def test_api_unplace_item_stamps_inside_transaction():
    """The stamp must run inside the same `transaction(conn)`
    block as the placements DELETE, so a concurrent reader (or a
    crash mid-write) can't see a deleted-placement-with-stale-
    reason state."""
    src = API_PY.read_text()
    fn_anchor = src.index("async def api_unplace_item(")
    fn_end = src.index("\n    @app.", fn_anchor + 1)
    body = src[fn_anchor:fn_end]
    # The transaction block opens with `with ... transaction(conn):`
    # somewhere inside the function.
    txn_start = body.index("with get_conn(db) as conn, transaction(conn):")
    # Find the next `with get_conn` (next transaction) OR the end of
    # the function — whichever comes first — to bound the search.
    after_txn = body[txn_start:]
    next_with = after_txn.find("with get_conn(db) as conn", 1)
    txn_block = after_txn if next_with == -1 else after_txn[:next_with]
    assert "DELETE FROM placements" in txn_block
    assert "last_place_attempt_reason = 'backup_only'" in txn_block, (
        "v1.16.12: the stamp UPDATE must be inside the same "
        "transaction(conn) block as the placements DELETE — "
        "otherwise concurrent reads can observe inconsistent state."
    )


# ── 2. _bulk_lps_run mirrors the stamp ────────────────────────


def test_bulk_lps_unplace_stage_stamps_last_place_attempt_reason():
    """The bulk-LPS UNPLACE stage must mirror api_unplace_item's
    stamp — otherwise a bulk LPS of 100+ rows produces the exact
    log-spam the user flagged."""
    src = API_PY.read_text()
    fn_anchor = src.index("def _bulk_lps_run(")
    # The function is long; bound by the next top-level `def`.
    fn_end = src.index("\ndef ", fn_anchor + 1)
    body = src[fn_anchor:fn_end]
    assert "UNPLACE stage" in body, (
        "anchor sanity: _bulk_lps_run should still have the "
        "UNPLACE stage comment"
    )
    # Find the UNPLACE stage marker and bound the stamp search to
    # that region (the function has a separate PROBE stage above).
    unplace_idx = body.index("UNPLACE stage")
    unplace_region = body[unplace_idx:]
    assert "UPDATE local_files SET" in unplace_region
    assert "last_place_attempt_reason = 'backup_only'" in unplace_region, (
        "v1.16.12: _bulk_lps_run UNPLACE stage must stamp "
        "last_place_attempt_reason = 'backup_only' inside the "
        "same transaction as the placements DELETE"
    )


def test_bulk_lps_stamp_inside_unplace_transaction():
    """The bulk-LPS stamp must run inside the per-target
    transaction block in the UNPLACE stage (so a partial write
    isn't observable to a concurrent reader). _bulk_lps_run has
    an earlier PROBE-stage batch transaction; this test anchors
    on the UNPLACE-stage marker to avoid picking up the wrong
    transaction block."""
    src = API_PY.read_text()
    fn_anchor = src.index("def _bulk_lps_run(")
    fn_end = src.index("\ndef ", fn_anchor + 1)
    body = src[fn_anchor:fn_end]
    unplace_idx = body.index("# ── UNPLACE stage")
    unplace_region = body[unplace_idx:]
    # Find the per-target transaction(conn) inside the UNPLACE stage.
    txn_start = unplace_region.index("with transaction(conn):")
    after = unplace_region[txn_start:]
    # Bound the search to the per-target block — the next
    # `with get_conn(db_path) as conn` opens a new iteration or
    # the post-loop bookkeeping.
    next_block = after.find("with get_conn(db_path) as conn", 1)
    region = after if next_block == -1 else after[:next_block]
    assert "DELETE FROM placements" in region
    assert "last_place_attempt_reason = 'backup_only'" in region


# ── 3. v1.13.76 retry-sweep filter contract preserved ─────────


def test_retry_sweep_still_skips_plex_has_theme():
    """The v1.13.76 filter is what makes the stamp useful. Pin
    the filter shape so a future scheduler refactor doesn't
    silently change which `last_place_attempt_reason` values get
    re-enqueued."""
    sched = (REPO / "app" / "core" / "scheduler.py").read_text()
    fn_anchor = sched.index("def _retry_pending_placements(")
    fn_end = sched.index("\ndef ", fn_anchor + 1)
    body = sched[fn_anchor:fn_end]
    assert "lf.last_place_attempt_reason != 'plex_has_theme'" in body, (
        "v1.13.76 / v1.16.12: the retry sweep must continue to "
        "skip rows where last_place_attempt_reason='plex_has_theme' "
        "— that's the contract the unplace stamp relies on."
    )
    assert "lf.last_place_attempt_reason NOT LIKE 'existing_theme:%'" in body
