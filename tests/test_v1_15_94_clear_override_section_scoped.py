"""v1.15.94 — place outcome stamp logs at warning, not debug.

TRIMMED in v0.51.251: this file originally also pinned the section-scoping of
the `/api/items/{mt}/{id}/override` DELETE endpoint (finding #1 of the v1.15.94
pass). That endpoint pair was REMOVED in v0.51.251 — dead since the v1.19.87
dialog removal, no UI caller, zero API tokens on the operator install — so its
behavioural tests went with it (test_v0_51_251 pins the removal). The live
flow is clear-url, which has carried per-section scoping since v1.12.72.

What remains is finding #3 of the original pass, unrelated to the endpoint and
still load-bearing: worker.py wrapped the place-outcome bookkeeping in
`except Exception: log.debug(...)`. A failed bookkeeping UPDATE (DB lock,
schema mismatch) logged only at DEBUG — invisible in default-INFO production
logs — so motif's plex_items state stayed inconsistent with the place outcome
until manual intervention. Bumped to log.warning; this test keeps it there.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent


def test_place_outcome_stamp_logs_at_warning_not_debug():
    """The except wrapper around the place-outcome bookkeeping
    UPDATE must log at warning level. log.debug is invisible in
    production (default INFO) — a swallowed stamp failure leaves
    motif's plex_items state stale until manual intervention."""
    src = (REPO / "app" / "core" / "worker.py").read_text()
    # Anchor to the v1.15.91 stamp block (introduced w/ the
    # natural-key WHERE) then look at the except handler.
    anchor = src.index("# v1.11.24: stamp the place outcome")
    end = src.index('"place outcome stamp failed', anchor)
    span = src[anchor:end + 200]
    # The log call after "place outcome stamp failed:" must be
    # log.warning (or log.error), not log.debug.
    assert "log.warning(\"place outcome stamp failed:" in span, (
        "v1.15.94: place outcome stamp's except handler must log "
        "at warning level. log.debug hides production stamp "
        "failures and Unraid setups won't see plex_enum catch-up."
    )
    # Counter-guard: log.debug for this message must be gone.
    assert 'log.debug("place outcome stamp failed:' not in span, (
        "v1.15.94 regression: log.debug returned for the stamp "
        "exception. Switch back to log.warning."
    )
