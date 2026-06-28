"""v1.13.89 — REPROBE PLEX THEMES surfaces individual error reasons.

the user's drawer showed "// REPROBE PLEX THEMES — Done — 1,975 items
processed — ERRORS 2" with no way to know what those 2 errors were.
The summary log line ("errors=N") aggregated the count but the
individual reasons were captured and discarded.

Looking at _reprobe_plex_themes_run (api.py:2126):
- _probe_one returns (rk, verdict, err) tuple
- Caller did `rk, verdict, err = fut.result()` — used verdict, ignored err
- err can be one of:
  - "sidecar missing"
  - "sidecar empty"
  - "plex range-GET failed"
  - "empty response"
  - "probe error: <exception>"

Fix: log each individual error at WARNING level with the rating_key
and reason. Operator can now see exactly what failed in the LOGS
event stream.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


def test_reprobe_logs_individual_error_reason():
    """Pin the v1.13.89 error logging. The reprobe loop must call
    log_event(level='WARNING', component='reprobe', ...) for each
    error verdict — pre-fix the err string was captured into the
    tuple but never used. Operator only saw the aggregate count."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    # Find the _reprobe_plex_themes_run function.
    anchor = src.index("def _reprobe_plex_themes_run(")
    # The error-handling block is in the verdict==None branch, which
    # is part of the as_completed loop. v1.22.82: slice to the
    # function's actual end — the fixed 12000 window went stale when
    # the per-thread client pool grew the body.
    block = src[anchor:src.index("\ndef ", anchor + 1)]
    # Must call log_event with WARNING + reprobe component.
    assert 'level="WARNING", component="reprobe"' in block, (
        "v1.13.89: each error verdict must log_event WARNING so "
        "the operator can see individual reasons in the events log"
    )
    # The message must include the rating_key and the err reason.
    assert 'message=f"REPROBE error rk=' in block
    # The detail dict must include both rating_key and reason
    # for structured queryability.
    assert '"rating_key": rk' in block
    assert '"reason": err or "unknown"' in block


def test_reprobe_summary_log_still_includes_aggregate_count():
    """Regression guard: the v1.13.50 summary log
    (REPROBE done: N probed, ..., errors=K) must still fire so
    the drawer's `// REPROBE PLEX THEMES — ERRORS K` aggregation
    keeps working alongside the new per-error WARNING lines."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    assert 'f"errors={error_count}"' in src
    # The summary stays at INFO level (operator sees the count
    # in normal flow). Per-error logs are WARNING (separable
    # via /logs?level=WARNING).
    summary_anchor = src.index('f"errors={error_count}"')
    summary_block = src[summary_anchor - 500:summary_anchor + 100]
    assert 'level="INFO"' in summary_block
