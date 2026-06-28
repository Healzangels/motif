"""v1.15.14 — drop probe concurrency to 1 + early-bail on rate-limit.

the user v1.15.12 verify-run results (after the classifier fix):

    BULK PROBE TDB done: 2127 probed, alive=355 (cleared=355),
    dead=238, indeterminate=0, other=1534, errors=0

The classifier fix worked — 1534 rate-limited probes correctly
went to "other" (no failure_kind write), down from the v1.15.11
run's 2005 false-dead. But concurrency=2 STILL crossed YouTube's
throttle threshold ~10% into the run, so 72% of probes (1534 /
2127) returned no useful verdict. Operator wasted ~17 minutes
on doomed probes.

## Fix (two parts)

1. **Pool size 2 → 1**. v1.15.12 already dropped 3 → 2 but
   that wasn't enough. 1 worker averages ~1 probe/s which
   should sit under YouTube's threshold even on cookied
   requests. If 1 worker still rate-limits, the bail logic
   (#2) bounds the wasted-time cost.

2. **Early-bail on consecutive transients**. Track count of
   transient results (NETWORK_ERROR / UNKNOWN / COOKIES_EXPIRED
   / probe-error exception). At
   BULK_PROBE_RATE_LIMIT_BAIL_THRESHOLD (15) consecutive
   transients, end the run with a clear "likely rate-limited,
   retry in ~1h" message. YouTube's throttle persists "up to
   an hour" so continuing past the threshold burns the throttle
   timer for nothing — better to bail and let the operator
   wait it out.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"


def test_bulk_probe_max_workers_dropped_to_one():
    """Pin the v1.15.14 pool-size cap. The v1.15.12 → v1.15.14
    progression (3 → 2 → 1) was driven by repeated rate-limit
    triggers on the user's deployment. If a future change bumps
    this back up, the bail logic only catches the symptom — the
    actual cause (workers crossing YouTube's throughput
    threshold) returns."""
    src = API_PY.read_text()
    assert "BULK_PROBE_MAX_WORKERS = 1" in src
    # Defensive guard against any value > 1 sneaking back.
    for n in (2, 3, 4, 5, 6):
        assert f"BULK_PROBE_MAX_WORKERS = {n}" not in src


def test_rate_limit_bail_threshold_constant_defined():
    """The bail threshold lives at module scope (not buried in
    the function body) so it's grep-able + tunable. 15 is the
    documented default — generous enough to not bail on a brief
    blip in a 2000-row sweep, tight enough to catch a sustained
    throttle within ~15s."""
    src = API_PY.read_text()
    assert "BULK_PROBE_RATE_LIMIT_BAIL_THRESHOLD = 15" in src, (
        "v1.15.14: bail threshold constant must live at module "
        "scope so it's tunable + visible in the bulk-probe header"
    )


def test_bulk_probe_run_has_consecutive_transient_counter():
    """The probe loop must maintain a counter of consecutive
    transient results, reset by any definitive verdict (alive
    or dead). Pin both the increment and the reset paths."""
    src = API_PY.read_text()
    fn_start = src.index("def _bulk_probe_tdb_run(")
    fn_end = src.index("\ndef ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    assert "consecutive_transient = 0" in fn_body, (
        "Counter must be initialized before the loop"
    )
    assert "consecutive_transient += 1" in fn_body, (
        "Counter must increment on transient results"
    )
    # Reset path on definitive results.
    assert "consecutive_transient = 0" in fn_body
    # The threshold check.
    assert "consecutive_transient >= BULK_PROBE_RATE_LIMIT_BAIL_THRESHOLD" in fn_body, (
        "Bail must trigger when counter exceeds threshold"
    )


def test_bail_path_writes_clear_activity_message():
    """When the bail fires, the activity message must name the
    likely cause (rate-limit) AND the recovery action (wait ~1h
    and retry) so the operator sees both in the LIVE OPS card
    without checking docker logs."""
    src = API_PY.read_text()
    fn_start = src.index("def _bulk_probe_tdb_run(")
    fn_end = src.index("\ndef ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    # The activity update on bail must mention rate-limit + 1h wait.
    assert "Bailed at" in fn_body
    assert "rate-limited" in fn_body.lower()
    assert "~1h" in fn_body or "1 h" in fn_body, (
        "Activity message must surface the recovery hint (~1h)"
    )


def test_bail_path_logs_warning_event():
    """In addition to the activity update, the bail must log a
    WARNING event so the events feed shows it. Operators reading
    /queue events should see the bail without scrolling through
    every per-row probe-error WARNING."""
    src = API_PY.read_text()
    fn_start = src.index("def _bulk_probe_tdb_run(")
    fn_end = src.index("\ndef ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    assert 'level="WARNING"' in fn_body
    # Look for the bail-specific log message.
    assert "BULK PROBE TDB bailing at" in fn_body or "bailed early" in fn_body.lower(), (
        "Bail event must include a recognizable message marker"
    )


def test_bail_path_shuts_pool_down_promptly():
    """The bail must cancel pending futures so workers stop
    issuing fresh probes. Otherwise the pool keeps draining
    its queue and burns the throttle timer for another N probes."""
    src = API_PY.read_text()
    fn_start = src.index("def _bulk_probe_tdb_run(")
    fn_end = src.index("\ndef ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    # The bail block must call pool.shutdown(cancel_futures=True).
    bail_anchor = fn_body.index(
        "consecutive_transient >= BULK_PROBE_RATE_LIMIT_BAIL_THRESHOLD")
    bail_block = fn_body[bail_anchor:bail_anchor + 1500]
    assert "pool.shutdown" in bail_block
    assert "cancel_futures=True" in bail_block, (
        "Bail must cancel pending futures so the pool stops "
        "submitting fresh probes after the threshold trips"
    )


def test_finish_log_includes_bail_suffix_when_bailed():
    """The final BULK PROBE TDB done log line must include a
    distinguishing suffix when the run bailed early — operators
    inspecting docker logs after the fact should see whether
    the run completed naturally or bailed on rate-limit."""
    src = API_PY.read_text()
    fn_start = src.index("def _bulk_probe_tdb_run(")
    fn_end = src.index("\ndef ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    assert "rate_limit_bail" in fn_body
    assert "bail_suffix" in fn_body, (
        "Final log line must conditionally include a bail suffix "
        "so docker-log readers can tell completed runs apart from "
        "early-bailed ones"
    )


def test_definitive_results_reset_consecutive_counter():
    """Alive and dead verdicts MUST reset the consecutive
    counter — otherwise a long stretch with mostly transients
    sprinkled with the occasional alive/dead would still trip
    the bail despite progress being made. Pin the reset path
    via the else branch on is_transient_result."""
    src = API_PY.read_text()
    fn_start = src.index("def _bulk_probe_tdb_run(")
    fn_end = src.index("\ndef ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    # The reset is in the `else` branch of the is_transient check.
    counter_anchor = fn_body.index("if is_transient_result:")
    counter_block = fn_body[counter_anchor:counter_anchor + 200]
    assert "consecutive_transient += 1" in counter_block
    assert "consecutive_transient = 0" in counter_block, (
        "Definitive results must reset the counter so steady "
        "progress prevents the bail from firing"
    )
