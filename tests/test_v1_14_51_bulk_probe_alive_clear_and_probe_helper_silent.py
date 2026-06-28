"""v1.14.51 — bulk-probe alive-clear parity + probe-helper silent-failure surfacing.

Audit-driven Bundle A. Two findings from the v1.14.50 holistic
audit:

## H2: bulk-probe didn't clear stale failure on alive

v1.14.49 added the alive-clear to the SINGLE-row /probe-tdb path
(failure_kind/message/at/acked_at = NULL + sfa DELETE when probe
returns alive on a row with a stale failure). The bulk-probe twin
at `_bulk_probe_tdb_run` only stamped `last_probed_at` and
`n_alive += 1` — leaving operators in a worse spot than the
single-row caller, since the bulk path is exactly the one they
run to RECONCILE the library after batch URL changes upstream.

Mirror-principle drift (CLAUDE.md class P): the same probe-alive
state should produce the same DB write regardless of which
endpoint triggers it.

Fix: bulk path now batches alive-clear writes into a third
`pending_alive` list, flushed inside the same per-batch
transaction. Counts surface as `alive=N (cleared=M)` in both the
op_progress activity line + the final INFO log line so operators
see how much state actually changed.

## M3: probe helpers silently proceeded on probe API failure

Both `_probeAndConfirmLPSAtTopLevel` (top-level v1.14.47 helper)
and `_probeAndConfirmLetPlexServe` (inner v1.14.28 helper) had
`catch (_) { /* probe failed — proceed without warning */ }` —
silently rendering the confirm dialog with NO probe wording. User
sees the dialog with no indication probe didn't run, clicks OK
assuming the empty preamble means ✓ alive. Violates the user's
v1.14.28 stated safety-net intent: "before I let Plex take over,
confirm the recovery path back is intact".

Fix: surface the API failure inline as `? TDB URL probe FAILED to
run (...). Couldn't verify recovery path.` so the user can decide
whether to proceed blind.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── H2: bulk-probe alive-clear parity ────────────────────────


def test_bulk_probe_has_pending_alive_batch():
    """The `_bulk_probe_tdb_run` flush must accumulate alive-
    clears in a `pending_alive` list, mirroring the existing
    `pending_dead` shape but for the inverse case."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("def _bulk_probe_tdb_run(")
    # v1.22.68: slice to the function's actual end — the fixed 25000
    # window was widened in v1.15.1/v1.15.11 and silently went stale
    # again when the v1.22.68 branch comments grew the body past it.
    body = src[fn_anchor:src.index("\ndef ", fn_anchor + 1)]
    # The new batch list declaration.
    assert "pending_alive: list[tuple] = []" in body
    # Marker pin.
    assert "v1.14.51: alive batch" in body


def test_bulk_probe_alive_branch_enqueues_to_pending_alive():
    """The alive branch (`elif result is None:`) must push the
    (mt, tid) tuple onto pending_alive in addition to the
    pre-existing `n_alive += 1` counter bump."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("def _bulk_probe_tdb_run(")
    # v1.22.68: slice to the function's actual end — the fixed 25000
    # window was widened in v1.15.1/v1.15.11 and silently went stale
    # again when the v1.22.68 branch comments grew the body past it.
    body = src[fn_anchor:src.index("\ndef ", fn_anchor + 1)]
    branch_anchor = body.index("elif result is None:")
    block = body[branch_anchor:branch_anchor + 800]
    assert "n_alive += 1" in block
    assert "pending_alive.append((mt, tid))" in block


def test_bulk_probe_flush_runs_alive_clear_writes():
    """The `_flush_batch` inner function must execute the
    failure-clear writes for every entry in pending_alive,
    matching the v1.14.49 single-row endpoint shape (UPDATE
    themes ... WHERE failure_kind IS NOT NULL + DELETE FROM
    section_failure_acks gated on rowcount > 0)."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("def _bulk_probe_tdb_run(")
    # v1.22.68: slice to the function's actual end — the fixed 25000
    # window was widened in v1.15.1/v1.15.11 and silently went stale
    # again when the v1.22.68 branch comments grew the body past it.
    body = src[fn_anchor:src.index("\ndef ", fn_anchor + 1)]
    flush_anchor = body.index("def _flush_batch():")
    block = body[flush_anchor:flush_anchor + 4000]
    # The conditional flush block exists.
    assert "if pending_alive:" in block
    # The clear UPDATE.
    assert "SET failure_kind = NULL" in block
    # The same WHERE failure_kind IS NOT NULL guard the single-row
    # path uses (avoids wasted writes on already-clean rows).
    assert "AND failure_kind IS NOT NULL" in block
    # The conditional sfa DELETE gated on rowcount.
    assert "if cur.rowcount > 0:" in block
    assert "DELETE FROM section_failure_acks" in block


def test_bulk_probe_n_alive_cleared_counter_surfaces():
    """The new `n_alive_cleared` counter must:
      • be initialised at the top of the function
      • be `nonlocal` inside `_flush_batch` so the per-flush
        rowcount transitions accumulate
      • surface in both the op_progress activity line + the
        final INFO log message so operators see the count"""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("def _bulk_probe_tdb_run(")
    # v1.22.68: slice to the function's actual end — the fixed 25000
    # window was widened in v1.15.1/v1.15.11 and silently went stale
    # again when the v1.22.68 branch comments grew the body past it.
    body = src[fn_anchor:src.index("\ndef ", fn_anchor + 1)]
    # Counter init.
    assert "n_alive_cleared = 0" in body
    # nonlocal declaration inside _flush_batch.
    assert "nonlocal n_alive_cleared" in body
    # Activity line includes the cleared count.
    assert "(cleared={n_alive_cleared})" in body


def test_bulk_probe_batch_size_check_includes_pending_alive():
    """The flush trigger must size against all three pending
    lists. Pre-fix the check was `len(pending_stamps) +
    len(pending_dead)` — adding a third list silently grows the
    batch to 1.5x BATCH_SIZE on alive-heavy sweeps, which on a
    big library spikes a single tx to hundreds of writes."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("def _bulk_probe_tdb_run(")
    # v1.22.68: slice to the function's actual end — the fixed 25000
    # window was widened in v1.15.1/v1.15.11 and silently went stale
    # again when the v1.22.68 branch comments grew the body past it.
    body = src[fn_anchor:src.index("\ndef ", fn_anchor + 1)]
    assert "+ len(pending_alive)) >= BATCH_SIZE" in body


# ── M3: probe helpers surface API-call failures ──────────────


def test_top_level_probe_helper_surfaces_api_failure():
    """The top-level `_probeAndConfirmLPSAtTopLevel` helper (the
    v1.14.47 SOURCE-menu twin) must surface a probe API failure
    inline in the confirm dialog. Pre-v1.14.51 a thrown error
    in `await api(...)` was silently swallowed → user clicked
    OK assuming the empty preamble meant ✓ alive."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    fn_anchor = js.index("async function _probeAndConfirmLPSAtTopLevel(")
    body = js[fn_anchor:fn_anchor + 3000]
    # The catch block now sets probeWarning to a "probe FAILED"
    # explanation referencing the error message.
    assert "catch (e)" in body
    assert "TDB URL probe FAILED to run" in body
    assert "Couldn't verify recovery path" in body
    # The pre-fix silent-swallow comment must NOT survive.
    assert "/* probe failed — proceed without warning */" not in body
    # The v1.14.51 marker pins the rationale.
    assert "v1.14.51:" in body


def test_inner_probe_helper_surfaces_api_failure():
    """Same fix on the inner `_probeAndConfirmLetPlexServe`
    helper (v1.14.28 / failure-flow purge-and-ack). Both helpers
    fed the same silent-failure shape; both get the same fix."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    fn_anchor = js.index("async function _probeAndConfirmLetPlexServe(")
    body = js[fn_anchor:fn_anchor + 3000]
    assert "catch (e)" in body
    assert "TDB URL probe FAILED to run" in body
    assert "Couldn't verify recovery path" in body
    assert "/* probe failed — proceed without warning */" not in body
