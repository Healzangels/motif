"""v1.18.71 — atomic api→file Plex teardown for SWITCH + REPLACE.

Closes out the v1.18.68 audit's remaining MED-risk findings. Both
endpoints used to call `plex.delete_theme()` SYNCHRONOUSLY before
enqueueing the place job that creates the new sidecar. If the
place job's hardlink failed (cross-FS + disk-full + permissions),
the Plex API selection was already cleared — row themeless until
manual recovery.

v1.18.71 mirrors the v1.18.68 atomic pattern in reverse:
  - Endpoints collect per-section Plex rks into a dict, thread
    them through each place job's payload as `clear_plex_api_rks`.
  - Worker `_do_place` runs `plex.delete_theme(rk)` for each
    payload-listed rk ONLY after the sidecar place succeeds.
    Best-effort — a Plex API failure here doesn't fail the
    job (the sidecar already landed, the row is themed). The
    next plex_enum catches any stranded API entries.

The shared `_teardown_plex_api_artifacts_for_placements` helper
stays SYNCHRONOUS for its destructive callers (PURGE / FORGET /
DELETE) — no async work follows them so the v1.18.71 atomic
pattern doesn't apply.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


def _api_py() -> str:
    return (REPO / "app" / "web" / "api.py").read_text()


def _worker_py() -> str:
    return (REPO / "app" / "core" / "worker.py").read_text()


def _switch_body() -> str:
    src = _api_py()
    fn_idx = src.index("async def api_switch_placement(")
    fn_end = src.index("@app.post", fn_idx + 1)
    return src[fn_idx:fn_end]


def _replace_body() -> str:
    src = _api_py()
    fn_idx = src.index("async def api_replace_item(")
    fn_end = src.index("@app.post", fn_idx + 1)
    return src[fn_idx:fn_end]


def _do_place_body() -> str:
    src = _worker_py()
    fn_idx = src.index("    def _do_place(self,")
    fn_end = src.index("    def _do_place_collection(", fn_idx + 1)
    return src[fn_idx:fn_end]


# ── SWITCH PLACEMENT [api→file]: synchronous delete_theme gone ─


def test_switch_no_longer_calls_delete_theme_synchronously():
    """The endpoint must NOT invoke `plex.delete_theme()` directly.
    Pre-fix this fired BEFORE the async place job; a hardlink
    failure left the Plex API selection cleared with no recovery."""
    body = _switch_body()
    assert "plex.delete_theme(rating_key=rk)" not in body, (
        "v1.18.71: synchronous Plex delete_theme call must be "
        "removed from api_switch_placement; the worker handles "
        "the teardown post-hardlink"
    )
    # And the embedded PlexClient construction block that wrapped
    # the delete_theme call is gone too.
    assert "from ..core.plex import PlexClient, PlexConfig" not in body, (
        "v1.18.71: the endpoint no longer needs to spin up a "
        "PlexClient — the worker has one"
    )


def test_switch_collects_per_section_rks_into_payload():
    """The endpoint must build `clear_rks_per_section` and thread
    it via the place job payload."""
    body = _switch_body()
    assert "clear_rks_per_section" in body, (
        "v1.18.71: per-section rk collection required for "
        "the api→file atomic-teardown flow"
    )
    # The payload field name pinned so the worker can read it.
    assert "clear_plex_api_rks" in body, (
        "v1.18.71: payload field name must be clear_plex_api_rks "
        "so worker's _do_place reads it"
    )


def test_switch_gated_on_target_kind_file():
    """The rk collection must fire ONLY for target_kind='file'
    (the api→file direction). For target_kind='api' the v1.18.68
    sidecar-removal flow applies; the two payload fields are
    mutually exclusive on a given job."""
    body = _switch_body()
    # The collection gate.
    assert 'if target_kind == "file" and outgoing:' in body
    # The payload-binding gate.
    assert 'if target_kind == "file" else []' in body, (
        "v1.18.71: payload binding gated on target_kind='file'"
    )


def test_switch_v1_18_71_marker_present():
    """v1.18.71 archaeology marker required in the SWITCH
    endpoint."""
    body = _switch_body()
    assert "v1.18.71" in body
    # Repro / pattern reference.
    body_flat = " ".join(body.split())
    assert ("atomic-teardown pattern" in body_flat
            or "REVERSE for api→file" in body_flat
            or "v1.18.68 atomic pattern" in body_flat)


# ── REPLACE [kind=file]: same atomic shape ──────────────────


def test_replace_does_not_call_helper_synchronously_for_file():
    """When kind='file', the endpoint must NOT call
    `_teardown_plex_api_artifacts_for_placements()` synchronously
    — same risk as SWITCH [api→file] pre-fix."""
    body = _replace_body()
    # The helper-call line that fired synchronously inside the
    # kind=='file' branch must be gone.
    assert "_teardown_plex_api_artifacts_for_placements(" not in body, (
        "v1.18.71: synchronous helper call must be removed from "
        "api_replace_item's kind=='file' branch; the worker "
        "handles the teardown post-hardlink"
    )


def test_replace_collects_per_section_rks_for_file_kind():
    """REPLACE [kind=file] must build its own per-section rk dict
    (named `repl_clear_rks_per_section` to avoid collision with
    SWITCH's `clear_rks_per_section`)."""
    body = _replace_body()
    assert "repl_clear_rks_per_section" in body
    assert "clear_plex_api_rks" in body
    # The kind=='file' gate.
    assert 'if kind == "file":' in body


def test_replace_v1_18_71_marker_present():
    """v1.18.71 marker in REPLACE endpoint."""
    body = _replace_body()
    assert "v1.18.71" in body


# ── Worker _do_place handles the payload field ──────────────


def test_worker_reads_clear_plex_api_rks_payload():
    """`_do_place` must read `clear_plex_api_rks` from the job's
    payload JSON."""
    body = _do_place_body()
    assert 'clear_plex_api_rks' in body, (
        "v1.18.71: _do_place must reference the payload field"
    )
    # v1.18.98 swapped the inline `json.loads(job["payload"]
    # ...) + except (TypeError, ValueError)` pattern for the
    # `_safe_payload_parse` helper (warn-once breadcrumb on
    # parse failure). The contract is the same — parse the
    # payload, read clear_plex_api_rks — just routed through
    # the helper. Pin EITHER shape so future test runs survive
    # another refactor pass.
    assert (
        "json.loads(job[\"payload\"]" in body
        or "_safe_payload_parse(" in body
    ), (
        "v1.18.71 + v1.18.98: _do_place must parse the payload "
        "(either inline json.loads OR via _safe_payload_parse)"
    )


def test_worker_calls_delete_theme_for_each_payload_rk():
    """For each rk in `clear_plex_api_rks`, the worker must call
    `plex.delete_theme(rating_key=...)`. Pin the call shape so a
    refactor can't break the contract with the Plex helper."""
    body = _do_place_body()
    import re
    body_flat = re.sub(r"\s+", " ", body)
    assert "_td_plex.delete_theme( rating_key=str(_rk))" in body_flat, (
        "v1.18.71: per-rk delete_theme call required (wrapped in "
        "try/except so a single failure doesn't break the loop)"
    )


def test_worker_teardown_runs_inside_outcome_placed_block():
    """The teardown must run ONLY when the sidecar place
    succeeded (`outcome.placed`). If the place failed, the row's
    Plex API selection MUST stay intact — that's the whole point
    of the atomic pattern."""
    body = _do_place_body()
    placed_idx = body.index("if outcome.placed:")
    payload_idx = body.index("clear_plex_api_rks")
    # The payload read must come AFTER the if outcome.placed check.
    assert placed_idx < payload_idx, (
        "v1.18.71: clear_plex_api_rks teardown must be gated "
        "on outcome.placed — running it on a failed place would "
        "re-create the same destructive-before-confirm bug "
        "v1.18.71 is supposed to fix"
    )


def test_worker_teardown_is_best_effort_doesnt_raise():
    """A Plex API failure during teardown must NOT raise. The
    sidecar already landed; the next plex_enum catches stranded
    API entries. Pin the try/except wrap so a refactor can't
    promote the warning to a job-fail."""
    body = _do_place_body()
    import re
    body_flat = re.sub(r"\s+", " ", body)
    # The inner per-rk try/except wraps the delete_theme call.
    assert (
        "except Exception as _e: # noqa: BLE001"
        in body_flat
        or "except Exception as _e:" in body_flat
    )
    # The warning text references continuing.
    body_strflat = re.sub(r'"\s*"', "", body)
    assert "continuing (sidecar landed)" in body_strflat, (
        "v1.18.71: warning must explain why the failure is "
        "non-fatal (sidecar already landed)"
    )


def test_worker_teardown_closes_plex_client_in_finally():
    """The plex client opened for the teardown must be closed in
    a finally block so a failure doesn't leak the connection."""
    body = _do_place_body()
    teardown_idx = body.index("clear_plex_api_rks")
    teardown_end = body.find("# v1.10.46", teardown_idx)
    if teardown_end == -1:
        teardown_end = teardown_idx + 3000
    block = body[teardown_idx:teardown_end]
    assert "finally:" in block
    assert "_td_plex.close()" in block


def test_worker_v1_18_71_marker_explains_pattern():
    """v1.18.71 marker in the worker block must explain the
    atomic-teardown rationale + reference v1.18.68 as the
    sibling pattern."""
    body = _do_place_body()
    teardown_idx = body.index("clear_plex_api_rks")
    block_pre = body[max(0, teardown_idx - 2000):teardown_idx]
    assert "v1.18.71" in block_pre
    block_flat = " ".join(block_pre.split())
    assert ("v1.18.68" in block_flat
            or "atomic-transition pattern" in block_flat)


# ── Shared helper stays synchronous (destructive callers) ───


def test_teardown_helper_still_used_by_destructive_callers():
    """The `_teardown_plex_api_artifacts_for_placements` helper
    must still exist + still be called by PURGE/FORGET/DELETE.
    Those endpoints don't enqueue async work, so the sync teardown
    is correct for them."""
    src = _api_py()
    # The helper is defined.
    assert "def _teardown_plex_api_artifacts_for_placements" in src, (
        "v1.18.71: shared helper must survive — destructive "
        "callers still use it"
    )
    # And at least one destructive caller still invokes it
    # (the v1.18.60 forget / delete paths).
    invocations = src.count(
        "_teardown_plex_api_artifacts_for_placements(")
    # 1 = definition; need at least 2 (def + at least one call).
    assert invocations >= 2, (
        "v1.18.71: destructive callers (PURGE/FORGET/DELETE) "
        "should still invoke the synchronous helper"
    )
