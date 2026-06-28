"""v1.18.60 — cleanup-symmetry audit: PURGE / DELETE / REPLACE.

the user's request after v1.18.59:

> "Code paths with cleanup tied to one user-action surface need
>  symmetric cleanup on the OTHER paths reaching the same end
>  state. v1.18.36 cleaned up on SWITCH; v1.18.59 extends to all
>  API placement paths. Worth a one-time audit pass for similar
>  gaps elsewhere."

## Audit findings

Walked every per-item endpoint (22 total). Found 3 callsites with
the same shape as v1.18.36/.59 (an action transitioning AWAY from
a plex_upload placement without clearing Plex's API entry):

  1. **PURGE (api_forget_item)** — iterates placements + unlinks
     `Path(media_folder) / 'theme.mp3'`. For plex_upload rows
     `media_folder=''` so the path resolves to `theme.mp3` in
     cwd (`.is_file()` returns False, unlink no-op). Plex's API
     entry stays. Result: PURGE drops DB tracking but Plex keeps
     serving motif's uploaded theme.
  2. **DELETE (api_delete_item)** — same shape as PURGE. SELECT
     even reads `placement_kind` but doesn't branch on it.
  3. **REPLACE with kind='file' (api_replace_item)** — when the
     caller specifies kind='file' AND the existing placement is
     plex_upload, REPLACE deletes the placement row + enqueues a
     sidecar place job. The new sidecar appears at the Plex
     media folder, but motif's API entry remains active in Plex.
     Plex's theme resolution may still serve the API entry over
     the new sidecar.

All three reach the same end state as the v1.18.36 SWITCH api→
file scenario but bypass the SWITCH endpoint entirely.

## Fix

Extracted the v1.18.36 SWITCH api→file Plex teardown into a
shared helper `_teardown_plex_api_artifacts_for_placements(db,
settings, media_type, tmdb_id, placements)`. Calls
`plex.delete_theme(rk)` for each plex_upload placement, logs
each operation, returns count. Idempotent (delete_theme on a
gone entry returns False harmlessly). Best-effort: per-row
errors WARN and continue.

Wired into all three callsites:
  - api_forget_item (PURGE) — before the sidecar unlink loop
  - api_delete_item (DELETE) — before the sidecar unlink loop
  - api_replace_item (REPLACE) — before DELETE FROM placements
    when kind='file'

Sidecar-unlink loops in PURGE + DELETE also skip plex_upload
rows now (the row has no sidecar to unlink; the v1.18.60
teardown above handled it).

## Not changed (verified safe by audit)

  - SWITCH (api_switch_placement) — already does both
    directions correctly (v1.18.36).
  - LET PLEX SERVE (api_unplace_item) — already does the
    DELETE + re-upload-trick restoration for plex_upload rows
    (v1.18.36 / v1.18.38).
  - UNMANAGE (api_unmanage_item) — semantically "stop tracking,
    leave the served theme alone". For plex_upload rows that
    means Plex keeps serving (parallel to sidecar's "leave the
    file" behavior). Correct as-is.
  - RESTORE FROM PLEX (api_restore_canonical) — skips
    plex_upload rows with "no_placement" because media_folder=''.
    Future feature: pull from Plex's API for plex_upload rows.
    Out of scope for this audit.
  - REDOWNLOAD (api_redownload) — leaves the existing placement
    in place; worker's place job (with v1.18.59 cleanup) handles
    the transition. Indirect coverage.
  - CONVERT-TO-MANUAL — DB metadata shuffle only, no file ops.
"""

from __future__ import annotations

import re
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"


def _flatten_concatenated_strings(body: str) -> str:
    """Same helper as v1.18.59 — collapse Python's adjacent-
    string-literal concatenation so substring asserts work on
    the runtime value."""
    return re.sub(r'"\s*"', '', body)


# ── Helper exists + has the right signature ──────────────────


def test_teardown_helper_defined():
    """The shared helper must exist at module scope so all three
    callsites can import / call it."""
    src = API_PY.read_text()
    assert "def _teardown_plex_api_artifacts_for_placements(" in src
    # Must take db, settings, media_type, tmdb_id, placements.
    sig_idx = src.index("def _teardown_plex_api_artifacts_for_placements(")
    sig_end = src.index(")", sig_idx)
    sig = src[sig_idx:sig_end]
    for arg in ("db:", "settings,", "media_type:", "tmdb_id:", "placements,"):
        assert arg in sig, (
            f"v1.18.60: teardown helper signature missing {arg!r}"
        )


def test_teardown_helper_is_no_op_when_plex_disabled():
    """The helper must early-return 0 when Plex isn't configured —
    PURGE / DELETE / REPLACE shouldn't fail just because the
    user has no Plex token set."""
    src = API_PY.read_text()
    idx = src.index("def _teardown_plex_api_artifacts_for_placements(")
    body = src[idx:idx + 5000]
    assert "settings.plex_enabled" in body
    assert "settings.plex_token" in body
    # The early-return on no-plex must be the FIRST `return 0`
    # in the function body. The docstring is long (~1500 chars)
    # so "near the top" by character index would mean wider than
    # the test originally allowed — anchor on "first return"
    # instead.
    early_idx = body.find("return 0")
    assert early_idx != -1, (
        "v1.18.60: helper must early-return 0 when Plex disabled"
    )
    # The early return must come BEFORE the PlexClient import.
    plex_client_idx = body.find("from ..core.plex import PlexClient")
    assert early_idx < plex_client_idx, (
        "v1.18.60: early-return must precede the PlexClient "
        "import so the helper short-circuits without paying the "
        "import cost when Plex isn't configured"
    )


def test_teardown_helper_filters_to_plex_upload_only():
    """The helper must filter the input list to placements with
    placement_kind='plex_upload' — sidecar placements have no
    Plex API entry to delete."""
    src = API_PY.read_text()
    idx = src.index("def _teardown_plex_api_artifacts_for_placements(")
    body = src[idx:idx + 5000]
    assert "'plex_upload'" in body
    assert "placement_kind" in body


def test_teardown_helper_calls_delete_theme():
    """The helper must call `plex.delete_theme(rating_key=...)` —
    the v1.18.36 canonical "clear motif's API selection" API
    surface."""
    src = API_PY.read_text()
    idx = src.index("def _teardown_plex_api_artifacts_for_placements(")
    body = src[idx:idx + 6500]  # v1.23.65: widened for the #9 edition clauses
    assert "plex.delete_theme" in body
    assert "rating_key=" in body


def test_teardown_helper_logs_per_call():
    """Each delete_theme call must log at INFO so docker logs show
    the cleanup actions alongside the existing SWITCH-path logs."""
    src = API_PY.read_text()
    idx = src.index("def _teardown_plex_api_artifacts_for_placements(")
    body = _flatten_concatenated_strings(src[idx:idx + 6500])  # v1.23.65
    assert "teardown_plex_api" in body
    assert "delete_theme=" in body


# ── Callsite A: PURGE (api_forget_item) ─────────────────────


def test_purge_calls_teardown_helper():
    """api_forget_item (PURGE) must call the teardown helper
    BEFORE the sidecar unlink loop. Pre-v1.18.60 PURGE on a
    plex_upload row dropped the DB row but left Plex serving
    the API entry."""
    src = API_PY.read_text()
    fn_idx = src.index("async def api_forget_item(")
    fn_end = src.index("@app.delete(", fn_idx)
    body = src[fn_idx:fn_end]
    # v1.22.42: the call is now `await run_in_threadpool(_teardown_..., ...)`
    # (comma after the bound name, not an open-paren) so the helper's per-rk
    # delete_theme loop doesn't freeze the event loop. Pin the bare name.
    assert "_teardown_plex_api_artifacts_for_placements" in body, (
        "v1.18.60: api_forget_item must call the teardown helper"
    )
    assert "await run_in_threadpool(" in body, (
        "v1.22.42: the teardown call must be off-loaded to a thread"
    )


def test_purge_select_reads_placement_kind():
    """The PURGE placements SELECT must include placement_kind +
    section_id so the teardown helper has the info it needs."""
    src = API_PY.read_text()
    fn_idx = src.index("async def api_forget_item(")
    fn_end = src.index("@app.delete(", fn_idx)
    body = src[fn_idx:fn_end]
    # The SELECT must now request placement_kind + section_id.
    flat = _flatten_concatenated_strings(body)
    assert "SELECT media_folder, placement_kind, section_id" in flat, (
        "v1.18.60: PURGE placements SELECT must include "
        "placement_kind + section_id columns"
    )


def test_purge_sidecar_loop_skips_plex_upload():
    """The sidecar unlink loop must skip plex_upload rows —
    media_folder='' for those is the sentinel, NOT a real path.
    Without the skip, Path('') / 'theme.mp3' = relative
    'theme.mp3' which is_file()=False harmlessly but only by
    coincidence."""
    src = API_PY.read_text()
    fn_idx = src.index("async def api_forget_item(")
    fn_end = src.index("@app.delete(", fn_idx)
    body = src[fn_idx:fn_end]
    # The explicit skip guard.
    assert 'placement_kind"] or "") == "plex_upload"' in body or \
           '"plex_upload"' in body, (
        "v1.18.60: PURGE sidecar loop must explicitly skip "
        "plex_upload rows"
    )


# ── Callsite B: DELETE (api_delete_item) ────────────────────


def test_delete_calls_teardown_helper():
    """api_delete_item (DELETE) — same gap as PURGE, same fix."""
    src = API_PY.read_text()
    fn_idx = src.index("async def api_delete_item(")
    fn_end = src.index("@app.post(", fn_idx)
    body = src[fn_idx:fn_end]
    # v1.22.42: off-loaded via run_in_threadpool (comma form) — pin bare name.
    assert "_teardown_plex_api_artifacts_for_placements" in body, (
        "v1.18.60: api_delete_item must call the teardown helper"
    )
    assert "await run_in_threadpool(" in body, (
        "v1.22.42: the teardown call must be off-loaded to a thread"
    )


def test_delete_select_reads_section_id():
    """DELETE placement_rows SELECT was already reading
    placement_kind pre-v1.18.60 but not branching on it. The
    v1.18.60 fix adds section_id so the teardown can scope rk
    lookups."""
    src = API_PY.read_text()
    fn_idx = src.index("async def api_delete_item(")
    fn_end = src.index("@app.post(", fn_idx)
    body = src[fn_idx:fn_end]
    flat = _flatten_concatenated_strings(body)
    assert "SELECT media_folder, placement_kind, section_id" in flat


def test_delete_sidecar_loop_skips_plex_upload():
    """Mirror of test_purge_sidecar_loop_skips_plex_upload."""
    src = API_PY.read_text()
    fn_idx = src.index("async def api_delete_item(")
    fn_end = src.index("@app.post(", fn_idx)
    body = src[fn_idx:fn_end]
    assert '"plex_upload"' in body


# ── Callsite C: REPLACE with kind='file' ────────────────────


def test_replace_schedules_api_teardown_when_kind_file():
    """api_replace_item must arrange for the Plex API selection
    to be cleared when the new kind is 'file' AND there's an
    existing placement_kind='plex_upload' row.

    v1.18.60 originally called the synchronous helper
    `_teardown_plex_api_artifacts_for_placements`. v1.18.71
    moves the teardown into the place job's `clear_plex_api_rks`
    payload so a hardlink failure leaves the Plex API selection
    intact (atomic-teardown audit rollover sibling of v1.18.68).

    Pin: per-section rk collection + payload field name +
    kind='file' guard."""
    src = API_PY.read_text()
    fn_idx = src.index("async def api_replace_item(")
    fn_end = src.index("@app.post(", fn_idx)
    body = src[fn_idx:fn_end]
    # Per-section rk collection (named to avoid collision with
    # SWITCH's `clear_rks_per_section`).
    assert "repl_clear_rks_per_section" in body, (
        "v1.18.71: REPLACE must collect per-section rks for the "
        "atomic api→file teardown flow"
    )
    # Payload field name.
    assert "clear_plex_api_rks" in body
    # kind='file' guard.
    assert 'kind == "file"' in body or "kind == 'file'" in body
    # And the synchronous teardown helper must NOT be invoked
    # from REPLACE anymore (it's still invoked by destructive
    # callers — PURGE/FORGET/DELETE — those don't enqueue async).
    assert "_teardown_plex_api_artifacts_for_placements(" not in body, (
        "v1.18.71: REPLACE no longer calls the synchronous "
        "helper; the worker handles the teardown post-hardlink"
    )


def test_replace_collects_rks_before_delete_placements():
    """The per-section rk collection (which reads from existing
    placements + plex_items) must happen BEFORE the DELETE FROM
    placements row drop — otherwise the lookup has no placement
    rows to walk.

    Anchor on the actual `.setdefault(` call (where the rks are
    populated from the plex_items lookup) rather than the
    declaration site (which can be anywhere — it's a typed empty
    dict initializer that may live before the SELECT for clarity)."""
    src = API_PY.read_text()
    fn_idx = src.index("async def api_replace_item(")
    fn_end = src.index("@app.post(", fn_idx)
    body = src[fn_idx:fn_end]
    select_idx = body.index("SELECT placement_kind, section_id")
    populate_idx = body.index("repl_clear_rks_per_section.setdefault(")
    delete_idx = body.index("DELETE FROM placements")
    assert select_idx < populate_idx < delete_idx, (
        "v1.18.71: REPLACE must SELECT existing placements + "
        "populate rks BEFORE the DELETE FROM placements row drop"
    )


# ── Audit exception list: paths intentionally NOT touched ───


AUDIT_EXCEPTIONS = {
    # Already handled in prior tags — verified during audit.
    "api_switch_placement": "v1.18.36 handles both api→file and file→api",
    "api_unplace_item": "v1.18.36 / v1.18.38 handle LET PLEX SERVE for plex_upload",
    "api_unmanage_item": "Semantic 'stop tracking, leave Plex serving' — correct as-is",
    "api_restore_canonical": "Plex-upload RESTORE is a feature gap (out of audit scope)",
    "api_redownload": "Worker's place job (v1.18.59) handles transition indirectly",
    "api_convert_to_manual": "DB metadata only, no file ops",
}


def test_audit_exceptions_documented():
    """Pin the audit exception list so a future audit pass sees
    exactly which paths were verified NOT to need the teardown
    helper, with the rationale for each."""
    # The exception list is part of the test module — pin its
    # contents so a future drift surfaces here.
    assert len(AUDIT_EXCEPTIONS) >= 6
    # Each entry must have a non-trivial rationale.
    for endpoint, reason in AUDIT_EXCEPTIONS.items():
        assert len(reason) > 20, (
            f"v1.18.60 audit exception {endpoint!r} needs a "
            f"longer rationale"
        )


# ── Version marker ──────────────────────────────────────────


def test_v1_18_60_markers_present():
    src = API_PY.read_text()
    # The helper definition + each callsite must carry the marker.
    assert src.count("v1.18.60") >= 4, (
        "v1.18.60: marker must appear at helper definition + "
        "each of the three callsites (PURGE / DELETE / REPLACE)"
    )
