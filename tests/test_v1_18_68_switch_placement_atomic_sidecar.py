"""v1.18.68 — SWITCH PLACEMENT (file→api) becomes atomic; better Plex
upload failure messages.

the user's repro: SWITCH PLACEMENT [file→api] on a U row for
'100 Foot Wave (2021)' (a recovery_v1.18.10 row, plex_orphan,
upstream='plex_orphan', file size 27,055,412 bytes / ~26 MB).

  > Attempted a switch to api on a row that was a src U DL Green,
  > PL Green, Link HL however the placement failed.

Docker logs show the same job (rk=153324) hammering HTTP 500
on every retry:

  upload_collection_theme: rk=153324 multipart HTTP 500
  upload_collection_theme: rk=153324 multipart failed — falling
  back to raw-body POST
  upload_collection_theme: rk=153324 raw-body HTTP 500
  RuntimeError: Plex collection upload failed for rk=153324

The 27MB file exceeds Plex's empirical ~25MB theme upload ceiling
(other rows in the same session uploaded fine at 1-7MB). Plex
returned HTTP 500 with no JSON body — just a generic 500 page.

Two issues compound:

## Issue 1: destructive ordering

`api_switch_placement` unlinked the sidecar SYNCHRONOUSLY before
enqueueing the place job. When the place job's Plex upload then
failed, the row was left with no sidecar AND no successful Plex
upload → unthemed until manual PUSH SIDECAR recovery.

Fix: move the sidecar unlink from the endpoint INTO the place
job (`_do_place_collection`). Only fires after a successful
upload. On failure the sidecar stays in place — row preserves
its pre-switch state.

## Issue 2: opaque failure note

The failed-job note read just "Plex collection upload failed for
rk=153324." No HTTP status, no body size, no hint that the file
might be too large. Operator had to dig through docker logs to
see the HTTP 500.

Fix: `PlexClient.upload_collection_theme` now returns
`(ok, status_code, body_snippet)` instead of bare `bool`. Worker
formats the RuntimeError as:
  Plex rejected upload (rk=153324, HTTP 500, size=25.8MB)
    — large files often fail Plex theme upload (observed ~25MB
    ceiling); try a shorter clip

(The "large file" hint fires when size >= 20MB AND status=500.)
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── PlexClient: upload_collection_theme returns 3-tuple ─────


def test_upload_collection_theme_return_type_is_tuple():
    """The signature documents the new 3-tuple return shape so
    every caller has to destructure (or the type check fails).
    Pin the type annotation so a future refactor can't silently
    revert to bool."""
    src = (REPO / "app" / "core" / "plex.py").read_text()
    sig_idx = src.index("def upload_collection_theme(")
    sig_end = src.index(":", sig_idx + 200)
    sig = src[sig_idx:sig_end]
    assert "tuple[bool, int | None, str]" in sig, (
        "v1.18.68: upload_collection_theme must return "
        "(ok, status_code, body_snippet) so worker can format "
        "actionable failure notes"
    )


def test_upload_collection_theme_returns_tuple_on_multipart_success():
    """Success path must return (True, status_code, body_snip)."""
    src = (REPO / "app" / "core" / "plex.py").read_text()
    fn_idx = src.index("def upload_collection_theme(")
    fn_end = src.index("def delete_collection_theme(", fn_idx)
    body = src[fn_idx:fn_end]
    # The multipart-success return shape:
    assert "return True, r.status_code, r_body_snip" in body, (
        "v1.18.68: multipart success returns the status+body"
    )


def test_upload_collection_theme_returns_tuple_on_raw_failure():
    """Raw-body POST failure must return (False, status_code,
    body_snip). When both attempts raise before getting a
    response, status_code is None."""
    src = (REPO / "app" / "core" / "plex.py").read_text()
    fn_idx = src.index("def upload_collection_theme(")
    fn_end = src.index("def delete_collection_theme(", fn_idx)
    body = src[fn_idx:fn_end]
    # Raw-body exception → no status, captures the exception type
    # in the snippet.
    assert "return False, None, f\"exception: {type(e).__name__}: {e}\"" in body
    # Raw-body normal return wraps ok2 / status / snip.
    assert "return ok2, r2.status_code, r2_body_snip" in body


def test_upload_theme_alias_returns_tuple_too():
    """The v1.18.23 alias `upload_theme` must follow the same
    return shape (it delegates)."""
    src = (REPO / "app" / "core" / "plex.py").read_text()
    alias_idx = src.index("def upload_theme(")
    sig_end = src.index(":", alias_idx + 100)
    sig = src[alias_idx:sig_end]
    assert "tuple[bool, int | None, str]" in sig


def test_lps_reupload_helper_destructures_new_tuple():
    """The LPS reupload trick at the bottom of plex.py calls
    upload_collection_theme and uses the bool. Since v1.18.68 the
    return is now a tuple, the call MUST destructure or the bool
    check would always be truthy (non-empty tuple)."""
    src = (REPO / "app" / "core" / "plex.py").read_text()
    # Find the reupload site.
    reupload_idx = src.index("# Step 2: re-upload via existing")
    # Walk forward to the return dict.
    return_idx = src.index("return {", reupload_idx)
    block = src[reupload_idx:return_idx]
    assert "upload_ok, upload_status, upload_body" in block, (
        "v1.18.68: LPS reupload must destructure the new 3-tuple"
    )


# ── Worker: better failure note ─────────────────────────────


def _do_place_collection_body() -> str:
    """Return the _do_place_collection function body."""
    src = (REPO / "app" / "core" / "worker.py").read_text()
    fn_idx = src.index("def _do_place_collection(")
    # Walk to the next top-level `def ` at the same indent —
    # _do_place_collection is the last method in its run group
    # before the file ends; use a long window cap as fallback.
    nxt = src.find("\n    def ", fn_idx + 1)
    if nxt == -1:
        nxt = len(src)
    return src[fn_idx:nxt]


def test_worker_destructures_new_upload_tuple():
    """Worker must read all three fields from upload_collection_theme."""
    body = _do_place_collection_body()
    assert (
        "ok, http_status, body_snip = plex.upload_collection_theme("
        in body
    ), (
        "v1.18.68: worker must destructure the 3-tuple from "
        "upload_collection_theme so the failure note has signal"
    )


def test_worker_failure_note_includes_status_and_size():
    """The RuntimeError raised on upload failure must include
    HTTP status + file size. Pin the format so a regression
    can't silently drop the operator's triage signal."""
    body = _do_place_collection_body()
    # Note construction.
    assert "Plex rejected upload" in body
    assert "size={size_mb:.1f}MB" in body
    assert "HTTP {http_status}" in body
    # And the no-response variant for network/timeout errors.
    assert "no response" in body


def test_worker_failure_note_adds_large_file_hint():
    """When size >= 20MB AND HTTP 500, the note appends a
    "large files often fail Plex theme upload" hint. the user's
    repro: 27MB → 500; without the hint the operator has no
    idea why it failed.

    v1.18.69: the v1.18.68 hint string survived but got wrapped
    by adjacent-string-literal concatenation when the
    size-rejection sidecar fallback was added. Flatten before
    asserting so the test still passes."""
    body = _do_place_collection_body()
    # v1.18.70: the predicate was extracted into a `size_rejection`
    # local so the size-rejection raise can be _JobPermanentFailure.
    # The check still binds size_mb >= 20 AND http_status == 500 —
    # flatten the body so the across-line break doesn't break the
    # substring assert.
    import re
    body_flat = re.sub(r"\s+", " ", body)
    # v1.19.94: 20MB literal → _PLEX_THEME_UPLOAD_CEILING_MB (10).
    assert ("size_mb >= _PLEX_THEME_UPLOAD_CEILING_MB and "
            "http_status == 500") in body_flat
    # Strip adjacent-string-literal "..." "..." concatenation for
    # the hint substring.
    body_strflat = re.sub(r'"\s*"', "", body)
    assert "large files often fail Plex theme upload" in body_strflat
    assert "10MB ceiling" in body_strflat


# ── Atomic sidecar removal: API endpoint → place job ────────


def test_switch_placement_no_longer_unlinks_synchronously():
    """The api_switch_placement endpoint must NOT unlink sidecars
    inline. Pre-fix the unlink fired before the place job, so a
    Plex 500 left the row with no sidecar and no upload."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_idx = src.index("async def api_switch_placement(")
    fn_end = src.index("@app.post", fn_idx + 1)
    body = src[fn_idx:fn_end]
    # The pre-fix inline unlink + log must be gone.
    assert "side.unlink()" not in body, (
        "v1.18.68: api_switch_placement must NOT unlink sidecars "
        "synchronously — the post-fix flow defers unlink to the "
        "place job after a successful Plex upload"
    )
    # And the pre-fix log message must be gone too.
    assert "removed sidecar" not in body, (
        "v1.18.68: pre-fix endpoint log message must not survive"
    )


def test_switch_placement_collects_sidecar_paths_for_payload():
    """The endpoint must collect sidecar paths into a per-section
    dict and thread them through the place job payload as
    `remove_sidecar_paths`."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_idx = src.index("async def api_switch_placement(")
    fn_end = src.index("@app.post", fn_idx + 1)
    body = src[fn_idx:fn_end]
    assert "sidecar_paths_per_section" in body, (
        "v1.18.68: per-section sidecar path collection required"
    )
    assert "remove_sidecar_paths" in body, (
        "v1.18.68: payload field must be named "
        "remove_sidecar_paths so worker can read it"
    )


def test_switch_placement_passes_paths_to_place_job_payload():
    """The place job INSERT must write the per-section sidecar
    paths into the payload JSON. Otherwise the worker has nothing
    to act on after upload success."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_idx = src.index("async def api_switch_placement(")
    fn_end = src.index("@app.post", fn_idx + 1)
    body = src[fn_idx:fn_end]
    # The payload-build snippet must reference both the kind
    # (api/file) and the sidecar paths.
    assert 'sec_payload["remove_sidecar_paths"]' in body, (
        "v1.18.68: payload key must be wired conditionally on "
        "per-section sidecars existing"
    )
    # Only fires when target_kind == 'api' (api→file path has no
    # sidecars to remove — the sidecar IS the new home).
    assert 'if target_kind == "api"' in body


def test_switch_placement_marker_explains_atomicity():
    """v1.18.68 marker required so a future refactor can't
    silently revert to synchronous unlink."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_idx = src.index("async def api_switch_placement(")
    fn_end = src.index("@app.post", fn_idx + 1)
    body = src[fn_idx:fn_end]
    assert "v1.18.68" in body
    # Repro reference.
    body_flat = " ".join(body.split())
    assert "100 Foot Wave" in body_flat


# ── Worker: removes sidecars after upload success ───────────


def test_worker_reads_remove_sidecar_paths_payload():
    """_do_place_collection must parse `remove_sidecar_paths` out
    of the job's payload JSON."""
    body = _do_place_collection_body()
    assert 'payload.get("remove_sidecar_paths")' in body, (
        "v1.18.68: worker must read remove_sidecar_paths from payload"
    )


def test_worker_unlinks_sidecars_AFTER_upload_success():
    """The unlink loop must run AFTER the upload success check,
    not before. Pin the ordering so a refactor can't accidentally
    fire it pre-upload (which would re-introduce the destructive-
    ordering bug)."""
    body = _do_place_collection_body()
    # The success raise is `if not ok:` — find it.
    fail_branch_idx = body.index("if not ok:")
    # Find the sidecar unlink loop.
    unlink_idx = body.index('payload.get("remove_sidecar_paths")')
    # unlink must come AFTER the fail-branch (which raises and
    # therefore short-circuits — control reaches the unlink loop
    # only on success).
    assert fail_branch_idx < unlink_idx, (
        "v1.18.68: sidecar unlink must run AFTER the upload "
        "success check. Pre-fix ordering inverted the invariant."
    )


def test_worker_unlink_is_safe_on_missing_file():
    """The unlink loop must check `is_file()` before unlinking so
    a retry (after a successful first attempt) doesn't crash on a
    FileNotFoundError. The retry case is real: the job table may
    fire two place jobs for the same row in some edge cases."""
    body = _do_place_collection_body()
    # The is_file() guard must be in the unlink loop block.
    unlink_idx = body.index('payload.get("remove_sidecar_paths")')
    # Walk to the next non-loop boundary — the next blank-line-
    # separated block boundary (next un-indented def/end-of-method).
    loop_end = body.find("with get_conn", unlink_idx)
    if loop_end == -1:
        loop_end = unlink_idx + 1500
    loop_block = body[unlink_idx:loop_end]
    assert "is_file()" in loop_block, (
        "v1.18.68: unlink must be guarded by is_file() so retries "
        "don't crash on already-removed sidecars"
    )


def test_worker_unlink_log_includes_path_and_rk():
    """When unlink succeeds, log a breadcrumb naming both the
    sidecar path AND the rk that triggered the removal. Without
    it the failure mode "wait, what file did motif delete?" has
    no audit trail."""
    body = _do_place_collection_body()
    unlink_idx = body.index('payload.get("remove_sidecar_paths")')
    loop_end = body.find("with get_conn", unlink_idx)
    if loop_end == -1:
        loop_end = unlink_idx + 1500
    loop_block = body[unlink_idx:loop_end]
    assert "removed sidecar" in loop_block
    assert "after successful upload" in loop_block
    assert "rk=" in loop_block


def test_worker_unlink_marker_explains_atomicity():
    """v1.18.68 marker required in the worker block so a future
    refactor that "simplifies" the sidecar handling has to confront
    the atomicity history."""
    body = _do_place_collection_body()
    assert "v1.18.68" in body
    # the user's repro reference.
    body_flat = " ".join(body.split())
    assert "27MB" in body_flat or "Foot Wave" in body_flat
