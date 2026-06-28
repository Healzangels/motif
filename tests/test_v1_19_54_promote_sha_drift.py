"""v1.19.54 — PROMOTE TO ACTIVE SHA-drift defense.

Audit-flagged S2 correctness gap from v1.19.52: the plex_cloud
PROMOTE TO ACTIVE path (BK-no-override branch in
api_set_override_intent) uploads motif's staged bytes back to
Plex via the v1.18.36 re-upload trick. Between motif's backup
time and the user's PROMOTE click, Plex's cloud catalog can
update the row's selected metadata entry to a different SHA-1
(catalog rotation, provider switch, etc.).

Without a drift check, motif silently deploys STALE bytes. Plex
content-dedups the upload to motif's older SHA, and that's what
Plex serves. The user clicked "promote my backup" expecting
"use my backup" and got "ah, but my backup was from before
Plex's last change — you're getting older content."

## Defense

1. Server (`api_set_override_intent` plex_cloud branch):
   a. Widen `bk_local` SELECT to also fetch `source_video_id`
      (where the v1.19.42 walker stored the SHA-1 at backup
      time).
   b. Before re-upload: GET `/library/metadata/{rk}/themes`
      from Plex, find the SELECTED entry, extract its SHA-1.
   c. Compare against the stored SHA.
   d. If drift AND no `force_stale` flag → 409 with structured
      detail: `{error: 'sha_drift', backup_sha, current_plex_sha,
      rk, message}`.
   e. If drift AND `force_stale=true` → proceed (user's
      informed choice).
   f. No drift → proceed normally.

2. Client (`_wireIntentFlip` in app.js):
   a. POST to /intent normally.
   b. On 409 with `error: 'sha_drift'`, show confirm dialog
      with both SHA fingerprints (12-char prefix) and the
      user-facing implication.
   c. If user confirms, retry with `force_stale: true`.
   d. If user cancels, restore the button label and bail.

3. `api()` helper extended to attach `err.status` + parsed
   `err.detail` to thrown errors (preserving backward-compat
   message format).
"""
from __future__ import annotations

import sys
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()


# ── Server: bk_local SELECT widened ──────────────────────────


def test_bk_local_select_includes_source_video_id():
    """The BK-no-override branch's local_files SELECT must
    fetch source_video_id so the drift check below has the
    stored SHA to compare against."""
    fn_idx = API_PY.index("async def api_set_override_intent(")
    fn_end = API_PY.index("\n    @app.", fn_idx + 1)
    body = API_PY[fn_idx:fn_end]
    assert (
        "SELECT source_kind, file_path, source_video_id "
        in body
    ), (
        "v1.19.54: bk_local SELECT must include source_video_id "
        "(the SHA-1 the v1.19.42 walker stored at backup time)"
    )


# ── Server: drift detection logic ────────────────────────────


def test_plex_cloud_branch_probes_themes_before_upload():
    """Before re-uploading, the branch must GET /themes from
    Plex to find the current selected SHA."""
    fn_idx = API_PY.index("async def api_set_override_intent(")
    fn_end = API_PY.index("\n    @app.", fn_idx + 1)
    body = API_PY[fn_idx:fn_end]
    pc_idx = body.index('bk_source_kind == "plex_cloud"')
    block = body[pc_idx:pc_idx + 6000]
    assert "plex_probe.get_themes" in block, (
        "v1.19.54: must call PlexClient.get_themes before "
        "re-uploading so the current selected SHA is known"
    )


def test_drift_check_uses_sha1_extractor():
    """The drift check must reuse the v1.19.42
    `_sha1_from_entry_uri` helper rather than re-implementing
    the regex (single source of truth for SHA extraction)."""
    fn_idx = API_PY.index("async def api_set_override_intent(")
    fn_end = API_PY.index("\n    @app.", fn_idx + 1)
    body = API_PY[fn_idx:fn_end]
    pc_idx = body.index('bk_source_kind == "plex_cloud"')
    block = body[pc_idx:pc_idx + 6000]
    assert "_sha1_from_entry_uri" in block, (
        "v1.19.54: drift check must reuse _sha1_from_entry_uri "
        "(single source of SHA-extraction logic)"
    )


def test_drift_only_compares_selected_metadata_entry():
    """The drift check must only look at the `selected: true`
    entry. Other entries (upload://, unselected metadata://)
    aren't what Plex is currently serving."""
    fn_idx = API_PY.index("async def api_set_override_intent(")
    fn_end = API_PY.index("\n    @app.", fn_idx + 1)
    body = API_PY[fn_idx:fn_end]
    pc_idx = body.index('bk_source_kind == "plex_cloud"')
    block = body[pc_idx:pc_idx + 6000]
    assert 'entry.get("selected") is not True' in block or (
        'entry.get("selected") is True' in block
    ), (
        "v1.19.54: drift check must scope to the Plex-selected "
        "entry (other entries aren't currently-serving)"
    )
    # And only metadata:// (cloud) entries — upload:// being
    # selected means Plex isn't using cloud bytes at all.
    assert 'uri.startswith("metadata://")' in block


# ── Server: force_stale parameter + 409 response ─────────────


def test_force_stale_parameter_recognized():
    """The endpoint must accept a `force_stale` body parameter
    to let the user bypass the drift check after confirmation."""
    fn_idx = API_PY.index("async def api_set_override_intent(")
    fn_end = API_PY.index("\n    @app.", fn_idx + 1)
    body = API_PY[fn_idx:fn_end]
    assert 'force_stale' in body, (
        "v1.19.54: endpoint must read force_stale from body "
        "to bypass drift check"
    )


def test_drift_returns_409_with_structured_detail():
    """When drift is detected (and !force_stale), the endpoint
    must 409 with a structured detail object (not a plain
    string) so the client can read both SHAs + the rk."""
    fn_idx = API_PY.index("async def api_set_override_intent(")
    fn_end = API_PY.index("\n    @app.", fn_idx + 1)
    body = API_PY[fn_idx:fn_end]
    pc_idx = body.index('bk_source_kind == "plex_cloud"')
    block = body[pc_idx:pc_idx + 8000]
    # Locate the 409 raise inside the drift branch.
    assert '"error": "sha_drift"' in block, (
        "v1.19.54: drift 409 must include error='sha_drift' "
        "in the detail object"
    )
    assert '"backup_sha"' in block
    assert '"current_plex_sha"' in block
    assert "status_code=409" in block


def test_drift_logs_event_for_audit_trail():
    """When drift is detected, the endpoint must log_event
    so the audit trail captures the blocked attempt
    (operators looking at LOGS see WHY a PROMOTE didn't fire
    silently)."""
    fn_idx = API_PY.index("async def api_set_override_intent(")
    fn_end = API_PY.index("\n    @app.", fn_idx + 1)
    body = API_PY[fn_idx:fn_end]
    pc_idx = body.index('bk_source_kind == "plex_cloud"')
    block = body[pc_idx:pc_idx + 8000]
    assert "sha_drift" in block
    # The log_event call surrounding the 409.
    log_idx = block.index("sha_drift")
    pre_log = block[max(0, log_idx - 800):log_idx]
    assert "log_event(" in pre_log


# ── Client: api() helper attaches status + detail ────────────


def test_api_helper_attaches_status_and_detail_to_errors():
    """The `api()` helper must attach r.status + parsed JSON
    detail to thrown errors so callers can distinguish
    structured 409s from generic failures."""
    idx = APP_JS.index("async function api(method, path, body)")
    fn_end = APP_JS.index("\n  }", idx) + 4
    block = APP_JS[idx:fn_end]
    assert "err.status = r.status" in block, (
        "v1.19.54: api() must attach status to thrown errors"
    )
    assert "err.detail = parsedDetail" in block, (
        "v1.19.54: api() must attach parsed FastAPI detail to "
        "thrown errors"
    )
    # Backward-compat: message format must stay the same.
    assert "${r.status}: ${text || r.statusText}" in block, (
        "v1.19.54: err.message format must stay backward-"
        "compatible — existing callers that .indexOf('409') "
        "still work"
    )


# ── Client: PROMOTE handler retries on sha_drift 409 ─────────


def test_promote_handler_handles_sha_drift_409():
    """The _wireIntentFlip handler must catch the sha_drift
    409 specifically and show a confirm dialog (not just
    surface the generic error)."""
    idx = APP_JS.index("const _wireIntentFlip = (selector, targetIntent)")
    fn_end = APP_JS.index("_wireIntentFlip(", idx + 1)
    block = APP_JS[idx:fn_end]
    assert "'sha_drift'" in block, (
        "v1.19.54: PROMOTE handler must detect "
        "error='sha_drift' in the 409 response"
    )
    assert "force_stale: true" in block, (
        "v1.19.54: on confirm, handler must retry with "
        "force_stale=true to bypass the drift check"
    )


def test_promote_dialog_includes_both_sha_fingerprints():
    """The confirm dialog must show both SHAs so the user
    can see which version Plex changed to."""
    idx = APP_JS.index("const _wireIntentFlip = (selector, targetIntent)")
    fn_end = APP_JS.index("_wireIntentFlip(", idx + 1)
    block = APP_JS[idx:fn_end]
    assert "backup_sha" in block
    assert "current_plex_sha" in block
    # Truncate to 12-char prefix for readability.
    assert "slice(0, 12)" in block, (
        "v1.19.54: SHAs in dialog should be truncated to a "
        "12-char prefix (full hex is unreadable in a confirm)"
    )


def test_promote_cancel_path_restores_button_state():
    """If the user cancels the confirm dialog, the button
    label + disabled state must be restored so they can
    re-click (or just leave the page)."""
    idx = APP_JS.index("const _wireIntentFlip = (selector, targetIntent)")
    fn_end = APP_JS.index("_wireIntentFlip(", idx + 1)
    block = APP_JS[idx:fn_end]
    # Locate the cancel branch.
    cancel_idx = block.index("if (!proceed)")
    cancel_block = block[cancel_idx:cancel_idx + 400]
    assert "btn.textContent = origLabel" in cancel_block, (
        "v1.19.54: cancel path must restore button label"
    )
    assert "btn.disabled = false" in cancel_block


def test_promote_dialog_mentions_download_plex_backup_refresh():
    """The confirm dialog should tell the user how to refresh
    the backup (DOWNLOAD PLEX BACKUP) so they have an
    actionable alternative to 'promote stale'."""
    idx = APP_JS.index("const _wireIntentFlip = (selector, targetIntent)")
    fn_end = APP_JS.index("_wireIntentFlip(", idx + 1)
    block = APP_JS[idx:fn_end]
    assert "DOWNLOAD PLEX BACKUP" in block, (
        "v1.19.54: dialog should point users at the refresh "
        "path (DOWNLOAD PLEX BACKUP) as an alternative"
    )


# ── Version pin ──────────────────────────────────────────────


def test_v1_19_54_version_pin():
    """Version bumped at v1.19.54 (then again at v1.19.55).
    Match 1.19.x prefix."""
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
