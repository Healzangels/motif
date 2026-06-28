"""v1.19.83 — two small cloud-backup / library UX fixes.

## Fix A — "QUEUING PLEX BACKUP" placeholder kind mismatch

The bulk + per-row DOWNLOAD PLEX BACKUP handlers set an optimistic
topbar placeholder with kind `download_queue`, but the real server
op is kind `cloud_themes_backup` (api.py). The placeholder only
clears on a SAME-KIND running op or its 5s TTL — so the mismatch
meant it never handed off. When a fast backup op finished within
the 5s window, the topbar fell back to RE-rendering "// QUEUING
PLEX BACKUP" AFTER the rows had already flipped to PB/UB badges
(the user's repro: stale QUEUING after the LINK changed over).

Fix: use `cloud_themes_backup` as the placeholder kind at both
sites so it hands off the instant the real op runs, AND clear it
explicitly when waitForOp resolves terminal (belt-and-suspenders
for a sub-poll-fast op, symmetric with the v1.15.35 failure-path
clear). The placeholder kind must be a real ops.js-mapped kind.

## Fix B — KEEP AS BACKUP checkbox never revealed in upload dialog

The upload dialog's KEEP AS BACKUP checkbox (`#upload-download-
only-row`, v1.15.75 / v1.18.86) is `display:none` and revealed by
`openUploadDialog` only when `srcLetter === 'P'`. But the
SOURCE-menu UPLOAD MP3 menu item omitted `srcLetter` from its
extras (its SET URL sibling passes it), and the INFO-card
upload path passed hardcoded blanks — so the gate never received
'P' and the checkbox (plus the hint copy promising it) never
appeared on P-rows.

Fix mirrors the v1.15.141 manual-url fix exactly: the upload-theme
menu item now passes `srcLetter`, and the INFO-card path computes
it from the matching library row + passes title/year.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
OPS_JS = (REPO / "app" / "web" / "static" / "ops.js").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()
LIBRARY_HTML = (REPO / "app" / "web" / "templates" / "library.html").read_text()


# ── Fix A: placeholder kind matches the real op ──────────────


def test_cloud_backup_placeholder_uses_real_op_kind():
    """Both DOWNLOAD PLEX BACKUP handlers must set the placeholder
    with kind 'cloud_themes_backup' (the real server op), not the
    old 'download_queue'."""
    assert APP_JS.count(
        "'cloud_themes_backup', '// QUEUING PLEX BACKUP'"
    ) == 2, (
        "v1.19.83: both the bulk + per-row cloud-backup placeholders "
        "must use kind 'cloud_themes_backup'"
    )


def test_cloud_backup_placeholder_no_longer_download_queue():
    """The pre-fix mismatched 'download_queue' placeholder for the
    cloud-backup label must be gone (it broke the handoff)."""
    assert "'download_queue', '// QUEUING PLEX BACKUP'" not in APP_JS, (
        "v1.19.83: cloud-backup placeholder must NOT use 'download_queue' "
        "— the kind mismatch left QUEUING lingering after the op ended"
    )


def test_placeholder_kind_matches_server_op_kind():
    """The placeholder kind must equal the op kind the server emits,
    otherwise the same-kind handoff in renderTopbar can never fire.
    This is the actual bug class — pin the cross-file contract."""
    assert 'kind="cloud_themes_backup"' in API_PY, (
        "v1.19.83: the server must emit op kind 'cloud_themes_backup' "
        "(the placeholder hands off to it)"
    )


def test_cloud_backup_placeholder_kind_is_ops_mapped():
    """A placeholder kind must be a real ops.js-mapped kind (label +
    tone + mini-priority), per the v1.18.53 status-bar contract —
    otherwise the indeterminate pill has no tone and the handoff
    target has no label."""
    assert "cloud_themes_backup: 'DOWNLOAD PLEX BACKUP'" in OPS_JS  # KIND_LABEL
    assert "cloud_themes_backup: 'plex'" in OPS_JS                  # TONE_BY_KIND
    assert "cloud_themes_backup: 4" in OPS_JS                       # OP_MINI_PRIORITY


def test_cloud_backup_clears_placeholder_on_terminal():
    """waitForOp's terminal handler must clear the placeholder so a
    fast op can't leave QUEUING lingering past the work. Both
    handlers' terminal + catch paths target 'cloud_themes_backup'."""
    # 2 terminal (.then) + 2 catch = 4 clear calls for the new kind.
    assert APP_JS.count(
        "clearOptimisticPlaceholder('cloud_themes_backup')"
    ) >= 4, (
        "v1.19.83: each cloud-backup handler must clear the "
        "'cloud_themes_backup' placeholder on terminal AND on error"
    )


# ── Fix B: KEEP AS BACKUP reveal wiring ──────────────────────


def test_upload_menu_item_passes_src_letter():
    """The SOURCE-menu UPLOAD MP3 item must pass srcLetter so the
    dialog can reveal KEEP AS BACKUP on P-rows — mirror of SET URL."""
    idx = APP_JS.index("'upload-theme', 'UPLOAD MP3'")
    window = APP_JS[idx:idx + 600]
    assert "srcLetter: srcLetter" in window, (
        "v1.19.83: the upload-theme menu item must include "
        "srcLetter in its extras (it drives the KEEP AS BACKUP reveal)"
    )


def test_set_url_menu_item_still_passes_src_letter():
    """Regression baseline — the SET URL sibling that already passes
    srcLetter must keep doing so."""
    idx = APP_JS.index("'manual-url', 'SET URL'")
    window = APP_JS[idx:idx + 600]
    assert "srcLetter: srcLetter" in window


def test_info_card_upload_path_computes_src_letter():
    """The INFO-card UPLOAD MP3 path must compute srcLetter from the
    matching library row + pass title/year (mirror of the v1.15.141
    manual-url fix), not the pre-fix hardcoded blanks."""
    assert (
        "srcLetter: uploadRowItem ? computeSrcLetter(uploadRowItem) : ''"
        in APP_JS
    ), (
        "v1.19.83: INFO-card upload path must compute srcLetter from "
        "the matching library row"
    )
    # The pre-fix blank-everything call must be gone.
    assert "openUploadDialog({ ratingKey, title: '', year: '' })" not in APP_JS, (
        "v1.19.83: the hardcoded-blank openUploadDialog call (which "
        "left the reveal gate on '') must be gone"
    )


def test_upload_dialog_reveal_gate_unchanged():
    """The reveal logic itself stays the same — P-rows show the
    checkbox. (We fixed the inputs to the gate, not the gate.)"""
    assert (
        "downloadOnlyRow.style.display = (srcLetter === 'P') ? '' : 'none';"
        in APP_JS
    )


def test_upload_dialog_checkbox_markup_present():
    """The KEEP AS BACKUP checkbox + its hint copy must exist in the
    upload dialog (the control the copy promises)."""
    assert 'id="upload-download-only-row"' in LIBRARY_HTML
    assert "KEEP AS BACKUP (Plex keeps serving)" in LIBRARY_HTML
    # The hint copy that references the checkbox stays in sync.
    assert "check <strong>KEEP AS BACKUP</strong>" in LIBRARY_HTML


# ── Version pin ──────────────────────────────────────────────


def test_v1_19_83_version_pin():
    # v1.19.x prefix (relaxes cleanly past the next tag) — the exact
    # bump-every-tag pin lives in test_v1_13_79.
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
