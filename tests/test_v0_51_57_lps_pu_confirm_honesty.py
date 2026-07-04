"""v0.51.57 — LET PLEX SERVE confirm honesty for PU (plex_upload) rows.

The SOURCE-menu LET PLEX SERVE confirm always said "DELETE motif's theme.mp3
from the Plex folder". That's only true for a hardlink/copy (HL/CP) row — a
plex_upload (PU/cyan) row has NO sidecar file in the media folder; motif POSTed
the theme to Plex's own theme list (v1.18.36). the user: a cyan PU row's
confirm is misleading because there's no local copy in the Plex folder.

Fix: letPlexServeFlow branches the removal bullet on the row's placement_kind
(the SAME signal the LINK badge uses: placement_kind === 'plex_upload' → PU).
PU → "REMOVE motif's uploaded theme from Plex … no file in the media folder";
HL/CP → the original folder-delete line. Mirrors the honest state-branched copy
v1.20.57 already gave DEL/UNMANAGE/PURGE. Confirm-text only; no handler change.

JS has no exec harness; source pins are the app.js contract.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def _lps_block() -> str:
    anchor = APP_JS.index("async function letPlexServeFlow(")
    return APP_JS[anchor:anchor + 3200]


def test_lps_removal_bullet_branches_on_placement_kind():
    block = _lps_block()
    # The branch derives PU from the same signal as the LINK badge.
    assert "lpsRow.placement_kind === 'plex_upload'" in block
    assert "const removalLine" in block
    # baseText composes the branched line, not a hardcoded folder-delete.
    assert "+ removalLine" in block


def test_lps_pu_variant_says_uploaded_not_folder():
    block = _lps_block()
    # PU wording: honest about the Plex theme list, no media-folder file.
    # Mirrors v1.20.57's "Remove motif's uploaded theme from Plex".
    assert "REMOVE motif\\'s uploaded theme from Plex" in block
    assert "no file in the media folder" in block


def test_lps_sidecar_variant_keeps_folder_delete():
    block = _lps_block()
    # HL/CP rows genuinely have a theme.mp3 sidecar — that copy is preserved.
    assert "DELETE motif\\'s theme.mp3 from the Plex folder" in block


def test_v0_51_57_version_pin():
    # Loose pin (canonical exact pin lives in test_v1_13_79).
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
