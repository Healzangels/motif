"""v1.20.57 — state-aware DEL/UNMANAGE/PURGE copy for PU rows (Option A).

A PU (placement_kind='plex_upload') row has NO Plex-folder sidecar — the
theme lives in Plex's metadata store as motif's API upload. The handlers
already do the right thing per kind (DEL tears down the upload v1.18.36/
.38, UNMANAGE leaves it serving → row P, PURGE removes it v1.18.60), but
the tooltips + confirm dialogs were sidecar-worded and LIED on PU rows
(claimed "Plex folder", "flips to M / sidecar stays"). This pins the
honest, state-branched copy. No handler behavior changed.

JS has no exec harness; source pins are the app.js contract.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()


def _menu_remove_block() -> str:
    anchor = APP_JS.index("const remIsPU = it.placement_kind === 'plex_upload';")
    # v1.21.84: widened from 4600 — the UNMANAGE menu item gained a
    # rk: it.rating_key extra (+ comment), pushing PURGE's kind: out.
    return APP_JS[anchor:anchor + 5200]


def test_menu_tooltips_branch_on_pu():
    block = _menu_remove_block()
    # DEL: PU variant talks about the uploaded theme, not the folder.
    assert "Remove motif's uploaded theme from Plex" in block
    assert "Remove the theme.mp3 from the Plex folder" in block  # sidecar
    # UNMANAGE: PU lands at P, sidecar flips to M.
    assert "Plex keeps serving its uploaded copy → row becomes P." in block
    assert "→ row flips to M." in block
    # PURGE: PU removes the upload.
    assert "remove the uploaded theme from Plex, and drop tracking" in block
    # All three carry placement_kind through for the confirm dialogs.
    assert block.count("kind: it.placement_kind") == 3


def test_del_confirm_branches_on_pu():
    anchor = APP_JS.index("async function unplaceTheme(")
    body = APP_JS[anchor:anchor + 1400]
    assert "isPlexUpload" in body.split("{", 1)[0]  # new param in signature
    assert "Motif will delete its API-uploaded theme" in body
    # handler passes the flag through
    assert "btn.dataset.kind === 'plex_upload'" in APP_JS[
        APP_JS.index("} else if (act === 'unplace') {"):
        APP_JS.index("} else if (act === 'unplace') {") + 300]


def test_unmanage_confirm_branches_on_pu():
    anchor = APP_JS.index("const unmanagePU = btn.dataset.kind === 'plex_upload';")
    body = APP_JS[anchor:anchor + 900]
    assert "LEAVE its uploaded theme on Plex" in body
    assert "the row becomes P (Plex-served)" in body
    # sidecar branch preserved
    assert "LEAVE the theme.mp3 in your Plex folder alone" in body


def test_purge_confirm_branches_on_pu():
    anchor = APP_JS.index("async function purgeTheme(")
    sig = APP_JS[anchor:APP_JS.index(")", anchor)]
    assert "isPlexUpload" in sig
    body = APP_JS[anchor:anchor + 3000]
    assert "remove the uploaded theme from Plex." in body
    assert "leave Plex serving its upload" in body
    # handler passes the flag
    purge_call = APP_JS[APP_JS.index("await purgeTheme(btn.dataset.mt"):]
    assert "btn.dataset.kind === 'plex_upload'" in purge_call[:400]


def test_backend_unmanage_docstring_notes_pu():
    anchor = API_PY.index("async def api_unmanage_item(")
    body = API_PY[anchor:anchor + 2500]
    assert "for a plex_upload (PU) row there is NO sidecar" in body
    assert "it lands at P" in body


def test_v1_20_57_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
