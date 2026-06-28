"""v1.20.59 — hide ADOPT FROM PLEX on PU (plex_upload) mismatch rows.

Bug audit (2026-05-31): ADOPT FROM PLEX on a PU mismatch row was a silent
no-op. api_adopt_from_plex hardcodes `Path(media_folder)/'theme.mp3'` and
`continue`s when that file is absent; a PU row has media_folder='' (the
theme lives in Plex's metadata store, no sidecar), so every section is
skipped and the endpoint returns {ok:true, sections_adopted:0} — the
confirm promised a destructive re-adopt, but nothing happened and the
mismatch stayed unresolved. Hide the menu item on PU rows; PUSH TO PLEX
(re-uploads via the API and clears the mismatch) is the right resolution.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()


def test_adopt_from_plex_gated_off_for_plex_upload():
    """The ADOPT FROM PLEX menuItemHtml must be wrapped in a
    placement_kind !== 'plex_upload' guard."""
    anchor = APP_JS.index("'adopt-from-plex', 'ADOPT FROM PLEX'")
    # Walk back to the nearest guard that encloses the push.
    pre = APP_JS[anchor - 400:anchor]
    assert "it.placement_kind !== 'plex_upload'" in pre, (
        "ADOPT FROM PLEX must be hidden on PU rows (the endpoint "
        "no-ops there)"
    )


def test_mismatch_push_still_offered_for_pu():
    """PUSH TO PLEX (the correct PU mismatch resolution) stays — it's
    pushed just before ADOPT FROM PLEX, unconditionally inside the
    mismatch block."""
    block_start = APP_JS.index("if (themed && isMismatch && downloaded && placed) {")
    block = APP_JS[block_start:block_start + 1200]
    assert "'replace', 'PUSH TO PLEX'" in block
    # The api-kind mismatch push for PU rows.
    assert "it.placement_kind === 'plex_upload' ? 'api' : 'file'" in block


def test_endpoint_still_skips_missing_placement_file():
    """The endpoint's skip-on-missing-file (the no-op cause) is still
    there — this fix is UI-side; we're not changing the endpoint, just
    not offering the action where it can't work."""
    anchor = API_PY.index("def api_adopt_from_plex(")
    body = API_PY[anchor:anchor + 3000]
    assert 'Path(r["media_folder"]) / "theme.mp3"' in body
    assert "placement file missing" in body


def test_v1_20_59_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
