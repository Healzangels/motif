"""v0.51.51 — per-row DOWNLOAD PLEX BACKUP goes straight to force-capture.

Supersedes the dead v0.51.50 RECAPTURE gate. That gate keyed on
`it.plex_theme_uri.startsWith('upload://')`, but plex_items.plex_theme_uri is
ALWAYS the `/library/metadata/{rk}/theme/{version}` association url — never the
`upload://<sha>` entry key (confirmed across the operator's prod DB: 6,379/6,379
themed rows, zero scheme-encoded uris; and there is no scheme column at all). So
the gate never fired and RECAPTURE never rendered.

The scheme (upload:// motif's own vs metadata:// Plex-Pass cloud) is only
knowable via a per-row /themes call, which motif doesn't persist. So instead of
a render-time relabel, the per-row DOWNLOAD PLEX BACKUP click now goes STRAIGHT
to force-capture — it captures whatever Plex is serving (either scheme) in one
step, non-destructively for a bare-P row, with a confirm only on the swap case
(a non-plex_cloud local file that would be REPLACED). The old strict-then-
"found nothing → capture anyway?" two-step is gone. Bulk DOWNLOAD PLEX BACKUP
stays the strict C1-only Plex-Pass-loss insurance sweep.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()


def test_dead_recapture_gate_removed():
    # the v0.51.50 render-time gate keyed on the wrong field — it must not
    # come back (there is no persisted upload/metadata scheme to gate on).
    assert "servingIsMotifUpload" not in APP_JS
    assert "RECAPTURE FROM PLEX" not in APP_JS
    assert "data-recapture" not in APP_JS
    assert "it.plex_theme_uri" not in APP_JS
    assert "pi.plex_theme_uri" not in API_PY


def test_per_row_handler_goes_straight_to_force_capture():
    i = APP_JS.index("act === 'backup-cloud-theme'")
    block = APP_JS[i:i + 2100]
    # delegates straight to the force-capture helper...
    assert "cloudBackupForceCapture(rk, hasTdb, allowExistingLocal)" in block
    # ...instead of an inline strict-run POST (that moved into the helper).
    assert "/api/admin/cloud-themes-backup-run" not in block


def test_force_capture_always_forces_and_confirms_only_on_swap():
    i = APP_JS.index("function cloudBackupForceCapture(rk, hasTdb, isSwap)")
    body = APP_JS[i:i + 2600]
    # captures whatever Plex is serving (force + allow_existing_local)
    assert "force: true, allow_existing_local: true" in body
    # the ONLY confirm is the destructive swap case
    assert "if (isSwap) {" in body
    assert "REPLACE it with whatever Plex is currently serving" in body
    # the old strict-run "found no Plex Pass cloud theme" framing is gone
    assert "found no Plex Pass cloud theme" not in APP_JS
