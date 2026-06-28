"""v1.19.87 — remove the dead failure-recovery override-dlg.

The // MANUAL THEME URL OVERRIDE dialog (`override-dlg`) was dead
code: `openOverrideDialog` was only ever invoked behind
`act === 'open-override'`, and nothing in the app emitted that
action (no `data-act="open-override"` in any template or JS). So the
dialog bound at init (bindOverrideDialog) but could never open.

The live SET URL dialog is `manual-url-dlg` (`act === 'manual-url'`),
which already carries the live YouTube preview + KEEP AS BACKUP
checkbox. v1.19.87 deletes the orphaned override-dlg markup + its
JS (openOverrideDialog / closeOverrideDialog / bindOverrideDialog +
the unreachable handler branch).

The v1.14.71 "light the topbar mini-bar the moment the user URL is
saved" optimistic-placeholder UX — which had lived (uselessly) in
the dead override submit — was relocated to the live manual-url
submit, so it now actually fires.

The `/api/items/{mt}/{id}/override` endpoint is intentionally KEPT
(external/scripted callers, per the v1.12.76 clear-failed precedent).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
LIB_HTML = (REPO / "app" / "web" / "templates" / "library.html").read_text()


# ── The dead dialog + its JS are gone ────────────────────────


def test_override_dialog_markup_removed():
    assert 'id="override-dlg"' not in LIB_HTML, (
        "v1.19.87: the dead override-dlg markup must be removed"
    )
    assert 'id="override-url"' not in LIB_HTML
    assert 'id="override-form"' not in LIB_HTML


def test_override_dialog_js_removed():
    assert "function openOverrideDialog(" not in APP_JS, (
        "v1.19.87: openOverrideDialog (dead) must be removed"
    )
    assert "function closeOverrideDialog(" not in APP_JS
    assert "function bindOverrideDialog(" not in APP_JS
    # The unreachable handler branch + its init call are gone.
    assert "act === 'open-override'" not in APP_JS
    assert "bindOverrideDialog();" not in APP_JS


def test_live_manual_url_dialog_survives():
    """The live SET URL dialog must remain intact — it's the
    reachable replacement and already has preview + KEEP AS BACKUP."""
    assert 'id="manual-url-dlg"' in LIB_HTML
    assert 'id="manual-url-download-only"' in LIB_HTML  # KEEP AS BACKUP
    assert 'id="manual-url-preview"' in LIB_HTML        # live preview
    assert "function openManualUrlDialog(" in APP_JS


# ── The v1.14.71 placeholder UX moved to the live dialog ─────


def test_optimistic_placeholder_now_on_manual_url_submit():
    """The 'light the mini-bar on URL save' placeholder must live in
    the manual-url submit now (it was dead in the override submit)."""
    anchor = APP_JS.index(
        "await api('POST', `/api/plex_items/${encodeURIComponent(rk)}"
        "/manual-url`, body);"
    )
    block = APP_JS[anchor:anchor + 1500]
    assert "setOptimisticPlaceholder(" in block
    assert "'download_queue'" in block
    assert "'// THEME DOWNLOAD QUEUED'" in block


# ── Version pin ──────────────────────────────────────────────


def test_v1_19_87_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
