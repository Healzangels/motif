"""v1.14.2 — ACCEPT UPDATE source-aware confirm + SET URL live preview.

Two behavioral additions on the SoundCloud rollout:

1. **ACCEPT UPDATE confirm dialog** now shows the actual current
   URL + new TDB URL + their detected sources. When the sources
   differ (e.g. SoundCloud user override → YouTube TDB URL), the
   prompt prefixes "⚠ SOURCE CHANGE" so the user notices the
   regression before clicking OK. the user's principle: "the user
   took the time to provide a URL, they might want to keep it."

2. **SET URL live preview**: as the user pastes into the override
   or manual-url dialog input, the status line shows
   "detected: YouTube" / "detected: SoundCloud" / "detected: not
   recognized" via a new `bindUrlSourcePreview()` helper.

Tests pin the JS via static-text guards (consistent with the
v1.14.0 / v1.14.1 pattern). Behavioral coverage of these flows
needs a Selenium/Playwright stack the project hasn't adopted —
deferring per CLAUDE.md test conventions.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── ACCEPT UPDATE confirm enhancements ────────────────────────


def test_accept_update_confirm_uses_url_source_detection():
    """The confirm prompt logic must call urlSource() on both the
    current URL and the new TDB URL — that's how it decides whether
    to surface the SOURCE CHANGE warning."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    fn_anchor = js.index("async function acceptUpdate(mediaType, tmdbId, btn)")
    body = js[fn_anchor:fn_anchor + 5000]
    assert "urlSource(currentUrl)" in body
    assert "urlSource(newTdbUrl)" in body


def test_accept_update_warns_on_source_change():
    """When the user override is one source and TDB is another,
    the confirm prompt must prefix '⚠ SOURCE CHANGE' so the user
    sees the regression risk before clicking OK."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    fn_anchor = js.index("async function acceptUpdate(mediaType, tmdbId, btn)")
    body = js[fn_anchor:fn_anchor + 5000]
    assert "SOURCE CHANGE" in body, (
        "v1.14.2: SC override → YT TDB URL transition must be "
        "flagged with a SOURCE CHANGE prefix in the confirm prompt"
    )
    # The branch must be guarded by sourcesDiffer.
    assert "sourcesDiffer" in body


def test_accept_update_shows_actual_urls_in_prompt():
    """The prompt must include the current URL AND the new TDB
    URL (truncated for readability) so the user sees what's
    being replaced. Pre-fix the prompt was generic — no URLs
    visible."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    fn_anchor = js.index("async function acceptUpdate(mediaType, tmdbId, btn)")
    body = js[fn_anchor:fn_anchor + 5000]
    # The truncate helper exists.
    assert "const truncate = (u)" in body
    # Prompt branches reference truncate(currentUrl) + truncate(newTdbUrl).
    assert "truncate(currentUrl)" in body
    assert "truncate(newTdbUrl)" in body


def test_accept_update_preserves_isUserSrcRow_branch():
    """Regression guard: the v1.12.42 user-source-tailored prompt
    branch must still exist (isUserSrcRow). v1.14.2 expanded it
    with URL detail but didn't drop it."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    fn_anchor = js.index("async function acceptUpdate(mediaType, tmdbId, btn)")
    body = js[fn_anchor:fn_anchor + 5000]
    assert "isUserSrcRow" in body
    assert "computeSrcLetter(rowItem) === 'U'" in body


# ── SET URL live preview ──────────────────────────────────────


def test_bind_url_source_preview_helper_exists():
    """The shared live-preview helper must be defined and used
    by both the override dialog and the manual-url dialog so
    the behavior is consistent across both SET URL surfaces."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert "function bindUrlSourcePreview(inputId, statusId)" in js


# v1.19.87: test_bind_url_source_preview_called_from_override_dialog
# removed — bindOverrideDialog/override-dlg was deleted as dead code.
# The live manual-url dialog's preview wiring is covered below.


def test_bind_url_source_preview_called_from_manual_url_dialog():
    """bindManualUrlDialog must wire the preview onto manual-url-input
    + manual-url-status. Library.html has both elements per the
    v1.14.0 dialog template."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    fn_anchor = js.index("function bindManualUrlDialog()")
    body = js[fn_anchor:fn_anchor + 600]
    assert "bindUrlSourcePreview('manual-url-input', 'manual-url-status')" in body


def test_preview_emits_correct_label_per_source():
    """The helper's labels must match the urlSource() return values:
    'detected: YouTube' / 'detected: SoundCloud' / 'detected: not
    recognized'. Pin so a copy refactor doesn't drop a source."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    fn_anchor = js.index("function bindUrlSourcePreview(inputId, statusId)")
    body = js[fn_anchor:fn_anchor + 1500]
    assert "'detected: YouTube'" in body
    assert "'detected: SoundCloud'" in body
    assert "not recognized" in body
