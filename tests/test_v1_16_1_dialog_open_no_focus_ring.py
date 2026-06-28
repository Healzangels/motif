"""v1.16.1 — drop auto-focus ring on <dialog>.showModal().

the user on v1.16.0 (clicking info button on TV row):

> when clicking the info the X in the corner has a square around
> it like it being clicked straight away. clicking on a blank
> space properly unselects it.

## Root cause

`<dialog>.showModal()` programmatically focuses the first
focusable child inside the dialog — typically the X close button.
Browsers (Chrome/Firefox/Safari all) treat programmatic focus as
focus-visible-eligible for the standard heuristic, so the v1.15.130
cyan ring at `.dlg-close:focus-visible { outline: 2px solid var
(--cyan) }` paints around the close button the instant the dialog
opens.

The v1.15.138 `mousedown preventDefault` gate doesn't help here:
no mouse event fired. Focus moved programmatically inside
`showModal()`.

## Fix

A `showModalNoFocusRing(dlg)` helper:

  1. Calls `dlg.showModal()`
  2. Reads `document.activeElement` (whichever child the browser
     auto-focused)
  3. Calls `.blur()` on it (unless it's the dialog itself or the
     body)

Keyboard users can still Tab into the dialog (Tab re-focuses the
first focusable child); only the initial automatic focus is
suppressed. Esc still closes (handler is on dialog, not the
focused child).

Helper replaces every existing `if (typeof dlg.showModal ===
'function') dlg.showModal()` callsite — 5 of them: info-dlg,
override-dlg, manual-url-dlg, upload-dlg, new-token-dlg.

## Tests
"""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"


def _read_js() -> str:
    return APP_JS.read_text()


def test_show_modal_no_focus_ring_helper_defined():
    js = _read_js()
    assert "function showModalNoFocusRing(" in js, (
        "v1.16.1: showModalNoFocusRing() helper missing."
    )
    fn_idx = js.index("function showModalNoFocusRing(")
    body = js[fn_idx:fn_idx + 1500]
    assert "dlg.showModal()" in body, (
        "v1.16.1: helper must actually call showModal()."
    )
    assert "document.activeElement" in body, (
        "v1.16.1: helper must read activeElement so it can blur "
        "whichever child the browser auto-focused."
    )
    assert ".blur()" in body, (
        "v1.16.1: helper must blur the auto-focused element."
    )


def test_helper_guards_against_blurring_body_or_dlg():
    """The blur target must NOT be document.body or the dialog
    itself — blurring body is a no-op but a misleading code
    pattern; blurring the dialog could close it in some
    implementations."""
    js = _read_js()
    fn_idx = js.index("function showModalNoFocusRing(")
    body = js[fn_idx:fn_idx + 1500]
    assert "focused !== document.body" in body
    assert "focused !== dlg" in body


def test_all_show_modal_callsites_use_helper():
    """The v1.16.1 fix only works if EVERY showModal() callsite
    routes through the helper. Walks app.js for `dlg.showModal()`
    or `.showModal()` invocations and asserts each one is inside
    the helper itself (the canonical definition site) — there
    should be exactly ONE direct call to `dlg.showModal()` in
    the codebase: the one inside showModalNoFocusRing."""
    js = _read_js()
    # Find all direct .showModal() calls.
    direct_calls = re.findall(
        r"\bdlg\.showModal\(\)", js,
    )
    # One inside the helper. Any more = a callsite was missed.
    assert len(direct_calls) == 1, (
        f"v1.16.1: found {len(direct_calls)} direct dlg.showModal() "
        "calls; expected exactly 1 (inside showModalNoFocusRing). "
        "Every other callsite must use showModalNoFocusRing(dlg) "
        "instead so the auto-focus ring suppression applies "
        "uniformly. Missing callsites will still show the X-button "
        "ring."
    )


def test_info_dialog_uses_helper():
    """info-dlg was the user's specific repro case — pin the
    callsite uses the helper."""
    js = _read_js()
    fn_idx = js.index("async function openInfoDialog(")
    body = js[fn_idx:fn_idx + 2000]
    assert "showModalNoFocusRing(dlg)" in body, (
        "v1.16.1: openInfoDialog must use the helper — this is "
        "the user's exact repro: clicking info on a TV row showed "
        "the X with a focus ring around it."
    )


# v1.19.87: test_override_dialog_uses_helper removed — the
# override-dlg was deleted as dead code (the live manual-url-dlg,
# tested below, is the reachable SET URL dialog).


def test_manual_url_dialog_uses_helper():
    js = _read_js()
    fn_idx = js.index("function openManualUrlDialog(")
    body = js[fn_idx:fn_idx + 3000]
    assert "showModalNoFocusRing(dlg)" in body


def test_helper_falls_back_to_setattribute_open_for_legacy():
    """Old browsers without `<dialog>.showModal()` still need to
    work — the helper must fall back to `dlg.setAttribute('open',
    '')` the same as the pre-helper code did. This is the same
    fallback every legacy site used."""
    js = _read_js()
    fn_idx = js.index("function showModalNoFocusRing(")
    body = js[fn_idx:fn_idx + 1500]
    assert "setAttribute('open', '')" in body, (
        "v1.16.1: helper must preserve the legacy-browser "
        "fallback. Pre-fix all callsites had `if showModal; "
        "else setAttribute('open')`."
    )
