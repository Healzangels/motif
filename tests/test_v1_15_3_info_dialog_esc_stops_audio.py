"""v1.15.3 — info dialog audio stops on Esc/backdrop close.

the user: "Also found when playing a theme song from within the
info card when closing the info card with esc the theme will
continue to play"

## Pre-fix

The INFO dialog has an embedded `<audio>` player at the bottom
of the card. v1.12.98 wired audio pause/reset INTO
`closeInfoDialog()` so the X-button click stops playback. But
Esc on a `<dialog>` element triggers the browser's native
cancel/close sequence which calls `dlg.close()` directly,
bypassing our function. The X button kept working because its
click handler routes through `closeInfoDialog`; Esc + backdrop
clicks didn't.

Net effect: theme song kept playing in the background after
Esc-closing the dialog. The user had no way to stop it short
of reopening the dialog and scrubbing to the end.

## Fix

Bind a `close` event listener on the dialog element. The
`close` event fires for ALL close paths (X-click via
`dlg.close()`, Esc-cancellation, backdrop click). Cleanup is
idempotent (pause on already-paused = no-op; currentTime=0 on
already-zero = no-op) so the X-button path running it twice
(once via closeInfoDialog, once via the close event) is
harmless.

Wired in `bindInfoDialog()` alongside the existing X-button
click binding.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"


def test_close_event_listener_bound_in_bind_info_dialog():
    """The dialog's native `close` event must have a listener
    that runs the audio cleanup. Pre-fix only the X-button
    click was wired (which routed through closeInfoDialog —
    the function that already had the cleanup)."""
    src = APP_JS.read_text()
    fn_start = src.index("function bindInfoDialog()")
    fn_end = src.index("function ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    assert "dlg.addEventListener('close'" in fn_body, (
        "bindInfoDialog must register a close event listener so "
        "Esc / backdrop closes also stop the audio"
    )


def test_close_event_listener_pauses_and_resets_audio():
    """The close event listener must pause AND reset
    currentTime on every audio element inside the dialog.
    Pause alone leaves the playhead mid-track so a re-open
    resumes from where it stopped — the user's mental model
    is "I closed it, I'm done with this row."""
    src = APP_JS.read_text()
    fn_start = src.index("function bindInfoDialog()")
    fn_end = src.index("function ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    # Anchor inside the close listener.
    listener_anchor = fn_body.index("dlg.addEventListener('close'")
    listener_block = fn_body[listener_anchor:listener_anchor + 800]
    assert "querySelectorAll('audio')" in listener_block
    assert "el.pause()" in listener_block
    assert "el.currentTime = 0" in listener_block


def test_close_listener_cleanup_is_defensive():
    """The audio cleanup must be wrapped in try/catch — the
    element may already be torn down between the dialog
    closing and the close event firing in edge cases (e.g.,
    DOM rewrite mid-close). Defensive swallow keeps the close
    handler robust."""
    src = APP_JS.read_text()
    fn_start = src.index("function bindInfoDialog()")
    fn_end = src.index("function ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    listener_anchor = fn_body.index("dlg.addEventListener('close'")
    listener_block = fn_body[listener_anchor:listener_anchor + 800]
    assert "try {" in listener_block
    assert "catch (_)" in listener_block


def test_existing_close_info_dialog_cleanup_preserved():
    """v1.12.98's audio cleanup INSIDE closeInfoDialog must
    stay — the X-button path still uses it. The new close
    event listener is additive, not a replacement."""
    src = APP_JS.read_text()
    fn_start = src.index("function closeInfoDialog()")
    fn_end = src.index("function ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    assert "querySelectorAll('audio')" in fn_body
    assert "el.pause()" in fn_body
    assert "el.currentTime = 0" in fn_body


def test_v1_15_3_marker_explains_the_fix():
    """A v1.15.3 marker on the close listener references both
    the v1.12.98 precedent AND the user's specific repro so a
    future cleanup pass sees the rationale."""
    src = APP_JS.read_text()
    fn_start = src.index("function bindInfoDialog()")
    fn_end = src.index("function ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    assert "v1.15.3" in fn_body
    assert "v1.12.98" in fn_body, (
        "Marker should reference the v1.12.98 precedent (audio "
        "cleanup wired into closeInfoDialog)"
    )
    # the user's repro phrase as evidence of intent.
    assert "Esc" in fn_body or "esc" in fn_body
