"""v1.24.91 — Esc-closing the INFO card drops the opener's focus ring.

the user: from the dashboard carousel, opening the // MOTIF INFO card then closing
it with Esc left the clicked card with a "clicked" outline; closing with the X
didn't. A native <dialog> restores focus to the element it was opened from (the
carousel card is a <button>). Esc is a KEYBOARD interaction so :focus-visible
matches on the restored opener → the ring shows; the X is a mouse interaction so
it doesn't. The info-dlg `close` listener now blurs the restored opener so Esc
matches the X. Mirrors showModalNoFocusRing's open-side blur (v1.16.1).
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def _close_listener_block() -> str:
    fn_start = APP_JS.index("function bindInfoDialog()")
    fn_end = APP_JS.index("\n  function ", fn_start + 1)
    fn_body = APP_JS[fn_start:fn_end]
    anchor = fn_body.index("dlg.addEventListener('close'")
    return fn_body[anchor:]


def test_close_listener_blurs_restored_opener():
    block = _close_listener_block()
    # the opener-blur runs after focus restoration (rAF) and targets the
    # restored active element.
    assert "requestAnimationFrame" in block
    assert "document.activeElement" in block
    assert ".blur()" in block
    assert "v1.24.91" in block


def test_blur_skips_focus_inside_another_open_dialog():
    # guard: don't yank focus out of a dialog that opened during the rAF gap.
    block = _close_listener_block()
    assert "dialog[open]" in block


def test_audio_cleanup_still_present():
    # v1.15.3's Esc-stops-audio cleanup must survive in the same listener.
    block = _close_listener_block()
    assert "querySelectorAll('audio')" in block
    assert "el.pause()" in block
    assert "el.currentTime = 0" in block
