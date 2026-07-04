"""v0.51.58/59 — INFO card layout cleanup (the user).

The // MOTIF INFO drawer read "askew": the native play bar stretched the whole
1fr value column ("going so far to the right") and the SOURCE PREVIEW thumbnail
felt "off centered". The two CSS-only changes that stuck (no DOM/JS):

  1. .info-audio capped at max-width:340px (was max-width:none) so the player is
     a tidy control in the value column; width:100% + flex still fill up to the
     cap and shrink on narrow phones.
  2. .info-source-thumb-wrap trimmed 480->360 (a smaller, less-dominant preview),
     kept CENTERED (margin:0 auto) with a centered caption.

v0.51.58 first shipped the thumbnail LEFT-aligned; v0.51.59 restored the
centering after the user said "I don't mind the smaller thumbnail but I prefer
it centered" — so this file pins the settled state (capped bar + smaller centered
preview). The 4:3 aspect box + YouTube/SoundCloud parity are unchanged (see
test_v1_15_144). Source pins are the app.css contract.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def _rule(sel: str) -> str:
    i = APP_CSS.index(sel + " {")
    return APP_CSS[i:APP_CSS.index("}", i)]


def test_play_bar_is_width_capped_not_full_bleed():
    block = _rule(".info-audio")
    assert "max-width: 340px" in block, (
        "v0.51.58: the play bar must be capped so it doesn't sprawl the whole 1fr")
    assert "max-width: none" not in block, (
        "v0.51.58: the pre-fix full-bleed max-width:none must be gone")
    # still fills up to the cap + keeps its taller controls.
    assert "width: 100%" in block and "height: 40px" in block


def test_thumbnail_centered_at_smaller_size():
    # v0.51.58 trimmed 480->360 AND briefly left-aligned; v0.51.59 (the user:
    # "I prefer it centered") kept the smaller 360 but restored the centering.
    block = _rule(".info-source-thumb-wrap")
    assert "margin: 0 auto" in block, (
        "v0.51.59: the thumbnail is centered (margin:0 auto), the user's preference")
    assert "max-width: 360px" in block, (
        "v0.51.58's smaller 360 preview is kept")
    # the 4:3 box is preserved (parity contract, test_v1_15_144).
    assert "aspect-ratio: 4 / 3" in block


def test_thumb_caption_centered():
    block = _rule(".info-thumb-caption")
    assert "text-align: center" in block, (
        "v0.51.59: caption follows the re-centered thumbnail")


def test_v0_51_58_version_pin():
    # Loose pin (canonical exact pin lives in test_v1_13_79).
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
