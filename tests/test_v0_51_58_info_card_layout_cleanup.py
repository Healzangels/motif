"""v0.51.58 — INFO card layout cleanup (the user).

The // MOTIF INFO drawer read "askew": the native play bar stretched the whole
1fr value column ("going so far to the right"), and the SOURCE PREVIEW thumbnail
was centered (margin:0 auto) while the grid above was left-aligned — a third
alignment axis that "feels off centered". Two CSS-only changes (no DOM/JS):

  1. .info-audio capped at max-width:340px (was max-width:none) so the player is
     a tidy control in the value column; width:100% + flex still fill up to the
     cap and shrink on narrow phones.
  2. .info-source-thumb-wrap left-aligned (margin:0, was `0 auto`) + trimmed
     480->360; .info-thumb-caption text-align:left. Now the section header,
     thumbnail + caption all hang off the card's left content edge like the grid.

The 4:3 aspect box + YouTube/SoundCloud parity are unchanged (see
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


def test_thumbnail_is_left_aligned_not_centered():
    block = _rule(".info-source-thumb-wrap")
    assert "margin: 0;" in block, (
        "v0.51.58: the thumbnail must be left-aligned (margin:0), not centered")
    assert "margin: 0 auto" not in block, (
        "v0.51.58: the pre-fix centering (margin:0 auto) must be gone")
    assert "max-width: 360px" in block
    # the 4:3 box is preserved (parity contract, test_v1_15_144).
    assert "aspect-ratio: 4 / 3" in block


def test_thumb_caption_left_aligned():
    block = _rule(".info-thumb-caption")
    assert "text-align: left" in block, (
        "v0.51.58: caption follows the now left-aligned thumbnail")
    assert "text-align: center" not in block


def test_v0_51_58_version_pin():
    # Loose pin (canonical exact pin lives in test_v1_13_79).
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
