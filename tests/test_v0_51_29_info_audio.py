"""v0.51.29 — info-card audio: drop the redundant download arrow + give the
native player room so the volume slider is usable (the user)."""
from __future__ import annotations
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def test_no_sibling_download_arrow_in_play_row():
    # the ↓ download <a> next to the <audio> is gone (native ⋮ menu has Download).
    assert 'download="theme.mp3"' not in APP_JS, (
        "v0.51.29: the redundant sibling download arrow must be removed")
    assert '.info-play-row > a' not in APP_CSS, (
        "the now-dead .info-play-row > a rule should be gone too")


def test_info_audio_gets_full_row_and_taller_controls():
    i = APP_CSS.index(".info-audio {")
    block = APP_CSS[i:APP_CSS.index("}", i)]
    assert "height: 40px" in block, (
        "v0.51.29: the player must be taller (40px, was a squished 32px) so the "
        "native controls aren't cramped")
    assert "max-width: none" in block and "width: 100%" in block, (
        "the player must fill the play row so the volume/seek sliders get width")
