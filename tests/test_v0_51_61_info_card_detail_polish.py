"""v0.51.61 — INFO card detail polish (the user, three bundled tweaks).

  1. Dark audio player: .info-audio gets color-scheme:dark + a green accent so
     the native control renders dark (was a bright-white pill clashing with the
     CRT card).
  2. Smarter value wrapping: .dlg-grid dd uses overflow-wrap:anywhere (was
     word-break:break-all) so paths/URLs break at cleaner points — "theme.mp3"
     stays whole instead of chopping to "theme.mp"+"3".
  3. Drop the redundant (youtube)/(soundcloud) PLATFORM tag from the themerrdb/
     applied/previous url labels (pinned in test_v1_14_20). The value shows the
     full URL; the tag repeated 3x and wrapped each label to 2 lines.

Source pins are the app.css / app.js contract.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def _rule(sel: str) -> str:
    i = APP_CSS.index(sel + " {")
    return APP_CSS[i:APP_CSS.index("}", i)]


def test_audio_player_dark_themed():
    block = _rule(".info-audio")
    assert "color-scheme: dark" in block, (
        "v0.51.61: the native player must render dark, not a bright-white pill")
    assert "accent-color: var(--green)" in block


def test_grid_values_wrap_smartly_not_break_all():
    block = _rule(".dlg-grid dd")
    assert "overflow-wrap: anywhere" in block, (
        "v0.51.61: values break at cleaner points")
    assert "word-break: break-all" not in block, (
        "v0.51.61: the char-chopping break-all is gone")


def test_url_labels_have_no_platform_tag():
    # the redundant platform qualifier is gone from all three URL labels.
    assert "(${urlSource(tdbUrl)})" not in APP_JS
    assert "(${urlSource(previousUrl)})" not in APP_JS
    assert "(${urlSource(currentUrl)})" not in APP_JS
    # the meaningful (pending) suffix survives on the themerrdb label.
    assert "const tdbSrcTag = _pendingSuffix;" in APP_JS


def test_v0_51_61_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
