"""v0.50.37 — notification source line carries a per-platform colour dot.

the user: "make the color on all youtube added videos red as blue is supposed to
be facebook." Apprise's Discord embed bar is SEVERITY-coloured (INFO=blue for every
theme-add) and not a per-source knob — and the external apprise-api path can't set
a colour at all — so the colour lives in the body's Source line, where it renders
identically on every transport: 🔴 YouTube, 🔵 Facebook (🟠 SoundCloud, 🟣 Instagram).
"""
from __future__ import annotations

from app.core.notify_content import _format_provenance_line


def _line(url, prov="themerrdb"):
    return _format_provenance_line({"provenance": prov, "theme_url": url})


def test_youtube_is_red_facebook_is_blue():
    assert "🔴 YouTube" in _line("https://www.youtube.com/watch?v=abcdefghijk")
    assert "🔴 YouTube" in _line("https://youtu.be/abcdefghijk")
    assert "🔵 Facebook" in _line("https://www.facebook.com/watch/?v=1", prov="user_url")
    assert "🔵 Facebook" in _line("https://fb.watch/xyz", prov="user_url")


def test_other_platforms_get_their_own_dot():
    assert "🟠 SoundCloud" in _line("https://soundcloud.com/foo/bar", prov="user_url")
    assert "🟣 Instagram" in _line("https://instagram.com/reel/abc", prov="user_url")


def test_no_url_has_no_dot():
    # nothing to classify → bare provenance, no colour dot
    assert _format_provenance_line({"provenance": "themerrdb", "theme_url": ""}) \
        == "Source: ThemerrDB"
