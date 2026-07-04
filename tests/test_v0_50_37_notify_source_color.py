"""v0.51.54 (supersedes v0.50.37) — the Source line's colour dot encodes SOURCE.

v0.50.37 originally coloured the dot by PLATFORM (YouTube-red / Facebook-blue).
the user then wanted it by SOURCE instead: "colour the dot by the source —
purple for a user url/upload, green for themerrdb, cyan for adopted ... at a
glance show where it came from." So the dot now sits next to the provenance
label and follows the SRC pill palette; the platform (YouTube/Facebook) reads as
plain text. Apprise's Discord embed bar is severity-coloured (INFO=blue for every
theme-add) and the external apprise-api path can't set a colour, so the colour
lives in the body where it renders identically on every transport.
"""
from __future__ import annotations

from app.core.notify_content import _format_provenance_line


def _line(url, prov="themerrdb"):
    return _format_provenance_line({"provenance": prov, "theme_url": url})


def test_dot_encodes_source_not_platform():
    # themerrdb -> green, whatever the platform is
    assert _line("https://www.youtube.com/watch?v=abcdefghijk") \
        == "Source: 🟢 ThemerrDB · YouTube"
    assert _line("https://youtu.be/abcdefghijk") \
        == "Source: 🟢 ThemerrDB · YouTube"
    # user_url -> violet; platform is just text
    assert _line("https://www.facebook.com/watch/?v=1", prov="user_url") \
        == "Source: 🟣 User URL · Facebook"
    assert _line("https://soundcloud.com/foo/bar", prov="user_url") \
        == "Source: 🟣 User URL · SoundCloud"


def test_source_dot_per_provenance():
    assert _line("https://youtu.be/x", prov="user_upload").startswith(
        "Source: 🟣 User upload")
    assert _line("https://youtu.be/x", prov="adopted").startswith(
        "Source: 🔵 Adopted")
    assert _line("https://youtu.be/x", prov="manual").startswith(
        "Source: 🟤 Manual sidecar")
    assert _line("https://youtu.be/x", prov="plex_served").startswith(
        "Source: 🟡 Plex-served")


def test_no_url_still_shows_the_source_dot():
    # v0.51.54: the dot is a SOURCE indicator, so it shows even without a URL
    # (was bare provenance / no dot under the old per-platform design).
    assert _format_provenance_line({"provenance": "themerrdb", "theme_url": ""}) \
        == "Source: 🟢 ThemerrDB"
    assert _format_provenance_line({"provenance": "adopted", "theme_url": ""}) \
        == "Source: 🔵 Adopted"
