"""v1.24.31 — uniform YouTube notification thumbnails (the FB treatment).

the user: the Discord theme notifications showed "some large some small" — that's
Discord auto-embedding the raw YouTube watch URL, whose rich card sizes itself to
the source video (landscape for normal videos, tall portrait for Shorts). He
wanted them uniform "like what we did for the facebook downloaded" (v1.22.94).

Fix: give YouTube the FB treatment — _render_url_lines wraps the URL in <...> to
suppress the auto-embed, and attachment_thumb_url returns the deterministic
mqdefault.jpg, which notify._prepare_attachment fetches + normalizes to the
uniform 400x224 canvas (ytimg.com added to the attachment CDN allowlist).
"""
from __future__ import annotations

from pathlib import Path

from app.core.notify_content import _render_url_lines, attachment_thumb_url

REPO = Path(__file__).resolve().parent.parent

YT = "https://www.youtube.com/watch?v=uJy8KTiMkbI"
YT_THUMB = "https://i.ytimg.com/vi/uJy8KTiMkbI/mqdefault.jpg"


def test_youtube_url_auto_embed_suppressed():
    # v1.24.35 (review #3): suppression now requires a thumb to attach.
    assert _render_url_lines({"theme_url": YT, "thumb_url": YT_THUMB}) == [f"<{YT}>"]
    # youtu.be short form too
    assert _render_url_lines(
        {"theme_url": "https://youtu.be/abc",
         "thumb_url": "https://i.ytimg.com/vi/abc/mqdefault.jpg"}) == [
        "<https://youtu.be/abc>"]


def test_youtube_without_thumb_keeps_native_unfurl():
    # v1.24.35 (review #3): a YouTube row with no parseable video_id → no thumb;
    # suppressing the embed there would leave NO image. Keep the plain URL so
    # Discord's native unfurl still renders something.
    assert _render_url_lines({"theme_url": YT, "thumb_url": None}) == [YT]
    assert _render_url_lines({"theme_url": YT}) == [YT]


def test_youtube_thumb_attaches():
    assert attachment_thumb_url({"theme_url": YT, "thumb_url": YT_THUMB}) == YT_THUMB


def test_soundcloud_unchanged_native_unfurl():
    # SoundCloud has no deterministic thumb → keep its native Discord unfurl
    # (plain URL, no attachment) rather than suppressing into a blank.
    sc = "https://soundcloud.com/foo/bar"
    assert _render_url_lines({"theme_url": sc}) == [sc]
    assert attachment_thumb_url({"theme_url": sc, "thumb_url": None}) is None


def test_ytimg_in_attachment_allowlist():
    notify = (REPO / "app" / "core" / "notify.py").read_text()
    assert '"ytimg.com"' in notify
    i = notify.index("_ATTACH_THUMB_HOSTS")
    block = notify[i:i + 200]
    assert "ytimg.com" in block and "fbcdn.net" in block


def test_attachment_host_check_accepts_ytimg_subdomain():
    # i.ytimg.com must pass the host-suffix allowlist check used by
    # _prepare_attachment (host == h or host.endswith('.' + h)).
    from urllib.parse import urlparse
    from app.core.notify import _ATTACH_THUMB_HOSTS
    host = urlparse(YT_THUMB).hostname
    assert any(host == h or host.endswith("." + h) for h in _ATTACH_THUMB_HOSTS)
