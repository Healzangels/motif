"""v1.22.94 — FB notification thumb as apprise attachment.

the user on the v1.22.92 result: "we're getting a huge url instead of
just url of the video and the image is a small vertical version
would be great to have it match the other notifications thumbnail."

The bare-image-URL body line rendered the signed fbcdn URL as a
six-line wall of link text, and the image embedded at the video's
native vertical aspect. Now: the body carries only <url> (unfurl
still suppressed); the thumbnail travels as a real apprise
attachment (notify._prepare_attachment fetches it server-side and
ffmpeg letterbox-normalizes it to 320x180 — the same footprint as
YouTube's mqdefault — falling back to the raw bytes without ffmpeg,
or to no attachment on any fetch failure).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
NOTIFY_PY = (REPO / "app" / "core" / "notify.py").read_text()
WORKER_PY = (REPO / "app" / "core" / "worker.py").read_text()

FB_THUMB = ("https://scontent-bos5-1.xx.fbcdn.net/v/t15.5256-10/"
            "x.jpg?oe=6A")


# ── notify_content: body line gone, attachment helper ────────


def test_render_url_lines_fb_is_url_only():
    from app.core.notify_content import _render_url_lines
    ctx = {"theme_url": "https://www.facebook.com/HGTV/videos/1/",
           "thumb_url": FB_THUMB}
    assert _render_url_lines(ctx) == [
        "<https://www.facebook.com/HGTV/videos/1/>"
    ], "v1.22.94: no bare image-URL line — the thumb is an attachment"


def test_attachment_thumb_url_fb_and_youtube():
    """v1.24.31: YouTube now returns its mqdefault thumb too — its auto-embed is
    suppressed, so the attachment carries the uniform image. SoundCloud / no-URL
    still return None."""
    from app.core.notify_content import attachment_thumb_url
    fb = {"theme_url": "https://www.facebook.com/HGTV/videos/1/",
          "thumb_url": FB_THUMB}
    assert attachment_thumb_url(fb) == FB_THUMB
    yt_thumb = "https://i.ytimg.com/vi/uJy8KTiMkbI/mqdefault.jpg"
    yt = {"theme_url": "https://www.youtube.com/watch?v=uJy8KTiMkbI",
          "thumb_url": yt_thumb}
    assert attachment_thumb_url(yt) == yt_thumb
    sc = {"theme_url": "https://soundcloud.com/foo/bar", "thumb_url": None}
    assert attachment_thumb_url(sc) is None, "SoundCloud keeps its native unfurl"
    assert attachment_thumb_url({}) is None


# ── _prepare_attachment ──────────────────────────────────────


def test_prepare_attachment_rejects_non_cdn_host():
    from app.core.notify import _prepare_attachment
    assert _prepare_attachment("https://evil.com/x.jpg") is None
    assert _prepare_attachment(
        "https://fbcdn.net.evil.com/x.jpg") is None


def test_prepare_attachment_fetch_and_no_ffmpeg_fallback(
        monkeypatch, tmp_path):
    """Allowlisted host + image response + no ffmpeg → the raw bytes
    land in a temp file the caller can attach + unlink."""
    import httpx
    import shutil
    import os

    class _FakeResp:
        status_code = 200
        headers = {"content-type": "image/jpeg"}
        content = b"\xff\xd8fakejpegbytes"

    class _FakeClient:
        def __init__(self, **kw): ...
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def get(self, url, headers=None): return _FakeResp()

    monkeypatch.setattr(httpx, "Client", _FakeClient)
    monkeypatch.setattr(shutil, "which", lambda _: None)
    from app.core.notify import _prepare_attachment
    path = _prepare_attachment(
        "https://scontent-bos5-1.xx.fbcdn.net/v/x.jpg")
    assert path is not None
    try:
        assert Path(path).read_bytes() == b"\xff\xd8fakejpegbytes"
    finally:
        os.unlink(path)


def test_prepare_attachment_non_image_rejected(monkeypatch):
    import httpx
    class _FakeResp:
        status_code = 200
        headers = {"content-type": "text/html"}
        content = b"<html>login wall</html>"
    class _FakeClient:
        def __init__(self, **kw): ...
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def get(self, url, headers=None): return _FakeResp()
    monkeypatch.setattr(httpx, "Client", _FakeClient)
    from app.core.notify import _prepare_attachment
    assert _prepare_attachment(
        "https://scontent.xx.fbcdn.net/v/x.jpg") is None


# ── threading: dispatch → pool → embedded send ───────────────


def test_dispatch_chain_threads_attach_url():
    assert "attach_url: str | None = None" in NOTIFY_PY
    # dispatch passes it on both the sync and pool paths.
    assert NOTIFY_PY.count("attach_url=attach_url") >= 2
    # the pool worker prepares + cleans up around the embedded send.
    # v0.51.302: the prepare is gated on an embedded sink existing —
    # external-only configs no longer pay the fetch.
    assert ("if (attach_url and urls) else None") in NOTIFY_PY
    assert "_prepare_attachment(attach_url)" in NOTIFY_PY
    assert "attach_path=attach_path" in NOTIFY_PY
    # apprise gets the attachment.
    assert '{"attach": [attach_path]} if attach_path' in NOTIFY_PY


def test_coalescer_carries_single_attach_url():
    assert "single_attach_url: str | None = None" in NOTIFY_PY
    # the bulk buffer keeps the thumb so a lone-bulk flush re-sends it.
    assert '"attach_url": single_attach_url,' in NOTIFY_PY
    # only-when-set kwargs (the v1.20.63 _sync convention) so
    # dispatch() mocks predating the kwarg stay compatible.
    assert '{"attach_url": it["attach_url"]}' in NOTIFY_PY
    # v1.23.46: the immediate single-action path carries the thumb too.
    assert '{"attach_url": single_attach_url}' in NOTIFY_PY


def test_ffmpeg_normalizes_to_embed_cover_crop():
    # v1.23.1: 400x224 cover-crop — full-bleed (no letterbox bars,
    # the v1.23.0 INFO-card look) at the YT embeds' 16:9 footprint
    # (v1.23.0's 4:3 rendered taller than the players beside it).
    # Even dimensions are load-bearing: v1.22.99's 400x225 odd
    # height failed yuv420 JPEG encoding and the silent fallback
    # shipped raw vertical posters — hence 224, not 225.
    assert ("scale=400:224:force_original_aspect_ratio=increase,"
            in NOTIFY_PY)
    assert "crop=400:224" in NOTIFY_PY
    # the fallback now leaves a breadcrumb (class 9).
    assert "notify thumb normalize failed" in NOTIFY_PY


def test_all_five_worker_sites_pass_attachment():
    # v1.24.38: +2 for the theme_auto_restored branches in _do_place /
    # _do_place_collection (review #6 moved the dispatch from the scheduler
    # to the worker's place-success path).
    assert WORKER_PY.count(
        "single_attach_url=_nc.attachment_thumb_url(ctx)") == 7, (
        "theme_backed_up ×1 + theme_pushed ×2 + theme_added ×2 + "
        "theme_auto_restored ×2"
    )
