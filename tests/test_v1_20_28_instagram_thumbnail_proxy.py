"""v1.20.28 — proxy Instagram CDN thumbnails (broken-image fix).

the user reported the INFO-card thumbnail rendering broken for an
Instagram download. Root cause: _instagram_preview_via_ytdlp returns
the raw scontent.*.cdninstagram.com thumbnail url, and IG's CDN 403s
cross-origin <img> hotlinks (signed url + referer check). YouTube
(img.youtube.com) and SoundCloud (i1.sndcdn.com) both allow hotlinks,
so only IG breaks.

Fix: the oembed endpoint hands the frontend a same-origin proxy url
(/api/source/ig-thumbnail?url=<ig post url>); the proxy resolves the
raw CDN url via the cached oembed path, fetches it server-side (SSRF-
guarded to IG CDN hosts), and streams the bytes back.
"""
from __future__ import annotations

from pathlib import Path
from urllib.parse import quote

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()

IG_POST = "https://www.instagram.com/reel/DUjo2PPgp9f/"
RAW_THUMB = "https://scontent-lax3-1.cdninstagram.com/v/abc/thumb.jpg?sig=xyz"
JPEG_BYTES = b"\xff\xd8\xff\xe0FAKEJPEGDATA\xff\xd9"
AUTH = {"X-Authentik-Username": "testadmin"}


# ── fixtures / mocks ─────────────────────────────────────────


def _fake_ydl(thumb: str):
    class _FakeYDL:
        def __init__(self, opts):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def extract_info(self, url, download=False):
            return {"title": "clip", "uploader": "someone", "thumbnail": thumb}
    return _FakeYDL


def _fake_httpx(status=200, ctype="image/jpeg", content=JPEG_BYTES,
                on_get=None):
    class _Resp:
        status_code = status
        headers = {"content-type": ctype}

        @property
        def content(self):
            return content

    class _Client:
        def __init__(self, **kw):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def get(self, url, headers=None, params=None):
            if on_get is not None:
                on_get(url)
            return _Resp()
    return _Client


@pytest.fixture
def admin_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    return TestClient(create_app(settings))


# ── behavioral: oembed rewrites IG thumbnail to the proxy ────


def test_oembed_rewrites_ig_thumbnail_to_same_origin_proxy(
        admin_client, monkeypatch):
    import yt_dlp
    monkeypatch.setattr(yt_dlp, "YoutubeDL", _fake_ydl(RAW_THUMB))
    r = admin_client.get(
        f"/api/source/oembed?url={quote(IG_POST, safe='')}", headers=AUTH)
    assert r.status_code == 200
    body = r.json()
    # The frontend must receive the same-origin proxy path, NOT the raw
    # CDN url (which would 403 on a cross-origin <img> load).
    assert body["thumbnail_url"].startswith("/api/source/ig-thumbnail?url=")
    assert "cdninstagram" not in body["thumbnail_url"]
    # Other fields untouched.
    assert body["author_name"] == "someone"


def test_oembed_does_not_poison_cache_with_proxy_url(
        admin_client, monkeypatch):
    """The rewrite must copy the dict — _fetch_oembed returns the cached
    object by reference, and the proxy reads the RAW url back out of that
    same cache. If the endpoint mutated it in place, a second oembed call
    (or the proxy's own resolve) would see the proxy url → self-reference
    → SSRF-guard 404 on a perfectly valid thumbnail. Two sequential
    oembed calls must both return a CDN-derived proxy, proving the cache
    still holds the raw url."""
    import yt_dlp
    monkeypatch.setattr(yt_dlp, "YoutubeDL", _fake_ydl(RAW_THUMB))
    first = admin_client.get(
        f"/api/source/oembed?url={quote(IG_POST, safe='')}",
        headers=AUTH).json()
    second = admin_client.get(
        f"/api/source/oembed?url={quote(IG_POST, safe='')}",
        headers=AUTH).json()
    assert first["thumbnail_url"] == second["thumbnail_url"]
    assert first["thumbnail_url"].startswith("/api/source/ig-thumbnail")
    # No proxy-of-a-proxy nesting: the cached url is the RAW CDN url, so
    # the proxy path's embedded url must be the IG POST url, never another
    # ig-thumbnail path. A poisoned cache would double-wrap here.
    assert first["thumbnail_url"].count("ig-thumbnail") == 1
    assert "instagram.com" in quote(first["thumbnail_url"], safe='') or \
        "instagram" in first["thumbnail_url"]


# ── behavioral: the proxy endpoint streams bytes ────────────


def test_ig_thumbnail_proxy_streams_image_bytes(admin_client, monkeypatch):
    import yt_dlp
    import httpx
    monkeypatch.setattr(yt_dlp, "YoutubeDL", _fake_ydl(RAW_THUMB))
    monkeypatch.setattr(httpx, "Client", _fake_httpx())
    r = admin_client.get(
        f"/api/source/ig-thumbnail?url={quote(IG_POST, safe='')}",
        headers=AUTH)
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("image/jpeg")
    assert r.content == JPEG_BYTES


def test_ig_thumbnail_proxy_rejects_non_instagram_url(admin_client):
    """The post url must be Instagram — a YouTube url to this endpoint is
    a 400, never proxied."""
    r = admin_client.get(
        f"/api/source/ig-thumbnail?url={quote('https://youtu.be/abcdefghijk', safe='')}",
        headers=AUTH)
    assert r.status_code == 400


def test_ig_thumbnail_proxy_ssrf_guard_blocks_non_cdn_target(
        admin_client, monkeypatch):
    """SSRF defense: if yt-dlp (compromised, or IG changes its metadata
    shape) hands back a thumbnail pointing at a non-CDN host — e.g. the
    cloud metadata endpoint — the proxy must NOT fetch it. httpx is
    monkeypatched to fail loud if called."""
    import yt_dlp
    import httpx
    evil = "http://169.254.169.254/latest/meta-data/iam/"
    monkeypatch.setattr(yt_dlp, "YoutubeDL", _fake_ydl(evil))

    def _boom(url):
        raise AssertionError(f"SSRF: proxy fetched non-CDN host {url}")
    monkeypatch.setattr(httpx, "Client", _fake_httpx(on_get=_boom))
    r = admin_client.get(
        f"/api/source/ig-thumbnail?url={quote(IG_POST, safe='')}",
        headers=AUTH)
    assert r.status_code == 404


def test_ig_thumbnail_proxy_404s_on_upstream_failure(
        admin_client, monkeypatch):
    """A CDN 403/404 (expired signed url) surfaces as a 404 from the
    proxy so the frontend onerror handler hides the wrapper."""
    import yt_dlp
    import httpx
    monkeypatch.setattr(yt_dlp, "YoutubeDL", _fake_ydl(RAW_THUMB))
    monkeypatch.setattr(httpx, "Client", _fake_httpx(status=403))
    r = admin_client.get(
        f"/api/source/ig-thumbnail?url={quote(IG_POST, safe='')}",
        headers=AUTH)
    assert r.status_code == 404


# ── source pins ──────────────────────────────────────────────


def test_proxy_helper_and_endpoint_defined():
    assert "def _fetch_ig_thumb_bytes" in API_PY
    assert '@app.get("/api/source/ig-thumbnail")' in API_PY
    assert "async def api_ig_thumbnail" in API_PY


def test_ssrf_host_allowlist_pinned():
    """The CDN host allowlist is the SSRF source-of-truth — pin both
    hosts IG serves thumbnails from."""
    anchor = API_PY.index("_IG_THUMB_HOSTS = (")
    block = API_PY[anchor:anchor + 120]
    assert "cdninstagram.com" in block
    assert "fbcdn.net" in block


def test_oembed_rewrite_copies_dict_not_mutates():
    """Both oembed endpoints must build a NEW dict ({**data, ...}) for
    the IG rewrite — mutating the cached dict in place poisons the cache
    (see test_oembed_does_not_poison_cache_with_proxy_url)."""
    # Each occurrence of the rewrite sits next to a {**data, dict-copy.
    rewrites = API_PY.count('"/api/source/ig-thumbnail?url=')
    copies = API_PY.count('data = {**data, "thumbnail_url":')
    # One rewrite + one copy in each of api_source_oembed and the alias;
    # plus the endpoint string itself in the route decorator.
    assert copies >= 2, "both oembed endpoints must copy before rewrite"
    assert rewrites >= 2


def test_frontend_binds_error_handler_before_src():
    """hydrateSourceThumbnails must attach the 'error' listener BEFORE
    setting img.src so a failed proxy load re-hides the wrapper instead
    of leaving a broken-image icon."""
    anchor = APP_JS.index("async function hydrateSourceThumbnails(root)")
    body = APP_JS[anchor:anchor + 1400]
    err_idx = body.index("addEventListener('error'")
    src_idx = body.index("img.src = data.thumbnail_url")
    assert err_idx < src_idx, "error listener must be bound before src"


def test_set_url_preview_handles_instagram():
    """The SET URL live preview thumbnail extends to Instagram (parity
    with SoundCloud) — its thumbnail_url is the same-origin proxy."""
    assert "src === 'soundcloud' || src === 'instagram'" in APP_JS


def test_v1_20_28_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
