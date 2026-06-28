"""v1.20.43 — URL-source hardening (audit findings #6 + #7).

#6 url_source/Shorts: the YouTube id regex (sync._YT_VID_RE, used by both
   url_source AND extract_video_id) matched v=/youtu.be//embed/ but NOT
   /shorts/ — so a YouTube Shorts URL was 'unknown' and yielded no id →
   the download failed. The JS YOUTUBE_URL_RE had the inverse gap
   (/shorts/ but no /embed/, no music.). Converged both on
   {v=, youtu.be, /embed/, /shorts/, music.}.

#7 IG SSRF: _oembed_source_for gated on a bare substring
   (`"instagram.com" in url`), so instagram.com.evil.com passed and got
   handed to yt-dlp (blind server-side request). Now classifies by parsed
   hostname. The IG thumbnail fetch also stopped following cross-host
   redirects.
"""
from __future__ import annotations

from pathlib import Path
from urllib.parse import quote

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
AUTH = {"X-Authentik-Username": "testadmin"}


# ── #6 Shorts (behavioral) ───────────────────────────────────


def test_shorts_url_classifies_and_extracts():
    from app.core.sync import url_source, extract_video_id
    u = "https://www.youtube.com/shorts/dQw4w9WgXcQ"
    assert url_source(u) == "youtube"
    assert extract_video_id(u) == "dQw4w9WgXcQ"


def test_existing_youtube_forms_still_work():
    from app.core.sync import url_source, extract_video_id
    for u in ("https://youtu.be/dQw4w9WgXcQ",
              "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
              "https://www.youtube.com/embed/dQw4w9WgXcQ",
              "https://music.youtube.com/watch?v=dQw4w9WgXcQ"):
        assert url_source(u) == "youtube", u
        assert extract_video_id(u) == "dQw4w9WgXcQ", u


def test_js_youtube_re_covers_embed_shorts_music():
    anchor = APP_JS.index("const YOUTUBE_URL_RE")
    line = APP_JS[anchor:anchor + 260]
    assert "shorts" in line
    assert "embed" in line
    assert "music" in line


# ── #7 oembed SSRF (behavioral + pins) ───────────────────────


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
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="testadmin", password="testpassword")
    return TestClient(create_app(settings))


def test_oembed_rejects_spoofed_host(admin_client):
    """instagram.com as a SUBDOMAIN of an attacker host must NOT be
    classified as instagram (it would reach yt-dlp = blind SSRF)."""
    r = admin_client.get(
        "/api/source/oembed?url="
        + quote("http://instagram.com.evil.example/p/x", safe=""),
        headers=AUTH)
    assert r.status_code == 400


def test_oembed_rejects_domain_only_in_query(admin_client):
    r = admin_client.get(
        "/api/source/oembed?url="
        + quote("http://evil.example/?x=youtube.com", safe=""),
        headers=AUTH)
    assert r.status_code == 400


def test_oembed_uses_hostname_not_substring():
    anchor = API_PY.index("def _oembed_source_for(")
    body = API_PY[anchor:anchor + 1700]
    assert "urlparse(url).hostname" in body
    assert '_host_is("instagram.com")' in body
    assert '_host_is("youtube.com")' in body


def test_ig_thumb_no_cross_host_redirect():
    anchor = API_PY.index("def _fetch_ig_thumb_bytes(")
    body = API_PY[anchor:anchor + 1700]
    assert "follow_redirects=False" in body


def test_v1_20_43_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
