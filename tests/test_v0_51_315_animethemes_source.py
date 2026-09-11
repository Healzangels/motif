"""v0.51.315 — AnimeThemes hosted audio as a theme source (5th source).

Feature brief #2 candidate B, tag 2 (docs/specs/ANIMETHEMES_SPEC.md §3.4).
Form: https://a.animethemes.moe/CowboyBebop-OP1.ogg — the `audio.link` the
v0.51.314 resolver returns. A USER source only (ThemerrDB never publishes
these), so it lands as a U row via SET URL and rides the existing
non-YouTube yt-dlp path (generic extractor → FFmpegExtractAudio → mp3).
The id convention is at-<basename> (case preserved: the basename IS the
catalogue slug). Structure mirrors test_v1_22_90_facebook_source.py — that
file is the checklist of sites a new source kind must touch.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()
LIB_HTML = (REPO / "app" / "web" / "templates" / "library.html").read_text()
NOTIFY = (REPO / "app" / "core" / "notify_content.py").read_text()
AT = "https://a.animethemes.moe/CowboyBebop-OP1.ogg"


# ── url_source + extract_video_id ────────────────────────────


def test_url_source_classifies_animethemes_audio_only():
    from app.core.sync import url_source
    assert url_source(AT) == "animethemes"
    assert url_source("http://a.animethemes.moe/Bleach-OP1v2.ogg") == "animethemes"
    assert url_source(AT + "?t=1") == "animethemes"
    for ext in ("oga", "opus", "flac", "mp3", "m4a"):
        assert url_source(f"https://a.animethemes.moe/X-OP1.{ext}") == "animethemes"
    # video files and the API/web hosts are NOT theme sources.
    assert url_source("https://a.animethemes.moe/CowboyBebop-OP1.webm") == "unknown"
    assert url_source("https://v.animethemes.moe/CowboyBebop-OP1.webm") == "unknown"
    assert url_source("https://api.animethemes.moe/anime") == "unknown"
    assert url_source("https://animethemes.moe/anime/cowboy_bebop") == "unknown"
    # host-anchored — lookalikes and path smuggling don't classify.
    assert url_source("https://a.animethemes.moe.evil.com/x.ogg") == "unknown"
    assert url_source("https://evil.com/?u=https://a.animethemes.moe/x.ogg") == "unknown"
    # the other sources are unaffected.
    assert url_source("https://www.youtube.com/watch?v=dQw4w9WgXcQ") == "youtube"
    assert url_source("https://fb.watch/abC1_2-xyz/") == "facebook"


def test_extract_video_id_keeps_the_slug_case_and_caps_length():
    from app.core.sync import extract_video_id
    assert extract_video_id(AT) == "at-CowboyBebop-OP1"
    assert extract_video_id("https://a.animethemes.moe/Bleach-OP1v2.ogg") == "at-Bleach-OP1v2"
    long = "https://a.animethemes.moe/" + "A" * 90 + ".ogg"
    assert extract_video_id(long) == "at-" + "A" * 60
    assert extract_video_id("https://a.animethemes.moe/CowboyBebop-OP1.webm") is None


def test_downloader_mirror_and_allowlist():
    from app.core.downloader import _FETCH_ALLOWED_HOSTS, _source_for, is_fetchable_theme_url
    assert _source_for(AT) == "animethemes"
    assert is_fetchable_theme_url(AT)
    assert "a.animethemes.moe" in _FETCH_ALLOWED_HOSTS
    # the API host is deliberately NOT a download target (spec §3.4 gotcha).
    assert not is_fetchable_theme_url("https://api.animethemes.moe/anime")
    assert not is_fetchable_theme_url("https://animethemes.moe/x.ogg")
    assert not is_fetchable_theme_url("https://a.animethemes.moe.evil.com/x.ogg")


def test_download_opts_take_the_non_youtube_path_and_still_make_mp3(tmp_path):
    from app.core.downloader import _opts
    o = _opts(output_path=tmp_path / "theme.mp3", cookies_file=None, source="animethemes")
    assert "js_runtimes" not in o and "remote_components" not in o and "extractor_args" not in o, (
        "generic-extractor direct media: the YouTube-only opts add 1-3s for nothing (v1.14.5)")
    assert o["postprocessors"][0]["key"] == "FFmpegExtractAudio"
    assert o["postprocessors"][0]["preferredcodec"] == "mp3", "the .ogg must become theme.mp3 like every other source"


def test_provider_health_has_its_own_lane():
    from app.core.provider_health import PROVIDERS, provider_for_url
    assert "animethemes" in PROVIDERS and PROVIDERS[-1] == "other"
    assert provider_for_url(AT) == "animethemes"
    assert provider_for_url("https://api.animethemes.moe/anime") == "other"


# ── JS mirror + UI wiring ────────────────────────────────────


def test_js_urlsource_mirror_has_animethemes():
    assert "ANIMETHEMES_URL_RE" in APP_JS
    assert "if (ANIMETHEMES_URL_RE.test(url)) return 'animethemes'" in APP_JS
    assert "${ANIMETHEMES_URL_RE.source}" in APP_JS
    # order: host-anchored sources before the YouTube test (mirrors sync.py).
    assert APP_JS.index("ANIMETHEMES_URL_RE.test(url)") < APP_JS.index("if (YOUTUBE_URL_RE.test(url)) return 'youtube'")


def test_js_preview_labels_and_prefix_sites():
    assert "detected: AnimeThemes" in APP_JS
    assert "Facebook, or AnimeThemes)" in APP_JS, "the not-recognized hint names every accepted source"
    assert APP_JS.count("svid.startsWith('at-')") == 5, (
        "the three family sites classify at- as a user URL (mirrors SQL) + the two v0.51.329 "
        "dedicated AT branches (computeSrcLetter + the inline render)")
    assert "ytId.startsWith('at-')" in APP_JS
    i = APP_JS.index("} else if (currentSrc === 'animethemes') {")
    blk = APP_JS[i:APP_JS.index("} else if", i + 10)]
    assert "currentUrl = currentRawUrl;" in blk and "currentVid = currentVidFromLf || '';" in blk, (
        "the diff tile preserves the URL verbatim like SoundCloud (no YouTube id to canonicalize)")


# ── SQL mirror ───────────────────────────────────────────────


def test_src_sql_classifies_at_as_url_in_both_variants():
    from app.web.api import _LIB_SRC_LETTER_SQL, _SRC_LETTER_SQL
    assert "LIKE 'at-%'" in _SRC_LETTER_SQL and "LIKE 'at-%'" in _LIB_SRC_LETTER_SQL


# ── preview endpoint: synthesized, no network ────────────────


def test_oembed_preview_is_synthesized_from_the_slug(tmp_path, monkeypatch):
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    from fastapi.testclient import TestClient
    import httpx
    (tmp_path / "data").mkdir()
    (tmp_path / "motif.yaml").write_text("paths: {}\n")
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path); init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    client = TestClient(create_app(s))
    calls = []

    class _NoNet:
        def __init__(self, *a, **kw):
            calls.append(kw)

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def get(self, *a, **kw):
            raise AssertionError("the AnimeThemes preview must not hit the network")
    monkeypatch.setattr(httpx, "Client", _NoNet)
    r = client.get("/api/source/oembed", params={"url": AT}, headers={"X-Authentik-Username": "testadmin"})
    assert r.status_code == 200, r.text
    assert r.json() == {"title": "CowboyBebop-OP1", "author_name": "AnimeThemes.moe",
                        "author_url": "https://animethemes.moe/", "thumbnail_url": None}
    assert calls == []
    # the thumbnail proxy still refuses it (no thumb, not an IG/FB host).
    r2 = client.get("/api/source/ig-thumbnail", params={"url": AT}, headers={"X-Authentik-Username": "testadmin"})
    assert r2.status_code == 400


# ── labels ───────────────────────────────────────────────────


def test_yt_thumb_guard_excludes_at_prefix():
    from app.core.notify_content import _youtube_thumb
    assert _youtube_thumb("at-12345678") is None
    assert _youtube_thumb("dQw4w9WgXcQ") is not None


def test_labels_mention_animethemes():
    assert 'plat = "AnimeThemes"' in NOTIFY
    assert "URL must be a YouTube, SoundCloud, Instagram, " in API_PY
    assert "Facebook, or AnimeThemes link" in API_PY
    assert "a.animethemes.moe" in LIB_HTML  # SET URL placeholder
    assert "Facebook, or AnimeThemes URL" in LIB_HTML  # dialog hint


def test_v0_51_315_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "0.51.315: " in init_py
