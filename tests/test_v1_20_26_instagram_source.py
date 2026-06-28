"""v1.20.26 — Instagram as a theme source (reels / posts / IGTV).

the user: add Instagram videos as a theme.mp3 source, treated the same as
YouTube/SoundCloud across the project. Example:
https://www.instagram.com/reel/DUjo2PPgp9f/

Instagram is a USER source only (ThemerrDB never publishes IG URLs), so
it lands as a U / UB row via SET URL. It drops into the existing
non-YouTube yt-dlp download path (like SoundCloud); the id convention is
ig-<shortcode> (CASE-SENSITIVE, mirroring sc-<artist>-<slug>).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()
LIB_HTML = (REPO / "app" / "web" / "templates" / "library.html").read_text()
NOTIFY = (REPO / "app" / "core" / "notify_content.py").read_text()


# ── url_source + extract_video_id (the central functions) ────


def test_url_source_classifies_instagram():
    from app.core.sync import url_source
    assert url_source("https://www.instagram.com/reel/DUjo2PPgp9f/") == "instagram"
    assert url_source("https://instagram.com/p/ABC123_x-y/") == "instagram"
    assert url_source("https://www.instagram.com/someuser/reel/XyZ/") == "instagram"
    assert url_source("https://www.instagram.com/tv/Code99/") == "instagram"
    # non-IG unaffected.
    assert url_source("https://youtu.be/abcdefghijk") == "youtube"
    assert url_source("https://soundcloud.com/artist/track") == "soundcloud"
    assert url_source("https://example.com/x") == "unknown"


def test_extract_video_id_instagram_is_case_preserving():
    from app.core.sync import extract_video_id
    # ig- prefix (mirrors sc-) and the shortcode's CASE is preserved
    # (it's a base64-ish id, unlike the lower-cased SoundCloud slug).
    assert extract_video_id(
        "https://www.instagram.com/reel/DUjo2PPgp9f/") == "ig-DUjo2PPgp9f"
    assert extract_video_id(
        "https://www.instagram.com/user/reel/XyZ_-/") == "ig-XyZ_-"


def test_downloader_source_for_instagram():
    from app.core.downloader import _source_for
    assert _source_for("https://www.instagram.com/reel/DUjo2PPgp9f/") == "instagram"


def test_instagram_failure_classification():
    from app.core.downloader import classify_yt_dlp_error, FailureKind
    # login wall (no IG cookies) → cookies-needed UX
    assert classify_yt_dlp_error(
        "ERROR: [instagram] x: Requested content is not available, rip."
    ) == FailureKind.COOKIES_EXPIRED
    assert classify_yt_dlp_error(
        "ERROR: login required to view this content") == FailureKind.COOKIES_EXPIRED
    # private account
    assert classify_yt_dlp_error(
        "ERROR: This account is private") == FailureKind.VIDEO_PRIVATE


# ── JS mirror + UI wiring ────────────────────────────────────


def test_js_urlsource_mirror_has_instagram():
    assert "INSTAGRAM_URL_RE" in APP_JS
    assert "if (INSTAGRAM_URL_RE.test(url)) return 'instagram'" in APP_JS
    # the THEME_URL_RE union includes Instagram.
    assert "${INSTAGRAM_URL_RE.source}" in APP_JS


def test_js_thumbnail_and_preview_handle_instagram():
    # thumbnail branch shares SoundCloud's oembed-hydration path.
    assert "tUrlSrc === 'soundcloud' || tUrlSrc === 'instagram'" in APP_JS
    # SET URL live preview labels Instagram.
    assert "detected: Instagram" in APP_JS
    # stale ig- ids dropped on YT rows (no img.youtube.com 404).
    assert "ytId.startsWith('sc-') || ytId.startsWith('ig-')" in APP_JS


# ── backend oEmbed (yt-dlp thumbnail) ────────────────────────


def test_oembed_instagram_via_ytdlp():
    assert "def _instagram_preview_via_ytdlp" in API_PY
    # _oembed_source_for recognizes instagram + the fetch branch routes
    # it to yt-dlp (no _OEMBED_PROVIDERS entry).
    assert 'return "instagram"' in API_PY
    # v1.22.90: Facebook joined the same yt-dlp preview branch.
    assert 'if source in ("instagram", "facebook"):' in API_PY


# ── labels ───────────────────────────────────────────────────


def test_labels_mention_instagram():
    assert "Instagram" in NOTIFY  # notify source label
    # v1.22.90: message now ends "or Facebook link" — pin the
    # Instagram mention, not the old tail.
    assert "SoundCloud, Instagram, " in API_PY  # SET URL error message
    assert "instagram.com/reel" in LIB_HTML  # SET URL placeholder


def test_v1_20_26_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
