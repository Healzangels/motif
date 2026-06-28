"""v1.22.90 — Facebook videos as a theme source (4th source).

the user: "similar to instagram, soundcloud, youtube we could also
download facebook videos for theme.mp3". Example:
https://www.facebook.com/HGTV/videos/castle-impossible-once-upon-a-time/705368522162731/

Facebook is a USER source only (ThemerrDB never publishes FB URLs), so
it lands as a U / UB row via SET URL. It drops into the existing
non-YouTube yt-dlp download path (like Instagram); the id convention is
fb-<id> (numeric for /videos//watch/reel, case-sensitive code for
share/fb.watch — mirroring ig-<shortcode>).

Critical ordering note: sync's _YT_VID_RE matches an UNANCHORED `v=`,
so facebook.com/watch?v=<id> classified as YouTube (id truncated to 11
chars) until url_source/extract_video_id learned to check the
host-anchored _FB_URL_RE first.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()
LIB_HTML = (REPO / "app" / "web" / "templates" / "library.html").read_text()
NOTIFY = (REPO / "app" / "core" / "notify_content.py").read_text()


# ── url_source + extract_video_id (the central functions) ────


def test_url_source_classifies_facebook_all_forms():
    from app.core.sync import url_source
    assert url_source(
        "https://www.facebook.com/HGTV/videos/"
        "castle-impossible-once-upon-a-time/705368522162731/"
    ) == "facebook"
    assert url_source(
        "https://www.facebook.com/HGTV/videos/705368522162731/"
    ) == "facebook"
    assert url_source(
        "https://www.facebook.com/reel/3513572898933526") == "facebook"
    assert url_source(
        "https://www.facebook.com/share/v/1aBcD2eFgH/") == "facebook"
    assert url_source("https://fb.watch/abC1_2-xyz/") == "facebook"
    assert url_source(
        "https://m.facebook.com/video.php?v=10153231379946729"
    ) == "facebook"
    # non-FB unaffected.
    assert url_source("https://youtu.be/abcdefghijk") == "youtube"
    assert url_source("https://soundcloud.com/artist/track") == "soundcloud"
    assert url_source(
        "https://www.instagram.com/reel/DUjo2PPgp9f/") == "instagram"
    # non-video FB pages are NOT theme sources.
    assert url_source("https://www.facebook.com/HGTV/") == "unknown"
    assert url_source("https://www.facebook.com/events/123/") == "unknown"
    # host-anchored — lookalike hosts don't classify.
    assert url_source(
        "https://facebook.com.evil.com/watch?v=123") == "unknown"


def test_watch_form_beats_the_unanchored_yt_regex():
    """THE bug this feature would have shipped with: _YT_VID_RE matches
    a bare `v=` anywhere, so facebook.com/watch?v=<id> classified as
    YouTube with the id truncated to 11 chars. FB must be checked
    first (its regex is host-anchored, so it can't steal YT URLs)."""
    from app.core.sync import url_source, extract_video_id
    assert url_source(
        "https://www.facebook.com/watch?v=705368522162731") == "facebook"
    assert extract_video_id(
        "https://www.facebook.com/watch?v=705368522162731"
    ) == "fb-705368522162731"
    # params before v= tolerated.
    assert url_source(
        "https://www.facebook.com/watch/?ref=saved&v=705368522162731"
    ) == "facebook"
    # real YouTube still wins its own URLs.
    assert url_source(
        "https://www.youtube.com/watch?v=dQw4w9WgXcQ") == "youtube"
    assert extract_video_id(
        "https://www.youtube.com/watch?v=dQw4w9WgXcQ") == "dQw4w9WgXcQ"


def test_extract_video_id_facebook_forms():
    from app.core.sync import extract_video_id
    assert extract_video_id(
        "https://www.facebook.com/HGTV/videos/"
        "castle-impossible-once-upon-a-time/705368522162731/"
    ) == "fb-705368522162731"
    assert extract_video_id(
        "https://www.facebook.com/reel/3513572898933526"
    ) == "fb-3513572898933526"
    # share/fb.watch codes are CASE-SENSITIVE (preserved, like ig-).
    assert extract_video_id(
        "https://www.facebook.com/share/v/1aBcD2eFgH/") == "fb-1aBcD2eFgH"
    assert extract_video_id("https://fb.watch/abC1_2-xyz/") == "fb-abC1_2-xyz"


def test_downloader_source_for_facebook():
    from app.core.downloader import _source_for
    assert _source_for(
        "https://www.facebook.com/HGTV/videos/705368522162731/"
    ) == "facebook"
    assert _source_for("https://fb.watch/abC1_2-xyz/") == "facebook"


def test_facebook_failure_classification():
    from app.core.downloader import classify_yt_dlp_error, FailureKind
    # login/age wall → cookies-needed UX (same recovery card as IG).
    assert classify_yt_dlp_error(
        "ERROR: [facebook] 123: This video is only available "
        "for registered users") == FailureKind.COOKIES_EXPIRED
    assert classify_yt_dlp_error(
        "ERROR: [facebook] You must log in to continue."
    ) == FailureKind.COOKIES_EXPIRED


# ── JS mirror + UI wiring ────────────────────────────────────


def test_js_urlsource_mirror_has_facebook():
    assert "FACEBOOK_URL_RE" in APP_JS
    assert "if (FACEBOOK_URL_RE.test(url)) return 'facebook'" in APP_JS
    # the THEME_URL_RE union includes Facebook.
    assert "${FACEBOOK_URL_RE.source}" in APP_JS


def test_js_thumbnail_and_preview_handle_facebook():
    # INFO-card thumbnail branch joins the oembed-hydration path.
    assert "tUrlSrc === 'facebook'" in APP_JS
    # SET URL live preview labels Facebook.
    assert "detected: Facebook" in APP_JS
    # stale fb- ids dropped on YT rows (no img.youtube.com 404).
    assert "ytId.startsWith('fb-')" in APP_JS


def test_js_svid_sites_classify_fb_as_url():
    # all three svid-classification sites treat fb- as a user URL
    # (mirrors the SQL LIKE 'fb-%' branch).
    assert APP_JS.count("svid.startsWith('fb-')") == 3


# ── SQL mirror ───────────────────────────────────────────────


def test_src_sql_classifies_fb_as_url():
    from app.web.api import _SRC_LETTER_SQL
    assert "LIKE 'fb-%'" in _SRC_LETTER_SQL


# ── backend oEmbed (yt-dlp thumbnail) + proxy ────────────────


def test_oembed_facebook_via_ytdlp():
    # _oembed_source_for recognizes facebook + fb.watch, and the fetch
    # branch routes both IG and FB to yt-dlp (no _OEMBED_PROVIDERS
    # entry for either).
    assert 'return "facebook"' in API_PY
    assert '_host_is("facebook.com") or _host_is("fb.watch")' in API_PY
    assert 'if source in ("instagram", "facebook"):' in API_PY


def test_thumbnail_proxy_accepts_facebook():
    # the same-origin thumbnail proxy serves FB too (both platforms'
    # thumbs live on the fbcdn.net CDN family _IG_THUMB_HOSTS already
    # allowlists), and both oembed endpoints rewrite to it.
    assert ('_oembed_source_for(url) not in ("instagram", "facebook")'
            in API_PY)
    assert API_PY.count(
        'if source in ("instagram", "facebook") and '
        'data.get("thumbnail_url"):') == 2


def test_yt_thumb_guard_excludes_prefixed_ids():
    # an 8-char fb-/ig- id makes an 11-char prefixed value that slips
    # the length check — the notify thumb guard must prefix-filter.
    from app.core.notify_content import _youtube_thumb
    assert _youtube_thumb("fb-12345678") is None
    assert _youtube_thumb("ig-12345678") is None
    assert _youtube_thumb("dQw4w9WgXcQ") is not None


# ── labels ───────────────────────────────────────────────────


def test_labels_mention_facebook():
    assert "Facebook" in NOTIFY  # notify source label
    # SET URL error message (literal wraps across two source lines).
    assert "URL must be a YouTube, SoundCloud, Instagram, " in API_PY
    assert "or Facebook link" in API_PY
    assert "facebook.com" in LIB_HTML  # SET URL placeholder
    assert "Instagram, or Facebook URL" in LIB_HTML  # dialog hint
