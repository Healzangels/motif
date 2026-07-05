"""v0.51.74 — SSRF: yt-dlp sinks only fetch from supported platform hosts.

Security-audit finding (medium, malicious-external-data). ThemerrDB (public,
community-editable) supplies youtube_theme_url values that motif stores verbatim
and later feeds to yt-dlp in two places: probe_youtube_url (extract_info) and
download_theme (download). The video-id gate used _YT_VID_RE, which is UNANCHORED
— it matches `v=`/`/embed/`/`/shorts/` + 11 chars on ANY host — so
`http://169.254.169.254/embed/aaaaaaaaaaa` minted a fake id and reached yt-dlp's
generic extractor → blind SSRF (attacker-directed server GETs to cloud-metadata /
LAN / localhost, firing unattended on cron sync when auto-download is on).

v0.51.74 adds is_fetchable_theme_url() — an http(s) + host-allowlist gate on the
four supported platforms — at BOTH yt-dlp chokepoints. The four platforms are the
only sources the app supports, so no legitimate URL is rejected.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from app.core.downloader import (
    DownloadError,
    FailureKind,
    download_theme,
    is_fetchable_theme_url,
    probe_youtube_url,
)

ALLOWED = [
    "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
    "https://youtube.com/watch?v=dQw4w9WgXcQ",
    "https://m.youtube.com/watch?v=dQw4w9WgXcQ",
    "https://music.youtube.com/watch?v=dQw4w9WgXcQ",
    "https://youtu.be/dQw4w9WgXcQ",
    "https://www.youtube-nocookie.com/embed/dQw4w9WgXcQ",
    "https://soundcloud.com/artist/track",
    "https://www.instagram.com/reel/Abc123/",
    "https://www.facebook.com/watch?v=123456789",
    "https://fb.watch/aBcDeF/",
    "http://youtube.com/watch?v=x",  # scheme http is allowed (behind Authentik/LAN)
]

BLOCKED = [
    "http://169.254.169.254/embed/aaaaaaaaaaa",   # cloud metadata — the SSRF PoC
    "http://127.0.0.1/embed/aaaaaaaaaaa",
    "http://localhost/shorts/aaaaaaaaaaa",
    "http://192.168.1.1/shorts/adminconsole",
    "http://10.0.0.1/embed/aaaaaaaaaaa",
    "http://evil.com/watch?v=aaaaaaaaaaa",         # arbitrary host with a v= param
    "http://youtube.com.attacker.com/watch?v=x",   # suffix-spoof host
    "http://ayoutube.com/watch?v=x",               # not a youtube subdomain
    "file:///etc/passwd",
    "ftp://youtube.com/x",                         # non-http scheme
    "gopher://127.0.0.1:6379/x",
    "",
    None,
]


@pytest.mark.parametrize("url", ALLOWED)
def test_allowlist_accepts_platform_urls(url):
    assert is_fetchable_theme_url(url) is True, url


@pytest.mark.parametrize("url", BLOCKED)
def test_allowlist_rejects_non_platform_urls(url):
    assert is_fetchable_theme_url(url) is False, url


def test_probe_rejects_ssrf_url_without_network():
    # is_fetchable_theme_url returns False → early FailureKind.UNKNOWN, no yt-dlp.
    # (If the gate were missing this would attempt a real GET to 169.254.169.254.)
    assert probe_youtube_url("http://169.254.169.254/embed/aaaaaaaaaaa") is FailureKind.UNKNOWN


def test_download_rejects_ssrf_url(tmp_path):
    with pytest.raises(DownloadError, match="SSRF guard"):
        download_theme(
            youtube_url="http://169.254.169.254/embed/aaaaaaaaaaa",
            video_id="aaaaaaaaaaa",
            output_dir=tmp_path,
            cookies_file=None,
        )
    # the guard must fire BEFORE any filesystem work (no output dir created).
    assert not any(tmp_path.iterdir()), "download gate must reject before mkdir/write"
