"""v1.13.52: regression tests for classify_yt_dlp_error.

The classifier has zero coverage pre-this. This file pins the existing
patterns plus the v1.13.52 additions (rights-holder regional blocks
phrased as "blocked it in your country on copyright grounds").
"""
from __future__ import annotations

import pytest

from app.core.downloader import FailureKind, classify_yt_dlp_error


@pytest.mark.parametrize("msg, kind", [
    # private
    ("This video is private.", FailureKind.VIDEO_PRIVATE),
    ("ERROR: Private video", FailureKind.VIDEO_PRIVATE),
    # removed
    ("Video unavailable. The uploader has not made this video available.",
     FailureKind.VIDEO_REMOVED),
    ("This video has been removed by the user.", FailureKind.VIDEO_REMOVED),
    ("This video has been removed.", FailureKind.VIDEO_REMOVED),
    ("Video is no longer available.", FailureKind.VIDEO_REMOVED),
    ("Video is unavailable.", FailureKind.VIDEO_REMOVED),
    ("This video is no longer available because the YouTube account "
     "associated with this video has been terminated.",
     FailureKind.VIDEO_REMOVED),
    # age-restricted
    ("Sign in to confirm your age. This video may be inappropriate for "
     "some users.", FailureKind.VIDEO_AGE_RESTRICTED),
    ("This video has been age-restricted.", FailureKind.VIDEO_AGE_RESTRICTED),
    ("This video may be inappropriate for some audiences.",
     FailureKind.VIDEO_AGE_RESTRICTED),
    # geo / regional blocks (v1.13.52 adds the SME pattern)
    ("This video is not available in your country.",
     FailureKind.GEO_BLOCKED),
    ("Geo-restriction error.", FailureKind.GEO_BLOCKED),
    ("This region is blocked.", FailureKind.GEO_BLOCKED),
    ("This video contains content from SME, who has blocked it in "
     "your country on copyright grounds.",
     FailureKind.GEO_BLOCKED),
    ("Blocked on copyright grounds.", FailureKind.GEO_BLOCKED),
    # cookies
    ("Sign in to confirm you're not a bot. Use --cookies to pass auth.",
     FailureKind.COOKIES_EXPIRED),
    ("Could not load cookies file.", FailureKind.COOKIES_EXPIRED),
    # network
    ("HTTP Error 503: Service Unavailable", FailureKind.NETWORK_ERROR),
    ("Connection reset by peer.", FailureKind.NETWORK_ERROR),
    ("Read timeout.", FailureKind.NETWORK_ERROR),
    # unknown — empty / unrecognized falls through
    ("", FailureKind.UNKNOWN),
    (None, FailureKind.UNKNOWN),
    ("Some weird error nobody has seen before.", FailureKind.UNKNOWN),
])
def test_classify_patterns(msg, kind):
    assert classify_yt_dlp_error(msg) == kind


def test_private_takes_precedence_over_unavailable():
    # "Video unavailable" + "private" should go PRIVATE, not REMOVED.
    msg = "Video unavailable: this video is private."
    assert classify_yt_dlp_error(msg) == FailureKind.VIDEO_PRIVATE


# v1.13.53: bypass-options wiring tests. Pin that the new
# geo_bypass / geo_bypass_country / proxy_url params land in the
# yt-dlp opts dict only when configured, with normalized values.
from pathlib import Path
from app.core.downloader import _opts


def test_opts_no_bypass_when_disabled(tmp_path: Path):
    opts = _opts(output_path=tmp_path / "theme",
                 cookies_file=None, audio_quality="0")
    assert "geo_bypass" not in opts
    assert "geo_bypass_country" not in opts
    assert "proxy" not in opts


def test_opts_geo_bypass_enabled(tmp_path: Path):
    opts = _opts(output_path=tmp_path / "theme",
                 cookies_file=None, audio_quality="0",
                 geo_bypass=True)
    assert opts["geo_bypass"] is True


def test_opts_country_normalized(tmp_path: Path):
    # Lowercase + whitespace input → uppercase, trimmed.
    opts = _opts(output_path=tmp_path / "theme",
                 cookies_file=None, audio_quality="0",
                 geo_bypass=True, geo_bypass_country="  us  ")
    assert opts["geo_bypass_country"] == "US"


def test_opts_country_omitted_when_empty(tmp_path: Path):
    opts = _opts(output_path=tmp_path / "theme",
                 cookies_file=None, audio_quality="0",
                 geo_bypass=True, geo_bypass_country="")
    assert "geo_bypass_country" not in opts


def test_opts_proxy_url(tmp_path: Path):
    opts = _opts(output_path=tmp_path / "theme",
                 cookies_file=None, audio_quality="0",
                 proxy_url="socks5://user:pass@example.com:1080")
    assert opts["proxy"] == "socks5://user:pass@example.com:1080"


def test_opts_proxy_url_trimmed(tmp_path: Path):
    opts = _opts(output_path=tmp_path / "theme",
                 cookies_file=None, audio_quality="0",
                 proxy_url="  http://proxy.example.com:8080  ")
    assert opts["proxy"] == "http://proxy.example.com:8080"


def test_opts_proxy_url_omitted_when_empty(tmp_path: Path):
    opts = _opts(output_path=tmp_path / "theme",
                 cookies_file=None, audio_quality="0",
                 proxy_url="")
    assert "proxy" not in opts
