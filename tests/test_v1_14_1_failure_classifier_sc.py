"""v1.14.1 — failure classifier widened for SoundCloud errors.

Pre-fix `classify_yt_dlp_error` had YouTube-string-shaped patterns
("private video", "video unavailable", etc.). SoundCloud failures
fell through to FailureKind.UNKNOWN, which is treated as transient
→ infinite retry-with-backoff loop until max_attempts.

Per-source human labels also said "YouTube" / "Video" — wrong on
a SoundCloud failure. v1.14.1 makes them source-agnostic
("Track or video", "Cookies missing").

Tests pin:
- YouTube patterns still classify correctly (regression guard)
- SoundCloud-specific phrasings classify to the right kind
- Human labels are source-agnostic
"""
from __future__ import annotations

from app.core.downloader import FailureKind, classify_yt_dlp_error


# ── YouTube regression guards (must not break) ────────────────


def test_yt_private_video_still_classifies():
    assert classify_yt_dlp_error(
        "ERROR: [youtube] xyz: Private video. Sign in if you've been granted access."
    ) == FailureKind.VIDEO_PRIVATE


def test_yt_video_unavailable_still_classifies():
    assert classify_yt_dlp_error(
        "ERROR: [youtube] xyz: Video unavailable"
    ) == FailureKind.VIDEO_REMOVED


def test_yt_geo_blocked_still_classifies():
    assert classify_yt_dlp_error(
        "ERROR: This video is not available in your country"
    ) == FailureKind.GEO_BLOCKED


# ── SoundCloud patterns (the v1.14.1 additions) ───────────────


def test_sc_private_track_classifies_as_private():
    assert classify_yt_dlp_error(
        "ERROR: [soundcloud] 12345: This track is private"
    ) == FailureKind.VIDEO_PRIVATE


def test_sc_track_is_private_classifies():
    """Variant phrasing — yt-dlp may use either."""
    assert classify_yt_dlp_error(
        "ERROR: track is private"
    ) == FailureKind.VIDEO_PRIVATE


def test_sc_track_removed_classifies_as_removed():
    assert classify_yt_dlp_error(
        "ERROR: [soundcloud] xyz: This track has been removed"
    ) == FailureKind.VIDEO_REMOVED


def test_sc_track_no_longer_available_classifies_as_removed():
    assert classify_yt_dlp_error(
        "ERROR: [soundcloud] xyz: Track is no longer available"
    ) == FailureKind.VIDEO_REMOVED


def test_sc_track_unavailable_classifies_as_removed():
    assert classify_yt_dlp_error(
        "ERROR: [soundcloud] xyz: Track unavailable"
    ) == FailureKind.VIDEO_REMOVED


def test_sc_track_not_available_classifies_as_removed():
    assert classify_yt_dlp_error(
        "ERROR: [soundcloud] xyz: Track not available"
    ) == FailureKind.VIDEO_REMOVED


def test_sc_geo_classifies_as_geo_blocked():
    assert classify_yt_dlp_error(
        "ERROR: [soundcloud] xyz: This track is not available in your location"
    ) == FailureKind.GEO_BLOCKED


def test_sc_premium_classifies_as_cookies_expired():
    """Go+ / paid tracks bucket into COOKIES_EXPIRED so the
    recovery card surfaces SET URL (find an alternative) +
    DROP cookies.txt (if user has a Go+ subscription)."""
    assert classify_yt_dlp_error(
        "ERROR: [soundcloud] xyz: Go+ subscription required"
    ) == FailureKind.COOKIES_EXPIRED


def test_sc_subscription_required_classifies():
    assert classify_yt_dlp_error(
        "ERROR: subscription required to play this track"
    ) == FailureKind.COOKIES_EXPIRED


# ── Source-agnostic human labels ──────────────────────────────


def test_failure_kind_humans_no_longer_say_youtube():
    """v1.14.1: human labels source-agnostic. Pre-fix every label
    mentioned "YouTube" or "Video" — wrong on SC failures.
    Static guard so a regression doesn't reintroduce YT-specific
    text."""
    for kind in FailureKind:
        if kind == FailureKind.UNKNOWN:
            continue
        h = kind.human.lower()
        assert "youtube" not in h, (
            f"v1.14.1: {kind.value}.human must be source-agnostic; "
            f"got: {kind.human!r}"
        )


def test_cookies_human_label_is_source_agnostic():
    assert "youtube" not in FailureKind.COOKIES_EXPIRED.human.lower()
    assert "cookies" in FailureKind.COOKIES_EXPIRED.human.lower()


def test_network_human_label_is_source_agnostic():
    assert "youtube" not in FailureKind.NETWORK_ERROR.human.lower()


# ── Negative case: random unknown errors still UNKNOWN ────────


def test_unrecognized_error_still_unknown():
    assert classify_yt_dlp_error(
        "ERROR: weird thing happened that's not in any pattern"
    ) == FailureKind.UNKNOWN


def test_empty_error_still_unknown():
    assert classify_yt_dlp_error("") == FailureKind.UNKNOWN
    assert classify_yt_dlp_error(None) == FailureKind.UNKNOWN
