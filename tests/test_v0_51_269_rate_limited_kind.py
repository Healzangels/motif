"""v0.51.269 — throttling becomes its own FailureKind, without becoming fatal.

Feature D of the implementation brief, first step only. The brief asks for an
adaptive rate controller; the prerequisite it does not mention is that motif
currently CANNOT SEE throttling as distinct from any other network fault. Both
YouTube's rate-limit prose and a literal HTTP 429 landed in NETWORK_ERROR — safe
(transient) but indistinguishable, so no controller could tell "the provider is
throttling me" from "the network hiccuped", and neither could a human reading
the events log. Splitting the kind is worth shipping on its own; an adaptive
controller built without this signal would be guessing.

The hazard this tag has to avoid is specific and historical. v1.15.12: a
throttle classified into a DEAD kind red-pilled 2005 of 2507 probed rows — the
worst regression in this area. So RATE_LIMITED must behave EXACTLY like
NETWORK_ERROR everywhere that matters: transient, retried, no failure_kind
write, amber `?` not red ✗.

"Transient" was expressed three ways: `needs_manual_override` (worker retry vs
permanent), a literal `indeterminate_set` in api_probe, and the bulk-probe's
`err is not None`. The middle one was the trap — a literal set a new kind could
silently miss — so it moved onto the enum as `is_indeterminate`.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from app.core.downloader import FailureKind, classify_yt_dlp_error

REPO = Path(__file__).resolve().parent.parent


# ── the new signal ───────────────────────────────────────────


@pytest.mark.parametrize("msg", [
    "ERROR: unable to download video data: HTTP Error 429: Too Many Requests",
    "HTTP Error 429",
    "ERROR: Unable to download webpage: HTTP Error 429: too many requests",
    "The current session has been rate-limited by YouTube for up to an hour.",
    "Video unavailable. This content isn't available, try again later. "
    "The current session has been rate-limited by YouTube for up to an hour.",
])
def test_throttling_classifies_as_rate_limited(msg):
    assert classify_yt_dlp_error(msg) is FailureKind.RATE_LIMITED


def test_the_youtube_rate_limit_prose_still_beats_the_dead_patterns():
    """v1.15.12's whole point: YouTube's throttle response OPENS with 'Video
    unavailable', so the throttle branch must be evaluated first. If this ever
    returns VIDEO_REMOVED, thousands of live rows get red-pilled again."""
    msg = ("Video unavailable. This content isn't available, try again later. "
           "The current session has been rate-limited by YouTube.")
    assert classify_yt_dlp_error(msg) is FailureKind.RATE_LIMITED


# ── and does NOT become fatal ─────────────────────────────────


def test_rate_limited_is_transient_exactly_like_network_error():
    rl, ne = FailureKind.RATE_LIMITED, FailureKind.NETWORK_ERROR
    assert rl.needs_manual_override is ne.needs_manual_override is False, (
        "a throttle must NOT be a permanent, user-must-fix failure — the worker "
        "raises _JobPermanentFailure on needs_manual_override")
    assert rl.is_indeterminate is ne.is_indeterminate is True, (
        "a throttled probe must paint amber `?`, never a red ✗ (v1.15.12)")


def test_rate_limited_has_a_human_label():
    assert FailureKind.RATE_LIMITED.human
    assert "rate-limit" in FailureKind.RATE_LIMITED.human.lower()


# ── the substring trap ───────────────────────────────────────


def test_a_video_id_containing_429_is_not_a_throttle():
    """The classified message includes the URL/ID, so a bare "429" token would
    false-match. Tokens are specific for this reason."""
    assert classify_yt_dlp_error(
        "ERROR: [youtube] abc429xyz: Video unavailable") is FailureKind.VIDEO_REMOVED
    assert classify_yt_dlp_error(
        "ERROR: [youtube] a429b: This video has been removed by the user"
    ) is FailureKind.VIDEO_REMOVED


# ── nothing else moved ───────────────────────────────────────


@pytest.mark.parametrize("msg,expect", [
    ("ERROR: read timed out", FailureKind.NETWORK_ERROR),
    ("ERROR: unable to download: connection reset", FailureKind.NETWORK_ERROR),
    ("HTTP Error 503", FailureKind.NETWORK_ERROR),
    ("ERROR: Private video. Sign in if you've been granted access",
     FailureKind.VIDEO_PRIVATE),
    ("ERROR: Video unavailable. The uploader has not made this video available",
     FailureKind.VIDEO_REMOVED),
    ("ERROR: Sign in to confirm you're not a bot", FailureKind.COOKIES_EXPIRED),
    ("ERROR: This video is not available in your country",
     FailureKind.GEO_BLOCKED),
])
def test_every_other_classification_is_unchanged(msg, expect):
    assert classify_yt_dlp_error(msg) is expect


def test_generic_http_error_still_falls_to_network_error():
    """Only the throttle tokens were carved out of the generic tail."""
    assert classify_yt_dlp_error("HTTP Error 500") is FailureKind.NETWORK_ERROR


# ── the rule is implemented once ─────────────────────────────


def test_the_probe_uses_the_enum_not_a_literal_set():
    api_py = (REPO / "app" / "web" / "api.py").read_text()
    assert "result.is_indeterminate" in api_py
    assert "indeterminate_set = {" not in api_py, (
        "v0.51.269: the literal set was the one place a new kind had to be "
        "remembered — that is why it moved onto FailureKind")


def test_every_kind_answers_both_predicates():
    """A future kind inherits an answer rather than being silently absent."""
    for k in FailureKind:
        assert isinstance(k.needs_manual_override, bool)
        assert isinstance(k.is_indeterminate, bool)
        assert not (k.needs_manual_override and k.is_indeterminate), (
            f"{k.value}: definitively-dead and indeterminate are exclusive")


def test_no_schema_migration_was_needed():
    """failure_kind carries no CHECK constraint, so a new value stores as-is."""
    db_py = (REPO / "app" / "core" / "db.py").read_text()
    assert "failure_kind TEXT CHECK" not in db_py
    assert "CHECK (failure_kind" not in db_py


def test_v0_51_269_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
