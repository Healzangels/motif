"""v1.15.12 — rate-limit misclassification + pool size reduction.

## Pre-fix

the user's v1.15.11 REPROBE FAILURES run on the deployed image
returned this distribution at completion:

    BULK PROBE TDB done: 2507 probed, alive=378 (cleared=378),
    dead=2005, indeterminate=115, other=9, errors=0

Most of those 2005 "dead" results were yt-dlp errors of the form:

    ERROR: [youtube] <id>: Video unavailable. This content isn't
    available, try again later. The current session has been
    rate-limited by YouTube for up to an hour. ...

That message starts with "Video unavailable", which means
`classify_yt_dlp_error` matched the existing branch:

    if "video unavailable" in m and "private" not in m:
        return FailureKind.VIDEO_REMOVED

VIDEO_REMOVED has `needs_manual_override == True`, so the bulk
probe writes failure_kind to the row. Net effect: alive videos
that just happened to be hit during a YouTube IP throttle window
got flagged TDB ✗ in the DB. On the user's repro, ~80% of the run's
"dead" verdicts were actually rate-limit false-positives.

## Fix (two parts)

1. **Classifier**: detect "rate-limited" / "rate limit" BEFORE
   the generic "video unavailable" branch and return NETWORK_ERROR.
   Bulk-probe handler treats NETWORK_ERROR as transient (puts it
   in n_other, no failure_kind write), so rate-limited probes no
   longer corrupt the DB.

2. **Pool size**: BULK_PROBE_MAX_WORKERS 3 → 2. The v1.15.11 run
   averaged 2.9 probes/s with 3 workers — over YouTube's unauth
   throughput threshold (the threshold the IP actually hit). 2
   workers averages ~2/s which should sit under the limit even
   for cookied requests. Run takes ~50% longer in exchange for
   not retriggering the throttle on every recovery cycle.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
DOWNLOADER_PY = REPO / "app" / "core" / "downloader.py"
API_PY = REPO / "app" / "web" / "api.py"


def test_rate_limited_classifies_as_network_error():
    """Pin the classification of yt-dlp's rate-limit message.

    v0.51.269 took the option this docstring anticipated: rate-limiting moved to
    its OWN kind (RATE_LIMITED) so an adaptive controller can distinguish a
    throttle from a network fault. The condition it set — "that kind MUST also
    be treated as transient by the bulk probe handler" — is what this test now
    asserts, which is stronger than pinning the member: the bulk probe routes
    dead-vs-other on `needs_manual_override`, so a False there IS the
    no-failure_kind-write guarantee that keeps a throttled probe from corrupting
    row state (the v1.15.11 repro: 2005 "dead" of 2507)."""
    import sys
    sys.path.insert(0, str(REPO))
    from app.core.downloader import classify_yt_dlp_error, FailureKind
    msg = (
        "[youtube] abc123: Video unavailable. This content isn't "
        "available, try again later. The current session has been "
        "rate-limited by YouTube for up to an hour. It is "
        "recommended to use `-t sleep` to add a delay between "
        "video requests to avoid exceeding the rate limit."
    )
    kind = classify_yt_dlp_error(msg)
    assert kind == FailureKind.RATE_LIMITED
    assert kind.needs_manual_override is False, (
        "the load-bearing property: bulk probe writes failure_kind ONLY for "
        "needs_manual_override kinds, so a throttle must answer False here")
    assert kind.is_indeterminate is True, (
        "v1.15.12's invariant, unchanged: a rate-limit response is transient — "
        "it must never render as VIDEO_REMOVED (dead)"
    )


def test_rate_limit_branch_precedes_video_unavailable_branch():
    """Source-level guard: the rate-limit check must appear BEFORE
    the generic `if "video unavailable" in m` branch. The generic
    branch matches the rate-limit message (it starts with "Video
    unavailable") so any reordering reintroduces the v1.15.12 bug."""
    src = DOWNLOADER_PY.read_text()
    fn_start = src.index("def classify_yt_dlp_error(")
    fn_end = src.index("\n\n", fn_start)
    fn_body = src[fn_start:fn_end]
    rate_idx = fn_body.index('"rate-limited" in m')
    unavail_idx = fn_body.index('"video unavailable" in m')
    assert rate_idx < unavail_idx, (
        "v1.15.12 ordering invariant: rate-limit check must come "
        "BEFORE the generic 'video unavailable' branch (which the "
        "rate-limit message would otherwise match first)"
    )


def test_actually_dead_video_unavailable_still_classifies_as_removed():
    """Regression guard for the non-rate-limit path. yt-dlp's
    canonical "Video unavailable" message for an actually-removed
    video must still classify as VIDEO_REMOVED — the rate-limit
    branch is a narrow precedent, not a blanket override."""
    import sys
    sys.path.insert(0, str(REPO))
    from app.core.downloader import classify_yt_dlp_error, FailureKind
    msg = (
        "[youtube] xyz789: Video unavailable. The uploader has not "
        "made this video available in your country."
    )
    # That specific phrasing falls into GEO_BLOCKED via the
    # "not available in your country" pattern further down. Use
    # the bare "Video unavailable" + "no longer available" combo
    # for the VIDEO_REMOVED case.
    msg2 = (
        "[youtube] zzz: Video unavailable. This video is no longer "
        "available because the YouTube account associated with "
        "this video has been terminated."
    )
    assert classify_yt_dlp_error(msg2) == FailureKind.VIDEO_REMOVED


def test_account_terminated_still_classifies_as_removed():
    """Most common dead-video phrasing in the user's logs — the
    bulk probe must still flag these as dead so // LET PLEX SERVE
    knows the recovery URL is gone."""
    import sys
    sys.path.insert(0, str(REPO))
    from app.core.downloader import classify_yt_dlp_error, FailureKind
    msg = (
        "[youtube] abc: Video unavailable. This video is no longer "
        "available because the YouTube account associated with "
        "this video has been terminated."
    )
    assert classify_yt_dlp_error(msg) == FailureKind.VIDEO_REMOVED


def test_bulk_probe_max_workers_reduced_to_one():
    """Pool size reduction is the second half of the v1.15.12 fix.
    v1.15.12 dropped 3 → 2; v1.15.14 went further to 1 because 2
    workers STILL triggered YouTube's throttle on the user's
    deployment (1534 of 2127 probes returned transient in the
    v1.15.12 verify run). 1 worker averages ~1 probe/s which
    should sit under the threshold even on cookied requests.
    Definitive current-value pin lives in the v1.15.14 test
    (test_bulk_probe_max_workers_dropped_to_one); this test
    keeps the broader "must be a low single-digit cap" invariant."""
    src = API_PY.read_text()
    assert "BULK_PROBE_MAX_WORKERS = 1" in src, (
        "v1.15.14: pool size dropped 2 → 1 to keep effective probe "
        "rate under YouTube's throttle threshold"
    )
    # Guard against larger values sneaking back in.
    for n in (2, 3, 4, 5, 6):
        assert f"BULK_PROBE_MAX_WORKERS = {n}" not in src


def test_classifier_handles_rate_limit_substring_variant():
    """Cover the alternate substring path. yt-dlp may use either
    "rate-limited" (hyphenated, current) or "rate limit" (spaced,
    in some error variants). Both are throttles (v0.51.269: RATE_LIMITED)."""
    import sys
    sys.path.insert(0, str(REPO))
    from app.core.downloader import classify_yt_dlp_error, FailureKind
    # Hyphenated form (the dominant message in the user's repro).
    assert classify_yt_dlp_error("session has been rate-limited") == FailureKind.RATE_LIMITED
    # Spaced form (yt-dlp variant phrasings).
    assert classify_yt_dlp_error("exceeding the rate limit") == FailureKind.RATE_LIMITED
