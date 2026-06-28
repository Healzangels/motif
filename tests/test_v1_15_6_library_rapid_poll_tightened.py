"""v1.15.6 — libraryRapidPoll cadence tightened + auto-stop debounced.

the user: "After accepting an update I am seeing the PL amber
flashing for a very long time + way after the status bar has
gone back to idle. Can we check this as well as all status DL,
PL to make sure they update right away with proper status
updates and stay in line with the actions on the status bar."

## Two compounding causes

1. **Polling cadence was 5s** — even on the happy path, up to
   a 5s gap between place-job completion and the chip clearing
   on the next loadLibrary render.

2. **Auto-stop fired on first empty poll** — there's a small
   window between download-done and the chained place job being
   enqueued (sub-second, but observable). If the rapid-poll
   landed in that window, it saw NO in-flight jobs and stopped.
   After the place worker finished, no further poll fired until
   the 30s background tick — so the row's PL chip flashed amber
   for ~30s past actual completion.

## Fix

- Cadence: 5000ms → 2000ms. Halves the visible lag on the
  happy path. The loadLibrary call is cheap; tbody full-rewrite
  is smooth at 2s.
- Auto-stop: requires 2 consecutive empty polls before
  clearing the timer. Single empty poll might be the
  between-jobs gap; two in a row means the queue actually
  drained. Adds ~2s of extra polling worst case (one tick
  past actual completion) — well worth avoiding the 30s+
  visual lag on the false-positive stop.

The 60s overall duration cap is unchanged — long enough to
cover any reasonable single-action's full cycle (download +
place), short enough that abandoned polls don't churn forever.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"


def test_rapid_poll_cadence_is_2_seconds():
    """The setInterval cadence must be 2000ms. Pre-fix it was
    5000ms — too slow for the chip to feel responsive after a
    user action.

    v1.18.88: scope the cadence check to the setInterval call
    specifically — the function body now also contains a
    setTimeout(..., 5000) for the v1.18.88 safety-net fetch
    which would false-positive the old `"}, 5000);" not in fn_body`
    guard."""
    src = APP_JS.read_text()
    fn_start = src.index("function libraryRapidPoll(")
    fn_end = src.index("function ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    # The setInterval closes with `}, 2000);` — this is the
    # cadence pin. v1.18.88 added a setTimeout(..., 5000) for
    # the safety-net fetch; that doesn't affect the cadence.
    assert "}, 2000);" in fn_body, (
        "libraryRapidPoll setInterval cadence must be 2000ms"
    )
    # Pre-v1.15.6 5s cadence — guard wording widened in v1.18.88
    # since the function body now legitimately contains a
    # `}, 5000)` literal in the safety-net setTimeout. Check the
    # full `}, 5000);` shape (with semicolon) — both setInterval
    # AND the safety-net setTimeout use it. To distinguish, we
    # count occurrences: pre-v1.15.6 had ONE (the setInterval);
    # v1.18.88 has ONE (the safety-net setTimeout). Either way,
    # at most one `}, 5000);` should exist + the `}, 2000);`
    # must be present.
    n_5s = fn_body.count("}, 5000);")
    assert n_5s <= 1, (
        f"At most one `}}, 5000);` allowed (the v1.18.88 safety-"
        f"net setTimeout); found {n_5s}. Suggests a regression "
        "to the pre-v1.15.6 5s setInterval cadence."
    )


def test_rapid_poll_auto_stop_requires_consecutive_empty_polls():
    """The auto-stop must wait for >= 2 consecutive empty polls
    before clearing the timer. Pre-v1.15.6 a single empty poll
    stopped it — vulnerable to the between-jobs gap race
    the user's repro hit (post-download / pre-place window).

    v1.18.88 widened the threshold from 2 → 5 to absorb a
    larger pipeline-gap race the user reported on user-URL
    flows (download 7s + place 2s with a 1-2s gap that aligned
    with the 4s pre-fix tolerance). The contract pinned here
    is the GENERAL shape (threshold >= 2), not a specific
    value — the central fix can widen further without
    breaking this test."""
    import re
    src = APP_JS.read_text()
    fn_start = src.index("function libraryRapidPoll(")
    fn_end = src.index("function ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    # The consecutive-empty counter must be tracked.
    assert "consecutiveEmpty" in fn_body, (
        "Auto-stop must track consecutiveEmpty count"
    )
    # Extract the actual threshold value — must be >= 2.
    m = re.search(r"consecutiveEmpty\s*>=\s*(\d+)", fn_body)
    assert m, "Auto-stop must compare consecutiveEmpty >= N"
    threshold = int(m.group(1))
    assert threshold >= 2, (
        f"Auto-stop threshold must be >= 2 (got {threshold}); "
        "v1.15.6 contract — single-empty-poll auto-stop hits the "
        "between-jobs gap race"
    )
    # The counter must reset on a busy poll.
    assert "consecutiveEmpty = 0" in fn_body


def test_busy_check_still_uses_job_in_flight():
    """The "still busy?" check must still look at job_in_flight
    on every row — pin the criteria so a refactor doesn't
    accidentally widen / narrow it."""
    src = APP_JS.read_text()
    fn_start = src.index("function libraryRapidPoll(")
    fn_end = src.index("function ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    assert "it.job_in_flight" in fn_body, (
        "Busy check must inspect each row's job_in_flight"
    )


def test_user_interaction_skip_preserved():
    """The pre-existing skip-during-interaction guards must
    survive (typing in search, dialog open, text selection).
    These prevent the tbody rewrite from disrupting active
    user input."""
    src = APP_JS.read_text()
    fn_start = src.index("function libraryRapidPoll(")
    fn_end = src.index("function ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    assert "library-search" in fn_body
    assert "dialog[open]" in fn_body
    assert "hasTextSelection" in fn_body


def test_overall_duration_cap_preserved():
    """An overall duration cap must stay — abandoned polls
    (e.g. user navigates away mid-job) shouldn't poll forever.

    v1.16.7 update: bumped 60_000 → 300_000 ms (1 → 5 min).
    The original 60s was insufficient under background-tab
    throttling + long yt-dlp downloads — the user's repro:
    "still see rows getting stuck amber DL but then refreshing
    it shows properly updated." The cap mechanism stays the
    same; only the value widened."""
    src = APP_JS.read_text()
    fn_start = src.index("function libraryRapidPoll(")
    fn_end = src.index("function ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    import re
    m = re.search(r"durationMs = (\d+)", fn_body)
    assert m, "durationMs default not found"
    duration = int(m.group(1))
    assert duration >= 60000, (
        f"Default duration cap must be at least 60s; got {duration}"
    )
    assert "Date.now() > libraryRapidUntil" in fn_body, (
        "Duration-cap check must remain in the tick loop"
    )


def test_v1_15_6_marker_explains_the_fix():
    """v1.15.6 markers reference the user's repro phrase + the
    between-jobs gap rationale so a future cadence/stop
    refactor sees the trade-offs."""
    src = APP_JS.read_text()
    fn_start = src.index("function libraryRapidPoll(")
    fn_end = src.index("function ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    assert "v1.15.6" in fn_body
    # Reference to the between-jobs gap rationale.
    assert "between-jobs" in fn_body or "between jobs" in fn_body
    # the user's repro framing as evidence of intent.
    assert "PL amber flashing" in fn_body or \
        "way after" in fn_body
