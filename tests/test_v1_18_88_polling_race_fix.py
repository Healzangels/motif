"""v1.18.88 — fix libraryRapidPoll auto-stop race + audit mutation handlers.

the user's repro: after a user-URL flow (SET URL → download → upload
→ place → Plex refresh, ~9s end-to-end), the row showed amber DL
pulse + empty other axes. Manual page refresh fixed it. Backend
state was fully resolved (DB query confirmed: no jobs, local_files
present, placements written, user_overrides intent='replace').

## Root cause

libraryRapidPoll auto-stopped MID-pipeline. v1.15.6 set the
consecutive-empty debounce at 2 polls (4s tolerance). the user's
chain had a 1-2s gap between download-done and place-enqueued
where NO jobs row matched `status IN ('pending','running')`. If
that gap aligned with 2 consecutive poll boundaries (4s window),
the auto-stop fired before the place job started. Place then
ran with no further polls until the 30s background tick — the
row's amber pulse persisted in the UI for ~25s after the
backend completed.

## Fix

Two-part defense in depth:

1. **Widen debounce 2 → 5** (4s → 10s tolerance). Catches most
   gap windows in the per-row job pipeline.
2. **Safety-net loadLibrary at +5s post-auto-stop**. Final
   one-shot fetch catches state that landed in the gap between
   the last poll and the auto-stop. Idempotent — if nothing
   changed, the hash-skip in loadLibrary suppresses the
   repaint. Cost: one extra fetch per rapid-poll cycle.

## Mutation-handler audit

The libraryRapidPoll central fix benefits every flow that uses
it. Audited every state-mutating handler in app.js; found three
handlers that called loadLibrary but NOT libraryRapidPoll:

- **`/intent` PROMOTE TO ACTIVE / MARK AS BACKUP** (v1.18.77):
  backup→replace transition triggers a force-place job; the PL
  transition needs polling to surface without a refresh.
- **`/adopt-from-plex`** (v1.11.99 mismatch flow): the LINK
  transition (M=mismatch → A=adopted) needs polling.
- **`/keep-mismatch`** (v1.11.99): mismatch ack cleanup affects
  DL chip + LINK chip; needs polling.

Each handler now calls libraryRapidPoll() after the loadLibrary()
to keep watching for any chained transitions.

## What's NOT changed (intentionally)

- `/ack-drop`, `/clear-failure`, `/mark-alive`, `/restore-canonical`
  — these flip a flag or ack a state without enqueuing chained
  jobs. The post-handler loadLibrary fetches the new state in
  one shot; no chained transitions to poll for.
- Bulk handlers (`/library/download-batch`, `/admin/bulk-*`) —
  these already kick libraryRapidPoll where needed.
"""
from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"


def _rapid_poll_body() -> str:
    """Extract the libraryRapidPoll function body."""
    src = APP_JS.read_text()
    start = src.index("function libraryRapidPoll(")
    # Body ends at the next top-level `function` declaration.
    end = src.find("\n  function ", start + 1)
    return src[start:end if end != -1 else len(src)]


# ── Central fix: debounce widened + safety net ───────────────


def test_consecutive_empty_threshold_widened_to_5():
    """v1.18.88: the auto-stop now requires 5 consecutive empty
    polls (10s tolerance) instead of 2 (4s pre-fix). The 4s
    window aligned with the 1-2s gap between download-done and
    place-enqueued; 10s comfortably exceeds the worst-case
    pipeline gap."""
    body = _rapid_poll_body()
    assert "consecutiveEmpty >= 5" in body, (
        "v1.18.88: debounce threshold must be 5 (was 2 pre-fix)"
    )
    assert "consecutiveEmpty >= 2" not in body, (
        "v1.18.88: pre-fix debounce of 2 must be gone — the "
        "regression-risk is shipping a debounce that's still too "
        "tight"
    )


def test_safety_net_load_library_at_5s_post_autostop():
    """v1.18.88: after the auto-stop clears the interval, schedule
    one final loadLibrary at +5s as a safety net. Catches state
    that landed in the gap between the last poll and the
    auto-stop. Idempotent — hash-skip suppresses no-op repaints."""
    body = _rapid_poll_body()
    # The setTimeout for the safety net.
    assert "setTimeout(" in body and "5000" in body, (
        "v1.18.88: must schedule a one-shot loadLibrary at +5s "
        "after auto-stop fires"
    )
    # The setTimeout's body must check visibility (don't fire on
    # hidden tabs — same gate the polling itself uses) and call
    # loadLibrary.
    assert "visibilityState" in body
    assert "loadLibrary()" in body


def test_v1_18_88_marker_explains_race():
    """The fix must carry a v1.18.88 marker explaining the race
    so future archaeology can re-derive the symptom + diagnosis."""
    body = _rapid_poll_body()
    assert "v1.18.88" in body
    # Marker must mention the race shape — the user's "stuck amber"
    # diagnosis + the pipeline-gap mechanism.
    flat = " ".join(body.split())
    assert (
        "race" in flat.lower()
        or "gap" in flat.lower()
        or "between the last poll" in flat.lower()
    )


# ── Handler audit: 3 gaps closed ────────────────────────────


def test_intent_flip_handler_kicks_libraryRapidPoll():
    """v1.18.77's PROMOTE TO ACTIVE / MARK AS BACKUP handler
    POSTs `/api/items/{mt}/{id}/intent` which can trigger a
    force-place job in the worker (on backup→replace). Pre-
    v1.18.88 only loadLibrary fired — no rapid-poll to surface
    the place transition. v1.18.88 closes the gap."""
    src = APP_JS.read_text()
    # Find the intent POST. The `/intent${qs}` literal appears
    # twice now (v1.19.54 added a confirm-and-retry path), so
    # anchor on the success-side block by widening the window
    # to cover both POSTs + their success/failure branches.
    idx = src.index("/intent${qs}")
    # v1.19.54: window widened 1200 → 5000. The SHA-drift
    # catch/retry block + dialog wording added ~3000 chars
    # between the first POST and the success path that fires
    # libraryRapidPoll.
    block = src[idx:idx + 5000]
    # libraryRapidPoll must be called after loadLibrary.
    assert "libraryRapidPoll" in block, (
        "v1.18.88: intent flip handler must kick libraryRapidPoll "
        "to watch the force-place job's PL transition"
    )


def test_adopt_from_plex_handler_kicks_libraryRapidPoll():
    """v1.11.99 `/adopt-from-plex` handler flips a mismatch row's
    LINK from M (mismatch) to A (adopted). The transition needs
    polling to surface without a manual refresh."""
    src = APP_JS.read_text()
    idx = src.index("/adopt-from-plex`")
    block = src[idx:idx + 600]
    assert "libraryRapidPoll" in block, (
        "v1.18.88: adopt-from-plex must kick libraryRapidPoll"
    )


def test_keep_mismatch_handler_kicks_libraryRapidPoll():
    """v1.11.99 `/keep-mismatch` handler acks a mismatch and
    cleans up DL chip + LINK chip. Needs polling."""
    src = APP_JS.read_text()
    # v1.21.80: anchor on the URL path (not the trailing backtick) — the
    # v1.21.79 handler appends a ${_kmRk} rating_key query before the
    # backtick, so "/keep-mismatch`" no longer appears literally.
    idx = src.index("/keep-mismatch")
    block = src[idx:idx + 600]
    assert "libraryRapidPoll" in block, (
        "v1.18.88: keep-mismatch must kick libraryRapidPoll"
    )


# ── Regression guards for existing call sites ────────────────


def test_existing_libraryRapidPoll_call_sites_preserved():
    """v1.18.88 must NOT remove any existing libraryRapidPoll
    calls. Pre-fix audit counted 26+ call sites. Post-fix must
    be ≥ pre-fix + 3 new ones (intent + adopt + keep-mismatch
    each get one)."""
    src = APP_JS.read_text()
    # Count actual function calls (not just identifier refs).
    # `libraryRapidPoll()` — open paren.
    n = len(re.findall(r"\blibraryRapidPoll\s*\(", src))
    assert n >= 29, (
        f"v1.18.88: expected >=29 libraryRapidPoll() call sites "
        f"(26+ pre-fix + 3 new); found {n}"
    )
