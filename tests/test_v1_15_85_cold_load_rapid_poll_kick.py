"""v1.15.85 — cold-load auto-kick of libraryRapidPoll on in-flight rows.

the user's screenshot of v1.15.83: a library row with an amber
DL pill blinking, topbar showing IDLE. Refreshing the page
resolved the row to a normal U-source row with proper DL/PL
state.

Root cause: there's no continuous background loadLibrary on
the /movies, /tv, /anime pages. The only library refresh
mechanisms are:

1. Initial page load (one fetch).
2. User filter / search / sort / pagination changes.
3. Manual REFRESH FROM PLEX button.
4. libraryRapidPoll — triggered by user actions in *this tab*
   (SET URL submit, UPLOAD MP3, bulk-apply, etc.).

When a download is queued from somewhere ELSE — settings-page
import, manual URL in another tab, an api caller — the queued
job's `job_in_flight='download'` field surfaces on the row at
the next /api/library fetch (load or filter change). But none
of the rapid-poll triggers fire from in-this-tab activity, so
the row's amber pill persists until the user manually refreshes
or otherwise re-triggers loadLibrary.

The topbar /api/progress poll has its own cadence (10s idle →
1s when ops active), so it'll catch the synth op independently.
But the row's amber lags until a manual refresh.

v1.15.85 fix: in loadLibrary, after libraryState.items is set,
check if any rendered item has `job_in_flight`. If so, fire
libraryRapidPoll(). The rapid-poll already exists; this just
gives it a load-time trigger.

The rapid-poll's auto-stop (2 consecutive empty polls) tears
it down once every visible row's job_in_flight clears, so this
adds no permanent cost — it's a transient response to detecting
in-flight work on the page.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
APP_JS = ROOT / "app" / "web" / "static" / "app.js"


def test_loadLibrary_kicks_rapid_poll_when_any_row_has_job_in_flight():
    """The v1.15.85 fix lives in loadLibrary right after
    libraryState.items is set. It calls libraryRapidPoll when
    .some(it => it.job_in_flight) returns true on the
    just-fetched items array."""
    src = APP_JS.read_text()
    # Anchor to the libraryState.items assignment line, then
    # confirm the kick logic follows within the next ~50 lines.
    anchor = src.index("libraryState.items = dedupedItems;")
    # The kick must come BEFORE the empty-state branch (which
    # paints "no items" and clears the hash) — otherwise an
    # empty page wouldn't kick (no items to inspect) AND a
    # populated page would already have a render fall-through
    # that bypasses the kick. We want it on the libraryState
    # assignment path for both branches.
    window = src[anchor:anchor + 3000]
    # Counter-guard: it should not be deep inside a branch
    # that requires non-empty items.
    assert "job_in_flight" in window, (
        "v1.15.85: the cold-load kick block must reference "
        "job_in_flight near libraryState.items = dedupedItems."
    )
    assert "libraryRapidPoll" in window, (
        "v1.15.85: the cold-load kick must invoke libraryRapidPoll."
    )
    # Pin the explicit shape: .some( ... it.job_in_flight ...)
    # so the test fails if a future change converts to .filter
    # or .find and accidentally loses the truthy check.
    assert re.search(
        r"\.some\(.*it\.job_in_flight",
        window,
    ), (
        "v1.15.85: cold-load kick must use .some(it => it.job_in_flight) "
        "form. Other forms (filter, find, indexOf) risk subtle bugs "
        "(e.g. filter().length truthy on empty array)."
    )


def test_kick_is_guarded_by_typeof_function_check():
    """libraryRapidPoll is declared further down the file (~7574).
    Function declarations are hoisted in the IIFE so the call
    works, but the `typeof === 'function'` guard preserves the
    same defensive pattern other call sites use (line 2330 etc.) —
    if a future refactor pulls libraryRapidPoll into a different
    scope or makes it conditional, the guard prevents an undefined
    ReferenceError from killing loadLibrary."""
    src = APP_JS.read_text()
    anchor = src.index("libraryState.items = dedupedItems;")
    window = src[anchor:anchor + 3000]
    assert re.search(
        r"typeof libraryRapidPoll === 'function'",
        window,
    ), (
        "v1.15.85: kick should guard with typeof === 'function' for "
        "parity with other libraryRapidPoll call sites."
    )


def test_existing_action_triggered_kicks_still_in_place():
    """The v1.15.85 cold-load kick AUGMENTS the action-triggered
    pattern (v1.14.86 and after) — it doesn't replace it. Verify
    the action-time kicks at the v1.14.86 and v1.14.80 anchor
    points are still present so the regression-prevention web
    stays intact."""
    src = APP_JS.read_text()
    # At least three action-handler kick sites should remain.
    kicks = re.findall(
        r"typeof libraryRapidPoll === 'function'[\s\S]{0,200}libraryRapidPoll\(\)",
        src,
    )
    assert len(kicks) >= 4, (
        f"v1.15.85: expected >=4 libraryRapidPoll() call sites "
        f"(3 action-handlers from v1.14.86 + 1 cold-load), found "
        f"{len(kicks)}. An earlier call site was removed?"
    )


def test_no_unrelated_optimistic_placeholder_added_on_load():
    """The v1.15.85 fix is targeted at the ROW (libraryRapidPoll
    refreshes the amber→green transition). It deliberately does
    NOT also fire motifOps.setOptimisticPlaceholder, because the
    topbar's own /api/progress poll catches the synth op within
    its cadence (10s idle, accelerating to 1s once any
    pending/running op is observed). Firing the placeholder on
    every load with in-flight rows would create a 5s flicker if
    the actual job had already completed by the time the page
    loaded. The placeholder belongs to user-action call sites
    (where the click→busy gap is the visual liability) — not
    load-time detection."""
    src = APP_JS.read_text()
    anchor = src.index("libraryState.items = dedupedItems;")
    # Window covers the v1.15.85 block + the empty-state branch +
    # the populated-render branch + the count update — anything
    # after that is unrelated. ~3500 chars is generous.
    window = src[anchor:anchor + 3500]
    assert "setOptimisticPlaceholder" not in window, (
        "v1.15.85: the cold-load block should NOT fire "
        "motifOps.setOptimisticPlaceholder. The placeholder is "
        "for the click→busy gap on user-action paths, not for "
        "detecting in-flight work on page load. See the rationale "
        "comment block in the v1.15.85 code."
    )
