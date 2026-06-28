"""v1.14.62 — dash REFRESH PLEX button no longer falsely shows
"// REFRESHING PLEX…" during a TDB sync.

the user repro on v1.14.59 dashboard: clicked SYNC THEMERRDB; the
sibling REFRESH PLEX button flipped to "// REFRESHING PLEX…" and
got disabled, making it look like Plex was being refreshed even
though only the TDB sync was running.

## Root cause

refreshTopbarStatus had:

    const dashPlexBusy = themerrdbBusy || plexEnumBusy;
    if (dashPlexBusy) {
      dashSyncPlexBtn.disabled = true;
      dashSyncPlexBtn.textContent = '// REFRESHING PLEX…';
    } else { ... }

The OR predicate controlled BOTH the disabled flag AND the label.
The disable side is the original v1.13.63 intent (writer-lock
contention — clicking REFRESH PLEX during an active TDB sync
would compete for BEGIN IMMEDIATE). The label flip was incidental
collateral.

## Fix

Split the two:

  - disabled = themerrdbBusy || plexEnumBusy   (unchanged intent)
  - label = plexEnumBusy ? '// REFRESHING PLEX…' : '// REFRESH PLEX'

During a TDB sync the button stays grayed-out (can't click) but
the label honestly says "// REFRESH PLEX" — the user sees the
button is locked without being misled into thinking Plex is
actively refreshing.

Mirror of v1.13.77 which already narrowed the OPPOSITE direction
(SYNC THEMERRDB no longer flips to "// SYNCING…" when REFRESH
PLEX clicks via `cascadeInFlight` narrowing).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
JS = REPO / "app" / "web" / "static" / "app.js"


def test_dash_refresh_plex_label_only_flips_when_plex_enum_busy():
    """The poll-driven refresh path must use plexEnumBusy alone
    for the label decision. themerrdbBusy is irrelevant to
    whether Plex is actually being refreshed."""
    js = JS.read_text()
    # The new shape: ternary on plexEnumBusy alone.
    anchor = js.index("const dashSyncPlexBtn = document.getElementById('sync-plex-btn');")
    block = js[anchor:anchor + 1500]
    # Label assignment uses plexEnumBusy only.
    assert "plexEnumBusy\n          ? '// REFRESHING PLEX…'\n          : '// REFRESH PLEX'" in block
    # v1.14.62 marker.
    block_with_marker = js[max(0, anchor - 1500):anchor + 1500]
    assert "v1.14.62:" in block_with_marker


def test_dash_refresh_plex_disable_predicate_is_plex_enum_only():
    """v1.14.66 supersedes the v1.13.63 "writer-lock prevention"
    rationale. The long worker is single-threaded
    (worker.py:_LONG_JOB_TYPES), so a plex_enum queued during a
    TDB sync just waits in the queue — no concurrent SQLite
    writer contention is even possible. Clicking REFRESH PLEX
    during a TDB sync is now allowed; the button stays clickable
    and the queued plex_enum runs after the sync finishes.

    The disable predicate is `.disabled = plexEnumBusy;` only.
    See test_v1_14_66 for the supersede rationale + the new
    behavior assertions."""
    js = JS.read_text()
    anchor = js.index("const dashSyncPlexBtn = document.getElementById('sync-plex-btn');")
    block = js[anchor:anchor + 1500]
    # The narrowed predicate.
    assert "dashSyncPlexBtn.disabled = plexEnumBusy;" in block
    # The pre-v1.14.66 form must NOT survive.
    assert "dashSyncPlexBtn.disabled = themerrdbBusy || plexEnumBusy" not in block


def test_dash_refresh_plex_no_longer_uses_old_dashPlexBusy_predicate():
    """Regression guard: the pre-fix `dashPlexBusy` local that
    drove BOTH disabled + label must not survive. The two
    decisions are now independent."""
    js = JS.read_text()
    anchor = js.index("const dashSyncPlexBtn = document.getElementById('sync-plex-btn');")
    block = js[anchor:anchor + 1500]
    # Strip JS line comments so the v1.14.62 marker's mention of
    # the old variable name doesn't trip the guard.
    block_no_comments = "\n".join(
        line for line in block.splitlines()
        if not line.lstrip().startswith("//")
    )
    assert "const dashPlexBusy" not in block_no_comments


def test_dash_sync_themerrdb_button_unaffected():
    """Sanity: the v1.13.77 cascadeInFlight narrowing on the
    SYNC THEMERRDB button stays — the OPPOSITE-direction bug
    (REFRESH PLEX click flipping SYNC THEMERRDB to // SYNCING…)
    was already fixed there; v1.14.62 must not regress it."""
    js = JS.read_text()
    assert "const dashSyncBtnBusy = themerrdbBusy || cascadeInFlight;" in js
    # The v1.13.77 marker is intact.
    assert "v1.13.77:" in js


def test_click_handler_still_sets_optimistic_label():
    """Sanity: clicking REFRESH PLEX directly still optimistically
    flips the label to // REFRESHING PLEX… (the click-handler
    path at app.js:1907). That path is correct because the user
    actually clicked the refresh; only the poll-driven path
    (refreshTopbarStatus) was wrongly mirroring TDB busy state."""
    js = JS.read_text()
    handler_anchor = js.index("const syncPlexBtn = $('#sync-plex-btn');")
    block = js[handler_anchor:handler_anchor + 1500]
    assert "syncPlexBtn.textContent = '// REFRESHING PLEX…';" in block
