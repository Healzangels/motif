"""v1.18.84 — library filter state moves localStorage → sessionStorage.

the user on /collections post-v1.18.83: "continue after refresh to
find myself back at this filter, or opening a new tab it's back
at this filter."

## The bug

`_LIB_STATE_KEY` ('motif:library_filter_state') was stored in
localStorage. localStorage is shared across all tabs of the same
origin. the user's pattern:

  1. Set STATUS=! on /collections in Tab A
  2. Open new tab → /collections
  3. New tab's bindLibrary runs → _hydrateLibraryFromStorage reads
     localStorage → finds Tab A's filter → restores STATUS=! → 0
     matches on /collections (since no collection rows have
     failure_kind set, the filter renders an empty table)

v1.18.50's reload-clear targeted refresh specifically, but
left cross-tab leakage intact. The cross-tab leak was a latent
bug since v1.13.13 (when the filter snapshot was first added);
it surfaced now because STATUS=! is one of the few states that
returns 0 matches on /collections, making the empty result
unambiguously a filter problem rather than a data problem.

## The fix

Storage backend switched to sessionStorage — same key, same
payload shape, same call sites. sessionStorage is per-tab:

  * new tab → empty, fresh view (the user's expectation) ✓
  * within-tab nav (MOVIES → TV → COLLECTIONS) → preserved
    (v1.13.13 intent) ✓
  * refresh → preserved by sessionStorage BUT v1.18.50's
    _maybeClearStorageOnReload still clears (now via
    sessionStorage.removeItem) ✓

Same scope discipline v1.17.15 applied to the search query `q`
— the v1.15.52 "cross-session resurfacing surprised me"
complaint applied equally to the filter snapshot, same fix.

One-shot eviction: _hydrateLibraryFromStorage calls
localStorage.removeItem(_LIB_STATE_KEY) so any stale entry from
a pre-v1.18.84 session gets cleaned up on the next page load.

## What's pinned

- _saveLibraryFilterState writes to sessionStorage (not
  localStorage)
- _hydrateLibraryFromStorage reads from sessionStorage AND
  opportunistically evicts the legacy localStorage entry
- _clearLibraryFilterStorage wipes both surfaces (sessionStorage
  is canonical; localStorage cleanup is paranoid)
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"


def _filter_storage_block() -> str:
    """The three functions that touch _LIB_STATE_KEY live together;
    extract from _saveLibraryFilterState to _clearLibraryFilterStorage
    inclusive."""
    src = APP_JS.read_text()
    start = src.index("function _saveLibraryFilterState()")
    end_marker = "function _clearLibraryFilterStorage()"
    end = src.index(end_marker, start)
    # Walk past the function body of _clearLibraryFilterStorage too.
    body_end = src.index("\n  }\n", end)
    return src[start:body_end + 5]


# ── Storage backend switched to sessionStorage ───────────────


def test_save_writes_sessionstorage_not_localstorage():
    """_saveLibraryFilterState must call sessionStorage.setItem
    on _LIB_STATE_KEY — NOT localStorage.setItem. The
    cross-tab leak that v1.18.84 closes is exactly the
    localStorage.setItem write on this key."""
    src = APP_JS.read_text()
    # Find the function body.
    start = src.index("function _saveLibraryFilterState()")
    end = src.index("\n  }\n", start)
    body = src[start:end]
    assert "sessionStorage.setItem(_LIB_STATE_KEY" in body, (
        "v1.18.84: _saveLibraryFilterState must write to "
        "sessionStorage so the snapshot stays per-tab"
    )
    # The pre-v1.18.84 localStorage.setItem on this key must be gone.
    assert "localStorage.setItem(_LIB_STATE_KEY" not in body, (
        "v1.18.84: pre-fix localStorage.setItem must be removed — "
        "leaving it in re-introduces the cross-tab leak"
    )


def test_hydrate_reads_sessionstorage():
    """_hydrateLibraryFromStorage must read from sessionStorage.
    Reading localStorage would re-introduce the cross-tab leak
    even if writes moved to sessionStorage."""
    src = APP_JS.read_text()
    start = src.index("function _hydrateLibraryFromStorage()")
    end = src.index("\n  }\n", start)
    body = src[start:end]
    assert "sessionStorage.getItem(_LIB_STATE_KEY" in body, (
        "v1.18.84: hydrate must read from sessionStorage"
    )
    # No reads from localStorage on the canonical key — only the
    # one-shot eviction removeItem is allowed (separate assertion).
    assert "localStorage.getItem(_LIB_STATE_KEY" not in body, (
        "v1.18.84: hydrate must NOT read localStorage — that's "
        "the cross-tab leak path"
    )


def test_hydrate_evicts_legacy_localstorage_entry():
    """Pre-v1.18.84 sessions left a localStorage entry under
    _LIB_STATE_KEY. v1.18.84's hydrate must opportunistically
    evict that entry so the migration is one-shot. Idempotent —
    once evicted, future hydrations see nothing to remove."""
    src = APP_JS.read_text()
    start = src.index("function _hydrateLibraryFromStorage()")
    end = src.index("\n  }\n", start)
    body = src[start:end]
    assert "localStorage.removeItem(_LIB_STATE_KEY" in body, (
        "v1.18.84: hydrate must evict the legacy localStorage "
        "entry so users upgrading from pre-v1.18.84 don't carry "
        "the old store around forever"
    )


def test_clear_wipes_both_surfaces():
    """_clearLibraryFilterStorage must wipe BOTH sessionStorage
    (the canonical store) AND localStorage (paranoid cleanup of
    any leftover legacy entry). CLEAR ALL semantics demand a
    clean slate on either store, even though sessionStorage is
    the only one we write to going forward."""
    src = APP_JS.read_text()
    start = src.index("function _clearLibraryFilterStorage()")
    end = src.index("\n  }\n", start)
    body = src[start:end]
    assert "sessionStorage.removeItem(_LIB_STATE_KEY" in body, (
        "v1.18.84: clear must wipe the canonical sessionStorage entry"
    )
    assert "localStorage.removeItem(_LIB_STATE_KEY" in body, (
        "v1.18.84: clear must also wipe any legacy localStorage "
        "entry — leftover from pre-v1.18.84 sessions"
    )


def test_v1_18_50_reload_clear_still_works():
    """v1.18.50's _maybeClearStorageOnReload calls
    _clearLibraryFilterStorage on F5/Cmd+R. v1.18.84's switch to
    sessionStorage must not break that — sessionStorage survives
    reload by default, so the explicit clear is still needed."""
    src = APP_JS.read_text()
    # _maybeClearStorageOnReload must still exist + call the
    # _clearLibraryFilterStorage helper (which v1.18.84 redirected
    # to sessionStorage).
    assert "function _maybeClearStorageOnReload()" in src
    fn_start = src.index("function _maybeClearStorageOnReload()")
    fn_end = src.index("\n  }\n", fn_start)
    body = src[fn_start:fn_end]
    assert "_clearLibraryFilterStorage" in body, (
        "v1.18.84: reload-clear must still invoke the (now-"
        "sessionStorage-backed) clear helper. Without it, "
        "F5/Cmd+R would leave a stale sessionStorage entry."
    )


# ── Sanity: v1.17.15 sessionStorage pattern preserved ────────


def test_session_q_storage_pattern_unchanged():
    """v1.17.15's per-tab search query stored in sessionStorage
    is the model v1.18.84 mirrors. Verify the pattern still
    exists — if v1.17.15's `q` migrates BACK to localStorage at
    any point, v1.18.84's rationale ('same as q') breaks."""
    src = APP_JS.read_text()
    # The _LIB_SESSION_Q_KEY constant + sessionStorage reads/writes
    # under that key.
    assert "_LIB_SESSION_Q_KEY = 'motif:library_q'" in src
    assert "sessionStorage.setItem(_LIB_SESSION_Q_KEY" in src
    assert "sessionStorage.getItem(_LIB_SESSION_Q_KEY" in src


# ── Sanity: no other callsite still writes the legacy key ────


def test_no_localstorage_writes_to_lib_state_key():
    """v1.18.84 must remove ALL localStorage.setItem calls on
    _LIB_STATE_KEY. A stray write elsewhere would re-leak."""
    src = APP_JS.read_text()
    # Either spelling (with or without the helper var).
    bad_patterns = [
        "localStorage.setItem(_LIB_STATE_KEY",
        "localStorage.setItem('motif:library_filter_state'",
        'localStorage.setItem("motif:library_filter_state"',
    ]
    for pat in bad_patterns:
        assert pat not in src, (
            f"v1.18.84: stray localStorage write on _LIB_STATE_KEY "
            f"({pat!r}) re-introduces the cross-tab leak"
        )
