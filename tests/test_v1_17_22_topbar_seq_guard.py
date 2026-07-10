"""v1.17.22 — refreshTopbarStatus seq guard (class-6 mirror).

CLAUDE.md bug class 6 documents the "syncWatcher vs
refreshTopbarStatus race" — but the v1.17.12 audit caught that
`refreshTopbarStatus` itself was never guarded. ~20 caller
sites overlap routinely:

* 10s `setInterval(refreshTopbarStatus, 10000)` at ~line 13542
* `visibilitychange` kick at ~line 135
* `loadQueue` piggyback at ~line 3416
* `syncWatcher` poll path
* 15+ post-action `setTimeout(refreshTopbarStatus, 1100)` sites
  (after every state-changing button click)

A slow A finishing AFTER fast B would otherwise:
* clobber the v1.14.37 `_lastHash` with stale payload — next
  tick gets hash-skipped against the stale hash, freezing the
  topbar until the hash naturally changes;
* re-enable a button B's response wanted disabled;
* paint OFFLINE on top of B's fresh success when A's await
  throws after B succeeded.

## Fix

Same `_seq` token pattern as `loadLibrary._seq` (line 6348)
and `loadQueue._seq` (line 3407): capture a monotonic token
at function entry, check after the await on BOTH the success
and error paths, bail if a newer call has superseded.

## Tests

Source-grep pins (no JS runtime in CI). Pin:
* Seq token declared at function entry.
* Success path bails on stale token AFTER api() resume but
  BEFORE the DOM-write block.
* Catch path bails on stale token BEFORE the OFFLINE pill
  write (a stale failure shouldn't claim OFFLINE over a
  fresh success).
* Stale-failure log line preserved so the error itself is
  still diagnosable in dev tools.
"""

from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"
APP_INIT = REPO / "app" / "__init__.py"


def _refresh_block() -> str:
    """Return the full body of `refreshTopbarStatus` from the
    `async function` declaration to its closing `}`. The
    function is ~800 lines so we walk forward to the matching
    brace rather than slicing a fixed window."""
    src = APP_JS.read_text()
    start = src.index("async function refreshTopbarStatus()")
    # Find the closing `}` of the function — first occurrence of
    # `\n  }\n` (two-space-indented closing brace at module
    # nesting level) after the start.
    end = src.index("\n  }\n", start)
    return src[start:end + 4]


def test_refresh_topbar_status_declares_seq_token():
    """The function must declare `refreshTopbarStatus._seq` +
    capture `_myToken` at function entry, BEFORE the api()
    await. Same shape as `loadLibrary._seq`."""
    body = _refresh_block()
    assert "refreshTopbarStatus._seq = " in body, (
        "v1.17.22: must declare _seq token at function entry."
    )
    assert "const _myToken = refreshTopbarStatus._seq" in body, (
        "v1.17.22: must capture _myToken before the await."
    )
    # Token capture must precede the api() call.
    seq_idx = body.index("const _myToken = refreshTopbarStatus._seq")
    api_idx = body.index("api('GET', '/api/stats')")
    assert seq_idx < api_idx, (
        "v1.17.22: _myToken must be captured BEFORE the api() "
        "await — capturing it later races against fast resumes."
    )


def test_success_path_bails_on_stale_token():
    """After `await api(...)`, the success path must check
    `_seq !== _myToken` and bail before touching the
    `_lastHash` / DOM state."""
    body = _refresh_block()
    # Look for the guard shortly after the await. v0.51.120: window 800 → 1700 —
    # the pre-bail _deriveEnumStashes write (button-lock stashes only) now sits
    # between the await and the guard. That's intentional: those stashes are kept
    # fresh even on a superseded poll; the guard still runs before _lastHash / DOM
    # (the actual stale-clobber targets), pinned below.
    await_idx = body.index("api('GET', '/api/stats')")
    success_window = body[await_idx:await_idx + 1700]
    assert "refreshTopbarStatus._seq !== _myToken" in success_window, (
        "v1.17.22: success path must check the seq token "
        "after the await."
    )
    # The guard must appear BEFORE the _lastHash write at
    # ~line 493 (which is the stale-clobber target).
    guard_idx = body.index("refreshTopbarStatus._seq !== _myToken")
    hash_idx = body.index("refreshTopbarStatus._lastHash = newHash")
    assert guard_idx < hash_idx, (
        "v1.17.22: the seq guard must run BEFORE _lastHash is "
        "written — pre-fix a stale resume would clobber the "
        "hash and freeze the topbar."
    )


def test_catch_path_bails_on_stale_token():
    """The catch block must also check the seq token before
    setting OFFLINE pill. A stale failure shouldn't paint
    OFFLINE on top of a fresh success."""
    body = _refresh_block()
    catch_idx = body.index("} catch (e) {")
    catch_window = body[catch_idx:]
    assert "refreshTopbarStatus._seq !== _myToken" in catch_window, (
        "v1.17.22: catch path must check the seq token before "
        "setting OFFLINE pill."
    )
    # The guard must appear BEFORE the OFFLINE class add.
    guard_idx = catch_window.index("refreshTopbarStatus._seq !== _myToken")
    offline_idx = catch_window.index("'op-pill-offline'")
    assert guard_idx < offline_idx, (
        "v1.17.22: stale-failure check must precede the "
        "OFFLINE pill write."
    )


def test_stale_failure_still_logs_error():
    """The stale-failure path must still console.error the
    underlying error — it's a real failure of THAT request
    even if a sibling request succeeded. Log line precedes
    or follows the stale-bail; either way the error is
    diagnosable in dev tools."""
    body = _refresh_block()
    catch_idx = body.index("} catch (e) {")
    catch_window = body[catch_idx:]
    # The stale guard branch must log before returning.
    stale_guard = catch_window.index("refreshTopbarStatus._seq !== _myToken")
    stale_branch = catch_window[stale_guard:stale_guard + 400]
    assert "console.error" in stale_branch, (
        "v1.17.22: stale-failure branch must still console."
        "error the underlying network failure — silent stale "
        "errors hide real outage signal across overlapping "
        "polls."
    )


# ── Pattern consistency with sibling _seq guards ──────────────


def test_seq_pattern_matches_load_library_load_queue():
    """The three _seq-guarded fetch sites (loadLibrary,
    loadQueue, refreshTopbarStatus) should use a uniform
    pattern — easier to audit, easier to teach."""
    src = APP_JS.read_text()
    # All three must declare `._seq = (X._seq || 0) + 1`
    for fn_name in ("loadLibrary", "loadQueue", "refreshTopbarStatus"):
        pattern = f"{fn_name}._seq = ({fn_name}._seq || 0) + 1"
        assert pattern in src, (
            f"v1.17.22: {fn_name} should use the canonical "
            f"`._seq = (._seq || 0) + 1` increment pattern."
        )


# ── Version pin (soft floor) ──────────────────────────────────


def test_version_pinned_at_or_above_1_17_22():
    src = APP_INIT.read_text()
    m = re.search(r'__version__\s*=\s*"(\d+)\.(\d+)\.(\d+)"', src)
    assert m
    found = tuple(int(x) for x in m.groups())
    assert found >= (0, 17, 22), (
        f"v1.17.22: __version__ must be >= 1.17.22 "
        f"(found {'.'.join(str(x) for x in found)})."
    )
