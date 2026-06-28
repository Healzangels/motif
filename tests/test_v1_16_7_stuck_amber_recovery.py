"""v1.16.7 — recover from stuck-amber DL chip after rapid-poll
expiry or tab throttling.

the user on v1.16.6:

> still see rows getting stuck amber DL but then refreshing it
> shows properly updated

## Diagnosis (3 contributing causes)

1. **Background-tab throttling.** Chromium / Firefox throttle
   `setInterval` in inactive tabs to ~1/min. The rapid-poll's
   60s ceiling was tight enough that the first throttled tick
   landed past `libraryRapidUntil` → poll stopped. When the
   user returned, no fresh `loadLibrary` fired and the row
   stayed amber.

2. **Long-running downloads.** yt-dlp extracts with rate-limit
   waits can take >60s. Each rapid-poll tick that sees in-
   flight rows re-extends the window, but if the window
   happened to expire between two ticks that both saw in-flight
   (rare but possible under sluggish browser timing), the poll
   could die.

3. **Browser HTTP cache.** `api()` used `fetch()` defaults. Some
   browsers cache repeated identical-URL GETs in the memory
   cache; a "row is in-flight" snapshot could be served back
   while the worker had actually finished.

## Fixes

### `app/web/static/app.js`

  1. `cache: 'no-store'` set on every api() fetch. Defensive
     against browser caching of API responses.
  2. Default rapid-poll ceiling raised 60_000 → 300_000 (5 min)
     so background-tab throttle + long-running downloads have
     more headroom.
  3. New `document.addEventListener('visibilitychange', ...)`
     that fires `loadLibrary()` when the tab becomes visible
     AND the library page is mounted. Catches the "I came back
     to the tab" case end-to-end — the cold-load auto-kick
     inside loadLibrary will respawn libraryRapidPoll if any
     fetched row still has in-flight state.

## Tests
"""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"


def _read() -> str:
    return APP_JS.read_text()


# ── 1. cache: 'no-store' on api() ────────────────────────────────

def test_api_helper_sets_cache_no_store():
    """The api() helper must set cache:'no-store' so the browser
    doesn't serve stale /api/library responses from its memory
    cache during the rapid-poll window."""
    js = _read()
    fn_idx = js.index("async function api(")
    body = js[fn_idx:fn_idx + 1500]
    assert "cache = 'no-store'" in body or "cache: 'no-store'" in body, (
        "v1.16.7: api() must set cache:'no-store'. Without it, "
        "browsers may serve repeated identical-URL GETs from "
        "their memory cache mid-rapid-poll, causing the UI to "
        "render stale in-flight state for rows whose worker "
        "has actually finished."
    )


# ── 2. rapid-poll ceiling extended ───────────────────────────────

def test_rapid_poll_default_ceiling_is_at_least_five_minutes():
    """The default `durationMs` parameter for libraryRapidPoll
    must be >= 300000 (5 minutes). 60s was insufficient for
    background-tab throttling + long yt-dlp downloads."""
    js = _read()
    sig = re.search(
        r"function libraryRapidPoll\(durationMs = (\d+)\)",
        js,
    )
    assert sig, "libraryRapidPoll signature not found"
    duration = int(sig.group(1))
    assert duration >= 300000, (
        f"v1.16.7: libraryRapidPoll default ceiling must be "
        f">= 300000 ms (5 min). Got {duration}. The pre-fix "
        "60000 ms was tight enough that background-tab "
        "throttling killed the poll before it could observe "
        "a long download finishing."
    )


# ── 3. visibilitychange listener ─────────────────────────────────

def test_visibilitychange_listener_registered():
    """A top-level visibilitychange listener must reload the
    library when the tab becomes visible — that's the recovery
    path for the background-tab throttle case."""
    js = _read()
    # Find the listener registration.
    assert "addEventListener('visibilitychange'" in js, (
        "v1.16.7: visibilitychange listener missing. Without "
        "it, returning to a backgrounded library tab leaves "
        "rows stale until the user takes an explicit action."
    )
    # The handler body must call loadLibrary().
    # v1.17.3: handler body grew (now also refreshes topbar / ops /
    # dashboard / queue on tab return). The pre-fix
    # `js.index("});", listener_idx)` found the FIRST `});` —
    # which after v1.17.3 lands inside one of the nested if-blocks,
    # not the handler close. Widen the bound to the next module-
    # level marker comment.
    listener_idx = js.index("addEventListener('visibilitychange'")
    # The next top-level comment ("v1.16.1:" was the v1.16.1
    # showModalNoFocusRing block right after the handler) bounds
    # us reliably regardless of internal structure.
    end_idx = js.index("// v1.16.1:", listener_idx)
    handler = js[listener_idx:end_idx]
    assert "document.visibilityState !== 'visible'" in handler, (
        "v1.16.7: handler must gate on visibilityState === "
        "'visible' so it only fires on the hidden→visible "
        "transition, not on every state change."
    )
    assert "loadLibrary()" in handler, (
        "v1.16.7: handler must call loadLibrary() to fetch "
        "fresh data + trigger the cold-load auto-kick that "
        "respawns libraryRapidPoll if rows still need it."
    )


def test_visibilitychange_listener_gated_on_library_body():
    """The handler must only fire `loadLibrary` when a library
    page is mounted (`#library-body` present). Otherwise it
    would attempt loadLibrary on /settings, /queue, /dash, etc.
    where the function may not be relevant."""
    js = _read()
    listener_idx = js.index("addEventListener('visibilitychange'")
    # v1.17.3: widened bound (see test above).
    end_idx = js.index("// v1.16.1:", listener_idx)
    handler = js[listener_idx:end_idx]
    assert "library-body" in handler, (
        "v1.16.7: handler must gate on #library-body so the "
        "loadLibrary call is scoped to library pages."
    )


# ── 4. cold-load auto-kick still wired (regression guard) ────────

def test_cold_load_auto_kick_still_fires_in_load_library():
    """The v1.15.85 cold-load auto-kick is what spawns
    libraryRapidPoll after the visibilitychange handler's
    loadLibrary call. v1.16.7 depends on this chain — pin it."""
    js = _read()
    # Anchor on the v1.15.85 marker.
    assert "v1.15.85: cold-load auto-kick" in js, (
        "v1.16.7: the cold-load auto-kick marker must still "
        "live in loadLibrary — that's the bridge that lets "
        "the new visibilitychange listener's loadLibrary "
        "respawn the rapid poll."
    )
    # And the actual call.
    assert (
        "dedupedItems.some((it) => !!it.job_in_flight)" in js
    ), "auto-kick condition (in-flight check) is the load-bearing logic"
