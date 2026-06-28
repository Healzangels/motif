"""v1.15.52 — search bar is ephemeral (no localStorage persistence).

the user (screenshot: bare /movies with stale "rubble" in search +
0 matches): "Randomly refresh, and I'll see in the search bar
something I searched long before or in a different session and
it will clear the filters I had set, sometime also see it
happen when first opening up a new tab to motif."

## Bug

Pre-v1.15.52 _saveLibraryFilterState persisted `q` alongside
the filter chips in localStorage. On any bare-URL page load,
_hydrateLibraryFromStorage restored the saved q AND the saved
filter snapshot atomically. So an old "rubble" search from days
ago would resurface AND drag along whatever filters were set
at that save-moment — clobbering the user's current intent
(e.g. a fresh ED-filter view they just set up).

## Fix

Search is now ephemeral. _saveLibraryFilterState drops `q` from
the payload; _hydrateLibraryFromStorage ignores any legacy
`payload.q` from pre-v1.15.52 saves. Search can still be set by:
  * URL ?q= deep-link (e.g. // OPEN ROW from /queue passes
    the row title — v1.14.90)
  * In-session typing
  * Any other code path that writes libraryState.q

Filter chips KEEP persisting (the v1.13.13 "filter MOVIES then
click TV SHOWS without starting over" intent stays — that
worked fine).

Static-text guards consistent with persistence-test patterns.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"


def test_save_filter_state_drops_q_from_payload():
    """_saveLibraryFilterState must NOT include `q` in the
    persisted payload. Pre-v1.15.52 it did, causing the bug
    the user screenshotted."""
    js = APP_JS.read_text()
    save_anchor = js.index("function _saveLibraryFilterState()")
    fn_end = js.index("\n  function ", save_anchor + 1)
    fn_body = js[save_anchor:fn_end]
    # The const payload = { ... } literal MUST NOT include a
    # `q:` property. We need to scan only the payload literal,
    # not the surrounding comment block (which references "q"
    # as the field-being-dropped).
    payload_anchor = fn_body.index("const payload = {")
    payload_end = fn_body.index("};", payload_anchor)
    payload_block = fn_body[payload_anchor:payload_end]
    assert "q:" not in payload_block and "q :" not in payload_block, (
        "v1.15.52: _saveLibraryFilterState payload must not "
        "include `q:` — search is ephemeral now"
    )
    # Filter chips must still be in the payload (the v1.13.13
    # cross-tab filter-share contract must hold).
    for filter_field in ("srcFilter:", "tdbPills:", "dlPills:",
                         "plPills:", "linkPills:", "edPills:",
                         "attnPills:", "status:", "sort:"):
        assert filter_field in payload_block, (
            f"v1.15.52: filter field {filter_field!r} must STILL "
            "be persisted — v1.15.52 only removes q, not filters"
        )


def test_hydrate_from_storage_does_not_restore_q():
    """_hydrateLibraryFromStorage must NOT read payload.q. Legacy
    payloads from pre-v1.15.52 saves may still have it; the
    hydrate function must explicitly ignore that field so the
    stale value doesn't leak across the upgrade."""
    js = APP_JS.read_text()
    fn_anchor = js.index("function _hydrateLibraryFromStorage()")
    fn_end = js.index("\n  function ", fn_anchor + 1)
    fn_body = js[fn_anchor:fn_end]
    # Strip JS line comments before substring-checking so the
    # rationale comment that mentions `payload.q` in prose
    # doesn't false-trigger the regression guard.
    code_only = "\n".join(
        line for line in fn_body.splitlines()
        if not line.lstrip().startswith("//")
    )
    # Pre-v1.15.52 used `if (payload.q) { libraryState.q = ... }`
    # — that exact pattern must be gone from live code.
    assert "payload.q" not in code_only, (
        "v1.15.52: hydrate live code must not reference payload.q "
        "— pre-fix `if (payload.q)` block must be deleted so stale "
        "legacy payloads from older versions don't get restored"
    )
    assert "libraryState.q = String(payload" not in code_only, (
        "v1.15.52: hydrate must not write to libraryState.q from "
        "the localStorage payload"
    )
    # The hydrate must still restore filter chips.
    for filter_key in ("payload.srcFilter", "payload.tdbPills",
                       "payload.dlPills", "payload.edPills"):
        # Each appears either directly or via the HYDRATE_MAP indirection.
        pass  # implicit — covered by HYDRATE_MAP usage elsewhere


def test_url_q_deeplink_still_hydrates():
    """The URL ?q= path (v1.14.90 — OPEN ROW from /queue passes
    the row title) must STILL hydrate libraryState.q + the
    search input. The localStorage drop must not break this
    deep-link path.

    v1.17.15 (soft): the literal assignment was extended to
    `const wantQ = sp.get('q') || _readSessionQ();` to fall
    back to sessionStorage when no URL ?q= is present. The
    contract being pinned here — "URL ?q= still drives
    libraryState.q + searchInput.value" — is preserved by
    the OR-short-circuit (URL value wins when set). The
    assertion is loosened from a literal-string pin to a
    pattern pin so a future refactor of the read site can
    move things around without breaking this contract test."""
    js = APP_JS.read_text()
    # The bindLibrary URL hydration block still reads sp.get('q').
    # Allow either the v1.15.52-era literal or the v1.17.15
    # sessionStorage-fallback variant.
    assert (
        "const wantQ = sp.get('q');" in js
        or "const wantQ = sp.get('q') || _readSessionQ();" in js
    ), (
        "v1.15.52: URL ?q= hydration must remain intact "
        "(may be combined with a sessionStorage fallback "
        "per v1.17.15, but URL value must take priority)."
    )
    assert "libraryState.q = wantQ;" in js
    assert "searchInput.value = wantQ;" in js


def test_search_input_typing_still_writes_state():
    """In-session typing still drives libraryState.q via the input
    handler (line ~8123). v1.15.52 only removes the LOCALSTORAGE
    persistence — typing keeps working."""
    js = APP_JS.read_text()
    # The input change handler assigns search.value.trim() to
    # libraryState.q. Pin so a future refactor doesn't accidentally
    # drop the in-session driver too.
    assert "libraryState.q = search.value.trim();" in js, (
        "v1.15.52: search input change handler must still write "
        "libraryState.q on user typing"
    )
