"""v1.17.15 — library search persists across tab navigation.

the user's ask:
> can we make it so search text from one library to search to
> another so you can search yu-gi-oh in movies and then go to tv
> and would be still in the search bar

The motif library is three separate URLs — `/movies`, `/tv`,
`/anime` — and clicking between them triggers a full-page
navigation. Pre-fix the search input cleared on every nav: the
in-memory `libraryState.q` only survived the page that wrote it.

## Why sessionStorage (not localStorage)

A previous design pass (v1.15.52) explicitly REMOVED localStorage
persistence of `q` because old searches were resurfacing:

> Pre-fix an old search query (e.g. "rubble" from days ago) would
> resurface on a fresh tab / refresh + drag along the stale
> filter snapshot from that save-moment, clobbering the user's
> current intent. the user: "I'll see in the search bar something
> I searched long before or in a different session and it will
> clear the filters I had set."

`sessionStorage` threads exactly the right needle:
* Persists across page navigations WITHIN one browser tab —
  `/movies` → `/tv` → `/anime` carries the search.
* Clears on tab close — no stale-query resurfacing days later.
* Separate per tab — a fresh tab doesn't see the search from
  your other open tab (which is what the v1.15.52 complaint was
  about).

## Implementation

Three helpers in `app.js`:
* `_writeSessionQ(q)` — write/clear the session-scoped query.
* `_readSessionQ()` — read it (empty string if missing).
* `_clearSessionQ()` — explicit clear, used by ✕ and CLEAR ALL.

Hook sites:
1. **Search input** writes on every debounced typing event.
2. **Clear ✕ button** clears the session key (the visible input
   AND the persistence — otherwise a tab-switch would resurrect
   what you just dismissed).
3. **CLEAR ALL** also clears the session key, since the user is
   asking to reset everything.
4. **bindLibrary URL hydration** falls back to sessionStorage
   when the URL has no `?q=`. URL deep-links still win.
"""

from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"
APP_INIT = REPO / "app" / "__init__.py"


# ── Helpers exist ─────────────────────────────────────────────


def test_session_q_key_constant_defined():
    """`_LIB_SESSION_Q_KEY` must be a single source of truth for
    the storage key. All three helpers use it."""
    src = APP_JS.read_text()
    assert "_LIB_SESSION_Q_KEY = 'motif:library_q'" in src, (
        "v1.17.15: must declare `_LIB_SESSION_Q_KEY` constant. "
        "Keep the value `'motif:library_q'` so a future audit "
        "can grep for both the constant and the literal."
    )


def test_session_q_helpers_defined():
    """The three helpers (_writeSessionQ / _readSessionQ /
    _clearSessionQ) must exist."""
    src = APP_JS.read_text()
    assert "function _writeSessionQ(q)" in src
    assert "function _readSessionQ()" in src
    assert "function _clearSessionQ()" in src


def test_session_q_helpers_use_session_storage_not_local():
    """v1.15.52 explicitly rejected localStorage for `q`. Pin
    that the helpers use `sessionStorage` so the fix can't
    silently regress to the old behavior."""
    src = APP_JS.read_text()
    # Find the helpers and assert they reference sessionStorage.
    for fn in ("_writeSessionQ", "_readSessionQ", "_clearSessionQ"):
        idx = src.index(f"function {fn}(")
        body = src[idx:idx + 400]
        assert "sessionStorage" in body, (
            f"v1.17.15: {fn} must use sessionStorage (not "
            "localStorage — see v1.15.52 rationale)."
        )
        assert "localStorage" not in body, (
            f"v1.17.15: {fn} must NOT use localStorage."
        )


# ── Hook sites ────────────────────────────────────────────────


def test_search_input_writes_session_q():
    """The debounced search input handler must call
    _writeSessionQ(libraryState.q) so typing carries across
    tab nav."""
    src = APP_JS.read_text()
    idx = src.index("search?.addEventListener('input'")
    window = src[idx:idx + 1200]
    assert "_writeSessionQ(libraryState.q)" in window, (
        "v1.17.15: the search input handler must persist the "
        "trimmed query to sessionStorage."
    )


def test_clear_x_button_clears_session_q():
    """The ✕ clear button must remove the sessionStorage key,
    otherwise a tab-switch would resurrect what was just
    dismissed."""
    src = APP_JS.read_text()
    # The clear-✕ handler is in bindLibrary near the search
    # input setup. Anchor on the comment marker.
    idx = src.index("if (clearBtn) {")
    # Find the immediately following addEventListener block.
    window = src[idx:idx + 1500]
    assert "_clearSessionQ()" in window, (
        "v1.17.15: the search-clear ✕ button must clear the "
        "sessionStorage key — otherwise a tab-switch would "
        "resurrect the just-dismissed query."
    )


def test_clear_all_button_clears_session_q():
    """CLEAR ALL must drop the session-persisted query too."""
    src = APP_JS.read_text()
    # Anchor on the unique CLEAR ALL handler.
    idx = src.index("library-clear-all")
    window = src[idx:idx + 3000]
    assert "_clearSessionQ()" in window, (
        "v1.17.15: CLEAR ALL must clear the session-persisted "
        "search so the next tab-switch doesn't bring back the "
        "search the user just blew away."
    )


def test_bind_library_falls_back_to_session_q():
    """When URL has no ?q=, bindLibrary must read from
    sessionStorage. URL deep-links still win."""
    src = APP_JS.read_text()
    # The hydration site uses `sp.get('q') || _readSessionQ()`.
    idx = src.index("// v1.14.90: ?q= search-term hydration")
    window = src[idx:idx + 2500]
    assert "_readSessionQ()" in window, (
        "v1.17.15: bindLibrary URL hydration must fall back to "
        "_readSessionQ() when the URL has no ?q= param."
    )
    # The pattern should be `URL || sessionStorage` (not the
    # other way around) so deep-links still take priority.
    assert "sp.get('q') || _readSessionQ()" in window, (
        "v1.17.15: URL ?q= must take priority over sessionStorage "
        "fallback — `sp.get('q') || _readSessionQ()`."
    )


def test_bind_library_seeds_session_q_from_url():
    """When the URL has ?q=, also write to sessionStorage so a
    deep-link → tab-switch flow continues to carry the search.
    Without this seeding, a deep-link wouldn't survive the
    next nav click."""
    src = APP_JS.read_text()
    idx = src.index("// v1.14.90: ?q= search-term hydration")
    window = src[idx:idx + 2500]
    assert "_writeSessionQ(wantQ)" in window, (
        "v1.17.15: URL-driven hydration must seed sessionStorage "
        "so a deep-link → tab-nav flow keeps the search active."
    )


# ── Counter-pin: legacy localStorage path still excludes q ────


def test_local_storage_state_save_still_excludes_q():
    """The v1.15.52 fix removed `q` from the localStorage
    payload because old searches were resurfacing across
    sessions. v1.17.15 explicitly does NOT re-introduce that
    pattern — `q` belongs in sessionStorage only."""
    src = APP_JS.read_text()
    idx = src.index("function _saveLibraryFilterState()")
    body = src[idx:idx + 2000]
    # The payload object must not include a `q` key.
    assert "q: libraryState.q" not in body, (
        "v1.17.15: _saveLibraryFilterState must continue to "
        "exclude `q` from the localStorage payload (v1.15.52). "
        "Search persistence lives in sessionStorage now."
    )
    assert "q:" not in body.split("const payload")[1].split("};")[0], (
        "v1.17.15: the localStorage payload object must not "
        "have a `q:` field."
    )


# ── Version pin (soft floor) ──────────────────────────────────


def test_version_pinned_at_or_above_1_17_15():
    src = APP_INIT.read_text()
    m = re.search(r'__version__\s*=\s*"(\d+)\.(\d+)\.(\d+)"', src)
    assert m
    found = tuple(int(x) for x in m.groups())
    assert found >= (0, 17, 15), (
        f"v1.17.15: __version__ must be >= 1.17.15 "
        f"(found {'.'.join(str(x) for x in found)})."
    )
