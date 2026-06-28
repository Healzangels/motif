"""v1.18.70 — Plex size-rejection goes terminal in one attempt;
filter+search state flushes on every pagehide.

the user's report after v1.18.69 deploy:

  > it looks like we stall here for a long time even though logs
  > indicate it's failing
  > also I have found that if I change a filter, the navigate to
  > logs then back to the section instead of being on the last
  > filter I set it will have been on the filter from before or
  > an earlier session it seems.
  > I also canceled the job that was pending for a long period
  > which brought us back to a manual state

Two unrelated fixes ride together because both surfaced from the
same testing session:

## Fix 1 — Plex size-rejection → _JobPermanentFailure

When `_do_place_collection` raises on a size-rejection (HTTP 500 +
size >= 20MB) AND the v1.18.69 sidecar fallback couldn't fire
(collection rows have no media folder, or the fallback itself
errored), the prior `raise RuntimeError(note)` ran through the
worker's retry-with-backoff path:

  - attempt 1 fails → next_run = now + 1 min → status='pending'
  - attempt 2 fails → next_run = now + 5 min → status='pending'
  - attempt 3 fails → status='failed' (terminal)

The dashboard "PLACE INTO PLEX QUEUED" mini-bar reads `status =
pending` between attempts → user sees a perpetual "queued" state
for ~6 minutes before the job goes terminal. the user manually
canceled to bail out.

Plex consistently rejects the same bytes at the same rk —
retrying every minute is pure waste. v1.18.70 raises
`_JobPermanentFailure` for the size-rejection case so the job goes
terminal in ONE attempt. Other upload failures (network errors,
HTTP 502/503, 500 on small files) stay on the RuntimeError retry
path — those CAN succeed on retry.

## Fix 2 — pagehide flushes filter+search state

`_saveLibraryFilterState()` is called inside `loadLibrary()` at
line 6644 — fires when the library refreshes after a filter
change. But on a fast click→navigate sequence:

  1. User clicks filter chip → libraryState mutated
  2. Filter handler calls loadLibrary() (async fetch)
  3. User clicks /logs nav link BEFORE loadLibrary's
     synchronous prelude reaches the save call

The page navigates with the OLD localStorage snapshot still
intact. Returning to /movies hydrates from the OLD snapshot →
filter state visibly regresses.

`pagehide` is the canonical "page is about to be hidden/unloaded"
event (bfcache-safe, fires on tab close + nav + back/forward).
A listener that calls `_saveLibraryFilterState()` +
`_writeSessionQ(libraryState.q)` flushes the current state every
time the user navigates away. The save is idempotent — if
loadLibrary already saved, this is a no-op write.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── Fix 1: permanent failure on size-rejection ──────────────


def _place_collection_body() -> str:
    src = (REPO / "app" / "core" / "worker.py").read_text()
    fn_idx = src.index("def _do_place_collection(")
    nxt = src.find("\n    def ", fn_idx + 1)
    if nxt == -1:
        nxt = len(src)
    return src[fn_idx:nxt]


def test_size_rejection_raises_job_permanent_failure():
    """When the upload fails with HTTP 500 + size>=20MB AND the
    v1.18.69 sidecar fallback didn't fire (collection, or fallback
    itself errored), the worker must raise _JobPermanentFailure
    so the job goes terminal in one attempt instead of cycling
    through the 1m/5m retry backoff."""
    body = _place_collection_body()
    # The classification line names the case. v1.19.94: the 20MB
    # literal moved to the _PLEX_THEME_UPLOAD_CEILING_MB constant (10).
    assert "size_rejection = (size_mb >= _PLEX_THEME_UPLOAD_CEILING_MB" in body, (
        "v1.18.70/v1.19.94: size-rejection classification required so "
        "the raise branch can pick _JobPermanentFailure vs RuntimeError"
    )
    assert "and http_status == 500)" in body
    # The permanent-failure raise must fire on size_rejection.
    assert "if size_rejection:" in body
    assert "raise _JobPermanentFailure(note)" in body


def test_non_size_failure_still_uses_runtime_error():
    """Non-size failures (network error, HTTP 502/503, 500 on
    small files) must STILL raise RuntimeError so the worker's
    retry-with-backoff path picks them up. Those failures CAN
    succeed on retry — only Plex's empirical 25MB ceiling is
    consistent across attempts."""
    body = _place_collection_body()
    # The fallthrough raise (non-size case) is still RuntimeError.
    # Pre-fix the function ended with `raise RuntimeError(note)`;
    # post-fix the size-rejection branch raises permanent BEFORE
    # the fallthrough RuntimeError.
    assert "raise RuntimeError(note)" in body, (
        "v1.18.70: non-size failures must retain RuntimeError "
        "(transient retry budget)"
    )


def test_permanent_failure_branch_orderd_correctly():
    """The size_rejection check must precede the RuntimeError
    fallthrough. Pin the ordering so a refactor can't accidentally
    invert it (which would re-introduce the retry-loop)."""
    body = _place_collection_body()
    perm_idx = body.index("raise _JobPermanentFailure(note)")
    rt_idx = body.index("raise RuntimeError(note)", perm_idx)
    # Both raises must be inside the `if fell_back_kind is None:`
    # block. The permanent raise comes BEFORE the RuntimeError
    # fallthrough so the size case short-circuits.
    gate_idx = body.index("if fell_back_kind is None:")
    assert gate_idx < perm_idx < rt_idx, (
        "v1.18.70: the permanent-failure raise must precede the "
        "RuntimeError fallthrough inside the no-fallback branch"
    )


def test_permanent_failure_keeps_size_hint_in_note():
    """The note formatted for the size-rejection case still
    includes the v1.18.68 '25MB ceiling' hint so the failed-job
    `last_error` column surfaces actionable info."""
    import re
    body = _place_collection_body()
    # Collapse adjacent-string-literal concatenation (`"a" "b"`).
    body_flat = re.sub(r'"\s*"', "", body)
    assert "large files often fail Plex theme upload" in body_flat
    assert "10MB ceiling" in body_flat


def test_v1_18_70_marker_explains_retry_change():
    """v1.18.70 marker must explain WHY the size-rejection branch
    became permanent. Future readers wondering 'why doesn't this
    retry?' should land in the marker."""
    body = _place_collection_body()
    assert "v1.18.70" in body
    body_flat = " ".join(body.split())
    # the user's repro reference + the retry-burn rationale.
    assert ("burn" in body_flat or "stalled" in body_flat
            or "PLACE INTO PLEX QUEUED" in body_flat), (
        "v1.18.70: marker should reference the retry-loop user "
        "impact (the user's 'PLACE INTO PLEX QUEUED' stall)"
    )


# ── Fix 2: pagehide listener flushes filter+search state ────


def _app_js() -> str:
    return (REPO / "app" / "web" / "static" / "app.js").read_text()


def test_pagehide_listener_registered_in_bindlibrary():
    """A `pagehide` listener must be installed during bindLibrary
    setup so any navigation away from a library tab flushes the
    current filter state. Pin the listener shape + the body's
    save calls."""
    js = _app_js()
    # The listener registration must appear inside bindLibrary.
    bind_idx = js.index("function bindLibrary()")
    # End anchor: next top-level function definition.
    bind_end = js.index("\n  function ", bind_idx + 1)
    bind_body = js[bind_idx:bind_end]
    assert "addEventListener('pagehide'" in bind_body, (
        "v1.18.70: pagehide listener must be installed during "
        "bindLibrary so it fires on every navigation away"
    )


def test_pagehide_listener_saves_filter_state():
    """The listener body must call `_saveLibraryFilterState()` so
    the filter snapshot is current when the user comes back."""
    js = _app_js()
    bind_idx = js.index("function bindLibrary()")
    bind_end = js.index("\n  function ", bind_idx + 1)
    bind_body = js[bind_idx:bind_end]
    # Find the pagehide listener block.
    pagehide_idx = bind_body.index("addEventListener('pagehide'")
    # Walk to the matching close brace — listener body fits in a
    # few hundred chars.
    block = bind_body[pagehide_idx:pagehide_idx + 600]
    assert "_saveLibraryFilterState()" in block, (
        "v1.18.70: pagehide must flush filter state"
    )
    assert "_writeSessionQ(libraryState.q" in block, (
        "v1.18.70: pagehide must also flush the search query so "
        "in-flight ?q= input is captured pre-navigation"
    )


def test_pagehide_listener_swallows_storage_exceptions():
    """Storage writes can throw in private/incognito mode or
    under quota. The listener must wrap the save in try/catch so
    a navigation isn't blocked by a thrown SecurityError."""
    js = _app_js()
    bind_idx = js.index("function bindLibrary()")
    bind_end = js.index("\n  function ", bind_idx + 1)
    bind_body = js[bind_idx:bind_end]
    pagehide_idx = bind_body.index("addEventListener('pagehide'")
    block = bind_body[pagehide_idx:pagehide_idx + 600]
    # try/catch around the save calls.
    assert "try {" in block
    assert "catch (_)" in block


def test_pagehide_marker_explains_race():
    """v1.18.70 marker required so a future refactor doesn't
    treat this listener as redundant with loadLibrary's save."""
    js = _app_js()
    bind_idx = js.index("function bindLibrary()")
    bind_end = js.index("\n  function ", bind_idx + 1)
    bind_body = js[bind_idx:bind_end]
    pagehide_idx = bind_body.index("addEventListener('pagehide'")
    block_pre = bind_body[max(0, pagehide_idx - 2000):pagehide_idx]
    assert "v1.18.70" in block_pre
    # The comment must reference the race + the fallback safety.
    block_flat = " ".join(block_pre.split())
    assert "race" in block_flat.lower() or "before" in block_flat.lower()


def test_pagehide_idempotent_with_loadlibrary_save():
    """Pin via comment that the listener is intentionally
    redundant with loadLibrary's save — the redundancy IS the
    feature (catches the click→nav race). Future code-archaeology
    that "deduplicates" the saves needs to confront the
    rationale."""
    js = _app_js()
    bind_idx = js.index("function bindLibrary()")
    bind_end = js.index("\n  function ", bind_idx + 1)
    bind_body = js[bind_idx:bind_end]
    pagehide_idx = bind_body.index("addEventListener('pagehide'")
    block_pre = bind_body[max(0, pagehide_idx - 2000):pagehide_idx]
    block_flat = " ".join(block_pre.split())
    assert ("idempotent" in block_flat.lower()
            or "no-op" in block_flat.lower()), (
        "v1.18.70: marker should explicitly note the listener is "
        "idempotent vs loadLibrary's save (so future readers don't "
        "deduplicate them)"
    )
