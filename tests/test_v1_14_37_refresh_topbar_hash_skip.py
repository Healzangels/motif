"""v1.14.37 — hash-skip on refreshTopbarStatus (frontend audit P1).

The 656-line `refreshTopbarStatus` function runs every 15s on the
topbar polling loop AND fires after every state-changing user
action (10+ call sites). Pre-fix every call re-ran ~600 lines of
DOM queries + classList toggles + textContent assigns + dataset
writes — most of them no-ops because the /api/stats payload
hadn't changed since the last poll.

On idle motif (the common case) this is pure CPU + paint waste.
The audit's frontend P1 finding flagged it as a "proven pattern
elsewhere" perf win — the loadLibrary tbody.dataset.lastHash
pattern (app.js ≈ 4470) does the same thing for the library row
render path.

## Fix

After the await api('GET', '/api/stats'), compute
`JSON.stringify(stats)` and compare to a function-property
cache (`refreshTopbarStatus._lastHash`). Match → return early
(skip all the DOM work). Mismatch → store the new hash + run
the body as before.

## Safety analysis

The skip would be unsafe if any of these were true:

  • The window.__motif_* globals get reset between ticks by
    something OTHER than refreshTopbarStatus
  • The OFFLINE-pill recovery depends on the body running on
    every tick

Verified via grep: window.__motif_(failed_count | themes_have |
cookies_present | enum_active) are written ONLY inside
refreshTopbarStatus + a localStorage-init early in the file.
The OFFLINE-pill set is in the catch{} branch which the hash-
check doesn't gate.

The first call always proceeds (lastHash is undefined). Callers
that fire after a state-change still hit the server (the await
runs unconditionally) — only the post-response paint work is
skipped when nothing actually changed.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


def _next_sibling_fn(js: str, start: int) -> int:
    """End of the app.js function beginning at `start`: the next SIBLING
    declaration at 2-space indent. v0.51.261 — a structural boundary the
    language guarantees, replacing a byte count that needed four bumps."""
    nxt = [i for i in (js.find("\n  function ", start + 10),
                       js.find("\n  async function ", start + 10)) if i != -1]
    return min(nxt) if nxt else len(js)


# ── Static-text guards on the fix ────────────────────────────


def test_refresh_topbar_status_uses_hash_skip():
    """The early-return based on the stats-payload hash must be
    in place. Pin every piece so a refactor can't silently
    weaken the cache (e.g. dropping the `return` would still
    record the hash but redo all the DOM work)."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    fn_anchor = js.index("async function refreshTopbarStatus()")
    body = js[fn_anchor:fn_anchor + 6000]  # v0.51.120 widened (pre-bail stash write)
    # The hash compute + early return + cache update.
    assert "JSON.stringify(stats)" in body
    assert "refreshTopbarStatus._lastHash === newHash" in body
    assert "return;" in body
    assert "refreshTopbarStatus._lastHash = newHash;" in body


def test_refresh_topbar_status_v1_14_37_marker_present():
    """The archaeology comment captures the rationale + the
    safety analysis (globals are write-once-only-here, OFFLINE
    branch is in catch{}). Pin so a future refactor that
    'simplifies' the cache reads the safety constraints."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert "v1.14.37: hash-skip" in js
    # Cross-references the proven loadLibrary pattern.
    fn_anchor = js.index("async function refreshTopbarStatus()")
    body = js[fn_anchor:fn_anchor + 5000]
    assert "loadLibrary" in body  # mentioned as the pattern source


def test_refresh_topbar_status_hash_compared_before_dom_work():
    """The hash check must happen IMMEDIATELY after the await —
    not later in the function. Pin via the offset between the
    await and the early-return. The whole point of the cache is
    to skip DOM work; if the check sits below 200 lines of DOM
    code, the savings are gone.

    v1.14.67: threshold widened from 1500 → 3000 chars to make
    room for the v1.14.67 client-state label-update block (~5
    LOC + a multi-line marker comment that explains why it must
    sit ABOVE the hash check). The widening is bounded — DOM
    work proper still sits below the hash check, the label
    update is a single textContent assign that can't be skipped
    without breaking the //4K toggle UX."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    fn_anchor = js.index("async function refreshTopbarStatus()")
    body = js[fn_anchor:fn_anchor + 6000]
    await_idx = body.index("await api('GET', '/api/stats')")
    return_idx = body.index("refreshTopbarStatus._lastHash === newHash) return")
    delta = return_idx - await_idx
    # v0.51.120: threshold 3000 → 4000 to make room for the pre-bail
    # _deriveEnumStashes write (cheap window-stash assigns, NOT DOM work — the
    # expensive DOM code still sits below the hash-skip, so the cache savings
    # hold). Same bounded-widening case as v1.14.67's label block.
    assert delta < 4000, (
        f"Hash-check sits {delta} chars after the await — too far. "
        "It should land immediately after to maximize the skip "
        "window. If the function grew with real DOM work (not the "
        "cheap pre-bail stash write), move the check up or factor it out."
    )


def test_refresh_topbar_status_offline_recovery_outside_hash_skip():
    """Critical safety pin: the OFFLINE-pill recovery in the
    catch{} block must NOT be inside the hash-skip path. If the
    server stops responding mid-poll, we MUST flip the idle pill
    to OFFLINE regardless of what the cached hash says."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    fn_anchor = js.index("async function refreshTopbarStatus()")
    # v0.51.261: was a byte count, bumped FOUR times for growth that was never
    # a regression — 50000 (v1.18.52) → 60000 → 64000 (v0.50.14) → 66000
    # (v0.51.63) → 68000 (v0.51.120), the last of which sat 485 chars from
    # failing again. The comment those bumps carried already named the right
    # boundary ("well-bounded by the next async function declaration") without
    # using it. Now it does.
    body = js[fn_anchor:_next_sibling_fn(js, fn_anchor)]
    # The catch{} block sets the OFFLINE class — must be present.
    catch_anchor = body.index("} catch (e) {")
    catch_block = body[catch_anchor:catch_anchor + 1000]
    assert "op-pill-offline" in catch_block
    # And it sits AFTER the early-return (i.e. the catch is the
    # post-await error path, not gated by the hash).
    return_idx = body.index("refreshTopbarStatus._lastHash === newHash) return")
    assert return_idx < catch_anchor, (
        "Hash-skip return must come BEFORE the catch{} block — "
        "otherwise the OFFLINE recovery is stuck behind the "
        "cache check."
    )


# ── Reuse pin: the loadLibrary hash-skip pattern stays ───────


def test_load_library_hash_skip_pattern_still_present():
    """The loadLibrary tbody.dataset.lastHash pattern is the
    proven prior-art the v1.14.37 fix mirrors. Pin so it
    doesn't regress out from under the new symmetry."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert "tbody.dataset.lastHash" in js
