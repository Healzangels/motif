"""v1.14.63 — static test for the v1.14.62 button-lock-label drift class.

The v1.14.62 fix split a single predicate that drove BOTH
`.disabled` and `.textContent` for the dash REFRESH PLEX button.
Pre-fix the predicate was `themerrdbBusy || plexEnumBusy` —
correct for the disable side (writer-lock prevention against
TDB-sync's BEGIN IMMEDIATE) but wrong for the label, which then
falsely showed "// REFRESHING PLEX…" during a TDB sync.

The general contract:

  **A button's busy LABEL may only depend on state predicates
  that belong to THAT BUTTON'S OWN OPERATION. The DISABLED flag
  may be broader (writer-lock prevention is correct).**

This test pins the contract for the two dashboard sync/refresh
buttons that historically had the lock-vs-label confusion.
Tests are designed to FAIL on the pre-v1.14.62 buggy shape
(verified by reverting the fix locally and re-running).

Same shape as v1.13.77 (which closed the OPPOSITE direction
via `cascadeInFlight` narrowing — SYNC THEMERRDB's label no
longer flips when REFRESH PLEX clicks) and v1.14.62 (REFRESH
PLEX label no longer flips when SYNC THEMERRDB clicks).

## Scope

Specific to refreshTopbarStatus poll-driven button-state
updates. The CLICK-handler optimistic label sets (~line 1909
for REFRESH PLEX, similar for SYNC THEMERRDB) are correct
because they fire only when the user clicks THAT button.
"""
from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
JS = REPO / "app" / "web" / "static" / "app.js"


def _refresh_topbar_status_block() -> str:
    """Slice the JS file to only the refreshTopbarStatus function
    body. Anchors on the `async function refreshTopbarStatus()`
    declaration; the body extends to the next top-level
    `async function` or `function` declaration at the same
    indent."""
    js = JS.read_text()
    start = js.index("async function refreshTopbarStatus()")
    body = js[start:]
    next_fn = re.search(r"\n  (async )?function ", body[100:])
    if next_fn is None:
        return body
    return body[:100 + next_fn.start()]


def _btn_state_block(btn_id: str) -> str:
    """Pull the if/else block in refreshTopbarStatus that sets
    state for the given button id. The block runs from the
    `getElementById('<btn_id>')` line forward ~2000 chars."""
    block = _refresh_topbar_status_block()
    needle = f"getElementById('{btn_id}')"
    idx = block.index(needle)
    return block[idx:idx + 2000]


def _strip_js_comments(text: str) -> str:
    """Strip JS line comments (// ...) so marker comments
    mentioning the buggy variable names don't trip our checks."""
    return "\n".join(
        line for line in text.splitlines()
        if not line.lstrip().startswith("//")
    )


# ── Direct contract pins (would have caught v1.14.62) ────────


def test_dash_refresh_plex_textContent_assignment_uses_plex_enum_only():
    """The post-v1.14.62 contract: the REFRESH PLEX button's
    textContent must be assigned via a ternary whose condition
    is `plexEnumBusy` alone — NOT a combined predicate that
    includes themerrdbBusy.

    Pre-fix the textContent was set inside an
    `if (dashPlexBusy) { ... }` block where
    `dashPlexBusy = themerrdbBusy || plexEnumBusy`. The label
    flipped to "// REFRESHING PLEX…" during a TDB sync."""
    block = _strip_js_comments(_btn_state_block("sync-plex-btn"))
    # Pin the ternary shape: textContent = plexEnumBusy ? '...' : '...'
    pattern = re.compile(
        r"\.textContent\s*=\s*plexEnumBusy\s*\?\s*"
        r"'//\s*REFRESHING PLEX[^']*'\s*"
        r":\s*'//\s*REFRESH PLEX'",
    )
    assert pattern.search(block), (
        "REFRESH PLEX button's textContent assignment doesn't "
        "match the v1.14.62 contract: a ternary on `plexEnumBusy` "
        "alone (NOT a combined themerrdbBusy||plexEnumBusy predicate). "
        "Pre-fix the assignment lived inside `if (dashPlexBusy) "
        "{ ... textContent = '// REFRESHING PLEX…' }` where "
        "dashPlexBusy = themerrdbBusy || plexEnumBusy → false "
        "REFRESHING label during a TDB sync."
    )


def test_dash_refresh_plex_no_combined_busy_local():
    """The pre-fix `const dashPlexBusy = themerrdbBusy ||
    plexEnumBusy;` local must not survive. That variable was
    THE bug: it coupled the disabled decision (legit) with
    the label decision (wrong). Banning the local at the
    syntactic level prevents the shape from being silently
    re-introduced under refactoring."""
    code = _strip_js_comments(_refresh_topbar_status_block())
    assert "const dashPlexBusy" not in code, (
        "The pre-v1.14.62 `const dashPlexBusy = themerrdbBusy "
        "|| plexEnumBusy;` local survived. This was the variable "
        "that drove both .disabled and .textContent off the same "
        "OR-predicate. Either delete it or rename so the contract "
        "isn't fragile."
    )


def test_dash_refresh_plex_disable_predicate_is_plex_enum_only():
    """v1.14.66 supersedes the v1.13.63 writer-lock-prevention
    rationale this test originally asserted. The long worker is
    single-threaded (worker.py:_LONG_JOB_TYPES one-thread spawn)
    so a plex_enum queued during a TDB sync just waits in queue —
    no concurrent BEGIN IMMEDIATE contention is possible.

    The disable predicate is now `plexEnumBusy` only. The button
    stays clickable during a TDB sync, the click queues a
    plex_enum, and the long worker runs it after sync drains."""
    block = _strip_js_comments(_btn_state_block("sync-plex-btn"))
    # Narrowed predicate.
    pattern = re.compile(r"\.disabled\s*=\s*plexEnumBusy\s*;")
    assert pattern.search(block), (
        "REFRESH PLEX button's `.disabled` assignment isn't "
        "`plexEnumBusy` only. v1.14.66 narrowed it from "
        "`themerrdbBusy || plexEnumBusy` — see the test file's "
        "module docstring + the v1.14.66 marker in app.js."
    )
    # Pre-v1.14.66 form must not return.
    assert "themerrdbBusy || plexEnumBusy" not in block


# ── Inverse direction: SYNC THEMERRDB label is also narrow ──


def test_dash_sync_themerrdb_label_predicate_is_narrowed():
    """The SYNC THEMERRDB button's label-set must be gated on
    `dashSyncBtnBusy`, which is `themerrdbBusy || cascadeInFlight`
    (both halves are TDB-side: the sync itself or its post-sync
    auto-enum cascade). Pre-v1.13.77 the predicate used
    `globalEnumPipeline` which included scan_all (manual
    REFRESH PLEX) — clicking REFRESH PLEX from the dashboard
    incorrectly flipped SYNC THEMERRDB to "// SYNCING…".

    Mirror of the v1.14.62 fix in the opposite direction."""
    full = _strip_js_comments(_refresh_topbar_status_block())
    # The narrowed v1.13.77 shape.
    assert "const dashSyncBtnBusy = themerrdbBusy || cascadeInFlight;" in full, (
        "v1.13.77 `dashSyncBtnBusy` narrowing missing or changed."
    )
    # Pre-v1.13.77 broad forms must not survive.
    assert "const dashSyncBtnBusy = themerrdbBusy || globalEnumPipeline" not in full
    assert "const dashSyncBtnBusy = themerrdbBusy || plexEnumBusy" not in full


# ── Generic shape guard: no OR-local driving combined assigns ─


# Allowlist for OR-locals that legitimately drive a combined
# disabled+label block. Each entry must carry a one-line
# rationale documenting why the coupling is intentional. A new
# OR-local matching the suspect shape MUST either be split (the
# v1.14.62 fix pattern) or added to this allowlist with a
# comment justifying the exception. Shipping a new entry without
# a rationale is the regression we're guarding against.
_OR_LOCAL_ALLOWLIST: dict[str, str] = {
    # Per-tab REFRESH MOVIES/TV/ANIME button. `stillBusy =
    # libRefreshBusy || graceActive` — `libRefreshBusy` itself is
    # already an OR (`myTabBusy || globalEnumPipeline`), making
    # `stillBusy` a transitive 3-way OR. Borderline case:
    # `globalEnumPipeline` includes `themerrdbBusy && autoEnum`
    # (cascade pending) which means the button briefly shows
    # "// REFRESHING…" during the TDB sync phase BEFORE the
    # per-tab cascade actually starts. the user accepted this in
    # v1.13.65/.69 because the cascade WILL hit this tab within
    # seconds and the user clicked SYNC THEMERRDB knowing both
    # phases would run. The label ambiguity is bounded (~10s)
    # and shorter than the user's mental model of "syncing".
    # If/when the per-tab UX gets stricter contract-checking,
    # split the labels per-tab using narrower per-tab signals.
    "stillBusy": (
        "per-tab REFRESH; cascade-pending TDB sync briefly mis-"
        "labels but the user accepted the bounded ambiguity in "
        "v1.13.65/.69. Tighten only if user-visible mid-flight "
        "label flicker becomes a real complaint."
    ),
}


def test_no_or_local_drives_combined_disabled_label_in_refresh_topbar_status():
    """Catch the ROOT shape of v1.14.62: a `const X = A || B;`
    local definition followed by an `if (X) { ... .disabled =
    true; .textContent = '...ING…' ... }` block. That shape
    couples the disable decision (legitimately broad) with the
    label decision (must reflect own-operation only).

    Walks every `const X = ... || ...;` definition in
    refreshTopbarStatus, then for each X, scans for an
    `if (X) { ... }` block that sets BOTH `.disabled = true` AND
    a `.textContent = '...ING…'` busy label. Any match not in
    `_OR_LOCAL_ALLOWLIST` is a suspect — either split the
    statements (v1.14.62 pattern) or add an allowlist entry
    with a rationale.

    The allowlist exists because some buttons have legitimate
    multi-condition busy states (e.g. per-tab REFRESH during
    cascade-pending TDB sync). Each entry must explain why the
    coupling is intentional; the test fails on any new
    unjustified OR-local."""
    code = _strip_js_comments(_refresh_topbar_status_block())
    or_locals: list[str] = re.findall(
        r"const (\w+)\s*=\s*[^;]*\|\|[^;]*;", code,
    )
    suspects: list[str] = []
    for name in or_locals:
        if name in _OR_LOCAL_ALLOWLIST:
            continue
        if_pattern = re.compile(
            rf"if\s*\(\s*{re.escape(name)}\s*\)\s*\{{([^}}]{{0,500}}?)\}}",
            flags=re.DOTALL,
        )
        for m in if_pattern.finditer(code):
            body = m.group(1)
            sets_disabled = re.search(
                r"\.disabled\s*=\s*true", body,
            )
            sets_busy_label = re.search(
                r"\.textContent\s*=\s*['\"][^'\"]*ING…?['\"]", body,
            )
            if sets_disabled and sets_busy_label:
                suspects.append(name)
                break
    assert not suspects, (
        f"Suspect shape detected: const(s) {suspects} are OR-derived "
        f"locals that gate an `if (X) {{ ... .disabled = true; "
        f".textContent = '...ING…' ... }}` block — same root shape "
        f"as v1.14.62 pre-fix. The OR couples disable + label "
        f"decisions; split into separate statements with a narrower "
        f"label predicate, OR add the local name to "
        f"_OR_LOCAL_ALLOWLIST in this file with a one-line "
        f"rationale explaining why the coupling is intentional."
    )


def test_or_local_allowlist_entries_still_exist_in_source():
    """Sanity: every name in `_OR_LOCAL_ALLOWLIST` must still
    appear in refreshTopbarStatus. Stale allowlist entries
    accumulate noise and lose their rationale value over time."""
    code = _strip_js_comments(_refresh_topbar_status_block())
    stale: list[str] = []
    for name in _OR_LOCAL_ALLOWLIST:
        if not re.search(rf"\bconst {re.escape(name)}\b\s*=", code):
            stale.append(name)
    assert not stale, (
        f"Allowlist entries {stale} no longer match a `const ... `"
        f"definition in refreshTopbarStatus — likely renamed or "
        f"deleted. Remove the stale entries from _OR_LOCAL_ALLOWLIST."
    )
