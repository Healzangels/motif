"""v1.14.79 — updateLibraryRefreshBtnLabel sets disabled state, not just label.

the user: "clicking refresh plex on the Standard movies section
it shows refreshing and locked, navigating to 4k movies shows
that button as clickable and unlocked like it should,
navigating back to the movies standard even though it's refresh
is still running the button will have become clickable and say
refresh movies instead of refreshing movies since it's still
running."

## Diagnosis

The v1.14.68 helper updated only the LABEL on chip toggle.
Disabled state was left to refreshTopbarStatus's main block,
which:
  1. Awaits /api/stats (hits the v1.13.10 750ms TTL cache)
  2. Hits the v1.14.37 hash-skip when stats payload matches
     the prior poll
  3. Returns early without running the libRefreshBtn block

Result: chip toggle updated label only; the disable update
could lag a full poll interval.

## Fix

Helper now stashes globalEnumPipeline + enumPending on
window globals (alongside the existing window.__motif_enum_active),
and recomputes the FULL libRefreshBusy predicate (myTabBusy +
pipeline + grace) at chip-toggle time so the toggle reflects
the lock synchronously.
"""
from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
JS = REPO / "app" / "web" / "static" / "app.js"


def _helper_body() -> str:
    src = JS.read_text()
    fn_start = src.index("function updateLibraryRefreshBtnLabel(")
    fn_end = src.index("\n  }\n", fn_start)
    return src[fn_start:fn_end]


# ── Helper now sets disabled, not just label ──────────────────


def test_helper_sets_disabled_state():
    """The helper must set btn.disabled (true OR false) — not
    just btn.textContent. Pre-fix the disabled state was left
    to refreshTopbarStatus's main block, which lagged the
    chip toggle by a poll interval."""
    body = _helper_body()
    assert "btn.disabled = true" in body
    assert "btn.disabled = false" in body


def test_helper_uses_full_lib_refresh_busy_predicate():
    """The helper must compute the same predicate as the
    main block: (myTabBusy from running OR pending) || pipeline
    || grace. Otherwise toggling chips would set a different
    state than the next poll's main block."""
    body = _helper_body()
    # myTabBusy from BOTH active and pending (v1.14.73 contract).
    assert "enumActive[tabKey] && enumActive[tabKey][variantKey]" in body
    assert "enumPending[tabKey] && enumPending[tabKey][variantKey]" in body
    # globalEnumPipeline (stashed as window.__motif_global_enum_pipeline).
    assert "window.__motif_global_enum_pipeline" in body
    # Grace window.
    assert "window.__motif_lib_click_grace" in body


def test_helper_reads_window_stashed_state():
    """The helper must read enumActive + enumPending +
    globalEnumPipeline from window globals (not closure
    variables, which aren't in scope at chip-toggle time)."""
    body = _helper_body()
    assert "window.__motif_enum_active" in body
    assert "window.__motif_enum_pending" in body
    assert "window.__motif_global_enum_pipeline" in body


def test_helper_busy_branch_sets_refreshing_label():
    """When stillBusy=true, the helper must set the
    "// REFRESHING…" busy label (matching what the main
    block sets) so the chip toggle and the next poll's main
    block agree on the label."""
    body = _helper_body()
    # Pattern: the if(stillBusy) branch sets disabled+REFRESHING.
    pattern = re.compile(
        r"if\s*\(\s*stillBusy\s*\)\s*\{\s*"
        r"btn\.disabled\s*=\s*true;\s*"
        r"btn\.textContent\s*=\s*'//\s*REFRESHING…';"
    )
    assert pattern.search(body), (
        "Helper's busy branch must set both disabled=true AND "
        "the // REFRESHING… textContent in lockstep with the "
        "main libRefreshBtn block."
    )


def test_helper_idle_branch_uses_libraryRefreshLabel():
    """When not-busy AND not (tightenOnly && already-disabled), the helper
    sets the scope-aware label via libraryRefreshLabel() + unlocks. v0.51.117:
    the idle branch is now gated so a section switch never optimistically
    unlocks a disabled button from a stale stash."""
    body = _helper_body()
    pattern = re.compile(
        r"\}\s*else\s+if\s*\(!\(tightenOnly && btn\.disabled && !stashFresh\)\)\s*\{"
        r"[\s\S]*?"
        r"btn\.disabled\s*=\s*false;\s*"
        r"btn\.textContent\s*=\s*`//\s*REFRESH\s*\$\{libraryRefreshLabel\(\)\}`"
    )
    assert pattern.search(body), (
        "Helper's idle branch must set disabled=false AND the "
        "scope-aware label via libraryRefreshLabel(), gated by the "
        "v0.51.117 tightenOnly guard."
    )


# ── Stashes are written by refreshTopbarStatus ────────────────


def test_enum_pending_stashed_on_window():
    """refreshTopbarStatus must stash enumPending on window so
    the helper can read it. Pin the assignment site."""
    js = JS.read_text()
    assert "window.__motif_enum_pending = enumPending;" in js


def test_global_enum_pipeline_stashed_on_window():
    """refreshTopbarStatus must stash globalEnumPipeline on
    window — same reason as enumPending. Stashed AFTER the
    variable is computed (line ordering enforced via the
    actual JS execution order, not pinned here)."""
    js = JS.read_text()
    assert "window.__motif_global_enum_pipeline = globalEnumPipeline;" in js


def test_global_enum_pipeline_stash_after_definition():
    """Order matters: the stash must come AFTER the const
    globalEnumPipeline declaration. Otherwise the assignment
    references undefined."""
    js = JS.read_text()
    decl = js.index("const globalEnumPipeline = ")
    stash = js.index("window.__motif_global_enum_pipeline = globalEnumPipeline;")
    assert decl < stash, (
        "globalEnumPipeline stash must follow its const "
        "declaration; otherwise it references undefined at "
        "runtime."
    )


# ── Regression guards: still solves v1.14.67/68/73 ────────────


def test_helper_still_called_from_fourk_toggle_handler():
    """v1.14.68 wired updateLibraryRefreshBtnLabel into the
    //4K toggle handler so the label flips on the same paint
    as chip-active. v1.14.79 extends what the helper does;
    the call site stays."""
    js = JS.read_text()
    handler_anchor = js.index("// 4K toggle")
    handler = js[handler_anchor:handler_anchor + 2000]
    assert "updateLibraryRefreshBtnLabel()" in handler


def test_helper_still_called_pre_hash_skip_in_refresh_topbar():
    """v1.14.67 wired the helper into refreshTopbarStatus
    before the hash-skip return. v1.14.79 doesn't move the
    call site."""
    js = JS.read_text()
    fn_start = js.index("async function refreshTopbarStatus()")
    body = js[fn_start:fn_start + 6500]  # v0.51.120 widened (pre-bail stash write)
    helper_idx = body.index("updateLibraryRefreshBtnLabel()")
    skip_idx = body.index(
        "if (refreshTopbarStatus._lastHash === newHash) return;"
    )
    assert helper_idx < skip_idx
