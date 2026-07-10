"""v0.51.117 — the library REFRESH button must not flash clickable on a fast
section switch while a refresh is running (the user).

switchLibraryTab (the v1.23.71 client-side section switch) re-applies the button
lock in two places with a gap: updateLibraryRefreshBtnLabel() runs SYNC on the
switch and sets the disabled state from the STASH window.__motif_global_enum_
pipeline (last written by a poll); the authoritative refreshTopbarStatus() (fresh
/api/stats) only runs AFTER await loadLibrary(). A full refresh's startup /
poll-lag window reads not-busy from the stash, so the switch UNLOCKED the button
for the loadLibrary duration — visible as a clickable button mid-refresh when
switching quickly.

Fix: the switch calls updateLibraryRefreshBtnLabel(tightenOnly=true) — it may
LOCK but never optimistically UNLOCK a currently-disabled button; the unlock is
left to refreshTopbarStatus. This harness runs the REAL function against a mock
button + stashes and pins the mechanism + the fix (bug repro on the default
mode, fix on tightenOnly).
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def _fn_src() -> str:
    start = JS.index("  function updateLibraryRefreshBtnLabel(tightenOnly) {")
    end = JS.index("\n  function ", start + 1)
    return JS[start:end]


def _run_btn(*, tighten: bool, disabled_init: bool,
             pipeline: bool = False, my_tab_busy: bool = False) -> dict:
    """Drive the real updateLibraryRefreshBtnLabel once and return the mock
    button's resulting {disabled, text}. pipeline = the global-pipeline STASH;
    my_tab_busy = this tab's per-tab enum stash."""
    quickjs = pytest.importorskip("quickjs")
    enum_active = {"tv": {"standard": True}} if my_tab_busy else {}
    # model the realistic pre-switch label: a disabled button is already showing
    # '// REFRESHING…'; an enabled one shows its section label.
    text_init = "// REFRESHING…" if disabled_init else "// REFRESH TV SHOWS"
    preamble = (
        f"var _btn = {{ disabled: {json.dumps(disabled_init)}, "
        f"textContent: {json.dumps(text_init)} }};\n"
        "var document = { getElementById: function(id){ "
        "return id === 'library-refresh-btn' ? _btn : null; } };\n"
        "var libraryState = { tab: 'tv', fourk: false };\n"
        "var window = {\n"
        f"  __motif_enum_active: {json.dumps(enum_active)},\n"
        "  __motif_enum_pending: {},\n"
        f"  __motif_global_enum_pipeline: {json.dumps(pipeline)},\n"
        "  __motif_lib_click_grace: {},\n"
        "  __motif_plex_enum_collections_busy: false,\n"
        "};\n"
        "function libraryRefreshLabel() { return 'TV SHOWS'; }\n"
        + _fn_src()
    )
    body = (
        f"updateLibraryRefreshBtnLabel({json.dumps(tighten)});\n"
        "JSON.stringify({ disabled: _btn.disabled, text: _btn.textContent });"
    )
    ctx = quickjs.Context()
    return json.loads(ctx.eval(preamble + "\n" + body))


# ── the bug + the fix (stale stash during a running refresh) ──


def test_default_mode_unlocks_from_stale_stash_this_is_the_bug():
    # refresh IS running but the stash reads not-busy (startup / poll-lag window);
    # the button was disabled (showing REFRESHING). The DEFAULT (non-tighten) mode
    # UNLOCKS it — the flash the user saw on a fast section switch.
    out = _run_btn(tighten=False, disabled_init=True, pipeline=False)
    assert out["disabled"] is False  # <-- clickable mid-refresh (the bug)


def test_tighten_only_keeps_the_lock_from_stale_stash_this_is_the_fix():
    # SAME stale-stash setup, but the section switch now passes tightenOnly=true:
    # a currently-disabled button is never optimistically unlocked here — it stays
    # locked until refreshTopbarStatus (fresh /api/stats) confirms.
    out = _run_btn(tighten=True, disabled_init=True, pipeline=False)
    assert out["disabled"] is True
    assert out["text"] == "// REFRESHING…"


# ── the fix must still LOCK, and must not over-lock an idle button ──


def test_tighten_only_still_locks_when_the_stash_is_busy():
    # a fresh stash that DOES see the pipeline still locks the button on switch.
    out = _run_btn(tighten=True, disabled_init=False, pipeline=True)
    assert out["disabled"] is True
    assert out["text"] == "// REFRESHING…"


def test_tighten_only_does_not_over_lock_an_already_idle_button():
    # button was enabled + nothing busy → the switch relabels + leaves it enabled
    # (no spurious REFRESHING on an idle button; only a DISABLED one is held).
    out = _run_btn(tighten=True, disabled_init=False, pipeline=False)
    assert out["disabled"] is False
    assert out["text"] == "// REFRESH TV SHOWS"


def test_variant_toggle_default_mode_keeps_instant_unlock():
    # the STANDARD/4K toggle callers stay in default mode (v1.14.79 instant
    # unlock): a disabled button on an idle stash unlocks immediately.
    out = _run_btn(tighten=False, disabled_init=True, pipeline=False)
    assert out["disabled"] is False
    assert out["text"] == "// REFRESH TV SHOWS"


# ── source guard: only the section switch passes tightenOnly ──


def test_only_the_section_switch_caller_tightens():
    # the switchLibraryTab caller (right after hydrateLibraryStateForTab) passes
    # true; the variant / status-chip callers stay bare.
    i = JS.index("hydrateLibraryStateForTab(tab, url.searchParams);")
    after = JS[i:i + 200]
    assert "updateLibraryRefreshBtnLabel(true)" in after
    # the toggle callers do NOT tighten (bare call).
    assert JS.count("updateLibraryRefreshBtnLabel();") >= 2
