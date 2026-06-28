"""v1.15.2 — status-surface consistency cleanup.

the user: "Can we do a quick check of all the status bar related
events, progress bars, queued queues, pending queues, status
etc to make sure everything is sound logic and constant across
the board. All things in the status bar should have the same
look and be consistent across the board"

A 3-finding cleanup from the topbar/drawer audit:

## A. Synth pending rows mark synthetic=true (was: dead CANCEL button)

`plex_enum_pending` + `tdb_sync_pending` synth rows rendered a
CANCEL button on the drawer card that did nothing — they have
no real op_progress row to cancel against. The card render's
cancel-button gate (`isLive && !synthetic`) was supposed to
suppress the button on synthetic rows + render a "// per-job
cancel via /queue" note instead. Pre-fix neither synth row set
`detail.synthetic=true`, so the dead CANCEL button rendered on
every queued-state card.

## B. Unified queued-state label format

Three different "queued behind something" formats across synth
rows:
  - `plex_enum_pending`  → "3 library refreshes queued"
  - `tdb_sync_pending`   → "Waiting for X to finish"
  - download_queue etc.  → "Downloading themes" (running, not queued)

Picked `<work> queued behind <blocker>` for both pending synth
rows. Queue-ops that describe ACTIVE work (downloads, places)
keep their gerund-action labels — that's a different state
(running, not queued).

## C. Extracted shared bar-variant helper

Three call sites (`renderCard`, `_structuralHash`,
`renderMiniBar`) each computed their own version of
`useRealBar` / `indeterminate`. Logic was equivalent but
written differently in each, making consistency non-obvious +
brittle. Extracted `_useRealBar(op)` + `_isIndeterminate(op)`
helpers with the canonical rule: real bar iff
`detail.bar_pct present OR stage_total > 1`. All three sites
now call the helpers; future changes land in lockstep.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
PROGRESS_PY = REPO / "app" / "core" / "progress.py"
OPS_JS = REPO / "app" / "web" / "static" / "ops.js"


# ── A. Synth pending rows are marked synthetic ─────────────────


def test_plex_enum_pending_sets_synthetic_true():
    """The plex_enum_pending synth row must include
    `detail.synthetic = True` so the drawer's cancel-button
    gate suppresses the dead CANCEL button."""
    src = PROGRESS_PY.read_text()
    # Anchor on the synth row's detail dict.
    anchor = src.index("queue:plex_enum_pending")
    block = src[anchor:anchor + 2500]
    assert '"queue_depth": n' in block
    assert '"synthetic": True' in block, (
        "plex_enum_pending detail must include synthetic=True"
    )


def test_tdb_sync_pending_sets_synthetic_true():
    """Same fix for tdb_sync_pending."""
    src = PROGRESS_PY.read_text()
    anchor = src.index("queue:tdb_sync_pending")
    block = src[anchor:anchor + 2500]
    assert '"synthetic": True' in block, (
        "tdb_sync_pending detail must include synthetic=True"
    )


def test_synthetic_gate_in_card_render_unchanged():
    """The drawer card's cancel-button gate must still check
    `op.detail.synthetic` to decide between the button and the
    'cancel via /queue' note. Pin so a future render refactor
    doesn't drop the gate (which would re-introduce the dead
    CANCEL button on synth rows)."""
    src = OPS_JS.read_text()
    # The gate appears in renderCard's cancel-button block.
    assert "op.detail.synthetic" in src or "detail && op.detail.synthetic" in src


# ── B. Queued-state label format unified ───────────────────────


def test_plex_enum_pending_label_uses_queued_behind_format():
    """plex_enum_pending stage_label must use the unified
    `<count> library refresh(es) queued behind <blocker>`
    format."""
    src = PROGRESS_PY.read_text()
    anchor = src.index("queue:plex_enum_pending")
    # Walk backward to find the stage_label assignment block.
    block = src[max(0, anchor - 2500):anchor + 200]
    assert "queued behind" in block, (
        "plex_enum_pending label must use the 'queued behind' format"
    )
    # The blocker label must distinguish plex-vs-other for
    # diagnostic clarity.
    assert "another Plex refresh" in block
    assert "the running sync / scan" in block


def test_tdb_sync_pending_label_uses_queued_behind_format():
    """tdb_sync_pending stage_label must use the unified
    `ThemerrDB sync queued behind <blocker>` format."""
    src = PROGRESS_PY.read_text()
    anchor = src.index("queue:tdb_sync_pending")
    block = src[max(0, anchor - 2500):anchor + 200]
    assert "ThemerrDB sync queued behind" in block, (
        "tdb_sync_pending label must use the 'queued behind' format"
    )


def test_pre_fix_label_shapes_are_gone():
    """Regression guard: the pre-fix runtime label shapes must
    be completely removed.
      - "Waiting for X to finish" was tdb_sync_pending's old
        passive-explanatory shape
      - The new format always includes "queued behind" so a
        bare "queued" without "behind" in the runtime
        stage_label assignment indicates a regression

    Whitespace-tolerant by collapsing source-formatting
    indentation; only checks runtime f-strings, not docstring
    comments (which legitimately reference historical labels)."""
    src = PROGRESS_PY.read_text()
    # Collapse whitespace so multi-line f-strings are checkable.
    flat = " ".join(src.split())
    # Find the runtime f-string that produces the actual
    # stage_label assignment for plex_enum_pending. Pre-fix:
    #   stage = f"{n} library refresh{plural} queued"
    # Post-fix:
    #   stage = f"{n} library refresh{plural} queued behind {blocker}"
    # The pre-fix shape would have `queued"` immediately after
    # `{plural}` with no `behind`.
    assert 'library refresh{plural} queued"' not in flat, (
        "Pre-fix bare-queued plex_enum_pending stage_label must be "
        "replaced with the queued-behind format"
    )
    # The new shape's f-string must be present.
    assert 'queued behind {blocker}' in flat
    # Same regression guard for tdb_sync_pending — the runtime
    # f-string must NOT include the pre-fix "Waiting for" phrase.
    assert 'f"Waiting for {' not in flat, (
        "Pre-fix 'Waiting for X to finish' tdb_sync_pending "
        "stage_label must be replaced"
    )


# ── C. Shared bar-variant helpers ──────────────────────────────


def test_use_real_bar_helper_defined():
    """The shared `_useRealBar(op)` helper must be defined so
    the three call sites can converge on a single rule."""
    src = OPS_JS.read_text()
    assert "function _useRealBar(op)" in src
    # The canonical rule: bar_pct present OR stage_total > 1.
    anchor = src.index("function _useRealBar(op)")
    block = src[anchor:anchor + 800]
    assert "op.detail.bar_pct" in block
    assert "op.stage_total" in block


def test_is_indeterminate_helper_is_inverse():
    """The `_isIndeterminate(op)` helper must be the literal
    inverse of `_useRealBar(op)` — same call sites can pick
    whichever framing reads better."""
    src = OPS_JS.read_text()
    assert "function _isIndeterminate(op)" in src
    anchor = src.index("function _isIndeterminate(op)")
    block = src[anchor:anchor + 200]
    assert "return !_useRealBar(op)" in block, (
        "_isIndeterminate must be defined as !_useRealBar — "
        "any other definition risks drift"
    )


def test_render_card_uses_helper():
    """renderCard's bar-variant decision must call _useRealBar
    instead of inlining the rule."""
    src = OPS_JS.read_text()
    fn_start = src.index("function renderCard(op)")
    fn_end = src.index("function ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    assert "_useRealBar(op)" in fn_body, (
        "renderCard must call _useRealBar instead of inlining the rule"
    )


def test_structural_hash_uses_helper():
    """_structuralHash's `useRealBar` must call the helper too —
    consistency with renderCard means the structural hash and
    the actual rendered bar always agree on the variant."""
    src = OPS_JS.read_text()
    fn_start = src.index("function _structuralHash(op)")
    fn_end = src.index("function ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    assert "_useRealBar(op)" in fn_body


def test_mini_bar_uses_helper():
    """The mini-bar's `indeterminate` decision must call
    _isIndeterminate. Pre-fix it inlined the inverted rule
    (`!hasRealPct && stage_total <= 1`); equivalent but
    non-obvious to readers verifying mini-bar matches card-bar.

    Anchor on the v1.15.2 marker comment. The first
    `mini.className =` in the file is in the optimistic-
    placeholder branch (hardcoded indeterminate); the real
    mini-bar render is further down."""
    src = OPS_JS.read_text()
    anchor = src.index("v1.15.2: shared helper, see _isIndeterminate")
    block = src[anchor:anchor + 800]
    assert "_isIndeterminate(op)" in block, (
        "Mini-bar must call _isIndeterminate instead of inlining "
        "the inverted rule"
    )


def test_no_residual_inlined_has_real_pct_in_render_paths():
    """Regression guard: the three render-path call sites must
    no longer compute `hasRealPct` locally + inline the rule.
    Pre-fix this was the source of the drift; the helper is
    the only path forward."""
    src = OPS_JS.read_text()
    # Slice to just the renderCard / _structuralHash / renderTopbar
    # functions (between the helper definitions and the end of the
    # render-path code).
    rc_start = src.index("function renderCard(op)")
    # Slice to the end of renderTopbar (just before the post-render
    # functions). Use the next "function tickCounters" anchor.
    tick_start = src.index("function tickCounters(")
    render_block = src[rc_start:tick_start]
    # The pre-fix shape inlined `hasRealPct` as a local const inside
    # the body of each render fn. Helper extraction means it lives
    # ONLY in _useRealBar's body.
    inlined_count = render_block.count("const hasRealPct =")
    assert inlined_count == 0, (
        f"Found {inlined_count} inlined `const hasRealPct =` in "
        f"render paths — should be zero (only the helper computes it)"
    )
