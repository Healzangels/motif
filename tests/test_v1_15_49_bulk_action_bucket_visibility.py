"""v1.15.49 — bucket the M / M+P / +P-only bulk actions + count badges.

the user (screenshot: M+P-filtered Movies page with ADOPT SELECTED,
LET PLEX SERVE, and ADOPT + LET PLEX SERVE all shown):
"I like the 50 on the adopt let plex server but its not consistant
across the board, i also like the running status on it. could we
make this consistant across all bulk actions. also when these
filters are set let plex server and adopt and let plex serve
becomes redundant and when its and M it should be the adopt
option."

Two related issues, both visible on the M+P-filtered Movies view:

## Issue A — count badges inconsistent

v1.15.46 added `(N)` count badge to // ADOPT + LET PLEX SERVE
(50). Other bulk buttons in the same bar didn't get the same
treatment, so the bar reads inconsistently. This tag extends
the (N) convention to // ADOPT SELECTED and // LET PLEX SERVE
(the two buttons most directly related to the new combo).

## Issue B — redundant + unsafe buttons on M+P selections

Pre-v1.15.49 the three buttons all showed on M+P composite
selections:
* // ADOPT SELECTED — would adopt but leave file at Plex folder
* // LET PLEX SERVE — would delete file with NO recovery path
  (motif doesn't own the M sidecar; no canonical at /themes/)
* // ADOPT + LET PLEX SERVE — the safe combo

LPS alone on an M sidecar is a footgun. The fix buckets rows
into three mutually-exclusive subsets so each button only shows
for its proper subset:

  adoptOnlyCount = M sidecar WITHOUT Plex independent → ADOPT
  adoptLpsCount  = M+P composite → ADOPT + LET PLEX SERVE
  lpsOnlyCount   = +P composite NOT M sidecar (T/A/U+P) → LPS

Each row falls into exactly one bucket. The LPS handler also
filters out M sidecars from its candidates (belt + suspenders —
visibility guards but the handler should be safe even if a stale
button click slips through).

Static-text guards (consistent with v1.15.46 bulk-action test
patterns).
"""
from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"


# ── 1. Bucket counters exist + populated correctly ──────────


def test_update_bulk_bar_declares_three_buckets():
    """All three bucket counters must be declared in
    updateBulkBar so visibility decisions have ground truth.
    Pre-v1.15.49 only adoptLpsCount existed (v1.15.46);
    adoptOnlyCount + lpsOnlyCount close the partition."""
    js = APP_JS.read_text()
    fn_anchor = js.index("function updateLibrarySelectionUi()")
    fn_body = js[fn_anchor:fn_anchor + 12000]
    for var in ("let adoptLpsCount = 0;",
                "let adoptOnlyCount = 0;",
                "let lpsOnlyCount = 0;"):
        assert var in fn_body, (
            f"v1.15.49: updateBulkBar missing counter declaration "
            f"{var!r} — bucket partition incomplete"
        )


def test_bucket_logic_is_mutually_exclusive_per_row():
    """The bucket assignment uses if/else-if so each row
    increments at most one counter. Reading the per-row gate as
    independent ifs would double-count rows that match multiple
    predicates (M+P composites match isMSidecar AND isPlexIndep)
    and break the count math."""
    js = APP_JS.read_text()
    marker = "v1.15.49: extend to fill all three buckets exactly once"
    anchor = js.index(marker)
    block = js[anchor:anchor + 1500]
    # Must use if / else if / else if — NOT three separate ifs.
    assert "else if (isMSidecar)" in block, (
        "v1.15.49: adoptOnlyCount branch must be `else if`, not "
        "`if` — otherwise M+P composites count in BOTH "
        "adoptLpsCount and adoptOnlyCount = wrong totals"
    )
    assert "else if (isPlexIndep" in block, (
        "v1.15.49: lpsOnlyCount branch must be `else if` — same "
        "double-counting hazard"
    )


def test_lps_only_excludes_m_sources():
    """The lpsOnlyCount bucket must filter on `isPlexIndep && NOT
    isMSidecar` — otherwise M+P composites fall into the lps-only
    bucket AND the adopt-lps bucket. The if/else-if structure
    enforces this implicitly (M-sidecar branch runs first), but
    pin the structure so a refactor that flattens the conditional
    catches the regression."""
    js = APP_JS.read_text()
    marker = "v1.15.49: extend to fill all three buckets exactly once"
    anchor = js.index(marker)
    block = js[anchor:anchor + 1500]
    # The lpsOnlyCount branch must come AFTER the isMSidecar
    # check (so M-sidecar rows are already routed).
    ms_idx = block.index("isMSidecar++") if "isMSidecar++" in block else block.index("if (isMSidecar")
    lps_idx = block.index("lpsOnlyCount++")
    assert ms_idx < lps_idx, (
        "v1.15.49: isMSidecar check must precede lpsOnlyCount "
        "branch in the if/else chain — otherwise M+P composites "
        "leak into the lps-only bucket"
    )


# ── 2. Visibility wired to buckets ───────────────────────────


def test_adopt_selected_visibility_uses_adopt_only_count():
    """// ADOPT SELECTED visibility must gate on adoptOnlyCount
    (NOT the legacy hasSidecarOnly). Pre-v1.15.49 it showed on
    every M sidecar including M+P composites — overlap with
    // ADOPT + LET PLEX SERVE that the user flagged as confusing."""
    js = APP_JS.read_text()
    # Find the v1.15.49 ADOPT SELECTED visibility block.
    marker = "v1.15.49: ADOPT SELECTED visibility narrowed"
    anchor = js.index(marker)
    # v1.15.59 widened slice — withCount() call lands past the
    # original 1000-char window after the v1.15.59 refactor.
    block = js[anchor:anchor + 1500]
    assert "adoptOnlyCount > 0" in block, (
        "v1.15.49: ADOPT SELECTED must gate on adoptOnlyCount > 0"
    )
    # Label gets count badge. v1.15.59 refactored inline ternary
    # → withCount() helper for convention parity. Either form
    # satisfies the original intent (count surfaces when > 1);
    # check for the withCount() call shape now.
    assert "withCount('// ADOPT SELECTED', adoptOnlyCount)" in block, (
        "v1.15.49: ADOPT SELECTED label must include count badge "
        "(post-v1.15.59 form: withCount('// ADOPT SELECTED', "
        "adoptOnlyCount))"
    )


def test_let_plex_serve_visibility_uses_lps_only_count():
    """LET PLEX SERVE visibility must include lpsOnlyCount in the
    decision so the M+P footgun (unsafe LPS-on-M-sidecar) stays
    eliminated. Click handler's M-sidecar skip (v1.15.49) still
    enforces the no-footgun contract.

    v1.15.60 added a +P-filter safety net for the case where
    lpsOnlyCount under-counted due to visible-page-only iteration.
    v1.16.10 dropped that safety net: the selectedRows cache makes
    lpsOnlyCount selection-wide, so the page-size undercount it
    guarded against no longer occurs. the user: "let plex server
    isn't displaying a number at all anytime." The bare-label
    fallback the safety net carried is gone — every visible LPS
    button now carries a count badge."""
    js = APP_JS.read_text()
    # v1.16.10: marker header trimmed back to v1.14.27 / v1.15.49.
    marker = "v1.14.27 / v1.15.49: bulk LET PLEX SERVE"
    anchor = js.index(marker)
    block = js[anchor:anchor + 2500]
    assert "lpsOnlyCount > 0" in block, (
        "v1.15.49: LET PLEX SERVE must gate on lpsOnlyCount > 0"
    )
    # v1.16.10: safety-net fallback dropped along with its
    # bare-label-no-count branch. The button is hidden if
    # lpsOnlyCount === 0 — no more "// LET PLEX SERVE" with
    # nothing behind it. Narrow the absence check to the actual
    # visibility statements; the LPS block ends at the next
    # marker comment (probe-tdb button block).
    lps_block_end = block.index("// v1.15.60: bulk PROBE TDB SELECTED")
    lps_block = block[:lps_block_end]
    assert "onPlusPFilter" not in lps_block, (
        "v1.16.10: the v1.15.60 onPlusPFilter safety-net must be "
        "removed — the selectedRows cache makes the lpsOnlyCount "
        "undercount it guarded against impossible"
    )
    assert "letPlexServeBtn.style.display = lpsOnlyCount > 0 ? '' : 'none';" in lps_block, (
        "v1.16.10: LPS visibility must reduce to a simple "
        "lpsOnlyCount > 0 ternary (no safety-net fallback)"
    )
    assert "withCount('// LET PLEX SERVE', lpsOnlyCount)" in block, (
        "v1.15.49: LET PLEX SERVE label must include count badge "
        "via withCount() (v1.15.59 convention)"
    )


# ── 3. LPS handler defends against M sidecars ────────────────


def test_lps_handler_filters_out_m_sidecars():
    """Belt+suspenders: the LPS click handler must skip M sidecars
    even if a stale button click reaches it (e.g. button visible
    during a brief selection-shape change). Defensive — visibility
    guards but the handler shouldn't trust that contract."""
    js = APP_JS.read_text()
    handler_anchor = js.index("library-let-plex-serve-btn')?.addEventListener")
    body = js[handler_anchor:handler_anchor + 3000]
    # Must check isMSidecar and `continue` past those rows.
    assert "v1.15.49: exclude M sidecars" in body, (
        "v1.15.49: LPS handler must have an explicit M-sidecar "
        "skip block with the v1.15.49 marker comment"
    )
    # v1.22.80: the predicate now uses the WIDENED placed shape — the
    # bare !it.media_folder this pin used to assert evaluated !'' as
    # TRUE for plex_upload rows (the v1.19.38 class), so the handler
    # skipped rows the bucket counted. THIS pin was enforcing the
    # drift its own message warned about.
    assert "it.plex_local_theme === 1 && !_lpsPlaced" in body, (
        "v1.15.49/v1.22.80: LPS handler M-sidecar predicate must match "
        "the isMSidecar gate from updateBulkBar — mirror principle. "
        "Drift = button shows for one shape, handler acts on another."
    )
    assert "it.placement_kind === 'plex_upload'" in body
    # The handler must `continue` (not error) on M sidecar so
    # mixed selections don't fail entirely — they just skip the
    # unsafe rows.
    skip_idx = body.index("v1.15.49: exclude M sidecars")
    skip_block = body[skip_idx:skip_idx + 1000]
    assert "continue" in skip_block, (
        "v1.15.49: LPS handler M-sidecar branch must `continue` "
        "past the unsafe rows, not error out — mixed selections "
        "should still LPS the eligible rows"
    )


def test_lps_handler_empty_targets_message_explains_m_route():
    """When the handler runs and finds zero eligible rows (e.g.
    because the user picked only M+P composites), the alert must
    point the operator at the correct route (ADOPT + LET PLEX
    SERVE). Don't just say "nothing to do" — explain the safer
    button exists for their selection shape."""
    js = APP_JS.read_text()
    handler_anchor = js.index("library-let-plex-serve-btn')?.addEventListener")
    body = js[handler_anchor:handler_anchor + 4000]
    empty_anchor = body.index("Nothing to LET PLEX SERVE")
    msg = body[empty_anchor:empty_anchor + 1000]
    assert "ADOPT + LET PLEX SERVE" in msg, (
        "v1.15.49: zero-targets alert must reference // ADOPT + "
        "LET PLEX SERVE so the operator knows where to look for "
        "the M+P composite case"
    )


# ── 4. Count-badge label consistency across the trio ─────────


def test_three_bulk_buttons_share_count_label_convention():
    """All three buttons (// ADOPT SELECTED, // LET PLEX SERVE,
    // ADOPT + LET PLEX SERVE) must use the same `// LABEL (N)`
    convention when count > 1, and bare label when count === 1.
    the user: "make this consistant across all bulk actions" —
    this test pins the consistency at the JS source level so a
    future refactor that diverges one button trips the test.

    v1.15.59: convention now expressed via the withCount()
    helper (which encodes "bare at 1, (N) at >1"). Test
    checks for the helper call pattern instead of the inline
    ternary that v1.15.49 originally emitted."""
    js = APP_JS.read_text()
    patterns = [
        ("ADOPT SELECTED", "adoptOnlyCount", "// ADOPT SELECTED"),
        ("LET PLEX SERVE", "lpsOnlyCount", "// LET PLEX SERVE"),
        ("ADOPT + LET PLEX SERVE", "adoptLpsCount", "// ADOPT + LET PLEX SERVE"),
    ]
    failures = []
    for name, counter, label in patterns:
        call = f"withCount('{label}', {counter})"
        if call not in js:
            failures.append(
                f"{name}: missing `{call}` — helper-based count "
                "badge required for convention parity"
            )
    assert not failures, (
        "v1.15.49: count-badge convention drift — " + "; ".join(failures)
    )
