"""v1.15.46 — bulk // ADOPT + LET PLEX SERVE button.

the user (screenshot of 14 M-source + red TDB rows selected with
just // ADOPT SELECTED and // LET PLEX SERVE in the bulk bar):
"for this instance it should be an adopt let plex server since
they are manual and red pill tdb similar to what we see in the
src drop down."

The per-row SOURCE menu has offered ADOPT + LET PLEX SERVE since
v1.14.47 for M+P composite rows (sidecar at the Plex folder that
motif didn't place + Plex serves its own independent theme). The
bulk bar had ADOPT SELECTED and LET PLEX SERVE as separate
buttons — the user had to click both in sequence (or run the
combo per row via the SOURCE menu). This tag surfaces the combo
as a single bulk button.

Use case the user flagged: rows with M source + red TDB pill (URL
dead). Can't re-download from TDB (URL broken); best path is
claim ownership of the sidecar, then drop it from the Plex
folder so Plex's own theme takes over.

## Implementation

* templates/library.html — new `#library-adopt-and-lps-btn`
  button (btn-plex tone, matches the existing LET PLEX SERVE
  button family). Display:none default; shown by updateBulkBar.

* static/app.js — three additions:
  1. updateBulkBar: count `adoptLpsCount` per the per-row gate
     (plex_local_theme=1 + !placed + plex_independent_theme=1 +
     rating_key). Mirrors line ~6619.
  2. Visibility: button shows when adoptLpsCount > 0. No
     filter-axis restriction (action is per-row unambiguous).
     Label scales with count.
  3. Click handler: iterate matching rows, chain adopt then
     unplace per row (fail-fast on adopt — no point unplacing
     if motif doesn't own the file yet). Partial-state rows
     (adopt OK, unplace failed) tracked separately so the
     operator knows to run LET PLEX SERVE on those manually
     (same v1.14.53 hazard the per-row variant handles).

Static-text guards (consistent with v1.14.27 bulk-action test
patterns).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
LIBRARY_HTML = REPO / "app" / "web" / "templates" / "library.html"
APP_JS = REPO / "app" / "web" / "static" / "app.js"


# ── 1. Template wires the button ─────────────────────────────


def test_bulk_adopt_and_lps_button_present_in_template():
    """The new bulk button must exist in library.html with the
    canonical id + btn-plex tone (matches LET PLEX SERVE family)
    + display:none default (shown by updateBulkBar)."""
    html = LIBRARY_HTML.read_text()
    assert 'id="library-adopt-and-lps-btn"' in html, (
        "v1.15.46: missing bulk // ADOPT + LET PLEX SERVE button"
    )
    # Locate the button + check its class + initial display.
    anchor = html.index('id="library-adopt-and-lps-btn"')
    btn_open = html.rfind('<button', 0, anchor)
    btn_close = html.index('>', anchor)
    btn_tag = html[btn_open:btn_close + 1]
    assert 'btn-plex' in btn_tag, (
        "v1.15.46: bulk button must use btn-plex tone (same family "
        "as LET PLEX SERVE — visual consistency in the bar)"
    )
    assert 'style="display:none"' in btn_tag, (
        "v1.15.46: button must default to display:none — visibility "
        "is driven by updateBulkBar based on selection shape"
    )
    # Confirm the button is positioned NEAR // LET PLEX SERVE
    # (within ~2KB) so the operator reads them as a related pair.
    lps_anchor = html.index('id="library-let-plex-serve-btn"')
    assert abs(anchor - lps_anchor) < 2000, (
        "v1.15.46: bulk ADOPT + LET PLEX SERVE button must be near "
        "LET PLEX SERVE so the operator sees them as a related pair"
    )


def test_bulk_adopt_and_lps_button_label_is_canonical():
    """The button label must match the per-row SOURCE-menu label
    (// ADOPT + LET PLEX SERVE). Drift between the two surfaces
    confuses the operator into thinking they do different things."""
    html = LIBRARY_HTML.read_text()
    anchor = html.index('id="library-adopt-and-lps-btn"')
    # Walk to the button's text content (between > and </button>).
    text_start = html.index('>', anchor) + 1
    text_end = html.index('</button>', text_start)
    label = html[text_start:text_end].strip()
    assert label == "// ADOPT + LET PLEX SERVE", (
        f"v1.15.46: bulk button label must match per-row SOURCE-menu "
        f"label exactly. Got {label!r}, expected '// ADOPT + LET PLEX SERVE'"
    )


# ── 2. JS visibility gate ────────────────────────────────────


def test_update_bulk_bar_counts_adopt_lps_candidates():
    """updateBulkBar must walk the selected items and count those
    matching the per-row ADOPT + LET PLEX SERVE gate (mirrors line
    ~6619): plex_local_theme=1 + !placed + plex_independent_theme=1
    + rating_key. Without the counter the button visibility
    decision has nothing to gate on."""
    js = APP_JS.read_text()
    assert "let adoptLpsCount = 0;" in js, (
        "v1.15.46: updateBulkBar must declare adoptLpsCount alongside "
        "the existing per-shape counters (pushableCount, etc)"
    )
    # The counter increment block must check all four gate clauses.
    # Anchor on the v1.15.46 marker comment.
    marker = "v1.15.46: M+P composite gate"
    anchor = js.index(marker)
    # v0.51.228: anchor-bounded, not a fixed 800-char window — the v0.51.228 count/handler
    # parity comment pushed `adoptLpsCount++` past the old edge, failing on a change that
    # PRESERVED the invariant exactly (the v0.51.222 slice-rot class). Bound on the next
    # bucket instead so the window grows with the code.
    block = js[anchor:js.index("adoptOnlyCount++", anchor)]
    for clause in [
        "it.plex_local_theme === 1",
        "!placed",
        "it.plex_independent_theme === 1",
        "it.rating_key",
        "adoptLpsCount++",
    ]:
        assert clause in block, (
            f"v1.15.46: M+P composite counter missing gate clause "
            f"{clause!r} — drift from per-row gate at line ~6619 "
            f"means the bulk button shows on wrong rows"
        )


def test_adopt_lps_button_visibility_wired():
    """The button's display must toggle off when adoptLpsCount == 0
    and on when > 0. Label must scale with count (single vs N)
    so the operator knows the actionable scope at-a-glance."""
    js = APP_JS.read_text()
    anchor = js.index("library-adopt-and-lps-btn")
    block = js[anchor:anchor + 800]
    assert "adoptLpsCount > 0" in block, (
        "v1.15.46: visibility must gate on adoptLpsCount > 0"
    )
    assert "// ADOPT + LET PLEX SERVE" in block, (
        "v1.15.46: visibility block must set the canonical label"
    )
    # v1.15.59: switched from inline `${adoptLpsCount}` template
    # literal to the withCount() helper for convention parity
    # with the other bulk buttons. Either form satisfies the
    # original "count surfaces" intent.
    assert "withCount('// ADOPT + LET PLEX SERVE', adoptLpsCount)" in block, (
        "v1.15.46: multi-row label must include the count so the "
        "operator sees how many rows the action will affect "
        "(post-v1.15.59 form: withCount() helper)"
    )


# ── 3. JS click handler ──────────────────────────────────────


def test_adopt_lps_click_handler_chains_adopt_then_unplace():
    """The click handler must POST adopt-sidecar FIRST, then
    unplace, per row. Fail-fast on adopt (skip the unplace if
    adopt fails — no point unplacing if motif doesn't own the
    file yet). Mirrors the per-row v1.14.47 flow."""
    js = APP_JS.read_text()
    # Find the handler — anchored on the addEventListener call.
    anchor = js.index("library-adopt-and-lps-btn')?.addEventListener")
    body = js[anchor:anchor + 5000]
    # adopt-sidecar POST must precede unplace POST in the body.
    adopt_idx = body.index("/adopt-sidecar")
    unplace_idx = body.index("/unplace")
    assert adopt_idx < unplace_idx, (
        "v1.15.46: handler must call adopt-sidecar BEFORE unplace "
        "— reverse order = unplace a file motif doesn't own yet"
    )
    # Continue-on-adopt-failure pattern (mirrors v1.14.53 fix).
    assert "adoptFail++" in body
    assert "continue" in body, (
        "v1.15.46: adopt-fail path must `continue` so a single bad "
        "row doesn't strand the rest of the selection"
    )


def test_adopt_lps_handler_tracks_partial_state():
    """If adopt succeeds but unplace fails, the row landed in
    adopt-succeeded but file-still-at-Plex state (the v1.14.53
    hazard — surface to the operator so they know to run LET PLEX
    SERVE on that specific row manually). Must show under a
    PARTIAL bucket in the result label, distinct from full success
    and from adopt-fail."""
    js = APP_JS.read_text()
    anchor = js.index("library-adopt-and-lps-btn')?.addEventListener")
    body = js[anchor:anchor + 5000]
    assert "unplaceFail++" in body, (
        "v1.15.46: post-adopt unplace failure must increment a "
        "separate counter (not silently merged with adopt-fail)"
    )
    assert "PARTIAL" in body, (
        "v1.15.46: result label must surface the PARTIAL bucket — "
        "the operator needs to know which rows landed mid-state "
        "(adopt OK, unplace failed) to recover them via LET PLEX "
        "SERVE on the row"
    )


def test_adopt_lps_handler_confirms_before_running():
    """Bulk action is destructive (deletes files at Plex folders).
    Must confirm() before proceeding — same UX contract as // ADOPT
    SELECTED, // LET PLEX SERVE, etc."""
    js = APP_JS.read_text()
    anchor = js.index("library-adopt-and-lps-btn')?.addEventListener")
    body = js[anchor:anchor + 5000]
    assert "confirm(" in body, (
        "v1.15.46: must show confirm() dialog before running — "
        "deleting placement files at Plex folders is destructive"
    )
    assert "ADOPT + LET PLEX SERVE on " in body, (
        "v1.15.46: confirm dialog must name the action + count so "
        "the operator knows exactly what they're about to do"
    )


def test_adopt_lps_handler_refreshes_topbar_past_stats_ttl():
    """Bulk ADOPT clears failures (FAIL pill should drop). Must
    refresh past the /api/stats 1s TTL or the topbar lags the
    actual state. Pattern from v1.13.56 (the // ADOPT SELECTED
    handler does the same)."""
    js = APP_JS.read_text()
    anchor = js.index("library-adopt-and-lps-btn')?.addEventListener")
    body = js[anchor:anchor + 5000]
    assert "refreshTopbarStatus" in body, (
        "v1.15.46: must refresh topbar after the run"
    )
    assert "1100" in body, (
        "v1.15.46: topbar refresh must be ≥1100ms (past the 1s "
        "/api/stats TTL — see CLAUDE.md § recurring bug class #7)"
    )
