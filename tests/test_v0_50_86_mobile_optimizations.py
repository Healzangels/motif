"""v0.50.86 — batch of mobile optimizations (the user, on-device testing).

Four issues found testing motif on a phone:

1. Login: an "Invalid username or password" error pushed the auth-card's stacked
   content (title + error + 2 fields + button) past what a strict circle can hold
   at mobile width, so PASSWORD started clipping against the ring. Fix: the card's
   aspect-ratio:1/1 becomes a min-height — stays a circle when content fits, grows
   into a taller oval instead of clipping when it doesn't.

2. Dashboard SYNC HISTORY table (5 columns, several nowrap cells) had no min-width
   floor or scroll affordance on its card, so it spilled past the card's right edge
   on a phone. Fix: its own horizontal-scroll context (mirrors #library-table
   .table-scroll) + a min-width so columns keep readable widths and scroll instead
   of crushing/spilling.

3. Library filterbar: // CLEAR ALL stranded itself on a 3rd line, alone, below
   // FILTERS. Cause: margin-left:auto lived on .library-presets-menu ALONE — when
   IT wrapped to a new flex line by itself, the auto margin consumed all the
   remaining room on that line before it, leaving zero space for // CLEAR ALL
   (next in source order) to share the line. Fix: group both under one
   .library-toolgroup wrapper carrying the auto margin, so they wrap TOGETHER.

4. Dashboard SOURCE BREAKDOWN pie cards: legend text ("ThemerrDB", "Manual
   sidecar", …) forced the card wider than a mobile 2-per-row column can afford
   (a fixed 110px donut ALONE already exceeds the ~125-160px available column
   width when squeezed beside a legend), so the whole dashboard grid spilled off
   the right edge of the viewport. Fix: min-width:0 + ellipsis on the legend name
   (general correctness fix, harmless on desktop) + a mobile-only layout that
   STACKS the (shrunk) donut above the legend instead of squeezing them side by
   side, giving the legend the card's full width.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
LIBRARY = (REPO / "app" / "web" / "templates" / "library.html").read_text()


def _rule(sel: str) -> str:
    i = APP_CSS.index(sel)
    return APP_CSS[i:APP_CSS.index("}", i) + 1]


# ── 1. Login auth-card no longer clips on the error state ──────────────────

def test_auth_card_grows_instead_of_clipping():
    block = _rule(".auth-card {")
    assert "min-height: min(460px, 92vw)" in block
    assert "width: min(460px, 92vw)" in block
    # the hard aspect-ratio is gone — it forced every state (incl. the taller
    # error state) into the SAME fixed diameter, clipping content that overflowed
    # the circle's safe chord.
    assert "aspect-ratio" not in block
    # still round when content fits (unchanged, no regression to the v0.50.18 shape).
    assert "border-radius: 50%" in block


# ── 2. SYNC HISTORY table scrolls instead of spilling ───────────────────────

def test_sync_history_gets_a_scroll_context_on_mobile():
    i = APP_CSS.index("@media (max-width: 600px)")
    block = APP_CSS[i:APP_CSS.index("\n}\n", i)]
    assert ".sync-history-bars { overflow-x: auto; -webkit-overflow-scrolling: touch; }" in block
    assert ".sync-hist-table { min-width: 480px; }" in block


# ── 3. Filterbar // CLEAR ALL wraps together with the presets star ─────────

def test_presets_and_clear_all_share_one_wrapper():
    assert '<div class="library-toolgroup">' in LIBRARY
    # both controls live inside it, in the original relative order.
    start = LIBRARY.index('<div class="library-toolgroup">')
    end = LIBRARY.index("</div>", LIBRARY.index("library-clear-all-btn", start))
    group = LIBRARY[start:end]
    assert 'id="library-presets-menu"' in group
    assert 'id="library-clear-all"' in group
    assert group.index("library-presets-menu") < group.index("library-clear-all")


def test_toolgroup_carries_the_auto_margin_not_the_presets_menu_alone():
    toolgroup = _rule(".library-toolgroup {")
    assert "margin-left: auto" in toolgroup
    assert "display: flex" in toolgroup
    presets = _rule(".library-presets-menu {")
    assert "margin-left: auto" not in presets


# ── 4. SOURCE BREAKDOWN pie cards no longer overflow on mobile ─────────────

def test_legend_name_truncates_instead_of_forcing_overflow():
    block = _rule(".source-legend-name {")
    assert "min-width: 0" in block
    assert "overflow: hidden" in block
    assert "text-overflow: ellipsis" in block
    assert "white-space: nowrap" in block


def test_legend_container_and_item_allow_shrink():
    assert "min-width: 0" in _rule(".source-breakdown-legend {")
    assert "min-width: 0" in _rule(".source-legend-item {")


def test_pie_col_stacks_chart_above_legend_on_mobile():
    # a SECOND @media(max-width:760px) block (after the base .source-pie-col rule)
    # restacks the card to full-width rows instead of squeezing a fixed-width donut
    # beside the legend.
    idx = APP_CSS.rindex("@media (max-width: 760px) {")
    block = APP_CSS[idx:APP_CSS.index("\n}\n", idx) + 3]
    assert '"label"\n      "chart"\n      "legend";' in block
    assert "grid-template-columns: 1fr;" in block
    assert "width: 90px;" in block
    assert "max-width: 90px;" in block


def test_base_pie_col_still_side_by_side_on_desktop():
    # the DESKTOP rule (matched first by index()) is untouched — only a mobile
    # override was added, not a redefinition of the base layout.
    col = _rule(".source-pie-col {")
    assert '"chart legend"' in col
