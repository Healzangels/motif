"""v0.51.3 — mobile topbar rework: the nav gets its own full-width second row.

Mobile audit finding A1 (P0): at 375px the brand | nav | status single-row grid
crushed the 7-tab nav into a ~45px horizontal-scroll sliver between the brand and
the status cluster — only "DAS…" of DASHBOARD showed, the other 6 tabs off-screen
with NO scroll affordance (measured navWidth=45px, 0 tabs visible). Navigation was
effectively broken on a phone.

Fix: on a phone the topbar becomes a 2-row grid-template-areas layout — brand +
status share row 1 (they fit ~330px < 375px once the nav leaves), the nav spans
the full width on row 2 as a horizontal-scroll strip (desktop tab order preserved,
now ~351px wide with a thin scroll track as the "more →" affordance and ~44px tap
targets). The op-mini "job running" strip still pins to the topbar bottom via the
v0.50.91 :has() reservation, below the nav row (no overlap).
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def _mobile_block(css: str) -> str:
    i = css.index("@media (max-width: 600px) {")
    depth = 0
    j = i
    while True:
        if css[j] == "{":
            depth += 1
        elif css[j] == "}":
            depth -= 1
            if depth == 0:
                return css[i:j + 1]
        j += 1


MOBILE = _mobile_block(APP_CSS)


def test_topbar_is_two_row_areas_grid_on_mobile():
    assert 'grid-template-areas: "brand status" "nav nav";' in MOBILE, \
        "nav must span its own full-width second row"
    # 2 columns now (brand auto-with-shrink-floor, status 1fr), not the old 3.
    assert "grid-template-columns: minmax(0, auto) 1fr;" in MOBILE


def test_the_crushed_middle_column_grid_is_gone():
    # the v0.50.88 floored middle column (a 24px-min nav sliver) is what crushed
    # the tabs; it must not survive anywhere in the mobile block.
    assert "grid-template-columns: auto minmax(24px, 1fr) auto;" not in MOBILE


def test_each_topbar_child_maps_to_its_area():
    assert ".brand { grid-area: brand; min-width: 0; }" in MOBILE, \
        "brand shrink floor (A9) so a long version/update badge can't re-widen it"
    assert "grid-area: status;" in MOBILE
    assert "grid-area: nav;" in MOBILE


def test_nav_row_has_scroll_affordance_and_tap_targets():
    # was hidden (height:0) — the audit flagged no scroll cue; now a thin track.
    assert ".nav::-webkit-scrollbar { height: 3px; }" in MOBILE
    assert ".nav::-webkit-scrollbar-thumb { background: var(--green-deep)" in MOBILE
    assert "scrollbar-width: thin;" in MOBILE
    assert ".nav::-webkit-scrollbar { height: 0; }" not in MOBILE
    # taller tabs on their dedicated row (~28px → ~44px).
    assert ".nav a { padding: 12px var(--gap-4); }" in MOBILE


def test_desktop_topbar_grid_unchanged():
    # the base rule stays the 3-col single row — the 2-row layout is a phone-tier
    # override only (motif_mobile_css_scope_breakpoint).
    base = APP_CSS[APP_CSS.index(".topbar {"):]
    base = base[:base.index("}")]
    assert "grid-template-columns: auto 1fr auto;" in base
    assert "grid-template-areas" not in base


def test_op_mini_job_strip_reservation_survives():
    # the v0.50.91 bottom-strip reservation must still fire — the job strip pins
    # below the new nav row (verified in-browser: no overlap).
    assert ".topbar:has(#op-mini:not([hidden])) { padding-bottom: 30px; }" in MOBILE
