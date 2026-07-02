"""v0.51.4 — hero action buttons reflow consistently across pages on mobile.

Mobile audit finding B1 (P1): the hero action groups reflowed THREE different
ways on a phone. Desktop docks them right of the title on every page. On mobile
the library page's .sync-actions went full-width below the title (width:100% +
#library-refresh-btn flex:1 1 100%), but the dashboard's .hero-actions had NO
mobile rule — under .hero-row's justify-content:space-between it wrapped to a 2nd
line and stayed LEFT-aligned at its natural width. So swiping between tabs, the
primary hero button jumped full-width (library) → left-aligned (dashboard).

The user chose one treatment for all: full-width below the title. This adds the
matching .hero-actions rule so the dashboard pair spans the row like the library.
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


def test_hero_actions_full_width_on_mobile():
    # the dashboard hero group now matches the library's full-width treatment.
    assert ".hero-actions { width: 100%; }" in MOBILE
    assert ".hero-actions .btn { flex: 1 1 100%; }" in MOBILE


def test_library_sync_actions_treatment_still_present():
    # the reference treatment we're matching — unchanged.
    assert ".sync-actions { width: 100%; }" in MOBILE
    assert "#library-refresh-btn { min-width: 0; flex: 1 1 100%; }" in MOBILE


def test_hero_actions_not_full_width_on_desktop():
    # desktop keeps the right-docked flex row (no width override in the base rule).
    base = APP_CSS[APP_CSS.index(".hero-actions {"):]
    base = base[:base.index("}")]
    assert "width: 100%" not in base
    assert "display: flex" in base
