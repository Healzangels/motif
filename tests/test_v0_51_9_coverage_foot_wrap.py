"""v0.51.9 — TDB coverage card foot wraps instead of clipping.

Mobile audit finding A7 (P3): the TDB COVERAGE cards (MOVIES/TV/ANIME/COLLECTIONS
THEMED) render two foot spans ("N of M themed" + "Z ready to add") in a base
.stat-foot flex row with no wrap. With 4-5 digit counts the two spans exceed the
card width and clip against the card's overflow:hidden — worst on a narrow 4-up
desktop card or a full-width phone card. The PLEX cards already stack their foot
outright (v1.21.4 flex-direction:column); coverage had no such relief.

Fix: flex-wrap:wrap on the base .stat-foot so the second span drops to a 2nd line
only when it must. Verified in-browser at 375px with injected big counts: the
second span wrapped below the first, the foot stayed within the card (no clip),
no page overflow.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def _rule(selector: str) -> str:
    i = APP_CSS.index(selector + " {")
    return APP_CSS[i:APP_CSS.index("}", i)]


def test_stat_foot_wraps():
    foot = _rule("\n.stat-foot")
    assert "flex-wrap: wrap;" in foot, "coverage foot must wrap so big counts don't clip"
    assert "display: flex;" in foot


def test_plex_cards_still_stack():
    # the v1.21.4 plex-card treatment (stack outright) is independent + unchanged.
    plex = _rule('[data-dash-card^="plex-"] .stat-foot')
    assert "flex-direction: column;" in plex
