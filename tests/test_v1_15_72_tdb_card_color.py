"""v1.15.72 — recolor THEMERRDB dashboard cards.

the user: "can we change the themerdb colors to match the
themerrdb status bar color instead of orange."

The dashboard's THEMERRDB MOVIES + THEMERRDB TV SERIES cards
(.stat-tdb-primary) had an orange (`--orange`) left accent bar
from v1.15.19. The comment claimed orange was "the // SYNC
THEMERRDB button family" — but that button has no special
class, it renders the default green like every other .btn. So
orange was off-palette on its own card.

The canonical motif THEMERRDB tone IS green-bright:
* `.link-badge-themerrdb` (T row pill in library)
* `.btn.lib-source-themerrdb` (SOURCE → THEMERRDB action)
* `.source-pie-T` (theme-source donut slice)
* the default green // SYNC THEMERRDB button

v1.15.72 swaps the .stat-tdb-primary accent from `--orange`
to `--green-bright` so the operator's eye groups TDB-side
surfaces by color consistently.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_CSS = REPO / "app" / "web" / "static" / "app.css"


def test_stat_tdb_primary_uses_themerrdb_green_not_orange():
    """The .stat-tdb-primary rule must use --green-bright (the
    canonical THEMERRDB tone) for its border-left-color, not
    the off-palette --orange the v1.15.19 cut shipped."""
    css = APP_CSS.read_text()
    rule_start = css.index(".stat-tdb-primary {")
    rule_end = css.index("}", rule_start)
    rule = css[rule_start:rule_end]
    assert "var(--green-bright)" in rule, (
        "v1.15.72: .stat-tdb-primary must use --green-bright (the "
        "link-badge-themerrdb / lib-source-themerrdb tone) so the "
        "dashboard TDB cards group visually with every other TDB "
        "surface in the app"
    )
    assert "var(--orange)" not in rule, (
        "v1.15.72: --orange must not appear in .stat-tdb-primary — "
        "the user flagged it as the wrong tone for the TDB cards"
    )


def test_stat_tdb_primary_matches_link_badge_themerrdb_tone():
    """Cross-source check: the dashboard card's accent color must
    match the library T-row pill's color. Both live in app.css —
    drift would split the operator's mental model of 'green ==
    motif manages from ThemerrDB' across two surfaces."""
    css = APP_CSS.read_text()
    # link-badge-themerrdb canonical tone (text color).
    badge_start = css.index(".link-badge-themerrdb {")
    badge_end = css.index("}", badge_start)
    badge = css[badge_start:badge_end]
    assert "var(--green-bright)" in badge, (
        "Precondition: .link-badge-themerrdb must use --green-bright "
        "(asserting just to anchor the canonical tone for this test)"
    )
    # Card accent uses the same.
    tdb_start = css.index(".stat-tdb-primary {")
    tdb_end = css.index("}", tdb_start)
    tdb = css[tdb_start:tdb_end]
    assert "var(--green-bright)" in tdb, (
        "v1.15.72: dashboard TDB card accent must match the canonical "
        "link-badge-themerrdb tone — both --green-bright"
    )
