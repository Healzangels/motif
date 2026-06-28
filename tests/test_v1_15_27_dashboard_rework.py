"""v1.15.27 — dashboard rework: drop TDB top cards, lead PLEX
cards with coverage %, add ACTIVITY (themes-added) row.

## Why

the user's v1.15.25 feedback after looking at the deployed
dashboard:

> "On the Dashboard I believe the Plex TB ThemerrDB is inaccurate
>  since it would be including anime shows that are in the TV
>  Shows list from themerrdb..." (the v1.15.26 fix)
>
> "Also the ThermerDB Movies card, in your library and themed
>  numbers are a bit confusing as it doesn't really tell me what
>  themed should be... themed doesn't indicate why its bigger
>  then in your library or why it's there. Same with tv shows
>  which is even more confusing... I am up to reworking these
>  entirely to make more sense and have better information.
>  Using best practices for dashboards."
>
> "Also it would be great to be able to tell either on the
>  dashboard or somewhere how many new themes were added
>  automatically since I get such a low rate per hour even with
>  bumping it. Maybe somewhere we should know how many it added
>  in the last day, week, month and we should know whats up
>  next."

## Three changes shipped together

### 1. Dropped // THEMERRDB MOVIES + // THEMERRDB TV SERIES top cards

The "in your library / themed" pairing on those cards confused
the user — and rightly so: the TDB cards displayed a TDB-side
catalogue total (e.g. 130k movies known to themerrdb) alongside
two motif-side aggregates (themed locally, library size). Three
numbers from two different sources sitting under one headline
read as a contradiction. The TDB catalogue size is reference
info, not actionable — moved to a single compact // THEMERRDB
CATALOG card in the new ACTIVITY row.

### 2. PLEX MOVIES / TV / ANIME cards lead with coverage %

Pre-fix the big number was the library total (e.g. "10,358")
which is reference info. Post-fix the big number is coverage %
("85%") with the total as a muted "of 10,358" header chip. A
horizontal bar fill mirrors the percentage. Foot row carries
the absolute counts (themed + ThemerrDB). the user: "Using best
practices for dashboards" — the focal big number should be the
actionable signal at-a-glance ("am I covered?"), not the
reference data.

### 3. ACTIVITY row with ADDED TODAY / ADDED THIS WEEK

Surfaces what motif actually did recently. Backed by a new SELECT
in /api/stats counting placements rows by placed_at relative to
now (`-1 day` and `-7 day`). the user saw "Project Hail Mary"
land via docker logs, not the UI — the dashboard had no surface
for "what motif did today."
"""
from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
DASH = REPO / "app" / "web" / "templates" / "dashboard.html"
APP_JS = REPO / "app" / "web" / "static" / "app.js"
API_PY = REPO / "app" / "web" / "api.py"


def _strip_comments(html: str) -> str:
    """Strip both `{# ... #}` Jinja comments and `<!-- ... -->`
    HTML comments. Tests that anchor on rendered content (or
    assert "old phrasing must be gone") need to ignore mentions
    in v1.15.27 marker comments AND in older comments
    documenting prior reworks."""
    html = re.sub(r"\{#.*?#\}", "", html, flags=re.DOTALL)
    html = re.sub(r"<!--.*?-->", "", html, flags=re.DOTALL)
    return html


# ── 1. /api/stats: placements_today + placements_week ─────────


def test_stats_query_selects_placements_today():
    """The /api/stats SQL block must SELECT a placements_today
    aggregate (COUNT(*) FROM placements WHERE placed_at >=
    datetime('now', '-1 day'))."""
    src = API_PY.read_text()
    assert "AS placements_today" in src, (
        "v1.15.27: /api/stats must surface a placements_today "
        "count for the new ACTIVITY row"
    )
    # The predicate must scope to the last 24 hours.
    today_anchor = src.index("AS placements_today")
    today_pre = src[max(0, today_anchor - 200):today_anchor]
    assert "FROM placements" in today_pre
    assert "datetime('now', '-1 day')" in today_pre


def test_stats_query_selects_placements_week():
    """Same shape as today, but `-7 day` for the week stat."""
    src = API_PY.read_text()
    assert "AS placements_week" in src, (
        "v1.15.27: /api/stats must surface a placements_week "
        "count for the new ACTIVITY row"
    )
    week_anchor = src.index("AS placements_week")
    week_pre = src[max(0, week_anchor - 200):week_anchor]
    assert "FROM placements" in week_pre
    assert "datetime('now', '-7 day')" in week_pre


def test_stats_payload_exposes_activity_section():
    """The /api/stats response must carry an `activity` dict with
    `placements_today` + `placements_week`. Pin both keys so a
    future SELECT-rename doesn't silently break the JS hydration
    (which does `stats.activity.placements_today`)."""
    src = API_PY.read_text()
    # The activity dict in the response payload.
    assert '"activity": {' in src or "'activity': {" in src, (
        "v1.15.27: /api/stats response must include 'activity' key"
    )
    # Locate the activity block — anchor on the comment marker.
    anchor = src.index('"activity": {')
    block = src[anchor:anchor + 400]
    assert '"placements_today"' in block
    assert '"placements_week"' in block
    # Both must hydrate from the SQL aliases.
    assert 'row["placements_today"]' in block
    assert 'row["placements_week"]' in block


# ── 2. dashboard.html: ACTIVITY row + dropped TDB cards ───────


def test_activity_section_present_with_new_dash_attributes():
    """The new ACTIVITY section must carry data-dash-section +
    data-dash-label so it shows up in the customize toolbar
    alongside the other rows. Replaces the v1.14.70 "top-stats"
    /COVERAGE section that v1.15.27 dropped."""
    html = DASH.read_text()
    assert 'data-dash-section="activity"' in html
    assert 'data-dash-label="ACTIVITY"' in html


def test_activity_section_has_added_today_and_week_stats():
    """ACTIVITY row must contain ADDED TODAY + ADDED THIS WEEK
    mini cards with the IDs the JS hydration writes to."""
    html = DASH.read_text()
    activity_start = html.index('data-dash-section="activity"')
    activity_end = html.index('</section>', activity_start)
    block = html[activity_start:activity_end]
    assert "ADDED TODAY" in block
    assert "ADDED THIS WEEK" in block
    assert 'id="activity-placements-today"' in block
    assert 'id="activity-placements-week"' in block


def test_top_cards_lead_with_library_coverage_pct():
    """v1.23.34 settled the long-iterated top-card headline (catalogue
    size vs coverage) firmly on COVERAGE: the big number is % of YOUR
    library themed (id=cov-*-pct). The catalogue-size data-stat hooks
    are gone — the user: that number "doesn't actually mean much"."""
    visible = _strip_comments(DASH.read_text())
    assert 'id="cov-movies-pct"' in visible
    assert 'id="cov-tv-pct"' in visible
    assert 'data-stat="movies.total"' not in visible
    assert 'data-stat="tv.total"' not in visible


def test_top_card_foot_uses_coverage_ids():
    """v1.23.34: foot anchors are cov-*-themed / -total / -ready
    (themed of total + ready-to-add), replacing the old
    tdb-*-in-library / -themed foot."""
    visible = _strip_comments(DASH.read_text())
    assert "cov-movies-themed" in visible
    assert "cov-tv-ready" in visible
    assert "tdb-movies-in-library" not in visible


# ── 3. PLEX cards: v1.15.27 coverage-% layout (superseded) ────


def test_plex_cards_no_longer_lead_with_coverage_percent():
    """v1.15.27 led the PLEX cards with coverage % via `-coverage-
    pct` headline IDs + `-bar` bars + `-total-chip` header chips.
    v1.15.31 reverted to the pre-v1.15.27 layout (total as big
    number, no card-level bar). Regression guard: those v1.15.27
    IDs must stay gone."""
    visible = _strip_comments(DASH.read_text())
    for stale_id in (
        "plex-movies-coverage-pct", "plex-movies-bar",
        "plex-movies-total-chip",
        "plex-tv-coverage-pct", "plex-tv-bar", "plex-tv-total-chip",
        "plex-anime-coverage-pct", "plex-anime-bar",
        "plex-anime-total-chip",
    ):
        assert stale_id not in visible, (
            f"v1.15.31: {stale_id} was a v1.15.27 surface — "
            f"reverted by v1.15.31 layout reset"
        )


def test_plex_cards_lead_with_total_per_pre_v1_15_27_layout():
    """v1.15.31 restored the pre-v1.15.27 PLEX-card shape: big
    number = library total via `#plex-{movies,tv,anime}-total`."""
    visible = _strip_comments(DASH.read_text())
    assert 'id="plex-movies-total"' in visible
    assert 'id="plex-tv-total"' in visible
    assert 'id="plex-anime-total"' in visible


# ── 4. JS hydration ────────────────────────────────────────────


def test_js_hydrates_activity_placements_stats():
    """The dashboard hydration must read stats.activity.placements_*
    and write to the new ADDED TODAY / WEEK elements. Pin both
    sides (read + write) so a payload-key rename can't silently
    leave the cards as '—'."""
    src = APP_JS.read_text()
    assert "stats.activity" in src or "activity.placements_today" in src
    assert "activity.placements_today" in src
    assert "activity.placements_week" in src
    assert "#activity-placements-today" in src
    assert "#activity-placements-week" in src


def test_js_no_longer_writes_v1_15_27_coverage_percent_ids():
    """v1.15.27 wrote coverage % + bar widths to PLEX cards via
    `-coverage-pct` / `-bar` / `-total-chip` IDs. v1.15.31
    reverted to the pre-v1.15.27 hydration (writes #plex-*-total
    as fmt.num(libraryTotal) and skips bar + chip writes).
    Regression guard: those v1.15.27 JS write paths must stay
    gone."""
    src = APP_JS.read_text()
    for stale_id in (
        "#plex-movies-coverage-pct",
        "#plex-tv-coverage-pct",
        "#plex-movies-total-chip",
        "#plex-tv-total-chip",
    ):
        assert stale_id not in src, (
            f"v1.15.31: {stale_id} was a v1.15.27 JS write target "
            f"— reverted by the layout reset"
        )


def test_js_anime_card_no_longer_uses_coverage_percent_lead():
    """v1.15.27's coverage-% writes (`-coverage-pct`, `-bar`, `-total-chip`)
    stay gone; v1.23.90 the anime card is populated in renderPlexCoverage from
    data.anime — big-number `plex-anime-total`, no bar / chip."""
    src = APP_JS.read_text()
    anchor = src.index("const animeItems = data.anime")
    fn_body = src[anchor:anchor + 900]
    assert "plex-anime-coverage-pct" not in fn_body
    assert "plex-anime-bar" not in fn_body
    assert "plex-anime-total-chip" not in fn_body
    # the big-number write target.
    assert "'#plex-anime-total'" in fn_body


def test_js_writes_coverage_bar_fill():
    """v1.23.34: setCov selects the bar anchors via a template
    literal (`[data-bar-fill="${key}"]`) for movies/tv/collections
    and fills them with the library coverage ratio."""
    src = APP_JS.read_text()
    assert '[data-bar-fill="${key}"]' in src
    assert "const setCov = (key, themed, totalN, ready)" in src
