"""v1.15.31 — dashboard layout reset to pre-v1.15.27 shape (with
ADDED TODAY/THIS WEEK kept).

## Why

the user reviewed v1.15.29's dashboard (still running on v1.15.28
in his deploy) and walked back to the pre-v1.15.27 shape:

> "Let's go to the pre 1.15.27 dashboard but include the added
>  today/this week. I like that one better so we'll edit from
>  that point."

## What changed

### Restored from pre-v1.15.27 (v1.15.19) layout

1. **THEMERRDB top cards** — `// THEMERRDB MOVIES` + `// THEMERRDB
   TV SERIES` cards back to the v1.15.19 shape:
     - Big number: TDB catalogue size (`data-stat="movies.total"`
       / `tv.total`)
     - Bar fill via `[data-bar-fill="movies"]` / `tv` (driven
       by `stats.movies.downloaded / stats.movies.total`)
     - Foot: `<X> in your library · <Y> themed`
       (`tdb-{movies,tv}-in-library` + `tdb-{movies,tv}-themed`
       IDs)
     - Header glyph (▷ for movies, ◇ for tv)
     - `.stat-tdb-primary` tone (orange)

2. **PLEX cards** — `// MOVIES THEMED` + `// TV THEMED` + `// PLEX
   ANIME` back to the v1.15.19 shape:
     - Big number: library total (`#plex-{movies,tv,anime}-total`)
     - NO bar on the card itself
     - Foot: `<X> with theme · <Y> ThemerrDB available`
     - PLEX TV keeps v1.15.29's `--blue` tone (separate the user
       ask)

### Kept from v1.15.27

3. **ACTIVITY row** with ADDED TODAY + ADDED THIS WEEK (the user:
   "include the added today/this week"). Slotted between
   OPERATIONS and PLEX LIBRARY so the operational flow reads
   "what's queued (OPERATIONS) → what was placed recently
   (ACTIVITY) → per-library coverage detail (PLEX LIBRARY)".

### Stays gone

4. **// THEMERRDB CATALOG mini-card** (the orange v1.15.27
   surface that confused the user) stays dropped.

### v1.15.29 walkbacks

5. v1.15.29's TDB top-card shape (catalog as big + themed/
   motif_available bar + "match yours / themed" foot) is
   superseded by the pre-v1.15.27 shape above.
6. v1.15.29's PLEX TV `--blue` tone is preserved (separate
   ask the user explicitly wanted).
"""
from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
DASH = REPO / "app" / "web" / "templates" / "dashboard.html"
APP_JS = REPO / "app" / "web" / "static" / "app.js"
APP_CSS = REPO / "app" / "web" / "static" / "app.css"


def _strip_comments(html: str) -> str:
    html = re.sub(r"\{#.*?#\}", "", html, flags=re.DOTALL)
    html = re.sub(r"<!--.*?-->", "", html, flags=re.DOTALL)
    return html


# ── 1. THEMERRDB top row: pre-v1.15.27 shape restored ─────────


def test_themerrdb_top_section_uses_coverage_label():
    """The top section's data-dash-label is back to "COVERAGE"
    (the v1.14.70 / v1.15.19 label). v1.15.27 dropped the
    section entirely; v1.15.29 brought it back as "THEMERRDB
    COVERAGE"; v1.15.31 reverts to the original "COVERAGE"
    so the customize-toolbar layout matches what users had
    before v1.15.27."""
    visible = _strip_comments(DASH.read_text())
    assert 'data-dash-section="top-stats"' in visible
    assert 'data-dash-label="COVERAGE"' in visible


def test_top_cards_lead_with_coverage_pct():
    """v1.23.34 reworked the top cards from TDB-catalogue-size
    headlines (the user: "doesn't actually mean much … the progress
    bars while fun to fill don't show anything useful") into a
    library-coverage tracker: big number = % of YOUR library themed
    (id=cov-movies-pct, via the ssr_pct macro). The reverted
    catalogue-size headline (data-stat="movies.total") is gone."""
    # v0.51.122: the % swapped onto the // MOVIES THEMED card (the user kept the
    # titles, swapped the numbers); // MOVIES carries the reach total.
    visible = _strip_comments(DASH.read_text())
    movies_anchor = visible.index("// MOVIES THEMED")
    movies_card_end = visible.index("</article>", movies_anchor)
    movies_card = visible[
        visible.rfind("<article", 0, movies_anchor):movies_card_end
    ]
    assert 'id="cov-movies-pct"' in movies_card
    assert "ssr_pct(" in movies_card
    assert 'data-stat="movies.total"' not in movies_card


def test_themerrdb_top_cards_have_data_bar_fill_anchors():
    """The pre-v1.15.27 bar fills used `[data-bar-fill="movies"]`
    / `[data-bar-fill="tv"]` selectors driven by
    `stats.movies.downloaded / stats.movies.total`. v1.15.27
    dropped these anchors; v1.15.31 restores them."""
    visible = _strip_comments(DASH.read_text())
    assert 'data-bar-fill="movies"' in visible, (
        "v1.15.31: TDB MOVIES card must carry [data-bar-fill=\"movies\"] "
        "for the JS bar-fill writer (driven by downloaded/total)"
    )
    assert 'data-bar-fill="tv"' in visible


def test_top_cards_foot_uses_themed_of_total_and_ready_to_add():
    """v1.23.34 foot: `<X> of <Y> themed` + `<Z> ready to add`
    (ready = TDB-available rows not yet themed) via the cov-* IDs.
    Replaces the old "in your library / themed" foot that hung off
    the catalogue-size headline."""
    visible = _strip_comments(DASH.read_text())
    for key in ("movies", "tv", "collections"):
        assert f'id="cov-{key}-themed"' in visible
        assert f'id="cov-{key}-total"' in visible
        assert f'id="cov-{key}-ready"' in visible
    assert "ready to add" in visible
    # The reverted "in your library" foot phrasing is gone.
    assert "in your library" not in visible
    assert 'id="tdb-movies-in-library"' not in visible


def test_themerrdb_top_cards_carry_tdb_primary_tone_and_glyph():
    """v1.15.19's `.stat-tdb-primary` tone (orange left bar,
    matches // SYNC THEMERRDB family) survives onto the restored
    cards. v1.21.4 swapped the glyphs to set B (▶ movies, ▭ tv)
    after the user flagged the outline/diamond set as inconsistent."""
    visible = _strip_comments(DASH.read_text())
    movies_anchor = visible.index("// MOVIES")
    movies_card = visible[
        visible.rfind("<article", 0, movies_anchor):movies_anchor + 600
    ]
    assert "stat-tdb-primary" in movies_card
    assert "media_glyph('movies')" in movies_card  # v1.24.66: SVG via macro

    tv_anchor = visible.index("// TV")
    tv_card = visible[
        visible.rfind("<article", 0, tv_anchor):tv_anchor + 600
    ]
    assert "stat-tdb-primary" in tv_card
    assert "media_glyph('tv')" in tv_card


# ── 2. PLEX cards: pre-v1.15.27 shape restored ────────────────


def test_plex_movies_card_leads_with_total_no_coverage_pct():
    """PLEX MOVIES card's big number must be the library total
    (`#plex-movies-total`), not the v1.15.27 coverage % nor
    the v1.15.27 `-total-chip` header chip. v1.23.41 foot carries
    the ThemerrDB reach — "in ThemerrDB / not in ThemerrDB"."""
    # v0.51.122: the reach total + reach foot swapped onto the // MOVIES
    # card (the user kept the titles, swapped the numbers); // MOVIES THEMED now
    # leads with the coverage %.
    visible = _strip_comments(DASH.read_text())
    reach_anchor = visible.index("// MOVIES")
    reach_card = visible[
        visible.rfind("<article", 0, reach_anchor):visible.index("</article>", reach_anchor)
    ]
    assert 'id="plex-movies-total"' in reach_card, (
        "reach card big number ID must be plex-movies-total"
    )
    # v1.15.27 IDs stay gone.
    assert "plex-movies-coverage-pct" not in reach_card
    assert "plex-movies-total-chip" not in reach_card
    # Reach foot.
    assert "in ThemerrDB" in reach_card
    assert "not in ThemerrDB" in reach_card
    assert 'id="plex-movies-not-tdb"' in reach_card
    # The coverage % moved off this card onto // MOVIES THEMED.
    assert 'id="cov-movies-pct"' not in reach_card


def test_plex_tv_card_uses_blue_tone_and_total_lead():
    """PLEX TV card: total as big number + v1.23.41 reach foot
    (`in ThemerrDB / not in ThemerrDB`). v1.15.29's .stat-plex-tv →
    blue tone is preserved."""
    # v0.51.122: blue tone stays on // TV THEMED; the total + reach foot swapped
    # onto the // TV card (the user kept the titles, swapped the numbers).
    visible = _strip_comments(DASH.read_text())
    tv_anchor = visible.index("// TV THEMED")
    tv_card = visible[
        visible.rfind("<article", 0, tv_anchor):visible.index("</article>", tv_anchor)
    ]
    assert "stat-plex-tv" in tv_card
    reach_anchor = visible.index("// TV")
    reach_card = visible[
        visible.rfind("<article", 0, reach_anchor):visible.index("</article>", reach_anchor)
    ]
    assert 'id="plex-tv-total"' in reach_card
    assert "in ThemerrDB" in reach_card
    assert "not in ThemerrDB" in reach_card
    assert 'id="plex-tv-not-tdb"' in reach_card

    # v1.15.29 PLEX TV blue color preserved (CSS-side).
    css = APP_CSS.read_text()
    tv_css_anchor = css.index(".stat-plex-tv")
    tv_css_block = css[tv_css_anchor:tv_css_anchor + 200]
    # v1.24.65: via --dash-tv-color (defaults to --blue in :root).
    assert "var(--dash-tv-color)" in tv_css_block
    assert "--dash-tv-color: var(--blue)" in css


def test_plex_anime_card_uses_total_lead_and_magenta_tone():
    """PLEX ANIME — total as big number, no bar, v1.23.41 reach
    foot ("in ThemerrDB / not in ThemerrDB"). Magenta tone
    preserved (the user's pick)."""
    # v0.51.122: magenta tone stays on // ANIME THEMED; the total + reach foot
    # swapped onto the // ANIME card (the user kept the titles).
    visible = _strip_comments(DASH.read_text())
    anime_anchor = visible.index("// ANIME THEMED")
    anime_card = visible[
        visible.rfind("<article", 0, anime_anchor):visible.index("</article>", anime_anchor)
    ]
    assert "stat-plex-anime" in anime_card
    reach_anchor = visible.index("// ANIME")
    reach_card = visible[
        visible.rfind("<article", 0, reach_anchor):visible.index("</article>", reach_anchor)
    ]
    assert 'id="plex-anime-total"' in reach_card
    assert "in ThemerrDB" in reach_card
    assert "not in ThemerrDB" in reach_card
    assert 'id="plex-anime-not-tdb"' in reach_card


def test_plex_cards_no_per_card_bar():
    """Pre-v1.15.27 PLEX cards had NO bars on the cards
    themselves. The visual coverage bars the user referenced —
    "I did like the graphs below that" — live in the v1.13.27
    per-section comparison block further down. Regression
    guard: don't accidentally re-add the v1.15.27 plex-card
    bar elements."""
    visible = _strip_comments(DASH.read_text())
    # Locate each PLEX card individually + check no `id="plex-*-bar"`.
    for label, bar_id in [
        ("// MOVIES THEMED", "plex-movies-bar"),
        ("// TV THEMED", "plex-tv-bar"),
        ("// ANIME THEMED", "plex-anime-bar"),
    ]:
        anchor = visible.index(label)
        card_end = visible.index("</article>", anchor)
        card = visible[visible.rfind("<article", 0, anchor):card_end]
        assert f'id="{bar_id}"' not in card, (
            f"v1.15.31: pre-v1.15.27 PLEX cards had no bar — "
            f"{bar_id} must stay gone from {label} card"
        )


# ── 3. ACTIVITY row preserved + repositioned ──────────────────


def test_activity_row_preserved_with_two_cards():
    """ACTIVITY row from v1.15.27 stays — the user: "include the
    added today/this week. I like that one better so we'll edit
    from that point." Two cards (ADDED TODAY + ADDED THIS WEEK);
    the v1.15.27 third // THEMERRDB CATALOG mini-card stays
    dropped."""
    html = DASH.read_text()
    activity_start = html.index('data-dash-section="activity"')
    activity_end = html.index('</section>', activity_start)
    block = html[activity_start:activity_end]
    assert "ADDED TODAY" in block
    assert "ADDED THIS WEEK" in block
    assert 'id="activity-placements-today"' in block
    assert 'id="activity-placements-week"' in block
    # Catalog mini-card stays dropped.
    assert "// THEMERRDB CATALOG" not in block
    assert block.count("<article") == 2


def test_activity_row_sits_between_operations_and_storage():
    """v1.15.31 originally slotted ACTIVITY between OPERATIONS
    and PLEX LIBRARY (order: COVERAGE → OPERATIONS → ACTIVITY
    → PLEX LIBRARY → STORAGE). v1.18.50 relocated PLEX LIBRARY
    up to immediately follow COVERAGE so the two COLLECTIONS
    cards (TDB + PLEX) land in the top-two-rows fold per
    the user's ask. New order:
      COVERAGE → PLEX LIBRARY → OPERATIONS → ACTIVITY → STORAGE
    The v1.15.31 narrative about "operational flow reads what's
    queued → placed → per-library detail" is preserved
    conceptually (OPERATIONS still precedes ACTIVITY) just with
    PLEX LIBRARY pulled forward to pair with COVERAGE. ACTIVITY's
    new neighborhood is between OPERATIONS and STORAGE; pin that."""
    html = DASH.read_text()
    operations_idx = html.index('data-dash-section="operations"')
    activity_idx = html.index('data-dash-section="activity"')
    storage_idx = html.index('data-dash-section="storage"')
    assert operations_idx < activity_idx < storage_idx, (
        "v1.18.50: ACTIVITY row must sit between OPERATIONS and "
        "STORAGE in source order (PLEX LIBRARY moved up in v1.18.50)"
    )
    # The new pairing — PLEX LIBRARY immediately follows COVERAGE.
    plex_idx = html.index('data-dash-section="plex-coverage"')
    coverage_idx = html.index('data-dash-section="top-stats"')
    assert coverage_idx < plex_idx < operations_idx, (
        "v1.18.50: PLEX LIBRARY must sit between COVERAGE and "
        "OPERATIONS so both COLLECTIONS cards land in the top "
        "two rows"
    )


# ── 4. JS hydration matches the restored shape ────────────────


def test_js_hydrates_coverage_bars_from_themed_over_total():
    """v1.23.34: the top-card bars are now written by setCov() in
    renderPlexCoverage and fill with themed/total (% of the library
    themed), not the old downloaded/TDB-catalogue ratio. setCov
    selects `[data-bar-fill="${key}"]` for movies/tv/collections."""
    src = APP_JS.read_text()
    anchor = src.index("const setCov = (key, themed, totalN, ready)")
    # v0.50.46: widened 900→1200 — the bar-write count-up-defer guard added lines.
    block = src[anchor:anchor + 1200]
    assert '[data-bar-fill="${key}"]' in block
    assert "themed / totalN * 100" in block
    # The reverted downloaded/total bar formula is gone.
    assert "stats.movies.downloaded / stats.movies.total" not in src


def test_js_hydrates_plex_total_ids_not_coverage_pct_ids():
    """The PLEX cards' big number write must target
    `#plex-{movies,tv}-total` (pre-v1.15.27 IDs), NOT the
    v1.15.27 `-coverage-pct` IDs. Same for the foot stats."""
    src = APP_JS.read_text()
    anchor = src.index("v1.15.31 reset: pre-v1.15.27 PLEX-card hydration")
    block = src[anchor:anchor + 2000]
    # Total writes.
    assert "#plex-movies-total'" in block
    assert "#plex-tv-total'" in block
    # Foot stats — v1.23.41 reach (in-TDB = motif, not-in-TDB).
    assert "#plex-movies-motif" in block
    assert "#plex-movies-not-tdb" in block
    assert "#plex-tv-motif" in block
    assert "#plex-tv-not-tdb" in block
    # v1.15.27 coverage-pct writes must be GONE from this block.
    assert "plex-movies-coverage-pct" not in block
    assert "plex-tv-coverage-pct" not in block
    assert "plex-movies-total-chip" not in block


def test_js_hydrates_coverage_foot_stats():
    """v1.23.34: setCov writes the foot stats `cov-{key}-themed`,
    `cov-{key}-total`, `cov-{key}-ready` (themed/total + ready-to-
    add). Replaces the old tdb-*-in-library / -themed writes."""
    src = APP_JS.read_text()
    anchor = src.index("const setCov = (key, themed, totalN, ready)")
    # v0.50.46: widened 900→1200 — the bar-write count-up-defer guard added lines.
    block = src[anchor:anchor + 1200]
    assert "cov-${key}-themed" in block
    assert "cov-${key}-total" in block
    assert "cov-${key}-ready" in block
    assert "#tdb-movies-in-library" not in src


def test_js_anime_card_uses_pre_v1_15_27_shape():
    """The anime card writes `#plex-anime-total` (big number) + foot stats;
    v1.15.27's `-coverage-pct` / `-bar` / `-total-chip` writes stay gone.
    v1.23.90: populated in renderPlexCoverage from data.anime (was the retired
    renderPlexAnimeCard)."""
    src = APP_JS.read_text()
    anchor = src.index("const animeItems = data.anime")
    fn_body = src[anchor:anchor + 900]
    assert "'#plex-anime-total'" in fn_body, (
        "v1.23.90: anime card hydration must write to plex-anime-total"
    )
    # v1.23.41: reach foot (in-TDB = motif, not-in-TDB).
    assert "plex-anime-not-tdb" in fn_body
    assert "plex-anime-motif" in fn_body
    # v1.15.27 IDs must be gone.
    assert "plex-anime-coverage-pct" not in fn_body
    assert "plex-anime-total-chip" not in fn_body
    assert "plex-anime-bar" not in fn_body


# ── 5. Activity stats SQL preserved (v1.15.27 server-side) ────


def test_activity_placements_select_clauses_preserved():
    """The /api/stats SELECTs that drive ADDED TODAY / WEEK
    (added in v1.15.27) must stay — the user explicitly wants
    these surfaces kept. Regression guard against a "revert
    everything from v1.15.27" overreach."""
    api_py = (REPO / "app" / "web" / "api.py").read_text()
    assert "AS placements_today" in api_py
    assert "AS placements_week" in api_py
    assert '"activity": {' in api_py
    assert '"placements_today"' in api_py
    assert '"placements_week"' in api_py
