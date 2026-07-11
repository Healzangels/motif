"""v1.15.29 — dashboard top-row correction + PLEX TV tone change.

## Why

the user reviewed the v1.15.27 dashboard rework and walked back
two of the three changes:

> "Whoa I'm not crazy about the dashboard rework. My big concern
>  were the top two rows of content I did like the graphs below
>  that. I do like the added today and added this week. I'm not
>  sure what the therrdb catalog card is and why its a different
>  color.
>  Let's re-add the graphs from before. I liked the plex movies,
>  tv, anime sections by color but lets make tv a new color my
>  a blue
>  and let's make the top row relavent information on themerrdb.
>  I don't mind the % covered so we can play with that more but
>  want it to be useful information for users that oh themerrdb
>  has this many themes in it, I've got this many themes applied
>  to my library."

## Three corrections shipped together

### 1. Drop the // THEMERRDB CATALOG mini-card (orange tone)

It read as a confusing third stat in the ACTIVITY row — a TDB-
side number sitting next to two motif-placement counts (themes-
added today/week). the user: "I'm not sure what the therrdb
catalog card is and why its a different color." The TDB
catalogue size moves back up to the headline row's big number
where it has clearer context.

### 2. Re-add // THEMERRDB MOVIES + // THEMERRDB TV SERIES top row

But reworked from the v1.15.19 shape that confused the user
("themed doesn't indicate why its bigger then in your library
or why it's there"):
  • Big number: TDB catalogue size (the reference count the user
    explicitly wants visible — "themerrdb has this many themes
    in it")
  • Bar fill: themed / motif_available coverage rate (the
    actionable "of the items TDB has themes for in your
    library, what % are themed?" — the user: "I don't mind the
    % covered")
  • Foot: "<X> match yours · <Y> themed" (clear bridge from
    the BIG TDB number to the SMALLER user-library counts —
    the user: "I've got this many themes applied to my library")

### 3. PLEX TV tone: green → blue

the user: "let's make tv a new color my a blue." The pre-fix
.stat-plex-tv shared --green with the default palette
(everything else green-on-black); --blue gives PLEX TV a
distinct family.
"""
from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
DASH = REPO / "app" / "web" / "templates" / "dashboard.html"
APP_CSS = REPO / "app" / "web" / "static" / "app.css"
APP_JS = REPO / "app" / "web" / "static" / "app.js"


def _strip_comments(html: str) -> str:
    """Strip both `{# ... #}` Jinja comments and `<!-- ... -->`
    HTML comments."""
    html = re.sub(r"\{#.*?#\}", "", html, flags=re.DOTALL)
    html = re.sub(r"<!--.*?-->", "", html, flags=re.DOTALL)
    return html


# ── 1. Drop // THEMERRDB CATALOG mini-card ────────────────────


def test_themerrdb_catalog_mini_card_removed():
    """v1.15.27's // THEMERRDB CATALOG mini-card (orange-toned,
    sat in the ACTIVITY row alongside ADDED TODAY / ADDED THIS
    WEEK) was dropped in v1.15.29. the user: "I'm not sure what
    the therrdb catalog card is and why its a different color."
    The catalogue size resurfaces as the big number on the new
    THEMERRDB top row cards."""
    visible = _strip_comments(DASH.read_text())
    assert "// THEMERRDB CATALOG" not in visible, (
        "v1.15.29: the // THEMERRDB CATALOG mini-card must be "
        "dropped — its orange tone confused the user in the "
        "ACTIVITY row context"
    )


def test_activity_row_only_has_two_added_cards():
    """The ACTIVITY row must contain only ADDED TODAY +
    ADDED THIS WEEK now (was 3 cards in v1.15.27 — the third
    was dropped in v1.15.29)."""
    html = DASH.read_text()
    activity_start = html.index('data-dash-section="activity"')
    activity_end = html.index('</section>', activity_start)
    block = html[activity_start:activity_end]
    # Both retained cards.
    assert "ADDED TODAY" in block
    assert "ADDED THIS WEEK" in block
    # Count <article> tags — exactly two.
    assert block.count("<article") == 2, (
        "v1.15.29: ACTIVITY row must hold exactly two cards "
        "(ADDED TODAY + ADDED THIS WEEK) — the dropped THEMERRDB "
        "CATALOG mini-card stays gone"
    )


# ── 2. THEMERRDB top row restored ─────────────────────────────


def test_themerrdb_top_row_present_with_pre_v1_15_27_label():
    """The THEMERRDB top row must include both // THEMERRDB
    MOVIES + // THEMERRDB TV SERIES cards. v1.15.29 used the
    "THEMERRDB COVERAGE" data-dash-label; v1.15.31 reverted
    to the pre-v1.15.27 "COVERAGE" label so the customize-
    toolbar matches the layout users had pre-v1.15.27."""
    visible = _strip_comments(DASH.read_text())
    # v1.23.34: relabelled to the coverage framing (was THEMERRDB *).
    assert "// MOVIES" in visible
    assert "// TV" in visible
    assert 'data-dash-section="top-stats"' in visible
    assert 'data-dash-label="COVERAGE"' in visible


def test_top_cards_lead_with_coverage_pct():
    """v1.23.34: the big number is now library theme coverage %
    (id=cov-*-pct via ssr_pct), not the TDB catalogue size that
    the user flagged as meaningless. data-stat="movies.total" gone."""
    # v0.51.122: the coverage % + bar swapped onto the // PLEX cards (the user
    # kept the titles, swapped the numbers) — the // …THEMED cards carry reach.
    visible = _strip_comments(DASH.read_text())
    movies_anchor = visible.index("// MOVIES THEMED")
    movies_card_start = visible.rfind("<article", 0, movies_anchor)
    movies_card_end = visible.index("</article>", movies_anchor)
    movies_card = visible[movies_card_start:movies_card_end]
    assert 'id="cov-movies-pct"' in movies_card
    assert 'data-stat="movies.total"' not in movies_card
    tv_anchor = visible.index("// TV THEMED")
    tv_card_start = visible.rfind("<article", 0, tv_anchor)
    tv_card_end = visible.index("</article>", tv_anchor)
    tv_card = visible[tv_card_start:tv_card_end]
    assert 'id="cov-tv-pct"' in tv_card


def test_themerrdb_top_cards_have_bar_and_foot_stats():
    """Each TDB top card must carry a bar fill + foot stats
    showing the user-library subset alongside the BIG TDB
    catalogue number. v1.15.29 used `tdb-{movies,tv}-bar` IDs
    + "match yours / themed" foot; v1.15.31 reverted to the
    pre-v1.15.27 form: `[data-bar-fill]` selectors for the
    bars + "in your library / themed" foot phrasing."""
    # v0.51.122: the bar + coverage foot swapped onto the // PLEX cards (the
    # user kept the titles, swapped the numbers).
    visible = _strip_comments(DASH.read_text())
    movies_anchor = visible.index("// MOVIES THEMED")
    movies_card_end = visible.index("</article>", movies_anchor)
    movies_card = visible[
        visible.rfind("<article", 0, movies_anchor):movies_card_end
    ]
    assert 'data-bar-fill="movies"' in movies_card
    assert 'id="cov-movies-themed"' in movies_card
    assert 'id="cov-movies-total"' in movies_card
    assert 'id="cov-movies-ready"' in movies_card
    assert "ready to add" in movies_card

    tv_anchor = visible.index("// TV THEMED")
    tv_card_end = visible.index("</article>", tv_anchor)
    tv_card = visible[
        visible.rfind("<article", 0, tv_anchor):tv_card_end
    ]
    assert 'data-bar-fill="tv"' in tv_card
    assert 'id="cov-tv-themed"' in tv_card
    assert 'id="cov-tv-ready"' in tv_card


def test_themerrdb_top_cards_use_tdb_primary_tone():
    """v1.15.19's .stat-tdb-primary tone (orange left bar, the
    // SYNC THEMERRDB family) survives onto the new TDB top
    cards — anchors the operator's eye on TDB-side surfaces."""
    visible = _strip_comments(DASH.read_text())
    movies_anchor = visible.index("// MOVIES")
    movies_card = visible[
        visible.rfind("<article", 0, movies_anchor):movies_anchor
    ]
    assert "stat-tdb-primary" in movies_card
    tv_anchor = visible.index("// TV")
    tv_card = visible[
        visible.rfind("<article", 0, tv_anchor):tv_anchor
    ]
    assert "stat-tdb-primary" in tv_card


# ── 3. PLEX TV tone: green → blue ────────────────────────────


def test_plex_tv_tone_uses_blue_not_green():
    """the user: "let's make tv a new color my a blue." The
    .stat-plex-tv CSS rule must use --blue (not the v1.15.19
    --green). Pin both the rule + the var() reference so a
    future palette refactor can't silently revert the tone."""
    css = APP_CSS.read_text()
    assert ".stat-plex-tv" in css
    tv_anchor = css.index(".stat-plex-tv")
    tv_block = css[tv_anchor:tv_anchor + 200]
    # v1.24.65: via --dash-tv-color, which defaults to --blue in :root (was
    # --green in v1.15.19). Pin the indirection + the default identity.
    assert "var(--dash-tv-color)" in tv_block
    assert "var(--green)" not in tv_block
    assert "--dash-tv-color: var(--blue)" in css


def test_plex_movies_and_anime_tones_unchanged():
    """Defensive guard: the v1.15.29 PLEX TV color change must
    not bleed into PLEX MOVIES (--amber) or PLEX ANIME
    (--magenta). the user explicitly said he liked those.
    v1.24.65: now via --dash-movies-color / --dash-anime-color, defaulting to
    those tokens in :root."""
    css = APP_CSS.read_text()
    movies_anchor = css.index(".stat-plex-primary")
    movies_block = css[movies_anchor:movies_anchor + 200]
    assert "var(--dash-movies-color)" in movies_block
    assert "--dash-movies-color: var(--amber)" in css

    anime_anchor = css.index(".stat-plex-anime")
    anime_block = css[anime_anchor:anime_anchor + 200]
    assert "var(--dash-anime-color)" in anime_block
    assert "--dash-anime-color: var(--magenta)" in css


# ── 4. JS hydration of new TDB top cards ──────────────────────


def test_js_hydrates_tdb_top_card_bars_and_foot_stats():
    """The dashboard hydration must populate the TDB top
    cards' bars + foot stats. v1.15.29 used `tdb-{movies,tv}-bar`
    IDs + `-match` / `-themed` foot; v1.15.31 reverted to the
    pre-v1.15.27 hydration: `[data-bar-fill]` selectors for the
    bars + `tdb-{movies,tv}-in-library` / `-themed` foot IDs."""
    src = APP_JS.read_text()
    # v1.23.34: setCov writes the bars + foot. Bar selector survives
    # as a template literal; foot uses the cov-* IDs.
    assert '[data-bar-fill="${key}"]' in src
    assert "cov-${key}-themed" in src
    assert "cov-${key}-total" in src
    assert "cov-${key}-ready" in src


def test_js_coverage_bar_uses_themed_over_total_ratio():
    """v1.23.34: the top-card bar now reads % of YOUR library themed
    (themed/total via setCov), replacing the old downloaded/TDB-
    catalogue ratio that "filled but meant nothing" (the user)."""
    src = APP_JS.read_text()
    anchor = src.index("const setCov = (key, themed, totalN, ready)")
    block = src[anchor:anchor + 900]
    assert "themed / totalN * 100" in block
    assert "stats.movies.downloaded / stats.movies.total" not in src


def test_js_no_legacy_themerrdb_catalog_card_writes():
    """The dropped // THEMERRDB CATALOG mini-card had no JS
    hydration of its own (it used data-stat="movies.total" /
    "tv.total" via renderStat). The new top cards reuse those
    same data-stat hooks for their big number — no rename
    needed. Regression guard: don't re-add a write to the
    dropped catalog mini-card id."""
    src = APP_JS.read_text()
    # Defensive — there was no specific catalog-card id to
    # check, but if a future rework adds one and forgets to
    # drop the corresponding JS write, this catches it.
    assert "themerrdb-catalog" not in src
