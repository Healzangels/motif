"""v1.23.34 — dashboard top cards show library theme COVERAGE.

the user: the top-3 cards led with ThemerrDB's *global* catalogue size
+ a "downloaded / catalogue" bar that "while fun to fill don't show
anything useful". Reworked into a coverage tracker that answers
"how themed is my library, and what can I do?":

  • big number + bar = % of YOUR library themed (with_theme / total)
  • foot = "<X> of <Y> themed · <Z> ready to add"
    where ready = TDB-available rows not yet themed — exactly the gap
    motif's next run can close.

TV combines tv+anime (the card's long-standing universe — TDB doesn't
split anime). All values are derived from the plex_* coverage
aggregates already computed for the bottom PLEX cards (no new query)
and from the /api/coverage/plex arrays the JS already fetches.
"""
from __future__ import annotations

import re
from pathlib import Path

from jinja2 import Environment


REPO = Path(__file__).resolve().parent.parent
DASH = (REPO / "app" / "web" / "templates" / "dashboard.html").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()


def _strip_comments(html: str) -> str:
    return re.sub(r"\{#.*?#\}", "", html, flags=re.DOTALL)


def _card(label: str) -> str:
    visible = _strip_comments(DASH)
    idx = visible.index(label)
    return visible[visible.rfind("<article", 0, idx):visible.index("</article>", idx)]


# ── 1. Template: cards lead with coverage %, foot = themed/total + ready ──


def test_cards_relabelled_to_coverage_framing():
    visible = _strip_comments(DASH)
    assert "// MOVIES THEMED" in visible
    assert "// TV THEMED" in visible
    assert "// COLLECTIONS THEMED" in visible
    # The meaningless-catalogue framing is fully gone.
    assert "// THEMERRDB MOVIES" not in visible
    assert 'data-stat="movies.total"' not in visible
    assert "in your library" not in visible


def test_movies_card_headline_is_coverage_pct():
    card = _card("// MOVIES THEMED")
    assert 'id="cov-movies-pct"' in card
    assert ("ssr_pct(_ssr_dash.plex_movies_with_theme, "
            "_ssr_dash.plex_movies_total)") in card
    assert 'data-bar-fill="movies"' in card


def test_movies_card_foot_is_themed_of_total_plus_ready():
    card = _card("// MOVIES THEMED")
    assert 'id="cov-movies-themed"' in card and "plex_movies_with_theme" in card
    assert 'id="cov-movies-total"' in card and "plex_movies_total" in card
    assert 'id="cov-movies-ready"' in card and "plex_movies_ready" in card
    assert "themed" in card and "ready to add" in card


def test_tv_card_is_tv_only():
    # v1.23.40: TV is tv-only (anime is its own card) so it matches the
    # TV SHOWS library tab.
    card = _card("// TV THEMED")
    assert ("ssr_pct(_ssr_dash.plex_tv_with_theme, "
            "_ssr_dash.plex_tv_total)") in card
    assert "plex_tv_ready" in card
    assert "plex_tv_combined" not in card


def test_anime_card_present_and_separate():
    # v1.23.40: ANIME is its own coverage card matching the ANIME tab.
    card = _card("// ANIME THEMED")
    assert 'id="cov-anime-pct"' in card
    assert ("ssr_pct(_ssr_dash.plex_anime_with_theme, "
            "_ssr_dash.plex_anime_total)") in card
    assert "plex_anime_ready" in card


def test_collections_card_coverage_shape():
    card = _card("// COLLECTIONS THEMED")
    assert 'id="cov-collections-pct"' in card
    assert ("ssr_pct(_ssr_dash.plex_collections_with_theme, "
            "_ssr_dash.plex_collections_total)") in card
    assert "plex_collections_ready" in card


# ── 2. ssr_pct macro — behavioral render (the new arithmetic) ──


def _render_ssr_pct(num, denom):
    m = re.search(r"\{%- macro ssr_pct.*?endmacro -%\}", DASH, re.DOTALL)
    assert m, "ssr_pct macro must be defined in dashboard.html"
    tmpl = Environment().from_string(m.group(0) + "{{ ssr_pct(num, denom) }}")
    return tmpl.render(num=num, denom=denom).strip()


def test_ssr_pct_rounds_to_whole_percent():
    assert _render_ssr_pct(33, 100) == "33%"
    assert _render_ssr_pct(1, 3) == "33%"    # 33.33 → 33
    assert _render_ssr_pct(2, 3) == "67%"    # 66.66 → 67
    assert _render_ssr_pct(1, 1) == "100%"
    # Zero themed is a real 0%, NOT the "—" no-data fallback.
    assert _render_ssr_pct(0, 100) == "0%"


def test_ssr_pct_dash_on_missing_or_zero_denominator():
    assert _render_ssr_pct(None, 100) == "—"   # SSR error → no number
    assert _render_ssr_pct(5, 0) == "—"        # empty library → no %
    assert _render_ssr_pct(5, None) == "—"


# ── 3. SSR derivation (api.py) ───────────────────────────────


def test_ssr_ready_reads_placement_aware_aggregate():
    # v1.23.36: ready reads a placement-aware SSR aggregate — TDB-available
    # AND Plex-unthemed AND NOT placed. The old motif_avail−tdb_themed
    # subtraction inflated it by counting motif-placed-but-Plex-stale rows.
    assert 'state["plex_movies_ready"] = int(cov["plex_movies_ready"]' in API_PY
    assert "AS plex_movies_ready" in API_PY
    assert "NOT EXISTS (SELECT 1 FROM placements p" in API_PY
    # the inflating subtraction must be gone.
    assert ('state["plex_movies_motif_avail"] - state["tdb_movies_themed"]'
            not in API_PY)


def test_ssr_has_separate_tv_and_anime_ready_aggregates():
    # v1.23.40: TV and ANIME are separate cards — TV ready gates is_anime=0,
    # anime ready is_anime=1 (was one combined media_type='show' count).
    # v1.23.90: anime aggregates take media_type IN ('show','movie') so anime
    # films count on the ANIME card (matches the ANIME library tab).
    assert "AS plex_tv_ready" in API_PY
    assert "AS plex_anime_ready" in API_PY
    assert "pi.media_type='show' AND ps.is_anime=0" in API_PY
    assert "pi.media_type IN ('show', 'movie') AND ps.is_anime=1" in API_PY
    assert 'state["plex_anime_ready"] = int(cov["plex_anime_ready"]' in API_PY


# ── 4. JS setCov writes % + bar + foot from the coverage arrays ──


def test_js_setcov_writes_pct_bar_and_foot():
    body = APP_JS[APP_JS.index("const setCov = (key, themed, totalN, ready)"):][:900]
    assert "cov-${key}-pct" in body
    assert '[data-bar-fill="${key}"]' in body
    assert "themed / totalN * 100" in body
    assert "cov-${key}-themed" in body
    assert "cov-${key}-total" in body
    assert "cov-${key}-ready" in body


def test_js_coverage_calls_for_all_four_types():
    # v1.23.40: four separate cards — movies / tv (tv-only) / anime / collections.
    assert "setCov('movies', withTheme, total," in APP_JS
    assert "setCov('tv'," in APP_JS
    assert "setCov('anime'," in APP_JS
    assert "setCov('collections'," in APP_JS
    # TV is tv-only now; the old combined universe is gone.
    assert "data.tv.concat(data.anime || [])" not in APP_JS
    assert "data.tv.filter((m) => m.has_theme)" in APP_JS
    assert "animeList.filter((m) => m.has_theme)" in APP_JS


def test_js_old_catalogue_bar_and_foot_gone():
    assert "stats.movies.downloaded / stats.movies.total" not in APP_JS
    assert "#tdb-movies-in-library" not in APP_JS
