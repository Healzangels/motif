"""v0.51.121 — swap the dashboard COVERAGE ↔ PLEX reach metrics in place.

the user, on a screenshot of the two top dashboard stat rows: "Can we swap
the info in the themerrdb cards and the plex cards", then chose "Swap the
metrics in place — the top row shows the Plex total + in/not-in-ThemerrDB
numbers, and the bottom row shows the themed % + 'X of Y themed'".

## The swap

The two `.grid-stats` sections are the top-two-rows fold (v1.18.50). Before
the tag:
  * top-stats  section (top)    → COVERAGE % cards (stat-tdb-primary, cov-*)
  * plex-coverage section (btm) → PLEX reach cards (stat-plex-*, plex-*-total)

After:
  * top-stats  section (top)    → PLEX reach cards
  * plex-coverage section (btm) → COVERAGE % cards

## Why the section shells stay put (not swapped)

dashboard-customize.js reorders `#dash-sections` children BY
`data-dash-section` id against a saved layout. Physically swapping the two
`<section>` blocks would be undone for any user whose saved layout lists
top-stats first. So the shells keep their ids + DOM order and only the card
BODIES swap — "PLEX reach on top" then holds for every user, customized or
not. Each body travels whole (tone class, anime/collections display:none
gating, JS-target IDs, plex-foot CSS) so nothing detaches.

## Count-up relocation

The parser-blocking count-up inline script resets cov-*-pct → 0% + stashes
data-countup so dashCountUp climbs. It reads the cov-* elements by
getElementById, so it MUST sit after they parse. The cov-* cards swapped
into the second (plex-coverage) section, so the script moved below that
section (it used to sit between the two).

## LIBRARY COLORS panel anchor

dashboard-customize.js injects the // LIBRARY COLORS panel next to the PLEX
(colored, --dash-*-color) cards it controls. Those cards moved into
top-stats, so both the inject + reposition anchors repoint from
plex-coverage → top-stats.

Source-order + presence guards (no JS runtime for the template in CI).
"""
from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
DASH = REPO / "app" / "web" / "templates" / "dashboard.html"
CUSTOMIZE = REPO / "app" / "web" / "static" / "dashboard-customize.js"
APP_INIT = REPO / "app" / "__init__.py"


def _section_body(html: str, section_id: str) -> str:
    """Return the `<section ...>…</section>` block whose data-dash-section
    equals section_id."""
    marker = f'data-dash-section="{section_id}"'
    mpos = html.index(marker)
    start = html.rindex("<section", 0, mpos)
    end = html.index("</section>", mpos)
    return html[start:end]


# ── The two sections show the swapped metrics ─────────────────


def test_top_stats_section_now_holds_plex_reach_cards():
    """top-stats (the top row) renders the PLEX reach cards + the PLEX
    LIBRARY label; the COVERAGE cards must NOT be in it."""
    body = _section_body(DASH.read_text(), "top-stats")
    assert 'data-dash-label="PLEX LIBRARY"' in body
    # PLEX reach cards present.
    assert 'data-dash-card="plex-movies"' in body
    assert 'id="plex-movies-total"' in body
    assert "in ThemerrDB" in body and "not in ThemerrDB" in body
    # COVERAGE cards must have left.
    assert 'data-dash-card="tdb-movies"' not in body
    assert 'id="cov-movies-pct"' not in body


def test_plex_coverage_section_now_holds_coverage_cards():
    """plex-coverage (the bottom row) renders the COVERAGE % cards + the
    COVERAGE label; the PLEX reach cards must NOT be in it."""
    body = _section_body(DASH.read_text(), "plex-coverage")
    assert 'data-dash-label="COVERAGE"' in body
    # COVERAGE cards present.
    assert 'data-dash-card="tdb-movies"' in body
    assert 'id="cov-movies-pct"' in body
    assert "themed</span>" in body and "ready to add</span>" in body
    # PLEX reach cards must have left.
    assert 'data-dash-card="plex-movies"' not in body
    assert 'id="plex-movies-total"' not in body


def test_section_ids_keep_their_dom_order():
    """The section SHELLS (ids) must NOT swap — top-stats stays first,
    plex-coverage second — so a saved customize-layout keeping top-stats
    first still renders PLEX reach on top. Only the bodies swapped."""
    html = DASH.read_text()
    assert html.index('data-dash-section="top-stats"') < html.index(
        'data-dash-section="plex-coverage"'
    ), "top-stats must stay ahead of plex-coverage in the DOM."


# ── Anime/collections gating travelled with the plex cards ────


def test_plex_anime_collections_gating_in_top_stats():
    """The display:none gating + reveal-target ids (#plex-anime-card /
    #plex-collections-card) must live in the top-stats section now — the
    gated cards moved there as whole articles, so renderPlexCoverage's
    getElementById reveal still finds them."""
    body = _section_body(DASH.read_text(), "top-stats")
    assert 'id="plex-anime-card"' in body
    assert 'id="plex-collections-card"' in body
    assert "_ssr_dash.plex_anime_total" in body  # the {% if not ... %} gate
    assert "_ssr_dash.plex_collections_total" in body


# ── Count-up script runs AFTER the cov-* cards ────────────────


def test_countup_script_below_the_coverage_cards():
    """The parser-blocking count-up must sit AFTER the cov-*-pct cards so
    getElementById finds them; otherwise the reset is a no-op and the % never
    climbs (it would paint the static SSR value)."""
    html = DASH.read_text()
    last_cov_card = html.rindex('id="cov-collections-ready"')
    countup = html.index("document.getElementById('cov-' + k + '-pct')")
    assert countup > last_cov_card, (
        "count-up script must run after the cov-* cards parse — it moved "
        "below the COVERAGE section when the cards swapped down."
    )


# ── Every live-update id survived the move ────────────────────


def test_all_live_update_ids_present():
    """renderPlexCoverage/setCov + the reveal logic write these by id —
    a swap that dropped one would silently freeze that card at its SSR
    value. Pin the full set."""
    html = DASH.read_text()
    for _id in (
        "plex-movies-total", "plex-tv-total", "plex-anime-total", "plex-collections-total",
        "plex-movies-motif", "plex-movies-not-tdb",
        "plex-anime-card", "plex-collections-card",
        "cov-movies-pct", "cov-tv-pct", "cov-anime-pct", "cov-collections-pct",
        "cov-movies-themed", "cov-movies-total", "cov-collections-ready",
    ):
        assert html.count(f'id="{_id}"') == 1, f"{_id} missing/duplicated after swap"


# ── LIBRARY COLORS panel anchor followed the plex cards ───────


def test_color_panel_anchors_on_top_stats():
    """injectColorPanel + repositionColorPanel must anchor the // LIBRARY
    COLORS panel on top-stats (where the colored --dash-*-color plex cards
    moved), not the old plex-coverage."""
    js = CUSTOMIZE.read_text()
    anchors = re.findall(
        r"querySelector\('\[data-dash-section=\"([a-z-]+)\"\]'\)", js
    )
    # Both color-panel anchors resolve to top-stats now.
    assert anchors.count("top-stats") >= 2, (
        "both color-panel section anchors must query top-stats"
    )
    # No color-panel anchor left pointing at plex-coverage. (A comment
    # mentioning the id is fine — only the querySelector args count.)
    assert "plex-coverage" not in "".join(anchors)


# ── Version pin ───────────────────────────────────────────────


def test_version_pinned_at_or_above_0_51_121():
    m = re.search(r'__version__\s*=\s*"(\d+)\.(\d+)\.(\d+)"', APP_INIT.read_text())
    assert m
    assert tuple(int(x) for x in m.groups()) >= (0, 51, 121)
