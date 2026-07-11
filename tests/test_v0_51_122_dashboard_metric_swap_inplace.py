"""v0.51.122 — swap the dashboard stat metrics IN PLACE (robust redo of v0.51.121).

v0.51.121 swapped the card BODIES between the two `.grid-stats` sections. But
dashboard-customize.js `applyLayout` moves cards into sections BY their
`data-dash-card` id to match a saved layout, so a user with a saved customize
layout had the cards moved right back on load — while the section-LABEL swap
stuck. Result: swapped section labels + un-swapped cards (the user's screenshot:
"the info on the cards doesn't look like it swapped").

## The robust fix

Swap only the NUMBERS, in place. Each card keeps its title, tone, section, and
`data-dash-card` id — only the big number / bar / foot / live-update ids swap:

  * The // …THEMED cards (top-stats section, tdb-* ids) keep their titles but
    now render the ThemerrDB REACH (library total + in/not-in-ThemerrDB) that
    used to sit on the PLEX cards.
  * The // PLEX … cards (plex-coverage section, plex-* ids) keep their titles
    but now render the COVERAGE % + bar + "X of Y themed · Z ready to add".

Because the cards never move by id, the saved customize layout can't undo it
(applyLayout finds tdb-* already in top-stats, plex-* already in plex-coverage —
a no-op). The user chose "keep titles" — so // MOVIES THEMED intentionally shows
a total and // PLEX MOVIES shows a %.

The anime/collections hide-gate (`display:none` + `#plex-anime-card` /
`#plex-collections-card` reveal id) moved onto the REACH cards (renderPlexCoverage
toggles those ids by presence + writes plex-*-total into them). The wide-reach
foot column-stack moved from `[data-dash-card^="plex-"]` to an explicit
`.stat-foot-stack` class. The count-up moved below the PLEX section (cov-*-pct
swapped down there). v0.51.121's section-label + color-panel changes were
reverted (the user: "names + color control back where they were").
"""
from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
DASH = REPO / "app" / "web" / "templates" / "dashboard.html"
CSS = REPO / "app" / "web" / "static" / "app.css"
CUSTOMIZE = REPO / "app" / "web" / "static" / "dashboard-customize.js"
APP_INIT = REPO / "app" / "__init__.py"


def _section_body(html: str, section_id: str) -> str:
    marker = f'data-dash-section="{section_id}"'
    mpos = html.index(marker)
    start = html.rindex("<section", 0, mpos)
    end = html.index("</section>", mpos)
    return html[start:end]


# ── The THEMED cards now show reach; the PLEX cards show % ─────


def test_themed_cards_show_reach_numbers():
    """The // …THEMED cards (top-stats) keep their titles but render the
    ThemerrDB reach (total + in/not-in-ThemerrDB) — NOT the % anymore."""
    body = _section_body(DASH.read_text(), "top-stats")
    assert 'data-dash-label="COVERAGE"' in body           # section label reverted
    assert "// MOVIES THEMED" in body                      # title kept
    assert 'id="plex-movies-total"' in body                # reach big number
    assert "in ThemerrDB" in body and "not in ThemerrDB" in body
    # The % + bar + ready-to-add must NOT be here anymore.
    assert 'id="cov-movies-pct"' not in body
    assert "ready to add" not in body
    assert 'data-bar-fill' not in body                     # no bar on reach cards


def test_plex_cards_show_coverage_pct():
    """The // PLEX … cards (plex-coverage) keep their titles but render the
    coverage % + bar + 'X of Y themed · Z ready to add' — NOT the reach total."""
    body = _section_body(DASH.read_text(), "plex-coverage")
    assert 'data-dash-label="PLEX LIBRARY"' in body        # section label reverted
    assert "// PLEX MOVIES" in body                         # title kept
    assert 'id="cov-movies-pct"' in body                    # % big number
    assert 'data-bar-fill="movies"' in body                 # bar present
    assert "themed</span>" in body and "ready to add" in body
    # The reach total + in/not-in-ThemerrDB must NOT be here anymore.
    assert 'id="plex-movies-total"' not in body
    assert "in ThemerrDB" not in body


def test_titles_kept_in_original_positions():
    """the user: keep the names where they were. The green THEMED titles stay
    in top-stats, the PLEX titles stay in plex-coverage."""
    html = DASH.read_text()
    top = _section_body(html, "top-stats")
    bot = _section_body(html, "plex-coverage")
    for t in ("// MOVIES THEMED", "// TV THEMED", "// ANIME THEMED", "// COLLECTIONS THEMED"):
        assert t in top
    for t in ("// PLEX MOVIES", "// PLEX TV", "// PLEX ANIME", "// PLEX COLLECTIONS"):
        assert t in bot


# ── Robustness: card ids stay in their home sections ──────────


def test_card_ids_stay_in_home_sections():
    """The crux of the robust fix: data-dash-card ids do NOT move. tdb-* stay
    in top-stats, plex-* stay in plex-coverage — so dashboard-customize's
    applyLayout (which moves cards into sections by id to match a saved layout)
    is a no-op and can't revert the swap."""
    html = DASH.read_text()
    top = _section_body(html, "top-stats")
    bot = _section_body(html, "plex-coverage")
    for cid in ("tdb-movies", "tdb-tv-series", "tdb-anime", "tdb-collections"):
        assert f'data-dash-card="{cid}"' in top
        assert f'data-dash-card="{cid}"' not in bot
    for cid in ("plex-movies", "plex-tv", "plex-anime", "plex-collections"):
        assert f'data-dash-card="{cid}"' in bot
        assert f'data-dash-card="{cid}"' not in top


# ── The anime/collections hide-gate moved to the reach cards ──


def test_hide_gate_on_reach_cards():
    """The display:none gate + #plex-anime-card / #plex-collections-card reveal
    ids must live on the reach (tdb-*) cards now — renderPlexCoverage toggles
    those ids by anime/collections presence + writes plex-*-total into them."""
    html = DASH.read_text()
    top = _section_body(html, "top-stats")
    bot = _section_body(html, "plex-coverage")
    # Gate on the reach cards, wired to the tdb-* article.
    assert 'id="plex-anime-card" data-dash-card="tdb-anime"' in top
    assert 'id="plex-collections-card" data-dash-card="tdb-collections"' in top
    assert "_ssr_dash.plex_anime_total" in top          # the {% if not %} gate
    assert "_ssr_dash.plex_collections_total" in top
    # The % cards must NOT carry the reveal id (they stay always-visible).
    assert 'id="plex-anime-card"' not in bot
    assert 'id="plex-collections-card"' not in bot


# ── Count-up runs after the cov-* cards (now in plex-coverage) ─


def test_countup_below_the_plex_section():
    html = DASH.read_text()
    last_cov = html.rindex('id="cov-collections-ready"')
    countup = html.index("document.getElementById('cov-' + k + '-pct')")
    assert countup > last_cov, "count-up must run after the cov-* cards parse"


# ── Wide-reach foot column-stack decoupled from data-dash-card ─


def test_reach_foot_uses_stat_foot_stack_class():
    """The 4 reach foots carry .stat-foot-stack; the CSS column rule keys on
    that class, not [data-dash-card^="plex-"] (which now holds the % cards)."""
    html = DASH.read_text()
    assert html.count("stat-foot stat-foot-stack") == 4
    css = CSS.read_text()
    assert ".stat-foot-stack {" in css
    # The old data-dash-card-scoped stack rule must be gone (a comment mentioning
    # it is fine; the SELECTOR must not remain).
    assert '[data-dash-card^="plex-"] .stat-foot {' not in css


# ── Section labels + color panel reverted to pre-v0.51.121 ────


def test_sections_and_color_panel_reverted():
    html = DASH.read_text()
    # top-stats is COVERAGE, plex-coverage is PLEX LIBRARY (v0.51.121 swap undone).
    assert html.index('data-dash-section="top-stats"') < html.index(
        'data-dash-section="plex-coverage"')
    top = _section_body(html, "top-stats")
    assert 'data-dash-label="COVERAGE"' in top
    # Color panel anchors back on plex-coverage in BOTH inject + reposition.
    js = CUSTOMIZE.read_text()
    anchors = re.findall(
        r"querySelector\('\[data-dash-section=\"([a-z-]+)\"\]'\)", js)
    assert anchors.count("plex-coverage") >= 2
    assert "top-stats" not in anchors


# ── Every live-update id survived ─────────────────────────────


def test_all_live_update_ids_present():
    html = DASH.read_text()
    for _id in (
        "plex-movies-total", "plex-tv-total", "plex-anime-total", "plex-collections-total",
        "plex-movies-motif", "plex-movies-not-tdb",
        "plex-anime-card", "plex-collections-card",
        "cov-movies-pct", "cov-tv-pct", "cov-anime-pct", "cov-collections-pct",
        "cov-movies-themed", "cov-movies-total", "cov-collections-ready",
    ):
        assert html.count(f'id="{_id}"') == 1, f"{_id} missing/duplicated"


def test_version_pinned_at_or_above_0_51_122():
    m = re.search(r'__version__\s*=\s*"(\d+)\.(\d+)\.(\d+)"', APP_INIT.read_text())
    assert m
    assert tuple(int(x) for x in m.groups()) >= (0, 51, 122)
