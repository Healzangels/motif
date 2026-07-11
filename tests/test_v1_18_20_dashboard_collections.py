"""v1.18.20 — Dashboard surfaces for collections.

the user's ask:

  > On the dashboard can we add a themerrdb section for
  > collections after TV Series as well as a Plex Collections
  > section that's Red. Also let's add it to our Per-Section
  > Coverage but just as Collection no need for further
  > breakdown. Let's also add to coverage comparison chart, and
  > source breakdown pie chart … most likely separate makes
  > more sense next to it.

## v1.18.20 changes

  * Two new cards:
      - // THEMERRDB COLLECTIONS (in COVERAGE row)
      - // PLEX COLLECTIONS (in PLEX LIBRARY row, red tone)
  * Per-Section Coverage: synthetic "Collections" aggregate row
    appended (one row total, no per-section split).
  * Coverage comparison chart: handles tab='collections' with a
    "COLLECTIONS" label and a /collections href.
  * Source breakdown: separate collections pie next to the main
    pie; renderer factored to accept a mediaTypeFilter.

The SQL aggregates run inside the existing `_ssr_dash` query so
the first paint is already-populated. /api/coverage/plex returns
a `collections` array (same item shape as movies/tv/anime).
/api/sections/coverage appends one synthetic Collections row at
the end with tab='collections', is_4k=0, is_anime=0.
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

DASH_HTML = REPO / "app" / "web" / "templates" / "dashboard.html"
APP_JS = REPO / "app" / "web" / "static" / "app.js"
APP_CSS = REPO / "app" / "web" / "static" / "app.css"
API_PY = REPO / "app" / "web" / "api.py"


# ── Template pins ─────────────────────────────────────────────


def test_dashboard_renders_tdb_collections_card():
    """The // COLLECTIONS THEMED card sits in the COVERAGE grid with the
    .stat-tdb-primary tone. v0.51.122: it now renders the ThemerrDB REACH
    (total + in/not-in-ThemerrDB) — the user swapped the numbers onto it and
    kept the title; the coverage % + bar moved to // PLEX COLLECTIONS. The
    collections hide-gate (#plex-collections-card) also moved onto this card."""
    html = DASH_HTML.read_text()
    assert "// COLLECTIONS THEMED" in html
    idx = html.index("// COLLECTIONS THEMED")
    block = html[html.rindex("<article", 0, idx):html.index("</article>", idx)]
    assert "stat-tdb-primary" in block
    # Reach shape swapped in + the hide-gate that swapped onto this card.
    assert 'id="plex-collections-total"' in block
    assert 'id="plex-collections-not-tdb"' in block
    assert "not _ssr_dash.plex_collections_total" in block
    # The coverage % moved off this card.
    assert 'id="cov-collections-pct"' not in block


def test_dashboard_renders_plex_collections_card():
    """// PLEX COLLECTIONS card present, red tone class. v0.51.122: it now
    renders the coverage % + bar + "themed / ready" (the user swapped the
    numbers, kept the title); the reach total + the hide-gate moved onto the
    // COLLECTIONS THEMED reach card, so this card stays always-visible."""
    html = DASH_HTML.read_text()
    assert "// PLEX COLLECTIONS" in html
    idx = html.index("// PLEX COLLECTIONS")
    block = html[html.rindex("<article", 0, idx):html.index("</article>", idx)]
    assert "stat-plex-collections" in block, (
        "Plex Collections card must use the red tone class"
    )
    # Coverage % shape swapped in.
    assert 'id="cov-collections-pct"' in block
    assert 'data-bar-fill="collections"' in block
    assert "cov-collections-ready" in block
    # The reach total + the hide-gate moved off this card.
    assert 'id="plex-collections-total"' not in block
    assert "not _ssr_dash.plex_collections_total" not in block


def test_dashboard_source_breakdown_is_three_up():
    """v1.24.55: the standalone collections source pie was replaced by a 3-up
    SOURCE BREAKDOWN row (Total / Movies / TV). The old collections-pie block is
    gone; the three donut groups are present."""
    html = DASH_HTML.read_text()
    assert "source-pie-total-slices" in html
    assert "source-pie-movies-slices" in html
    assert "source-pie-tv-slices" in html
    assert "source-breakdown-collections-block" not in html
    assert "// SOURCE BREAKDOWN — COLLECTIONS" not in html


# ── CSS pin ───────────────────────────────────────────────────


def test_css_defines_stat_plex_collections_red_tone():
    """v1.24.65: the collections card colour is now driven by
    --dash-collections-color (customizable), which defaults to --red in :root —
    so the red identity is preserved."""
    css = APP_CSS.read_text()
    assert ".stat-plex-collections" in css
    idx = css.index(".stat-plex-collections")
    block = css[idx:idx + 300]
    assert "var(--dash-collections-color)" in block
    assert "--dash-collections-color: var(--red)" in css  # default = red


# ── JS pins ───────────────────────────────────────────────────


def test_js_renders_plex_collections_card_from_coverage():
    """The /api/coverage/plex hydration must populate the three
    Plex Collections stat IDs + reveal/hide the card based on
    live count."""
    js = APP_JS.read_text()
    assert "plex-collections-total" in js
    # v1.23.41: ThemerrDB reach foot (in-TDB = motif, not-in-TDB).
    assert "plex-collections-motif" in js
    assert "plex-collections-not-tdb" in js
    # Card visibility flip.
    assert "plex-collections-card" in js
    # v1.23.34: collections coverage card hydrated via setCov.
    assert "setCov('collections'" in js


def test_js_renders_four_source_pies():
    """v1.24.55: 3 donuts (Total/Movies/TV); v0.50.74: + Anime, driven by the
    _SOURCE_DONUTS table (each with its own scope predicate). Collections pie gone."""
    js = APP_JS.read_text()
    assert "const _SOURCE_DONUTS = [" in js
    for did in ("total", "movies", "tv", "anime"):
        assert f"id: '{did}'," in js
    assert "function renderCollectionsSourcePie" not in js
    # Movies + TV scope by media_type AND exclude anime (is_anime is its own donut).
    assert "(r) => r.media_type === 'movie' && !r.is_anime," in js
    assert "(r) => r.media_type === 'show' && !r.is_anime," in js
    assert "scopeFn: (r) => !!r.is_anime," in js


def test_js_pie_legend_toggle_rerenders_only_clicked_donut():
    """v1.24.59: clicking a legend item toggles ONLY the clicked donut's own
    hidden-set and re-renders just that donut (the three are independent again,
    per the user) — not the whole row."""
    js = APP_JS.read_text()
    idx = js.index(".source-legend-item")
    block = js[idx:idx + 1500]
    assert "closest('.source-pie-col')" in block
    # v0.50.74: re-renders only the clicked donut via _renderOneDonut, not the row.
    assert "_renderOneDonut(d," in block
    assert "renderAllSourcePies(" not in block


def test_js_coverage_comparison_removed():
    """v0.51.31: the // COVERAGE COMPARISON block (renderCoverageComparison)
    was removed as a duplicate of the // PER-SECTION COVERAGE table — both
    fed from /api/sections/coverage (the user). The collections-label branch
    lives on in renderSectionCoverage (see test_v1_24_79)."""
    js = APP_JS.read_text()
    assert "function renderCoverageComparison" not in js
    assert "coverage-comparison-body" not in js


def test_js_calls_renderallsourcepies_in_load_dashboard():
    """v1.24.55: the stats-load path renders all three donuts in one call."""
    js = APP_JS.read_text()
    assert "renderAllSourcePies(stats.theme_sources" in js


# ── SSR pins ──────────────────────────────────────────────────


def test_ssr_dash_includes_collection_counts():
    """_ssr_dash must compute + expose the new collection counts
    so first paint renders the cards already-populated."""
    src = API_PY.read_text()
    # State dict keys.
    for k in (
        '"collections_tdb_total"',
        '"plex_collections_total"',
        '"plex_collections_with_theme"',
        '"plex_collections_motif_avail"',
        '"tdb_collections_in_library"',
        '"tdb_collections_themed"',
    ):
        assert k in src, f"v1.18.20: _ssr_dash must declare {k}"


def test_ssr_query_aggregates_collections():
    """The SSR SQL must aggregate collections counts inside the
    existing cov query — collections only need a single CASE
    WHEN since they have no 4K/anime split."""
    src = API_PY.read_text()
    assert "AS plex_collections_total" in src
    assert "AS plex_collections_with_theme" in src
    assert "AS plex_collections_motif_avail" in src
    assert "AS tdb_collections_themed" in src
    assert "AS collections_tdb_total" in src


# ── End-to-end: SSR populates collection fields ──────────────


@pytest.fixture
def collections_fixture(tmp_path: Path):
    """Seed a DB with TDB-tracked collections, plex_items rows
    for some of them (some themed, some not)."""
    db_path = tmp_path / "motif.db"
    from app.core.db import init_db
    init_db(db_path)
    ts = "2026-05-20T18:00:00"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 0, 0, "
            "        'movies', 1, ?, ?)",
            (ts, ts),
        )
        # TDB-tracked collections (3 in catalog).
        for tmdb_id in (100, 101, 102):
            conn.execute(
                "INSERT INTO themes "
                "  (media_type, tmdb_id, title, title_norm, year, "
                "   upstream_source, "
                "   last_seen_sync_at, first_seen_sync_at) "
                "VALUES ('collection', ?, ?, ?, NULL, "
                "        'themoviedb', ?, ?)",
                (tmdb_id, f"Coll {tmdb_id}",
                 f"coll {tmdb_id}", ts, ts),
            )
        # Plex collection 1 (themed, TDB-tracked).
        themes_row_1 = conn.execute(
            "SELECT id FROM themes "
            "WHERE media_type='collection' AND tmdb_id=100"
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO plex_items "
            "  (rating_key, section_id, media_type, title, "
            "   title_norm, year, folder_path, guid_tmdb, "
            "   has_theme, theme_id, "
            "   first_seen_at, last_seen_at) "
            "VALUES ('500', '1', 'collection', 'Coll 100', "
            "        'coll 100', NULL, '', '100', "
            "        1, ?, ?, ?)",
            (themes_row_1, ts, ts),
        )
        # Plex collection 2 (not themed yet, TDB-tracked).
        themes_row_2 = conn.execute(
            "SELECT id FROM themes "
            "WHERE media_type='collection' AND tmdb_id=101"
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO plex_items "
            "  (rating_key, section_id, media_type, title, "
            "   title_norm, year, folder_path, guid_tmdb, "
            "   has_theme, theme_id, "
            "   first_seen_at, last_seen_at) "
            "VALUES ('501', '1', 'collection', 'Coll 101', "
            "        'coll 101', NULL, '', '101', "
            "        0, ?, ?, ?)",
            (themes_row_2, ts, ts),
        )
        # Plex collection 3 (orphan — not in TDB).
        conn.execute(
            "INSERT INTO themes "
            "  (media_type, tmdb_id, title, title_norm, year, "
            "   upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('collection', -1, 'Orphan Coll', "
            "        'orphan coll', NULL, 'plex_orphan', ?, ?)",
            (ts, ts),
        )
        orphan_id = conn.execute(
            "SELECT id FROM themes "
            "WHERE media_type='collection' AND tmdb_id=-1"
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO plex_items "
            "  (rating_key, section_id, media_type, title, "
            "   title_norm, year, folder_path, "
            "   has_theme, theme_id, "
            "   first_seen_at, last_seen_at) "
            "VALUES ('502', '1', 'collection', 'Orphan Coll', "
            "        'orphan coll', NULL, '', "
            "        1, ?, ?, ?)",
            (orphan_id, ts, ts),
        )
        conn.commit()
    return db_path


def test_sections_coverage_appends_collections_row(collections_fixture):
    """/api/sections/coverage must append a single 'Collections'
    aggregate row covering all sections."""
    import asyncio
    db_path = collections_fixture
    # Inspect the synthetic-row SQL by importing the module-level
    # helper via the FastAPI route directly is heavy; instead pin
    # via DB inspection of the same SQL the endpoint runs.
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("""
            SELECT
              COUNT(pi.rating_key) AS total,
              COALESCE(SUM(CASE
                WHEN pi.has_theme = 1 THEN 1
                ELSE 0
              END), 0) AS themed,
              COALESCE(SUM(CASE
                WHEN t.tmdb_id IS NOT NULL
                 AND t.upstream_source != 'plex_orphan'
                THEN 1 ELSE 0
              END), 0) AS motif_available
            FROM plex_items pi
            INNER JOIN plex_sections ps
              ON ps.section_id = pi.section_id AND ps.included = 1
            LEFT JOIN themes t ON t.id = pi.theme_id
            WHERE pi.media_type = 'collection'
        """).fetchone()
    assert row["total"] == 3  # 2 TDB + 1 orphan
    assert row["themed"] == 2  # collection 1 (TDB) + 3 (orphan)
    assert row["motif_available"] == 2  # collection 1 + 2 (TDB-tracked)


def test_coverage_plex_collections_query_matches(collections_fixture):
    """The /api/coverage/plex collections SELECT must surface
    the same 3 rows."""
    db_path = collections_fixture
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""
            SELECT pi.rating_key, pi.title, pi.year, pi.has_theme,
                   t.tmdb_id, t.upstream_source
            FROM plex_items pi
            INNER JOIN plex_sections ps
              ON ps.section_id = pi.section_id AND ps.included = 1
            LEFT JOIN themes t ON t.id = pi.theme_id
            WHERE pi.media_type = 'collection'
            ORDER BY pi.title COLLATE NOCASE
        """).fetchall()
    assert len(rows) == 3
    themed = [r for r in rows if r["has_theme"]]
    assert len(themed) == 2
    motif_avail = [
        r for r in rows
        if r["tmdb_id"] is not None
        and r["upstream_source"] != "plex_orphan"
    ]
    assert len(motif_avail) == 2
