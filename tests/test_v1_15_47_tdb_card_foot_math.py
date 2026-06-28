"""v1.15.47 — fix TDB top-card foot math (themed > total impossible
+ anime missing from TV count).

the user (screenshot: TDB TV SERIES card showing 930 total, 377 in
library, 1,842 themed): "the themerdb available under tv and
under anime don't make sense with the themerrdb tv series card
above the numbers don't add up."

Two compounding bugs surfaced by the user's screenshot:

## Bug A — "themed" counted ALL Plex-themed (not TDB-tracked)

The TDB card foot's "themed" value used `has_theme` directly,
which is True for ANY Plex item with a theme — including M
(user-uploaded sidecars), U (user URL overrides), and P (Plex's
own embed/cloud) themes that aren't TDB-tracked. For the TDB
card the semantic should be "of TDB's catalog, how many are
themed *via TDB-trackable means*" — strictly ≤ "in your library"
since you can't TDB-theme a row TDB doesn't know about.

For TV the bug was loud: 1,842 themed > 930 TDB-tracked total
(mathematically impossible under any sane reading of the label).
For movies it was masked because 3,276 themed < 4,382 TDB total
(numbers happened to be plausible by coincidence).

Fix: count `motif_available AND has_theme` (TDB-tracked AND
themed). Now strictly ≤ in_library.

## Bug B — TDB TV SERIES card omitted anime

v1.15.26 correctly split anime out of /api/coverage/plex's `tv`
array (so PLEX TV and PLEX ANIME cards don't double-count). But
TDB tracks anime as media_type='tv' — from TDB's POV, "TV
SERIES in your library" includes anime matches. Pre-v1.15.47
the TDB card foot used only non-anime TV, undercounting by the
anime population (266 in the user's library = 41% miss).

Fix: extend /api/coverage/plex to return an `anime` array (same
item shape as `tv`); JS combines (tv + anime) for the TDB TV
SERIES foot computations. PLEX TV / PLEX ANIME cards stay
single-section (correct).

Static-text guards (consistent with v1.15.26 split-test patterns).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"
APP_JS = REPO / "app" / "web" / "static" / "app.js"


# ── 1. Coverage endpoint exposes anime array ─────────────────


def test_api_coverage_plex_queries_anime_rows():
    """/api/coverage/plex must run a third SELECT for anime
    sections (pi.media_type='show' AND ps.is_anime=1). Without
    it, the JS combine has nothing to fold in and bug B
    silently returns."""
    src = API_PY.read_text()
    # Find the api_coverage_plex handler.
    anchor = src.index("async def api_coverage_plex(")
    body = src[anchor:anchor + 8500]
    assert "anime_rows = conn.execute(" in body, (
        "v1.15.47: /api/coverage/plex must execute a separate "
        "anime_rows query — same shape as tv_rows but is_anime=1"
    )
    # Anchor on the anime query and check it gates on is_anime=1.
    anime_q_anchor = body.index("anime_rows = conn.execute(")
    anime_q = body[anime_q_anchor:anime_q_anchor + 1300]
    assert "ps.is_anime = 1" in anime_q, (
        "v1.15.47: anime query must filter ps.is_anime=1 (NOT 0) — "
        "mirror image of the v1.15.26 TV-side gate"
    )
    assert "pi.media_type IN ('show', 'movie')" in anime_q, (
        "v1.23.90: anime query takes media_type IN ('show','movie') — anime "
        "sections hold films too (anime movies/OVAs), matching the ANIME "
        "library tab (v1.19.28). Pre-tag it was 'show'-only (the 1,244 bug)."
    )


def test_api_coverage_plex_response_includes_anime_key():
    """The response dict must include the `anime` key so the JS
    `data.anime` lookup doesn't fall through to a no-op. v1.15.47
    contract: `{enabled, movies, tv, anime}`."""
    src = API_PY.read_text()
    anchor = src.index("async def api_coverage_plex(")
    # v1.23.96 widened 8500→10000: the run_in_threadpool offload added the _run()
    # wrapper + comment, pushing the response dict past the old window.
    body = src[anchor:anchor + 10000]
    # Anchor on the LAST `return {` (the real response — the
    # first `return {` is the early-exit when plex is disabled).
    return_block = body[body.rindex("return {"):body.rindex("return {") + 600]
    assert '"anime": [to_item(r) for r in anime_rows]' in return_block, (
        "v1.15.47: response must include anime array — keyed off "
        "to_item(r) so it's the same shape as the existing tv/movies "
        "arrays (drop-in for JS combine)"
    )


# ── 2. JS combines anime + tv, uses motif_available+has_theme ──


# v1.23.34 NOTE: the TDB-tracked "in library / themed" foot these
# tests guarded was reworked into a library-coverage card (the user:
# the old numbers "don't mean much"). The tv+anime combine survives
# (TDB still doesn't split anime); the "themed ≤ in-library"
# invariant intentionally does not (the foot "themed" is now whole-
# library has_theme, any source). The four JS tests below now guard
# the new coverage math.


def test_js_tv_and_anime_are_separate_coverage_cards():
    """v1.23.40: TV is tv-only (data.tv) + ANIME is its own card (animeList)
    so each matches its library tab; the v1.15.47/v1.23.34 combined universe
    is retired."""
    js = APP_JS.read_text()
    assert "data.tv.concat(data.anime || [])" not in js
    assert "const animeList = data.anime || [];" in js
    assert "setCov('tv'," in js
    assert "setCov('anime'," in js


def test_js_ready_to_add_excludes_already_themed():
    """v1.23.38: "ready to add" excludes already-themed/placed/dead-URL rows
    via the centralized `isAddable` predicate; all four cards route through it."""
    js = APP_JS.read_text()
    assert "const isAddable = (m) =>" in js
    assert "data.movies.filter(isAddable)" in js
    assert "data.tv.filter(isAddable)" in js
    assert "animeList.filter(isAddable)" in js
    # the predicate still ANDs motif_available + !has_theme (in isAddable).
    i = js.index("const isAddable = (m) =>")
    assert "m.motif_available && !m.has_theme" in js[i:i + 320]


def test_js_foot_themed_is_whole_library_coverage():
    """v1.23.34: the headline % + foot "themed" measure whole-
    library coverage (any-source has_theme / total), not the old
    TDB-tracked subset. The #tdb-*-themed setters are gone; the
    movies foot themed = the library has_theme count (withTheme)."""
    js = APP_JS.read_text()
    assert "#tdb-movies-themed" not in js
    assert "#tdb-tv-themed" not in js
    assert "setCov('movies', withTheme, total," in js


def test_js_collections_coverage_mirrors_movies_and_tv():
    """v1.23.34: collections coverage uses the same setCov shape. v1.23.38:
    its ready count routes through the shared isAddable predicate."""
    js = APP_JS.read_text()
    assert "setCov('collections'" in js
    assert "collectionsList.filter(isAddable)" in js
