"""v1.15.19 — dashboard rework: TDB labels, ANIME tone, derived
ANIME ThemerrDB-available stat.

the user v1.15.16 planning:
- "Movies Tracked and TV series tracked Don't indicate that's
  themerdb tracked themes"
- "the downloaded and placed while handy at a glance it doesn't
  really mean very much"
- "with theme and motif available while cool to know it doesn't
  really mean very much ... doesn't clearly make it known that
  in your library themerdb has that many themes available that
  match"
- "Plex TV, Plex anime again total and with theme is cool but
  since themerrdb doesn't classify shows as anime ... we're
  currently not displaying anything. I think we could find that
  information by filter on the anime library and TDB Pill as
  available."
- "I'd like to also add some more color ... using the colors and
  the color definitions we've used throughout for consistency"
- ANIME tone choice: magenta/pink (from the v1.15.16 question)

## Pre-fix

- Top cards: "MOVIES TRACKED" / "TV SERIES TRACKED" — labels
  didn't say "by ThemerrDB" so the numbers (themes catalogue
  size) read as motif library counts. Sub-stats were
  "downloaded / placed" (motif filesystem ops) — accurate
  but not actionable at-a-glance.
- PLEX cards: "motif available" — phrasing didn't make clear
  that the count = "ThemerrDB has a theme for these items in
  your library."
- PLEX ANIME card: third stat hardcoded "—" because
  /api/coverage/plex only returned movies + tv splits.
- Color: only PLEX MOVIES had a left-bar tone (amber); the
  other four cards had no tone at all.

## Fix

1. Top cards relabelled "// THEMERRDB MOVIES" / "// THEMERRDB
   TV SERIES" (with the // prefix matching the existing UI
   convention). Sub-stats reframed: "downloaded / placed" →
   "X in your library · Y themed" — derived client-side from
   the existing /api/coverage/plex motif_available + has_theme
   per-item flags.
2. PLEX cards' "motif available" → "ThemerrDB available"
   across all three (movies, tv, anime).
3. PLEX ANIME's "ThemerrDB available" derived from the new
   motif_available aggregate added to /api/sections/coverage
   SQL — same predicate /api/coverage/plex uses per-item, just
   summed at SQL time per section.
4. Color tones: top cards get .stat-tdb-primary (orange,
   matching // SYNC THEMERRDB family), PLEX TV gets
   .stat-plex-tv (green), PLEX ANIME gets .stat-plex-anime
   (magenta — the user's pick).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
DASHBOARD_HTML = REPO / "app" / "web" / "templates" / "dashboard.html"
APP_CSS = REPO / "app" / "web" / "static" / "app.css"
APP_JS = REPO / "app" / "web" / "static" / "app.js"
API_PY = REPO / "app" / "web" / "api.py"


def _strip_comments(html: str) -> str:
    """Strip both `{# ... #}` Jinja comments and `<!-- ... -->`
    HTML comments. Tests that anchor on rendered content (or
    assert "old phrasing must be gone") need to ignore
    historical-reference mentions in v1.15.19 marker comments
    AND in the older v1.14.70 HTML comment that documented the
    section split."""
    import re
    html = re.sub(r"\{#.*?#\}", "", html, flags=re.DOTALL)
    html = re.sub(r"<!--.*?-->", "", html, flags=re.DOTALL)
    return html


# Backwards-compat alias — older test draft used this name.
_strip_jinja_comments = _strip_comments


# ── 1. Top THEMERRDB cards: labels + sub-stats ────────────────


def test_top_themerrdb_cards_present_with_v1_15_19_labels():
    """v1.15.19 introduced // THEMERRDB MOVIES / // THEMERRDB
    TV SERIES labels (// prefix matches the rest of the UI's
    convention). v1.15.27 dropped them; v1.15.29 brought them
    back (the user walked back the v1.15.27 drop). The labels
    are once again present in rendered content. Historical
    guard: pre-v1.15.19 "MOVIES TRACKED" / "TV SERIES TRACKED"
    labels (no // prefix) must stay gone."""
    raw = DASHBOARD_HTML.read_text()
    visible = _strip_comments(raw)
    # Pre-v1.15.19 labels stay gone.
    assert "MOVIES TRACKED" not in visible
    assert "TV SERIES TRACKED" not in visible
    # v1.23.34 relabelled these to the coverage framing.
    assert "// MOVIES" in visible
    assert "// TV" in visible


def test_tdb_primary_tone_preserved_on_top_cards():
    """v1.15.19 introduced .stat-tdb-primary (orange left bar,
    matching // SYNC THEMERRDB family). v1.15.27 routed the
    tone onto a // THEMERRDB CATALOG mini-card; v1.15.29
    restored the top-card layout so the tone is back on the
    bigger // THEMERRDB MOVIES + TV SERIES cards. The visual
    association with the // SYNC THEMERRDB action stays intact
    either way — anchor on whichever cards currently carry
    the .stat-tdb-primary class."""
    html = _strip_comments(DASHBOARD_HTML.read_text())
    movies_anchor = html.index("// MOVIES")
    movies_card_start = html.rfind("<article", 0, movies_anchor)
    movies_card = html[movies_card_start:movies_anchor]
    assert "stat-tdb-primary" in movies_card


# ── 2. PLEX cards: relabelled stat + new tone classes ─────────


def test_plex_cards_relabel_motif_available_to_themerrdb_available():
    """All PLEX cards must use TDB-anchored phrasing. Pre-v1.15.19
    said 'motif available' which the user flagged as unclear; v1.15.19
    renamed to 'ThemerrDB available'. v1.23.41 reframed the foot to
    ThemerrDB REACH — "in ThemerrDB / not in ThemerrDB" (still TDB-
    anchored) so the PLEX row stops restating the top card's themed
    count. The invariant: 'motif available' must stay gone, and every
    card's foot is TDB-anchored."""
    raw = DASHBOARD_HTML.read_text()
    visible = _strip_jinja_comments(raw)
    # One "not in ThemerrDB" per PLEX card (movies/tv/anime/collections).
    assert visible.count("not in ThemerrDB") >= 4
    # Pre-v1.15.19 phrasing must be gone from rendered content.
    assert "motif available" not in visible


def test_plex_tv_card_carries_dedicated_tone():
    """v1.15.19 added .stat-plex-tv (green left bar) so the PLEX
    TV card has the same visual weight as PLEX MOVIES (amber).
    v1.15.29 changed the underlying color to --blue (the user's
    pick) but the .stat-plex-tv class anchor is unchanged —
    pin the class so the tone stays attached to the card
    regardless of the color value."""
    html = DASHBOARD_HTML.read_text()
    tv_anchor = html.index("// TV THEMED")
    tv_card_start = html.rfind("<article", 0, tv_anchor)
    tv_card = html[tv_card_start:tv_anchor + 200]
    assert "stat-plex-tv" in tv_card


def test_plex_anime_card_uses_magenta_tone():
    """ANIME tone is magenta per the user's pick — distinct from
    amber (movies), green (tv), red (failure), blue (TDB ↑)."""
    html = DASHBOARD_HTML.read_text()
    anime_anchor = html.index("// ANIME THEMED")
    anime_card_start = html.rfind("<article", 0, anime_anchor)
    anime_card = html[anime_card_start:anime_anchor + 200]
    assert "stat-plex-anime" in anime_card


def test_new_tone_classes_defined_in_css():
    """The .stat-tdb-primary, .stat-plex-tv, and
    .stat-plex-anime CSS rules must exist with their assigned
    color variables. Pin both the rule + the var() reference.
    v1.15.29 changed .stat-plex-tv from --green to --blue
    (the user's pick); the v1.15.29-specific test pins the new
    color exclusively."""
    src = APP_CSS.read_text()
    # v1.15.72: tdb-primary recolored --orange → --green-bright
    # (matches the canonical link-badge-themerrdb tone). The orange
    # tone was supposed to track // SYNC THEMERRDB but that button
    # has no special class — it renders default green. v1.15.72's
    # own test pins the green-bright tone specifically.
    assert ".stat-tdb-primary" in src
    tdb_anchor = src.index(".stat-tdb-primary")
    tdb_block = src[tdb_anchor:tdb_anchor + 200]
    assert "var(--src-t-bright)" in tdb_block
    # plex-tv has its own dedicated tone (color value pinned by
    # the v1.15.29 test).
    assert ".stat-plex-tv" in src
    # plex-anime uses --magenta. v1.24.65: via --dash-anime-color (defaults to
    # --magenta in :root).
    assert ".stat-plex-anime" in src
    anime_anchor = src.index(".stat-plex-anime")
    anime_block = src[anime_anchor:anime_anchor + 200]
    assert "var(--dash-anime-color)" in anime_block
    assert "--dash-anime-color: var(--magenta)" in src


# ── 3. /api/sections/coverage SQL aggregate ───────────────────


def test_sections_coverage_has_motif_available_aggregate():
    """The new motif_available aggregate must live in the
    sections/coverage SQL. Mirrors /api/coverage/plex's
    per-item motif_available predicate (theme has tmdb_id +
    upstream_source != plex_orphan), summed per section."""
    src = API_PY.read_text()
    fn_anchor = src.index('@app.get("/api/sections/coverage")')
    fn_end = src.index('@app.get(', fn_anchor + 1)
    fn_body = src[fn_anchor:fn_end]
    assert "AS motif_available" in fn_body, (
        "v1.15.19: sections/coverage must SUM(motif_available) "
        "per section so the PLEX ANIME card can derive its third stat"
    )
    # The predicate matches the per-item version in
    # /api/coverage/plex (tmdb_id IS NOT NULL AND upstream_source
    # != 'plex_orphan').
    assert "t.tmdb_id IS NOT NULL" in fn_body
    assert "upstream_source != 'plex_orphan'" in fn_body


# ── 4. JS hydration ────────────────────────────────────────────


def test_js_writes_top_card_coverage_substats():
    """v1.23.34: the top-card foot stats are written by setCov to
    `cov-{key}-themed` / `-total` / `-ready` (themed of total +
    ready-to-add), superseding the tdb-*-in-library / -themed IDs."""
    src = APP_JS.read_text()
    assert "cov-${key}-themed" in src
    assert "cov-${key}-total" in src
    assert "cov-${key}-ready" in src
    assert "#tdb-movies-in-library" not in src


def test_js_anime_card_populates_motif_available():
    """v1.23.90: the anime card reads motif_available from /api/coverage/plex's
    `anime` array (per item, in renderPlexCoverage) and writes plex-anime-motif —
    was a per-section sum in the retired renderPlexAnimeCard."""
    src = APP_JS.read_text()
    anchor = src.index("const animeItems = data.anime")
    fn_body = src[anchor:anchor + 900]
    # Reads motif_available per anime item.
    assert "m.motif_available" in fn_body, (
        "v1.23.90: the anime card must count motif_available across data.anime")
    # Writes to the plex-anime-motif element.
    assert "plex-anime-motif" in fn_body
