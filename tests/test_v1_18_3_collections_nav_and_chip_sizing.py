"""v1.18.3 — /collections nav active-state + chip sizing parity.

Two visual regressions on the /collections tab after v1.18.1:

  1. The active-tab underline didn't render under COLLECTIONS in
     the topbar nav. Other tabs (MOVIES / TV SHOWS / ANIME) light
     up with `.nav a.active` (green-bright text + 2px green bottom
     border per app.css:289), but COLLECTIONS stayed dim with no
     indicator. Root cause: the path→nav-key map in app.js's
     active-nav initializer (around line 271) was {'/': 'dashboard',
     '/movies': 'movies', '/tv': 'tv', '/anime': 'anime',
     '/queue': 'queue', '/settings': 'settings'} — no
     '/collections' entry. `map[path]` returned undefined, the
     function early-returned without adding `.active`.

  2. The per-section chip row (`// ALL // MOVIES // TV SHOWS //
     ANIME`) rendered with smaller padding + font than the
     STANDARD/4K chip row on other tabs. Root cause: the existing
     hero-chip sizing rule was scoped to
     `.chips[aria-label="resolution"] .chip` only. The v1.18.1
     section chip row uses `aria-label="section"` so it fell
     through to the default `.chip` style (6px 12px padding,
     --t-tiny font) while its sibling `// REFRESH PLEX` button
     stayed chunky — three controls in the same row at two
     different visual weights, exactly the problem the v1.13.18
     comment had documented.

Fixes:

  * `app.js` path map gains `'/collections': 'collections'`.
  * `app.css` hero-chip sizing rule extends from a single selector
    to a comma-separated pair: `.chips[aria-label="resolution"] .chip,
    .chips[aria-label="section"] .chip`. Same padding + font
    on both rows.
"""

from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"
APP_CSS = REPO / "app" / "web" / "static" / "app.css"


# ── Active-nav underline on /collections ─────────────────────


def test_app_js_path_map_includes_collections():
    """The path→nav-key map must include '/collections' so the
    nav underline lights up when the user is on that page."""
    js = APP_JS.read_text()
    # Find the map declaration.
    assert "'/collections': 'collections'" in js, (
        "v1.18.3: app.js path→nav-key map must include "
        "'/collections': 'collections' so the active-tab "
        "underline renders on /collections"
    )


def test_app_js_path_map_keeps_other_tabs_intact():
    """Sanity: the v1.18.3 fix must not have dropped any of the
    existing tab mappings — otherwise other tabs would lose
    their underline."""
    js = APP_JS.read_text()
    for expected in [
        "'/': 'dashboard'",
        "'/movies': 'movies'",
        "'/tv': 'tv'",
        "'/anime': 'anime'",
        "'/queue': 'queue'",
        "'/settings': 'settings'",
    ]:
        assert expected in js, (
            f"v1.18.3 regression: app.js path map lost the "
            f"{expected!r} entry"
        )


# ── Chip sizing parity on /collections hero ──────────────────


def test_chip_sizing_rule_covers_section_chips():
    """The hero-chip sizing rule (10px 18px padding + --t-small
    font) must apply to BOTH `aria-label="resolution"` (STANDARD/
    4K on movies/tv/anime) AND `aria-label="section"` (the v1.18.1
    per-section chip row on /collections). Without this, the
    /collections hero reads with chips visually lighter than its
    sibling // REFRESH PLEX btn — the user's "slightly smaller"
    audit."""
    css = APP_CSS.read_text()
    # Pin the comma-separated combined selector.
    assert (
        '.chips[aria-label="resolution"] .chip,\n'
        '.chips[aria-label="section"] .chip'
    ) in css, (
        "v1.18.3: the hero-chip sizing rule must cover both "
        "aria-label='resolution' AND aria-label='section'"
    )


def test_chip_sizing_rule_keeps_existing_values():
    """The size bump values (10px 18px padding, --t-small font)
    must be unchanged — v1.18.3 widens the SELECTOR not the
    values. Other tabs would shift if values regressed."""
    css = APP_CSS.read_text()
    # Locate the rule + capture its body.
    idx = css.index(
        '.chips[aria-label="resolution"] .chip,\n'
        '.chips[aria-label="section"] .chip'
    )
    body = css[idx:idx + 200]
    assert "padding: 10px 18px;" in body
    assert "font-size: var(--t-small);" in body
