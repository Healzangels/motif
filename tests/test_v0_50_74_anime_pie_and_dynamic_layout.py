"""v0.50.74 — 4th ANIME source donut + dynamic fill-the-arena layout + no-theme
default off.

the user: "add a 4th pie chart for anime"; "if you display 4 there are 4 across
the row which get resized down, if there are 3 they get large ... they dynamically
fill their arena depending on how many charts you are displaying"; mobile "goes 2
stacks of 2, 3, 2, 1"; and "have no-theme unchecked by default for all of them".

- ANIME splits off TV via is_anime (theme_sources carries it since v1.24.56);
  Movies + TV now EXCLUDE anime so it doesn't double-count.
- renderAllSourcePies hides a donut whose scope has 0 items + sets
  .source-pie-row[data-visible=N]; CSS makes N charts fill the row (mobile: 2/row,
  the lone odd one full-width).
- '–' (No theme) is seeded into every donut's hidden set once (idempotent marker).
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
DASH = (REPO / "app" / "web" / "templates" / "dashboard.html").read_text()


# ── 1. Anime donut ──

def test_four_donuts_including_anime():
    for did in ("total", "movies", "tv", "anime"):
        assert f"id: '{did}'," in JS
    # anime scopes on is_anime; TV + Movies EXCLUDE anime so no double-count.
    assert "scopeFn: (r) => !!r.is_anime," in JS
    assert "(r) => r.media_type === 'show' && !r.is_anime," in JS
    assert "(r) => r.media_type === 'movie' && !r.is_anime," in JS


def test_anime_column_in_template():
    assert 'id="source-pie-anime-col"' in DASH
    assert 'id="source-pie-anime-slices"' in DASH
    assert 'id="source-pie-anime-legend"' in DASH
    assert "// ANIME" in DASH


def test_anime_hidden_key_and_set():
    assert "_ANIME_PIE_HIDE_KEY = 'motif:dash:src-hide-anime'" in JS
    assert "let _animePieHidden = _loadHiddenSet(_ANIME_PIE_HIDE_KEY);" in JS


# ── 2. No-theme unchecked by default ──

def test_no_theme_hidden_by_default_once():
    # one-time '–' seed into every donut's hidden set, guarded by a marker so a
    # user who re-enables '–' keeps it.
    assert "function _defaultHideNoTheme()" in JS
    assert "'motif:dash:src-hide-notheme-applied'" in JS
    assert "s.add('-'); _persistHiddenSet(k, s);" in JS


# ── 3. Dynamic fill-the-arena layout ──

def test_render_hides_empty_scope_and_sets_visible_count():
    idx = JS.index("function renderAllSourcePies(")
    body = JS[idx:JS.index("\n  function ", idx + 1)]
    # a 0-item scope (non-total) is hidden; Total always shows.
    assert "const show = d.id === 'total' || total > 0;" in body
    assert "col.style.display = show ? '' : 'none';" in body
    # the visible count drives the grid; the last visible col is marked for the
    # mobile odd-full rule.
    assert "row.setAttribute('data-visible', String(visible));" in body
    assert "lastVisibleCol.classList.add('is-last-visible');" in body


def test_desktop_grid_fills_by_visible_count():
    for n in (1, 2, 3, 4):
        assert f'.source-pie-row[data-visible="{n}"] {{ grid-template-columns:' in CSS
    # the old auto-fit floor (which wrapped instead of filling) is gone.
    assert "repeat(auto-fit, minmax(320px, 1fr))" not in CSS


def test_mobile_two_per_row_with_odd_full():
    # inside the mobile @media: 2-per-row, lone odd chart spans full width.
    i = CSS.index("@media (max-width: 760px) {", CSS.index(".source-pie-row {"))
    block = CSS[i:i + 500]
    assert "grid-template-columns: repeat(2, 1fr);" in block
    assert '.source-pie-row[data-visible="1"] { grid-template-columns: 1fr; }' in block
    assert '.source-pie-row[data-visible="3"] .source-pie-col.is-last-visible { grid-column: 1 / -1; }' in block


def test_template_seeds_default_visible():
    assert '<div class="source-pie-row" data-visible="4">' in DASH
