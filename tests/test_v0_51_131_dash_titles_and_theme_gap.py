"""v0.51.131 — dashboard card titles match their swapped content + THEME dropdown
spacing consistent with other fields.

(1) After the v0.51.122 in-place data swap, the top-stats cards showed the
    library TOTAL + ThemerrDB reach under a "// … THEMED" title, and the
    plex-coverage cards showed the themed % under "// PLEX …". The titles no
    longer matched the numbers. Retuned: top-stats cards are plain (// MOVIES /
    TV / ANIME / COLLECTIONS — the total), plex-coverage cards are // … THEMED
    (the %). Only the display TEXT changed; ids/sections/positions are untouched
    so dashboard-customize's applyLayout stays a no-op.

(2) .theme-select carried an anomalous `margin-top: var(--gap-1)` that .input
    fields don't — it pushed the dropdown down off its label so it read as too
    close to the hint below. Zeroed → matches every other labelled field.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DASH = (REPO / "app" / "web" / "templates" / "dashboard.html").read_text()
CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()

TYPES = ("MOVIES", "TV", "ANIME", "COLLECTIONS")


def test_top_stats_cards_are_plain_type_titles():
    for t in TYPES:
        assert f'stat-label">// {t}</span>' in DASH, f"top card // {t} title missing"


def test_coverage_cards_are_themed_titles():
    for t in TYPES:
        assert f'stat-label">// {t} THEMED</span>' in DASH, \
            f"coverage card // {t} THEMED title missing"


def test_old_plex_prefixed_titles_are_gone():
    # the pre-v0.51.131 coverage-card titles ("// PLEX MOVIES" …) are retired.
    for t in TYPES:
        assert f'stat-label">// PLEX {t}</span>' not in DASH


def test_theme_select_has_no_margin_top():
    start = CSS.index(".theme-select {")
    block = CSS[start:CSS.index("}", start) + 1]  # the whole rule, comments incl.
    assert "margin-top: 0;" in block
    assert "margin-top: var(--gap-1)" not in block
    # it stays block-level (v0.51.130) so it drops below the label.
    assert "display: block;" in block
