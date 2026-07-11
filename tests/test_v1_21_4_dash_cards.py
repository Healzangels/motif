"""v1.21.4 — DASH card glyph redesign + PLEX stat-foot stacking.

the user's feedback on the v1.21.2 dashboard:

  1. The per-media-type glyphs (▷ Movies, ◇ TV, ◆ Anime,
     ◈ Collections) read as inconsistent — visually similar
     outline/diamond shapes that didn't map to anything
     recognizable. the user picked set B (evocative): ▶ play
     (Movies), ▭ screen (TV), ✦ star (Anime), ▦ grid
     (Collections).

  2. The PLEX MOVIES card's stat-foot wrapped onto two lines
     because its 4-digit counts overflowed the flex row, while
     the other PLEX cards (smaller numbers) stayed on one line —
     inconsistent across the related cards. Fix: stack the foot
     (flex-direction: column) for ALL PLEX cards so every PLEX
     card shows "X with theme" / "Y ThemerrDB available" on two
     lines uniformly. Scoped to [data-dash-card^="plex-"] so the
     THEMERRDB cards keep their compact one-line foot.

Browser-verified via the Claude Preview MCP against the real
app.css before ship (the v1.21.1 ACK-alignment miss taught the
render-then-measure lesson — pixel/layout CSS is not reasoned
blind).

Source-text pins (consistent with the dashboard UI-tweak tags).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── Glyph set B applied in dashboard.html ────────────────────

def test_dash_glyphs_use_feather_svg():
    """v1.24.66: the stat-card glyphs are now Feather-style SVG line icons via
    the media_glyph() macro (matching the carousel's new icons), replacing the
    v1.21.4 ▶/▭/✦/▦ unicode set. Each media type renders twice — COVERAGE +
    PLEX rows."""
    html = (REPO / "app" / "web" / "templates" / "dashboard.html").read_text()
    assert "{% macro media_glyph(kind)" in html
    for kind in ("movies", "tv", "anime", "collections"):
        assert html.count("media_glyph('%s')" % kind) == 2, kind
    # the old unicode glyphs are gone from the stat-glyph spans.
    for old in ("▶", "▭", "✦", "▦"):
        assert f'<span class="stat-glyph">{old}</span>' not in html


def test_old_glyphs_fully_retired():
    """The pre-v1.21.4 outline/diamond glyphs must not survive in
    any .stat-glyph span — they're the exact inconsistency the user
    flagged."""
    html = (REPO / "app" / "web" / "templates" / "dashboard.html").read_text()
    for old in ("▷", "◇", "◆", "◈"):
        assert f'<span class="stat-glyph">{old}</span>' not in html, (
            f"Old DASH glyph {old} resurfaced in a stat-glyph span"
        )


# ── PLEX stat-foot stacks; THEMERRDB foot stays compact ──────

def test_plex_stat_foot_stacks_vertically():
    """The wide ThemerrDB-reach foot uses flex-direction: column so all four
    reach cards show a uniform two-line foot — fixing the 4-digit wrap. v0.51.122:
    keyed on an explicit .stat-foot-stack class (was [data-dash-card^=\"plex-\"])
    because the reach numbers swapped onto the // …THEMED (tdb-*) cards while the
    % + bar swapped onto the // PLEX cards, so the stack follows the WIDE reach
    foot, not the card id (the user kept the titles)."""
    css = (REPO / "app" / "web" / "static" / "app.css").read_text()
    idx = css.index('.stat-foot-stack {')
    rule = css[idx:idx + 200]
    assert "flex-direction: column" in rule, (
        "the reach stat-foot must stack so every reach card matches"
    )
    # The old data-dash-card-scoped selector must be gone (it would wrongly
    # column-stack the % foot now on the plex-* cards).
    assert '[data-dash-card^="plex-"] .stat-foot {' not in css


def test_plex_cards_carry_dash_card_attr():
    """The four PLEX cards carry a data-dash-card id for the customize feature.
    v0.51.122: the foot-stack no longer keys on this (moved to the explicit
    .stat-foot-stack class), but the ids must persist so a saved customize
    layout can still address the cards."""
    html = (REPO / "app" / "web" / "templates" / "dashboard.html").read_text()
    for slug in ("plex-movies", "plex-tv", "plex-anime", "plex-collections"):
        assert f'data-dash-card="{slug}"' in html, (
            f"Missing data-dash-card=\"{slug}\" — stack rule won't apply"
        )


# ── Version pin (loose form — canonical pin lives in v1_13_79) ─

def test_version_bumped():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
