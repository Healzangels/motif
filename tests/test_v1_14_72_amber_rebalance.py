"""v1.14.72 — amber chip rebalance (semantic color cleanup).

the user v1.14.71 follow-up:

> "I think we need to think about our filters and their colors
>  right now that's a lot of amber"

The chip palette had drifted into "amber-as-default" for any
non-red non-green chip. By the v1.14.71 screenshot the filter
row had 7 amber-or-orange chips fighting for visual attention:
TDB △ (cookies), SRC P, SRC +P composite dot, STATUS !M / !P
/ ↻, LINK HL, LINK PS, ED ED. Amber's "needs attention" signal
was diluted because half of those chips don't actually need
attention — they're just states.

v1.14.72 narrows the amber palette to genuine attention chips
by reassigning two states whose semantics don't match amber:

  1. **HL (hardlink)** → green. Hardlink IS the SUCCESS state
     for file realization (efficient, primary). Green is its
     semantic family. The paired .link-badge-hardlink (used at
     /pending placement details) was already green; this brings
     the row LINK column + filter chip into agreement with that.

  2. **ED (edition)** → muted gray. Edition is neutral metadata
     ("Director's Cut", "IMAX", etc.) — not action-needed.
     Amber implied attention the user doesn't owe. Demoted
     across all three surfaces that read the same logical
     concept (row glyph .edition-pill, filter chip
     .ed-pill-btn-on, INFO card .info-scope-chip-edition) so
     the v1.13.51 cross-surface alignment contract still holds
     — they all moved together from amber to muted.

Genuine attention chips kept as amber:
  - STATUS !M / !P / ↻ (state-pill-btn-await): real action items
  - TDB △ (cookies): user needs to drop cookies.txt
  - PL pending dot: row-level pending placement
  - SRC P, +P, LINK PS: kept amber as Plex visual identity
    (separate concern from amber-as-attention)
"""
from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
CSS = REPO / "app" / "web" / "static" / "app.css"


def _block(anchor_substr: str, size: int = 600) -> str:
    """Slice from a CSS anchor through the next `size` chars."""
    css = CSS.read_text()
    i = css.index(anchor_substr)
    return css[i:i + size]


# ── HL: orange/amber → green ──────────────────────────────────


def test_link_glyph_hardlink_color_is_green():
    """The HL row glyph + filter chip must use the green color
    family (matching .link-badge-hardlink at /pending). Pre-fix
    used --orange (#ff7a3a) which read as amber-family on the
    filter row alongside ED, P, PS, !M etc."""
    block = _block(".link-glyph-hardlink {", size=800)
    assert "color: var(--ok);" in block
    # The orange/amber predecessors must NOT survive in this
    # specific block. v1.15.113 migrated rgba triplets to
    # `--<color>-rgb` tokens — check for the token form too
    # (post-migration the literal RGB never appears).
    assert "color: var(--orange)" not in block
    assert "rgba(255,122,58" not in block          # raw orange tuple
    assert "var(--orange-rgb)" not in block        # tokenized orange
    # The green border + bg are present (post-v1.15.113 token form).
    assert "rgba(var(--ok-rgb), 0.4)" in block
    assert "rgba(var(--ok-rgb), 0.05)" in block


def test_link_badge_hardlink_still_green():
    """The /pending detail badge .link-badge-hardlink was already
    green — pin so a future "consistency" refactor doesn't flip
    it to match the historical orange .link-glyph-hardlink color
    (the v1.14.72 direction is the OTHER way)."""
    block = _block(".link-badge-hardlink {", size=400)
    assert "color: var(--ok);" in block
    assert "border-color: var(--ok-deep);" in block


def test_v1_14_72_marker_on_hardlink_change():
    """A v1.14.72 marker explains the HL color flip rationale.
    Pin so a future audit can grep for the change reason."""
    block = _block(".link-glyph-hardlink {", size=2000)
    # Looking before the rule for the explanation marker.
    css = CSS.read_text()
    rule_idx = css.index(".link-glyph-hardlink {")
    pre_block = css[max(0, rule_idx - 1500):rule_idx]
    assert "v1.14.72: HL color flipped from orange → green" in pre_block


# ── ED: amber → muted (across all 3 surfaces) ──────────────────


def test_edition_pill_row_glyph_uses_muted_color():
    """The row's .edition-pill glyph must use --fg-dim (muted),
    NOT --amber-bright. Edition is neutral metadata."""
    block = _block(".edition-pill {", size=600)
    assert "color: var(--fg-dim);" in block
    assert "color: var(--amber-bright)" not in block
    # Border / bg also moved off the amber palette.
    assert "rgba(255,184,74" not in block


def test_ed_filter_chip_on_state_uses_muted_color():
    """The filter row's .ed-pill-btn-on (the "this matches
    edition-having rows" representation) mirrors the row glyph
    color — both moved to muted gray together."""
    block = _block(".ed-pill-btn-on {", size=400)
    assert "color: var(--fg-dim);" in block
    assert "color: var(--amber-bright)" not in block


def test_info_card_edition_chip_uses_muted_color():
    """The INFO card's .info-scope-chip-edition is the third
    surface that the v1.13.51 contract said must stay aligned
    with the row pill + filter chip. v1.14.72 keeps that
    contract — all three move together to muted gray."""
    block = _block(".info-scope-chip-edition {", size=500)
    assert "color: var(--fg-dim);" in block
    assert "color: var(--amber-bright)" not in block


def test_v1_14_72_marker_on_edition_change():
    """A v1.14.72 marker on at least one ED surface explains the
    rebalance rationale (so a future change touching any of the
    3 surfaces can grep here)."""
    css = CSS.read_text()
    # Marker text appears in the .edition-pill comment block.
    assert "v1.14.72: demoted from amber to muted gray" in css
    # And on the INFO card chip.
    assert "v1.14.72: rebalanced to muted gray" in css


# ── Amber chips that MUST remain amber (regression guard) ─────


def test_status_attention_pills_remain_amber():
    """v1.14.72 deliberately KEPT !M / !P / ↻ amber — they're
    real action items (missing motif theme / missing
    placement / reprobe needed). Pin so a future "more amber
    cleanup" pass doesn't accidentally lump these in with the
    HL/ED changes."""
    block = _block(".state-pill-btn-await", size=300)
    assert "var(--amber)" in block


def test_tdb_cookies_pill_uses_lemon_post_v1_15_121():
    """Cookies pill color history:
       pre-v1.15.17 — amber (clustered with Plex)
       v1.15.17     — --yellow #ffe066 (still too close)
       v1.15.43     — --brown #c08552 (the user: didn't like brown)
       v1.15.121    — --lemon #f5dd2b (current — yellow that's
                      distinct from amber's orange-warmth)
    The pill is still its own non-default color (test intent
    unchanged); just track the current canonical tone."""
    block = _block(".tdb-pill-cookies {", size=300)
    assert "color: var(--lemon);" in block, (
        "v1.15.121: cookies pill uses --lemon (was --brown pre-"
        "v1.15.121, --yellow pre-v1.15.43, --amber pre-v1.15.17). "
        "If a future change reverts to amber, the cookies-as-Plex-"
        "problem visual confusion will resurface."
    )
    # Defensive: must not slip back to amber or brown.
    assert "color: var(--amber);" not in block
    assert "color: var(--brown);" not in block


def test_plex_source_pills_remain_amber():
    """SRC P (Plex-served) keeps amber as the "Plex visual identity"
    color family — distinct concern from amber-as-attention. v1.14.72
    was targeted, not sweeping; the Plex chip stays so the user's eye
    still groups them. (v1.20.47: the LINK PS half dropped — the PS row
    badge / .link-glyph-ps was removed as dead CSS; the LPS signal now
    lives on the SRC=P pill and link_pills=ps is an api.py no-op.)"""
    p_block = _block(".link-badge-cloud {", size=300)
    assert "color: var(--amber);" in p_block
