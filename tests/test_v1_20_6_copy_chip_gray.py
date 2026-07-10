"""v1.20.6 — C (copy) LINK chip settled on muted-sage grey.

C = placement_kind='copy' — a placement MECHANISM (motif placed its
file via a cross-FS copy instead of a hardlink; double disk, usually
signals non-hardlink volume mounts), NOT a source. Its color cycled
through two wrong families before grey:
  - pre-v1.20.5: --violet  → read as user content (SRC=U, UB backup)
  - v1.20.5:     --blue    → read as TDB-related (TDB↑ / ACCEPT UPDATE
                            / chip-info are blue) — the user's repro
  - v1.20.6:     --fg-dim  → neutral muted-sage grey

Grey is off both the user-violet AND TDB-blue families, distinct from
HL's green (so wasteful cross-FS copies stay spottable), and distinct
from the LINK=— "no link" state (a borderless dimmer --fg-mute dash
vs this bordered --fg-dim pill). Supersedes test_v1_20_5_copy_chip_blue.
"""
from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def _rule(selector: str) -> str:
    idx = APP_CSS.index(selector + " {")
    return APP_CSS[idx:idx + APP_CSS[idx:].index("}") + 1]


def test_fg_dim_rgb_token_exists():
    """The grey pill's border/bg need an rgb triplet token (the
    v1.15.113 --color-rgb pattern)."""
    assert "--fg-dim-rgb:" in APP_CSS, (
        "v1.20.6: --fg-dim-rgb token must exist for the copy chip's "
        "border/bg rgba()"
    )


def test_link_glyph_copy_is_fg_dim_grey():
    rule = _rule(".link-glyph-copy")
    assert "color: var(--fg-dim);" in rule, (
        "v1.20.6: the C (copy) row chip must use --fg-dim grey"
    )
    assert "rgba(var(--fg-dim-rgb)" in rule
    # The two prior wrong families must both be gone.
    assert "--violet" not in rule, "copy must not use user-violet"
    assert "--blue" not in rule, "copy must not use TDB-blue"


def test_link_badge_copy_matches_fg_dim_grey():
    """Cross-surface consistency: the /pending placement-detail copy
    badge must use the SAME grey."""
    rule = _rule(".link-badge-copy")
    assert "color: var(--fg-dim);" in rule
    assert "--amber" not in rule and "--blue" not in rule, (
        "v1.20.6: copy badge must shed the prior amber/blue colors"
    )


def test_copy_chip_distinct_from_hardlink_and_none():
    """C (grey, bordered) must stay distinct from HL (green) and from
    the LINK=— 'no link' state (--fg-mute, borderless dash)."""
    hl = _rule(".link-glyph-hardlink")
    c = _rule(".link-glyph-copy")
    none = _rule(".link-glyph-none")
    assert "color: var(--ok);" in hl
    assert "color: var(--fg-dim);" in c
    # 'none' uses the DIMMER fg-mute and has no border (just a color).
    assert "var(--fg-mute)" in none
    assert "border" not in none, (
        "the LINK=— none state must stay a borderless dim dash so the "
        "bordered grey C pill reads as distinct"
    )


def test_no_link_glyph_uses_blue_anymore():
    """Counter-guard for the v1.20.5 → v1.20.6 revert: no LINK glyph
    should color with --blue (it read as TDB-related)."""
    blues = re.findall(
        r"\.link-glyph-([a-z]+)\s*\{[^}]*?color:\s*var\(--blue\)",
        APP_CSS,
    )
    assert blues == [], (
        f"v1.20.6: no LINK glyph should use --blue; found: {blues}"
    )


def test_v1_20_6_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
