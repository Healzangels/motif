"""v0.50.69 — the durable hero-alignment RULE.

the user: the hero height (and so where the first content block below it starts,
and where the wave sits) drifted out of alignment between tabs AGAIN when the
v0.50.67 wave padding shrank the content-area below the tallest hero's content —
"can we make a rule to fix this as this keeps coming up ... uniform professional
clean look ... the wave in the same Y in every section ... same when hidden."

THE RULE (this file guards it): hero geometry is defined by exactly TWO :root
tokens — --hero-content-zone (>= the tallest hero's content) and --hero-wave-band
(the reserved wave clearance). BOTH hero heights derive from them:
  - wave ON : min-height = calc(zone + band); padding-bottom = band
  - wave OFF: min-height = zone;              padding-bottom = 0
So the two states can never drift, and since the floor (content-area) is always
== the zone (>= tallest content), every tab renders the hero at the SAME height →
first content block + wave land on the same Y everywhere, wave shown or hidden.
"""
from __future__ import annotations

import re
from pathlib import Path

APP_CSS = (Path(__file__).resolve().parent.parent / "app" / "web" / "static" / "app.css").read_text()


def _rule(sel: str) -> str:
    i = APP_CSS.index(sel)
    return APP_CSS[i:APP_CSS.index("}", i) + 1]


def test_hero_geometry_tokens_exist():
    assert re.search(r"--hero-content-zone:\s*\d+px;", APP_CSS)
    assert re.search(r"--hero-wave-band:\s*\d+px;", APP_CSS)


def test_wave_on_hero_derives_both_dimensions_from_the_tokens():
    """The floor is zone+band and the reserved padding is the band — so the
    content-area (= floor - band) is exactly the zone, guaranteeing the same
    render height on every tab whose content fits the zone."""
    h = _rule(".hero { margin-bottom")
    assert "min-height: calc(var(--hero-content-zone) + var(--hero-wave-band))" in h
    assert "padding-bottom: var(--hero-wave-band)" in h
    # no stray literal px height that could drift from the tokens.
    assert "min-height: 150px" not in h
    assert "min-height: 164px" not in h


def test_wave_off_hero_uses_the_same_zone_token():
    """Wave-off drops to JUST the content zone — the SAME token, so it stays
    aligned with wave-on's content top and consistent across tabs."""
    off = _rule("html.viz-no-hero-wave .hero {")
    assert "min-height: var(--hero-content-zone)" in off
    assert "padding-bottom: 0" in off
    # must NOT reintroduce an independent literal (the old 130px drift source).
    assert "130px" not in off


def test_content_zone_clears_the_tallest_hero_content():
    """The invariant: --hero-content-zone >= the tallest hero's content. The
    tallest is the dashboard's 2-line subtitle ≈ title(64*0.9=57.6) + gap-5(20) +
    sub(~19.5) + gap-1(4) + sync-line(~18) ≈ 119px. Pin a floor so a future
    shrink can't silently re-break alignment."""
    zone = int(re.search(r"--hero-content-zone:\s*(\d+)px;", APP_CSS).group(1))
    assert zone >= 120, (
        f"--hero-content-zone={zone}px is below the ~119px tallest hero content; "
        "the dashboard hero would grow past the floor and misalign the tabs again"
    )


def test_only_two_hero_min_heights_and_both_are_token_derived():
    """Exactly two rules set a .hero min-height (wave-on base + wave-off), and
    NEITHER hard-codes a px value — so there's a single source of truth."""
    heights = re.findall(r"\.hero\b[^{]*\{[^}]*?min-height:\s*([^;]+);", APP_CSS)
    assert len(heights) == 2, f"expected 2 .hero min-height rules, found {heights}"
    for val in heights:
        assert "var(--hero-content-zone)" in val, (
            f"hero min-height '{val.strip()}' must derive from --hero-content-zone"
        )
