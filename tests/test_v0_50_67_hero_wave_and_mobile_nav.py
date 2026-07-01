"""v0.50.67 — mobile nav scroll + richer/reactive hero wave + calm brand EQ.

Four asks from the user:
1. Mobile: the top nav wrapped LOGS/SETTINGS to a 2nd row — make it a horizontal
   scroll strip like the settings tabs.
2. Mobile: the hero wave overlapped the Collections subtitle (and behaved
   differently per tab) — make it uniform + non-overlapping.
3. The single repeating sine "gets a bit boring" — spice it up.
4. Move the activity reactivity off the upper-left brand EQ and onto the hero
   wave (bigger + more visible). The brand EQ becomes a calm ambient drift.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def _rule(sel: str) -> str:
    i = APP_CSS.index(sel)
    return APP_CSS[i:APP_CSS.index("}", i) + 1]


# ── 1. Mobile nav → horizontal scroll (scoped to a max-width media query) ──

def test_base_nav_unchanged_desktop_wraps():
    """Desktop base rule is untouched — the scroll behaviour is a mobile override
    only (the dual-axis-clip trap means we never want overflow-x as a base rule)."""
    assert ".nav { display: flex; gap: 0; flex-wrap: wrap; min-width: 0; }" in APP_CSS


def test_mobile_nav_is_nowrap_horizontal_scroll_inside_media_query():
    marker = ".nav { flex-wrap: nowrap; overflow-x: auto;"
    assert marker in APP_CSS
    # the override must live INSIDE a max-width media query, not as a base rule
    # (motif_mobile_css_scope_breakpoint / motif_overflow_dual_axis_clip). Verify
    # via brace depth: from the nearest preceding @media to the marker the braces
    # are still unbalanced-open (i.e. we're inside the media block).
    pre = APP_CSS[:APP_CSS.index(marker)]
    seg = APP_CSS[pre.rindex("@media (max-width:"):APP_CSS.index(marker)]
    assert seg.count("{") > seg.count("}"), \
        "mobile .nav scroll rule must be scoped inside the @media block"
    # the keyboard focus ring is inset so overflow-y:auto doesn't crop it.
    assert ".nav a:focus-visible { outline-offset: -2px; }" in APP_CSS


# ── 2. Hero wave overlap fix: a reserved clearance band, uniform across tabs ──

def test_hero_reserves_padding_band_for_the_wave():
    h = _rule(".hero { margin-bottom")
    assert "padding-bottom: 38px" in h
    # when the wave is toggled OFF, the reserved band is reclaimed.
    off = _rule("html.viz-no-hero-wave .hero {")
    assert "padding-bottom: 0" in off


# ── 3. Richer wave: a second cross-weaving layer ──

def test_second_wave_layer_exists():
    b = _rule(".hero::before {")
    assert "background: var(--green)" in b
    assert "mask: url(\"data:image/svg+xml," in b
    assert "z-index: -1" in b
    assert "pointer-events: none" in b
    assert "animation: hero-wave-2" in b
    # scrolls the OTHER way (positive mask-position) for the cross-weave.
    assert "@keyframes hero-wave-2 {" in APP_CSS
    assert "to { -webkit-mask-position: 320px center; mask-position: 320px center; }" in APP_CSS


def test_both_wave_layers_hidden_when_wave_toggled_off():
    assert "html.viz-no-hero-wave .hero::after," in APP_CSS
    assert "html.viz-no-hero-wave .hero::before { display: none; }" in APP_CSS


# ── 4. Activity reactivity moved to the hero (brand EQ calmed) ──

def test_busy_makes_the_wave_faster_brighter_taller():
    # v0.50.68: the base motif-busy rule is now LEVEL 1 (mild); the old 3.6s/1.25
    # values moved to level 2 (see test_v0_50_68). Base is still brighter + faster
    # than idle + taller.
    a = _rule("html.motif-busy .hero::after {")
    assert "var(--green-bright)" in a          # brighter colour
    assert "animation-duration: 4.8s" in a     # faster than idle 9s
    assert "scaleY(1.13)" in a                  # taller than idle
    b = _rule("html.motif-busy .hero::before {")
    assert "animation-duration: 9s" in b        # faster than idle 14s
    assert "scaleY(1.10)" in b


def test_wave_transition_eases_the_idle_to_busy_ramp():
    a = _rule(".hero::after {")
    assert "transition: opacity var(--motion-normal), transform var(--motion-normal);" in a


def test_brand_eq_is_now_calm_no_reactivity():
    """The brand EQ keeps its slow idle drift always; the old .is-active
    tempo/colour override is gone (the energy moved to the hero)."""
    assert ".brand-mark.is-active .brand-bar {" not in APP_CSS
    # base idle drift is untouched.
    assert "animation: brand-eq-1 2.8s ease-in-out infinite;" in APP_CSS


def test_js_toggles_motif_busy_on_root_from_activity_signal():
    # v0.50.68: the toggle now keys off heroBusy (anyMutatingOpActive unioned with
    # the click-time optimistic signal) on the cached root element.
    assert "const _root = document.documentElement;" in APP_JS
    assert "_root.classList.toggle('motif-busy', heroBusy);" in APP_JS
