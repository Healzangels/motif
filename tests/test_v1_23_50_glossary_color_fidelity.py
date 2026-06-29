"""v1.23.50→56 — the glossary + legend SRC/LINK chips ARE the real row chips.

History: the decode chips started as a parallel `gc-*` palette that had to mirror
the row chips by hand. It drifted on every color axis in turn — v1.23.50 (text
color), v1.23.54 (border), v1.23.55 (background fill) — each a fresh "colors don't
match" report. Worse, those recolors were invisible behind a stale cached app.css,
so the legend kept showing OLD colors while the years-stable `.link-badge-*` /
`.link-glyph-*` rendered correctly (the user: "the T is not the same green").

v1.23.56 fixes it at the root: the SRC + LINK decode chips REUSE the canonical row
classes straight from the markup (`link-badge link-badge-*` for SRC, `link-glyph-*`
for LINK), so they're identical to the rows by construction and immune to asset
staleness. Only the – / — dashes and the topbar // UPD/FAIL/DROP/DISK pills keep a
`gc-*` class (no row chip to borrow). The DL/PL dots (gd-*) and FLAG glyphs (gg-*)
stay a small parallel palette — those are pinned to their real row class here.
"""
from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
BASE_HTML = (REPO / "app" / "web" / "templates" / "base.html").read_text()
LIBRARY_HTML = (REPO / "app" / "web" / "templates" / "library.html").read_text()


def _prop_var(selector: str, prop: str) -> str:
    """Return the --token in `prop: var(--token)` inside `selector { ... }`.
    The negative lookbehind keeps `prop='color'` from matching `border-color:`."""
    m = re.search(re.escape(selector) + r"\s*\{([^}]*)\}", APP_CSS)
    assert m, f"selector {selector} not found in app.css"
    body = m.group(1)
    pm = re.search(r"(?<![-\w])" + prop + r":\s*var\((--[a-z0-9-]+)\)", body)
    assert pm, f"{selector} has no {prop}: var(--…)"
    return pm.group(1)


# Every SRC/LINK decode chip must carry the real row class, so it can't drift.
SRC_REAL = [
    "link-badge link-badge-themerrdb",
    "link-badge link-badge-user",
    "link-badge link-badge-adopt",
    "link-badge link-badge-manual",
    "link-badge link-badge-cloud",
]
# the full glossary (base.html) decodes every LINK glyph.
LINK_REAL_GLOSSARY = [
    "link-glyph link-glyph-hardlink", "link-glyph link-glyph-copy",
    "link-glyph link-glyph-pu", "link-glyph link-glyph-mismatch",
    "link-glyph link-glyph-b", "link-glyph link-glyph-tb",
    "link-glyph link-glyph-ab", "link-glyph link-glyph-bk",
]
# the compact in-tab legend (library.html) decodes the common subset.
# v0.50.55: + copy / mismatch / ab — the legend claims "every chip on THIS tab".
LINK_REAL_LEGEND = [
    "link-glyph link-glyph-hardlink", "link-glyph link-glyph-copy",
    "link-glyph link-glyph-pu", "link-glyph link-glyph-mismatch",
    "link-glyph link-glyph-b", "link-glyph link-glyph-tb",
    "link-glyph link-glyph-ab", "link-glyph link-glyph-bk",
]

DOT_PAIRS = [
    (".gd-on", ".state-pill-btn-on"),
    (".gd-await", ".state-pill-btn-await"),
    (".gd-broken", ".state-pill-btn-broken"),
    (".gd-off", ".state-pill-btn-off"),
]
# v0.50.55: the FLAGS decode completes the ATTN axis. Each new gg-* mirrors the
# real row class it borrows its hue from — pinned here so it can't drift like the
# v1.23.50 update/broken cyan regression did.
FLAG_PAIRS = [
    (".gg-fail", ".title-glyph-fail"),
    (".gg-update", ".title-glyph-update"),
    (".gg-broken", ".title-glyph-broken"),
    (".gg-mismatch", ".attn-pill-mismatch"),
    (".gg-await", ".title-glyph-await"),
    (".gg-restore", ".attn-pill-restore"),
    (".gg-toobig", ".title-glyph-toobig"),
]


def test_glossary_src_link_chips_reuse_real_row_classes():
    for cls in SRC_REAL + LINK_REAL_GLOSSARY:
        assert f'class="{cls}"' in BASE_HTML, f"glossary chip must reuse {cls}"


def test_legend_src_link_chips_reuse_real_row_classes():
    # v0.50.56: scope to the legend body — several of these class strings also live
    # in the SRC/LINK filter-chip buttons above the table, so an unscoped file check
    # wouldn't actually guard the legend decode rows.
    legend = LIBRARY_HTML[LIBRARY_HTML.index("library-legend-body"):LIBRARY_HTML.index("library-legend-foot")]
    for cls in SRC_REAL + LINK_REAL_LEGEND:
        assert f'class="{cls}"' in legend, f"legend chip must reuse {cls}"


def test_no_parallel_gc_palette_for_src_link():
    """The recolor-prone gc-t/u/a/m/p + gc-hl/c/pu/mm/pb/tb/ab/ub are gone; only
    the dash (gc-none) and the topbar pills (gc-upd/fail/disk) keep a gc-* class."""
    for gone in ("gc-t", "gc-u", "gc-a", "gc-m", "gc-p", "gc-hl", "gc-c", "gc-pu",
                 "gc-mm", "gc-pb", "gc-tb", "gc-ab", "gc-ub"):
        assert f".{gone} " not in APP_CSS and f".{gone}{{" not in APP_CSS, (
            f".{gone} should be deleted — SRC/LINK chips reuse the real classes now")
    assert ".gc-none" in APP_CSS, "the – / — dashes still use gc-none"


def test_no_theme_dash_reuses_link_badge_box():
    """v1.23.60: the – / — dashes carry .link-badge so their box matches the SRC
    letter chips (T/U/A/M/P) and the glyph aligns in the column; gc-none only
    supplies the muted color. (The topbar DROP keeps help-gloss-chip — it aligns
    with the other topbar pills, a different surface.)"""
    assert 'class="link-badge gc-none">\u2013' in BASE_HTML      # SRC – (en dash)
    assert 'class="link-badge gc-none">\u2014' in BASE_HTML      # LINK — (em dash)
    assert 'class="link-badge gc-none">\u2013' in LIBRARY_HTML   # legend SRC –
    # the topbar DROP pill stays on help-gloss-chip (topbar section, not SRC/LINK).
    assert 'help-gloss-chip gc-none">DROP' in BASE_HTML


def test_dot_colors_match_real_state_pills():
    # the glossary DL/PL dot is a filled circle (background:); the row state-pill
    # colors its glyph (color:) — same token, different property.
    for gloss, real in DOT_PAIRS:
        assert _prop_var(gloss, "background") == _prop_var(real, "color"), (
            f"{gloss} background must equal {real} color")


def test_flag_glyph_colors_match_real_title_glyphs():
    for gloss, real in FLAG_PAIRS:
        assert _prop_var(gloss, "color") == _prop_var(real, "color"), (
            f"{gloss} color must equal {real} (the row's title flag)")
