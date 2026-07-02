"""v1.23.58 — glossary/legend chip rows align their definitions into a column.

After v1.23.56 the SRC/LINK decode chips reuse the real row classes and so have
varying widths, which left the def text ragged. .help-gloss-grid is a 2-col grid
with the rows as display:contents so chips + defs line up.

v0.50.97: unified the chip rail across ALL sections. Pre-fix the narrow sections
shared a 26px fixed rail while the wider TDB + TOPBAR text-pill sections used a
`.help-gloss-grid-wide` `auto` rail — so their chips + defs sat ~25-33px right of
the rest (the user: "the TDB chips at the top and the TOPBAR at the bottom are
not in line"). Now every section shares ONE 60px rail (fits the widest chip,
RE-PUSH / NO TDB ≈ 58px). v0.50.99 centres the chips in that rail (the user
preferred centred), so all chip centres + all defs line up in one column.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def _rule(selector: str) -> str:
    i = APP_CSS.index(selector + " {")
    return APP_CSS[i:APP_CSS.index("}", i)]


def test_help_gloss_grid_is_two_column():
    grid = _rule(".help-gloss-grid")
    # v0.51.2: the uniform rail is now the named --gloss-chip-rail token (was a bare
    # 60px literal; code-review altitude). It stays FIXED on purpose — the modal
    # stacks its sections (one shared rail aligns their defs) while the inline legend
    # is multi-column, so a content-auto column can't span both surfaces.
    assert "--gloss-chip-rail: 60px;" in grid, "the rail is a named, documented token"
    assert "grid-template-columns: var(--gloss-chip-rail) 1fr" in grid, "rail token + 1fr defs"
    assert "justify-items: start" in grid, "chips keep natural width, not stretched"


def test_no_wide_rail_exception():
    """v0.50.97: the per-section auto-rail exception is gone — the 60px base rail
    fits the widest chip, so no section may opt out (that's what left TDB/TOPBAR
    misaligned). Guard both the CSS rule and the template class usage.

    v0.51.0: scan EVERY template, not just base.html. The class lives on two
    surfaces — the modal glossary (base.html) AND the inline library legend
    (library.html) — and the original base.html-only check let a stale
    library.html:813 usage slip through (code review)."""
    assert ".help-gloss-grid-wide {" not in APP_CSS, "the auto-rail exception must not return"
    tpl_dir = REPO / "app" / "web" / "templates"
    for tpl in sorted(tpl_dir.glob("*.html")):
        assert "help-gloss-grid-wide" not in tpl.read_text(), (
            f"{tpl.name} must not carry the dropped .help-gloss-grid-wide class"
        )


def test_help_gloss_row_uses_display_contents():
    row = _rule(".help-gloss-row")
    assert "display: contents" in row, "chip + def join the grid for column alignment"


def test_chips_centre_in_the_rail():
    """v0.50.99: every chip/dot/glyph CENTRES in the uniform 60px rail (the user
    preferred centred over the v0.50.97 left-align). The uniform rail still aligns
    the defs across sections; centring re-centres the chips within it."""
    assert ".help-gloss-row > :not(.help-gloss-def) { justify-self: center; }" in APP_CSS
    assert ".help-gloss-row > :not(.help-gloss-def) { justify-self: start; }" not in APP_CSS
    glyph = _rule(".help-gloss-glyph")
    assert "text-align: center" in glyph
    assert "align-items: center" in _rule(".help-gloss-grid")
