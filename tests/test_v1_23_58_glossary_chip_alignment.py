"""v1.23.58 — glossary/legend chip rows align their definitions into a column.

After v1.23.56 the SRC/LINK decode chips reuse the real row classes and so have
varying widths (C/M narrower than HL/PU/PB; DISK wider than UPD), which left the
def text ragged. .help-gloss-grid is now a 2-col (auto 1fr) grid with the rows as
display:contents, so the chip column sizes to the widest chip per section and
every definition lines up — while the chips keep their natural (row-identical)
size.
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
    # v1.23.68: the chip rail is now a FIXED width (was `auto`) so every section's
    # def column lands at the same x — auto sized each section's rail to its own
    # content, so the 9px DL/PL dots left their defs left of the wider chips.
    assert "grid-template-columns: 26px 1fr" in grid, "fixed chip rail + 1fr defs"
    assert "justify-items: start" in grid, "chips keep natural width, not stretched"
    # the text-pill sections (TDB / TOPBAR) opt into a wider auto rail.
    wide = _rule(".help-gloss-grid-wide")
    assert "grid-template-columns: auto 1fr" in wide


def test_help_gloss_row_uses_display_contents():
    row = _rule(".help-gloss-row")
    assert "display: contents" in row, "chip + def join the grid for column alignment"


def test_flag_glyphs_left_align_with_the_rail():
    """v1.23.86: the FLAGS glyphs (⚠ / ! / ↺) must left-align in the 26px rail
    like the SRC badges + DL dots above — they were centered, sitting ~13px
    right of the column (the user's screenshot)."""
    glyph = _rule(".help-gloss-glyph")
    assert "text-align: left" in glyph
    assert "text-align: center" not in glyph
