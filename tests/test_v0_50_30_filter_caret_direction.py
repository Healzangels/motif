"""v0.50.30 — the // FILTERS dropdown caret points the right way.

The user reported the closed-state caret "is still downward instead of >".
It was a ▾ glyph rotating 180°→▴ on open — both the wrong rest glyph and the
wrong direction vs the LEGEND/GLOSSARY carets (► closed, ▾ open). v0.50.30
swaps it to ► and rotates 90° on open so all three carets read identically.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
LIBRARY = (REPO / "app" / "web" / "templates" / "library.html").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def test_caret_rest_glyph_is_right_pointing():
    # The caret span rests as ► (&#9656;), not the old ▾.
    i = LIBRARY.index('class="library-filter-caret"')
    span = LIBRARY[i:i + 120]
    assert "&#9656;" in span, "closed-state caret must be ► (right-pointing)"
    assert "▾" not in span, "the old ▾ rest glyph must be gone"


def test_open_state_rotates_ninety_not_oneeighty():
    j = APP_CSS.index(".library-filter-toggle.is-open .library-filter-caret")
    block = APP_CSS[j:APP_CSS.index("}", j)]
    # 90° turns ► into ▾ (down); 180° would turn ▾ into ▴ (the old wrong way).
    assert "rotate(90deg)" in block
    assert "rotate(180deg)" not in block
