"""v1.24.76 — TDB legend/glossary glyphs must match the row-pill render.

the user asked to make sure the in-context LEGEND (library.html) + the // GLOSSARY
(base.html) are up to date. Audit found one drift: the cookies state renders as
"TDB ⚿" on the row pill (the v1.15.17 squared-key glyph) but all three decode
surfaces (glossary, legend, filter chip) still showed the old "TDB ⚠". The
legend's whole job (v1.23.56 reuse-don't-mirror) is to show EXACTLY what's on the
row — so they're synced to ⚿.

This lint pins render↔decode glyph agreement for the whole TDB axis so the legend
can't silently drift from the pill again (would have caught the cookies drift).
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
BASE_HTML = (REPO / "app" / "web" / "templates" / "base.html").read_text()
LIBRARY_HTML = (REPO / "app" / "web" / "templates" / "library.html").read_text()


def _glyphs(text):
    """class -> set of label strings shown for each tdb-pill-* state."""
    out = {}
    for m in re.finditer(r'tdb-pill (tdb-pill-[a-z]+)"[^>]*>([^<]+)<', text):
        out.setdefault(m.group(1), set()).add(m.group(2).strip())
    return out


RENDER = _glyphs(APP_JS)
GLOSSARY = _glyphs(BASE_HTML)
LEGEND = _glyphs(
    # scope to the in-context legend section, not the filter chips above it
    LIBRARY_HTML[LIBRARY_HTML.index("TDB — upstream status"):])


def test_cookies_glyph_synced_to_squared_key():
    # the specific drift this tag fixed: ⚿, not the old ⚠.
    assert RENDER["tdb-pill-cookies"] == {"TDB ⚿"}
    assert GLOSSARY["tdb-pill-cookies"] == {"TDB ⚿"}
    assert LEGEND["tdb-pill-cookies"] == {"TDB ⚿"}
    # the retired glyph is gone from every user-facing surface.
    assert "tdb-pill-cookies\">TDB ⚠" not in BASE_HTML
    assert "TDB ⚠" not in LIBRARY_HTML


def test_glossary_glyphs_match_render():
    for cls, labels in RENDER.items():
        if cls in GLOSSARY:
            assert GLOSSARY[cls] == labels, (
                f"glossary {cls} {GLOSSARY[cls]} != row render {labels}")


def test_legend_glyphs_match_render():
    for cls, labels in RENDER.items():
        if cls in LEGEND:
            assert LEGEND[cls] == labels, (
                f"legend {cls} {LEGEND[cls]} != row render {labels}")


def test_every_rendered_tdb_state_is_decoded_in_both_surfaces():
    # completeness: the 7 row-pill states all appear in glossary + legend.
    for cls in RENDER:
        assert cls in GLOSSARY, f"glossary missing {cls}"
        assert cls in LEGEND, f"legend missing {cls}"
