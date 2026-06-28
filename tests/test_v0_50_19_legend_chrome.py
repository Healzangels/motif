"""v0.50.19 — library-chrome polish (the user's toolbar asks).

1. LEGEND caret moved to the END of the label (was at the front), ► collapsed /
   ▾ open (the existing .open rotate), matching the status-bar caret.
2. GLOSSARY help-bar button gets the same trailing caret; app.js toggles .open on
   it on the <dialog> open/close so it rotates ►→▾ in step.
3. the open LEGEND panel OVERLAYS the table (position:absolute, scoped to
   .library-block) instead of pushing it down.
4. #library-subtitle gets margin-top so the /collections section selectors no
   longer almost-touch the subtitle.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
LIB_HTML = (REPO / "app" / "web" / "templates" / "library.html").read_text()
BASE_HTML = (REPO / "app" / "web" / "templates" / "base.html").read_text()


def test_legend_caret_is_at_the_end():
    i = LIB_HTML.index('id="library-legend-toggle"')
    btn = LIB_HTML[i:LIB_HTML.index("</button>", i)]
    # the label comes BEFORE the caret span now (caret at the end). v0.50.21:
    # no literal space — the gap is the caret's margin-left (consistent w/ glossary).
    assert "// LEGEND<span class=\"library-legend-caret\"" in btn
    # and NOT the old front-caret order.
    assert ">&#9656;</span>// LEGEND" not in btn


def test_glossary_button_has_trailing_caret():
    i = BASE_HTML.index('id="help-glossary-open"')
    btn = BASE_HTML[i:BASE_HTML.index("</button>", i)]
    assert "// GLOSSARY<span class=\"library-legend-caret\"" in btn


def test_glossary_caret_rotates_with_modal():
    # CSS: the GLOSSARY button's caret rotates when the button has .open.
    assert "#help-glossary-open.open .library-legend-caret { transform: rotate(90deg)" in APP_CSS
    # JS: .open added on click, removed on the <dialog> close event.
    assert "open.classList.add('open');" in APP_JS
    assert "dlg.addEventListener('close', () => open.classList.remove('open'));" in APP_JS


def test_legend_panel_is_inflow_dropdown():
    # v0.50.21: the v0.50.19 absolute overlay was reverted — the open panel is an
    # in-flow dropdown again (the user: the overlay hid the top rows).
    assert "<section class=\"block library-block\">" not in LIB_HTML
    assert ".library-block { position: relative; }" not in APP_CSS
    assert ".library-block .library-legend-panel.open {" not in APP_CSS
    assert ".library-legend-panel.open { display: block; }" in APP_CSS


def test_carets_share_one_consistent_gap():
    # v0.50.21: the gap is a single margin-left on the shared caret class (no
    # literal HTML space, no glossary-only extra margin → even on both).
    block = APP_CSS[APP_CSS.index(".library-legend-caret {"):]
    block = block[:block.index("}")]
    assert "margin-left:" in block
    assert "#help-glossary-open .library-legend-caret { margin-left:" not in APP_CSS


def test_subtitle_has_top_gap_from_section_selectors():
    assert "#library-subtitle { margin-top: var(--gap-3); }" in APP_CSS
