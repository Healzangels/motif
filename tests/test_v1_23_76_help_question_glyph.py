"""v1.23.76 — the // HELP topbar toggle becomes a compact `?` glyph.

the user: change // HELP to a `?` that's cyan on hover and full cyan when clicked,
to take up less room along the top. The button keeps its id / aria-pressed /
help-mode behaviour (app.js is unchanged — it never read the label); only the
visible glyph + the .help-toggle styling change. Hover + active stay the SAME
color (the v1.23.52 "unify on one" rule), now cyan instead of green.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
BASE_HTML = (REPO / "app" / "web" / "templates" / "base.html").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def _help_toggle_button() -> str:
    i = BASE_HTML.index('id="help-toggle"')
    return BASE_HTML[BASE_HTML.rfind("<button", 0, i):
                     BASE_HTML.index("</button>", i)]


def _rule(selector: str) -> str:
    i = APP_CSS.index(selector + " {")
    return APP_CSS[i:APP_CSS.index("}", i)]


def _decls(selector: str) -> str:
    # rule body with /* … */ comments stripped (the comment prose mentions the
    # very properties we assert are GONE).
    return re.sub(r"/\*.*?\*/", "", _rule(selector), flags=re.DOTALL)


def test_button_label_is_a_question_mark():
    btn = _help_toggle_button()
    assert ">?" in btn, "the help toggle renders a `?` glyph"
    assert "// HELP" not in btn, "the // HELP label is gone (reclaims room)"
    # the meaning moved to the tooltip — the `?` isn't self-describing.
    assert 'title="Toggle help mode' in btn


def test_help_toggle_is_a_compact_centered_glyph():
    decls = _decls(".help-toggle")
    assert "min-width: 22px" in decls and "height: 22px" in decls, "square icon"
    assert "justify-content: center" in decls, "lone glyph centered, not off-axis"
    # the label-only typography is gone so the `?` sits on its axis.
    assert "letter-spacing" not in decls
    assert "text-transform" not in decls


def test_hover_and_active_are_cyan():
    hover = _rule(".help-toggle:hover")
    assert "var(--cyan)" in hover and "var(--green)" not in hover
    pressed = APP_CSS[APP_CSS.index('.help-toggle[aria-pressed="true"]'):]
    pressed = pressed[:pressed.index("}")]
    assert "background: var(--cyan)" in pressed, "active = full cyan fill"
    assert "rgba(var(--cyan-rgb)" in pressed, "glow tokenized, not hardcoded"
