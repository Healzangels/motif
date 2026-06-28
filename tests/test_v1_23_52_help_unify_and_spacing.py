"""v1.23.52 — // HELP unifies on green + IDLE/HELP/logout evenly spaced.

the user (deploy): the // HELP toggle was cyan on hover but a green fill when
active (aria-pressed) — "unify on one." And the IDLE / // HELP / logout trio was
unevenly spaced: the help-toggle carried margin-left:gap-2 (IDLE→HELP = 16px) and
the logout carried margin-left:14px (HELP→logout = 22px). Both ad-hoc margins are
gone; the single .topbar-status gap now spaces all three evenly and pulls
help+logout closer together.
"""
from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def _rule(selector: str) -> str:
    i = APP_CSS.index(selector + " {")
    return APP_CSS[i:APP_CSS.index("}", i)]


def _decls(selector: str) -> str:
    """The rule's declarations with /* … */ comments stripped (the comments
    mention the removed margins by name)."""
    return re.sub(r"/\*.*?\*/", "", _rule(selector), flags=re.DOTALL)


def test_help_hover_and_active_are_both_cyan():
    # v1.23.76: the user re-picked cyan for the `?` glyph. Hover + active stay the
    # SAME color (the v1.23.52 "unify on one" rule), now cyan instead of green.
    hover = APP_CSS[APP_CSS.index(".help-toggle:hover"):]
    hover = hover[:hover.index("}")]
    assert "var(--cyan)" in hover, "hover unified to cyan"
    assert "var(--green)" not in hover, "no green left on the help toggle hover"
    pressed = APP_CSS[APP_CSS.index('.help-toggle[aria-pressed="true"]'):]
    pressed = pressed[:pressed.index("}")]
    assert "background: var(--cyan)" in pressed, "active fill is cyan"


def test_help_and_logout_have_no_adhoc_margins():
    # both relied on bespoke margins that broke the even rhythm; gone now.
    assert "margin-left" not in _decls(".help-toggle"), "help uses the status gap"
    assert "margin-left" not in _decls(".topbar-logout"), "logout uses the status gap"


def test_topbar_status_supplies_the_uniform_gap():
    status = _rule(".topbar-status")
    assert "gap: var(--gap-2)" in status, "the one gap that evenly spaces the trio"
