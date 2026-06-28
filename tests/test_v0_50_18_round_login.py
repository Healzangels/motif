"""v0.50.18 — login AUTHENTICATE window is a round vinyl record label.

the user: "make the authenticate window fit into the theme of the record better,
maybe make it round or look like the center dot of the record." .auth-card
becomes a circle centred over the spinning-record backdrop; .auth-card-inner
keeps the form inside the circle's safe centre zone.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
LOGIN = (REPO / "app" / "web" / "templates" / "login.html").read_text()


def _block(sel: str) -> str:
    i = APP_CSS.index(sel)
    return APP_CSS[i:APP_CSS.index("}", i)]


def test_auth_card_is_a_circle():
    block = _block(".auth-card {")
    assert "border-radius: 50%" in block
    assert "aspect-ratio: 1 / 1" in block
    # scales down on narrow viewports instead of a fixed 460px box.
    assert "width: min(460px, 92vw)" in block
    # the old rectangular accent is gone (replaced by the circular ::after ring).
    assert "border-left: 3px solid var(--green)" not in block


def test_circular_ring_replaces_border_left():
    block = _block(".auth-card::after {")
    assert "border-radius: 50%" in block
    assert "rgba(var(--green-rgb), 0.30)" in block


def test_inner_wrapper_clamps_form_to_safe_zone():
    # the template wraps the head + form so the round edge never clips them.
    assert 'class="auth-card-inner"' in LOGIN
    block = _block(".auth-card-inner {")
    assert "max-width: 70%" in block


def test_card_centres_its_content():
    block = _block(".auth-card {")
    assert "align-items: center" in block
    assert "justify-content: center" in block


def test_v0_50_1_centring_and_backdrop_preserved():
    # the round restyle must not undo the v0.50.1 flex-centring or the record.
    assert "body.auth-page .auth-card { margin: 0; }" in APP_CSS
    assert 'class="record-spinner login-record"' in LOGIN
    assert LOGIN.count('class="login-ripple"') == 3
