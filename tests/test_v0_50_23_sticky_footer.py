"""v0.50.23 — sticky footer: the healthz bar locks to the viewport bottom.

the user: on short sections (// ORPHAN SCAN) the footer "jumps way up in page";
want it at the screen bottom like on movies. body → flex column + .content
flex:1 0 auto so content grows to fill and the footer pins to the bottom.
"""
from __future__ import annotations

from pathlib import Path

APP_CSS = (Path(__file__).resolve().parent.parent / "app" / "web" / "static" / "app.css").read_text()


def _block(sel: str) -> str:
    i = APP_CSS.index(sel)
    return APP_CSS[i:APP_CSS.index("}", i)]


def test_body_is_a_flex_column_with_full_height():
    b = _block("body {")
    assert "min-height: 100vh" in b
    assert "display: flex" in b
    assert "flex-direction: column" in b


def test_content_grows_to_push_footer_down():
    c = _block(".content {")
    assert "flex: 1 0 auto" in c
