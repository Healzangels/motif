"""v0.51.139 — CSS-audit T6: token hygiene (inline value → existing token).

All value-preserving (the literal equals the token), so rendering is unchanged;
this just enforces the design-system rule of using the token instead of inlining
its value. Guards against regressing the specific literals back in.

  * border-radius: 2px  → var(--radius)   (--radius is 2px)
  * font-size: 11px     → var(--t-tiny)   (--t-tiny is 11px)
  * .block-head ≤600 padding-left/right 12px → var(--gap-3)  (--gap-3 is 12px)
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def test_no_raw_border_radius_2px():
    # only the --radius token definition may carry the literal 2px.
    assert "border-radius: 2px" not in APP_CSS, (
        "border-radius should use var(--radius), not the inline 2px literal"
    )
    assert "--radius: 2px;" in APP_CSS  # the token itself keeps the literal


def test_no_raw_font_size_11px():
    assert "font-size: 11px" not in APP_CSS, (
        "font-size should use var(--t-tiny), not the inline 11px literal"
    )
    assert "--t-tiny: 11px;" in APP_CSS


def test_block_head_mobile_padding_tokenized():
    assert ("padding-left: var(--gap-3); padding-right: var(--gap-3);"
            in APP_CSS), "the ≤600 .block-head padding must use var(--gap-3)"
