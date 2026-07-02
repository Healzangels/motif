"""v0.51.5 — IMPORT preview table swipes on mobile instead of crushing.

Mobile audit finding A4 (P1): the Settings > IMPORT preview table
(#import-preview-table) is table-layout:fixed; width:100%; max-width:100% with
fixed columns summing ~736px (state 36 + status 100 + imdb 140 + current 130 +
url 170 + action 160) plus a fluid TITLE. Its wrapper is the generic .table-scroll
(overflow-x:auto at ≤1080px), but the table had NO min-width floor — so at 375px it
crushed its columns to ~281px instead of holding width and swiping, unlike the
sync-history (.sync-hist-table 480px) and jobs (.jobs-grid-row 760px) siblings.

Fix: pin #import-preview-table { min-width: 880px } in the ≤1080px block so the
.table-scroll swipes. Verified in-browser: table offsetWidth 880, wrapper
scrollWidth 880 > clientWidth 281 → swipes; page scrollWidth stays 375.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def _block(css: str, opener: str) -> str:
    i = css.index(opener)
    depth = 0
    j = i
    while True:
        if css[j] == "{":
            depth += 1
        elif css[j] == "}":
            depth -= 1
            if depth == 0:
                return css[i:j + 1]
        j += 1


TABLET = _block(APP_CSS, "@media (max-width: 1080px) {")


def test_import_table_has_mobile_min_width_floor():
    assert "#import-preview-table { min-width: 880px; }" in TABLET


def test_floor_lives_with_the_shared_table_scroll_swipe_context():
    # the floor must sit in the SAME tier as the .table-scroll overflow-x that
    # provides the swipe (≤1080px), not the phone-only ≤600px block — otherwise
    # the 601-880px range still crushes.
    assert ".table-scroll { overflow-x: auto; -webkit-overflow-scrolling: touch; }" in TABLET


def test_import_table_base_still_fluid_on_desktop():
    # desktop keeps width:100% / max-width:100% (fits its card, no swipe). Anchor on
    # the multi-line base rule (the new one-liner #import-preview-table { min-width }
    # in the ≤1080px block appears earlier in the file).
    base = APP_CSS[APP_CSS.index("#import-preview-table {\n"):]
    base = base[:base.index("}")]
    assert "width: 100%;" in base
    assert "max-width: 100%;" in base
    assert "min-width" not in base
