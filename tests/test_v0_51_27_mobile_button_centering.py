"""v0.51.27 — centre the library toolbar's button groups + de-jumble the filter
drawer on a phone (the user). All scoped to @media (max-width: 600px).
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def _mobile_block() -> str:
    i = APP_CSS.index("@media (max-width: 600px) {")
    j = i + APP_CSS[i:].index("{")
    depth = 0
    for k in range(j, len(APP_CSS)):
        if APP_CSS[k] == "{":
            depth += 1
        elif APP_CSS[k] == "}":
            depth -= 1
            if depth == 0:
                return APP_CSS[j:k + 1]
    raise AssertionError("unterminated @media (max-width: 600px)")


MOBILE = _mobile_block()


def test_toolbar_button_groups_centre_on_mobile():
    assert ".filterbar { justify-content: center; }" in MOBILE
    assert ".chips { justify-content: center; }" in MOBILE
    # the resolution + section chip groups go full width so they centre on-screen.
    assert '.chips[aria-label="resolution"], .chips[aria-label="section"] { width: 100%; }' in MOBILE
    # the preset-star + CLEAR ALL toolgroup drops its desktop right-pin.
    assert ".library-toolgroup { margin-left: 0; }" in MOBILE


def test_filter_drawer_labels_centre_on_their_own_line():
    assert ".pill-filter-row { justify-content: center; }" in MOBILE
    i = MOBILE.index(".pill-filter-label {")
    block = MOBILE[i:MOBILE.index("}", i)]
    assert "flex-basis: 100%" in block, (
        "v0.51.27: the axis label must span its own line so the chips centre "
        "beneath it instead of wrapping ragged after a 72px label column")
    assert "text-align: center" in block
