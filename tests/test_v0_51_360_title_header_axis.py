"""v0.51.360 — the TITLE header sits over the titles, and the play glyph against its title.

v0.51.333 put an 18px play slot inside the title cell as its first flex child. Every title moved
right by slot + gap; `th.col-title`'s label did not. Measured on the running app at 1800px, before:

    header label 267 · play glyph ink 271.8-280.2 · title 293

so the column's label sat 26px left of its own column, with the play glyph under it rather than
beside the title it plays — the operator's report, from a screenshot of prod. The glyph was also
centered in its slot, which put it 4.8px off the cell's left edge and 12.8px off the title: nearer
the column than the row. After: label 293, titles 293 (play button or not), glyph ink 276.6-285,
8px off the title — one gap, the distance every other title glyph keeps.

A browser is the only thing that lays this out, so what is guarded here is the arithmetic that
makes it true: the slot's width and the header's indent read ONE token, and the indent's second
term is the gap `.title-cell` actually uses.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
HEADER_RULE = "#library-table th.col-title .th-stack"


def _rule(selector: str) -> str:
    m = re.search(rf"^{re.escape(selector)} \{{(.*?)\}}", CSS, re.S | re.M)
    assert m, f"no rule for {selector}"
    return m.group(1)


def _decl(body: str, prop: str) -> str:
    m = re.search(rf"(?:^|;|\*/)\s*{re.escape(prop)}:\s*([^;]+);", body, re.S | re.M)
    assert m, f"no {prop} in {body!r}"
    return " ".join(m.group(1).split())


def test_the_title_header_clears_the_play_slot():
    """The indent is the slot plus the gap the title cell really uses — not a copied number."""
    gap = _decl(_rule(".title-cell"), "gap")
    indent = _decl(_rule(HEADER_RULE), "margin-left")
    assert indent == f"calc(var(--row-play-w) + {gap})", (
        f"v0.51.360: the TITLE label must clear the play slot by exactly slot + the title cell's "
        f"own gap ({gap}) or it stops sitting over the titles; got {indent!r}"
    )


def test_the_slot_and_the_header_read_one_width():
    """A literal width here is the drift: the header's indent would keep the old number."""
    body = _rule(".row-play")
    assert _decl(body, "width") == "var(--row-play-w)", body
    assert _decl(body, "flex") == "0 0 var(--row-play-w)", body
    assert re.search(r"^\s*--row-play-w:\s*\d+px;", CSS, re.M), "the token is not defined"


def test_the_glyph_sits_against_its_title():
    """Trailing edge, so the only space between the glyph and the title is the flex gap."""
    assert _decl(_rule(".row-play"), "text-align") == "right", (
        "v0.51.360: centered in its slot the glyph reads as the column's, not the row's"
    )


def test_the_phone_slot_repoints_the_token():
    """<=600px wants a 30px tap target. Re-setting the WIDTH there would leave the header at 18."""
    m = re.search(r"@media \(max-width: 600px\) \{(.*?)\n\}", CSS, re.S)
    assert m, "the <=600px block moved"
    block = m.group(1)
    play = re.findall(r"^\s*\.row-play \{([^}]*)\}", block, re.M)
    assert play, "the phone .row-play rule is gone"
    for body in play:
        assert "width" not in body and "flex-basis" not in body, (
            f"v0.51.360: the phone slot must re-point --row-play-w so the TITLE indent follows "
            f"it; setting the width directly leaves the header behind: {body!r}"
        )
    assert re.search(r":root \{[^}]*--row-play-w:\s*30px", block), (
        "the phone block no longer re-points --row-play-w"
    )
