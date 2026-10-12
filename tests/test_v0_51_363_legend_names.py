"""v0.51.363 — the dashboard legend says the names again.

`.source-legend-name` was `overflow: hidden; text-overflow: ellipsis; white-space: nowrap`
since v0.50.86, whose comment said it was "harmless on desktop — there's always room
there, so the ellipsis never engages". Measured on the running dashboard, four donuts at
1800px, it engaged on five of seven rows:

    ThemerrDB       52px of the 63 it needs        Manual sidecar   65 of 99
    User-supplied   59 of 92                       Plex-served      52 of 77

and at 375px the name box measured ZERO — a legend of swatches and numbers. The width
does not come from the viewport but from how many libraries the operator has: four
donuts leave each legend 181px whatever the screen.

So the name wraps now instead of truncating (the row still cannot force its card wider,
which is all the ellipsis was for), the parenthesised share yields first when the row is
tight — a container query on the legend, since the legend's own width is what decides —
and the share lives in the row's tooltip so nothing is lost when it goes. Below 440px
the donut cards go one-per-row: v0.50.74's 2-up is the user's and holds at 600px, where
a card is ~276 wide and every name fits WITH its share; at 375 it leaves 115.

After: 1800px 0 clipped / 0 wrapped, 1280px 0 clipped (names on two lines), 375px 0
clipped / 0 wrapped and the share still shown.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def _rule(selector: str) -> str:
    m = re.search(rf"^{re.escape(selector)} \{{(.*?)^\}}", CSS, re.S | re.M)
    assert m, f"no rule for {selector}"
    return m.group(1)


def test_the_name_wraps_rather_than_truncating():
    """The name IS the legend's content — a row that cannot fit it should get taller,
    not quieter."""
    body = _rule(".source-legend-name")
    for banned in ("text-overflow: ellipsis", "white-space: nowrap", "overflow: hidden"):
        assert banned not in body, (
            f"v0.51.363: {banned} is back on .source-legend-name — it reads 'Themerr…' "
            f"at the width four donuts leave, which is the bug"
        )
    assert "overflow-wrap" in body, body
    assert "min-width: 0" in body, (
        "v0.50.86: without this the row floors at the full text width and pushes the "
        "card past the viewport — the reason the ellipsis was reached for"
    )


def test_the_share_yields_first_and_only_on_a_container_query():
    """A media query is the wrong instrument: the legend's width comes from the card
    count, not the screen. Four donuts leave 181px at 1800px and at 1100px alike."""
    m = re.search(r"@container \(max-width: (\d+)px\) \{([^}]*\})", CSS, re.S)
    assert m, "v0.51.363: the share's rule is not a container query any more"
    assert "source-legend-pct" in m.group(2), m.group(2)
    assert "display: none" in m.group(2), m.group(2)
    # and the container it queries must actually be declared, or the rule never fires
    assert "container-type: inline-size" in _rule(".source-breakdown-legend"), (
        "v0.51.363: @container without a container-type is a rule that silently never "
        "applies — the legend has to declare itself the container"
    )


def test_the_tooltip_keeps_what_the_row_may_drop():
    """The share is hidden, not deleted."""
    i = APP_JS.index("source-legend-item")
    title = APP_JS[APP_JS.index('title="', i):APP_JS.index('">', i)]
    assert "pct" in title and "toLocaleString()" in title, (
        f"v0.51.363: the row tooltip must carry the count and the share, because the "
        f"container query hides the share on a narrow card: {title!r}"
    )


def test_the_phone_gets_one_card_per_row():
    """v0.50.74's 2-up is the user's choice and is kept wherever a card can carry a
    legend; below 440px it cannot."""
    m = re.search(r"@media \(max-width: 440px\) \{(.*?)\n\}", CSS, re.S)
    assert m, "v0.51.363: the phone override is gone"
    block = m.group(1)
    assert "grid-template-columns: 1fr" in block, block
    for n in ("2", "3", "4"):
        assert f'[data-visible="{n}"]' in block, (
            f"v0.51.363: a {n}-donut dashboard still splits the row on a phone: {block}"
        )
    # the user's 2-up must survive above it (several 760px blocks exist; find the one
    # that speaks about the donut row)
    blocks = [b for b in re.findall(r"@media \(max-width: 760px\) \{(.*?)\n\}", CSS, re.S)
              if ".source-pie-row" in b]
    assert blocks, "v0.50.74's donut-row block at 760px is gone"
    assert any("repeat(2, 1fr)" in b for b in blocks), (
        "v0.50.74's 2-up is the user's call for tablet widths — it stays; only the "
        "phone, where a card leaves the legend 115px, goes one-up"
    )
