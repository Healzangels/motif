"""v0.51.365 — the last three items from the 2026-10-07 GUI pass.

(1) The donut's centre caption. The 2026-10-07 note called it "clipped"; measuring it
properly says otherwise — SVG text does not clip, and "movies" occupied 9.12 of the ring's
28.8-unit hole. What is true is that 2px in a 42-unit viewBox renders at ~5px: a caption
too small to read rather than one cut off. At 4px it reads ~8.6px in the 90px phone chart
and ~19px at the 200px declared size, and "movies" grows to 18.24 units — still inside the
hole, with the 5px display-face number above it still dominant.

(2) One date notation. Three sites rendered `toLocaleDateString()` ("9/28/2026" on en-US)
while SYNC HISTORY printed the server's ISO ("2026-09-28 17:08:09"), so one dashboard
showed a date two ways — and the locale form differs operator to operator. `fmt.date`
builds `YYYY-MM-DD` from local parts.

(3) The horizontal-scroll track. The 2026-10-07 note said mobile tables scroll with no
affordance. Also not quite right: v0.51.26 gave `.table-scroll` the thin-green track at
<=600 — the same treatment v0.51.3 gave `.nav`. The real gap was 600-1080, where
`#library-table` is pinned to 1080px and scrolls with nothing saying so. The track moved to
the <=1080 block, beside the `overflow-x` that creates the scrolling, rather than being
duplicated at a second breakpoint with a different height (3px vs 4px) — which is what the
first attempt at this did.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
DASH = (REPO / "app" / "web" / "templates" / "dashboard.html").read_text()
LEGIBLE_PX = 8  # the floor this tag sets for the smallest chart motif ships


def _decl(selector: str, prop: str) -> str:
    m = re.search(rf"{re.escape(selector)} \{{[^}}]*?(?:^|;|\*/)\s*{prop}:\s*([^;]+);",
                  CSS, re.S | re.M)
    assert m, f"no {prop} on {selector}"
    return m.group(1).strip()


def test_the_donut_caption_is_readable_at_the_smallest_chart():
    """Derived, not pinned: the caption's size in px is its font-size scaled by the
    chart's px-per-viewBox-unit, and the smallest chart motif ships is the phone's."""
    label = float(re.sub(r"px$", "", _decl(".source-pie-center-label", "font-size")))
    # the DONUT's viewBox, not the first one in the file — dashboard.html opens with
    # 24x24 stat glyphs, and matching those made this test read 15px and sail past a
    # chart shrunk to 60 (the mutation that caught it).
    donut = DASH[:DASH.index("source-pie-center-label")]
    vb = re.findall(r'viewBox="0 0 (\d+) \d+"', donut)
    assert vb, "the donut viewBox moved"
    units = int(vb[-1])
    assert units == 42, f"the donut viewBox is {units} units, not 42 — re-check the maths"
    smallest = min(float(w) for w in re.findall(
        r"\.source-pie-col \.source-breakdown-chart \{[^}]*?width:\s*(\d+)px", CSS, re.S))
    rendered = label / units * smallest
    assert rendered >= LEGIBLE_PX, (
        f"v0.51.365: the caption renders at {rendered:.1f}px in the {smallest:.0f}px chart "
        f"— under the {LEGIBLE_PX}px floor. It was 2px/42 = 4.3px before this tag."
    )


def test_the_caption_stays_under_the_number():
    num = float(re.sub(r"px$", "", _decl(".source-pie-center-num", "font-size")))
    label = float(re.sub(r"px$", "", _decl(".source-pie-center-label", "font-size")))
    assert label < num, (
        f"v0.51.365: the caption ({label}px) must stay secondary to the figure ({num}px)"
    )


def test_one_date_notation():
    calls = [ln for ln in APP_JS.splitlines()
             if "toLocaleDateString" in ln and not ln.strip().startswith("//")]
    assert not calls, (
        f"v0.51.365: a locale date is back — the dashboard shows ISO everywhere else: {calls}"
    )
    m = re.search(r"date: \(v\) => \{(.*?)\n    \},", APP_JS, re.S)
    assert m, "v0.51.365: fmt.date is gone"
    body = m.group(1)
    for part in ("getFullYear", "getMonth", "getDate", "padStart"):
        assert part in body, f"fmt.date must build the date from padded local parts: {part}"


def test_the_scroll_track_sits_with_the_scrolling():
    """One treatment, at the breakpoint that creates the scroll — not a second copy at
    another width with its own height."""
    # app.css has FOUR @media (max-width: 760px) blocks and two at 600, so key-by-width
    # must concatenate — a dict comprehension keeps only the last and reads the wrong one.
    blocks: dict = {}
    for m in re.finditer(r"@media \(max-width: (\d+)px\) \{(.*?)\n\}", CSS, re.S):
        blocks[m.group(1)] = blocks.get(m.group(1), "") + "\n" + m.group(2)
    owns_overflow = [w for w, b in blocks.items()
                     if re.search(r"\.table-scroll \{[^}]*overflow-x:\s*auto", b)]
    owns_track = [w for w, b in blocks.items()
                  if ".table-scroll::-webkit-scrollbar" in b]
    assert len(owns_overflow) == 1, f"the swipe context is declared at {owns_overflow}"
    assert owns_track == owns_overflow, (
        f"v0.51.365: the track is declared at {owns_track} and the scrolling at "
        f"{owns_overflow} — put them together, or a band scrolls with nothing saying so "
        f"(600-1080 did) and two blocks drift on the track's height"
    )
    block = blocks[owns_overflow[0]]
    assert "scrollbar-width: thin" in block and "--green-deep" in block, (
        "v0.51.365: the track keeps the thin-green language v0.51.3 set for .nav"
    )
