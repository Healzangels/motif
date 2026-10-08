"""v0.51.357 — the banners and the topbar pin as one box.

Each used to be `position: sticky; top: 0` on its own, with a comment that said the stack would
sort itself out ("Sticky positioning means both the banner AND topbar stick at the top" /
"nothing special needed; sticky stacks naturally"). Siblings do not stack that way: each pins to
the viewport top and the later one renders UNDER the earlier. Measured on the running app at
scrollY 1250, before the fix:

    desktop 1440: banner 0-45, topbar 0-65   → 45 of the topbar's 65px behind the banner
    mobile   390: banner 0-93, topbar 0-139  → brand, status pills and nav all covered

and the `.dry-run-banner + .paths-banner { top: 41px }` offset was a hardcoded guess against a
banner that is 45px on desktop and 93px wrapped on a phone. After: one `.sticky-head` wrapper
pins, the children stack in flow, and the same measurement reads banner 0-45 / topbar 45-110
(desktop) and 0-93 / 93-232 (mobile), overlap 0 at both.

A browser is the only thing that computes sticky, so this guards the structure that makes it
work: one sticky ancestor, no sticky children, all three inside it.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
BASE = (REPO / "app" / "web" / "templates" / "base.html").read_text()
HEADER_PARTS = (".dry-run-banner", ".paths-banner", ".topbar")


def _rule(selector: str) -> str:
    """The body of the first top-level rule for `selector`."""
    m = re.search(rf"^{re.escape(selector)} \{{(.*?)^\}}", CSS, re.S | re.M)
    assert m, f"no rule for {selector}"
    return m.group(1)


def test_one_sticky_box_pins_the_header_stack():
    body = _rule(".sticky-head")
    assert "position: sticky" in body and "top: 0" in body, body


def test_no_header_part_pins_itself():
    """The regression this tag fixes: two siblings pinned at top:0 overlap, they do not stack."""
    for sel in HEADER_PARTS:
        body = _rule(sel)
        assert "position: sticky" not in body, (
            f"{sel} pins itself again — inside .sticky-head that puts it back on top of its "
            f"siblings instead of beside them")


def test_the_topbar_stays_a_containing_block():
    """Mobile's #op-mini is `position: absolute` against the topbar (app.css ~6135). Dropping the
    sticky without leaving a position would have re-parented it to the sticky wrapper."""
    assert "position: relative" in _rule(".topbar")


def _sticky_head_block() -> str:
    """The wrapper's contents, found by walking div depth — not by the next `</div>`, which a
    missing close tag would silently hand to some later element (the first draft of this test
    passed with the wrapper left open)."""
    start = BASE.index('<div class="sticky-head">')
    depth, i = 0, start
    while i < len(BASE):
        nxt_open, nxt_close = BASE.find("<div", i), BASE.find("</div>", i)
        if nxt_close == -1:
            break
        if nxt_open != -1 and nxt_open < nxt_close:
            depth += 1
            i = nxt_open + 4
            continue
        depth -= 1
        if depth == 0:
            return BASE[start:nxt_close]
        i = nxt_close + 6
    raise AssertionError(".sticky-head is never closed")


def test_both_banners_and_the_topbar_live_inside_the_wrapper():
    block = _sticky_head_block()
    for frag in ('id="dry-run-banner"', 'id="paths-banner"', '<header class="topbar">'):
        assert frag in block, f"{frag} is outside .sticky-head"
    assert "<main" not in block, ".sticky-head swallowed the page — its closing tag is missing"


def test_the_template_still_balances():
    """The cheap backstop for the above: base.html has no conditional block elements, so its div
    and header tags balance exactly."""
    assert BASE.count("<div") == BASE.count("</div>"), "unbalanced <div> in base.html"
    assert BASE.count("<header") == BASE.count("</header>"), "unbalanced <header> in base.html"


def test_a_banner_is_opaque_enough_to_scroll_rows_under():
    """Both banners carried only a colour tint, which never showed while they sat ON TOP of the
    topbar. With the stack fixed, what passes under a banner is page content."""
    for sel in (".dry-run-banner", ".paths-banner"):
        body = _rule(sel)
        assert "background-color: rgba(var(--bg-rgb)" in body, sel
        assert "backdrop-filter: blur" in body, sel


def test_the_dry_run_pulse_cannot_erase_that_background():
    """It animates background-image; the `background` shorthand it used to animate reset the
    background-color every frame."""
    m = re.search(r"@keyframes dry-run-pulse \{(.*?)\}", CSS, re.S)
    assert m, "no dry-run-pulse keyframes"
    assert "background-image:" in m.group(1)
    assert not re.search(r"\n\s*background:", m.group(1)), m.group(1)
