"""v0.51.43 — the mobile section-nav scroll track no longer hides the active-tab
colored underline.

the user (scrolling the DASHBOARD/MOVIES/TV/ANIME/COLLECTIONS strip on a phone):
"you can't see the colored bar since the scroll bar covers it, can we make it so
we can still see those with the scroll bar as well".

The ≤600px nav is a horizontal-scroll strip with a 3px green scroll track
(v0.51.3 "more tabs →" affordance). The active tab's colored underline is a 2px
border-bottom at the same bottom edge — and macOS overlay scrollbars draw OVER
the box (no reserved space), so the track covered the underline. Fix: bottom
padding on the nav ≥ the track height, so the underline sits above the track and
both stay visible. Scoped to the ≤600px block (desktop nav is unchanged).
"""
from __future__ import annotations

import re
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


def _px(rule_prefix: str, prop: str, text: str) -> int:
    i = text.index(rule_prefix)
    block = text[i:text.index("}", i) + 1]
    m = re.search(rf"{re.escape(prop)}\s*:\s*(\d+)px", block)
    assert m, f"{prop} not found in {rule_prefix!r} rule"
    return int(m.group(1))


def test_nav_bottom_padding_clears_the_scroll_track():
    # the ≤600px nav grid-area rule carries a bottom pad …
    pad = _px(".nav { grid-area: nav;", "padding-bottom", MOBILE)
    # … and the custom horizontal scroll track is 3px …
    track = _px(".nav::-webkit-scrollbar {", "height", MOBILE)
    # … the pad must exceed the track so the 2px active underline clears it.
    assert pad > track, (
        f"nav padding-bottom ({pad}px) must exceed the scroll-track height "
        f"({track}px) so the active-tab underline isn't hidden by the track")


def test_desktop_nav_has_no_bottom_pad():
    # the base .nav rule (outside any @media) must stay pad-free — the fix is
    # mobile-only (no scroll strip on desktop).
    base = APP_CSS[APP_CSS.index("\n.nav {"):]
    base_rule = base[:base.index("}")]
    assert "padding-bottom" not in base_rule
