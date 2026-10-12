"""v0.51.364 — a column that cannot collapse, and the controls the phone bump missed.

S1. `.libraries-table` is `table-layout: fixed` with SIX explicit column widths summing to
780px and `.lib-locations` as the no-width remainder. Below 780px of table space the
remainder is negative, so the column computed to ZERO at both `th` and `td` — and its
`overflow: hidden` put the folder paths out of reach even though `.table-scroll` swipes.
Measured at 375: every other column kept its width, LOCATIONS got 0 of the 105 its header
alone needs. `#library-table` already had this floor (v0.50.58); this is the same one, one
table over.

S2. v0.51.6 set a 24px tap floor for phones and bumped a NAMED list to 30px. Measured at
375, the list was short: `.btn-tiny` 23, `.dlg-close` 22, `.ops-drawer-close` 22,
`.op-pill` 22, `button.lib-flag-pill` 19 (the A / 4K role toggles), and every checkbox at
the browser's 13x13 — including the MGD one, which includes or excludes a whole Plex
section. `.btn-tiny` grows through its PADDING token rather than a min-height, so
`--btn-tiny-h` — what `.info-ctl-label` and `.loud-ctl-label` centre a control line
against — follows it instead of drifting.

Left deliberately: the brand lockup (23px, a nav affordance inside a 30px row) and inline
prose links (15px), which cannot take a 30px box without breaking the sentence.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
OPS = (REPO / "app" / "web" / "static" / "ops.css").read_text()
FLOOR = 24  # the WCAG-AA floor v0.51.6 names


def _media_block(src: str, query: str) -> str:
    """Every top-level block for `query`, concatenated."""
    blocks = re.findall(rf"@media \({re.escape(query)}\) \{{(.*?)\n\}}", src, re.S)
    assert blocks, f"no @media ({query}) block"
    return "\n".join(blocks)


def _px(value: str) -> float:
    m = re.search(r"(-?\d+(?:\.\d+)?)px", value)
    assert m, f"not a px length: {value!r}"
    return float(m.group(1))


# ── S1 ────────────────────────────────────────────────────────────────────

def test_the_locations_column_cannot_be_squeezed_to_nothing():
    """The floor is derived from the columns that DO have widths, so it stays true when
    one of them changes — a hardcoded 1040 would not."""
    fixed = re.findall(r"^\.lib-col-[a-z]+\s*\{[^}]*?width:\s*(\d+)px", CSS, re.M)
    assert len(fixed) >= 5, f"the libraries table's fixed columns moved: {fixed}"
    total = sum(int(x) for x in fixed)
    block = _media_block(CSS, "max-width: 1080px")
    m = re.search(r"\.libraries-table \{[^}]*min-width:\s*(\d+)px", block)
    assert m, "v0.51.364: .libraries-table has no min-width floor — LOCATIONS collapses"
    floor = int(m.group(1))
    assert floor > total, (
        f"v0.51.364: the floor ({floor}px) must exceed the sum of the columns that have "
        f"widths ({total}px), or the no-width LOCATIONS column gets what is left: nothing"
    )
    assert floor - total >= 150, (
        f"v0.51.364: {floor - total}px is not a usable LOCATIONS column — the header "
        f"alone measures 105px"
    )


def test_locations_is_still_the_remainder_column():
    """The arithmetic above assumes it. A width here would change what the floor means."""
    assert not re.search(r"^\.lib-locations\s*\{[^}]*width:", CSS, re.M | re.S), (
        "v0.51.364: .lib-locations took a width — re-derive the floor if so"
    )


# ── S2 ────────────────────────────────────────────────────────────────────

PHONE_CONTROLS = (
    (".dlg-close", CSS),
    ("button.lib-flag-pill", CSS),
    (".op-pill", OPS),
    (".ops-drawer-close", OPS),
)


def test_every_bumped_control_reaches_the_floor():
    for sel, src in PHONE_CONTROLS:
        block = _media_block(src, "max-width: 600px")
        m = re.search(rf"{re.escape(sel)} \{{([^}}]*)\}}", block)
        assert m, f"v0.51.364: {sel} lost its phone bump"
        body = m.group(1)
        h = re.search(r"(?:min-)?height:\s*([^;]+);", body)
        assert h and _px(h.group(1)) >= FLOOR, (
            f"v0.51.364: {sel} is under the {FLOOR}px floor v0.51.6 names: {body!r}"
        )


def test_checkboxes_reach_the_floor():
    block = _media_block(CSS, "max-width: 600px")
    m = re.search(r'input\[type="checkbox"\] \{([^}]*)\}', block)
    assert m, "v0.51.364: the checkbox bump is gone — the MGD toggle goes back to 13x13"
    body = m.group(1)
    for prop in ("width", "height"):
        v = re.search(rf"{prop}:\s*([^;]+);", body)
        assert v and _px(v.group(1)) >= FLOOR, f"{prop} under the floor: {body!r}"


def test_btn_tiny_grows_through_the_token_not_a_min_height():
    """A min-height would leave --btn-tiny-h (the control-line height .info-ctl-label and
    .loud-ctl-label centre against) describing a button that is no longer that tall."""
    block = _media_block(CSS, "max-width: 600px")
    assert not re.search(r"\.btn-tiny[^{]*\{[^}]*min-height", block), (
        "v0.51.364: .btn-tiny must grow by its padding token, so --btn-tiny-h follows it"
    )
    m = re.search(r":root \{[^}]*--btn-tiny-pad-y:\s*([\d.]+)px", block)
    assert m, "v0.51.364: the phone padding token is gone"
    pad = float(m.group(1))
    tiny = _px(re.search(r"--t-tiny:\s*([^;]+);", CSS).group(1))
    lh = float(re.search(r"--btn-tiny-lh:\s*([\d.]+)", CSS).group(1))
    computed = tiny * lh + 2 * pad + 2
    assert computed >= FLOOR, (
        f"v0.51.364: the phone .btn-tiny computes to {computed}px, under the {FLOOR}px floor"
    )
