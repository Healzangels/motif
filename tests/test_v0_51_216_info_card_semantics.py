"""v0.51.216 — info-card semantics: valid dl, distinguishable chip, honest RAW copy.

Three findings from the ultra review, all in the v0.51.207 loudness card work:

  1. The controls block was a bare <div> intermixed with <dt>/<dd> siblings inside
     <dl class="dlg-grid">. The HTML content model allows a dl to contain EITHER dt/dd
     groups OR div children, never both — it laid out fine, so nothing complained, but a
     screen reader reached the target stepper, // PREVIEW AT TARGET, // LEVEL THIS THEME
     and // UNDO LEVELING with no preceding term, and the over-ceiling explanation was
     orphaned from any label.
  2. The card's loudness chip renders as bare letters immediately beside the 4K badge in
     the same <h3>, and .tier-badge-loud is byte-identical to .tier-badge-4k. Both the JS
     and CSS comments justify that shared amber ONLY because the library marker is a
     GLYPH — a justification bare letters destroy.
  3. The RAW copy claimed the theme was "measured but not yet leveled". _loudness_marker
     returns "raw" for a local file with NO measurement at all, so the claim was false for
     exactly the rows whose next action is // MEASURE NOW.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def _loudness_rows() -> str:
    i = APP_JS.index("let controls;")
    return APP_JS[i:APP_JS.index("const _grp = (title, rows)", i)]


# ── 1. the dl content model ──────────────────────────────────────────────────

def test_controls_ride_in_a_dt_dd_pair():
    """Every branch must emit a labelled term + definition, not a bare div sibling."""
    blk = _loudness_rows()
    assert "<dt>action</dt><dd class=\"loud-controls\"" in blk
    assert "<dt>cannot level</dt><dd class=\"loud-controls\"" in blk
    assert '`<div class="loud-controls' not in blk, (
        "a bare div is not valid as a sibling of dt/dd inside a dl")


def test_no_branch_leaves_an_unclosed_dd():
    """Each of the three branches opens exactly one dd and closes it."""
    blk = _loudness_rows()
    assert blk.count('<dd class="loud-controls"') == 3
    assert blk.count("</dd>`") == 3


def test_the_over_ceiling_warning_keeps_its_own_colour():
    """accent-red moved to an inner span deliberately: `.dlg-grid dd` (0,1,1) out-specifies
    a bare `.accent-red` (0,1,0), so leaving it on the dd would silently repaint the
    warning as ordinary value text."""
    blk = _loudness_rows()
    assert '<span class="accent-red">' in blk
    dd = blk[blk.index("<dt>cannot level</dt>"):]
    assert 'class="loud-controls accent-red"' not in dd


def test_loud_controls_sit_in_the_value_column():
    """v0.51.243 REPLACES this guard's original premise. It used to assert the full-width
    span "or the v0.51.207 de-squishing regresses" — measured false: at the real 720px
    drawer the value column is 523px against 366px of buttons, and the span was instead
    what stranded the <dt> alone in the label column and misaligned the whole block.

    What still has to hold is the anti-squish MECHANISM, which was never the span: the
    steppers drop the .btn-tiny min-width, and .loud-ctl-row wraps instead of overflowing
    when the column really is narrow. Those are pinned here alongside the placement."""
    rule = APP_CSS[APP_CSS.index(".loud-controls {"):]
    rule = rule[:rule.index("}")]
    assert "grid-column: 2" in rule
    assert "1 / -1" not in rule
    assert ".loud-stepper .btn-tiny" in APP_CSS
    assert ".loud-ctl-row { display: flex; flex-wrap: wrap;" in APP_CSS


# ── 2. the chip is distinguishable from the 4K badge ─────────────────────────

def test_loud_and_4k_badges_are_still_colour_identical():
    """Pins the PREMISE of the fix rather than assuming it. These two rules are meant to
    share the amber — it ENCODES state and is fixed across themes — so the collision must
    be resolved by SHAPE, not by re-colouring one of them."""
    def _rule(sel):
        i = APP_CSS.index(sel)
        return APP_CSS[i:APP_CSS.index("}", i)]
    loud, k4 = _rule(".tier-badge-loud {"), _rule(".tier-badge-4k {")
    for prop in ("color: var(--amber-bright)", "border: 1px solid var(--amber)",
                 "background: rgba(var(--amber-rgb), 0.14)"):
        assert prop in loud and prop in k4, prop


def test_the_card_chip_carries_the_meter_glyph():
    """With identical colour, the glyph is the only thing separating this chip from the 4K
    badge sitting next to it in the same <h3> — and it matches the library row marker."""
    i = APP_JS.index("const _loudChip")
    fn = APP_JS[i:APP_JS.index("</span>`;", i)]
    assert "▂▄▆ ${spec[1]}" in fn
    # the same glyph the library row uses, so one symbol means one thing on both surfaces
    assert '">▂▄▆</span>' in APP_JS


# ── 3. RAW never claims a measurement that doesn't exist ─────────────────────

def test_card_raw_copy_branches_on_whether_it_was_measured():
    i = APP_JS.index("const _loudChip")
    fn = APP_JS[i:APP_JS.index("</span>`;", i)]
    assert "not measured yet" in fn, "an un-audited raw row must say so"
    assert re.search(r"raw:\s*li !== null", fn), "the RAW copy must branch on the measurement"


def test_library_raw_tooltip_makes_no_measurement_claim():
    """v0.51.202 pops loudness_i from the library payload, so this surface cannot tell a
    measured row from an un-audited one — it must not assert either."""
    i = APP_JS.index("LOUD_MARK = {")
    m = APP_JS[i:APP_JS.index("};", i)]
    raw_line = [ln for ln in m.splitlines() if ln.strip().startswith("raw:")][0]
    assert "measured but not yet leveled" not in raw_line
    assert "not leveled" in raw_line


def test_loudness_i_is_still_absent_from_library_rows():
    """The premise of the test above — if a future tag ships loudness_i to the rows, the
    library tooltip could become specific again and this reasoning should be revisited."""
    api_py = (REPO / "app" / "web" / "api.py").read_text()
    assert 'it.pop("loudness_i", None)' in api_py
