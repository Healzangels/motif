"""v0.51.243 — the loudness controls sit in the value column like every other dd.

Reported: "the loudness section in the info card is not centered well, making the
info card look off."

v0.51.207 gave .loud-controls `grid-column: 1 / -1` to escape a squeeze. That
threw it onto its OWN row starting at the LABEL edge, so its <dt> ("action" /
"cannot level" / "which cut?") rendered as a lone word with an empty value cell
beside it, and the whole block sat ~156px left of every other value — a broken
row, not a section.

The room argument was re-MEASURED in a browser against the real CSS before
changing it (720px drawer, 375px mobile):

    720px : value column = 523px, buttons = 366px -> one row, no overflow
    375px : value column = 187px -> .loud-ctl-row's existing flex-wrap drops
            them to a row each (187px / 179px), no overflow

.dlg-grid is never overridden inside a media query, so `140px 1fr` — and
therefore this placement — holds at every width.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def _loud_controls_rule() -> str:
    i = APP_CSS.index(".loud-controls {")
    return APP_CSS[i:APP_CSS.index("}", i) + 1]


def test_controls_sit_in_the_value_column():
    rule = _loud_controls_rule()
    assert "grid-column: 2" in rule, (
        "the controls must occupy the value column so the card keeps ONE "
        "alignment; spanning put them ~156px left of every other dd")


def test_controls_do_not_span_both_columns():
    rule = _loud_controls_rule()
    assert "1 / -1" not in rule, (
        "grid-column: 1 / -1 is what stranded the <dt> with an empty value "
        "cell and misaligned the block (the reported symptom)")


def test_the_dt_is_a_real_term_not_a_bare_div():
    """Regression lock on v0.51.216: the controls must keep a preceding <dt> so a
    screen reader doesn't reach the stepper and two audio-rewriting buttons with
    no label. Placement changed; the term must not be dropped."""
    for m in re.finditer(r'<dd class="loud-controls"', APP_JS):
        before = APP_JS[max(0, m.start() - 200):m.start()]
        assert re.search(r"<dt[^>]*>[^<]+</dt>\s*$", before), (
            "every .loud-controls dd must be immediately preceded by its <dt>")


def test_every_controls_dd_is_inside_the_dlg_grid_contract():
    """A dl takes dt/dd groups OR div children, never intermixed (the v0.51.216
    content-model fix). Assert the controls are still emitted as a dd."""
    assert '<div class="loud-controls"' not in APP_JS, (
        "a bare div sibling inside <dl class='dlg-grid'> is the content-model "
        "violation v0.51.216 removed")
    assert APP_JS.count('<dd class="loud-controls"') >= 3, (
        "expected the ambiguous-picker, cannot-level and action variants")


def test_no_leftover_margin_top_offsetting_the_label():
    """With label and controls sharing a row, a margin-top on the dd would push
    the controls below their own term again."""
    assert "margin-top" not in _loud_controls_rule(), (
        "margin-top re-breaks the label/controls baseline now they share a row")
