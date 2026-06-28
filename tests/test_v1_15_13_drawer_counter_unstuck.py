"""v1.15.13 — drawer big-counter stuck on initial value during long ops.

the user v1.15.12 repro: REPROBE FAILURES running, drawer big number
shows "130 / 2,127" and stays at 130 across many polls. Activity log
entries below it kept ticking (07:39:43 PM 150/2127 probed, etc.) and
the elapsed timer kept updating, but the big counter at the top was
frozen. Page refresh would jump it to a fresh value (e.g. 210/2127),
then it would stick again.

## Root cause

Two functions interact to produce the freeze:

1. `_updateCardInPlace` was setting only `data-op-counter-target` on
   the counter span — never `textContent` directly. The comment
   explicitly said "let tickCounters() smooth-animate the visible
   value toward it".

2. `tickCounters()` reads:
       const target = +el.getAttribute('data-op-counter-target') || 0;
       const current = +el.getAttribute('data-op-counter-current') || target;
   The `|| target` fallback fires whenever the current attr is unset
   (i.e. after every initial render — the template literal sets
   textContent inline but never seeds data-op-counter-current). With
   `current === target`, the early-return at the next line fires
   without writing textContent. So textContent stays on whatever the
   initial render put there.

Activity log + elapsed timer use direct textContent writes via
`metaUpdate(...)`, which is why ONLY the big counter sticks.

## Fix

Snap the counter to its new value inside `_updateCardInPlace` —
update the target attr, the current attr (so subsequent ticks have
a clean baseline), AND the textContent. Loses the smooth
interpolation effect on each poll, but correctness wins.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
OPS_JS = REPO / "app" / "web" / "static" / "ops.js"


def test_update_card_writes_textcontent_when_target_changes():
    """Pin the v1.15.13 fix: the counter element's textContent must
    be written directly when the target changes. Pre-fix the
    textContent was ONLY updated by tickCounters(), which short-
    circuited because of the `current || target` fallback."""
    src = OPS_JS.read_text()
    fn_start = src.index("function _updateCardInPlace(")
    fn_end = src.index("\n  function ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    # The counter section must write textContent directly.
    counter_anchor = fn_body.index(".op-card-counter-current")
    counter_block = fn_body[counter_anchor:counter_anchor + 800]
    assert "cur.textContent = fmtNum(op.stage_current" in counter_block, (
        "v1.15.13: counter must update textContent directly, not "
        "rely on tickCounters animation (which short-circuits "
        "because data-op-counter-current is never seeded)"
    )


def test_update_card_seeds_counter_current_attribute():
    """Also seed `data-op-counter-current` so subsequent
    tickCounters reads have a real starting point. Without this,
    the next time tickCounters runs it'd see no current attr and
    re-trigger the fallback bug — though the textContent fix above
    masks it, seeding the attr keeps the contract honest."""
    src = OPS_JS.read_text()
    fn_start = src.index("function _updateCardInPlace(")
    fn_end = src.index("\n  function ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    counter_anchor = fn_body.index(".op-card-counter-current")
    counter_block = fn_body[counter_anchor:counter_anchor + 800]
    assert "setAttribute('data-op-counter-current'" in counter_block, (
        "v1.15.13: counter element must seed data-op-counter-current "
        "alongside data-op-counter-target so tickCounters has a "
        "consistent baseline"
    )


def test_counter_update_is_gated_on_target_change():
    """The textContent write must still be gated on the
    target-actually-changed check — otherwise we'd be re-writing
    textContent every poll even when stage_current is unchanged,
    which would defeat the v1.14.81 hash-skip flicker fix."""
    src = OPS_JS.read_text()
    fn_start = src.index("function _updateCardInPlace(")
    fn_end = src.index("\n  function ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    counter_anchor = fn_body.index(".op-card-counter-current")
    counter_block = fn_body[counter_anchor:counter_anchor + 800]
    # The if-guard wrapping the writes.
    assert "if (cur.getAttribute('data-op-counter-target') !== target)" in counter_block, (
        "v1.15.13 fix must stay inside the target-changed gate so "
        "unchanged polls don't repaint textContent (would re-introduce "
        "the v1.14.81 flicker bug on idle ops)"
    )
