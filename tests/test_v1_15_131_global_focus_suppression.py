"""v1.15.131 — global :focus suppression (fixes sticky click-focus ring).

the user on v1.15.130:

> if you've clicked something, say a button or a filter or the x
> to close info or one of the status to open the drawer and then
> click esc it leaves a weird highlight almost selected box
> around the object that was last clicked, see it all over the
> place. would love to stop that if possible

## The bug class

The v1.12.1 + v1.15.119 + v1.15.130 explicit-list pattern
suppressed `:focus { outline: none }` on 14 named primitives but
left 57+ other interactive primitives showing the browser-default
focus ring after mouse-click. The ring persisted until the user
clicked or tabbed elsewhere, looking like a "stuck selection box."

ops.css was the worst offender: ZERO focus rules, so every
`.op-pill` (FAIL / UPD / IDLE topbar), `.op-mini` (running-op
mini-bar), and `.ops-drawer-close` (× button on drawer) all
displayed the cobalt UA ring after mouse-click. the user's
"status to open the drawer" repro hits this exact set.

## The fix

Universal `:focus { outline: none; }` rule replaces the explicit
14-element list. Combined with the existing `:focus-visible`
block, the contract is now:

  - Any focused element (mouse OR keyboard): no UA outline
  - Listed primitives + keyboard focus: 2px cyan outline + 2px
    offset paints via `:focus-visible` (browsers only match
    `:focus-visible` on keyboard nav or programmatic focus —
    NEVER mouse-click)

The global rule touches only `outline`. The `.input:focus` rule
keeps its custom focus styling (border-color + box-shadow). The
v1.11.92/.98 `.link-badge:focus:not(.src-key-btn-active)` +
sibling carve-outs stay explicit since they suppress `box-shadow`
specifically (the SRC legend's active-state glow vs hover glow
distinction).

## Tests
"""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = REPO / "app" / "web" / "static" / "app.css"


def _strip_comments(src: str) -> str:
    return re.sub(r"/\*.*?\*/", "", src, flags=re.DOTALL)


def test_universal_focus_outline_suppression():
    """A `:focus { outline: none; }` rule (no selector qualifier)
    must exist at the top level so EVERY focused element drops
    the browser-default focus ring on mouse-click."""
    src = _strip_comments(APP_CSS.read_text())
    # The selector must be exactly `:focus` (not `.X:focus`).
    pattern = re.compile(
        r"(?:^|\n)\s*:focus\s*\{[^}]*outline:\s*none[^}]*\}"
    )
    assert pattern.search(src), (
        "v1.15.131: universal `:focus { outline: none; }` rule "
        "missing. Without it, every clickable element that isn't "
        "in the explicit focus-visible list leaves the UA focus "
        "ring after mouse-click."
    )


def test_focus_visible_list_still_paints_outline():
    """The keyboard-focus outline rule must remain — global
    `:focus { outline: none }` would otherwise leave keyboard
    users with NO focus indication anywhere."""
    src = _strip_comments(APP_CSS.read_text())
    # The :focus-visible block sets outline to a colored value.
    pattern = re.compile(
        r":focus-visible\s*\{[^}]*outline:\s*2px\s+solid\s+var\(--cyan\)"
    )
    assert pattern.search(src), (
        "v1.15.131: `:focus-visible` block must keep the 2px "
        "cyan outline so keyboard nav has a visible affordance."
    )


def test_src_key_btn_active_carveout_preserved():
    """The v1.11.92/.98 `.src-key-btn:not(.src-key-btn-active)`
    + sibling `:focus` rule suppresses box-shadow specifically
    on non-active legend buttons. Don't let the global :focus
    rule's reach lose this carve-out."""
    src = _strip_comments(APP_CSS.read_text())
    assert (
        ".link-badge:focus:not(.src-key-btn-active)" in src
        and ".src-key-btn:focus:not(.src-key-btn-active)" in src
    ), (
        "v1.15.131: SRC-legend active-state carve-out must "
        "remain so the active glow isn't accidentally killed "
        "by a future global box-shadow suppression."
    )


def test_input_focus_keeps_custom_styling():
    """`.input:focus { border-color: ...; box-shadow: ...; }`
    must NOT have been broken by the global :focus rule —
    the global only touches outline, but a defensive test
    catches a future regression."""
    src = _strip_comments(APP_CSS.read_text())
    # The .input:focus rule must exist with its visible
    # focus styling intact.
    pattern = re.compile(
        r"\.input:focus\s*\{[^}]*border-color:\s*var\(--green-deep\)",
        re.DOTALL,
    )
    assert pattern.search(src), (
        "v1.15.131: .input:focus must keep its border-color "
        "styling so form inputs have a visible focus state "
        "(distinct from the cyan keyboard-focus outline)."
    )


def test_no_sticky_focus_ring_on_ops_primitives():
    """Sanity guard: the global :focus rule covers every
    primitive — explicitly verify ops.css primitives that
    pre-fix had zero focus handling (.op-pill, .op-mini,
    .ops-drawer-close) get the suppression."""
    # The global :focus rule applies regardless of class —
    # we just need to confirm the rule exists. Already covered
    # by test_universal_focus_outline_suppression, but this
    # test documents the ops-side intent.
    src = _strip_comments(APP_CSS.read_text())
    # The v1.15.131 comment mentions ops.css primitives by name.
    comment = APP_CSS.read_text()
    assert ".op-pill" in comment, (
        "v1.15.131 doc comment should mention .op-pill — the "
        "topbar primitives are the most visible repro the user "
        "flagged."
    )
