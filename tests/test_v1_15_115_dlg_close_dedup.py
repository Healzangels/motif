"""v1.15.115 — consolidate duplicate `.dlg-close` rules.

Audit pass surfaced two `.dlg-close` rule blocks in app.css with
overlapping properties + same specificity. The second won the
cascade for every overlapping property, silently shrinking the
× from 22px to 13px on 4 of 5 dialogs (the four that nest the
close button inside a `.dlg-head` flex header).

## The two rules

Pre-fix:

  Rule A (~line 1898):
    .dlg-close {
      background: transparent; border: 0;
      color: var(--fg-dim); font-size: 22px;
      padding: 0 6px; line-height: 1;
    }

  Rule B (~line 2073):
    .dlg-close {
      position: absolute; top: 8px; right: 8px;
      background: none; border: none;
      color: var(--fg-mute);
      cursor: pointer; padding: var(--gap-2) var(--gap-3);
      font-family: var(--font-mono);
      font-size: var(--t-base);  ← 13px, overrode A's 22px
    }

The cascade: same specificity, later rule wins → font-size
13px on all 5 dialogs. Visual symptom: tiny × on info /
upload / override / manual-url dialogs.

## Fix

  - Rule A becomes the canonical base (`.dlg-close`) — 22px ×,
    no absolute positioning. Used by the 4 dialogs that nest
    the close inside `.dlg-head` (flex layout puts the × at
    the header's right edge).
  - Rule B replaced by a contextual selector `.dlg-body >
    .dlg-close` — applies only when the close button is a
    direct child of `.dlg-body` (settings.html's #new-token-dlg
    is the only dialog with this shape). Adds `position:
    absolute; top/right: var(--gap-2)`.
  - `.dlg-body` rule extended with `position: relative` so the
    absolute child anchors to .dlg-body's box.

Visual: × now renders at the intended 22px on all dialogs;
positioning unchanged.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = REPO / "app" / "web" / "static" / "app.css"


def test_only_one_base_dlg_close_rule():
    """No more two-rule cascade fight. The canonical base rule
    is `.dlg-close {`; the absolute-corner variant uses the
    descendant selector `.dlg-body > .dlg-close {`."""
    src = APP_CSS.read_text()
    src = re.sub(r"/\*.*?\*/", "", src, flags=re.DOTALL)
    # Count standalone `.dlg-close {` rules (not preceded by ` > `).
    matches = list(re.finditer(r"(?<!> )\.dlg-close\s*\{", src))
    assert len(matches) == 1, (
        f"v1.15.115: expected exactly 1 base `.dlg-close {{` rule, "
        f"found {len(matches)}. Pre-fix two rules with the same "
        "specificity silently overrode each other."
    )


def test_dlg_close_base_keeps_22px_font_size():
    """The base rule must specify the visually-correct × size."""
    src = APP_CSS.read_text()
    src = re.sub(r"/\*.*?\*/", "", src, flags=re.DOTALL)
    # Find the base rule body.
    m = re.search(r"\n\.dlg-close\s*\{([^}]+)\}", src)
    assert m, ".dlg-close base rule not found"
    body = m.group(1)
    assert "font-size: 22px" in body, (
        "v1.15.115: the × close button must remain 22px. Pre-fix "
        "the second cascade-fight rule shrunk it to 13px."
    )


def test_dlg_body_corner_variant_retired_v1_17_8():
    """v1.17.8 retired the absolute-corner variant — its only
    consumer (new-token-dlg) was refactored to nest its close
    button inside a `.dlg-head` like every other dialog, so the
    one-off rule has no remaining callsite. Pin the absence so
    a future re-introduction surfaces in tests.

    Note: pre-v1.17.8 this test asserted the rule's EXISTENCE.
    The v1.17.8 retirement comment in app.css still names the
    selector by string (archaeology); strip comments before
    asserting absence of the rule itself."""
    src = APP_CSS.read_text()
    src = re.sub(r"/\*.*?\*/", "", src, flags=re.DOTALL)
    assert ".dlg-body > .dlg-close {" not in src, (
        "v1.17.8: the v1.15.115 absolute-corner variant must "
        "stay retired — its only consumer (new-token-dlg) was "
        "refactored to use a .dlg-head."
    )


def test_dlg_body_has_position_relative():
    """Without `position: relative` on .dlg-body, the absolute
    child would anchor to the dialog viewport (which works on
    Chromium but differs across UAs). The explicit anchor matches
    the pre-fix behavior the absolute positioning was tuned for."""
    src = APP_CSS.read_text()
    src = re.sub(r"/\*.*?\*/", "", src, flags=re.DOTALL)
    # Find a .dlg-body rule body that includes position: relative.
    rules = re.findall(r"\.dlg-body\s*\{([^}]+)\}", src)
    assert any("position: relative" in r for r in rules), (
        "v1.15.115: at least one .dlg-body rule must set "
        "`position: relative` so the absolute-corner close child "
        "anchors correctly."
    )
