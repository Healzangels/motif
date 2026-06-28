"""v1.22.37 — status-bar progress bars reach 100% at completion.

the user: "our progress bars jumping from some % complete to done or just onto
the next phase without ever hitting 100%."

Mechanism (ops.js): a finished op leaves the running set the instant it reaches
the final count, so (a) the drawer card drops the bar entirely once it's no
longer live (showLiveSections=false) and (b) the topbar mini-bar — which only
renders running ops — switches straight to idle. The 100% frame is never
rendered.

Fix:
- pctOf returns 100 when op.status === 'done' (the chokepoint for card +
  mini-bar + in-place update).
- finished real-bar cards render a full 100% bar instead of no bar.
- the mini-bar holds a 100% DONE flash for ~1.5s before going idle (mirroring
  the ✓ DONE button flash), tracked via _lastMiniOpId / _flashedMiniOpId.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
OPS_JS = (REPO / "app" / "web" / "static" / "ops.js").read_text()


def test_pctof_snaps_to_100_on_done():
    i = OPS_JS.index("function pctOf(op)")
    body = OPS_JS[i:i + 700]
    assert "op.status === 'done') return 100" in body, (
        "v1.22.37: pctOf must return 100 for a finished op")


def test_finished_card_renders_full_bar():
    # The done-card branch (where the live counter/bar were dropped) now
    # renders a 100% bar for real-bar ops.
    assert "if (!showLiveSections) {" in OPS_JS, (
        "v1.22.37: the finished-card branch must render a 100% bar, not drop it")
    i = OPS_JS.index("if (!showLiveSections) {")
    block = OPS_JS[i:i + 300]
    assert "_useRealBar(op)" in block and "width:100%" in block


def test_minibar_done_flash_state_and_timer():
    assert "_lastMiniOpId" in OPS_JS and "_flashedMiniOpId" in OPS_JS, (
        "v1.22.37: the mini-bar DONE flash needs last/flashed op tracking")
    # The flash fires only for a recently-finished real-bar op, on a ~1.5s timer.
    # Anchor on the guard expression (unique to the flash block, vs the word
    # "DONE flash" which also appears in the pctOf comment).
    i = OPS_JS.index("_flashedMiniOpId !== _lastMiniOpId")
    block = OPS_JS[i:i + 1000]
    assert "o.status === 'done'" in block
    assert "_useRealBar(doneOp)" in block
    assert "width:100%" in block
    assert ", 1500)" in block, "the DONE flash must clear after ~1.5s"


def test_minibar_running_render_tracks_and_cancels_flash():
    # A fresh running render must remember the op + cancel any pending flash
    # timer so a new op taking the slot isn't clobbered.
    assert "_lastMiniOpId = op.op_id" in OPS_JS
    assert "clearTimeout(_doneFlashTimer)" in OPS_JS


def test_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
