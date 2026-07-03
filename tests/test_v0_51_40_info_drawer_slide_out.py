"""v0.51.40 — the // MOTIF INFO left drawer slides away on close.

the user: "the info card drawer doesn't have the same slide away or retract
effect like our status bar drawer can we add that so it matches".

v0.51.32 made the info card a left drawer that slides IN (@starting-style) but
close()d instantly — the overlay/display allow-discrete slide-OUT froze
mid-transition in-browser and was dropped. This mirrors the ops-drawer's retract
instead: closeInfoDialog adds `.is-closing` (transform slide-out + scrim fade),
then delays dlg.close() by the 0.28s transition (ops.js closeDrawer uses the same
remove-class + 280ms display:none pattern). Reopening mid-retract cancels the
pending close in showModalNoFocusRing so showModal() never hits an [open] dialog;
Esc routes through the same slide via a cancel-event interceptor.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def _fn(name: str) -> str:
    i = APP_JS.index(name)
    return APP_JS[i:APP_JS.index("\n  function ", i + 1)]


# ── CSS: the .is-closing out-state mirrors the @starting-style enter ──

def test_is_closing_slides_panel_out_and_fades():
    assert ".dlg.dlg-drawer-left.is-closing { transform: translateX(-100%); opacity: 0; }" in APP_CSS
    # the scrim fades too, matching the ops-drawer's scrim opacity 0 on close.
    assert ".dlg.dlg-drawer-left.is-closing::backdrop { opacity: 0; }" in APP_CSS
    # the exit is the mirror of the enter (@starting-style) — same off-screen frame.
    assert ".dlg.dlg-drawer-left[open] { opacity: 0; transform: translateX(-100%); }" in APP_CSS


# ── JS: closeInfoDialog runs the slide then the delayed close ──────────

def test_close_adds_is_closing_then_delays_close():
    fn = _fn("function closeInfoDialog()")
    assert "classList.add('is-closing')" in fn
    assert "_infoCloseTimer = setTimeout(" in fn
    assert ", 280)" in fn                      # matches the 0.28s CSS transition
    # the actual close still happens (v0.51.32 machinery pin) — inside the timer.
    assert "if (typeof dlg.close === 'function') dlg.close();" in fn
    # scoped to the drawer; a non-drawer .dlg keeps the instant close.
    assert "classList.contains('dlg-drawer-left')" in fn
    # audio teardown (v1.15.3) still runs BEFORE the slide, not lost.
    assert "querySelectorAll('audio')" in fn
    assert fn.index("querySelectorAll('audio')") < fn.index("classList.add('is-closing')")


def test_close_is_idempotent_while_retracting():
    # a 2nd close during the 280ms slide must not re-fire / re-schedule.
    fn = _fn("function closeInfoDialog()")
    assert "if (dlg._infoCloseTimer) return;" in fn


# ── JS: reopening mid-retract cancels the pending close ───────────────

def test_reopen_cancels_pending_retract():
    fn = _fn("function showModalNoFocusRing(dlg)")
    assert "if (dlg._infoCloseTimer) {" in fn
    assert "clearTimeout(dlg._infoCloseTimer)" in fn
    assert "classList.remove('is-closing')" in fn
    # guard showModal() against an already-[open] dialog (would throw).
    assert "if (!dlg.open) dlg.showModal();" in fn


# ── JS: Esc retracts with the slide too (not a native snap) ───────────

def test_esc_routes_through_the_slide():
    fn = _fn("function bindInfoDialog()")
    assert "addEventListener('cancel'" in fn
    anchor = fn.index("addEventListener('cancel'")
    stmt = fn[anchor:anchor + 120]
    assert "preventDefault()" in stmt and "closeInfoDialog()" in stmt
