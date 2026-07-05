"""v0.51.32 — the // MOTIF INFO card renders as a LEFT overlay drawer.

the user: "make the info card into a left drawer instead of what it is currently
so it's more in theme with the status bar drawer, would like it have everything
it current has in it but a drawer instead".

The card STAYS a native <dialog> (showModal keeps the scrim, Esc, focus trap +
all the v1.12.x open/close + audio-teardown JS) — only the layout + entry
animation change via a `dlg-drawer-left` modifier that mirrors the right-pinned
.ops-drawer: left-pinned full-height panel, inner-edge (right) green-deep border,
slide-in reusing the .dlg @starting-style pattern, sticky head, scrolling body,
ops-drawer-matched scrim. Scoped so every OTHER .dlg stays a centered modal.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
BASE = (REPO / "app" / "web" / "templates" / "base.html").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def _rule(css: str, sel: str) -> str:
    i = css.index(sel)
    return css[i:css.index("}", i) + 1]


# ── the info dialog opts into the drawer, and stays a <dialog> ──────────

def test_info_dlg_is_a_left_drawer_dialog():
    # still a native <dialog> (keeps showModal/Esc/focus/audio machinery) …
    # v0.51.84: attr-tolerant — the tag gained aria-labelledby (a11y) and may
    # wrap, so pin the class+id pair, not the whole one-line tag with its `>`.
    assert 'class="dlg dlg-drawer-left" id="info-dlg"' in BASE
    # … and NO other dialog opts into the drawer (glossary etc. stay modals):
    # the drawer class-attr appears on exactly one element.
    assert BASE.count('class="dlg dlg-drawer-left"') == 1


# ── the drawer CSS mirrors the ops-drawer (left-pinned, slide, scrim) ──

def test_drawer_is_left_pinned_full_height():
    r = _rule(APP_CSS, ".dlg.dlg-drawer-left {")
    assert "left: 0;" in r and "top: 0;" in r
    assert "right: auto;" in r          # not stretched to the right edge
    assert "height: 100dvh;" in r
    # inner-edge border + page-ward drop, mirroring the ops-drawer's border-left
    assert "border-right: 1px solid var(--green-deep);" in r


def test_drawer_display_is_gated_on_open():
    # display:flex ONLY when [open] so the UA's dialog:not([open]){display:none}
    # still hides the closed drawer (setting it unconditionally would leak it).
    assert "display: none" not in _rule(APP_CSS, ".dlg.dlg-drawer-left {")
    assert ".dlg.dlg-drawer-left[open] { display: flex;" in APP_CSS


def test_drawer_slides_in_via_starting_style():
    # the proven .dlg @starting-style entry pattern, but along X.
    assert "transform: translateX(0);" in _rule(APP_CSS, ".dlg.dlg-drawer-left {")
    assert ".dlg.dlg-drawer-left[open] { opacity: 0; transform: translateX(-100%); }" in APP_CSS


def test_drawer_scrim_matches_ops_drawer():
    # v0.51.33: both scrims pull the shared --drawer-scrim token (parity by
    # reference, not two copies of the literal).
    r = _rule(APP_CSS, ".dlg.dlg-drawer-left::backdrop {")
    assert "var(--drawer-scrim)" in r
    assert "backdrop-filter: none;" in r
    OPS_CSS = (REPO / "app" / "web" / "static" / "ops.css").read_text()
    assert "background: var(--drawer-scrim);" in OPS_CSS   # ops-drawer scrim too
    assert "--drawer-scrim:" in APP_CSS                    # token defined in :root


def test_drawer_head_is_sticky_and_body_scrolls():
    head = _rule(APP_CSS, ".dlg.dlg-drawer-left .dlg-head {")
    assert "position: sticky;" in head and "top: 0;" in head
    body = _rule(APP_CSS, ".dlg.dlg-drawer-left .dlg-body {")
    assert "overflow-y: auto;" in body and "flex: 1 1 auto;" in body


def test_drawer_head_no_redundant_border_override():
    # v0.51.33: the drawer head inherits border-bottom from the base .dlg-head —
    # it must NOT re-declare it (dead override the review flagged).
    head = _rule(APP_CSS, ".dlg.dlg-drawer-left .dlg-head {")
    assert "border-bottom" not in head


def test_drawer_is_full_screen_on_mobile():
    # v0.51.33 (the user): on <=600px the info card fills the viewport again
    # (v0.50.50 intent), re-stated at drawer specificity so it beats the base
    # drawer rule on source order. Kept inside the single canonical mobile block.
    assert ".dlg.dlg-drawer-left { width: 100vw; max-width: none; border-right: 0; }" in APP_CSS
    # the override must live inside the @media (max-width: 600px) block, AFTER the
    # mobile `.dlg` full-screen rule it complements.
    i_media = APP_CSS.index("@media (max-width: 600px) {")
    i_close = APP_CSS.index("\n}\n", i_media)
    assert i_media < APP_CSS.index(".dlg.dlg-drawer-left { width: 100vw;") < i_close


def test_no_duplicate_starting_style_block():
    # v0.51.33: the two-step edit had left two identical @starting-style blocks.
    assert APP_CSS.count(
        ".dlg.dlg-drawer-left[open] { opacity: 0; transform: translateX(-100%); }") == 1


def test_other_dialogs_stay_centered_modals():
    # the base .dlg keeps its centered-modal width — the drawer scoping must
    # not have converted every dialog.
    base = _rule(APP_CSS, ".dlg {")
    assert "max-width: 720px;" in base


# ── the open/close machinery is preserved + scrim-click closes ─────────

def test_info_dialog_machinery_preserved():
    # still opened as a modal + closed via the native dialog API (not a div).
    assert "showModalNoFocusRing(dlg)" in APP_JS
    assert "if (typeof dlg.close === 'function') dlg.close();" in APP_JS


def test_scrim_click_closes_the_drawer():
    # v0.51.32: added scrim-click-to-close matching the ops-drawer, routed
    # through closeInfoDialog so the audio-teardown + focus-blur still fire.
    i = APP_JS.index("function bindInfoDialog()")
    body = APP_JS[i:i + 1500]
    assert "if (e.target === dlg) closeInfoDialog();" in body
