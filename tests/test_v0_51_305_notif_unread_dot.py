"""v0.51.305 — the inbox unread dot: mark ONE row read without navigating.

Every per-row read path had a side effect: a row click marks read but
click-throughs to the INFO card (v0.51.151), the × deletes the row.
Unread rows now render a dot button that routes to the v0.51.266
markRead and returns BEFORE the click-through branch. These pins hold
the three load-bearing invariants: the control exists, the click branch
short-circuits navigation, and the keydown nested-button guard covers
it (else the dot's native Enter/Space activation is doubled by the row
branch, which navigates). CSS-side: the dot is visible ONLY while the
row is .unread, so markRead's class flip retires the affordance without
DOM surgery.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
OPS_CSS = (REPO / "app" / "web" / "static" / "ops.css").read_text()


def _notif_click_block() -> str:
    # the notif drawer's delegate is the one AFTER rowHtml (7296 is /queue's);
    # it ends at its click-through call.
    i = APP_JS.index("listEl.addEventListener('click'",
                     APP_JS.index("function rowHtml("))
    return APP_JS[i:APP_JS.index("openNotifRow(row);", i)]


def test_row_markup_renders_the_dot_button():
    blk = APP_JS[APP_JS.index("function rowHtml("):
                 APP_JS.index("function renderEmpty(")]
    # v0.51.307 (audit): bind the checks to the dot's OWN tag — the first
    # draft's three independent substring checks were satisfied by the
    # sibling ×'s type="button", so a <span class="notif-dot"> passed.
    i = blk.index('class="notif-dot"')
    tag = blk[blk.rindex("<", 0, i):blk.index(">", i)]
    assert tag.startswith("<button"), (
        "the dot must be a REAL button — native Enter/Space activation is "
        "what the keydown guard defers to; a span kills keyboard mark-read")
    assert 'type="button"' in tag and 'aria-label="Mark read"' in tag


def test_dot_click_marks_read_and_returns_before_the_click_through():
    blk = _notif_click_block()
    d = blk.index("closest('.notif-dot')")
    nav = blk.index("closest('.notif-row.notif-clickable')")
    assert d < nav, "the dot branch must precede the click-through lookup"
    # scope to the dot branch ITSELF (up to the next sibling branch) — the
    # group-head branch between here and nav has its own return, which let a
    # return-stripped mutant pass the first draft of this pin.
    seg = blk[d:blk.index("const head", d)]
    assert "markRead(" in seg and "return" in seg, (
        "a dot click must mark read and RETURN — falling through would hit "
        "the v0.51.151 click-through and navigate away, which is the exact "
        "thing this control exists to avoid")


def test_keydown_guard_covers_the_dot():
    i = APP_JS.index("listEl.addEventListener('keydown'")
    blk = APP_JS[i:APP_JS.index("openNotifRow(row);", i)]
    # v0.51.307 (audit): membership, not the selector literal — the first
    # draft froze member ORDER ('.notif-x, .notif-dot'), so a harmless
    # reorder or a third nested control would red this pin (mirror class).
    bail = blk[:blk.index("const head")]
    assert "'.notif-dot" in bail or ".notif-dot'" in bail, (
        "the nested-button bail must recognise the dot")
    # v0.51.309 (audit r2): pin the guard LINE as an if→return — a bare
    # "return" in the segment was satisfied by the handler's key-filter, so
    # a neutered guard (closest() call without the if/return) stayed green.
    # v0.51.311 (review): the guard LINE is the one calling closest() with
    # .notif-x in ANY position — the prefix anchor re-froze member order.
    line = next(l.strip() for l in bail.splitlines()
                if "closest(" in l and ".notif-x" in l)
    assert line.startswith("if (") and line.endswith("return;"), (
        f"the nested-control guard must BAIL, not just look: {line!r}")
    assert blk.index(".notif-dot") < blk.index("closest('.notif-row')"), (
        "the v0.51.213 nested-button guard must fire before the row branch — "
        "else Enter on a focused dot marks read AND navigates (double-fire)")


def test_css_shows_the_dot_only_while_unread():
    i = OPS_CSS.index(".notif-dot {")
    base = OPS_CSS[i:OPS_CSS.index("}", i)]
    assert "display: none" in base, (
        "the dot's base state is hidden — it is rendered on EVERY row and "
        "gated by class, so markRead's unread→seen flip retires it in place")
    # v0.51.307 (audit): the unread rule must actually FLIP display — the
    # first draft only checked the selector existed, so a rule that lost its
    # display declaration (invisible dot, dead feature) stayed green.
    j = OPS_CSS.index(".notif-row.unread .notif-dot")
    body = OPS_CSS[OPS_CSS.index("{", j):OPS_CSS.index("}", j)]
    assert "display:" in body and "none" not in body, (
        "the .unread rule must set display to a visible value")


def test_css_mobile_tap_target_covers_the_dot():
    blk = OPS_CSS[OPS_CSS.index("@media (max-width: 600px)"):]
    blk = blk[:blk.index("}", blk.index(".notif-clear-all"))]
    # v0.51.307 (audit): presence wasn't enough — equal padding on an 8px
    # circle yields ~28px, and a missed tap falls through to the row and
    # NAVIGATES.
    # v0.51.309 (audit r2): the EXACT values (18px/-12px) are owned by ONE
    # pin — test_v0_51_307_audit_fixes — two files mirroring the literal
    # red'd in tandem on any equivalent rework. Here: the dot has its own
    # sizing rule at all.
    i = blk.index(".notif-dot")
    rule = blk[blk.index("{", i):blk.index("}", i)]
    assert "padding" in rule, (
        "the ≤600px dot rule must enlarge the 8px circle's tap target")


def test_v0_51_305_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "0.51.305: " in init_py
