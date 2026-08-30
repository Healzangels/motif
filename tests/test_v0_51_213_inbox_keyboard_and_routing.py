"""v0.51.213 — the two v0.51.209 regressions plus the a11y gap that pass left behind.

v0.51.209 set out to make the INBOX drawer keyboard-operable and to route collection
notifications to the right tab. Both landed half-done:

  1. Collections were routed to /collections, but the info_open auto-open gate only fired
     on /movies|/tv|/anime — so the click-through navigated and then opened NOTHING. Worse
     than before the "fix" (it used to land on /tv, where the gate passed).
  2. The keydown handler matched any Enter/Space inside .notif-group-head — including the
     group's own Dismiss-all ×, which is a CHILD of the head — so preventDefault() ate that
     button's activation and toggled the group instead. dismissGroup became unreachable.
  3. The pass made the group HEADER focusable and left the drawer's PRIMARY action — the
     click-through rows — with no tabindex, no role and no keydown path.

These are source-shape guards: the drawer is bound in a closure with no DOM harness in the
suite, so the discriminator is the handler's ordering, which is exactly what broke.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def _keydown_block(*, code_only: bool = False) -> str:
    """The drawer's keydown listener, bounded by the next listener (not a byte window).

    code_only strips `//` comments — the ordering guard below compares where two selectors
    are TESTED, and a comment naming a selector would otherwise satisfy the check without
    any matching code (it did, on the first run of this test)."""
    i = APP_JS.index("if (listEl) listEl.addEventListener('keydown'")
    blk = APP_JS[i:APP_JS.index("document.addEventListener('keydown'", i)]
    if not code_only:
        return blk
    return "\n".join(ln for ln in blk.splitlines() if not ln.lstrip().startswith("//"))


def test_collection_deep_links_actually_open_the_card():
    """The regression the review caught: routing without the matching gate is a dead link."""
    i = APP_JS.index("v1.14.85: ?info_open=<tmdb_id>")
    block = APP_JS[i:APP_JS.index("URLSearchParams not supported", i)]
    assert "path === '/collections'" in block, (
        "a collection notification navigates to /collections — if the auto-open gate does "
        "not list it, the card never opens and the click-through is silently dead")


def test_every_routed_tab_is_a_gated_tab():
    """Drift guard: the router and the gate are one axis. Any tab openNotifRow can send a
    user to must be a tab the deep-link gate opens on, or that route is a dead end."""
    r = APP_JS.index("function openNotifRow(")
    router = APP_JS[r:APP_JS.index("window.location.href", r)]
    routed = {t for t in ("/movies", "/tv", "/anime", "/collections") if f"'{t}'" in router}
    assert routed, "the router must name its tabs literally for this guard to see them"
    i = APP_JS.index("v1.14.85: ?info_open=<tmdb_id>")
    gate = APP_JS[i:APP_JS.index("URLSearchParams not supported", i)]
    ungated = {t for t in routed if f"path === '{t}'" not in gate}
    assert not ungated, f"openNotifRow routes to {ungated} but the deep-link gate skips them"


def test_dismiss_all_is_not_swallowed_by_the_group_toggle():
    """The × lives INSIDE .notif-group-head, so the head lookup matches it too — the
    handler must bail on a nested control BEFORE it claims the event."""
    blk = _keydown_block(code_only=True)
    # v0.51.305: the guard's selector widened to cover the unread dot too —
    # pin the INVARIANT (a bail that recognises .notif-x, ahead of the head
    # match), not the selector's exact membership.
    # v0.51.307 (audit): membership not member-ORDER — the prefix form froze
    # .notif-x as the first selector, so alphabetising the list red'd this.
    assert ".notif-x" in blk, "keydown must recognise the dismiss buttons"
    # v0.51.309 (audit r2): the bail must actually BAIL — pin the guard line
    # as if→return (a mutant stripping the return survived the old pins).
    gi = blk.index(".notif-x")
    line = blk[blk.rindex("\n", 0, gi) + 1:blk.index("\n", gi)].strip()
    assert line.startswith("if (") and line.endswith("return;"), (
        f"nested-control guard must return: {line!r}")
    assert blk.index(".notif-x") < blk.index("closest('.notif-group-head')"), (
        "the nested-control bail must come BEFORE the group-head match, mirroring the click "
        "handler's ordering — otherwise Enter/Space on Dismiss-all toggles the group")


def test_click_and_keyboard_share_one_navigation_path():
    """Reuse, not a mirrored second copy: both handlers call openNotifRow, so the routing
    table cannot drift between mouse and keyboard (the gc-* mirror lesson)."""
    assert APP_JS.count("function openNotifRow(") == 1
    # 3 = the definition + one call from each handler; a 4th would mean a third caller
    # drifted in, a 2nd that one of the handlers stopped using it.
    assert APP_JS.count("openNotifRow(row)") == 3
    assert "openNotifRow(row)" in _keydown_block()
    click = APP_JS[APP_JS.index("const x = e.target.closest('.notif-x')"):
                   APP_JS.index("if (listEl) listEl.addEventListener('keydown'")]
    assert "openNotifRow(row)" in click


def test_clickable_rows_are_focusable_and_announced():
    """The v0.51.209 a11y pass fixed the header and left the rows mouse-only."""
    r = APP_JS.index("function rowHtml(")
    body = APP_JS[r:APP_JS.index("function renderEmpty(", r)]
    # v0.51.274: EVERY row is a control now — activation marks it read
    # (v0.51.266 made reading per-row), so the old "non-clickable rows must not
    # advertise as controls" rationale is superseded: they DO something.
    assert "const mainAttrs = ' role=\"button\" tabindex=\"0\"';" in body
    assert "clickable ? ' role=" not in body, "the conditional form must not return"
    # The control is .notif-main, NOT the <li>: putting it on the list item would strip the
    # listitem role AND nest the dismiss <button> inside a role="button". Mirrors
    # .notif-group-head, which is likewise a div inside its <li>.
    assert '<div class="notif-main"${mainAttrs}>' in body
    li = body[body.index("return `<li class="):body.index('<div class="notif-main"')]
    assert "role=" not in li and "tabindex=" not in li


def test_keyboard_reaches_the_rows_not_just_the_group_header():
    blk = _keydown_block()
    assert ".notif-row.notif-clickable" in blk, (
        "expanding a group by keyboard is useless if its children can't then be activated")


def test_every_tabbable_drawer_surface_has_a_visible_focus_ring():
    """app.css strips the UA outline app-wide and restores it only for an allow-list, so
    making something tabbable WITHOUT adding it here yields focus that is invisible —
    strictly worse than not being tabbable at all. v0.51.209 did exactly that to the group
    header. Anything we give tabindex in this drawer must land on the list."""
    ops_css = (REPO / "app" / "web" / "static" / "ops.css").read_text()
    # v0.51.309 (audit r2): the ring covers EVERY row's main now — digest
    # rows are controls too (v0.51.274) and the .307 focus park lands there.
    for sel in (".notif-group-head:focus-visible",
                ".notif-row .notif-main:focus-visible"):
        assert sel in ops_css, f"{sel} is tabbable but has no focus ring"
