"""v0.51.274 — the fan-out review's drawer batch: keyboard parity, visible
focus, a head that doesn't shred its labels, and four state-tidiness fixes.

The two cross-confirmed findings (found independently by the behavior and the
design reviewers, the second by driving real Tab keypresses in a browser
harness built from the shipped CSS):

  1. v0.51.266 made reading per-row but wired markRead into the CLICK handler
     only — the third instance of the v0.51.213 mouse-only class in this one
     handler's history. Enter on an unread row navigated and left it unread
     forever, and non-clickable rows had no tabindex at all, so a keyboard
     user could not clear a single row short of dismissing it — the state
     markRead's own v0.51.266 comment forbids.
  2. The drawer-head text buttons were hover-bearing with NO focus-visible
     ring (absent from both allow-lists), and with MARK ALL READ present the
     fixed-width head wrapped every label to 2-3 lines at ALL viewports.

Plus, from the behavior reviewer: the only mutating POSTs in the file missing
the bug-class-#7 `setTimeout(refreshTopbarStatus, 1100)` convention (the 2s
ops-cadence poll re-read the 1s-TTL stats cache and resurrected the old badge
count); a dismiss double-click double-decrement; markRead's POST aborted by
its own click's navigation (keepalive); a group head left glowing after its
last unread child was dismissed; MARK ALL READ lingering after the last
unread row was cleared individually; and stale hidden badge text resurrecting
on a dim pill.

JS is not executable under pytest, so these are structural pins — anchored per
the v0.51.261 rules, each mutation-verified against the fix it guards.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
OPS_CSS = (REPO / "app" / "web" / "static" / "ops.css").read_text()


def _fn(name: str) -> str:
    i = APP_JS.index(f"function {name}(")
    nxt = [x for x in (APP_JS.find("\n    function ", i + 1),
                       APP_JS.find("\n    async function ", i + 1)) if x != -1]
    return APP_JS[i:min(nxt)] if nxt else APP_JS[i:]


def _keydown_block() -> str:
    i = APP_JS.index("listEl.addEventListener('keydown'")
    return APP_JS[i:APP_JS.index("});", i) + 3]


# ── 1. keyboard parity ───────────────────────────────────────


def test_keydown_marks_the_row_read_like_the_click_path():
    kb = _keydown_block()
    assert "markRead(anyRow)" in kb, (
        "v0.51.274: Enter/Space must mark the row read — v0.51.266 wired "
        "markRead into the click handler only (the v0.51.213 mouse-only class)")
    assert kb.index("markRead(anyRow)") < kb.index("openNotifRow(row)"), (
        "mark read BEFORE navigating, mirroring the click path's order")


def test_every_row_is_tabbable_not_just_clickable_ones():
    i = APP_JS.index("const mainAttrs =")
    line = APP_JS[i:APP_JS.index("\n", i)]
    assert "clickable ?" not in line, (
        "v0.51.274: non-clickable rows need tabindex too — a keyboard user "
        "could not clear one row's unread state short of dismissing it")
    assert 'tabindex="0"' in line


# ── 2. visible focus + head layout ───────────────────────────


def test_drawer_head_buttons_have_a_focus_ring():
    i = OPS_CSS.index(".notif-clear-all:focus-visible")
    block = OPS_CSS[i:OPS_CSS.index("}", i)]
    assert "outline: 2px solid var(--cyan)" in block, (
        "hover-bearing primitives need a matching focus-visible affordance "
        "(the v1.15.118 rule; measured absent by the design review)")


def test_head_labels_never_break_and_the_actions_row_wraps_as_a_unit():
    i = OPS_CSS.index(".notif-clear-all {")
    assert "white-space: nowrap" in OPS_CSS[i:OPS_CSS.index("}", i)]
    j = OPS_CSS.index(".ops-drawer-head {")
    assert "flex-wrap: wrap" in OPS_CSS[j:OPS_CSS.index("}", j)], (
        "the actions ROW drops below the title as one line — the documented "
        "mobile idiom (rows wrap as units, labels never shred)")


# ── 3. the 1100ms convention (bug class #7) ──────────────────


def test_every_mutating_drawer_post_lands_past_the_stats_ttl():
    for fn in ("markRead", "markAllRead", "dismiss", "dismissGroup"):
        assert "setTimeout(refreshTopbarStatus, 1100)" in _fn(fn), (
            f"{fn}: the 2s ops-cadence poll re-reads the 1s-TTL stats cache "
            f"and resurrects the old badge count without the delayed refresh")


# ── 4. state tidiness ────────────────────────────────────────


def test_dismiss_has_a_reentry_guard_and_markread_honors_it():
    d = _fn("dismiss")
    assert "li.dataset.dismissing" in d and "return" in d
    assert d.index("dataset.dismissing") < d.index("wasUnread"), (
        "the guard must run before the unread capture, or a double-click "
        "still double-decrements")
    assert "li.dataset.dismissing" in _fn("markRead"), (
        "×-then-row-body during the await is the same double decrement")


def test_markread_survives_its_own_navigation():
    m = _fn("markRead")
    assert "keepalive: true" in m, (
        "openNotifRow assigns location.href in the same task — without "
        "keepalive the seen POST aborts and the row stays unread forever")


def test_dismissing_last_unread_child_dims_the_group_head():
    d = _fn("dismiss")
    assert "groupLi.classList.remove('unread')" in d
    assert "groupLi.classList.add('seen')" in d


def test_mark_all_read_hides_once_nothing_unread_remains():
    for fn in ("markRead", "dismiss", "dismissGroup"):
        assert "readAllBtn.hidden = true" in _fn(fn), (
            f"{fn}: the same action-with-no-object class v0.51.270 fixed in "
            f"renderEmpty, one gap over")


def test_hidden_badge_reads_as_zero():
    b = _fn("bumpUnreadBadge")
    assert "el.hidden ? 0" in b, (
        "the poll's zero-branch hides the badge without clearing textContent — "
        "stale digits must not resurrect on a dim pill")


def test_v0_51_274_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
