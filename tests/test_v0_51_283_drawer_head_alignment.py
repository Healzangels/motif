"""v0.51.283 — the drawer head is a deterministic two-row layout.

The operator, with a prod screenshot: "the mark all as read and the clear all
are off centered or pushed all the way to the right which makes the alignment
look off." The .274 fix stopped the label-shredding but parked the wrapped
actions row right-hugging under a left-aligned title — technically wrapped,
visually broken. Per the house idiom (labelled button rows sit LEFT, groups
centre): title + × on row one, the text buttons left-aligned beneath. Scoped
to #notif-drawer so the LIVE OPS head (one short title + ×) is untouched.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
BASE = (REPO / "app" / "web" / "templates" / "base.html").read_text()
OPS_CSS = (REPO / "app" / "web" / "static" / "ops.css").read_text()


def _notif_head() -> str:
    i = BASE.index('id="notif-drawer"')
    return BASE[i:BASE.index("</header>", i)]


def test_close_lives_on_the_title_row_not_in_the_actions_div():
    head = _notif_head()
    actions_start = head.index('class="notif-head-actions"')
    actions_end = head.index("</div>", actions_start)
    assert "ops-drawer-close" not in head[actions_start:actions_end], (
        "× rides the TITLE line — inside the actions div it would drop to "
        "the second row with the buttons")
    assert "ops-drawer-close" in head[actions_end:], "…but stays in the header"


def test_actions_row_is_full_width_and_left_aligned():
    i = OPS_CSS.index("#notif-drawer .notif-head-actions")
    block = OPS_CSS[i:OPS_CSS.index("}", i)]
    assert "flex-basis: 100%" in block, "its own row, always — no sometimes-wrap"
    assert "justify-content: flex-start" in block, (
        "labelled button rows sit LEFT (the idiom); right-hugging under a "
        "left title is what the operator reported as broken")


def test_close_is_pushed_to_the_right_of_the_title():
    i = OPS_CSS.index("#notif-drawer .ops-drawer-close")
    block = OPS_CSS[i:OPS_CSS.index("}", i)]
    assert "margin-left: auto" in block


def test_scoping_leaves_the_live_ops_head_alone():
    for sel in ("#notif-drawer .ops-drawer-head",
                "#notif-drawer .ops-drawer-title",
                "#notif-drawer .ops-drawer-close",
                "#notif-drawer .notif-head-actions"):
        assert sel in OPS_CSS
    # the order/basis rules must not exist unscoped. Property-match, not
    # substring: "border:" CONTAINS "order:" — the same substring trap as
    # the 429-in-a-video-id (v0.51.269), caught here by this test's own
    # first draft flagging an innocent border rule.
    import re
    for m in re.finditer(r"^\.ops-drawer-close\s*[,{]", OPS_CSS, re.M):
        blk = OPS_CSS[m.start():OPS_CSS.index("}", m.start())]
        assert not re.search(r"^\s*order\s*:", blk, re.M), (
            "ordering is notif-scoped only")


def test_labels_still_never_break():
    i = OPS_CSS.index(".notif-clear-all {")
    assert "white-space: nowrap" in OPS_CSS[i:OPS_CSS.index("}", i)], (
        "the .274 half of the fix survives — rows move, labels never shred")


def test_v0_51_283_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
