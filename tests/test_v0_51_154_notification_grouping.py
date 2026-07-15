"""v0.51.154 — smart batch grouping in the notification drawer.

Phase 4 (final) of the notification center. A burst of same-kind notifications (a
bulk sync's adds, a mass theme-lost) collapses into ONE expandable group row so the
drawer isn't flooded. Done CLIENT-SIDE in bindNotifInbox (groupRows: adjacent
same-event_kind runs within a 10-min window, length >= 3) — no backend/API change,
and it PRESERVES the v0.51.151 click-through: expanding a group reveals its children,
which remain individually clickable + dismissable. Runs of 1–2 stay as normal rows.

Source-pin guards for the grouping logic + CSS (client-side render + expand/dismiss
is animation + fetch, verified in a browser harness, so pinned like every UI tag).
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
OPS_CSS = (REPO / "app" / "web" / "static" / "ops.css").read_text()


def _bind_body() -> str:
    i = APP_JS.index("function bindNotifInbox()")
    # the whole binder (dismiss/dismissGroup live near the end).
    return APP_JS[i:APP_JS.index("function bindUploadDialog()", i)]


def test_grouprows_collapses_bursts():
    body = _bind_body()
    assert "function groupRows(items)" in body
    # collapse only real bursts (>= 3), within a time window.
    assert "const GROUP_MIN = 3;" in body
    assert "GROUP_WINDOW_MS" in body
    # grouping keys on event_kind + adjacency within the window.
    assert "prev.event_kind === n.event_kind" in body
    # render dispatches group vs single.
    assert "x.group ? groupHtml(x) : rowHtml(x.n)" in body


def test_group_header_and_children_render():
    body = _bind_body()
    assert "function groupHtml(g)" in body
    # header shows "N <noun>" + a dismiss-all × + a caret; children reuse rowHtml.
    assert "notif-group-head" in body
    assert "notif-x-group" in body
    assert "notif-group-children" in body
    assert "g.children.map(rowHtml).join('')" in body
    # a GROUP noun map exists (per-kind plural label).
    assert "const GROUP = {" in body
    assert "'themes added'" in body


def test_group_interactions_wired():
    body = _bind_body()
    # expand/collapse toggles the children + aria-expanded.
    assert "e.target.closest('.notif-group-head')" in body
    assert "aria-expanded" in body
    # group dismiss-all + per-child count tick-down.
    assert "function dismissGroup(groupLi)" in body
    assert "groupLi.remove()" in body
    # a per-child dismiss inside a group updates or removes the group.
    assert "li.closest('.notif-group')" in body


def test_group_css():
    assert ".notif-group {" in OPS_CSS
    assert ".notif-group-head {" in OPS_CSS
    assert ".notif-group-children {" in OPS_CSS
    # the group carries the FIXED tier stripe (like single rows).
    assert ".notif-group.tier-add" in OPS_CSS
    assert ".notif-group.tier-fyi" in OPS_CSS
    # caret rotates open; children indent + drop their own stripe.
    assert ".notif-group-head.is-open .notif-caret" in OPS_CSS
    assert ".notif-group-children .notif-row { border-left: 0; padding-left: 34px; }" in OPS_CSS
