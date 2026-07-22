"""v0.51.225 (ultra-review #10) — the two inbox record_notification sites must not drift.

notify.dispatch (per-event) and notify.dispatch_coalesced (per-item, for a bulk burst)
each write an inbox row via notify_inbox.record_notification, threading the same item
identity off the ItemContext: media_type / tmdb_id / section_id / edition_key. The two call
sites are ~160 lines apart and structurally identical — a mirror pair.

The review flagged them as mirror-drift: any NEW identity field the drawer click-through
needs must be added to BOTH or the two dispatch paths silently diverge (v0.51.220's
edition_key had to be added to both in lockstep — a real instance of the risk). CLAUDE.md
leans against extracting a helper here ("three duplicated lines beats a helper that hides
intent"); the codebase's established answer for "N sites must stay in sync" is a drift
LINT (cf. test_v1_18_53 status-bar maps, test_v1_19_38 SRC-axis sixth site). This is that
lint: it fails the moment the two calls' keyword sets diverge, so the drift is caught at
test time instead of shipping a half-updated notification identity.
"""
from __future__ import annotations

import ast
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
NOTIFY_PY = (REPO / "app" / "core" / "notify.py").read_text()

# the ItemContext identity fields the drawer click-through resolves the row from — both
# record sites must carry all of them (they came in over v0.51.151 + v0.51.220).
_IDENTITY_KWARGS = {"media_type", "tmdb_id", "section_id", "edition_key"}


def _record_notification_calls() -> list[ast.Call]:
    tree = ast.parse(NOTIFY_PY)
    return [n for n in ast.walk(tree)
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
            and n.func.attr == "record_notification"]


def test_there_are_exactly_two_inbox_record_sites():
    """dispatch + dispatch_coalesced. If a third appears, it joins the mirror contract —
    update this count and confirm the new site carries the same identity kwargs."""
    calls = _record_notification_calls()
    assert len(calls) == 2, (
        f"expected 2 notify_inbox.record_notification sites in notify.py, found "
        f"{len(calls)} at lines {[c.lineno for c in calls]} — a new inbox writer must "
        "thread the same item identity, so fold it into this mirror lint")


def test_both_sites_pass_identical_keyword_sets():
    """The drift guard. The two calls differ only in the ctx var (_ic vs _sic) and
    title/body source — the KEYWORD NAMES must match exactly, so a field added to one path
    but not the other trips this instead of shipping a half-identified notification."""
    calls = _record_notification_calls()
    kw_sets = [frozenset(k.arg for k in c.keywords if k.arg) for c in calls]
    assert kw_sets[0] == kw_sets[1], (
        "the two inbox record_notification sites pass different keyword sets — symmetric "
        f"difference {sorted(kw_sets[0] ^ kw_sets[1])}. A new ItemContext identity field "
        "was added to one dispatch path (dispatch / dispatch_coalesced) but not the other; "
        "add it to both.")


def test_both_sites_carry_the_full_item_identity():
    """Documents WHAT must be mirrored: the click-through identity. If a future edit drops
    one of these from both sites at once, the equal-sets test above stays green but the
    drawer loses its ability to open the right row — so pin the set explicitly."""
    calls = _record_notification_calls()
    for c in calls:
        names = {k.arg for k in c.keywords if k.arg}
        missing = _IDENTITY_KWARGS - names
        assert not missing, (
            f"inbox record site at notify.py:{c.lineno} is missing identity kwargs "
            f"{sorted(missing)} — the drawer click-through resolves the row from these")
