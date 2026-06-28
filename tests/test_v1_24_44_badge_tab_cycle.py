"""v1.24.44 — topbar badges cycle through EVERY impacted tab (incl. collections).

the user: a "7 RE-PUSH"-style badge with 6 movies + 1 collection used to land
clicks only on /movies — the collection match was unreachable. The cycle badges
carry a per-tab breakdown + the shared bindBadgeCycle, and the cycle regex
includes /collections, so each badge routes to every section that has a match.

v0.50.34: the AWAIT badge (one of the cycle family) was removed — it flickered in
during the download→place handoff. The remaining cycle badges (RE-PUSH, DROP) keep
the breakdown wiring; the collections-in-breakdown behavior is covered directly by
test_v1_24_47's _breakdown_tabs() dedup tests.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent


def test_breakdown_queries_and_cycle_wired():
    api = (REPO / "app" / "web" / "api.py").read_text()
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # API: per-tab breakdown queries + the tabs arrays in the responses
    assert "_REPUSH_TAB_BREAKDOWN_SQL" in api
    # v1.24.47: failures/updates keep the inline comprehension; the cycle family
    # (drops/repush) routes through the shared, deduped _breakdown_tabs().
    # v0.50.34: AWAIT badge removed → 2 cycle breakdowns (drops, repush), was 3.
    assert api.count('"tabs": [') >= 2  # failures, updates (inline)
    assert api.count('"tabs": _breakdown_tabs(') == 2  # drops, repush
    # JS: shared cycle binder + the remaining cycle badges wired + collections regex
    assert "function bindBadgeCycle(" in js
    assert "bindBadgeCycle('topbar-repush-badge', 'repushTabs', 'attn_pills=repush')" in js
    assert "bindBadgeCycle('topbar-drops-badge', 'dropTabs', 'tdb_pills=dropped')" in js
    assert js.count("movies|tv|anime|collections") == 1  # v1.24.48: one shared binder


def test_badges_stash_their_breakdown_dataset():
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert "repushBadge.dataset.repushTabs = JSON.stringify(breakdown)" in js
    assert "dropBadge.dataset.dropTabs = JSON.stringify(dropBreakdown)" in js
