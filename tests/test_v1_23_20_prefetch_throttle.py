"""v1.23.20 — hotfix: INFO-card prefetch is intent-based, not per-row.

the user's repro: after v1.23.19 the DATABASE settings locked up. Cause:
the v1.23.19 prefetch fired an api_item GET for EVERY library row the
pointer swept over, saturating the browser's ~6-connection-per-host
limit; later requests (a settings-page load, the restore-cancel
confirming GET) queued behind the backlog and the UI appeared frozen
(closing the tab — which cancels pending requests — fixed it).

Fix: only prefetch once the pointer RESTS on the ⓘ for 150ms
(mouseover→setTimeout, mouseout→clear), so a casual sweep fires
nothing. Plus: restore-cancel hides the banner optimistically so a
slow confirming GET can't leave it stuck (the banner-still-showing
symptom in the user's screenshot).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def test_prefetch_is_rest_delayed_not_immediate():
    i = APP_JS.index("_lib?.addEventListener('mouseover'")
    block = APP_JS[i:i + 600]
    # the prefetch must be inside a setTimeout (rest delay), not called
    # directly in the mouseover handler.
    assert "setTimeout(" in block
    assert "clearTimeout(_infoHoverTimer)" in block
    assert "prefetchInfo(" in block
    # a mouseout must cancel a pending (not-yet-fired) prefetch.
    # v1.23.21: the handler gained a relatedTarget boundary check, so
    # it's no longer the bare one-liner — assert the mouseout listener
    # clears the timer (the boundary-check shape is pinned in v1_23_21).
    assert "_lib?.addEventListener('mouseout'" in APP_JS
    m = APP_JS.index("_lib?.addEventListener('mouseout'")
    assert "clearTimeout(_infoHoverTimer)" in APP_JS[m:m + 200]


def test_cancel_hides_banner_optimistically():
    i = APP_JS.index("database-restore/cancel")
    block = APP_JS[i:i + 500]
    # after a successful cancel the banner is hidden directly, before /
    # independent of the confirming refreshPending GET.
    assert "pendingBanner.hidden = true" in block
