"""v0.50.72 — dashboard "all events" → Events Log + clearer no-change notification.

Two small clarity fixes the user flagged:

1. The RECENT ACTIVITY "// all events →" link hit bare /queue, which SSRs the JOBS
   view by default — but the section IS the event stream, so it must land on the
   Events Log. Now the link carries ?view=events, and BOTH the /queue route (SSR)
   and bindQueue (client re-apply) honor an explicit ?view=events|jobs.

2. The "no catalog changes (N checked)" sync notification read as if N things in
   the user's library were checked; the number creeping up run-to-run looked
   unexplained. It's the ThemerrDB catalog size — name the unit.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
API = (REPO / "app" / "web" / "api.py").read_text()
JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
DASH = (REPO / "app" / "web" / "templates" / "dashboard.html").read_text()
WORKER = (REPO / "app" / "core" / "worker.py").read_text()


# ── 1. all-events → events view ──

def test_dashboard_link_targets_events_view():
    assert '<a href="/queue?view=events" class="link-tiny">// all events →</a>' in DASH
    assert '<a href="/queue" class="link-tiny">' not in DASH


def test_queue_route_honors_explicit_view_param():
    idx = API.index("async def queue_page(")
    body = API[idx:API.index("\n    @app.", idx)]
    assert 'qp.get("view") == "events"' in body
    assert 'qp.get("view") == "jobs"' in body
    # the explicit view check must come BEFORE the status/since heuristics so it wins.
    assert body.index('qp.get("view")') < body.index('"status" in qp')


def test_bindqueue_honors_explicit_view_param():
    idx = JS.index("function bindQueue()")
    body = JS[idx:idx + 4000]
    assert "const _viewParam = _qp.get('view');" in body
    assert "(_viewParam === 'events' || _viewParam === 'jobs')" in body
    # explicit view wins over the status/since fallbacks below it.
    assert body.index("_viewParam ===") < body.index("_qp.has('status')")


# ── 2. sync notification names the count's unit ──

def test_no_change_notification_names_themerrdb_unit():
    assert '({checked:,} ThemerrDB themes checked)"' in WORKER
    # the old bare "(N checked)" (ambiguous unit) is gone.
    assert "no catalog changes ({checked} checked)" not in WORKER
    # comma-formatted for readability at ~6k scale.
    assert "{checked:,}" in WORKER
