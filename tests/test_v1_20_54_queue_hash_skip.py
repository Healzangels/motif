"""v1.20.54 — hash-skip the /queue poll renders (scroll-flicker fix).

Design-system audit (2026-05-31): `loadQueue` rewrote both #jobs-body
AND #event-stream-full innerHTML on every 10s poll with NO
`dataset.lastHash` guard — so an idle poll that returned the same rows
blew the .jobs-scroll-y / event-stream scroll position. This is bug
class #4 ("innerHTML flicker on poll") + DESIGN_SYSTEM §3 "Hash-skip
pattern", which the library table already honors (tbody.dataset.lastHash)
but the highest-frequency poll page (/queue) did not.

Fix: capture each pane's rendered HTML and skip the innerHTML write when
it matches the stored lastHash. Source pins (app.js has no exec harness).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def _loadqueue_body() -> str:
    start = APP_JS.index("async function loadQueue()")
    # loadQueue ends before bindQueue.
    end = APP_JS.index("function bindQueue()", start)
    return APP_JS[start:end]


def test_jobs_pane_hash_skips_the_rewrite():
    body = _loadqueue_body()
    # Render captured into a const, not written straight to innerHTML.
    assert "const _jobsHtml = data.jobs.map(" in body
    # Conditional write guarded by lastHash.
    assert "_jobsBody.dataset.lastHash !== _jobsHtml" in body
    assert "_jobsBody.innerHTML = _jobsHtml" in body
    assert "_jobsBody.dataset.lastHash = _jobsHtml" in body


def test_event_stream_pane_hash_skips_the_rewrite():
    body = _loadqueue_body()
    assert "const _evHtml = evs.events.map(" in body
    assert "_evBody.dataset.lastHash !== _evHtml" in body
    assert "_evBody.innerHTML = _evHtml" in body
    assert "_evBody.dataset.lastHash = _evHtml" in body


def test_no_unconditional_innerhtml_write_to_either_pane():
    """Regression guard: the old `$('#jobs-body').innerHTML = ...map`
    / `$('#event-stream-full').innerHTML = ...map` direct writes must
    not return (they'd reset scroll every poll again)."""
    assert "$('#jobs-body').innerHTML = data.jobs.map(" not in APP_JS
    assert "$('#event-stream-full').innerHTML = evs.events.map(" not in APP_JS


def test_v1_20_54_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
