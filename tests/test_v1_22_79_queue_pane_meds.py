"""v1.22.79 (audit round 2, Batch C #8) — /queue pane MEDs.

(1) loadQueue's SECOND await (the events fetch) had no _seq re-check
and no error surface: a slow in-flight poll (since=0) resolving after
a fresh SINCE-chip load clobbered the filtered render with the
unfiltered list, and an events-fetch failure threw AFTER the jobs
render — the events pane silently froze on stale rows (the jobs pane
got its v1.17.13 in-pane error row; events never did). Now: try/catch
with an in-pane <li> error + v1.20.60 hash clear, and a stale-token
re-check after the await (the loadDashboard discipline).

(2) The jobs-pane error row still emitted `<tr><td>` into what has
been an `<ol>` since the v1.19.6 grid rework — the HTML parser DROPS
table tags in list context, so the v1.17.13 error rendered as a bare
unstyled text node (no accent-red, no row). Now an <li>.

(3) The dashboard's // SHOW IN LOGS links carry
/queue?level=WARNING&component=download, and the template has
promised "lights up the matching filter" for tags — but nothing ever
consumed the params: v1.22.56 only used them to pick the events
panel, and the fetch was built from `since` alone. bindQueue now
reads them one-shot into eventStreamLevel/Component and the fetch
forwards them to /api/events (which has accepted level= + component=
all along).
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def _load_queue_body() -> str:
    i = APP_JS.index("async function loadQueue()")
    return APP_JS[i:APP_JS.index("\n  function bindQueue()", i)]


def test_events_fetch_has_seq_recheck_and_error_surface():
    body = _load_queue_body()
    j = body.index("evs = await api('GET', evsPath);")
    after = body[j:j + 1600]
    assert "if (loadQueue._seq !== _myToken) return;" in after, (
        "v1.22.79: the SECOND await needs the stale-token re-check"
    )
    assert "event stream load failed" in after
    assert "_ebErr.dataset.lastHash = ''" in after, (
        "the v1.20.60 hash clear — without it the error row sticks"
    )


def test_jobs_error_row_is_li_not_tr():
    body = _load_queue_body()
    j = body.index("queue load failed")
    region = body[j - 300:j + 100]
    assert '<li class="accent-red">' in region, (
        "v1.22.79: #jobs-body is an <ol>; <tr><td> gets dropped by the "
        "parser and the error renders unstyled"
    )
    assert "<tr><td" not in body


def test_deep_link_level_component_applied_to_fetch():
    assert "let eventStreamLevel = ''" in APP_JS
    assert "let eventStreamComponent = ''" in APP_JS
    assert "_evq.set('level', eventStreamLevel)" in APP_JS
    assert "_evq.set('component', eventStreamComponent)" in APP_JS
    # bindQueue consumes the params.
    assert "eventStreamLevel = (_qp.get('level') || '').trim()" in APP_JS
    assert ("eventStreamComponent = (_qp.get('component') || '').trim()"
            in APP_JS)
