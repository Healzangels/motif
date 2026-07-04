"""v0.51.55 — branded record-spinner on the JOBS log + EVENT STREAM first load.

the user: "anywhere we say loading, include the spinning animation — the jobs log
and the event viewer, like the results section." loadQueue now paints
recordLoaderHtml (the shared library "results section" loader) into #jobs-body
and #event-stream-full on first load (lastHash == null), instead of the SSR
"loading…" text (jobs) / empty list (events). Gated on the empty state so the 10s
poll + hash-skip re-render don't reset it.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def test_loadqueue_shows_record_spinner_on_first_load():
    i = APP_JS.index("async function loadQueue()")
    body = APP_JS[i:i + 1400]
    # both lists get the shared record loader, gated on the empty state.
    assert body.count("recordLoaderHtml('loading…')") >= 2
    assert "#jobs-body" in body and "#event-stream-full" in body
    assert "dataset.lastHash == null" in body
    assert 'class="event-stream-loading"' in body


def test_event_stream_loading_row_spans_full_width():
    # the event li is a 4-col grid; the loading row overrides to block so the
    # record-loader centres.
    assert ".event-stream li.event-stream-loading { display: block; }" in APP_CSS
