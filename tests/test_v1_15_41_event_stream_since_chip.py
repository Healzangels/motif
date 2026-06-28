"""v1.15.41 — SINCE chip filter for /queue's EVENT STREAM pane.

the user: "would it be possible to make it so we can sort log events
for new entries as right now you can tell how many were addedin a
day but not which easily."

The dashboard's ADDED TODAY / ADDED THIS WEEK cards surface a count
but not the underlying rows. Pre-fix the operator had to scroll
the full /queue event stream and eyeball timestamps. This tag adds
a SINCE chip row (ALL / 1H / 24H / 7D) on the EVENT STREAM pane
backed by a new `?since=<seconds>` param on /api/events, so the
count-vs-rows mismatch becomes a one-click drill-down.

Static-text guards (consistent with v1.15.40 patterns).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"
APP_JS = REPO / "app" / "web" / "static" / "app.js"
QUEUE_HTML = REPO / "app" / "web" / "templates" / "queue.html"


# ── 1. /api/events accepts ?since=<seconds> ──────────────────


def test_api_events_accepts_since_query_param():
    """The /api/events endpoint must declare a `since: int` Query
    param so the SINCE chip can ask for "events in the last N
    seconds". Capped at 30d so a malformed deep-link can't ask
    SQLite for an open-ended scan of the full events table."""
    src = API_PY.read_text()
    anchor = src.index('async def api_events(')
    sig = src[anchor:anchor + 800]
    assert 'since: int = Query(0' in sig, (
        "v1.15.41: /api/events must declare `since: int = Query(0, "
        "ge=0, le=30 * 86400)` so the SINCE chip can pass a window"
    )
    assert 'le=30 * 86400' in sig, (
        "v1.15.41: cap `since` at 30d so a malformed deep-link "
        "can't scan the full events table"
    )


def test_api_events_filters_with_datetime_since():
    """The endpoint must apply the `since` value as a SQL clause
    using SQLite's `datetime('now', '-N seconds')` so the comparison
    happens server-side (vs the client filtering 200 unrelated rows
    and ending up with 3 visible)."""
    src = API_PY.read_text()
    anchor = src.index('async def api_events(')
    body = src[anchor:anchor + 2500]
    assert "ts >= datetime('now', ?)" in body, (
        "v1.15.41: SINCE filter must be a SQL clause (server-side), "
        "not a post-fetch JS trim"
    )
    assert 'f"-{int(since)} seconds"' in body, (
        "v1.15.41: the offset string MUST cast `since` through int() "
        "before formatting — defense-in-depth against a Query() "
        "validator regression letting a non-int through"
    )


# ── 2. queue.html renders the SINCE chip row ────────────────


def test_queue_html_has_since_chip_row():
    """The /queue EVENT STREAM pane must render a chip row with the
    canonical 4 buckets (ALL / 1H / 24H / 7D). data-evfilter values
    must match the JS allowlist in bindQueue()."""
    html = QUEUE_HTML.read_text()
    # Anchor on the EVENT STREAM panel so we don't mistake a JOBS chip
    # for the SINCE row. v1.22.56: the anchor moved from the `// EVENT
    # STREAM` text (now the toggle chip near the top of the section) to
    # the panel marker data-logpanel="events", which directly precedes
    # the SINCE chip row.
    anchor = html.index('data-logpanel="events"')
    block = html[anchor:anchor + 2000]
    assert 'data-evfilter="0"' in block, (
        "v1.15.41: SINCE chip row must include the ALL bucket "
        "(data-evfilter=\"0\") as the default"
    )
    assert 'data-evfilter="3600"' in block
    assert 'data-evfilter="86400"' in block
    assert 'data-evfilter="604800"' in block
    # ALL must start chip-active so the default view shows everything.
    all_chip_idx = block.index('data-evfilter="0"')
    # Walk back to the opening <button> tag for ALL.
    btn_open = block.rfind('<button', 0, all_chip_idx)
    btn_close = block.index('>', all_chip_idx)
    btn_tag = block[btn_open:btn_close + 1]
    assert 'chip-active' in btn_tag, (
        "v1.15.41: ALL chip must start chip-active so the default "
        "/queue view shows all events (matches eventStreamSince=0)"
    )


# ── 3. app.js wires fetch + click + deep-link ────────────────


def test_app_js_passes_since_to_events_endpoint():
    """When `eventStreamSince > 0` the fetch URL must carry
    `since=<value>`. Otherwise the chip is just a UI ornament.
    v1.22.79: the path is now built via URLSearchParams (level/
    component deep-link filters joined the query) — pin the param
    plumbing instead of the old literal template string."""
    js = APP_JS.read_text()
    assert "let eventStreamSince = 0;" in js, (
        "v1.15.41: must declare `eventStreamSince` state alongside "
        "`queueFilter` so the chip selection persists across polls"
    )
    assert "_evq.set('since', String(eventStreamSince))" in js, (
        "v1.15.41/v1.22.79: events fetch must include since=<value> "
        "when the SINCE chip is non-ALL"
    )
    assert "const evsPath = `/api/events?${_evq.toString()}`" in js


def test_app_js_binds_evfilter_chip_clicks():
    """bindQueue() must wire click handlers on the SINCE chips that
    update eventStreamSince + re-fetch the queue, mirroring the
    existing `data-jobfilter` pattern."""
    js = APP_JS.read_text()
    bind_anchor = js.index("function bindQueue()")
    body = js[bind_anchor:bind_anchor + 5000]
    assert "$$('.chip[data-evfilter]').forEach" in body, (
        "v1.15.41: bindQueue() must iterate the SINCE chips to wire "
        "click handlers"
    )
    assert "eventStreamSince = parseInt(c.dataset.evfilter" in body, (
        "v1.15.41: SINCE chip click must update eventStreamSince "
        "via parseInt(dataset.evfilter)"
    )
    assert "loadQueue()" in body, (
        "v1.15.41: SINCE chip click must trigger loadQueue() so the "
        "EVENT STREAM re-renders with the new filter"
    )


def test_app_js_honors_since_deep_link_param():
    """A ?since=<value> URL param must apply the matching chip on
    initial /queue load. Allowlist must mirror the chip values so
    a malformed param can't poison the state."""
    js = APP_JS.read_text()
    bind_anchor = js.index("function bindQueue()")
    body = js[bind_anchor:bind_anchor + 5000]
    assert "new URLSearchParams(window.location.search).get('since')" in body, (
        "v1.15.41: must read ?since=<value> from URL on initial load"
    )
    # Allowlist mirrors queue.html chip values.
    assert "new Set(['0', '3600', '86400', '604800'])" in body, (
        "v1.15.41: deep-link allowlist must mirror the 4 chip values"
    )


def test_app_js_evfilter_allowlist_matches_template_chips():
    """Cross-check: every data-evfilter chip in queue.html must be
    in the JS allowlist (and vice versa). Mirror-principle guard —
    pre-fix the JOBS chip axis was bitten by exactly this kind of
    drift between template-defined chips and the JS validator."""
    import re
    html = QUEUE_HTML.read_text()
    js = APP_JS.read_text()
    template_values = set(re.findall(r'data-evfilter="(\d+)"', html))
    bind_anchor = js.index("function bindQueue()")
    body = js[bind_anchor:bind_anchor + 5000]
    allowlist_block = body[body.index("new Set(["):body.index("new Set([") + 80]
    js_values = set(re.findall(r"'(\d+)'", allowlist_block))
    assert template_values == js_values, (
        f"v1.15.41: SINCE chip drift — template has "
        f"{template_values}, JS allowlist has {js_values}. "
        "Both must agree or a chip click will silently no-op."
    )
