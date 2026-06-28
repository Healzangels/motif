"""v1.22.56 — LOGS page JOBS / EVENT STREAM toggle.

the user: "rework the Logs section to be similar to how there are Standard
and 4K library you can toggle between … make the current log being
displayed use the full size and get rid of the horizontal scrolling all
together … user can use the new button to toggle between viewing the
jobs or the event stream … make sure linking from the dashboard goes to
the right section when clicked with the new format."

The split layout (two .split-half panes) is replaced by a single full-
width .block with a JOBS/EVENT STREAM toggle (.chip[data-logview]); each
.log-panel[data-logpanel] shows when .is-active. The horizontal scroll
(.jobs-scroll-x / .scroll-chevron-wrap + chevron pseudos) is removed.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
QUEUE = (REPO / "app" / "web" / "templates" / "queue.html").read_text()
CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
API = (REPO / "app" / "web" / "api.py").read_text()

# Comment-stripped variants — the retired class names legitimately appear
# in explanatory comments documenting their removal; absence checks must
# run against live code only.
QUEUE_CODE = re.sub(r"<!--.*?-->", "", re.sub(r"\{#.*?#\}", "", QUEUE, flags=re.S), flags=re.S)
CSS_CODE = re.sub(r"/\*.*?\*/", "", CSS, flags=re.S)


# ── Template: toggle + two panels, no split ──────────────────

def test_split_layout_retired():
    """The side-by-side split (.block-split / .split-half) is gone —
    LOGS is a single full-width .block now."""
    assert "block-split" not in QUEUE_CODE
    assert "split-half" not in QUEUE_CODE


def test_toggle_has_jobs_and_events_chips():
    """Two toggle chips (data-logview) — like the library STANDARD/4K
    resolution toggle."""
    assert 'data-logview="jobs"' in QUEUE
    assert 'data-logview="events"' in QUEUE
    assert "// JOBS" in QUEUE
    assert "// EVENT STREAM" in QUEUE
    assert 'role="tablist"' in QUEUE


def test_two_panels_one_active_by_default():
    """Both panels exist; exactly one is .is-active in the default
    (jobs) render."""
    assert 'data-logpanel="jobs"' in QUEUE
    assert 'data-logpanel="events"' in QUEUE
    # The default SSR view is jobs → its panel + chip carry is-active /
    # chip-active. Render-time is Jinja, but the default branch is the
    # `view == 'jobs'` path, so the jobs panel's class includes is-active
    # literally in the template source for that branch.
    assert "is-active" in QUEUE


def test_jobs_and_events_filter_chips_preserved():
    """The per-panel filter chips survive the rework — 6 job-status
    filters + 4 since filters (the JS binds on these selectors)."""
    for f in ("all", "pending", "running", "failed", "cancelled", "done"):
        assert f'data-jobfilter="{f}"' in QUEUE
    for s in ("0", "3600", "86400", "604800"):
        assert f'data-evfilter="{s}"' in QUEUE


def test_no_horizontal_scroll_machinery_in_template():
    """The horizontal-scroll wrappers are removed from the template."""
    assert "jobs-scroll-x" not in QUEUE_CODE
    assert "scroll-chevron-wrap" not in QUEUE_CODE


# ── CSS: panel toggle + no horizontal scroll ─────────────────

def test_log_panel_toggle_css():
    """.log-panel hidden by default, shown when .is-active."""
    assert ".log-panel { display: none; }" in CSS
    assert ".log-panel.is-active { display: block; }" in CSS


def test_live_indicator_hidden_override_present():
    """The LIVE indicator's `hidden` attribute must actually hide it —
    the class's `display: flex` outranks UA `[hidden]{display:none}`, so
    an explicit `.live-indicator[hidden] { display: none }` is required
    or LIVE leaks onto the JOBS view."""
    assert ".live-indicator[hidden] { display: none; }" in CSS


def test_chevron_and_scroll_x_css_removed():
    """The chevron pseudos + horizontal-scroll container rules are gone
    (no orphan rules left behind)."""
    assert ".scroll-chevron-wrap::before" not in CSS_CODE
    assert ".scroll-chevron-wrap::after" not in CSS_CODE
    assert "[data-can-scroll-left" not in CSS_CODE
    assert "[data-can-scroll-right" not in CSS_CODE
    # .jobs-scroll-x rule removed (it had overflow-x: auto).
    assert ".jobs-scroll-x {" not in CSS_CODE


def test_jobs_grid_row_columns_are_fluid():
    """The jobs row columns are fluid (fit the full-width panel +
    truncate) — no fixed px widths forcing horizontal overflow."""
    start = CSS.index(".jobs-grid-row {")
    block = CSS[start:start + CSS[start:].index("}")]
    assert "minmax(0," in block
    assert "fr)" in block


def test_event_message_wraps_not_scrolls():
    """Long event messages wrap within the panel instead of extending
    sideways under a horizontal scrollbar."""
    start = CSS.index(".event-msg {")
    block = CSS[start:start + CSS[start:].index("}")]
    assert "white-space: normal" in block
    assert "overflow-wrap: anywhere" in block


def test_panels_fill_viewport_height():
    """Both scroll regions fill the viewport height (single full-width
    panel) via clamp(), not the old fixed 700px split-pane height."""
    for sel in (".jobs-scroll-y {", ".event-stream-tall {"):
        start = CSS.index(sel)
        block = CSS[start:start + CSS[start:].index("}")]
        assert "clamp(" in block and "100vh" in block, (
            f"{sel} must use a viewport-relative clamp height"
        )


# ── JS: toggle handler + deep-link routing ───────────────────

def test_js_has_setlogview_and_binds_toggle():
    """setLogView flips the active panel; bindQueue wires the toggle
    chips + the affordance helper is gone."""
    assert "function setLogView(" in JS
    assert "data-logpanel" in JS
    assert "setLogView(c.dataset.logview)" in JS
    # The horizontal-scroll affordance helpers are removed (the bare name
    # survives only in the comment documenting the removal; the function
    # definition + its inline call are gone).
    assert "function _updateScrollAffordance" not in JS
    assert "_updateScrollAffordance(document" not in JS
    assert "_updateJobsScrollAffordance()" not in JS


def test_js_deep_link_routes_to_correct_panel():
    """?status -> jobs, ?since/?level/?component -> events."""
    idx = JS.index("function bindQueue()")
    body = JS[idx:idx + 4000]
    assert "_qp.has('status')" in body
    assert "_qp.has('since')" in body
    assert "'events'" in body and "'jobs'" in body
    assert "setLogView(" in body


def test_chip_filter_bindings_unchanged():
    """The per-panel filter chip selectors the JS binds on still match
    the template (these survive the layout change unchanged)."""
    assert ".chip[data-jobfilter]" in JS
    assert ".chip[data-evfilter]" in JS


# ── Route: SSR the initial panel from deep-link params ───────

def test_queue_route_ssrs_initial_logview():
    """The /queue handler computes initial_logview from the query so the
    right panel is active on first paint (flash-free deep-link)."""
    # v1.22.89: slice to the function's end, not a fixed window — the
    # initial_jobfilter SSR grew the body past the old 1200 chars.
    idx = API.index("async def queue_page(")
    body = API[idx:API.index("\n    @app.", idx)]
    assert "initial_logview" in body
    assert '"status" in qp' in body
    assert '"since" in qp' in body
    assert 'TemplateResponse' in body
