"""v0.51.97 — the LOGS event-stream ?level=/?component= filter is visible + clearable.

v1.22.79 made the // SHOW IN LOGS deep-links carry ?level=/?component= and the
event stream honor them — but the filter was applied INVISIBLY (the SINCE chips
still read ALL) with no way to clear it short of a full nav back to /queue.
v0.51.97 renders a dismissable amber pill per active filter; clicking it clears
that var, drops the URL param (so a refresh doesn't re-apply it), and reloads.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
QUEUE_HTML = (REPO / "app" / "web" / "templates" / "queue.html").read_text()


def test_queue_template_has_active_filter_container():
    assert 'id="ev-active-filters"' in QUEUE_HTML
    # it lives in the EVENT STREAM chips bar (next to the SINCE chips).
    i = QUEUE_HTML.index('id="ev-active-filters"')
    bar = QUEUE_HTML[QUEUE_HTML.rindex("chips-bar", 0, i):i]
    assert "data-evfilter" in bar, "the indicator must sit in the SINCE chips bar"


def test_render_reads_both_filter_vars_and_emits_clear_buttons():
    i = APP_JS.index("function renderEventFilters()")
    fn = APP_JS[i:i + 700]
    assert "eventStreamLevel" in fn and "eventStreamComponent" in fn
    assert "data-clear-evfilter" in fn
    assert "ev-filter-clear" in fn
    # hidden when neither filter is active.
    assert "box.hidden = active.length === 0" in fn


def test_clear_handler_resets_var_drops_param_and_reloads():
    i = APP_JS.index("data-clear-evfilter")
    # the delegated click handler lives just after the deep-link var assignment.
    j = APP_JS.index("closest('[data-clear-evfilter]')")
    block = APP_JS[j:j + 600]
    assert "eventStreamLevel = ''" in block
    assert "eventStreamComponent = ''" in block
    # drops the URL param so a refresh doesn't silently re-apply the filter.
    assert "searchParams.delete(which)" in block
    assert "history.replaceState" in block
    assert "renderEventFilters()" in block
    assert "loadQueue()" in block


def test_pill_css_specificity_beats_chip_color():
    # `.chip.ev-filter-clear` (0,2,0) must win the amber over the later
    # `.chip { color: var(--fg-dim) }` (0,1,0) — a bare `.ev-filter-clear`
    # (equal specificity, defined earlier) loses on source order.
    assert ".chip.ev-filter-clear { color: var(--amber);" in CSS
    # the losing bare form (`.ev-filter-clear {` at column 0, no `.chip` prefix)
    # must not exist — it would tie `.chip` on specificity and lose on order.
    assert "\n.ev-filter-clear {" not in CSS
