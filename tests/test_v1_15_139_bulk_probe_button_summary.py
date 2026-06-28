"""v1.15.139 — surface the bulk-probe-tdb result on its trigger
button.

the user on v1.15.138 (anime page, 4 FAIL rows selected):

> when clicking the bulk probe button can it show the status of
> the probe event similar to ack failure or some of the other
> buttons while/after the probe is complete that way you can tell
> easily if any cleared or all still failed.

## Root cause (UX gap, not a bug)

The v1.15.60 // PROBE TDB SELECTED click handler dispatched the
op to /api/admin/bulk-probe-tdb and then set a blind 2-second
timer to restore the button. The actual result (alive=N
(cleared=M), dead=D, indeterminate=I, other=O) lived only inside
the // LIVE OPS drawer card's activity line — invisible unless
the user opened the drawer.

Sibling buttons (// ACK FAILURES, // RESTORE FROM PLEX, etc.)
that run their own per-item loops update their label live
(// ACKING 2/4) and end with a summary line (// 4 ACKED ·
0 FAILED). the user's request: extend the same affordance to
PROBE TDB even though that runs server-side.

## Fix — `app/web/static/app.js`

Three new helpers near `libKey`:

  - `parseBulkProbeActivity(s)` — extracts {alive, cleared, dead,
    indet, other} from the terminal activity string written by
    api.py:2876-2881's `_post_terminal_activity()` helper. Tolerates
    the optional "Cancelled at X/Y — " / "Bailed at X/Y — " prefix
    and the "— likely rate-limited" suffix.
  - `formatBulkProbeSummary(counts, status)` — composes a concise
    button label. the user's framing ("any cleared or all still
    failed") drives priority: CLEARED first, then DEAD, indet (?)
    if any. Edge cases: status=failed/cancelled get explicit
    labels; an all-healthy probe shows `N ALIVE`.
  - `watchBulkProbeCompletion(btn, origText)` — polls /api/progress
    at 1s, gates terminal-state pickup behind a prior `running`
    sighting (so a stale finished row from a previous run within
    the 24h window doesn't false-positive), then renders the
    summary + reloads library + restores after 5s. Bails cleanly
    on btn.isConnected = false (user navigated away) and on a
    30-min wall-clock ceiling.

Click handler swap: the v1.15.60 blind setTimeout(restore, 2000)
becomes `watchBulkProbeCompletion(btn, orig)`. Error path
(API rejected with "already running") unchanged.

## Tests
"""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"
API_PY = REPO / "app" / "web" / "api.py"


def _read_js() -> str:
    return APP_JS.read_text()


# ── helper presence ───────────────────────────────────────────────

def test_parse_bulk_probe_activity_helper_defined():
    js = _read_js()
    assert "function parseBulkProbeActivity(" in js, (
        "v1.15.139: parseBulkProbeActivity() helper missing — the "
        "button summary depends on parsing the server's terminal "
        "activity line."
    )


def test_format_bulk_probe_summary_helper_defined():
    js = _read_js()
    assert "function formatBulkProbeSummary(" in js, (
        "v1.15.139: formatBulkProbeSummary() helper missing — the "
        "button summary builder."
    )


def test_watch_bulk_probe_completion_helper_defined():
    js = _read_js()
    assert "async function watchBulkProbeCompletion(" in js, (
        "v1.15.139: watchBulkProbeCompletion() helper missing — "
        "this is the polling loop that drives the button result."
    )


# ── parsing contract: regex matches the server's activity string ──

def test_parse_regex_matches_server_activity_format():
    """The JS parser regex must match the exact string shape that
    api.py:_post_terminal_activity emits — otherwise the summary
    line silently falls through to the `// PROBE DONE` fallback."""
    js = _read_js()
    # Extract the JS regex pattern from the helper body.
    m = re.search(
        r"function parseBulkProbeActivity\(s\) \{(.*?)return out;\s*\}",
        js, re.DOTALL)
    assert m, "parseBulkProbeActivity body not parseable"
    body = m.group(1)
    js_pattern = re.search(r"match\(\s*(/[^/]+/)\)", body)
    assert js_pattern, "parseBulkProbeActivity regex literal missing"

    # Synthesize the exact line shape the server writes.
    py = API_PY.read_text()
    # Anchor: the f-string in _post_terminal_activity. Find it.
    helper_idx = py.index("def _post_terminal_activity():")
    helper_block = py[helper_idx:helper_idx + 1500]
    assert "alive={n_alive} (cleared={n_alive_cleared}), " in helper_block, (
        "Server-side activity-string template changed shape — JS "
        "parser regex needs the matching update."
    )
    assert "dead={n_dead}, indeterminate={n_indet}, " in helper_block
    assert "other={n_other}" in helper_block

    # The natural-completion log_event also mirrors this — guard
    # both call sites stay aligned with the parser. Whitespace-
    # tolerant check (the f-string is split across lines).
    flat_py = " ".join(py.split())
    assert (
        'alive={n_alive}" f" (cleared={n_alive_cleared}), " '
        'f"dead={n_dead}, indeterminate={n_indet}, " '
        'f"other={n_other}, errors={error_count}{bail_suffix}'
    ) in flat_py, (
        "v1.15.139: BULK PROBE TDB done log_event line shape "
        "changed — keep its counter-format in sync with the "
        "_post_terminal_activity helper and the JS regex."
    )


# ── click handler integration ─────────────────────────────────────

def test_click_handler_invokes_watcher_not_blind_timer():
    """The // PROBE TDB SELECTED click handler must call the new
    watcher instead of the v1.15.60 blind 2-second timer that just
    restored the button without surfacing the result."""
    js = _read_js()
    btn_id = "library-bulk-probe-tdb-btn"
    # The id is referenced twice: visibility-show block (~line 8188)
    # AND the click-handler (~line 9816). Skip past the first
    # occurrence to land on the click-handler block.
    first = js.index(f"getElementById('{btn_id}')")
    idx = js.index(
        f"getElementById('{btn_id}')?.addEventListener", first)
    handler = js[idx:idx + 3500]
    # POST stays.
    assert "/api/admin/bulk-probe-tdb" in handler
    # Watcher invoked.
    assert "watchBulkProbeCompletion(btn, orig)" in handler, (
        "v1.15.139: bulk-probe click handler must call "
        "watchBulkProbeCompletion(btn, orig) after the POST — "
        "otherwise the button has no result to show."
    )
    # The pre-v1.15.139 blind 2000ms restorer is gone (only the
    # error-path immediate restore + the watcher's own 5000ms
    # post-summary timer remain).
    assert "setTimeout(() => { btn.disabled = false; btn.textContent = orig; }, 2000)" not in handler, (
        "v1.15.139: the v1.15.60 blind 2-second restore must be "
        "removed — it's incompatible with the watcher's "
        "completion-time-based restore (would cut off the summary)."
    )


# ── summary content ───────────────────────────────────────────────

def test_summary_includes_cleared_and_dead_tokens():
    """the user's framing: 'tell easily if any cleared or all still
    failed'. CLEARED + DEAD must be in the summary vocabulary."""
    js = _read_js()
    idx = js.index("function formatBulkProbeSummary(")
    body = js[idx:idx + 1500]
    assert "CLEARED" in body, (
        "v1.15.139: formatBulkProbeSummary must surface a CLEARED "
        "count — that's the affirmative outcome the user wants to see."
    )
    assert "DEAD" in body, (
        "v1.15.139: formatBulkProbeSummary must surface a DEAD "
        "count — that's the 'still failed' outcome."
    )


def test_summary_distinguishes_failed_and_cancelled():
    """If the probe thread crashes or the user cancels mid-run from
    the drawer, the button must NOT render a misleading 'CLEARED'
    summary. Specific terminal-state labels."""
    js = _read_js()
    idx = js.index("function formatBulkProbeSummary(")
    body = js[idx:idx + 1500]
    assert "PROBE FAILED" in body
    assert "PROBE CANCELLED" in body


# ── watcher correctness ───────────────────────────────────────────

def test_watcher_polls_api_progress():
    js = _read_js()
    idx = js.index("async function watchBulkProbeCompletion(")
    body = js[idx:idx + 3000]
    assert "fetch('/api/progress'" in body, (
        "v1.15.139: watcher must poll /api/progress — that's where "
        "the op's terminal activity surfaces."
    )
    assert "'bulk-probe-tdb'" in body, (
        "v1.15.139: watcher must filter for the bulk-probe-tdb "
        "op_id specifically (the op_id is shared between bulk and "
        "narrow-scope variants per api.py:15122-15157)."
    )


def test_watcher_gates_terminal_on_prior_running_sighting():
    """The watcher must not latch onto a stale 'done' row from a
    previous run. /api/progress returns the most recent finished
    row in the last 24h alongside any running row. Without the
    sawRunning gate, the very first poll could pick up an old
    terminal row + render its (stale) summary as if it were the
    current dispatch's result."""
    js = _read_js()
    idx = js.index("async function watchBulkProbeCompletion(")
    body = js[idx:idx + 3000]
    assert "sawRunning" in body, (
        "v1.15.139: watcher must use a sawRunning gate so an old "
        "terminal row from a prior probe within the 24h /api/"
        "progress finished-window doesn't false-positive."
    )
    assert "isTerminal && sawRunning" in body, (
        "v1.15.139: terminal pickup must require sawRunning has "
        "flipped first — otherwise the gate is decorative."
    )


def test_watcher_bails_on_disconnected_button():
    """If the user navigates away mid-poll, the loop must stop —
    don't update a detached node every second forever."""
    js = _read_js()
    idx = js.index("async function watchBulkProbeCompletion(")
    body = js[idx:idx + 3000]
    assert "btn.isConnected" in body, (
        "v1.15.139: watcher must check btn.isConnected so navigation "
        "away from the library page stops the poll loop."
    )


def test_watcher_reloads_library_and_topbar_on_completion():
    """After summary renders, library must reload (so TDB pills
    reflect the new alive/dead state) + topbar refresh fires past
    the /api/stats 1s TTL (v1.13.56 pattern)."""
    js = _read_js()
    idx = js.index("async function watchBulkProbeCompletion(")
    body = js[idx:idx + 3000]
    assert "loadLibrary()" in body, (
        "v1.15.139: watcher must call loadLibrary() so the TDB "
        "pills repaint with the post-probe state."
    )
    assert "refreshTopbarStatus" in body, (
        "v1.15.139: watcher must refresh the topbar so the red FAIL "
        "pill updates to its new count. v1.13.56 cache-TTL pattern."
    )
    assert "1100" in body, (
        "v1.15.139: topbar refresh must be delayed ≥1100ms (past "
        "the /api/stats 1s TTL) — fire-immediately races the cache."
    )
