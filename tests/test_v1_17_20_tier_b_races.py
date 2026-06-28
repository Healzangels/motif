"""v1.17.20 — Tier B frontend race fixes.

The v1.17.12 three-agent audit returned 8 frontend-race findings.
v1.17.13 closed the Tier A subset (optimistic-placeholder pairing,
visibility-guard polling). v1.17.20 picks up 3 of the remaining
Tier B race items + the closely-related Error UX MED 6.

## Race #3 — stuck watcher reset on timeout

Both `_watchOpForCompletion` (settings-page PROBE / REPROBE
buttons) and the `bulk_lps` `finishWatcher` (library page LET
PLEX SERVE) already had a 30-minute TIMEOUT_MS. Pre-fix:
* `_watchOpForCompletion` silently called `clearInterval(...)`
  and returned. Status text was left at whatever progress hint
  was last written; user had no signal the watcher gave up.
* `finishWatcher` reset the button on timeout but didn't
  reload the library or topbar — the page sat stale.

Fix:
* `_watchOpForCompletion` writes a "× watcher timed out after
  30 min — see // LIVE OPS for actual op status" message to
  the statusEl + auto-dismisses (mirrors the terminal-state
  paths just below).
* `finishWatcher` reloads the library + refreshes topbar on
  timeout so the page reflects whatever final state the op
  landed in. Logs a `console.warn` so the timeout is greppable
  in dev tools.

## Race #6 — status-flash timer collision on re-click

`bindConfigSaves` and the libraries-save dispatcher scheduled
status auto-clears via raw `setTimeout(...)` with no awareness
of fresh clicks. A re-click within the 2.5-4 s dismiss window
left two timers running; the first fires while the second's
'saving…' / '✓ saved' is on screen and wipes it.

Fix: snapshot-compare pattern (mirrors `_autoDismissOpStatus`
at line ~5464). Capture `status.textContent` BEFORE scheduling
the dismiss; inside the setTimeout, bail if the textContent
has changed since the snapshot. The fresh click's status
survives because its textContent differs from the prior
click's snapshot.

## Race #7 — openInfoDialog in-flight seq guard

Two rapid INFO clicks on different rows both wrote
`'loading…'` to the dialog body, both awaited /api/items, and
whichever response landed last painted its data — even if it
wasn't the user's currently-intended row. The recovery-action
buttons attached inside the rendered body then carried the
wrong data-mt/data-id, so subsequent action clicks fired
against the wrong row.

Fix: `openInfoDialog._seq` token, same pattern as
`loadLibrary._seq` (line 6348). Check the token after the
await; bail without rewriting body.innerHTML if a newer click
has started.

## Error UX MED 6 — bulk per-row "N FAILED" with no reason

Two bulk-action loops (accept-all-updates, decline-all-updates)
rendered `// 3 ACCEPTED · 2 FAILED` toast without surfacing
WHY rows failed. The catch path console.error'd the per-item
error, but the operator had to open devtools to learn the
reason.

Fix: capture the FIRST failure message in `firstFailMsg`,
append it (truncated to 60 chars) to the FAILED segment of
the toast. Mirrors libraries-save at line ~4099.
"""

from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"
APP_INIT = REPO / "app" / "__init__.py"


# ── Race #3 — watcher timeout surfaces ────────────────────────


def test_watch_op_for_completion_surfaces_timeout():
    """`_watchOpForCompletion` must write a timeout message to
    statusEl instead of silently clearing the interval."""
    src = APP_JS.read_text()
    idx = src.index("function _watchOpForCompletion(")
    body = src[idx:idx + 2500]
    # The TIMEOUT_MS branch must surface a "timed out" message.
    assert "watcher timed out" in body.lower(), (
        "v1.17.20: _watchOpForCompletion 30-min timeout must "
        "write a 'watcher timed out' message to the statusEl. "
        "Audit race #3."
    )
    # And must call _autoDismissOpStatus so the stale timeout
    # message doesn't linger.
    assert "_autoDismissOpStatus(statusEl)" in body


def test_bulk_lps_finish_watcher_reloads_on_timeout():
    """`finishWatcher` (bulk LPS) must reload library + topbar
    on timeout so the page isn't stale."""
    src = APP_JS.read_text()
    idx = src.index("const finishWatcher = setInterval(")
    body = src[idx:idx + 2000]
    # The timeout branch must trigger a library reload.
    timeout_block = body[body.index("> TIMEOUT_MS"):]
    assert "loadLibrary().catch" in timeout_block, (
        "v1.17.20: finishWatcher timeout must trigger a "
        "loadLibrary() reload so the page reflects the op's "
        "final state."
    )
    assert "refreshTopbarStatus()" in timeout_block, (
        "v1.17.20: finishWatcher timeout must also refresh "
        "the topbar."
    )


# ── Race #6 — status-flash snapshot-compare ───────────────────


def test_bind_config_saves_uses_snapshot_compare():
    """`bindConfigSaves` finally block must snapshot the
    status textContent before scheduling the dismiss + bail
    inside the setTimeout if the snapshot doesn't match."""
    src = APP_JS.read_text()
    idx = src.index("function bindConfigSaves()")
    body = src[idx:idx + 5000]
    assert "const snapshot = s.textContent" in body, (
        "v1.17.20: bindConfigSaves dismiss timer must capture "
        "a snapshot before scheduling so a re-click within "
        "the dismiss window keeps its fresh status."
    )
    assert "if (s.textContent !== snapshot) return" in body


def test_libraries_save_uses_snapshot_compare():
    """The libraries-save dispatcher must use the same
    snapshot-compare pattern in its dismiss timer."""
    src = APP_JS.read_text()
    # Anchor on the unique "saved N sections" or "of failed"
    # string used only in this site.
    idx = src.index("saved ${ok} section")
    # The snapshot block follows shortly after the success path.
    window = src[idx:idx + 1500]
    assert "const snapshot = status.textContent" in window, (
        "v1.17.20: libraries-save dismiss timer must use "
        "snapshot-compare for re-click safety."
    )


# ── Race #7 — openInfoDialog seq guard ────────────────────────


def test_open_info_dialog_uses_seq_guard():
    """`openInfoDialog` must use the same _seq token pattern
    that loadLibrary / loadQueue use to bail when a newer
    click has superseded the current one."""
    src = APP_JS.read_text()
    idx = src.index("async function openInfoDialog(")
    body = src[idx:idx + 4000]
    # Seq token declared.
    assert "openInfoDialog._seq" in body, (
        "v1.17.20: openInfoDialog must declare an _seq token "
        "for in-flight guard (same pattern as loadLibrary._seq)."
    )
    # Token captured pre-await.
    assert "const _myToken = openInfoDialog._seq" in body
    # Guarded BOTH the error path AND the success path after
    # the await — a stale catch shouldn't paint either way.
    seq_check_count = body.count("openInfoDialog._seq !== _myToken")
    assert seq_check_count >= 2, (
        "v1.17.20: openInfoDialog must check the _seq token "
        "on BOTH the success and error paths after the api() "
        "await."
    )


# ── Error UX MED 6 — bulk failure-reason capture ──────────────


def test_bulk_accept_all_captures_first_failure_message():
    """The accept-all-updates bulk loop captures the first
    error.message into firstFailMsg + appends it to the
    FAILED segment of the toast."""
    src = APP_JS.read_text()
    # Anchor on the progress text unique to the accept-all
    # click handler (the per-iteration label written inside
    # the loop body).
    idx = src.index("// ACCEPTING ${i + 1}/${selection.length}")
    body = src[max(0, idx - 500):idx + 1500]
    assert "let firstFailMsg = '';" in body, (
        "v1.17.20: accept-all bulk loop must declare "
        "firstFailMsg before the per-row loop."
    )
    # The catch block captures the first error.
    assert "if (!firstFailMsg)" in body
    # The toast appends the truncated message.
    assert "firstFailMsg.slice(0, 60)" in body, (
        "v1.17.20: the FAILED toast must include the first "
        "error.message (truncated to 60 chars) so the user "
        "has WHY without opening devtools."
    )


def test_bulk_decline_all_captures_first_failure_message():
    """Same shape applies to the decline-all-updates loop."""
    src = APP_JS.read_text()
    # Anchor on the unique DISMISSING progress text.
    idx = src.index("// DISMISSING ${i + 1}/${selection.length}")
    body = src[max(0, idx - 500):idx + 1500]
    assert "let firstFailMsg = '';" in body
    assert "firstFailMsg.slice(0, 60)" in body


# ── Version pin (soft floor) ──────────────────────────────────


def test_version_pinned_at_or_above_1_17_20():
    src = APP_INIT.read_text()
    m = re.search(r'__version__\s*=\s*"(\d+)\.(\d+)\.(\d+)"', src)
    assert m
    found = tuple(int(x) for x in m.groups())
    assert found >= (0, 17, 20), (
        f"v1.17.20: __version__ must be >= 1.17.20 "
        f"(found {'.'.join(str(x) for x in found)})."
    )
