"""v0.51.17 — round-4 audit Batch F2: frontend behavior.

Findings fixed here (all CONFIRMED by the adversarial verify pass):

  #17 (dashboard-customize.js) — fetchLayout silently mapped any failed
    GET to the default empty layout, and exitCustomize unconditionally
    flushes saveLayout(): a transient GET failure at page load (500
    while SQLite is locked, proxy 502) followed by merely entering and
    leaving customize mode permanently overwrote the user's stored
    layout with template defaults. The v1.17.13 loadConfigIntoForms
    failed-GET-then-SAVE data-loss class; v1.21.43 fixed only the PUT
    side. Now: a load failure latches LAYOUT_LOAD_FAILED; saveLayout
    refuses to PUT while latched; enterCustomize retries the GET and
    refuses entry (alert) if it still fails.

  #13 (ops.js) — poll() unconditionally scheduled its own successor
    without clearing the pending timer, so every direct invocation via
    motifOps.refresh (app.js visibilitychange + the opsHidden kick)
    permanently forked one extra self-perpetuating /api/progress loop;
    ~30 tab returns = ~30 parallel poll chains. Now poll() cancels the
    pending successor at entry and coalesces re-entrant calls via a
    pollInFlight latch (fetchProgress never throws, so the latch always
    resets at the re-arm).

  #28 (app.js) — six post-action sites called refreshTopbarStatus()
    immediately instead of the documented class-7
    setTimeout(..., 1100): the /api/stats 1s TTL cache + the v1.14.37
    hash-skip re-ratified the PRE-action payload, freezing stale
    topbar state (FAIL count after ACK, the amber DRY-RUN banner after
    disable) for up to 10s. 34 sibling sites already used the delay —
    contract-drift stragglers.

  #29 (app.js) — bulk DOWNLOAD and bulk TDB BACKUP alerted on error
    without clearing their optimistic '// QUEUING …' placeholder, and
    bulk ADOPT's three early exits (scan failure, zero candidates,
    cancelled confirm) stranded '// QUEUING ADOPTS' — the topbar kept
    promising queued work for the rest of the placeholder TTL after
    the failure (v1.15.35 convention). The INFO-card RE-DOWNLOAD site
    the audit also cited was already covered by the recovery
    dispatcher's v1.17.13 outer catch.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
DASH_JS = (REPO / "app" / "web" / "static" / "dashboard-customize.js").read_text()
OPS_JS = (REPO / "app" / "web" / "static" / "ops.js").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


# ── #17: layout GET-fail latch ────────────────────────────────


def test_fetch_layout_latches_failure():
    assert "let LAYOUT_LOAD_FAILED = false;" in DASH_JS
    i = DASH_JS.index("async function fetchLayout()")
    block = DASH_JS[i:i + 1200]
    # non-ok now routes through the catch (throw), which latches.
    assert "if (!r.ok) throw new Error('HTTP ' + r.status);" in block
    assert "LAYOUT_LOAD_FAILED = true;" in block
    # success clears the latch.
    assert "LAYOUT_LOAD_FAILED = false;" in block
    # the old silent default on non-ok must be gone.
    assert "if (!r.ok) return { sections: [] };" not in DASH_JS


def test_save_layout_refuses_while_latched():
    i = DASH_JS.index("async function saveLayout()")
    block = DASH_JS[i:i + 1600]
    guard = block.index("if (LAYOUT_LOAD_FAILED)")
    put = block.index("method: 'PUT'")
    assert guard < put, (
        "v0.51.17 #17: the latch guard must sit BEFORE the PUT — "
        "flushing template defaults overwrites the stored layout")
    assert "return;" in block[guard:put]


def test_enter_customize_retries_failed_load():
    i = DASH_JS.index("async function enterCustomize()")
    block = DASH_JS[i:i + 1000]
    assert "if (LAYOUT_LOAD_FAILED)" in block
    assert "LAYOUT = await fetchLayout();" in block
    # still-failed → refuse entry (return before CUSTOMIZE = true).
    refuse = block.index("return;")
    engage = block.index("CUSTOMIZE = true;")
    assert refuse < engage


# ── #13: poll fork ────────────────────────────────────────────


def test_poll_clears_pending_timer_at_entry():
    i = OPS_JS.index("async function poll()")
    block = OPS_JS[i:i + 1400]
    entry_clear = block.index("clearTimeout(state.pollTimer);")
    fetch = block.index("await fetchProgress();")
    assert entry_clear < fetch, (
        "v0.51.17 #13: the pending successor must be cancelled BEFORE "
        "the fetch, or direct motifOps.refresh calls fork the chain")


def test_poll_coalesces_reentrant_calls():
    assert "pollInFlight: false," in OPS_JS, "state must declare the latch"
    i = OPS_JS.index("async function poll()")
    block = OPS_JS[i:i + 1400]
    assert "if (state.pollInFlight) return;" in block
    assert "state.pollInFlight = true;" in block
    # the latch resets adjacent to the re-arm so the chain never dies.
    j = OPS_JS.index("state.pollTimer = setTimeout(poll, state.pollInterval);")
    assert "state.pollInFlight = false;" in OPS_JS[j - 200:j]


# ── #28: class-7 delays at the six straggler sites ────────────


def _delay_within(anchor: str, window: int = 500) -> bool:
    i = APP_JS.index(anchor)
    return "setTimeout(refreshTopbarStatus, 1100)" in APP_JS[i:i + window]


def test_dry_run_banner_disable_uses_delay():
    i = APP_JS.index("function bindDryRunBanner()")
    block = APP_JS[i:i + 1300]
    assert "setTimeout(refreshTopbarStatus, 1100)" in block
    assert "refreshTopbarStatus().catch" not in block


def test_set_dry_run_uses_delay():
    i = APP_JS.index("async function setDryRun(value)")
    block = APP_JS[i:i + 900]
    assert "setTimeout(refreshTopbarStatus, 1100)" in block
    assert "refreshTopbarStatus().catch" not in block


def test_libraries_save_kick_uses_delay():
    assert _delay_within("the next stats poll to surface a freshly-enabled tab.")


def test_config_save_paths_banner_kick_uses_delay():
    assert _delay_within(
        "// Refresh topbar so paths banner updates if themes_dir was just set")


def test_mark_alive_and_ack_use_delay():
    i = APP_JS.index("/mark-alive`);")
    block = APP_JS[i:i + 700]
    assert "setTimeout(refreshTopbarStatus, 1100)" in block
    assert "refreshTopbarStatus().catch" not in block
    # NB: "await api('POST', ackUrl);" also appears in a bulk ACK loop —
    # anchor on the INFO-card branch's unique v1.13.35 comment instead.
    j = APP_JS.index("forward sectionId on re-hydrate")
    block2 = APP_JS[j:j + 700]
    assert "setTimeout(refreshTopbarStatus, 1100)" in block2
    assert "refreshTopbarStatus().catch" not in block2


# ── #29: placeholder cleared on error paths ───────────────────


def test_bulk_download_clears_placeholder_on_failure():
    i = APP_JS.index("alert('Bulk download failed: ' + err.message);")
    block = APP_JS[i - 600:i]
    assert "clearOptimisticPlaceholder('download_queue')" in block, (
        "v0.51.17 #29: the failure alert must not leave '// QUEUING "
        "DOWNLOADS' pulsing (v1.15.35 convention)")


def test_bulk_tdb_backup_clears_placeholder_on_failure():
    i = APP_JS.index("alert('Bulk TDB backup failed: ' + err.message);")
    block = APP_JS[i - 600:i]
    assert "clearOptimisticPlaceholder('download_queue')" in block


def test_bulk_adopt_clears_placeholder_on_all_early_exits():
    i = APP_JS.index("const clearAdoptPulse = () => {")
    helper = APP_JS[i:i + 400]
    assert "clearOptimisticPlaceholder('adopt_queue')" in helper
    # scan-fail + zero-candidates + cancelled-confirm all clear.
    tail = APP_JS[i:i + 4000]
    assert tail.count("clearAdoptPulse();") == 3, (
        "all three early exits (scan failure, 0 candidates, cancelled "
        "confirm) must clear the pulse")
