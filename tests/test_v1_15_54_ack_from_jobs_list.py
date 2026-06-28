"""v1.15.54 — per-job ACK button in /queue failed-jobs list.

the user: "let's add the ability to ack failures in the failed
section of jobs. they'll remain there after ack but allows us
to see unack from the dashboard at a glance."

Pre-v1.15.54 the operator had to navigate from /queue (where
they were reviewing the failed job) to the library row (via the
OPEN ROW event-stream button or by hunting through /movies) to
ACK. Now there's an inline `// ACK` button on each failed-non-
acked job row.

## Behavior

* Button shows when `j.status === 'failed' && !j.acked_at`.
  (v1.19.93 dropped the original `&& j.media_type && j.tmdb_id`
  requirement — theme-less global jobs, e.g. a failed SYNC run,
  are ackable too via /api/jobs/{id}/ack-failure, which stamps
  jobs.acked_at and skips the theme ack. Op-progress synthetic
  rows are excluded — their string ids can't hit the int endpoint.)
* Click → POST `/api/items/{mt}/{id}/clear-failure` (same
  endpoint the library row + bulk ACK paths use). Clears the
  theme's `failure_kind` + `failure_acked_at`.
* Job row STAYS in the failed list (still status='failed') —
  flips visually to the green "failed (acknowledged)" style
  via the existing `isAckedFail` branch (v1.12.12). FAILED
  filter chip still surfaces the row for audit; the topbar
  FAIL pill drops via the `unacked_failures` count.

Static-text guards consistent with v1.11.36 (cancel-job button)
test patterns.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"


# ── 1. Button rendering ─────────────────────────────────────


def test_ackable_fail_predicate_gates_button():
    """v1.19.93: the predicate gates on status==='failed' +
    !acked_at + !isOpProgress — and NO LONGER on media_type/
    tmdb_id. A failed SYNC/enum job (no theme binding) must get a
    // ACK button too, since /api/jobs/{id}/ack-failure handles
    theme-less jobs (stamps jobs.acked_at, skips the theme ack).
    Pre-fix the crashed v1.19.71 sync job was undismissable."""
    js = APP_JS.read_text()
    marker = "v1.15.54: per-job ACK button"
    anchor = js.index(marker)
    block = js[anchor:anchor + 1800]
    assert "j.status === 'failed'" in block, (
        "v1.19.93: ackableFail must still gate on status==='failed'"
    )
    assert "!j.acked_at" in block, (
        "v1.19.93: ackableFail must still gate on !j.acked_at "
        "(already-acked rows shouldn't show ACK again)"
    )
    assert "!isOpProgress" in block, (
        "v1.19.93: ackableFail must exclude op-progress rows — "
        "their string ids can't hit the int-typed ack endpoint"
    )
    # The media_type/tmdb_id gate is GONE — scope the absence
    # check to the predicate line so the explanatory comment
    # (which mentions 'no media_type') doesn't false-match.
    p = js.index("const ackableFail =")
    pred = js[p:p + 160]
    assert "j.media_type" not in pred, (
        "v1.19.93: ackableFail must NOT require j.media_type — "
        "theme-less sync/enum failures are ackable too"
    )


def test_ack_button_renders_with_btn_warn_tone():
    """// ACK button uses btn-warn (amber) tone — distinct from
    the btn-danger (red) CANCEL button so the visual register
    matches the semantic ("warn = dismiss the alert" vs
    "danger = abort the work")."""
    js = APP_JS.read_text()
    marker = "v1.15.54: per-job ACK button"
    anchor = js.index(marker)
    # v1.23.81: widened 2000→2400 — the label grew "// ACK" → "// ACK
    # FAILURE", pushing its tail just past the old window boundary.
    block = js[anchor:anchor + 2400]
    assert 'data-act="ack-job-failure"' in block, (
        "v1.15.54: ACK button must use data-act='ack-job-failure' "
        "so the delegated click handler can pick it up"
    )
    # v1.15.60: btn-warn (amber) dropped in favor of default
    # green theme tone per the user: "can we make this ack button
    # green". Test now asserts the bare `btn btn-tiny` class.
    assert 'class="btn btn-tiny"' in block, (
        "v1.15.60: ACK button must use the bare 'btn btn-tiny' "
        "class for the default green theme tone"
    )
    assert "// ACK FAILURE" in block, (
        "v1.23.81: per-row label is '// ACK FAILURE' — aligned with "
        "the bulk bar's '// ACK FAILURES' and the // -prefix "
        "convention (was the bare '// ACK', which read as a different "
        "action than the bulk control)"
    )


def test_ack_button_carries_mt_and_id_data_attrs():
    """The button must carry data-mt + data-id from the job row
    so the click handler can build the /clear-failure URL
    without re-querying the row."""
    js = APP_JS.read_text()
    marker = "v1.15.54: per-job ACK button"
    anchor = js.index(marker)
    block = js[anchor:anchor + 2500]
    assert 'data-mt="${htmlEscape(j.media_type)}"' in block
    assert 'data-id="${htmlEscape(j.tmdb_id)}"' in block


# ── 2. Click handler ─────────────────────────────────────────


def test_ack_click_handler_targets_ack_failure_endpoint():
    """v1.15.60: handler must POST to /api/jobs/{id}/ack-failure
    (NEW endpoint that acks BOTH job + theme atomically).

    Pre-v1.15.60 the handler POSTed /api/items/{mt}/{id}/clear-
    failure — that cleared the theme but left jobs.acked_at NULL
    so the job row stayed red. the user: "clicking htem actually
    doesn't do anything currently." The fix routes through a
    single endpoint that updates both surfaces."""
    js = APP_JS.read_text()
    marker = "v1.15.54 / v1.15.60: per-job ACK handler"
    anchor = js.index(marker)
    body = js[anchor:anchor + 2500]
    assert 'data-act="ack-job-failure"' in body, (
        "v1.15.54: handler must close over button[data-act='ack-job-failure']"
    )
    assert "/api/jobs/${encodeURIComponent(jobId)}/ack-failure" in body, (
        "v1.15.60: handler must POST /api/jobs/{id}/ack-failure "
        "— the v1.15.60 endpoint that acks job + theme together"
    )


def test_ack_handler_refreshes_topbar_past_stats_ttl():
    """Post-ACK the topbar FAIL pill must drop within ~1.1s
    (same 1100ms pattern every other ACK path uses to clear
    the /api/stats 1s TTL cache)."""
    js = APP_JS.read_text()
    # v1.15.60: marker comment header extended.
    marker = "v1.15.54 / v1.15.60: per-job ACK handler"
    anchor = js.index(marker)
    body = js[anchor:anchor + 2500]
    assert "refreshTopbarStatus" in body, (
        "v1.15.54: handler must refresh topbar so FAIL count drops"
    )
    assert "1100" in body, (
        "v1.15.54: topbar refresh must wait ≥1100ms past the 1s "
        "/api/stats TTL — see CLAUDE.md § recurring bug class #7"
    )


def test_ack_handler_reloads_queue_to_reflect_acked_style():
    """After ACK the queue must reload so the row flips from
    red "failed" to green "failed (acknowledged)" via the
    existing isAckedFail branch (v1.12.12). Otherwise the
    button stays gone but the row still reads as un-acked
    until the next poll cycle."""
    js = APP_JS.read_text()
    # v1.15.60: marker comment header extended.
    marker = "v1.15.54 / v1.15.60: per-job ACK handler"
    anchor = js.index(marker)
    body = js[anchor:anchor + 2500]
    assert "loadQueue()" in body, (
        "v1.15.54: handler must call loadQueue() after the POST "
        "so the row flips to the acknowledged style immediately"
    )


def test_ack_handler_shows_progress_label():
    """During the POST the button shows '// ACKING…' so the
    operator knows the click registered. Matches the v1.15.51
    bulk-button progress convention."""
    js = APP_JS.read_text()
    # v1.15.60: marker comment header extended.
    marker = "v1.15.54 / v1.15.60: per-job ACK handler"
    anchor = js.index(marker)
    body = js[anchor:anchor + 2500]
    assert "// ACKING…" in body, (
        "v1.15.54: in-flight label must be '// ACKING…' (matches "
        "v1.15.51 bulk-button progress convention)"
    )


# ── 3. Counter-guard: existing cancel button still works ────


def test_cancel_job_button_still_present_alongside_ack():
    """The new ACK button must not displace the existing CANCEL
    button — pending/running rows still need their CANCEL
    affordance. Pin so a refactor doesn't accidentally swap
    one for the other."""
    js = APP_JS.read_text()
    # Both buttons must exist in the actionCell ternary chain.
    actionCell_anchor = js.index("const actionCell = cancelling")
    block = js[actionCell_anchor:actionCell_anchor + 2000]
    assert 'data-act="cancel-job"' in block
    assert 'data-act="ack-job-failure"' in block
    # The ternary order matters: cancelling → cancellable →
    # ackableFail → ''. ACK must come AFTER cancellable so a
    # pending job with a stuck-fail history still shows CANCEL.
    cancel_pos = block.index('data-act="cancel-job"')
    ack_pos = block.index('data-act="ack-job-failure"')
    assert cancel_pos < ack_pos, (
        "v1.15.54: CANCEL branch must precede ACK in the ternary "
        "so pending/running jobs prioritize the cancel affordance"
    )
