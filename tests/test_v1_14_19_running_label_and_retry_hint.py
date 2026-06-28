"""v1.14.19 — live-ops "+N running" + queue retry-hint visibility.

Two related clarity fixes shipped together. Both came from
the user's bulk-download repro where a 5-item batch ran with
download_concurrency=2 and a transient 503 made one row look
"stuck".

## Item A — `+N more` → `+N running`

The progress headline previously read:

    Downloading: 2 Guns +1 more (3 queued)

The math was correct (extras = running_n - 1, so "+1 more" =
"+1 additional concurrent download"), but the wording reads
ambiguously next to the `(N queued)` suffix. User can't tell
if "+1 more" means "+1 running" or "+1 of the 3 queued".

v1.14.19 changes the suffix from `+N more` to `+N running`.
Now the two suffixes carry distinct, non-overlapping
semantics — how many are concurrently running, how many
are still waiting. Singular vs plural unchanged (the
`+N` shape works for both).

## Item B — queue retry hint

the user's 1408 row hit a YouTube 503 → classified as
network_error (transient, retry-eligible). The worker
re-queued it with exponential backoff
(`worker._mark_failed`: 1m → 5m → 25m → 125m). Job sat in
`pending` with the error message visible.

Pre-fix the /queue page showed:

    1124  download  movie 3021  pending  4m ago  ERROR: 503...

…with no indication an auto-retry was scheduled. Reads as
"stuck attempting to download" — the user's exact words.

v1.14.19 surfaces `next_run_at` inline with the last_error
when a pending row carries both:

    1124  download  movie 3021  pending  4m ago  ERROR: 503... ↻ retry in 4m 32s

Once the user can see the retry is scheduled, "stuck"
reads as "waiting" — accurate.

Implementation: pure JS render change in `loadQueue`. The
/api/jobs endpoint already returns `next_run_at` (it's a
column on the jobs table; the endpoint does `SELECT *`).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── Item A: +N running ────────────────────────────────────────


def test_progress_headline_uses_running_suffix():
    """The headline format string for multi-running downloads
    must use ` +N running` (not `+N more`).

    v1.14.21 superseded the literal `f"Downloading: {head} +{extras} running"`
    string from v1.14.19 with a multi-title-aware form
    (`titles_inline = running_dl_titles[:2]`). The CONTRACT is
    unchanged: "Downloading:" prefix on running labels, "+N running"
    suffix when more titles exist than fit inline. Pinning the new
    suffix shape directly to keep this test stable across the
    superseding tag."""
    src = (REPO / "app" / "core" / "progress.py").read_text()
    # The "running" suffix wording (vs the pre-v1.14.19 "more")
    # must still be present.
    assert "+{extras} running" in src
    # And the single-active form (no extras suffix) must exist —
    # v1.14.21 changed this to use `joined` (inline-titles list)
    # but the prefix shape `Downloading: ` stays.
    assert 'f"Downloading: {joined}"' in src or 'f"Downloading: {head}"' in src


def test_progress_headline_no_more_suffix_in_live_code():
    """Regression guard: the pre-fix `+{extras} more` shape
    must not survive in live code. Comment-stripped check so
    the rationale comment quoting the old shape doesn't trip
    the guard."""
    src_raw = (REPO / "app" / "core" / "progress.py").read_text()
    src = "\n".join(
        line for line in src_raw.splitlines()
        if not line.lstrip().startswith("#")
    )
    assert 'f"Downloading: {head} +{extras} more"' not in src


def test_progress_extras_math_counts_correctly():
    """The `extras` value on the running label is "concurrent
    running minus the titles shown inline" — what `+N running`
    actually counts.

    v1.14.21 superseded the v1.14.19 form (`running_n - 1`,
    one head title) with `running_n - len(titles_inline)`
    (cap of 2 inline). The CONTRACT — "+N running" reads as
    "N additional running titles not listed inline" — is
    preserved. This test pins the correct math expression
    against either shape."""
    src = (REPO / "app" / "core" / "progress.py").read_text()
    # v1.14.21 form: extras = running_n - len(titles_inline)
    # v1.14.19 form: extras = running_n - 1
    assert (
        "extras = running_n - len(titles_inline)" in src
        or "extras = running_n - 1" in src
    )


# ── Item B: retry hint on pending rows ────────────────────────


def test_queue_render_emits_retry_hint_for_pending_with_error():
    """The /queue render must compute a retryHint when status is
    pending AND last_error is set AND next_run_at is in the
    future. Surfaced inline with the error cell so users
    investigating a "stuck" row can tell it's actually
    auto-retrying."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # The gating condition.
    assert "if (j.status === 'pending' && j.last_error && j.next_run_at)" in js
    # The hint format — minutes/seconds rendered as "Xm Ys" or "Ys".
    assert "↻ retry in ${dur}" in js
    # Edge case: next_run_at already past → "↻ retry pending".
    assert "↻ retry pending" in js


def test_retry_hint_ms_threshold_avoids_flicker():
    """The hint shows "retry in Xm Ys" only when next_run_at is
    >1 second in the future. Below that we show "↻ retry
    pending" instead of "0s" to avoid flickering between the
    two formats during the worker's pickup window."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert "if (deltaMs > 1000)" in js


def test_retry_hint_appended_to_error_cell():
    """The hint appends to the last_error cell, not the status
    cell. Status stays "pending" (the literal job status) and
    the error cell carries the retry context — keeping the
    status column scannable."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # The error cell template includes ${retryHint} after the
    # error string slice. v1.19.6 replaced the <table> with a
    # grid-based <li>/<span> structure, so the closing tag is
    # now </span> not </td>. Accept either to survive the
    # architecture migration.
    assert (".slice(0, 60))}${retryHint}</td>" in js
            or ".slice(0, 60))}${retryHint}</span>" in js), (
        "v1.14.19 + v1.19.6: retry hint must close the error "
        "cell (either </td> legacy or </span> v1.19.6)"
    )


def test_retry_hint_skipped_for_non_pending_rows():
    """retryHint only fires on `j.status === 'pending'`. Failed,
    done, cancelled rows don't get the hint — they're terminal
    states with no auto-retry. Pin via the gating expression."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # The condition must specifically gate on 'pending'.
    pos = js.index("if (j.status === 'pending' && j.last_error && j.next_run_at)")
    # No alternate gates like ['pending','failed'] in this branch.
    surrounding = js[pos:pos + 500]
    assert "j.status === 'failed'" not in surrounding


def test_retry_hint_carries_full_timestamp_in_tooltip():
    """The relative-time text is the at-a-glance reading; the
    full ISO timestamp goes into the title= tooltip so the user
    can hover to see the exact scheduled retry moment if they
    want it."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert 'title="auto-retry scheduled at ${htmlEscape(fmt.time(j.next_run_at))}"' in js


def test_retry_hint_try_block_swallows_date_parse_errors():
    """next_run_at parsing wrapped in try/catch so a malformed
    timestamp can't crash the queue render. Defensive — the
    server emits ISO timestamps reliably, but the JS render
    shouldn't depend on that contract for liveness."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # The try wraps the Date parse + delta math + hint assignment.
    block_start = js.index("if (j.status === 'pending' && j.last_error && j.next_run_at)")
    block = js[block_start:block_start + 1500]
    assert "try {" in block
    assert "} catch (_)" in block


# ── /api/jobs endpoint emits next_run_at ──────────────────────


def test_api_jobs_endpoint_returns_next_run_at():
    """The /api/jobs endpoint must include next_run_at in its
    response so the client-side hint has the data it needs.
    The endpoint uses `SELECT *` which includes every column
    on the jobs table — pin via the SELECT shape."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    # Anchor on api_jobs handler.
    fn_anchor = src.index("async def api_jobs(")
    # v1.18.95: window widened 1500 → 6000 — the op_progress
    # synthesis added ~80 lines (comment block + SELECT op_progress
    # + dict translation) inside the function ahead of the
    # SELECT * FROM jobs, pushing it past the old window.
    # v1.19.47: widened 6000 → 9000 for the active/terminal
    # partitioning logic + recency-sort key that pushed both
    # the dict-comp and the return further down.
    body = src[fn_anchor:fn_anchor + 9000]
    # SELECT * grabs every column including next_run_at.
    assert "SELECT * FROM jobs" in body
    # And the response carries job rows as dicts so column names
    # come through verbatim. v1.18.95 wrapped the dict-list in
    # synthesized + [...] + [:limit] so the literal full-line
    # match no longer holds — assert the dict-comp shape instead.
    # v1.19.47: the dict-comp moved into a `real_jobs = ...`
    # assignment when terminal-vs-active partitioning landed.
    assert "[dict(r) for r in rows]" in body
    assert 'return {"jobs":' in body


# ── Regression guard: backoff schedule is the source of truth ─


def test_worker_backoff_schedule_unchanged():
    """The retry hint reads from `next_run_at` which is set by
    `_mark_failed` per the v1.11.x exponential backoff schedule
    (5 ** (attempts - 1) minutes → 1m, 5m, 25m, 125m). Pin
    that schedule so a future change to the backoff math
    doesn't silently change what the hint counts down toward."""
    src = (REPO / "app" / "core" / "worker.py").read_text()
    assert "backoff_minutes = 5 ** (row[\"attempts\"] - 1)" in src
