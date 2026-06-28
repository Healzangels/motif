"""v1.14.21 — multi-title download headline + hide misleading ELAPSED on pending+0-progress.

Two related live-ops UX clarity fixes from the user's bulk-download
repro screenshot:

## Item A — list multiple running titles inline

Pre-fix the download-queue card headline read:

    Downloading: (500) Days of Summer +1 running (26 queued)

The "+1 running" was technically correct (running_n - 1 = 1
extra concurrent download) but the user couldn't see WHICH
second download was happening. With download_concurrency=2,
only one of the two running titles was named.

v1.14.21 lists both titles inline when running_n <= 2:

    1 running:    Downloading: (500) Days of Summer
    2 running:    Downloading: (500) Days of Summer, Avatar
    3+ running:   Downloading: A, B +N running

Cap of 2 inline keeps the headline scannable on higher-
concurrency setups while showing both names in the common
default case.

## Item B — hide ELAPSED meta-row on pending+0-progress cards

Pre-fix the place-queue card showed:

    // PLACE QUEUE                            PENDING
    Place into Plex queued (20)
    0 / 20
    ELAPSED 1m 18s

The ELAPSED counter ticking up from 0 reads as "stuck working"
even though the worker hasn't picked the job up yet — it's
queued behind in-flight downloads. the user's exact word:
"not doing anything".

v1.14.21 suppresses the ELAPSED meta-row when the op is
pending AND stage_current is 0. The PENDING badge in the card
header + the 0/N counter already convey waiting state — the
ticking timer was the misleading element.

The card still shows when work IS happening (status='running'
OR stage_current > 0) — only the genuine "queued, hasn't
started" combo loses the timer.
"""
from __future__ import annotations

from pathlib import Path

from app.core.progress import _synthesize_queue_ops


REPO = Path(__file__).resolve().parent.parent


# ── Item A: behavioral test of headline format ───────────────


def _counts_row(jt, running_n, pending_n):
    """Helper: build a dict that quacks like a sqlite3.Row."""
    return {"job_type": jt, "running_n": running_n, "pending_n": pending_n}


def test_single_running_download_shows_one_title():
    """1 running download → headline is `Downloading: <title>`."""
    ops = _synthesize_queue_ops(
        counts=[_counts_row("download", running_n=1, pending_n=0)],
        running_dl_jobs=[1001],
        running_dl_titles=["(500) Days of Summer"],
    )
    assert any(
        op.get("kind") == "download_queue"
        and op.get("stage_label") == "Downloading: (500) Days of Summer"
        for op in ops
    ), f"got: {[op.get('stage_label') for op in ops]}"


def test_two_running_downloads_list_both_titles_inline():
    """2 running downloads → both titles shown comma-separated.
    This is the headline shape the user was missing — with
    download_concurrency=2 the user can now see BOTH titles.

    v1.15.5: the "(N queued)" inline suffix was dropped from
    download_queue specifically — the topbar's +N QUEUED badge
    surfaces it now (parity with plex_enum_pending /
    tdb_sync_pending). The detail.queue_depth field carries
    the count for the badge to read; the label stays clean.
    Other queue kinds (place / scan / refresh / relink / adopt)
    still keep their inline suffix since they don't get the
    badge surface."""
    ops = _synthesize_queue_ops(
        counts=[_counts_row("download", running_n=2, pending_n=26)],
        running_dl_jobs=[1001, 1002],
        running_dl_titles=["(500) Days of Summer", "Avatar"],
    )
    matched = [op for op in ops if op.get("kind") == "download_queue"]
    assert matched, "expected a download_queue op"
    label = matched[0].get("stage_label", "")
    # Both titles inline.
    assert "(500) Days of Summer" in label
    assert "Avatar" in label
    # No "+N running" suffix (both fit inline).
    assert "+1 running" not in label
    assert "+0 running" not in label
    # v1.15.5: queue depth moved to detail.queue_depth — the
    # badge reads it, the label stays clean.
    assert "queued" not in label, (
        "v1.15.5: download_queue label must NOT inline the "
        "(N queued) suffix — the +N badge handles it"
    )
    detail = matched[0].get("detail", {}) or {}
    assert detail.get("queue_depth") == 26


def test_three_running_downloads_show_two_inline_plus_extras():
    """3 running downloads → first 2 inline + "+1 running" suffix.
    Cap keeps the headline scannable on higher-concurrency setups.

    v1.15.5: queue depth moved to detail.queue_depth, see the
    test_two_running variant above for the rationale."""
    ops = _synthesize_queue_ops(
        counts=[_counts_row("download", running_n=3, pending_n=10)],
        running_dl_jobs=[1001, 1002, 1003],
        running_dl_titles=["A", "B", "C"],
    )
    matched = [op for op in ops if op.get("kind") == "download_queue"]
    assert matched
    label = matched[0].get("stage_label", "")
    # First two inline.
    assert "A, B" in label
    # Third one collapsed into +N running.
    assert "+1 running" in label
    # v1.15.5: queue depth in detail, not in label.
    assert "queued" not in label
    detail = matched[0].get("detail", {}) or {}
    assert detail.get("queue_depth") == 10


def test_four_running_downloads_show_two_inline_plus_two_extras():
    """4 running → "Downloading: A, B +2 running"."""
    ops = _synthesize_queue_ops(
        counts=[_counts_row("download", running_n=4, pending_n=0)],
        running_dl_jobs=[1, 2, 3, 4],
        running_dl_titles=["A", "B", "C", "D"],
    )
    matched = [op for op in ops if op.get("kind") == "download_queue"]
    label = matched[0].get("stage_label", "")
    assert "A, B" in label
    assert "+2 running" in label


def test_zero_running_uses_queued_label_unchanged():
    """Regression guard: when 0 running + N pending, the headline
    uses the queued-label form (`Theme download queued (N)`)
    not the `Downloading: ...` form."""
    ops = _synthesize_queue_ops(
        counts=[_counts_row("download", running_n=0, pending_n=5)],
        running_dl_jobs=[],
        running_dl_titles=[],
    )
    matched = [op for op in ops if op.get("kind") == "download_queue"]
    label = matched[0].get("stage_label", "")
    assert "Downloading:" not in label
    assert "Theme download queued" in label
    # Status is 'pending' (worker hasn't picked anything up).
    assert matched[0].get("status") == "pending"


# ── Item A: source-text guards on the new logic ──────────────


def test_progress_titles_inline_uses_two_cap():
    """The new logic caps inline titles at 2 via
    `running_dl_titles[:2]`. Pin so a refactor doesn't push
    the cap higher and overflow the headline on N-way
    concurrency."""
    src = (REPO / "app" / "core" / "progress.py").read_text()
    assert "titles_inline = running_dl_titles[:2]" in src


def test_progress_extras_computed_from_inline_count():
    """The new extras count is `running_n - len(titles_inline)`.
    Pre-v1.14.21 used `running_n - 1` (always one head title).
    Pin the new shape so a regression doesn't silently flip
    the meaning of `+N running`."""
    src = (REPO / "app" / "core" / "progress.py").read_text()
    assert "extras = running_n - len(titles_inline)" in src


def test_pre_fix_single_head_shape_is_gone():
    """Regression guard: the pre-fix `head = running_dl_titles[0]`
    + `extras = running_n - 1` shape must not survive — it
    hardcoded "show 1 title, the rest as +N running" which is
    exactly what the user flagged as confusing.

    Strip line-comments so the rationale comment quoting the
    deleted shape doesn't trip the guard."""
    src_raw = (REPO / "app" / "core" / "progress.py").read_text()
    src = "\n".join(
        line for line in src_raw.splitlines()
        if not line.lstrip().startswith("#")
    )
    assert "head = running_dl_titles[0]" not in src
    # And the live extras computation no longer uses the bare -1 form.
    assert "extras = running_n - 1" not in src


# ── Item B: ops.js suppresses ELAPSED on pending+0-progress ──


def test_ops_js_hides_elapsed_when_pending_and_zero_progress():
    """The ELAPSED meta-row template now sits inside an IIFE
    that bails when `op.status === 'pending' && stage_current ===
    0`. The PENDING badge + 0/N counter already convey waiting;
    the ticking timer was the misleading element."""
    js = (REPO / "app" / "web" / "static" / "ops.js").read_text()
    # The new isStuckPending check.
    assert "const isStuckPending = isLive" in js
    assert "op.status === 'pending'" in js
    assert "(op.stage_current || 0) === 0" in js
    # The early-return that suppresses the row.
    assert "if (elapsed == null || isStuckPending) return ''" in js


def test_ops_js_still_shows_elapsed_when_running():
    """Regression guard: when status='running' (worker is
    actively on the job) the ELAPSED row must still render —
    the user wants to know how long the active work has been
    going. Only the pending+0 combo is suppressed."""
    js = (REPO / "app" / "web" / "static" / "ops.js").read_text()
    # Inside the IIFE, after the bail, we render the elapsed.
    block_start = js.index("const isStuckPending = isLive")
    block = js[block_start:block_start + 1500]
    assert 'isLive ? \'ELAPSED\' : \'RAN\'' in block
    assert 'esc(fmtDuration(elapsed))' in block


def test_ops_js_pre_fix_unconditional_elapsed_render_is_gone():
    """Regression guard: the pre-fix simple ternary
    `${(elapsed != null) ? ... : ''}` must not survive — that's
    the shape that rendered the misleading timer for pending+0
    cards.

    Strip comments before the check so the rationale doesn't
    trip it."""
    js_raw = (REPO / "app" / "web" / "static" / "ops.js").read_text()
    js = "\n".join(
        line for line in js_raw.splitlines()
        if not line.lstrip().startswith("//")
    )
    # The pre-fix one-liner ternary that wraps the elapsed span.
    forbidden = "${(elapsed != null) ? `\n            <span class=\"op-card-meta-item\">"
    assert forbidden not in js, (
        "v1.14.21: pre-fix unconditional ELAPSED render must "
        "not survive — would re-show the misleading timer on "
        "pending+0-progress cards"
    )
