"""v1.15.5 — three fixes the user pulled from his v1.15.4 docker logs.

## A. Probe: "Requested format is not available" = alive

Docker log spam during the bulk-probe-tdb run:
    ERROR: [youtube] zguqSx-QN6Q: Requested format is not
        available. Use --list-formats for a list of available
        formats

This error means yt-dlp got far enough to enumerate the
video's available formats but the format spec we want isn't
available. Common causes: stale yt-dlp version, age-gated
video without auth (only low-res formats exposed), region-
locked format manifest, members-only content. NONE of these
are deadness signals — the URL is alive.

Pre-fix `classify_yt_dlp_error` had no pattern for this and
returned `UNKNOWN`. The bulk-probe routed it as "ambiguous"
(could be transient or dead), red-pilling rows that were
actually fine. the user's UI showed "? Unknown error" on rows
whose videos were perfectly playable.

Fix: in `probe_youtube_url`, check the error string for
"requested format is not available" and return `None` (alive)
before falling through to `classify_yt_dlp_error`.

## B. +N QUEUED badge for queued downloads

the user: "I would like to update the way we handle multiple
downloads queued up similar to the way we show queued up
refresh jobs or sync and refresh jobs"

Pre-fix download_queue inlined "(N queued)" in the mini-bar
label; plex_enum_pending and tdb_sync_pending each had a
separate +N QUEUED badge alongside their running mini-bars.
Visual treatment was inconsistent.

Fix:
- `_build_queue_detail` exposes `queue_depth` on
  download_queue when running > 0 + pending > 0.
- The download_queue stage_label drops the inline "(N queued)"
  suffix in that case (badge surfaces it instead).
- The topbar overflow pill adds a download_queue branch with
  warn tone (matches the download_queue card's tone family).
- Composes naturally with plex/sync queued: priority order is
  plex → sync → download. Mixed states fall back to the
  highest-priority queue's badge; the drawer has the full
  breakdown.

## C. LET PLEX SERVE recovery option gated on motif_has_content

the user: "let plex server is an option which doesn't make sense
here since its already a P row and let plex server would try
to download the themerrdb url and have a downloaded copy but
let plex server it's theme which is impossible on a failed
url or red tdb pill."

The pre-fix `purge-and-ack` recovery option (labeled "LET
PLEX SERVE") fired whenever Plex served its own theme
(`plex_independent_theme=1`). For pure-P rows with no motif
content (DL=off, PL=off), the action would just ack the
failure since there's nothing to purge — but the LABEL "LET
PLEX SERVE" implied a Plex transition that wasn't happening
(Plex was already serving).

Fix: gate the option additionally on `motif_has_content` (a
local_files OR placements row exists in this section). When
motif owns nothing, the user gets ACK FAILURE alone —
semantically accurate (TDB URL dead, Plex already serves,
just dismiss the failure flag).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
DOWNLOADER_PY = REPO / "app" / "core" / "downloader.py"
PROGRESS_PY = REPO / "app" / "core" / "progress.py"
OPS_JS = REPO / "app" / "web" / "static" / "ops.js"
API_PY = REPO / "app" / "web" / "api.py"


# ── A. Probe format-unavailable returns alive ──────────────────


def test_probe_returns_none_on_format_not_available():
    """probe_youtube_url must short-circuit the
    classify_yt_dlp_error path when the error mentions
    "requested format is not available" — that's an alive
    signal, not a deadness signal."""
    src = DOWNLOADER_PY.read_text()
    fn_start = src.index("def probe_youtube_url(")
    fn_end = src.index("def ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    assert '"requested format is not available"' in fn_body, (
        "probe_youtube_url must check the format-unavailable "
        "pattern before classify_yt_dlp_error"
    )
    # The pattern check must precede the classify call (early
    # return saves the misclassification).
    pattern_idx = fn_body.index('"requested format is not available"')
    classify_idx = fn_body.index("classify_yt_dlp_error", pattern_idx)
    assert pattern_idx < classify_idx, (
        "Format-unavailable check must come BEFORE "
        "classify_yt_dlp_error so the early return takes effect"
    )
    # The early return must be `None` (alive), not a FailureKind.
    return_block = fn_body[pattern_idx:classify_idx]
    assert "return None" in return_block


def test_v1_15_5_marker_explains_probe_fix():
    """v1.15.5 marker on the probe fix references the user's repro."""
    src = DOWNLOADER_PY.read_text()
    fn_start = src.index("def probe_youtube_url(")
    fn_end = src.index("def ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    assert "v1.15.5" in fn_body
    # The pattern the user saw in the docker logs.
    assert "Requested format" in fn_body or "format" in fn_body.lower()


# ── B. +N QUEUED badge for downloads ───────────────────────────


def test_build_queue_detail_exposes_queue_depth_for_downloads():
    """_build_queue_detail must include `queue_depth` on the
    download_queue detail when running > 0 + pending > 0. The
    topbar badge reads this to show "+N QUEUED" without the
    label needing to inline it."""
    src = PROGRESS_PY.read_text()
    fn_start = src.index("def _build_queue_detail(")
    fn_end = src.index("\ndef ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    # The function signature must accept pending_n + running_n.
    assert "pending_n: int = 0" in fn_body
    assert "running_n: int = 0" in fn_body
    # The queue_depth assignment must fire only for the
    # running > 0 + pending > 0 case.
    assert 'detail["queue_depth"] = pending_n' in fn_body
    assert "running_n > 0 and pending_n > 0" in fn_body


def test_download_running_label_drops_inline_queued_suffix():
    """The download_queue's stage_label must drop the inline
    "(N queued)" suffix when running + pending — the +N badge
    surfaces it instead. Other queue kinds (place / scan /
    refresh / relink / adopt) keep the inline suffix since
    they don't get the badge surface."""
    src = PROGRESS_PY.read_text()
    # Anchor on the v1.15.5 marker in the stage composition.
    anchor = src.index(
        'v1.15.5: drop the inline "(N queued)" suffix from'
    )
    block = src[anchor:anchor + 1500]
    # The download branch suppresses the "(N queued)" suffix.
    assert 'jt == "download"' in block
    assert "stage = running_label" in block
    # The else branch (non-download queues) keeps the suffix.
    assert '({pending_n} queued)' in block, (
        "Non-download queue kinds must KEEP the inline suffix"
    )


def test_topbar_overflow_badge_renders_download_branch():
    """The topbar overflow pill must include a download_queue
    branch that fires when only downloads are queued (no plex
    or sync queue active). Same shape as the v1.14.84 plex
    branch + v1.14.90 sync branch — the user wanted parity."""
    src = OPS_JS.read_text()
    # Anchor on the v1.15.5 marker in the overflow logic.
    # Widened slice to 6000 chars to reach past the existing
    # plex/sync if-else chain to the new dlQueueDepth branch.
    anchor = src.index(
        "v1.15.5: also surface download_queue's pending count"
    )
    block = src[anchor:anchor + 6000]
    assert "download_queue" in block
    # The badge reads queue_depth from download_queue's detail.
    assert "queue_depth" in block
    # New `else if (dlQueueDepth > 0)` branch.
    assert "dlQueueDepth > 0" in block


def test_download_badge_uses_queue_tone():
    """The download +N badge must match the download_queue card's
    tone family so the visual identity is consistent across
    surfaces. v1.19.88 realigned that family from amber 'warn' to
    the dedicated cyan 'queue' tone (TDB→green / Plex→amber /
    queue→cyan), so the badge follows."""
    src = OPS_JS.read_text()
    # Anchor on the dlQueueDepth branch.
    anchor = src.index("dlQueueDepth > 0")
    block = src[anchor:anchor + 1500]
    assert "op-tone-queue" in block, (
        "v1.19.88: download +N badge must use the cyan queue tone "
        "(matches the download_queue card's realigned tone family)"
    )


def test_topbar_priority_order_preserved():
    """The badge composition picks one queue type at a time in
    priority order: plex → sync → download. Pin the order so a
    refactor doesn't accidentally surface the wrong queue
    when multiple are active."""
    src = OPS_JS.read_text()
    # Find the if/else-if chain in the overflow logic.
    chain_anchor = src.index("if (queueDepth > 0 && hasSyncPending)")
    chain_block = src[chain_anchor:chain_anchor + 3000]
    # Plex+sync mixed comes first.
    pmix_idx = chain_block.index("queueDepth > 0 && hasSyncPending")
    # Plex alone.
    plex_idx = chain_block.index("else if (queueDepth > 0)", pmix_idx)
    # Sync alone.
    sync_idx = chain_block.index("else if (hasSyncPending)", plex_idx)
    # Download alone.
    dl_idx = chain_block.index("else if (dlQueueDepth > 0)", sync_idx)
    assert pmix_idx < plex_idx < sync_idx < dl_idx, (
        "Priority order must be: plex+sync > plex > sync > download. "
        "Download is the lowest priority so it only fires when no "
        "other queue is active."
    )


# ── C. LET PLEX SERVE gated on motif_has_content ───────────────


def test_let_plex_serve_recovery_gated_on_motif_has_content():
    """The recovery card's LET PLEX SERVE option must require
    BOTH p_available AND motif having canonical/placement
    content to drop. Pre-fix the gate was just `if p_available`
    — fired on pure-P rows with no motif content, where the
    action would just ack the failure (no purge target)."""
    src = API_PY.read_text()
    # Anchor on the v1.15.5 marker.
    anchor = src.index(
        "v1.15.5: gate LET PLEX SERVE on motif having something"
    )
    block = src[anchor:anchor + 3000]
    assert "motif_has_content" in block, (
        "Gate must compute motif_has_content"
    )
    # The gate query checks both local_files AND placements.
    assert "FROM local_files" in block
    assert "FROM placements" in block
    # The new conditional applies the gate.
    assert "if p_available and motif_has_content:" in block


def test_let_plex_serve_query_section_scoped():
    """The motif_has_content check must scope to the row's
    specific section_id — multi-section installs must not
    confuse 4K presence with standard absence (or vice versa).

    Anchor on the v1.15.5 marker so we get the right slice
    (the second `motif_has_content = ` on line 10792 is just
    the result assignment, not the SQL)."""
    src = API_PY.read_text()
    anchor = src.index(
        "v1.15.5: gate LET PLEX SERVE on motif having something"
    )
    block = src[anchor:anchor + 4000]
    assert "section_id = ?" in block, (
        "motif_has_content SQL must scope to the row's section_id"
    )
    # state["section_id"] must be the parameter source — confirms
    # the section_id binding comes from the row's own section.
    assert 'state["section_id"]' in block
