"""v1.18.65 — inline TDB row pill respects pending_update over failure_kind.

the user's follow-up on v1.18.63 (same "Am I Actually the Strongest?" row,
post-pending-surface fix):

  >  I think this should be a Blue update TDB Pill instead of red, also
  >  even after doing a reprobe it remains red. since it's a row that is
  >  offering an upgrade the pill should be blue tdb not red or green
  >
  >  Also the images we see in the proposed and current plus the button
  >  thumbnail are a bit confusing

Row state:
  - themes.failure_kind = 'video_removed' (the OLD committed TDB URL
    kEp_ZMPWWdU is dead — that's TRUE)
  - pending_updates.new_youtube_url = 8budHRQkBLU (a fresh alive TDB
    URL, surfaced via v1.18.63's pending-display work)
  - user_overrides.youtube_url = je_uIV5zv5c (the user's working URL)
  - lf.source_video_id = kEp_ZMPWWdU (STALE — from the pre-override
    download cycle)

Two bugs:

## Bug 1: Inline row pill renders red TDB ✗

`computeTdbPill` (app.js line 7341) returned 'update' for this row.
But the visible row uses a SEPARATE inline render block at line 7901
that checked `failure_kind` BEFORE `pending_update`. Class-9
mirror-drift sub-pattern from v1.18.24 (plex_upload triple-fix).

v1.18.65 fix: re-order the inline block so pending_update is checked
FIRST, matching computeTdbPill's priority exactly. A row offering an
upgrade is in an actionable state — the FORWARD pill (blue ↑) wins
over the historical-failure pill (red ✗).

## Bug 2: Diff-tile CURRENT thumbnail shows stale lf.source_video_id

`renderPendingUpdateDiff` (app.js line 14226) derived the CURRENT tile
thumbnail via `currentVidFromLf || extractYouTubeVideoId(currentRawUrl)`.
When hasOverride, currentRawUrl=ovr.youtube_url (fresh), but
currentVidFromLf=lf.source_video_id (stale from pre-override download).
The OR short-circuits to the stale lf id, so the tile renders the
DEAD kEp_ZMPWWdU thumbnail labeled "CURRENT (your URL)" — the dead
TDB id under the user's override label.

v1.18.65 fix: when hasOverride, prefer extractYouTubeVideoId(url)
over lf. Mirrors v1.18.63's info-card ytId fix (same stale-lf-vs-
fresh-URL pattern).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── Bug 1: inline row pill priority ─────────────────────────


def _inline_tdb_block() -> str:
    """Return the inline TDB cell render block from renderLibraryRow
    (separate from the computeTdbPill function — same logic, two
    sites, hence the class-9 mirror-drift class)."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # The inline block is the per-row TDB-pill IIFE in renderLibraryRow.
    # v1.23.39: anchor moved off the local `TDB_DEAD_FAILURES` set (deduped
    # into the module-scope TDB_DEAD_FAILURES_GLOBAL) to the IIFE head.
    idx = js.index("const tdbAvailLabel = (() => {")
    # Walk forward to the closing IIFE — the next `})();` after the
    # default green "ThemerrDB tracked" return.
    end = js.index("})();", idx)
    return js[idx:end]


def test_inline_tdb_pill_checks_pending_update_before_failure_kind():
    """The inline TDB cell render in renderLibraryRow must check
    `it.pending_update` BEFORE `it.failure_kind` — matching
    computeTdbPill's priority. Pre-fix the order was reversed and
    a row with both pending + failure rendered red TDB ✗
    instead of blue TDB ↑."""
    block = _inline_tdb_block()
    # Locate the pending_update branch (TDB ↑) and the failure
    # branch (TDB ✗). The pending branch must appear FIRST.
    # v1.19.71: gate shape widened to OR a new_theme_available
    # SRC=— exception, so the literal one-liner check no longer
    # matches. Use the bare `it.pending_update` token instead.
    pending_idx = block.index("it.pending_update")
    failure_idx = block.index("TDB_DEAD_FAILURES_GLOBAL.has(it.failure_kind)")
    assert pending_idx < failure_idx, (
        "v1.18.65: pending_update branch must precede the "
        "TDB_DEAD_FAILURES branch in the inline row render. "
        "the user's repro: 'Am I Actually the Strongest?' row had "
        "BOTH a video_removed failure (on the dead committed URL) "
        "AND a pending update (with the fresh TDB URL). Inline "
        "render flipped red because it checked failure first; "
        "computeTdbPill (the OTHER site) checked pending first "
        "and returned 'update'. Mirror-drift class-9 — both sites "
        "must agree on priority."
    )


def test_inline_tdb_pill_priority_matches_compute_tdb_pill():
    """Both sites must check pending_update before failure_kind.
    Pin the canonical priority so a future "simplification" can't
    silently flip one without the other."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # computeTdbPill function block.
    fn_start = js.index("function computeTdbPill(it)")
    fn_end = js.index("}", js.index("return 'tdb';", fn_start))
    fn = js[fn_start:fn_end]
    # v1.19.71: both sites widened the gate from a one-liner
    # `it.pending_update && computeSrcLetter(it) !== '-'` to
    # a multi-line conditional that also accepts new_theme_
    # available on SRC=—. Pin the bare token instead.
    fn_pending = fn.index("it.pending_update")
    fn_failure = fn.index("TDB_DEAD_FAILURES_GLOBAL.has(it.failure_kind)")
    assert fn_pending < fn_failure, (
        "computeTdbPill must also have pending_update before failure"
    )
    # Inline block already checked above — confirm both agree.
    inline = _inline_tdb_block()
    inline_pending = inline.index("it.pending_update")
    inline_failure = inline.index("TDB_DEAD_FAILURES_GLOBAL.has(it.failure_kind)")
    assert inline_pending < inline_failure


def test_inline_tdb_pill_v1_18_65_marker_present():
    """The v1.18.65 archaeology marker must sit in the inline
    block so a future code-archaeologist sees WHY the order is
    what it is."""
    block = _inline_tdb_block()
    assert "v1.18.65" in block, (
        "v1.18.65: marker required in the inline TDB block — "
        "explains the mirror-drift class-9 priority constraint"
    )
    # Comment text wraps across lines + uses `// ` per-line
    # prefixes, so the title literal appears split. Strip out the
    # `//` markers and collapse whitespace before substring-check.
    import re
    block_flat = re.sub(r"\s*//\s*", " ", block)
    block_flat = " ".join(block_flat.split())
    assert "Am I Actually the Strongest" in block_flat, (
        "v1.18.65: repro reference required so future readers "
        "can find the bug class"
    )
    assert "mirror-drift" in block_flat or "class-9" in block_flat, (
        "v1.18.65: bug-class reference helps future code archaeology"
    )


def test_inline_tdb_pill_urls_match_kind_still_uses_blue():
    """When pending_update_kind === 'urls_match' (the U→T
    conversion offer), the row must still get the blue update
    pill — same as before, just re-ordered relative to failure.
    Pin the tooltip copy so the U→T variant doesn't get lost in
    the re-order."""
    block = _inline_tdb_block()
    assert "Your manual URL matches TDB" in block, (
        "v1.18.65: urls_match tooltip must survive the re-order"
    )


def test_inline_tdb_pill_cookies_branch_still_works():
    """The cookies_expired → TDB ⚿ branch must still fire when
    pending_update is false. Re-ordering pending above failure
    doesn't change the cookies path."""
    block = _inline_tdb_block()
    # Cookies-required tooltip must still be present.
    assert "TDB ⚿" in block
    assert "cookies will refresh on next download" in block


# ── Bug 2: diff-tile thumbnail prefers URL extraction ───────


def _diff_fn_body() -> str:
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    start = js.index("function renderPendingUpdateDiff(pu, lf, t, ovr)")
    # Walk to the next top-level function (next `function ` at the
    # same indent — the diff function spans ~100 lines).
    end = js.index("async function hydrateDiffTitles", start)
    return js[start:end]


def test_diff_tile_prefers_url_extraction_when_override_is_active():
    """When hasOverride, the CURRENT tile must extract the vid
    from the override URL FIRST, falling back to lf.source_video_id
    only if URL extraction fails. Pre-fix `currentVidFromLf ||
    extract(url)` short-circuited to the stale lf id and rendered
    the dead-kEp thumbnail under the (your URL) tile label."""
    body = _diff_fn_body()
    # The hasOverride branch on currentVid derivation must put
    # extractYouTubeVideoId FIRST when hasOverride is true.
    assert "hasOverride" in body
    # The conditional: extract first when hasOverride, lf first
    # otherwise (preserves legacy behaviour for non-override rows).
    assert (
        "extractYouTubeVideoId(currentRawUrl) || currentVidFromLf"
        in body
    ), (
        "v1.18.65: when hasOverride, currentVid must come from "
        "extractYouTubeVideoId(currentRawUrl) first — lf.source_"
        "video_id can be stale relative to a recently-set "
        "override (same v1.18.63 archaeology applies)."
    )
    # The pre-fix unconditional `currentVidFromLf || extract(...)`
    # must be gone for the YT-with-override path. The non-override
    # branch keeps the legacy lf-first ordering (no override means
    # lf is the authoritative source).
    assert "currentVidFromLf || extractYouTubeVideoId(currentRawUrl)" in body, (
        "v1.18.65: non-override branch still uses lf-first "
        "(preserves legacy semantic for committed-URL diffs)"
    )


def test_diff_tile_v1_18_65_marker_in_currentvid_block():
    """Marker required so a future refactor that flattens the
    conditional doesn't silently regress to the pre-fix
    short-circuit."""
    body = _diff_fn_body()
    # The marker must live inside the YT vid derivation block.
    yt_block_idx = body.index("if (currentSrc === 'youtube')")
    yt_block_end = body.index("} else if (currentSrc === 'soundcloud')",
                              yt_block_idx)
    yt_block = body[yt_block_idx:yt_block_end]
    assert "v1.18.65" in yt_block
    assert "kEp_ZMPWWdU" in yt_block or "Am I Actually the Strongest" in yt_block


def test_diff_tile_url_first_marker_references_v1_18_63():
    """The v1.18.65 comment must link to v1.18.63's info-card
    fix — they share the stale-lf-vs-fresh-URL pattern."""
    body = _diff_fn_body()
    yt_block_idx = body.index("if (currentSrc === 'youtube')")
    yt_block_end = body.index("} else if (currentSrc === 'soundcloud')",
                              yt_block_idx)
    yt_block = body[yt_block_idx:yt_block_end]
    assert "v1.18.63" in yt_block, (
        "v1.18.65: cross-reference v1.18.63 so future readers "
        "find the related info-card fix"
    )


# ── Version pin ─────────────────────────────────────────────


# v1.18.66: tag-local version pins are anti-pattern (per v1.18.65
# journal). The canonical pin lives in
# tests/test_v1_13_79_link_fixes.py::test_version_string_matches_current_release
# and gets bumped with each tag. Dropping this tag-local pin so the
# next tag's ship isn't gated on a stale assertion.
