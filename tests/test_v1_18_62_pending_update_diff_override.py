"""v1.18.62 — PROPOSED CHANGE diff shows the user override as CURRENT.

the user's question on "Am I Actually the Strongest?" (orphan-pending-
update row): "Trying to figure out why this item is being offered
as an upgrade from themerrdb when it's a red pill."

The row had:
  - themerrdb url: kEp_ZMPWWdU (DEAD — yt-dlp returned video_removed)
  - applied url:   je_uIV5zv5c (user override — working)
  - pending_update detected (TDB pushed a new URL replacing kEp)
  - failure_kind=video_removed (acked)
  - SRC=U, TDB=✗ (red), TDB↑ (pending update)

The // PROPOSED CHANGE block showed:
  CURRENT:  kEp_ZMPWWdU  (the dead TDB URL)
  PROPOSED: <new TDB URL>

But ACCEPT UPDATE actually replaces the **user override**
(je_uIV5zv5c) with the new TDB URL. Showing the dead TDB URL as
"CURRENT" was misleading — the user read it as "you want to swap
the dead URL for a new one" without realizing his working
override would also be dropped.

## Root cause

`renderPendingUpdateDiff` derived currentRawUrl from
`pu.old_youtube_url` (the TDB URL at the moment the
pending_update was detected). It didn't consult user_overrides,
even though user_overrides is the authoritative "what's actually
playing" signal for U rows.

## Fix

Pass the `ovr` (user_overrides row) into renderPendingUpdateDiff.
When `ovr.youtube_url` is set, use IT as the CURRENT tile's URL
instead of `pu.old_youtube_url`. Also adapt the labels:

  - CURRENT label: 'CURRENT' → 'CURRENT (your URL)'
  - Header hint: 'ACCEPT UPDATE will replace the left source
                  with the right' → 'ACCEPT UPDATE drops your
                  override and switches to the new TDB URL'

For T rows (no override), behavior is unchanged.
"""

from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"


# ── Signature change: ovr passed in ─────────────────────────


def test_render_pending_update_diff_takes_ovr_arg():
    """The function signature must include `ovr` as the 4th
    parameter so the caller can hand the user_overrides row in."""
    src = APP_JS.read_text()
    assert "function renderPendingUpdateDiff(pu, lf, t, ovr)" in src, (
        "v1.18.62: renderPendingUpdateDiff must accept `ovr` "
        "as its 4th parameter"
    )


def test_caller_passes_ovr():
    """The info-card call site must pass the `ovr` local (which
    is `data.override` from the API response) into the diff
    renderer."""
    src = APP_JS.read_text()
    assert "renderPendingUpdateDiff(pu, lf, t, ovr)" in src, (
        "v1.18.62: the info-card call site must pass ovr"
    )


# ── CURRENT tile uses override URL when present ─────────────


def test_override_url_overrides_pu_old_url():
    """When `ovr.youtube_url` is present, the CURRENT tile's URL
    must come from the override (the actual playing URL), not
    from `pu.old_youtube_url` (the captured-at-detection TDB
    URL)."""
    src = APP_JS.read_text()
    # Pin the conditional logic.
    assert "hasOverride = !!(ovr && ovr.youtube_url)" in src, (
        "v1.18.62: must compute hasOverride from ovr.youtube_url"
    )
    # The currentRawUrl assignment must branch on hasOverride.
    fn_idx = src.index("function renderPendingUpdateDiff(")
    body = src[fn_idx:fn_idx + 5000]
    assert "hasOverride\n      ? ovr.youtube_url" in body \
        or "hasOverride ? ovr.youtube_url" in body \
        or "? ovr.youtube_url" in body, (
        "v1.18.62: currentRawUrl must use ovr.youtube_url when "
        "override is present"
    )
    # The pre-fix unconditional `pu.old_youtube_url` must NOT
    # survive as the sole source — it can still be the fallback,
    # just not unconditional.
    assert "const currentRawUrl = pu.old_youtube_url" not in body


def test_current_tile_label_indicates_override():
    """When override is in play, the CURRENT tile must label
    itself 'CURRENT (your URL)' so the user knows the comparison
    references their override, not TDB's stored URL."""
    src = APP_JS.read_text()
    assert "'CURRENT (your URL)'" in src, (
        "v1.18.62: tile label must indicate it's the user's URL"
    )


def test_header_hint_explains_override_drop():
    """The PROPOSED CHANGE header hint must explicitly explain
    that ACCEPT UPDATE drops the user override. Pre-fix the hint
    just said 'replace the left source with the right' which
    didn't surface the override-loss consequence."""
    src = APP_JS.read_text()
    assert ("ACCEPT UPDATE drops your override "
            "and switches to the new TDB URL") in src, (
        "v1.18.62: header hint must explain the override drop"
    )


# ── T-row regression guard (no override) ────────────────────


def test_no_override_keeps_existing_behavior():
    """When `ovr` is null/missing (T rows), the CURRENT tile
    still derives from `pu.old_youtube_url` as before. Pin the
    fallback so a future refactor doesn't break T-row diffs."""
    src = APP_JS.read_text()
    fn_idx = src.index("function renderPendingUpdateDiff(")
    body = src[fn_idx:fn_idx + 5000]
    # The fallback ternary must still reach pu.old_youtube_url
    # when hasOverride is false.
    assert ": (pu.old_youtube_url || '')" in body, (
        "v1.18.62: must fall back to pu.old_youtube_url for T rows"
    )


def test_t_row_label_stays_plain():
    """T rows (no override) get the plain 'CURRENT' label — not
    'CURRENT (your URL)'. Pin the conditional so a future
    refactor doesn't accidentally relabel every row.

    v1.18.65: slice widened from 5000 → 15000 chars — the v1.18.65
    diff-tile thumbnail fix added an extra archaeology comment
    block that pushed this assertion past the old window."""
    src = APP_JS.read_text()
    fn_idx = src.index("function renderPendingUpdateDiff(")
    body = src[fn_idx:fn_idx + 15000]
    # The label is computed via a ternary on hasOverride.
    assert "hasOverride ? 'CURRENT (your URL)' : 'CURRENT'" in body, (
        "v1.18.62: CURRENT label must be conditional on hasOverride"
    )


# ── Header hint for T rows stays original ───────────────────


def test_t_row_header_hint_stays_original():
    """T rows still see the original 'ACCEPT UPDATE will replace
    the left source with the right' hint — only U rows get the
    override-drop variant."""
    src = APP_JS.read_text()
    assert ("ACCEPT UPDATE will replace the left "
            "source with the right.") in src, (
        "v1.18.62: T-row header hint preserved"
    )


# ── Version marker ─────────────────────────────────────────


def test_v1_18_62_marker_present():
    src = APP_JS.read_text()
    fn_idx = src.index("function renderPendingUpdateDiff(")
    body = src[fn_idx:fn_idx + 5000]
    assert "v1.18.62" in body, (
        "v1.18.62: marker must live in the renderer fn"
    )
