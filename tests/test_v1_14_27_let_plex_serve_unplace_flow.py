"""v1.14.27 — LET PLEX SERVE rewires from /forget (full purge) to /unplace
(delete placement only, keep canonical) + 5 related fixes.

Bundle scope:
1. /unplace endpoint gains inline-verify (Plex HEAD probe)
2. LET PLEX SERVE click handlers rewire to /unplace
3. Confirm dialogs explain delete-vs-keep + recovery path
4. M-row state mismatch in /forget: only flip flags for rks where motif
   actually unlinked something
5. ADOPT + LET PLEX SERVE recovery option for M+P composite rows
6. Bulk LET PLEX SERVE button on +P SRC pill filter

Why bundle: all six are part of the same "new LET PLEX SERVE flow"
feature. Each is small but they interact (the bulk action calls the
new /unplace inline-verify; the M-row fix supports the new flow's
correctness on edge cases).

Per the user: "rather than purge we could just do a delete of the plex
theme.mp3 and leave our download copy in place. that way if we wanted
to revert from plex we can do a push to plex to revert to whatever the
source was before. I like this option."

Recovery path post-v1.14.27: motif still has the canonical in /themes/.
PUSH TO PLEX from the SOURCE menu re-places it from canonical, no
re-download from TDB needed — survives even when the TDB URL has gone
dead since the operation.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── #1: /unplace inline-verify ────────────────────────────────


def test_unplace_does_inline_plex_verify():
    """The /unplace endpoint must run a Plex HEAD probe after
    deleting the placement file so the row's chip flips to P
    (Plex still serves its own theme) instead of pessimistically
    setting has_theme=0 (which would drop chip to '-')."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    # v1.18.46: anchor-based slicing. Pre-fix the fixed-window
    # widened from 12000 → 24000 (v1.18.36) → 28000 (v1.18.38)
    # across the LPS bug cycle. Now slices to the next route
    # decorator so it grows naturally with future body changes.
    from _slice_helpers import slice_to_next, PY_NEXT_ROUTE
    body = slice_to_next(
        src, "async def api_unplace_item(", *PY_NEXT_ROUTE,
    )
    # The new inline-verify block.
    assert "v1.14.27: inline-verify Plex's theme" in body
    # v1.21.39: tristate verify_theme_claim (was item_has_theme).
    # v1.22.58: offloaded via run_in_threadpool (event-loop lint) — the
    # method rides as an argument, not an inline call.
    assert "plex.verify_theme_claim, rk)" in body
    # The pre_has_theme dict gets populated for affected rks.
    assert "pre_has_theme: dict[str, int] = {}" in body


def test_unplace_no_longer_pessimistically_zeros_has_theme():
    """Pre-fix /unplace did blanket UPDATE plex_items SET
    local_theme_file=0, has_theme=0. That flipped chip to '-'
    even on +P composite rows where Plex still served its own.

    Strip line comments so the rationale comment quoting the
    deleted shape doesn't trip the guard."""
    src_raw = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src_raw.index("async def api_unplace_item(")
    end = src_raw.index("@app.post(", fn_anchor + 100)
    body_raw = src_raw[fn_anchor:end]
    body = "\n".join(
        line for line in body_raw.splitlines()
        if not line.lstrip().startswith("#")
    )
    pre_fix = '"UPDATE plex_items SET local_theme_file = 0, has_theme = 0 "'
    assert pre_fix not in body, (
        "v1.14.27: pre-fix blanket has_theme=0 UPDATE must not "
        "survive — would drop chip to '-' on +P composite rows"
    )


# ── #2: LET PLEX SERVE click handlers rewire to /unplace ─────


def test_purge_and_ack_handler_calls_unplace():
    """The purge-and-ack click handler (LET PLEX SERVE on failed
    rows) must call /unplace, not /forget. v1.14.28 grew the
    handler with a probe-on-confirm step; widen the window to
    the next `} else if` boundary so the clear-failure assertion
    still finds the line."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    handler_anchor = js.index("} else if (act === 'purge-and-ack') {")
    end = js.index("} else if", handler_anchor + 10)
    body = js[handler_anchor:end]
    # New v1.14.27 marker.
    assert "v1.14.27: switched from /forget" in body
    # Calls the unplace endpoint.
    assert "/api/items/${mt}/${id}/unplace" in body
    # Still chains the clear-failure call (it's a failed-row variant).
    assert "/api/items/${mt}/${id}/clear-failure" in body


def test_purge_revert_to_plex_handler_calls_unplace():
    """The purge-revert-to-plex click handler (LET PLEX SERVE on
    non-failed +P composite rows) must reach /unplace.

    v1.14.47 reorg: the inline dispatcher branch was extracted to
    the top-level `letPlexServeFlow` helper (so the SOURCE-menu
    dispatcher can call it). The CONTRACT (handler ultimately
    POSTs /unplace) is preserved; assert the dispatcher delegates
    to the helper AND the helper hits /unplace."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    handler_anchor = js.index("} else if (act === 'purge-revert-to-plex') {")
    body = js[handler_anchor:handler_anchor + 2500]
    # Dispatcher delegates to letPlexServeFlow.
    assert "letPlexServeFlow(" in body
    # The helper itself hits /unplace.
    helper_anchor = js.index("async function letPlexServeFlow(")
    helper_body = js[helper_anchor:helper_anchor + 3000]
    assert "/api/items/${mt}/${id}/unplace" in helper_body


def test_pre_fix_forget_calls_gone_from_let_plex_serve_handlers():
    """Regression guard: neither LET PLEX SERVE click handler can
    still reference /forget. Comment-stripped check."""
    js_raw = (REPO / "app" / "web" / "static" / "app.js").read_text()
    js = "\n".join(
        line for line in js_raw.splitlines()
        if not line.lstrip().startswith("//")
    )
    # Find both handlers and verify NEITHER calls /forget.
    for marker in ("act === 'purge-and-ack'", "act === 'purge-revert-to-plex'"):
        anchor = js.index(marker)
        # End at the next else-if boundary or closing.
        end = js.index("} else if", anchor + 10)
        block = js[anchor:end]
        assert "/api/items/${mt}/${id}/forget" not in block, (
            f"v1.14.27: handler at {marker!r} still calls /forget; "
            f"should call /unplace"
        )


# ── #3: Confirm dialogs ───────────────────────────────────────


def test_let_plex_serve_handlers_show_confirm_dialog():
    """Both LET PLEX SERVE flows must show a confirm dialog with
    explicit copy describing what gets deleted vs what survives
    + how to revert.

    History:
      v1.14.27 inlined `confirm()` in each dispatcher branch.
      v1.14.28 extracted probe-then-confirm into
        `_probeAndConfirmLetPlexServe`.
      v1.14.47 moved the no-failure branch to the SOURCE menu
        and extracted the whole flow into `letPlexServeFlow` /
        `adoptAndLetPlexServeFlow` top-level helpers (with their
        own probe-then-confirm via `_probeAndConfirmLPSAtTopLevel`).

    The CONTRACT survives: a confirm dialog with the right copy
    fires before /unplace. Pin both surviving sites:
      • inline `purge-and-ack` dispatcher (failure flow)
      • `letPlexServeFlow` helper (no-failure SOURCE-menu flow)

    NB: the JS source has escaped apostrophes (`motif\\'s`)
    inside single-quoted strings — the test searches the
    literal source bytes, including the backslash."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    sites: list[tuple[str, int]] = []
    # Site 1: failure-flow dispatcher branch (still inline).
    pa_anchor = js.index("act === 'purge-and-ack'")
    sites.append(("purge-and-ack", pa_anchor))
    # Site 2: no-failure SOURCE-menu helper.
    helper_anchor = js.index("async function letPlexServeFlow(")
    sites.append(("letPlexServeFlow", helper_anchor))
    for label, anchor in sites:
        # Cap each block generously; both are <3000 chars.
        block = js[anchor:anchor + 4000]
        assert (
            "confirm(" in block
            or "_probeAndConfirmLetPlexServe(" in block
            or "_probeAndConfirmLPSAtTopLevel(" in block
        ), (
            f"{label}: must show a confirm dialog (inline confirm() OR "
            "via _probeAndConfirmLetPlexServe / _probeAndConfirmLPSAtTopLevel)"
        )
        # Apostrophe escaped in JS source: motif\'s
        assert "DELETE motif\\'s theme.mp3 from the Plex folder" in block, label
        assert "KEEP motif\\'s canonical" in block, label
        # v1.14.47 SOURCE-menu copy says "PLACE menu" (the new
        # home of PUSH TO PLEX); failure-flow copy still says
        # "SOURCE menu". Either form satisfies the contract
        # (recovery hint mentions PUSH TO PLEX).
        assert "PUSH TO PLEX" in block, label


# ── #4: M-row state mismatch fix in /forget ──────────────────


def test_forget_only_flag_flips_rks_with_actual_unlink():
    """The /forget endpoint must only flip local_theme_file=0 +
    has_theme=0 for rks where motif actually unlinked a file
    (rk_from_placement set). Pre-fix all rks in rk_clear got
    flipped — including M-row rks reached via theme_id /
    guid_tmdb where motif had no placement to unlink. the user's
    repro: PURGE on M row flipped the chip to P even though
    the file at the Plex folder was untouched."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    # The v1.14.27 fix marker.
    marker = "v1.14.27: only flag-flip rks where motif actually"
    assert marker in src
    block_start = src.index(marker)
    block = src[block_start:block_start + 2500]
    # The intersection with rk_from_placement.
    assert "_rks_we_actually_touched = rk_clear & rk_from_placement" in block
    # Both rk_keep_p and rk_zero now derive from the touched subset.
    assert "for rk in _rks_we_actually_touched" in block


# ── #5: ADOPT + LET PLEX SERVE recovery option ───────────────


def test_adopt_and_let_plex_serve_recovery_option_offered_on_m_plus_p():
    """The ADOPT + LET PLEX SERVE option must be surfaced for M+P
    composite rows (m_available AND p_available). Pure-M without
    P must NOT see this option (would delete the only theme).

    v1.14.47 reorg: the option moved from the api_recovery_options
    no-fail branch to the client-side SOURCE-menu render in app.js
    (per the user's UX principle: TRY THIS NEXT is for error states
    only). The gate predicate moved with it. The CONTRACT (option
    exists for M+P, hidden for pure-M) survives via the SOURCE-
    menu render's inline gate."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # Anchor on the SOURCE-menu menuItemHtml that emits the option.
    anchor = js.index("'adopt-and-let-plex-serve', 'ADOPT + LET PLEX SERVE'")
    # Walk back to the surrounding gate (~600 chars covers the
    # condition and helper text).
    block_start = js.rfind("if (", anchor - 800, anchor)
    block = js[block_start:anchor + 600]
    # Gate references the M+P composite predicate. The exact JS
    # form: `it.plex_local_theme === 1 && !placed && it.plex_
    # independent_theme === 1 && it.rating_key`.
    assert "plex_local_theme === 1" in block
    assert "plex_independent_theme === 1" in block
    # rating_key required for /adopt-sidecar.
    assert "rating_key" in block


def test_adopt_and_let_plex_serve_handler_chains_two_api_calls():
    """The 'adopt-and-let-plex-serve' flow must call /adopt-sidecar
    then /unplace in sequence.

    v1.14.47 reorg: the dispatcher branch was extracted to the
    top-level `adoptAndLetPlexServeFlow` helper (so the SOURCE-
    menu dispatcher can call it from outside hydrateRecoveryOptions
    scope). The CONTRACT (chained /adopt-sidecar → /unplace +
    confirm dialog) survives in the helper."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    helper_anchor = js.index("async function adoptAndLetPlexServeFlow(")
    body = js[helper_anchor:helper_anchor + 4000]
    # Both API calls present.
    assert "/api/plex_items/${encodeURIComponent(ratingKey)}/adopt-sidecar" in body
    assert "/api/items/${mt}/${id}/unplace" in body
    # Confirm dialog explains the two-step.
    assert "confirm(" in body
    assert "ADOPT" in body
    assert "DELETE" in body


# ── #6: Bulk LET PLEX SERVE on +P ─────────────────────────────


def test_bulk_let_plex_serve_button_in_template():
    """Library template must include the bulk LET PLEX SERVE
    button between RESTORE FROM PLEX and ACK SELECTED in the
    Plex-touching button group."""
    html = (REPO / "app" / "web" / "templates" / "library.html").read_text()
    assert 'id="library-let-plex-serve-btn"' in html
    # btn-plex amber (matches the P chip palette).
    assert 'class="btn btn-tiny btn-plex" id="library-let-plex-serve-btn"' in html


def test_bulk_let_plex_serve_visible_on_lps_only_selections():
    """v1.14.27 gated visibility on the +P SRC pill filter
    (srcFilter.has('Pp')). v1.15.49 narrowed the gate to
    lpsOnlyCount > 0 — selection-shape based instead of
    filter-axis based — so the unsafe LPS-on-M-sidecar surface
    is eliminated (M+P composites route to the safer // ADOPT +
    LET PLEX SERVE button). Test intent preserved: the button is
    NOT always-visible; it gates on a meaningful predicate.

    v1.15.60 added a +P-filter-active safety-net that re-displayed
    the button when lpsOnlyCount=0 (but with NO count label) —
    the user's later complaint "let plex server isn't displaying a
    number at all anytime." v1.16.10 dropped that safety net: the
    selectedRows cache makes lpsOnlyCount selection-wide, so the
    page-size undercount it guarded against no longer occurs. The
    visibility gate is back to pure lpsOnlyCount > 0 with a
    count badge whenever shown."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # v1.16.10: marker header trimmed back to v1.14.27 / v1.15.49.
    marker = "v1.14.27 / v1.15.49: bulk LET PLEX SERVE"
    assert marker in js
    block_start = js.index(marker)
    block = js[block_start:block_start + 1500]
    assert "lpsOnlyCount > 0" in block, (
        "v1.15.49: LPS visibility must gate on lpsOnlyCount > 0 "
        "(replaces v1.14.27's srcFilter.has('Pp') gate)"
    )
    # v1.16.10: the bare-label fallback for the safety-net path
    # is gone — every visible LPS button now carries a count.
    assert "withCount('// LET PLEX SERVE', lpsOnlyCount)" in block


def test_bulk_let_plex_serve_handler_uses_server_side_composite():
    """v1.14.27 had the bulk handler iterate the per-row /unplace
    endpoint client-side (mirroring REVERT MISMATCH / RESTORE
    FROM PLEX bulk pattern). v1.15.28 moved the probe-then-
    unplace orchestration server-side into _bulk_lps_run via
    POST /api/admin/bulk-let-plex-serve. The +P composite
    predicate stays the same — it filters which targets get
    SENT to the new endpoint."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    handler_anchor = js.index("getElementById('library-let-plex-serve-btn')?.addEventListener")
    end = js.index("// v1.12.94: shared param-builder", handler_anchor)
    body = js[handler_anchor:end]
    # New flow: single POST to the composite endpoint.
    assert "/api/admin/bulk-let-plex-serve" in body, (
        "v1.15.28: bulk LPS handler must call the new server-side "
        "composite endpoint"
    )
    # The +P composite predicate (filters which rows go to the
    # endpoint) is unchanged.
    assert "it.plex_independent_theme === 1" in body
    assert "computeSrcLetter(it) !== 'P'" in body
    assert "computeSrcLetter(it) !== '-'" in body


def test_bulk_let_plex_serve_supports_no_selection_visible_page_fallback():
    """Per the v1.14.15 no-selection bulk pattern: when nothing
    is selected, fall back to the visible page (every +P row on
    the current page). v1.16.10 reshaped the wiring onto the
    selectedRows-cache pattern — selection mode walks the cache,
    no-selection mode walks libraryState.items."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    handler_anchor = js.index("getElementById('library-let-plex-serve-btn')?.addEventListener")
    end = js.index("// v1.12.94: shared param-builder", handler_anchor)
    body = js[handler_anchor:end]
    assert "const useSelection = libraryState.selected.size > 0;" in body
    # v1.16.10: dual-mode source selector.
    assert "const source = useSelection" in body
    assert "libraryState.selectedRows.values()" in body
    assert "(libraryState.items || [])" in body


def test_bulk_let_plex_serve_confirm_dialog_explains_scope():
    """The confirm dialog must explicitly describe what gets
    deleted vs kept + how to recover, mirroring the per-row
    confirm dialog text. v1.15.28 collapsed the v1.14.29 two-
    stage prompt into a single confirm — the scope description
    is now bundled with the probe-then-unplace sequence
    explanation."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    handler_anchor = js.index("getElementById('library-let-plex-serve-btn')?.addEventListener")
    end = js.index("// v1.12.94: shared param-builder", handler_anchor)
    body = js[handler_anchor:end]
    assert "confirm(" in body
    # Apostrophe escaped in JS source: motif\'s
    assert "DELETE motif\\'s theme.mp3" in body
    assert "motif\\'s canonical" in body
    # v1.15.28 wraps the recovery clause across a string-concat
    # boundary; check both halves rather than the joined form.
    assert "PUSH TO" in body and "PLEX recovers" in body
