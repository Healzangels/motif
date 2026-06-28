"""v1.14.22 — bulk ACK section_id threading + buildLibraryFilterParams attn_pills.

Wave 1.1 from the audit. Both findings target the same bug
class (silent scope-leak via missing-from-helper) but on
different surfaces — bundling because both are 1-3 line
shapes that close visible UX bugs.

## Item A — bulk ACK SELECTED threads section_id

(Audit ref: AUDIT_FRONTEND.md H2)

Pre-fix the bulk ACK SELECTED handler omitted `section_id`
when calling `/clear-failure`, so the endpoint took the
legacy title-global path — dismissing the failure on every
section that owns the title, not just the section the user
picked the row from.

the user's repro: select rows from 4K MOVIES tab, click
// ACK FAILURES → also dismisses the failure on the
standard MOVIES siblings.

Recovery-card paths at app.js:9171-9173 / 9221-9223 /
9241-9242 thread `section_id` correctly. Bulk was missed
in the v1.13.54 sweep that introduced section_failure_acks.

Same bug class (CLAUDE.md class K — cross-section bleed)
the v1.14.8 sfa contract was supposed to close. Wave 1.4
(v1.14.24) closes the api side of this same class for
manual-url / upload-theme.

## Item B — buildLibraryFilterParams threads attn_pills

(Audit ref: AUDIT_FRONTEND.md H3)

Pre-fix the helper that SELECT ALL FILTERED + EXPORT CSV
both call to build /api/library scope params missed the
v1.13.68 ATTN axis. Net effect: user lands on
`?attn_pills=fail` filter (e.g. 4 visible failures), clicks
// SELECT ALL FILTERED → selects all 1500 section rows
because the scope helper ignored the ATTN filter the user
was staring at.

Same bug class (CLAUDE.md class P — mirror-principle drift)
the v1.13.84-88 + v1.14.17 chain has been chipping at.
The function's own docstring warns about this exact failure
mode for the earlier ed/dl/pl/link omissions. attn_pills
was added in v1.13.68 and never threaded through.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── Item A: bulk ACK threads section_id ───────────────────────


def test_bulk_ack_threads_section_id_when_present():
    """The bulk ACK SELECTED handler must include section_id in
    the /clear-failure URL when the row carries one."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # The new section-aware URL builder in the bulk ACK loop.
    handler_anchor = js.index("getElementById('library-ack-selected-btn')?.addEventListener")
    body = js[handler_anchor:handler_anchor + 4000]
    assert "const ackUrl = it.section_id" in body
    assert "?section_id=${encodeURIComponent(it.section_id)}" in body


def test_bulk_ack_falls_back_to_title_global_when_no_section():
    """Defensive: when a row has no section_id (legacy /
    not_in_plex paths), fall back to the bare URL — same
    shape as recovery-card paths."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    handler_anchor = js.index("getElementById('library-ack-selected-btn')?.addEventListener")
    body = js[handler_anchor:handler_anchor + 4000]
    # The fallback branch.
    assert ": `/api/items/${it.theme_media_type}/${it.theme_tmdb}/clear-failure`" in body


def test_bulk_ack_pre_fix_section_omission_gone():
    """Regression guard: the pre-fix unconditional
    `/api/items/{mt}/{id}/clear-failure` (no section_id, no
    branch) must not survive in the bulk ACK handler.

    Strip line comments so the rationale comment quoting the
    deleted shape doesn't trip the guard."""
    js_raw = (REPO / "app" / "web" / "static" / "app.js").read_text()
    js = "\n".join(
        line for line in js_raw.splitlines()
        if not line.lstrip().startswith("//")
    )
    # Locate the bulk ACK handler.
    anchor = js.index("getElementById('library-ack-selected-btn')?.addEventListener")
    end = js.index("setTimeout(refreshTopbarStatus, 1100)", anchor)
    body = js[anchor:end]
    # The pre-fix line was a single api() call with no section
    # branching — should now use the new ackUrl variable.
    pre_fix = "await api('POST', `/api/items/${it.theme_media_type}/${it.theme_tmdb}/clear-failure`)"
    assert pre_fix not in body, (
        "v1.14.22: pre-fix bulk ACK without section_id must not "
        "survive — recurses the v1.13.54 cross-section bleed"
    )
    # Confirm the new shape is present.
    assert "await api('POST', ackUrl)" in body


def test_recovery_card_paths_unchanged():
    """Regression guard: the recovery-card paths at
    app.js:9171-9173 + 9221-9223 + 9241-9242 already thread
    section_id correctly (since v1.13.54). v1.14.22 doesn't
    touch them; pin the shape so a refactor doesn't drift."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # The recovery-card pattern: ternary on sectionId.
    recovery_pattern = "?section_id=${encodeURIComponent(sectionId)}"
    # Should appear at least 3 times (once per recovery action: ack,
    # purge, replace_with_themerrdb — actual count is higher).
    assert js.count(recovery_pattern) >= 3, (
        "recovery-card section_id threading should still appear "
        "3+ times — v1.14.22 only touches the bulk ACK path"
    )


# ── Item B: buildLibraryFilterParams threads attn_pills ───────


def test_build_library_filter_params_includes_attn_pills():
    """The helper must include an attn_pills block matching
    the same shape the other 6 axes use."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    fn_anchor = js.index("function buildLibraryFilterParams(perPage = 200)")
    # Cap at the closing `return params; }` of this function.
    body_end = js.index("return params;", fn_anchor) + len("return params;\n    }")
    body = js[fn_anchor:body_end]
    assert "if (libraryState.attnPills && libraryState.attnPills.size > 0)" in body
    assert "params.set('attn_pills', Array.from(libraryState.attnPills).join(','))" in body


def test_build_library_filter_params_attn_block_before_return():
    """Pin position: attn_pills block sits inside the function
    body (before `return params`). A stray placement outside
    would silently no-op."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    fn_anchor = js.index("function buildLibraryFilterParams(perPage = 200)")
    # Cap at the closing `return params; }` of this function.
    body_end = js.index("return params;", fn_anchor) + len("return params;\n    }")
    body = js[fn_anchor:body_end]
    attn_pos = body.index("if (libraryState.attnPills")
    return_pos = body.index("return params;")
    assert attn_pos < return_pos


def test_all_seven_filter_axes_present_in_helper():
    """End-to-end mirror check: all 7 filter axes (status, tdb,
    src, dl, pl, link, ed, attn — actually 8 with status/tdb
    base modes) must appear in buildLibraryFilterParams. This
    is the v1.13.68 + v1.14.22 mirror-principle close: the
    helper must agree with loadLibrary + _buildPresetQueryString
    on the axis set."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    fn_anchor = js.index("function buildLibraryFilterParams(perPage = 200)")
    # Cap at the closing `return params; }` of this function.
    body_end = js.index("return params;", fn_anchor) + len("return params;\n    }")
    body = js[fn_anchor:body_end]
    expected_axes = [
        "src_pills",
        "tdb_pills",
        "dl_pills",
        "pl_pills",
        "link_pills",
        "ed_pills",
        "attn_pills",  # ← v1.14.22 addition
    ]
    for axis in expected_axes:
        assert f"params.set('{axis}'" in body, (
            f"v1.14.22 mirror check: buildLibraryFilterParams "
            f"missing {axis} block"
        )


# ── Mirror with loadLibrary (the OTHER param emitter) ─────────


def test_load_library_already_emitted_attn_pills_pre_fix():
    """Regression guard: the loadLibrary param-build (where
    /api/library is called for rendering rows) already emits
    attn_pills. v1.14.22 only fixes the SCOPE helper; the
    LOAD path was already correct. Pin so a refactor doesn't
    accidentally drop it from loadLibrary."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # Search for the loadLibrary param-build site — it sits
    # earlier in the file at ~line 4418 (per audit).
    assert "if (libraryState.attnPills && libraryState.attnPills.size > 0)" in js
    # The string appears at LEAST twice now (loadLibrary + the new
    # buildLibraryFilterParams addition).
    assert js.count("if (libraryState.attnPills && libraryState.attnPills.size > 0)") >= 2
