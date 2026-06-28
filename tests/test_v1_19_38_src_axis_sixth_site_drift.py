"""v1.19.38 — SRC-axis sixth-site drift: bulk PUSH predicates.

Audit (post-v1.19.36 cross-reference) found three sites in the
bulk-PUSH flow that predicate on `!it.media_folder` alone — never
the v1.18.0 widening `!!it.media_folder ||
placement_kind === 'plex_upload'`. plex_upload rows store
media_folder=`''`, so `!''` is TRUE → these predicates classified
plex_upload rows as "not yet placed" and:

  - Wrongly counted them toward `pushableCount` (bulk PUSH
    visibility gate)
  - Wrongly counted them toward `pushCount` (the (N) badge on
    `// PUSH TO PLEX` button)
  - Wrongly scooped them into the bulk-PUSH click handler's
    candidates list (server-side re-push would be a no-op
    but the intent surface was wrong)

Same class-9 mirror-drift sub-pattern v1.18.24 / v1.18.75 fixed
at the inline-SRC + isPlexAgentRow sites. CLAUDE.md SRC-axis
table now has six load-bearing sites instead of five.

## Fix

Each predicate updated to `!it.media_folder && it.placement_kind
!== 'plex_upload'` (or equivalently uses the existing local
`placed` derivation, which already carries the canonical
widening). Three sites in `app.js`:

  - `updateBulkBar` per-row pushableCount predicate (line ~9766)
    — reuses the in-scope `placed` variable.
  - `pushCount` effectiveCount predicate (line ~9818) — inline
    widening since no scope-local `placed`.
  - Bulk PUSH click handler `awaitingApproval` (line ~11456) —
    inline widening, same shape.

## Tests

Pin the v1.18.0 widening present at every site that gates on
"motif owns no placement on this row." Catches the class-9
forgetting cost (a future site that introduces the bare
`!it.media_folder` shape fails the lint).
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


# ── Helpers ─────────────────────────────────────────────────


def _slice_function(name: str) -> str:
    """Return the body of the JS function starting with `function NAME(`."""
    start = APP_JS.index(f"function {name}(")
    # Walk forward to the next sibling function or end-of-IIFE.
    next_fn = APP_JS.find("\n  function ", start + 1)
    return APP_JS[start:next_fn if next_fn > 0 else start + 80000]


def _slice_handler(anchor: str, length: int = 3000) -> str:
    """Return a chunk around an anchor string (for in-function code)."""
    idx = APP_JS.index(anchor)
    return APP_JS[idx:idx + length]


# ── Three patched sites: each uses the widening ─────────────


def test_pushable_count_per_row_uses_placed_widening():
    """The per-row pushableCount predicate inside updateBulkBar's
    for-loop must reuse the local `placed` variable (which carries
    the v1.18.0 widening), NOT the bare `!it.media_folder` shape."""
    # The block from `pushableCount = 0` to the per-row
    # awaitingApproval is ~92 lines (~5500 chars) post-v1.19.38
    # comment block. Widen the slice to 7000 chars to capture it.
    body = _slice_handler("pushableCount = 0", length=7000)
    aa_idx = body.index("const awaitingApproval = !it.job_in_flight")
    chunk = body[aa_idx:aa_idx + 400]
    # The new shape uses !placed (reuses the canonical derivation
    # declared earlier in the same for-loop body).
    assert "!placed" in chunk, (
        "v1.19.38: per-row pushableCount awaitingApproval must "
        "reuse the local `placed` derivation so the v1.18.0 "
        "plex_upload widening is honored"
    )


def test_push_count_effectivecount_predicate_widens_placed_check():
    """The pushCount effectiveCount predicate must include
    `placement_kind !== 'plex_upload'` so plex_upload rows aren't
    counted as awaiting placement."""
    # Find the pushCount declaration.
    idx = APP_JS.index("const pushCount = effectiveCount")
    chunk = APP_JS[idx:idx + 600]
    assert "placement_kind !== 'plex_upload'" in chunk, (
        "v1.19.38: pushCount predicate must exclude plex_upload "
        "rows — they have media_folder='' so `!it.media_folder` "
        "is TRUE; the v1.18.0 widening is required"
    )


def test_bulk_push_click_handler_awaitingapproval_widens_placed_check():
    """The bulk PUSH click handler at #library-push-selected-btn
    iterates candidates with `awaitingApproval`. Must use the
    widening so plex_upload rows aren't scooped into the
    candidates list."""
    # The handler addEventListener registration sits ~1500 lines
    # past the button's `getElementById` declaration. Anchor on
    # the addEventListener line itself, then slice forward.
    handler_idx = APP_JS.index(
        "getElementById('library-push-selected-btn')?.addEventListener"
    )
    chunk = APP_JS[handler_idx:handler_idx + 4000]
    aa_idx = chunk.index("const awaitingApproval = !it.job_in_flight")
    aa_chunk = chunk[aa_idx:aa_idx + 400]
    assert "placement_kind !== 'plex_upload'" in aa_chunk, (
        "v1.19.38: bulk PUSH click handler's awaitingApproval "
        "must exclude plex_upload rows via the v1.18.0 widening"
    )


# ── Inverse lint: every awaitingApproval in app.js must widen ─


def test_no_bare_media_folder_only_awaiting_approval_predicate():
    """Lint: no `awaitingApproval = ... && !it.media_folder` block
    in app.js may exist WITHOUT either `!placed` or
    `placement_kind !== 'plex_upload'` in the same predicate.
    Catches the class-9 forgetting cost for any future
    awaitingApproval site."""
    # Find every `const awaitingApproval = ... !it.media_folder` block.
    # The shape varies (multi-line), so we slice 350 chars per match.
    awaiting_starts = [
        m.start() for m in re.finditer(
            r"const awaitingApproval = ", APP_JS,
        )
    ]
    assert awaiting_starts, "expected awaitingApproval declarations"
    bare_offenders = []
    for start in awaiting_starts:
        # Slice to the next `;` to capture the full multi-line assignment.
        end = APP_JS.index(";", start)
        block = APP_JS[start:end]
        # Four accepted shapes (all equivalent to the v1.18.0
        # widening `!!media_folder || placement_kind === 'plex_upload'`):
        #   1. uses !placed (reuses local canonical derivation)
        #   2. uses placement_kind !== 'plex_upload' inline
        #   3. uses !isPlexUpload (local variable wrapping #2 — see
        #      app.js:7954 `const isPlexUpload = it.placement_kind
        #      === 'plex_upload'`)
        #   4. does NOT reference media_folder at all (different sema)
        uses_placed = "!placed" in block
        uses_widening = "placement_kind !== 'plex_upload'" in block
        uses_is_plex_upload_var = "!isPlexUpload" in block
        references_mf = "it.media_folder" in block or "media_folder" in block
        if references_mf and not (
            uses_placed or uses_widening or uses_is_plex_upload_var
        ):
            bare_offenders.append((start, block.strip()))
    assert not bare_offenders, (
        f"v1.19.38: {len(bare_offenders)} awaitingApproval "
        f"predicate(s) gate on `!it.media_folder` alone — must "
        f"add the v1.18.0 widening (`!placed` or `placement_kind"
        f" !== 'plex_upload'`). Offenders:\n"
        + "\n".join(
            f"  offset {o}: {b[:200]}..." for o, b in bare_offenders
        )
    )


# ── Existing 5 SRC-axis sites still carry the widening ──────


def test_compute_src_letter_uses_widening():
    """`computeSrcLetter` (app.js:~7367 per CLAUDE.md; actually
    closer to 7687 post-v1.19.x) — pin the v1.18.0 widening."""
    body = _slice_function("computeSrcLetter")
    assert (
        "!!it.media_folder" in body
        and "placement_kind === 'plex_upload'" in body
    ), (
        "v1.19.38 (regression guard): computeSrcLetter must "
        "preserve the v1.18.0 placed-widening shape"
    )


def test_render_library_row_inline_src_uses_widening():
    """`renderLibraryRow` inline-SRC site (~7461 per CLAUDE.md)."""
    body = _slice_function("renderLibraryRow")
    # Find the first occurrence of the placed derivation.
    placed_idx = body.index("const placed = !!it.media_folder")
    chunk = body[placed_idx:placed_idx + 200]
    assert "placement_kind === 'plex_upload'" in chunk, (
        "v1.19.38 (regression guard): renderLibraryRow inline-SRC "
        "must preserve the v1.18.0 placed-widening shape"
    )


def test_update_bulk_bar_selection_bucket_uses_widening():
    """Bulk-bar selection-bucket `placed` derivation (CLAUDE.md
    table says updateBulkBar:~9344, but actually lives in
    updateLibrarySelectionUi as of v1.19.38 — see CLAUDE.md
    table line-number drift, audit observation #6). Pin the
    v1.18.0 widening at the relevant `placed` declaration."""
    body = _slice_function("updateLibrarySelectionUi")
    placed_idx = body.index("const placed = !!it.media_folder")
    chunk = body[placed_idx:placed_idx + 200]
    assert "placement_kind === 'plex_upload'" in chunk, (
        "v1.19.38 (regression guard): selection-bucket `placed` "
        "derivation must preserve the v1.18.0 widening"
    )


def test_is_plex_agent_row_uses_widening():
    """`isPlexAgentRow` predicate (~12241 per CLAUDE.md). The
    v1.18.75 fix added the widening here; pin it stays."""
    body = _slice_function("isPlexAgentRow")
    assert (
        "media_folder" in body
        and ("plex_upload" in body or "placement_kind" in body)
    ), (
        "v1.19.38 (regression guard): isPlexAgentRow must "
        "consider plex_upload as a motif-owned placement (the "
        "v1.18.75 fix); regression would make the confirm prompt "
        "fire on plex_upload rows again"
    )


def test_src_letter_sql_uses_media_folder_not_null():
    """The DB-side `_SRC_LETTER_SQL` mirrors the JS via the schema's
    `media_folder TEXT NOT NULL` guarantee on placements — empty
    string '' is the plex_upload sentinel, so `media_folder IS NOT
    NULL` matches both sidecar AND plex_upload placements. Pin the
    SQL shape so a future regression to `media_folder IS NOT NULL
    AND media_folder != ''` (which would silently exclude
    plex_upload) is caught."""
    # v1.21.57: _SRC_LETTER_SQL is now built by _src_letter_sql(); check
    # the RENDERED constant (byte-identical default), not the source text.
    from app.web.api import _SRC_LETTER_SQL as sql
    # The placed predicate must remain `p.media_folder IS NOT NULL`.
    assert "p.media_folder IS NOT NULL" in sql, (
        "v1.19.38 (regression guard): _SRC_LETTER_SQL must use "
        "`p.media_folder IS NOT NULL` (the schema's NOT NULL "
        "constraint guarantees plex_upload rows have '' not NULL, "
        "so this predicate captures both sidecar AND plex_upload)"
    )
    # Must NOT have added a `!= ''` clause that would silently
    # exclude plex_upload.
    assert "media_folder != ''" not in sql, (
        "v1.19.38: regression — `media_folder != ''` would exclude "
        "plex_upload rows (their sentinel value). The schema's "
        "NOT NULL is the canonical mechanism."
    )
