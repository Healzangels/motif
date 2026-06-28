"""v1.14.18 — info-card polish bundle.

Two unrelated info-card fixes shipped together (both small,
both single-file changes that don't touch shared infrastructure).

## Item A — previous_url hides when reverting won't help

the user's report (Batman: Hush red-pill TDB row): the "previous
youtube url" line shows the same URL as the "themerrdb youtube"
line — and that URL is the dead one motif keeps failing on.
Right below it the dialog says "REVERT unavailable — the
previous URL was a ThemerrDB URL...". So we know reverting
won't help, and showing the dead URL is just visual noise.

The existing v1.13.4 hidePrev predicate already hid the row
when previousUrl === currentUrl. v1.14.18 extends it: hide
when revertHint is set (any of the three branches: no-op
revert, identical URL, or themerrdb-source previous). Single
source of truth — if reverting is unavailable, the URL itself
is non-actionable.

The TDB-source previous case is where the `red-pill TDB` shape
naturally falls (motif tried TDB → failed → user set override
→ now previousKind is 'themerrdb' and revertHint fires).

Implementation note: revertHint computation moved up above the
previous_url render block so hidePrev can reference it. The
revert-hint rendering at the end of the puBlock stays in
place — same DOM position, just reads from the earlier-computed
variable.

## Item B — RESOLVED VIA banner color matches SRC chip color

Pre-fix the "// RESOLVED VIA URL" / "// RESOLVED VIA UPLOAD" /
"// RESOLVED VIA ADOPT" banner used `tone: 'user'` (violet)
unconditionally. So an adopted-sidecar resolution rendered in
violet even though the row's SRC chip is blue (A).

v1.14.18 maps tone by `local_source` so the banner matches:
  - url    → 'user'  (violet, matches U chip)
  - upload → 'user'  (violet, matches U chip — uploads also
                      render as U via _SRC_LETTER_SQL)
  - adopt  → 'adopt' (blue, matches A chip)

Mirror principle: the banner color and the row's SRC chip
share a single source of truth (local_source) and now agree
visually.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── Item A: previous_url hide on revertHint ───────────────────


def test_revert_hint_computed_before_hide_prev():
    """The v1.14.18 reorder moves revertHint computation above
    the previous_url render so hidePrev can reference it. Pin
    the order via index-of comparison."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # Anchor on the new-position revertHint declaration.
    revert_decl = js.index("let revertHint = '';")
    hide_pos = js.index("const hidePrev =", revert_decl)
    render_pos = js.index("const previousUrlLink", hide_pos)
    assert revert_decl < hide_pos < render_pos


def test_hide_prev_now_includes_revert_hint_branch():
    """The hidePrev predicate must OR-in `!!revertHint` so any
    of the three revert-unavailable cases hides the row."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # The new hidePrev shape lives near the v1.14.18 marker.
    marker = "v1.14.18: also suppress when revertHint fires"
    assert marker in js
    # v1.22.14: anchor on the hidePrev declaration itself (where the OR clause
    # lives) instead of a fixed window from the marker — robust as the revert
    # branch above it grows.
    hp = js.index("const hidePrev")
    block = js[hp:hp + 300]
    # The OR-clause check.
    assert "(previousUrl !== '' && !!revertHint)" in block


def test_revert_hint_branches_unchanged():
    """The three revertHint branches (the contract for "reverting
    won't help") must survive the reorder verbatim. Pin so a
    refactor doesn't accidentally narrow the cases that hide
    the previous_url row."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # The reordered block lives above the hidePrev declaration.
    marker = "let revertHint = '';"
    assert marker in js
    anchor = js.index(marker)
    # v1.22.14: anchor on the hidePrev close (not a fixed window) — the v1.22.14
    # isFileSourced branch sits above these three and grew the block.
    block = js[anchor:js.index("const hidePrev", anchor)]
    # Three branches — no-op, identical URL, themerrdb-source — survive the
    # v1.22.14 isFileSourced generalization (they're the `else if` tail).
    assert "if (!previousUrl && pu && pu.decision === 'accepted')" in block
    assert "else if (previousUrl && previousUrl === currentCanonical)" in block
    assert "else if (previousUrl && previousKind === 'themerrdb')" in block


def test_revert_hint_rendered_to_pu_block_unchanged():
    """The revert-hint <dt>/<dd> rendering at the end of puBlock
    stays in place — same DOM position as before, just reads
    from the earlier-computed variable."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # The render site is unchanged; pin its presence.
    assert 'puBlock += `<dt class="muted">revert</dt>`' in js


def test_no_duplicate_revert_hint_declaration():
    """Defensive: only ONE `let revertHint = '';` declaration
    must exist — a duplicate would shadow the earlier value
    or fail at parse time depending on scope."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # Count declarations.
    decl_count = js.count("let revertHint = '';")
    assert decl_count == 1, (
        f"v1.14.18: expected exactly 1 `let revertHint` declaration, "
        f"found {decl_count} — the reorder may have left a duplicate"
    )
    # Same for currentCanonical (used inside the reorder).
    canon_count = js.count("const currentCanonical = (ovr && ovr.youtube_url) || tdbUrl || '';")
    assert canon_count == 1


# ── Item B: RESOLVED VIA banner tone matches SRC chip ─────────


def test_resolved_via_tone_branches_by_local_source():
    """The api_recovery_options "RESOLVED VIA …" banner must
    use tone='adopt' on adopted-sidecar resolutions and
    tone='user' on URL/upload resolutions. Pre-fix it was
    hardcoded to 'user' regardless."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    # Anchor on the v1.14.18 marker comment.
    marker = "v1.14.18: tone now matches the SRC chip color"
    assert marker in src
    block_start = src.index(marker)
    block = src[block_start:block_start + 1500]
    # The new branched assignment.
    assert 'tone = "adopt" if local_source == "adopt" else "user"' in block
    # The dict that uses it.
    assert '"priority": 1, "tone": tone,' in block


def test_resolved_via_tone_no_longer_hardcoded_user():
    """Regression guard: the pre-fix line `"tone": "user"` (in
    the RESOLVED VIA dict literal) must not survive. Anchor
    inside the api_recovery_options resolved-options block so
    we don't false-match other tone='user' usages elsewhere
    (the recovery code uses tone='user' legitimately for SET
    URL options)."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    # Strip Python comment lines so the rationale comment quoting
    # the deleted shape doesn't trip the guard.
    src_live = "\n".join(
        line for line in src.splitlines()
        if not line.lstrip().startswith("#")
    )
    # Locate the v1.14.18 block.
    marker = 'tone = "adopt" if local_source == "adopt" else "user"'
    assert marker in src_live
    block_start = src_live.index(marker)
    # The dict literal that follows the assignment.
    block = src_live[block_start:block_start + 1500]
    # The dict must reference the variable, not a hardcoded string.
    assert '"tone": "user"' not in block, (
        "v1.14.18: tone in the RESOLVED VIA dict must read from "
        "the `tone` variable, not a hardcoded 'user' string"
    )


def test_tone_class_map_unchanged_for_user_and_adopt():
    """Regression guard on the JS side: TONE_CLASS must still
    map 'user' → lib-source-user (violet) and 'adopt' →
    lib-source-adopt (blue). The server emits these tone keys;
    the client picks the CSS class. If the map drifts, the
    server fix won't render the right color."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    anchor = js.index("const TONE_CLASS = {")
    block = js[anchor:anchor + 500]
    assert "user: 'lib-source-user'" in block
    assert "adopt: 'lib-source-adopt'" in block


def test_locally_resolved_predicate_still_recognizes_three_kinds():
    """Defensive: locally_resolved still fires on adopt/url/upload.
    If a refactor narrows this set, the v1.14.18 tone branching
    becomes unreachable for the dropped kind."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    assert 'local_source in ("adopt", "url", "upload")' in src
