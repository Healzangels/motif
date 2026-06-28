"""v1.18.67 — pending-update UX polish + KEEP CURRENT filter alignment.

the user's polish list from v1.18.66 deploy:

  > wondering if we could fix the blue pending accept update text as
  > right now it looks a bit crowded.
  >
  > also can we make it clearer that the bottom thumbnail is the
  > current active thumbnail, I like the current -> proposed above
  > but then have the large current below becomes confusing like
  > you're seeing it twice.
  >
  > also selecting keep current removes the blue ! at the start of
  > the row but the filter still tracks it as having the blue !
  > and doesn't go away.

## Scope

1. Info-card "(pending — ACCEPT UPDATE to commit)" → "(pending)"
   with the full action hint moved into a `title` tooltip. Pre-fix
   the long label wrapped the row and crowded the URL display.

2. Bottom thumbnail block (the big "▸ click to watch on YouTube"
   preview at the end of the info card) hides when the PROPOSED
   CHANGE diff is rendered. The diff's CURRENT tile already shows
   the same thumbnail; the bottom block was a redundant second copy.
   When no pending update is in flight (diffSection === ''), the
   bottom block stays — that's still the only "what's playing"
   preview for normal T/U rows.

3. attn_pills=update filter SQL now uses section-scoped COALESCE
   (per-section first, '' fallback) instead of unsectioned EXISTS.
   Mirrors the row exclamation predicate (actionable_update at
   api.py:2155). Pre-fix KEEP CURRENT wrote decision='declined' to
   pu_sec but the filter walked pending_updates without section
   scope → '' global row at decision='pending' still matched →
   row stayed in the // NEEDS WORK result set even though its
   blue ! exclamation cleared. Class-9 mirror-drift fix (same
   class as v1.18.65's TDB-pill priority alignment).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── Item 1: (pending) label shortened ───────────────────────


def test_pending_suffix_label_shortened():
    """The visible label must read '(pending)' — not the verbose
    pre-fix '(pending — ACCEPT UPDATE to commit)' which wrapped
    the URL row."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # The new compact form.
    assert ">(pending)<" in js, (
        "v1.18.67: visible label must be the compact '(pending)' "
        "form. Pre-fix the verbose 'ACCEPT UPDATE to commit' "
        "label wrapped the URL row on narrow viewports."
    )
    # The old verbose form must be gone from the suffix render.
    # (May still appear in archaeology comments — search only the
    # _pendingSuffix template literal.)
    suffix_idx = js.index("_pendingSuffix")
    suffix_block = js[suffix_idx:suffix_idx + 800]
    assert "ACCEPT UPDATE to commit)" not in suffix_block, (
        "v1.18.67: verbose suffix label must be removed from the "
        "rendered template (the hint moves into the `title` "
        "tooltip)"
    )


def test_pending_suffix_hint_lives_in_title_attribute():
    """Full action hint moves into the `title` tooltip so hover
    still reveals what ACCEPT UPDATE / KEEP CURRENT do. Pin the
    tooltip text so a future copy edit doesn't drop both the
    label AND the hint."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    suffix_idx = js.index("_pendingSuffix")
    suffix_block = js[suffix_idx:suffix_idx + 800]
    assert 'title="ACCEPT UPDATE to commit · KEEP CURRENT to dismiss"' in suffix_block, (
        "v1.18.67: full action hint must live in the title tooltip"
    )


def test_pending_suffix_still_blue_colored():
    """v1.18.63's blue-coloured pending suffix is the visual cue
    that distinguishes pending from committed labels. The shorter
    text must keep the blue colour."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    suffix_idx = js.index("_pendingSuffix")
    suffix_block = js[suffix_idx:suffix_idx + 800]
    # v1.19.89: the inline color:var(--blue) moved to the
    # .info-tag-pending class (info-card inline-style cleanup); the
    # blue cue is preserved via that class + its CSS rule.
    assert "info-tag-pending" in suffix_block, (
        "v1.18.67 + v1.19.89: blue pending cue preserved via the "
        ".info-tag-pending class"
    )
    css = (REPO / "app" / "web" / "static" / "app.css").read_text()
    pend_idx = css.index(".info-tag-pending")
    assert "var(--blue)" in css[pend_idx:pend_idx + 60], (
        "v1.19.89: .info-tag-pending must keep the blue color"
    )


def test_pending_suffix_marker_explains_why_it_shortened():
    """Marker required so a future code reader knows why the label
    is compact + has the verbose hint moved to a tooltip."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    suffix_idx = js.index("_pendingSuffix")
    suffix_block = js[max(0, suffix_idx - 1500):suffix_idx + 800]
    assert "v1.18.67" in suffix_block, (
        "v1.18.67: marker required near the pending suffix render"
    )


# ── Item 2: bottom thumbnail hides when diff is showing ─────


def test_bottom_thumbnail_skips_when_diff_section_renders():
    """The info-card's big bottom thumbnail block (line ~13233
    IIFE) must early-return '' when diffSection is truthy. Pre-fix
    a row with a pending update rendered the CURRENT tile (in the
    diff) AND a redundant big copy at the bottom — the user: 'like
    you're seeing it twice.'"""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # The thumbnail IIFE is anchored on the v1.15.129 comment.
    iife_idx = js.index("v1.15.129: source-aware thumbnail block")
    # Walk to the closing })() that ends the IIFE.
    iife_end = js.index("})()", iife_idx)
    iife = js[iife_idx:iife_end]
    # The early-return must fire on diffSection.
    assert "if (diffSection) return '';" in iife, (
        "v1.18.67: bottom thumbnail must early-return when "
        "diffSection is non-empty. Pre-fix the diff's CURRENT "
        "tile + the bottom big thumbnail rendered the same vid "
        "twice on rows with a pending update."
    )


def test_bottom_thumbnail_still_renders_when_no_diff():
    """When diffSection is '' (no pending update), the YouTube /
    SoundCloud thumbnail branches must still fire — that's the only
    'what's playing' preview for normal T/U/P rows."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    iife_idx = js.index("v1.15.129: source-aware thumbnail block")
    iife_end = js.index("})()", iife_idx)
    iife = js[iife_idx:iife_end]
    # The YT + SC branches survive.
    assert "tUrlSrc === 'youtube'" in iife
    assert "tUrlSrc === 'soundcloud'" in iife
    # And the diffSection gate is BEFORE the source-discrimination
    # branches — the early-return happens first.
    gate_idx = iife.index("if (diffSection) return '';")
    yt_idx = iife.index("tUrlSrc === 'youtube'")
    assert gate_idx < yt_idx, (
        "v1.18.67: diffSection gate must precede the source "
        "discrimination (otherwise it'd be dead code)"
    )


def test_bottom_thumbnail_marker_present():
    """v1.18.67 archaeology marker explains why the early-return
    exists. Pin so a refactor doesn't silently drop it."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    iife_idx = js.index("v1.15.129: source-aware thumbnail block")
    iife_end = js.index("})()", iife_idx)
    iife = js[iife_idx:iife_end]
    assert "v1.18.67" in iife
    # the user's quote anchor — survives line-wrap.
    iife_flat = " ".join(iife.split())
    assert "seeing it twice" in iife_flat, (
        "v1.18.67: the user's repro phrase preserved in the marker"
    )


# ── Item 3: attn_pills=update filter is section-scoped ──────


def _attn_update_branch() -> str:
    """Return the ATTN-pill axis `elif p == "update":` body.

    api.py has TWO `elif p == "update":` branches — the first is
    the TDB-pill axis (uses the PENDING_EXISTS constant, already
    section-scoped) and the second is the ATTN-pill axis (the
    STATUS=! row exclamation filter that v1.18.67 fixes). Anchor
    on the surrounding `attn_branches.append` calls — that array
    is unique to the ATTN axis."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    # The ATTN axis is the second `elif p == "update":` AND lives
    # in the block that uses `attn_branches.append`. Find both
    # `elif p == "update":` matches and pick the one whose
    # surrounding context has `attn_branches.append`.
    attn_axis_anchor = src.index("if attn_pills and not attn_needs_post_stat:")
    upd_idx = src.index('elif p == "update":', attn_axis_anchor)
    upd_end = src.index('elif p == "cookies":', upd_idx)
    return src[upd_idx:upd_end]


def test_attn_update_filter_uses_section_scoped_coalesce():
    """The 'update' attn_pill SQL filter must use COALESCE(pu_sec
    decision, pu_global decision) = 'pending' — matching the row-
    level actionable_update predicate at api.py:2155. Pre-fix the
    filter used an unsectioned EXISTS subquery which kept matching
    rows whose ONLY pending decision was the title-global '' row,
    even after the user KEEP-CURRENTed the per-section row."""
    upd_branch = _attn_update_branch()
    # Pre-fix shape that must NOT survive.
    assert "EXISTS (SELECT 1 FROM pending_updates pu " not in upd_branch.split(
        "AND pu.decision = 'pending'"
    )[0], (
        "v1.18.67: pre-fix EXISTS shape with no section scope must "
        "be replaced by the section-scoped COALESCE"
    )
    # The section-scoped COALESCE pattern must be present.
    assert "COALESCE(" in upd_branch
    assert "pu.section_id = pi.section_id" in upd_branch
    assert "pu.section_id = ''" in upd_branch
    # The decision check fires on the COALESCE result.
    assert ") = 'pending'" in upd_branch, (
        "v1.18.67: decision='pending' must be checked on the "
        "COALESCE expression, not on an inner WHERE"
    )


def test_attn_update_filter_marker_explains_mirror_drift():
    """The v1.18.67 marker must explain WHY the filter became
    section-scoped — class-9 mirror-drift sibling of v1.18.65's
    TDB pill fix. Future code readers tracing 'why does this
    filter use a 5-line COALESCE instead of a one-line EXISTS'
    should land in this marker."""
    upd_branch = _attn_update_branch()
    assert "v1.18.67" in upd_branch
    # Mirror-drift reference.
    assert "actionable_update" in upd_branch or "mirror" in upd_branch, (
        "v1.18.67: marker should reference the mirror-drift class "
        "or the sibling actionable_update predicate"
    )


def test_attn_update_filter_mirrors_actionable_update_predicate():
    """The filter's decision check must match exactly what the
    row-level actionable_update SQL produces — section-scoped
    COALESCE → 'pending'. If they drift, KEEP CURRENT will produce
    visible/filter mismatches again."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    # actionable_update from the row payload.
    aff_idx = src.index(") AS actionable_update,")
    # Walk back to the start of the CASE WHEN block.
    case_start = src.rindex("(CASE WHEN", 0, aff_idx)
    aff_sql = src[case_start:aff_idx + 50]
    # actionable_update uses COALESCE(pu_dsec.decision, pu_dglobal.decision)
    # = 'pending'. The aliases pu_dsec/pu_dglobal are LEFT JOINs in
    # the row SELECT — different from the filter context which
    # uses subqueries. But the SEMANTIC must be identical:
    # per-section decision first, '' fallback, check 'pending'.
    # v1.21.81: the decision JOINs split off from the pu_sec/pu_global
    # detection JOINs and gained edition_key=pi.edition_key — so they're
    # named pu_dsec/pu_dglobal now (pu_sec/pu_global stay for kind reads).
    assert "COALESCE(pu_dsec.decision, pu_dglobal.decision, 'pending') = 'pending'" in aff_sql, (
        "row-level actionable_update predicate must remain the "
        "canonical shape v1.18.67's filter mirrors"
    )


def test_attn_update_filter_keeps_src_letter_and_presence_gates():
    """The v1.13.86 / v1.12.108 gates (SRC != '-' + motif-tracking
    presence + URL-diff check) must SURVIVE the section-scope fix
    — they're independent invariants. Pin so the rewrite didn't
    accidentally drop them."""
    upd_branch = _attn_update_branch()
    # SRC letter gate (excludes pure-P / pure-'-' rows).
    assert ") != '-'" in upd_branch
    # Presence-check (local_files OR user_overrides OR placements
    # OR local_theme_file).
    assert "local_files" in upd_branch
    assert "user_overrides" in upd_branch
    assert "COALESCE(p_e.media_folder, p_g.media_folder) IS NOT NULL" in upd_branch
    assert "pi.local_theme_file = 1" in upd_branch
    # v1.22.10: urls_match + URL-diff now in the shared actionable helper
    # (which this attn_pills=update branch invokes). The SRC + presence gates
    # above stay inline — they're independent invariants.
    assert "_pending_update_actionable_sql" in upd_branch
