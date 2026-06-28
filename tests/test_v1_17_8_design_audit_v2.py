"""v1.17.8 — design audit v2 rollover.

Second-pass audit of motif's UI for design consistency, after
v1.17.5 / v1.17.7 closed the first round. Scope this pass:
dialogs, empty/loading states, typography + glyphs, form inputs,
motion tokens.

## Findings closed

* **A1 (HIGH)** — `new-token-dlg` was the outlier on every
  dialog dimension. Refactored to match the canonical
  `<header class="dlg-head"> <h2 class="dlg-title"> ... <button
  class="dlg-close">` shell that the other 4 dialogs share.
  Added a `// CANCEL` button to the form-actions row (was
  missing — user could only close via × or Escape). Dropped
  `.form-label-row` wrappers (settings-page convention, not
  needed in dialog forms). Retired the v1.15.115 `.dlg-body >
  .dlg-close` absolute-corner CSS rule since its only consumer
  (this dialog) no longer needs it.

* **E1 (HIGH)** — `ops.css` motion-token migration completed.
  v1.15.110 migrated app.css but missed ops.css; 10 raw
  transition durations (`0.15s`, `0.2s`, `0.25s`, `0.4s`,
  `0.3s`) now use `var(--motion-normal)` / `var(--motion-slow)`.
  Deliberate-outlier timings (1s bar-fills + 0.22s/0.28s
  drawer slide pair) kept raw with documenting comments.

* **C6 (MED)** — `▲` glyph overload closed. Dashboard backfill
  banner switched to `⚠` (matching `.dry-run-banner`); `▲`/`▼`
  retained as column-header sort indicators only. Banners now
  read uniformly: `▸` info (cyan/green), `⚠` warning (amber).
  Also dropped redundant inline `border-color:var(--amber);
  background:var(--bg-tint-amber)` + glyph-color overrides
  since `.missing-banner` already provides those by default.

* **C1 (MED)** — `<h2>// LIVE OPS</h2>` in the ops drawer now
  carries `.ops-drawer-title`. The ops.css rule selector
  changed from `.ops-drawer-head h2` (descendant selector,
  brittle) to `.ops-drawer-title` (class-based) — and migrated
  the raw `font-size: 13px` to `var(--t-small)` while there.

## Skipped (LOW, no user-visible impact)

- B1/B2 empty-state primitive — works as-is, no canonical
  primitive added (would be a different refactor scope)
- D4 aria-required / aria-invalid — homelab single-tenant, not
  a current goal
- C5 / C4 — contextual one-offs / future-use primitives
- A2/A3/A4 — info-dlg differences are justified divergences
- C7/C8 — `×` and `▸` mild overloads are context-distinguished
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_CSS = REPO / "app" / "web" / "static" / "app.css"
OPS_CSS = REPO / "app" / "web" / "static" / "ops.css"
BASE_HTML = REPO / "app" / "web" / "templates" / "base.html"
SETTINGS_HTML = REPO / "app" / "web" / "templates" / "settings.html"
DASHBOARD_HTML = REPO / "app" / "web" / "templates" / "dashboard.html"


# ── A1: new-token-dlg canonical shell ─────────────────────────


def test_new_token_dlg_uses_canonical_header_shell():
    """The new-token-dlg must compose its title via
    `<header class="dlg-head">` + `<h2 class="dlg-title">` —
    matching the other 4 dialogs."""
    html = SETTINGS_HTML.read_text()
    dlg_idx = html.index('id="new-token-dlg"')
    dlg_end = html.index("</dialog>", dlg_idx)
    block = html[dlg_idx:dlg_end]
    assert '<header class="dlg-head">' in block
    assert '<h2 class="dlg-title">// NEW API TOKEN</h2>' in block, (
        "v1.17.8 A1: title must be `<h2 class=\"dlg-title\">// "
        "NEW API TOKEN</h2>` — the `// ` prefix matches every "
        "other dialog title."
    )


def test_new_token_dlg_close_button_inside_header():
    """The `.dlg-close` button moves from a bare `.dlg-body`
    child to a `.dlg-head` child — retiring the v1.15.115
    absolute-corner variant."""
    html = SETTINGS_HTML.read_text()
    dlg_idx = html.index('id="new-token-dlg"')
    dlg_end = html.index("</dialog>", dlg_idx)
    block = html[dlg_idx:dlg_end]
    head_idx = block.index('<header class="dlg-head">')
    head_end = block.index("</header>", head_idx)
    head_block = block[head_idx:head_end]
    assert 'class="dlg-close"' in head_block, (
        "v1.17.8 A1: .dlg-close must live inside .dlg-head, not "
        "be a bare child of .dlg-body."
    )


def test_new_token_dlg_has_cancel_button():
    """The form-actions row must contain BOTH the // CREATE
    submit and a // CANCEL button — every other form dialog
    has this pair."""
    html = SETTINGS_HTML.read_text()
    dlg_idx = html.index('id="new-token-dlg"')
    dlg_end = html.index("</dialog>", dlg_idx)
    block = html[dlg_idx:dlg_end]
    assert ">// CREATE</button>" in block
    assert ">// CANCEL</button>" in block, (
        "v1.17.8 A1: new-token-dlg needs a // CANCEL button so "
        "users can dismiss the dialog without × or Escape."
    )


def test_new_token_dlg_labels_drop_form_label_row():
    """Settings-page convention is `<label class="form-label">
    <div class="form-label-row">...` — but dialog forms use
    bare `<label class="form-label">` (library dialogs are the
    template). The new-token-dlg now matches the library-dialog
    convention since it IS a dialog."""
    html = SETTINGS_HTML.read_text()
    dlg_idx = html.index('id="new-token-dlg"')
    dlg_end = html.index("</dialog>", dlg_idx)
    block = html[dlg_idx:dlg_end]
    # Active HTML usage of `.form-label-row` must be gone. The
    # marker comment above the dialog references the class name
    # by intent — that's documentation, not a consumer.
    assert '<div class="form-label-row">' not in block, (
        "v1.17.8 D8: new-token-dlg labels should not wrap in "
        ".form-label-row — dialog-form convention is bare label."
    )


def test_v1_15_115_dlg_body_close_variant_retired():
    """The `.dlg-body > .dlg-close` absolute-corner CSS RULE
    must be gone from app.css (the v1.17.8 retirement comment
    in app.css references the rule by name as archaeology —
    that text is fine). What must NOT survive is the rule
    declaration itself."""
    css = APP_CSS.read_text()
    # The active CSS rule has the `{` directly after, so anchor
    # on `.dlg-body > .dlg-close {` specifically.
    assert ".dlg-body > .dlg-close {" not in css, (
        "v1.17.8: the v1.15.115 absolute-corner CSS rule must "
        "be retired — its only consumer was refactored this tag."
    )


# ── E1: ops.css motion-token migration ────────────────────────


def _strip_css_comments(block: str) -> str:
    """Strip `/* ... */` comments out of a CSS block so the
    test assertions only inspect the active declarations.
    Marker comments referencing raw values for archaeology
    purposes shouldn't break assertions about active rules."""
    import re
    return re.sub(r"/\*.*?\*/", "", block, flags=re.DOTALL)


def test_ops_css_op_pill_uses_motion_token():
    """The .op-pill transition (most user-visible interactive
    surface in the ops drawer) must use --motion-normal."""
    css = OPS_CSS.read_text()
    pill_idx = css.index(".op-pill {")
    pill_end = css.index("\n}", pill_idx)
    block = _strip_css_comments(css[pill_idx:pill_end])
    assert "var(--motion-normal)" in block
    # No raw 0.15s seconds in the .op-pill block (comments
    # mentioning historical raw values are stripped above).
    assert "0.15s" not in block


def test_ops_css_op_card_uses_motion_token():
    """The .op-card transition (border-color + box-shadow on
    hover/state-change) uses --motion-normal — 0.2s + 0.25s
    both round to it within imperceptible drift."""
    css = OPS_CSS.read_text()
    card_idx = css.index(".op-card {")
    card_end = css.index("\n}", card_idx)
    block = _strip_css_comments(css[card_idx:card_end])
    assert "var(--motion-normal)" in block
    assert "0.2s ease" not in block and "0.25s" not in block


def test_ops_css_timeline_step_uses_motion_slow():
    """The .op-card-timeline-step background fade was 0.4s
    raw — exact match for --motion-slow."""
    css = OPS_CSS.read_text()
    step_idx = css.index(".op-card-timeline-step {")
    step_end = css.index("\n}", step_idx)
    block = css[step_idx:step_end]
    assert "var(--motion-slow)" in block


def test_ops_css_op_card_cancel_uses_motion_token():
    css = OPS_CSS.read_text()
    cancel_idx = css.index(".op-card-cancel {")
    cancel_end = css.index("\n}", cancel_idx)
    block = css[cancel_idx:cancel_end]
    assert "var(--motion-normal)" in block


def test_ops_css_drawer_scrim_documents_intentional_raw():
    """The drawer scrim + panel slide are a paired animation
    deliberately faster than --motion-slow but slower than
    --motion-normal. The scrim rule keeps raw `0.22s` with a
    documenting comment explaining why."""
    css = OPS_CSS.read_text()
    scrim_idx = css.index(".ops-drawer-scrim {")
    scrim_end = css.index("\n}", scrim_idx)
    block = css[scrim_idx:scrim_end]
    # The raw 0.22s timing stays.
    assert "0.22s" in block
    # And it's flanked by a v1.17.8-marker comment documenting
    # the intentional-outlier rationale.
    assert "v1.17.8" in block, (
        "v1.17.8 E2: deliberate-outlier raw timings need a marker "
        "comment explaining why they're not migrated."
    )


# ── C6: ▲ → ⚠ on dashboard backfill banner ────────────────────


def test_dashboard_backfill_banner_uses_warning_glyph():
    """The dashboard backfill banner now uses `⚠` instead of
    `▲`. `▲` is reserved for the column-header sort-asc
    indicator; `⚠` matches `.dry-run-banner`'s warning glyph
    for visual parity."""
    html = DASHBOARD_HTML.read_text()
    backfill_idx = html.index('id="dash-backfill-banner"')
    backfill_end = html.index("</div>", backfill_idx)
    block = html[backfill_idx:backfill_end + 6]
    # ⚠ replaces ▲ in this banner.
    assert ">⚠</span>" in block, (
        "v1.17.8 C6: dashboard backfill banner must use ⚠ "
        "(warning) instead of ▲ (sort-asc — collides with "
        "column-header indicators)."
    )
    # ▲ must NOT appear in this specific banner block.
    assert ">▲<" not in block


def test_dashboard_backfill_banner_drops_redundant_amber_inline():
    """Inline `border-color:var(--amber)` and `background:
    var(--bg-tint-amber)` overrides match the `.missing-banner`
    base rule defaults and are now dropped. Inline glyph-color
    override likewise — `.missing-banner-glyph` is already amber
    by default."""
    html = DASHBOARD_HTML.read_text()
    backfill_idx = html.index('id="dash-backfill-banner"')
    backfill_end = html.index("</div>", backfill_idx)
    block = html[backfill_idx:backfill_end + 6]
    assert "border-color:var(--amber)" not in block
    assert "background:var(--bg-tint-amber)" not in block
    # The glyph color inline override is gone too.
    assert 'style="color:var(--amber)">' not in block


# ── C1: ops-drawer title class ────────────────────────────────


def test_ops_drawer_title_uses_dedicated_class():
    """The ops drawer header's `<h2>` carries `.ops-drawer-title`
    (was a bare `<h2>` styled via the brittle `.ops-drawer-head
    h2` descendant selector)."""
    html = BASE_HTML.read_text()
    assert '<h2 class="ops-drawer-title">// LIVE OPS</h2>' in html


def test_ops_css_drawer_title_rule_uses_class_not_descendant():
    """`.ops-drawer-title` is a class-based rule (replaces the
    pre-fix `.ops-drawer-head h2` descendant selector). Also
    migrates the raw `font-size: 13px` to `var(--t-small)`."""
    css = OPS_CSS.read_text()
    assert ".ops-drawer-title {" in css
    title_idx = css.index(".ops-drawer-title {")
    title_end = css.index("\n}", title_idx)
    block = css[title_idx:title_end]
    assert "var(--t-small)" in block, (
        "v1.17.8 C1: ops-drawer title font-size migrates from "
        "raw 13px to var(--t-small)."
    )
    assert "13px" not in block
    # The brittle descendant-selector rule should not survive
    # alongside the class-based one (we replaced, not added).
    assert ".ops-drawer-head h2 {" not in css
