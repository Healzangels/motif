"""v1.17.7 — design-system § 6 hygiene: promote inline overrides to primitives.

v1.17.5 audit's deferred C-class findings landed here. Stable inline
`style=` overrides on existing primitives (`.missing-banner`,
`.block-body`, `.block-head`, `.muted` paragraph) got promoted to
real CSS classes per DESIGN_SYSTEM § 6's "primitive needs variants"
hygiene rule.

New primitives (CSS additions in app.css):

  * `.missing-banner-cyan` / `.missing-banner-green` — tone variants
    of the existing amber-default `.missing-banner`. Captures the
    border-color + background-tint + glyph color tuple in one
    class. Two consumers:
      - library-scan-hint banner (was inline cyan)
      - library-bulk-bar (was inline green-deep + green-bright glyph)
    The amber default stays as-is (no extra class — backwards-
    compatible for the dashboard backfill banner).

  * `.block-body--tight` — `padding: var(--gap-3) 18px`. Closes the
    v1.15.114 mixed-axis padding smell (gap-3 vertical token + 18px
    horizontal raw px). Used by dashboard SYNC HISTORY + STORAGE
    WASTE sub-blocks where the inner content (charts, tables)
    provides its own row spacing.

  * `.block-head--divided` — `border-top: 1px solid var(--line);
    margin-top: var(--gap-5)`. For sub-section headers inside a
    single panel — closes the v1.17.0 inline `border-top` + spaced
    pattern the user's EVENTS sub-block-head needed.

  * `.help-text` — `font-size: var(--t-tiny); margin-bottom:
    var(--gap-2)`. For intro/help paragraphs above tables or
    controls. Composes with `.muted` (the existing tone primitive)
    so `<p class="muted help-text">` reads as "an intentionally-
    quiet explanatory line."

Template edits:

  - dashboard.html: 2 STORAGE WASTE / SYNC HISTORY block-body
    inline-padding → `.block-body--tight`. Plus the help-text
    paragraph above the STORAGE WASTE table → `.muted .help-text`.
  - library.html: scan-hint banner → `.missing-banner-cyan`;
    bulk-bar → `.missing-banner-green`. Glyph color inline
    `style="color:var(--cyan)"` etc. dropped (now inherited from
    the tone variant).
  - settings.html:920 EVENTS sub-header → `.block-head--divided`.
  - settings.html:215 redundant `style="flex-wrap:wrap"` removed —
    `.form-input-actions` already had `flex-wrap: wrap` in its
    primitive rule.

Intentionally NOT promoted (one-off contextual exceptions, leave
inline per § 5.6 — uses tokens, no recurring pattern):

  - settings.html:95 `style="margin-top:var(--gap-2)"` (PATHS panel
    save row)
  - settings.html:1000 `style="margin-top: var(--gap-3)"` (deferred-
    notice form-hint)
  - settings.html:1167 `style="margin-top:var(--gap-6)"` (import-
    wizard block-head)
  - settings.html:1227 `style="margin-top:var(--gap-4)"` (import-
    summary form-actions)

Each uses a different gap-N magnitude for a one-off context — 4
modifier classes for 4 different magnitudes would be the v1.15.x
"modifier-class explosion" anti-pattern.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_CSS = REPO / "app" / "web" / "static" / "app.css"
DASHBOARD_HTML = REPO / "app" / "web" / "templates" / "dashboard.html"
LIBRARY_HTML = REPO / "app" / "web" / "templates" / "library.html"
SETTINGS_HTML = REPO / "app" / "web" / "templates" / "settings.html"


# ── New primitives in app.css ─────────────────────────────────


def test_missing_banner_cyan_variant_exists():
    css = APP_CSS.read_text()
    assert ".missing-banner-cyan {" in css
    cyan_idx = css.index(".missing-banner-cyan {")
    block = css[cyan_idx:cyan_idx + 400]
    assert "border-color: var(--cyan)" in block
    assert "background: var(--bg-tint-cyan)" in block
    # Glyph + strong tone driven by the variant.
    assert ".missing-banner-cyan .missing-banner-glyph" in css


def test_missing_banner_green_variant_exists():
    css = APP_CSS.read_text()
    assert ".missing-banner-green {" in css
    green_idx = css.index(".missing-banner-green {")
    block = css[green_idx:green_idx + 400]
    assert "border-color: var(--green-deep)" in block
    assert "background: var(--bg-tint-green)" in block
    assert ".missing-banner-green .missing-banner-glyph" in css


def test_block_body_tight_variant_exists():
    """`.block-body--tight` packages the gap-3-vertical / 18px-
    horizontal padding the dashboard sync-history + storage-waste
    blocks were using inline. Mixed-axis is still mixed-axis at
    the CSS level, but now scoped to one rule + documented."""
    css = APP_CSS.read_text()
    assert ".block-body--tight {" in css
    tight_idx = css.index(".block-body--tight {")
    block = css[tight_idx:tight_idx + 200]
    assert "padding: var(--gap-3) 18px" in block


def test_block_head_divided_variant_exists():
    """`.block-head--divided` packages the border-top + margin-top
    sub-section-header pattern that v1.17.0's EVENTS sub-header
    needed. Two-rule primitive."""
    css = APP_CSS.read_text()
    assert ".block-head--divided {" in css
    div_idx = css.index(".block-head--divided {")
    block = css[div_idx:div_idx + 300]
    assert "border-top: 1px solid var(--line)" in block
    assert "margin-top: var(--gap-5)" in block


def test_help_text_primitive_exists():
    """`.help-text` composes with `.muted` for narrative
    intros above structural content. Bundles t-tiny font-size +
    gap-2 bottom-margin."""
    css = APP_CSS.read_text()
    assert ".help-text {" in css
    ht_idx = css.index(".help-text {")
    block = css[ht_idx:ht_idx + 200]
    assert "font-size: var(--t-tiny)" in block
    assert "margin-bottom: var(--gap-2)" in block


# ── Templates use the new primitives ──────────────────────────


def test_dashboard_uses_block_body_tight():
    """Two dashboard sub-blocks (SYNC HISTORY + STORAGE WASTE)
    use the new `.block-body--tight` variant instead of inline
    `style="padding:var(--gap-3) 18px"`."""
    html = DASHBOARD_HTML.read_text()
    # Both occurrences of the inline override are gone.
    assert 'style="padding:var(--gap-3) 18px"' not in html
    # Both consumers now compose with the new variant.
    assert html.count("block-body block-body--tight") >= 2


def test_dashboard_storage_waste_intro_uses_help_text():
    """The intro paragraph above the STORAGE WASTE table uses
    `.muted .help-text` instead of the dual inline override."""
    html = DASHBOARD_HTML.read_text()
    assert 'style="font-size:var(--t-tiny);margin-bottom:var(--gap-2)"' not in html
    assert '<p class="muted help-text">' in html


def test_library_scan_hint_uses_cyan_variant():
    """The library-page scan-hint banner uses the new
    `.missing-banner-cyan` tone variant instead of three inline
    overrides (border-color + background + glyph color)."""
    html = LIBRARY_HTML.read_text()
    assert 'class="missing-banner missing-banner-cyan"' in html
    # Inline border-color + background overrides gone.
    assert 'border-color:var(--cyan)' not in html
    # Glyph inline color override gone.
    assert 'style="color:var(--cyan)">▸' not in html


def test_library_bulk_bar_uses_green_variant():
    """The library bulk-action bar uses the new
    `.missing-banner-green` tone variant. Note: the v1.17.6
    `#library-bulk-bar` ID-scoped overrides (flex-shrink + no-
    wrap + overflow-x + min-height) stay; only the tone-
    specific inline `style=` is replaced by the variant class."""
    html = LIBRARY_HTML.read_text()
    assert 'class="missing-banner missing-banner-green"' in html
    assert 'border-color:var(--green-deep)' not in html
    assert 'style="color:var(--green-bright)">▸' not in html


def test_settings_events_header_uses_divided_variant():
    """The EVENTS sub-block-head in settings.html uses the new
    `.block-head--divided` variant instead of inline
    `style="border-top: 1px solid var(--line); margin-top: var(--gap-5)"`."""
    html = SETTINGS_HTML.read_text()
    assert 'class="block-head block-head--divided"' in html
    # Inline border-top override gone from this header.
    assert (
        'style="border-top: 1px solid var(--line); margin-top: var(--gap-5)"'
        not in html
    )


def test_form_input_actions_redundant_inline_wrap_removed():
    """`.form-input-actions` already has `flex-wrap: wrap` in its
    primitive rule — the v1.15.x-era inline override at
    settings.html:215 was redundant."""
    html = SETTINGS_HTML.read_text()
    assert 'style="flex-wrap:wrap"' not in html


# ── Existing primitives unchanged ────────────────────────────


def test_base_missing_banner_still_amber_default():
    """The base `.missing-banner` rule keeps its amber default.
    v1.17.7 added cyan + green variants as OVERRIDES, not by
    refactoring the default away — the dashboard backfill banner
    + any future amber banner relies on the default."""
    css = APP_CSS.read_text()
    base_idx = css.index(".missing-banner {")
    base_rule = css[base_idx:base_idx + 400]
    assert "border: 1px solid var(--amber)" in base_rule
    assert "rgba(var(--amber-rgb), 0.05)" in base_rule


def test_form_input_actions_primitive_keeps_flex_wrap():
    """Regression guard: `.form-input-actions` must keep its
    `flex-wrap: wrap` rule so the v1.17.7 removal of the
    redundant inline override at settings.html:215 still works."""
    css = APP_CSS.read_text()
    fi_idx = css.index(".form-input-actions {")
    fi_rule = css[fi_idx:fi_idx + 400]
    assert "flex-wrap: wrap" in fi_rule
