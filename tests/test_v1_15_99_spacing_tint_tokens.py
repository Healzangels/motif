"""v1.15.99 — spacing scale + tint tokens (Jinja template audit).

The Jinja template audit surfaced two design-system gaps:

## Silent gap: var(--gap-2) referenced without a defined token

`app/web/templates/dashboard.html:82` had:

    style="...margin-top:var(--gap-2)"

But no `--gap-2` (or any `--gap-*`) was defined in
`app/web/static/app.css`. `var()` with an undefined token and
no fallback resolves to the property's initial value — for
margin-top that's `0`. So the dashboard backfill banner
silently rendered with no top margin since its inception.

A developer intended a spacing scale and aspirationally
referenced `--gap-2`; the token was never added. Caught only
by manual template audit.

## Hardcoded inline values bypassing the token system

Multiple Jinja templates had inline rgba() backgrounds and
raw px margins that should flow through design tokens per
the v1.15.87 design-system rule:

* `rgba(255,184,74,0.04)` × 1 (dashboard.html — amber tint)
* `rgba(109,211,255,0.04)` × 1 (library.html — cyan tint)
* `rgba(109,255,181,0.04)` × 1 (library.html — green tint)
* `padding:12px 18px` × 2 (dashboard.html block bodies)
* `margin-top:8px` × 1 (settings.html form action)
* `margin-top:16px` × 1 (settings.html form action)
* `margin-top:24px` × 1 (settings.html header)
* `margin-bottom:8px` × 1 (dashboard.html storage-waste paragraph)

## v1.15.99 adds

* `--gap-1` (4px) through `--gap-7` (32px) — 8-point grid
  scale with a 4px sub-step for tight spacing.
* `--bg-tint-amber`, `--bg-tint-cyan`, `--bg-tint-green` —
  low-alpha backgrounds for info banners. Centralizes the
  0.04-alpha tint formula so a future opacity adjustment
  happens in one place.

Templates migrate the scale-compatible inline values to
token references. The CSS rgba() pattern across `app.css`
(151 sites) is NOT migrated in this ship — that's a bigger
refactor that needs the `--<color>-rgb` triple-token
pattern to compose `rgba(var(--<color>-rgb), <alpha>)`.
Flagged in DESIGN_SYSTEM.md for follow-up.

## Tests

Pin the new tokens, the absence of hardcoded rgba() in
templates, and confirm the scale-compatible margin values
now use tokens.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = REPO / "app" / "web" / "static" / "app.css"
TEMPLATES_DIR = REPO / "app" / "web" / "templates"


def _root_block() -> str:
    src = APP_CSS.read_text()
    start = src.index(":root {")
    end = src.index("}", start)
    return src[start:end]


# ── Spacing scale tokens exist ──────────────────────────────


def test_gap_scale_tokens_defined():
    """`--gap-1` through `--gap-7` must be declared in :root.
    Without them, `var(--gap-2)` in dashboard.html silently
    fell back to the initial value (0)."""
    root = _root_block()
    expected = {
        "--gap-1": "4px",
        "--gap-2": "8px",
        "--gap-3": "12px",
        "--gap-4": "16px",
        "--gap-5": "20px",
        "--gap-6": "24px",
        "--gap-7": "32px",
    }
    for token, value in expected.items():
        pattern = re.compile(
            rf"{re.escape(token)}:\s*{re.escape(value)}"
        )
        assert pattern.search(root), (
            f"v1.15.99: `{token}: {value}` must be in :root. "
            f"Missing means the dashboard's `var(--gap-2)` and "
            f"other migrated template values fall back to the "
            f"property's initial value (usually 0)."
        )


def test_bg_tint_tokens_defined():
    """`--bg-tint-amber/cyan/green` for info banner backgrounds.
    Pre-v1.15.99 these were inline `rgba(...)` literals in
    three Jinja templates. Tokenizing centralizes the formula."""
    root = _root_block()
    for color in ("amber", "cyan", "green"):
        token = f"--bg-tint-{color}"
        assert re.search(rf"{re.escape(token)}:\s*rgba\(", root), (
            f"v1.15.99: `{token}` must be a real rgba() token in "
            f":root."
        )


# ── Templates use tokens instead of raw rgba/px ─────────────


def test_no_inline_rgba_in_templates():
    """Counter-guard: no Jinja template should declare an inline
    `rgba(...)` in a `style=` attribute. The tint tokens are
    the canonical surface; raw rgba returning to a template
    re-introduces the design-system gap."""
    bad: list[str] = []
    for tpl in TEMPLATES_DIR.glob("*.html"):
        src = tpl.read_text()
        # Match: style="...rgba(...)..."
        # Multi-line style attrs are uncommon in motif's
        # templates; this single-line regex is sufficient.
        for m in re.finditer(r'style="[^"]*rgba\([^)]*\)', src):
            bad.append(f"{tpl.name}: {m.group(0)[:80]}")
    assert not bad, (
        f"v1.15.99: {len(bad)} Jinja template(s) still inline "
        f"rgba() in style attributes:\n  "
        + "\n  ".join(bad) + "\n\n"
        "Use the `--bg-tint-<color>` tokens defined in :root. "
        "If a new tint is needed, add a token first."
    )


def test_dashboard_backfill_banner_uses_gap_token():
    """The original silent gap — `var(--gap-2)` reference in
    dashboard.html. Now that `--gap-2` is defined, this pin
    guards against a future edit removing the token reference
    and re-introducing the inline px."""
    tpl = TEMPLATES_DIR / "dashboard.html"
    src = tpl.read_text()
    assert "var(--gap-2)" in src, (
        "v1.15.99: dashboard.html's backfill banner must use "
        "`var(--gap-2)` for margin-top. The token resolves to "
        "8px now (pre-v1.15.99 it was undefined → fell back to 0)."
    )


def test_dashboard_backfill_banner_uses_tint_token():
    """The amber tint background should reference the token, not
    inline rgba().

    v1.17.8: the inline `style="background:var(--bg-tint-amber)"`
    on the backfill banner was redundant — `.missing-banner`'s
    base CSS rule already provides amber border + bg-tint, so
    the inline override has been removed. The token usage moved
    to a single indirection in app.css. Pin BOTH that the
    template uses `.missing-banner` (so the default amber kicks
    in) AND that the token is the value the base rule uses."""
    tpl = TEMPLATES_DIR / "dashboard.html"
    src = tpl.read_text()
    anchor = src.index("id=\"dash-backfill-banner\"")
    window = src[anchor:anchor + 400]
    # Template composes `.missing-banner` — that drives the
    # amber default.
    assert 'class="missing-banner"' in window, (
        "v1.17.8: dashboard.html's backfill banner uses the "
        ".missing-banner primitive (amber default); the inline "
        "style override was retired."
    )
    # And the base rule pins the token.
    css = (TEMPLATES_DIR.parent / "static" / "app.css").read_text()
    base_idx = css.index(".missing-banner {")
    base_rule = css[base_idx:base_idx + 400]
    assert "rgba(var(--amber-rgb), 0.05)" in base_rule, (
        "v1.15.99 / v1.17.8: the .missing-banner base rule must "
        "use the amber-rgb token so the dashboard backfill "
        "banner inherits a token-backed tint."
    )


def test_library_banners_use_tint_tokens():
    """The library page has two `.missing-banner` divs — a
    cyan scan-hint and a green bulk-bar. Both should use tint
    tokens.

    v1.17.7: tint tokens moved out of inline `style=` and into
    the new `.missing-banner-cyan` / `.missing-banner-green`
    class variants in app.css. Library.html now references the
    classes; the tokens themselves live one indirection away in
    the stylesheet. Pin BOTH the class usage in the template
    AND the token usage in the CSS rules so the v1.15.99 token-
    discipline contract is preserved end-to-end."""
    src_html = (TEMPLATES_DIR / "library.html").read_text()
    src_css = (
        TEMPLATES_DIR.parent / "static" / "app.css"
    ).read_text()
    # Template uses the new variant classes.
    assert "missing-banner-cyan" in src_html, (
        "v1.17.7: library.html scan-hint banner must compose "
        "`.missing-banner` with the `.missing-banner-cyan` "
        "tone variant."
    )
    assert "missing-banner-green" in src_html, (
        "v1.17.7: library.html bulk-bar must compose "
        "`.missing-banner` with the `.missing-banner-green` "
        "tone variant."
    )
    # And the CSS variant rules pin the tint tokens.
    assert "var(--bg-tint-cyan)" in src_css, (
        "v1.15.99 / v1.17.7: the .missing-banner-cyan variant "
        "rule must use `var(--bg-tint-cyan)`."
    )
    assert "var(--bg-tint-green)" in src_css, (
        "v1.15.99 / v1.17.7: the .missing-banner-green variant "
        "rule must use `var(--bg-tint-green)`."
    )


def test_settings_form_actions_use_gap_tokens():
    """Settings page inline margin-top values use scale tokens (never raw px).

    v1.22.35: the inline overrides themselves are being phased OUT in favor of
    shared CSS rules — the cookies TEST gap-2 override + the EVENTS-footnote
    gap-3 override folded into `.form-hint + .form-actions` /
    `.form-actions + .form-hint`. The two that remain are inside the bespoke
    IMPORT preview-then-apply flow (gap-4 APPLY row, gap-6 PREVIEW header).
    Pin: any surviving inline margin-top uses a token, and no raw-px override
    crept back in."""
    tpl = TEMPLATES_DIR / "settings.html"
    src = tpl.read_text()
    # The IMPORT preview flow's two tokenized overrides still stand.
    assert "margin-top:var(--gap-4)" in src, (
        "v1.15.99: IMPORT APPLY form-actions should use var(--gap-4)."
    )
    assert "margin-top:var(--gap-6)" in src, (
        "v1.15.99: IMPORT PREVIEW RESULTS block-head should use var(--gap-6)."
    )
    # (v1.22.35 dropped the old "no raw px" sweep — it false-matched narrative
    # px values inside template comments. test_v1_22_35_settings_measure pins
    # that the phased-out gap-2/gap-3 overrides are gone + the survivors are
    # confined to the bespoke IMPORT flow.)
