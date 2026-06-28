"""v1.15.93 — promote utility primitives (close design-system gaps).

Closes 6 of the 8 work-by-accident classnames flagged in the
v1.15.89 hygiene-lint allowlist. Pre-v1.15.93 these were used
in JS template literals but had no matching CSS rules — they
"worked" via parent inheritance for the most part but the STATUS
pills (CLEAN/DUPLICATE/NO MATCH/SKIPPED) and the VERIFY badge
were actually rendering as plain inline text instead of pills.

Primitives added:

* `.mono` — font-family monospace + tabular-nums for IDs
* `.small` — font-size: t-tiny for "muted small" patterns
* `.pill` — small inline badge base (distinct from .chip and .btn)
* `.pill-warn` — amber tone variant (VERIFY badge)
* `.btn-tone-ok` — green tone modifier (composes with .pill or .btn)
* `.btn-tone-muted` — muted gray tone modifier (composes likewise)

Note on lint coverage: btn-tone-ok and btn-tone-muted weren't
flagged by v1.15.89's lint because the JS sets them via template
interpolation (`class="pill ${tone[r.status]} small"`) which my
strict-shape regex skips. They're a separate silent-gap pattern
the lint should ideally catch — future enhancement.

## Tests

Static guards that each primitive's CSS rule exists with the
expected core property (font-family, font-size, display, color,
or border-color depending on what the primitive promises).
"""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = REPO / "app" / "web" / "static" / "app.css"


def _has_rule(class_name: str, expected_decl: str) -> bool:
    """Return True if .class_name { ... expected_decl ... } exists
    in app.css. Strips /* ... */ comments before searching to
    avoid false positives on narrative text."""
    css = APP_CSS.read_text()
    css_clean = re.sub(r"/\*.*?\*/", "", css, flags=re.DOTALL)
    # Find the rule body for the class.
    pattern = re.compile(
        rf"(^|\s)\.{re.escape(class_name)}\s*\{{([^}}]*)\}}",
        re.MULTILINE,
    )
    for m in pattern.finditer(css_clean):
        body = m.group(2)
        if expected_decl in body:
            return True
    return False


def test_mono_primitive_exists():
    """`.mono` must declare font-family: var(--font-mono).
    Used on the import-preview IMDB cell to ensure ID digits
    align even if a future stylesheet shifts the default font."""
    assert _has_rule("mono", "font-family: var(--font-mono)"), (
        "v1.15.93: `.mono` must declare `font-family: var(--font-mono)`. "
        "Was a silent gap until v1.15.93."
    )


def test_small_primitive_exists():
    """`.small` must declare font-size: var(--t-tiny). The class
    is used extensively in `class=\"muted small\"` patterns."""
    assert _has_rule("small", "font-size: var(--t-tiny)"), (
        "v1.15.93: `.small` must declare `font-size: var(--t-tiny)`. "
        "Pre-fix the class had no rule and depended on parent context."
    )


def test_pill_primitive_exists():
    """`.pill` is the base badge class. Pin the core declarations
    so a refactor doesn't accidentally remove what defines a
    'pill' visually (display inline + border + padding).

    v1.15.132: switched `display: inline-block` → `inline-flex`
    for chip-family glyph centering. Test checks for any
    inline-* display value since both shapes preserve the
    "inline badge" intent."""
    assert _has_rule("pill", "display: inline-flex") or \
           _has_rule("pill", "display: inline-block"), (
        "v1.15.93/.132: `.pill` must be inline-flex or inline-block "
        "(badge shape — sits inline like a span, not block-level)."
    )
    assert _has_rule("pill", "border:"), (
        "v1.15.93: `.pill` needs a `border:` declaration to render "
        "as a visible badge. Without it the STATUS pills + VERIFY "
        "badge render as plain text."
    )
    assert _has_rule("pill", "padding:"), (
        "v1.15.93: `.pill` needs padding so it doesn't look like "
        "inline text."
    )


def test_pill_warn_primitive_exists():
    """`.pill-warn` applies the amber tone to a pill. Used by
    the import-preview VERIFY badge."""
    assert _has_rule("pill-warn", "var(--amber)"), (
        "v1.15.93: `.pill-warn` must reference var(--amber) (the "
        "warning tone). Used by the import-preview VERIFY badge."
    )


def test_btn_tone_ok_primitive_exists():
    """`.btn-tone-ok` applies the green tone. Used by STATUS pill
    for CLEAN rows + any future ok-tone badges/buttons."""
    assert _has_rule("btn-tone-ok", "var(--green"), (
        "v1.15.93: `.btn-tone-ok` must reference var(--green) (the "
        "ok tone). Used by CLEAN status pills in the import preview."
    )


def test_btn_tone_muted_primitive_exists():
    """`.btn-tone-muted` applies muted gray. Used by STATUS pill
    for DUPLICATE / NO MATCH / SKIPPED rows."""
    assert _has_rule("btn-tone-muted", "var(--fg-dim)"), (
        "v1.15.93: `.btn-tone-muted` must reference var(--fg-dim). "
        "Used by DUPLICATE / NO MATCH / SKIPPED status pills."
    )


# ── Counter-guard: the v1.15.89 allowlist no longer carries these ──


def test_v1_15_89_allowlist_dropped_promoted_classes():
    """Once a class gets a real CSS rule, it must come OUT of the
    v1.15.89 hygiene-lint allowlist. v1.15.89's own counter-guard
    (test_allowlist_entries_lack_css_rules) catches this, but pin
    explicitly that the v1.15.93-promoted classes are absent so a
    rebase-merge can't accidentally re-add them."""
    from tests.test_v1_15_89_js_classname_hygiene_lint import ALLOWLIST
    promoted = ["mono", "small", "pill", "pill-warn"]
    for cls in promoted:
        assert cls not in ALLOWLIST, (
            f"v1.15.93: '{cls}' was promoted to a real CSS primitive "
            f"and must no longer appear in the v1.15.89 lint "
            f"allowlist. Remove from ALLOWLIST in "
            f"test_v1_15_89_js_classname_hygiene_lint.py."
        )
