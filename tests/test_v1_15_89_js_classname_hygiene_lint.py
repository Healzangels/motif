"""v1.15.89 — JS classname hygiene lint (the user's design-system rule, follow-up B).

Forward-looking insurance against the bug class that drove
the user's v1.15.87 design-system rule. Two recent silent-gap
incidents this catches:

* v1.15.66 — JS template literals rendered `class="src-pill src-X"`
  but `.src-pill` had no CSS rule (invented class name). The SRC
  letter pills rendered colorless. Fixed in v1.15.71 by switching
  to the canonical `.link-badge-*` family.
* v1.15.87 — JS template literals rendered `class="input input-tiny"`
  on the import-preview action select, but `.input-tiny` had no
  CSS rule. The select silently inherited only `.input`'s 8/12
  padding (sized for full-width dialog inputs). v1.15.88 promoted
  `.input-tiny` to a real primitive.

Both were caught only by visual inspection after deployment. The
lint catches the pattern at test time.

## What the lint checks

1. Scan JS files (app.js, ops.js, dashboard-customize.js) for
   `class="..."` literals with strict-shape content (alphanumeric +
   hyphen + space only — no `${`, no Jinja, no quotes inside).
2. Tokenize by space → individual class names.
3. Scan CSS files (app.css, ops.css) for class selectors `.X`.
4. Diff: any JS class with no CSS selector + not in the allowlist
   fails the test.

## What's allowlisted (and why)

Each entry has a reason. Categories:

* **JS state hooks** — classes added/removed via classList for
  state but never styled (toggle drives JS behavior, not CSS).
* **Retired CSS rules kept as JS selector hooks** — v1.15.76
  explicitly removed `.import-info-btn` CSS while JS kept the
  class for click-delegation. Comment in app.css documents the
  intent.
* **Documented gaps** — utility primitives the JS uses that
  WOULD benefit from real CSS rules (`.mono`, `.small`, `.pill`,
  etc.). Tracked in docs/DESIGN_SYSTEM.md § 6 for future
  cleanup; allowlisted here so the lint can ship without
  blocking on a refactor.

Adding entries to the allowlist requires a reason in the comment
above the entry. Anyone removing one should verify the class
either has a CSS rule now or has been removed from the JS.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent

JS_SOURCES = [
    REPO / "app" / "web" / "static" / "app.js",
    REPO / "app" / "web" / "static" / "ops.js",
    REPO / "app" / "web" / "static" / "dashboard-customize.js",
]
CSS_SOURCES = [
    REPO / "app" / "web" / "static" / "app.css",
    REPO / "app" / "web" / "static" / "ops.css",
]


# Allowlist of class tokens used in JS but intentionally not in
# CSS. Each MUST have a reason comment.
ALLOWLIST: dict[str, str] = {
    # ── JS selector hooks (no CSS expected) ───────────────────
    "import-info-btn": (
        "v1.15.76 retired the .import-info-btn CSS rule; the class "
        "is kept on the JS template literal as a click-delegation "
        "selector hook only. Documented in app.css ~line 2572."
    ),
    # ── JS state classes / structural-only marker classes ─────
    "history-section-empty": (
        "JS-only state marker on the history-section element when "
        "empty (used by the structural <details> rendering). No "
        "visual styling distinguishes it from the non-empty state."
    ),
    # ── Documented design-system gaps (tracked in DESIGN_SYSTEM.md § 6) ──
    # v1.15.93 promoted .mono, .small, .pill, .pill-warn to real
    # primitives — their entries moved out of this allowlist. The
    # `test_allowlist_entries_lack_css_rules` counter-guard would
    # catch any of them coming back here after a CSS rule lands.
    # v1.15.112 promoted .op-card-cancel-note + .op-card-meta-value
    # to real primitives in ops.css. Entries removed.
    # ── v1.15.96 enhancement surfaced BEM-style namespace base ──
    "recovery-option": (
        "v1.15.96: BEM-style namespace base for the recovery "
        "card's option rows (info card's // TRY THIS NEXT). The "
        "variants .recovery-option-btn / .recovery-option-info / "
        ".recovery-option-disabled / .recovery-option-label / "
        ".recovery-option-tip carry all the styling; the bare "
        "base is a structural / semantic marker. Add a real "
        "rule (e.g., display:flex for option-row layout) if a "
        "shared base behavior is needed."
    ),
}


def _extract_js_classnames() -> set[str]:
    """Static class tokens from JS files.

    v1.15.89 (original): matched only purely-static `class="..."`
    attributes where the body contained alphanumeric + hyphen
    + space only.

    v1.15.96 enhancement: also handles template-literal
    interpolation in mixed attributes like `class="static ${var}
    more-static"`. Strips the `${...}` segments and keeps the
    surrounding static tokens. Skips tokens that end in `-` or
    `_` since those are prefix-of-dynamic patterns (e.g.,
    `class="event-level event-level-${level}"` yields
    `event-level-` as a prefix — not a real complete class
    name on its own).

    Limitation: doesn't track class-like values inside JS
    object literals (e.g., `tone = {clean: 'btn-tone-ok'}`).
    Those slip through the regex-based extraction because the
    string literal isn't in a `class=` attribute. Future
    enhancement could scan for known class-mapping object names
    and pull their values, but that's project-specific
    heuristics with false-positive risk.
    """
    classes: set[str] = set()
    for jf in JS_SOURCES:
        if not jf.exists():
            continue
        src = jf.read_text()
        # Match class="..." attributes with broader content that
        # may contain `${...}` interpolations. Still reject
        # bodies with chars that suggest non-attribute matches
        # (=, <, >, double-quotes inside) so we don't false-match
        # JS code that happens to contain the substring class=.
        for m in re.finditer(r'class="([^"<>=]+)"', src):
            body = m.group(1)
            # Strip ${...} interpolation segments — keep only
            # the static tokens around them.
            static_body = re.sub(r"\$\{[^}]*\}", " ", body)
            for token in static_body.split():
                # Skip incomplete prefix-of-dynamic tokens
                # (end in - or _) — they're meant to be
                # completed by an interpolated suffix and
                # aren't real complete class names.
                if token.endswith("-") or token.endswith("_"):
                    continue
                if re.fullmatch(r"[A-Za-z][A-Za-z0-9_\-]*", token):
                    classes.add(token)
    return classes


def _extract_css_classnames() -> set[str]:
    """Class selectors from CSS files. Strip /* ... */ comments
    first so commented-out class names don't count as defined."""
    classes: set[str] = set()
    for cf in CSS_SOURCES:
        if not cf.exists():
            continue
        src = cf.read_text()
        src = re.sub(r"/\*.*?\*/", "", src, flags=re.DOTALL)
        for m in re.finditer(r"\.([a-zA-Z][a-zA-Z0-9_\-]*)", src):
            classes.add(m.group(1))
    return classes


# ── Main lint ────────────────────────────────────────────────


def test_every_js_class_has_a_css_rule_or_is_allowlisted():
    """The core hygiene lint. For every class token used in
    static `class="..."` attributes in JS files, assert that
    either:
      (a) A CSS rule `.X { ... }` exists in app.css or ops.css, or
      (b) The class is in ALLOWLIST with a documented reason.

    Failures here mean a JS-only class name was introduced
    without a matching CSS rule — the v1.15.66 (.src-pill) and
    v1.15.87 (.input-tiny) silent-gap pattern. Fix one of:
      * Add a real CSS rule for the class (preferred).
      * Confirm the class is intentionally JS-only and add it
        to ALLOWLIST above with a reason.
      * Remove the class from the JS if it's dead.
    """
    js_classes = _extract_js_classnames()
    css_classes = _extract_css_classnames()
    unmatched = js_classes - css_classes
    not_allowlisted = unmatched - ALLOWLIST.keys()
    if not_allowlisted:
        listing = "\n".join(sorted(f"  - {c}" for c in not_allowlisted))
        raise AssertionError(
            f"v1.15.89 hygiene lint: {len(not_allowlisted)} JS class "
            f"name(s) reference no CSS rule and are not in ALLOWLIST:\n"
            f"{listing}\n\n"
            "Each silent-gap class was the root cause of a visual bug "
            "in the v1.15.66-87 cascade (.src-pill rendered colorless; "
            ".input-tiny silently inherited wrong padding). Either:\n"
            "  (1) add a real CSS rule for the class in app.css\n"
            "  (2) add the class to ALLOWLIST with a reason\n"
            "  (3) remove the class from the JS template literal"
        )


# ── Counter-guards on the allowlist's hygiene ────────────────


def test_allowlist_entries_are_actually_used_in_js():
    """If an allowlist entry no longer appears in JS, remove it
    from the allowlist — otherwise the lint quietly accepts
    pretend-allowed classes that aren't real. Keeps the allowlist
    honest as JS evolves."""
    js_classes = _extract_js_classnames()
    stale = [c for c in ALLOWLIST if c not in js_classes]
    assert not stale, (
        f"v1.15.89: allowlist has stale entries no longer used in JS: "
        f"{stale}. Remove them from ALLOWLIST in this test file."
    )


def test_allowlist_entries_lack_css_rules():
    """Counter-guard: if an allowlisted class GAINS a CSS rule
    later, it shouldn't still be allowlisted. The allowlist exists
    for classes that are intentionally JS-only OR documented gaps;
    once a real rule is added, the class belongs in the normal
    matched-set, not the allowlist."""
    css_classes = _extract_css_classnames()
    promoted = [c for c in ALLOWLIST if c in css_classes]
    assert not promoted, (
        f"v1.15.89: allowlist entries now have CSS rules — remove "
        f"them from ALLOWLIST: {promoted}. The lint will catch them "
        "via the normal css_classes set after removal."
    )


def test_lint_detects_known_pre_v1_15_88_silent_gap_pattern():
    """Synthetic self-check: confirm the lint mechanism actually
    detects the silent-gap pattern. Simulates the pre-v1.15.88
    state by removing `.input-tiny` from the matched-set and
    asserting it would have failed without the allowlist."""
    js_classes = _extract_js_classnames()
    css_classes = _extract_css_classnames() - {"input-tiny"}
    unmatched = js_classes - css_classes
    not_allowlisted = unmatched - ALLOWLIST.keys()
    assert "input-tiny" in not_allowlisted, (
        "v1.15.89 self-check failed: the lint should detect "
        "input-tiny as missing if it's not in CSS and not in the "
        "allowlist. If this assertion fails, the lint's extraction "
        "logic is broken — fix before relying on the main test."
    )
