"""v1.15.96 — extend v1.15.89 hygiene lint to mixed-attribute interpolation.

The v1.15.89 lint scanned only purely-static `class="..."` attributes —
template-literal interpolation patterns slipped past it:

  class="event-level event-level-${e.level}"   ← skipped entirely
  class="pill ${tone[r.status]} small"         ← skipped entirely
  class="kind-${kind}"                         ← skipped entirely

This let `.btn-tone-ok` / `.btn-tone-muted` reach v1.15.93's
visual-bug state (STATUS pills rendered as plain text) without
the lint catching them — the lint missed the entire template-
literal class= attribute because of the `$` character.

v1.15.96 enhancement:
* Broader regex matches `class="..."` with `${...}` segments
  inside the body.
* Strips the `${...}` interpolation segments before tokenizing.
* Filters out trailing-hyphen tokens (`event-level-`) which are
  prefix-of-dynamic patterns — not real complete class names.
* Keeps the surrounding static tokens (`event-level` is
  checkable; `event-level-` is not).

## Known limitation

The enhancement DOESN'T catch class names that live inside
JS object literals being interpolated:

  const tone = { clean: 'btn-tone-ok', conflict: 'btn-warn' };
  ...
  class="pill ${tone[r.status]} small"

The string `'btn-tone-ok'` isn't inside a `class=` attribute,
so the regex extraction can't see it. Catching this pattern
would need AST-level analysis or project-specific heuristics.
Documented as a v1.15.96 limitation; addressing it can be a
future enhancement.

## What v1.15.96 surfaced

Running the enhanced lint produced exactly one new unmatched
class: `.recovery-option` — a BEM-style namespace base used
alongside `.recovery-option-*` variants in the info card's
TRY THIS NEXT section. The variants carry all the styling;
the base is a structural marker. Added to the lint allowlist
with a reason note.

## Tests

This file adds self-checks confirming the enhanced extraction
handles the new patterns correctly. The v1.15.89 lint's own
tests (the main hygiene check + the counter-guards) continue
to run on the enhanced extraction — passing them means the
allowlist accounts for everything the new regex surfaces.
"""

from __future__ import annotations

from tests.test_v1_15_89_js_classname_hygiene_lint import (
    _extract_js_classnames,
)


def test_enhanced_lint_extracts_static_parts_of_mixed_attributes():
    """Concrete: `event-level` (the static prefix of
    `class="event-level event-level-${e.level}"`) is extracted
    by the v1.15.96 enhancement. Pre-v1.15.96 the entire
    attribute was skipped because the body contained `$`."""
    js_classes = _extract_js_classnames()
    assert "event-level" in js_classes, (
        "v1.15.96: the enhanced extraction should pick up "
        "'event-level' from class=\"event-level event-level-"
        "${e.level}\". If it doesn't, the regex broadening "
        "didn't take effect."
    )


def test_enhanced_lint_skips_prefix_of_dynamic_tokens():
    """Counter-guard: tokens that end in `-` or `_` are
    incomplete prefixes (e.g. `event-level-` from `event-level-
    ${var}`) — they're not real class names. The lint must
    skip them to avoid false-positive failures on every
    interpolated attribute."""
    js_classes = _extract_js_classnames()
    # If `event-level-` (trailing hyphen) appears in the set,
    # the prefix-filter isn't working.
    assert "event-level-" not in js_classes
    assert "kind-" not in js_classes
    assert "source-pie-" not in js_classes


def test_enhanced_lint_still_catches_purely_static_classes():
    """Regression-guard: the v1.15.96 broadening shouldn't
    accidentally drop purely-static class= attributes. Spot-
    check a known-static class continues to appear."""
    js_classes = _extract_js_classnames()
    # `.muted` is a heavily-used static class everywhere.
    assert "muted" in js_classes, (
        "v1.15.96 regression: heavily-used static class 'muted' "
        "no longer surfaces. The regex broadening broke purely-"
        "static matching."
    )
    # `.btn` likewise.
    assert "btn" in js_classes


def test_enhanced_lint_catches_recovery_option_namespace_base():
    """The v1.15.96 enhancement surfaced `recovery-option` as a
    new lint hit (BEM-style namespace base, no CSS rule). It
    should appear in the JS classes set so it's visible to the
    main lint test and routed through the allowlist."""
    js_classes = _extract_js_classnames()
    assert "recovery-option" in js_classes, (
        "v1.15.96: should detect `recovery-option` as a JS class "
        "(used in `class=\"recovery-option recovery-option-info"
        "${disabledClass}\"`)."
    )
