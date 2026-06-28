"""v1.15.98 — extend hygiene lint to class-mapping object literals.

The v1.15.96 enhancement caught static-prefix tokens around
`${...}` interpolations but missed class names that live INSIDE
JS object literals being interpolated. The btn-tone-* gap from
v1.15.93 escaped both v1.15.89 (static-only) and v1.15.96
(template-interpolation static prefixes):

  const tone = {
      clean: 'btn-tone-ok',
      conflict: 'btn-warn',
      duplicate: 'btn-tone-muted',
      ...
  };
  ...
  class="pill ${tone[r.status]} small"   ← the values weren't checked

v1.15.98 closes that gap with a two-step trace:

1. **Find class-mapping variables**: scan JS `class="..."`
   attributes for `${VAR[expr]}` / `${VAR.field}` / `${VAR}`
   interpolations. Capture each leading identifier (the
   variable name).
2. **Resolve their object-literal definitions**: for each
   captured variable, find a `const VAR = { ... }` /
   `let VAR = { ... }` / `var VAR = { ... }` definition.
   Use brace-matching (not regex) to extract the full body.
3. **Extract class-shape values**: scan the object body for
   string literals (`'name'` / `"name"`) that look like CSS
   classes (multi-component kebab, at least one hyphen).
4. **Check each value against CSS**: if a class-shape value
   has no matching CSS rule, the lint fails.

## Known limitations

* Only finds `const/let/var VAR = {...}` patterns. Doesn't
  track object-property assignments (`obj.field = '...'`),
  destructuring, or computed property names.
* Multi-component filter (`-` required) reduces false positives
  from single-word strings like 'movie' / 'download'. A
  single-component class like `.btn` won't be checked via
  this lint (it would surface via v1.15.89's static-attribute
  lint anyway when used directly).
* Only finds the FIRST `const/let/var VAR = {` in each file
  per variable. Multiple definitions or scoped redefinitions
  may slip past.

## What this would have caught

Pre-v1.15.93, the `tone` object had `clean: 'btn-tone-ok'` and
`btn-tone-ok` had NO CSS rule. The v1.15.98 lint would have
fired:

  tone: missing CSS for ['btn-tone-ok', 'btn-tone-muted']

Forcing the v1.15.93 work to happen at write time instead of
being caught after deployment.
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


def _css_classnames() -> set[str]:
    """Class selectors from CSS (comment-stripped)."""
    classes: set[str] = set()
    for cf in CSS_SOURCES:
        if not cf.exists():
            continue
        src = cf.read_text()
        src = re.sub(r"/\*.*?\*/", "", src, flags=re.DOTALL)
        for m in re.finditer(r"\.([a-zA-Z][a-zA-Z0-9_\-]*)", src):
            classes.add(m.group(1))
    return classes


def _class_interpolation_vars() -> set[str]:
    """Variable names that appear inside JS `class="..."`
    attribute interpolations. Captures the leading identifier
    of each `${VAR...}` segment."""
    var_names: set[str] = set()
    for jf in JS_SOURCES:
        if not jf.exists():
            continue
        src = jf.read_text()
        for m in re.finditer(r'class="[^"<>=]+"', src):
            body = m.group(0)
            for vm in re.finditer(
                r"\$\{([a-zA-Z_][a-zA-Z0-9_]*)",
                body,
            ):
                var_names.add(vm.group(1))
    return var_names


def _extract_object_body(src: str, var_name: str) -> str | None:
    """Find `const/let/var <var_name> = { ... }` in src and
    return the object body (between the matching braces).
    Returns None if not found."""
    pattern = re.compile(
        r"(?:const|let|var)\s+" + re.escape(var_name) + r"\s*=\s*\{"
    )
    m = pattern.search(src)
    if not m:
        return None
    start = m.end()
    depth = 1
    i = start
    while i < len(src) and depth > 0:
        c = src[i]
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
        i += 1
    if depth != 0:
        return None
    return src[start:i - 1]


def _class_mapping_objects() -> dict[str, set[str]]:
    """Return {var_name → set of class-shape values} for every
    variable used inside class= interpolations that has a
    resolvable object-literal definition with class-shape
    string values."""
    interp_vars = _class_interpolation_vars()
    out: dict[str, set[str]] = {}
    for jf in JS_SOURCES:
        if not jf.exists():
            continue
        src = jf.read_text()
        for var in interp_vars:
            body = _extract_object_body(src, var)
            if body is None:
                continue
            values: set[str] = set()
            # String literals that LOOK like CSS classes
            # (multi-component kebab, must contain a hyphen).
            for vm in re.finditer(
                r"""['"]([a-z][a-z0-9_]*-[a-z0-9_\-]+)['"]""",
                body,
            ):
                values.add(vm.group(1))
            if values:
                out.setdefault(var, set()).update(values)
    return out


# ── Main lint ─────────────────────────────────────────────


def test_class_mapping_object_values_have_css_rules():
    """For every variable used inside a `class="..."`
    interpolation that resolves to an object literal of
    class-shape string values, every value must have a
    matching CSS rule.

    This is the gap the v1.15.96 enhancement DIDN'T close:
    the static-prefix extraction caught the prefix of an
    interpolation but the interpolated VALUE itself (set
    via a class-mapping variable) slipped through. v1.15.98
    traces the variable to its object literal and checks
    its values."""
    css_classes = _css_classnames()
    mapped = _class_mapping_objects()
    failures: list[str] = []
    for var, values in mapped.items():
        missing = sorted(v for v in values if v not in css_classes)
        if missing:
            failures.append(f"  {var}: {missing}")
    assert not failures, (
        f"v1.15.98 class-mapping lint: {len(failures)} variable(s) "
        f"reference class-shape values without matching CSS rules:\n"
        + "\n".join(failures) + "\n\n"
        "Each value is a string in an object literal whose values "
        "get interpolated into a `class=` attribute. The CSS rule "
        "must exist. Options:\n"
        "  (1) Add a real CSS rule for each missing class\n"
        "  (2) Remove the value from the object if dead\n"
        "Pre-v1.15.98 these slipped past the lint because they\n"
        "weren't in static `class=` attributes — they reach the\n"
        "DOM via `class=\"${VAR[key]}\"` interpolation."
    )


def test_lint_finds_known_class_mapping_variable():
    """Self-check: the lint must detect at least one
    class-mapping variable (the `tone` object in app.js is the
    canonical example — used by the import-preview STATUS
    pill rendering). If the lint stops finding it, the
    variable-extraction logic is broken."""
    mapped = _class_mapping_objects()
    assert "tone" in mapped or len(mapped) > 0, (
        "v1.15.98 self-check failed: lint should detect at least "
        "one class-mapping variable (e.g., `tone` in app.js's "
        "import-preview renderer). If not, _class_interpolation_vars "
        "or _extract_object_body is broken."
    )


def test_synthetic_silent_gap_detected_by_v1_15_98_lint():
    """Self-check: simulate the pre-v1.15.93 state (btn-tone-ok
    missing from CSS) and verify the lint would have detected
    it. Regression-guard against the lint mechanism breaking."""
    css_classes = _css_classnames() - {"btn-tone-ok", "btn-tone-muted"}
    mapped = _class_mapping_objects()
    detected_missing = set()
    for var, values in mapped.items():
        for v in values:
            if v not in css_classes:
                detected_missing.add(v)
    assert "btn-tone-ok" in detected_missing, (
        "v1.15.98 self-check failed: the lint should detect "
        "btn-tone-ok as missing if it's removed from CSS. The "
        "_class_mapping_objects extraction is broken."
    )
