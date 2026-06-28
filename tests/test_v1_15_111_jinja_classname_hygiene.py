"""v1.15.111 — Jinja template class-hygiene lint.

Sibling to v1.15.89's JS class-hygiene lint. The JS lint scans
`class="..."` literals in app.js / ops.js / dashboard-customize.js
and diffs against CSS. The Jinja templates were never lint-scoped
— this test extends coverage so SSR-side invented class names get
caught at test time.

## Initial scan (2026-05-18)

Scanning every `<...>` template under `app/web/templates/` and
comparing class tokens against `.X` selectors in app.css + ops.css
found 3 invented classes:

  - `stat-label` (13 uses in dashboard.html)
    → v1.15.111: promoted to real primitive (`.stat-label
      { flex: 0 1 auto; }`)
  - `cache-gauge-total` (1 use in settings.html)
    → v1.15.111: removed; the id="cache-gauge-total" was
      load-bearing for JS, the class was redundant
  - `dash-customize-line` (1 use in dashboard.html)
    → v1.15.111: removed; `.muted .small` already styled it,
      class was a structural-only marker with no behavior

Post-fix the diff should be empty. New invented classes added
in templates fail the lint until they get a real CSS rule OR
are explicitly allowlisted.

## Allowlist

None at initial scan — the 3 above were resolved in v1.15.111.
Future structural-only marker classes can be added here with a
reason comment (same convention as the v1.15.89 lint).
"""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
TEMPLATE_DIR = REPO / "app" / "web" / "templates"
CSS_SOURCES = [
    REPO / "app" / "web" / "static" / "app.css",
    REPO / "app" / "web" / "static" / "ops.css",
]


ALLOWLIST: dict[str, str] = {
    # ── No initial allowlist entries. v1.15.111 resolved all
    #    three invented-class sites by promoting `.stat-label`
    #    to a real primitive and removing two redundant
    #    structural markers. Add entries here when a future
    #    structural-only marker class is needed in templates.
}


def _collect_css_classes() -> set[str]:
    classes: set[str] = set()
    for f in CSS_SOURCES:
        for m in re.finditer(r"\.([A-Za-z][\w-]*)", f.read_text()):
            classes.add(m.group(1))
    return classes


def _collect_template_classes() -> set[str]:
    classes: set[str] = set()
    for f in TEMPLATE_DIR.glob("*.html"):
        src = f.read_text()
        # Strip HTML + Jinja comments — comment bodies are not
        # rendered to the DOM so any `class="..."` inside doesn't
        # need a CSS rule.
        src = re.sub(r"<!--.*?-->", "", src, flags=re.DOTALL)
        src = re.sub(r"\{#.*?#\}", "", src, flags=re.DOTALL)
        for m in re.finditer(r'class="([^"]+)"', src):
            body = m.group(1)
            # Strip Jinja interpolation — `class="x {{ active }} y"`
            # tokens still produce x and y as static parts.
            body = re.sub(r"\{\{[^}]+\}\}", "", body)
            body = re.sub(r"\{%[^%]+%\}", "", body)
            for tok in body.split():
                if not tok:
                    continue
                # Reject tokens containing non-class chars (e.g.
                # leftover `${...}` from JS-side rendering).
                if not re.fullmatch(r"[A-Za-z][\w-]*", tok):
                    continue
                classes.add(tok)
    return classes


def test_jinja_template_classes_have_css_rules():
    """Every class referenced in a Jinja template's `class="..."`
    must have a `.X` selector in app.css or ops.css — OR be in
    the ALLOWLIST with a reason."""
    css = _collect_css_classes()
    template = _collect_template_classes()
    missing = sorted(t for t in (template - css) if t not in ALLOWLIST)
    assert not missing, (
        "v1.15.111: Jinja templates reference classes with no CSS "
        "rule and no allowlist entry. Each must either get a real "
        "CSS rule (preferred) or be allowlisted with a reason "
        "comment. Missing: "
        f"{missing}"
    )


def test_allowlist_entries_are_still_in_templates():
    """If a class is allowlisted but no longer appears in any
    template, drop the entry — stale allowlist drift accumulates
    cruft. Matches v1.15.89's lint hygiene contract."""
    if not ALLOWLIST:
        return  # nothing to check
    template = _collect_template_classes()
    stale = [c for c in ALLOWLIST if c not in template]
    assert not stale, (
        f"v1.15.111: allowlist entries no longer used in any "
        f"template — remove: {stale}"
    )
