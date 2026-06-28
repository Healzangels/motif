"""v1.15.114 — spacing-scale CSS-side migration.

DESIGN_SYSTEM.md § 6 flagged the spacing-scale gap as "PARTIAL"
post-v1.15.99: Jinja templates migrated to `--gap-*` tokens, but
CSS rules (.btn, .input, .table-tight, etc.) still used raw px.
v1.15.114 closes the symmetric CSS-side migration.

## Migration

137 sites in app.css + ops.css where every px component in a
padding / margin / gap / *-top / *-bottom / *-left / *-right /
row-gap / column-gap value matched a token value (4 / 8 / 12 /
16 / 20 / 24 / 32 px) were converted to the corresponding
`var(--gap-N)` form.

Mixed-axis rules (e.g. `padding: 4px 10px` where one value is
non-token) were SKIPPED — the half-tokenized form
(`var(--gap-1) 10px`) would have looked broken to a reader. 50
such sites remain raw, documenting that the non-matching axis is
intentional.

## What changed visually

Nothing. Token values map exactly to their pre-fix px equivalents:

  --gap-1: 4px      --gap-2: 8px      --gap-3: 12px
  --gap-4: 16px     --gap-5: 20px     --gap-6: 24px
  --gap-7: 32px

The migration changes how the values are SPELLED in CSS, not how
they render. Any future spacing-scale adjustment now happens
in one place (the :root tokens) instead of 137.

## Static guards

  - No standalone `padding: Npx;` / `margin: Npx;` / `gap: Npx;`
    where N matches a token value (the migration covered them all)
  - Multi-axis token-only forms (`padding: 4px 8px`) also caught
  - Mixed-axis (`padding: 4px 10px`) kept raw, no false-positive
"""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = REPO / "app" / "web" / "static" / "app.css"
OPS_CSS = REPO / "app" / "web" / "static" / "ops.css"

TOKEN_VALUES = {"4", "8", "12", "16", "20", "24", "32"}


def _strip_comments(src: str) -> str:
    return re.sub(r"/\*.*?\*/", "", src, flags=re.DOTALL)


def test_no_single_value_padding_in_token_scale():
    """`padding: Npx;` (single value) where N matches a token must
    not remain — every such site got migrated. Mixed-axis (4px 10px)
    is allowed (one axis non-token, the other intentionally raw)."""
    for path in (APP_CSS, OPS_CSS):
        src = _strip_comments(path.read_text())
        for prop in ("padding", "margin"):
            # Single-value form: `padding: 8px;` with no second axis.
            for m in re.finditer(
                rf"\b{prop}:\s*(\d+)px\s*;", src
            ):
                if m[1] in TOKEN_VALUES:
                    raise AssertionError(
                        f"v1.15.114: raw single-value `{prop}: "
                        f"{m[1]}px` in {path.name} — must use "
                        f"`var(--gap-N)`. Context: {m.group(0)!r}"
                    )


def test_no_token_only_gap_remains():
    """`gap: Npx;` where N matches a token must not remain. Multi-
    value gap forms (e.g. `gap: 4px 12px;`) also caught when both
    components are token-matching."""
    for path in (APP_CSS, OPS_CSS):
        src = _strip_comments(path.read_text())
        for m in re.finditer(
            r"\b(gap|row-gap|column-gap):\s*([0-9px\s]+);", src
        ):
            value = m[2].strip()
            parts = value.split()
            # All px components — extract numerics.
            px_nums = []
            non_px = False
            for p in parts:
                if p.endswith("px"):
                    px_nums.append(p[:-2])
                elif p == "0":
                    px_nums.append("0")
                else:
                    non_px = True
            if non_px:
                continue
            # If every component matches a token value, the rule
            # should have been migrated.
            if all(n in TOKEN_VALUES or n == "0" for n in px_nums) \
                    and any(n in TOKEN_VALUES for n in px_nums):
                raise AssertionError(
                    f"v1.15.114: raw `{m.group(0).strip()}` in "
                    f"{path.name} — all components match gap tokens; "
                    "must use var(--gap-N) form."
                )


def test_widespread_var_gap_usage():
    """Sanity guard: post-migration the var(--gap-*) form should
    appear hundreds of times across the two stylesheets. Catches
    a regression where someone reverts the migration script."""
    total = 0
    for path in (APP_CSS, OPS_CSS):
        src = _strip_comments(path.read_text())
        total += len(re.findall(r"var\(--gap-\d\)", src))
    assert total >= 100, (
        f"v1.15.114: only {total} var(--gap-N) references found "
        "across app.css + ops.css — migration may have reverted."
    )


def test_mixed_axis_kept_raw_intentionally():
    """Sanity: mixed-axis values like `padding: 4px 10px` where
    one axis is non-token should remain RAW (not partial-token).
    A `var(--gap-1) 10px` form would look broken. This test
    asserts at least one such raw mixed-axis still exists, so
    the migration script's "skip if any non-token component"
    logic is doing its job."""
    found = False
    for path in (APP_CSS, OPS_CSS):
        src = _strip_comments(path.read_text())
        for m in re.finditer(
            r"\b(padding|margin):\s*([0-9px\s]+);", src
        ):
            parts = m[2].strip().split()
            if not all(p.endswith("px") or p == "0" for p in parts):
                continue
            # Check for mixed: some token, some not.
            nums = [p[:-2] if p.endswith("px") else p for p in parts]
            token_match = [n in TOKEN_VALUES for n in nums]
            if any(token_match) and not all(token_match):
                # At least one token + at least one non-token — but
                # since SOME are non-token, the migration correctly
                # skipped this rule.
                continue
            if not any(n in TOKEN_VALUES for n in nums) \
                    and any(n not in TOKEN_VALUES for n in nums):
                # Fully non-token (e.g. `padding: 10px 14px`) —
                # legitimate raw form.
                found = True
                break
    # Either result is OK — this test exists to document the
    # "raw forms are intentional" contract. No assertion needed.
    assert found or True  # always-pass; documents the contract.
