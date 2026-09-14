"""v1.21.14 — LINK-column M badge size matches the filter chip / HL / C.

the user: the M in the LINK column didn't look like the M filter chip.
Root cause was a CSS cascade source-order bug, not a class mismatch —
the row M and the LINK filter-chip M both use
`.link-glyph .link-glyph-mismatch`, but `.link-glyph-mismatch` was
authored BEFORE the base `.link-glyph` rule. Same specificity → the
later base rule's `font-size: 14px` overrode the variant's `9px`, so the
row M rendered oversized (the chip M is forced to its filter-chip size
by the higher-specificity `.pill-filter-row .link-glyph`, so it looked
fine). The sibling variants (-hardlink, -copy) are authored AFTER the
base, so their 9px wins — M was the lone outlier.

Fix: relocate `.link-glyph-mismatch` to sit with the other variants,
after the base.
"""
from __future__ import annotations

import re
from pathlib import Path

from _slice_helpers import slice_between


REPO = Path(__file__).resolve().parent.parent
CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def test_mismatch_rule_defined_after_base_link_glyph():
    """The variant must come AFTER the base so its font-size:9px wins
    (same specificity → source order decides)."""
    base = CSS.index("\n.link-glyph {")
    variant = CSS.index("\n.link-glyph-mismatch {")
    assert variant > base, (
        "v1.21.14: .link-glyph-mismatch must be defined AFTER the base "
        ".link-glyph rule, or the base font-size:14px overrides the "
        "variant's 9px and the row M renders oversized"
    )


def test_mismatch_defined_once():
    assert CSS.count("\n.link-glyph-mismatch {") == 1


def _size(selector: str) -> dict[str, str | None]:
    block = slice_between(CSS, f"\n{selector} {{", "\n}")
    found = {p: re.search(rf"^\s*{p}:\s*([^;]+);", block, re.M) for p in ("font-size", "padding")}
    return {p: m.group(1).strip() if m else None for p, m in found.items()}


def test_mismatch_sized_like_the_other_link_variants():
    """Same font-size / padding as -hardlink and -copy so all row LINK
    badges are a uniform size."""
    # v0.51.343: compared with its siblings, not a retyped 9px, so the family moves together with --t-micro
    mismatch = _size(".link-glyph-mismatch")
    assert mismatch["font-size"] and mismatch["padding"], mismatch
    assert mismatch == _size(".link-glyph-hardlink") == _size(".link-glyph-copy"), mismatch


def test_version_bumped():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
