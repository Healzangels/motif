"""v0.51.331: badge ink centred.

The operator, from the live anime tab: "the AT isn't centered in its
box … or the A for adopted". Measured in the browser: the TEXT BOX of
every .link-badge was centred exactly (6px each side), but
letter-spacing trails the last glyph too, so the box carried 0.15em of
dead space on its right and the ink sat ~half of that left of centre —
on every badge (T/U/AT/A/M/P) and every SRC key button, not just AT.

Fix = the v1.12.30 .link-glyph idiom: the right padding gives the
trailing letter-spacing back. This guards the INVARIANT (right padding
== left padding minus the rule's letter-spacing) for both sites, not
the literal, so a retuned letter-spacing that forgets the padding fails
here.
"""
from __future__ import annotations

import re
from pathlib import Path

from _slice_helpers import slice_between

REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def _block(selector: str) -> str:
    return slice_between(APP_CSS, f"\n{selector} {{", "\n}")


def _decl(block: str, prop: str) -> str:
    m = re.search(rf"^\s*{re.escape(prop)}:\s*([^;]+);", block, re.M)
    assert m, f"{prop} not declared in block"
    return m.group(1).strip()


def _padding_lr(block: str) -> tuple[str, str]:
    """(right, left) of a 4-value padding shorthand."""
    parts = re.split(r"\s+(?![^()]*\))", _decl(block, "padding"))
    assert len(parts) == 4, f"expected a 4-value padding, got {parts}"
    return parts[1], parts[3]


def test_badge_letter_spacing_is_the_shared_value():
    assert _decl(_block(".link-badge"), "letter-spacing") == "0.15em"


def test_link_badge_right_padding_gives_the_trailing_spacing_back():
    block = _block(".link-badge")
    ls = _decl(block, "letter-spacing")
    right, left = _padding_lr(block)
    assert right == f"calc({left} - {ls})", (right, left, ls)


def test_src_key_button_takes_the_badge_padding():
    """Every SRC key button also carries .link-badge, whose later rule
    wins by source order at equal specificity. The `2px 8px` the key
    button declared since v1.11.66 was dead; it must stay gone so the
    badge rule is the one owner of the compensated padding."""
    block = _block(".src-key-btn")
    assert not re.search(r"^\s*(padding|letter-spacing)(-\w+)?:", block, re.M), block
    assert APP_CSS.index("\n.src-key-btn {") < APP_CSS.index("\n.link-badge {"), "source order is what makes the badge rule win"
    for tpl in (REPO / "app" / "web" / "templates").glob("*.html"):
        for line in tpl.read_text().splitlines():
            if "src-key-btn" in line and 'class="' in line:
                assert "link-badge" in line, (tpl.name, line.strip())


def test_v0_51_331_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "0.51.331: badge ink centred" in init_py
