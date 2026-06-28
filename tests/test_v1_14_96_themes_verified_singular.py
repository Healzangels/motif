"""v1.14.96 — `1 themes verified` → `1 theme verified` (pluralization).

the user: "what in this row 09:22:48 PM 'Anime': 0 new, 1231
updated, 1 themes verified — what is 1 themes verified mean?
also if thats accurate can we make it not plural when just 1"

The count is rows where motif HEAD-probed Plex's
`theme="..."` claim to confirm it's real (not stale Plex
metadata cache). Targets `has_theme=1 AND
local_theme_file=0` rows. Cached for 30 days (success) /
7 days (failure) so steady-state cost is near zero —
verified_n is typically 0 (or low single digits) per
section after the first verify pass.

Pre-fix the suffix was hardcoded `f"{verified_n} themes
verified"` so a single verification rendered as
`1 themes verified`.

## Fix

Pick `theme` vs `themes` based on verified_n. Singular
when ==1, plural otherwise. Keep the suffix off entirely
when verified_n == 0 (the existing gate).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
PLEX_ENUM_PY = REPO / "app" / "core" / "plex_enum.py"


def test_pluralization_picks_singular_word_for_one():
    """The activity string-builder must pick `theme` (singular)
    when verified_n == 1. Pre-fix every count rendered as
    `themes`."""
    src = PLEX_ENUM_PY.read_text()
    # Anchor on the v1.14.96 marker.
    anchor = src.index("v1.14.96: pluralize the verified-count suffix")
    block = src[anchor:anchor + 1000]
    # The selection logic.
    assert (
        '"theme" if verified_n == 1 else "themes"' in block
    ), "Pluralization must pick theme/themes based on verified_n"


def test_activity_string_uses_the_pluralization_variable():
    """The f-string suffix must interpolate the chosen word —
    NOT a hardcoded `themes` literal that would defeat the
    pluralization."""
    src = PLEX_ENUM_PY.read_text()
    anchor = src.index("v1.14.96: pluralize the verified-count suffix")
    # Walk down to the activity= keyword arg's f-string.
    block = src[anchor:anchor + 1500]
    assert "{verified_word} verified" in block, (
        "Activity string must use the pluralization variable, "
        "not a hardcoded `themes` literal"
    )
    # Pre-fix shape must be gone.
    assert "{verified_n} themes verified" not in block, (
        "Pre-fix hardcoded `themes` literal must be removed"
    )


def test_zero_gate_preserved():
    """When verified_n == 0 the suffix stays OFF entirely (not
    `0 themes verified`). The existing `if verified_n` guard
    keeps that behavior; pin it so the pluralization rewrite
    didn't accidentally drop the gate."""
    src = PLEX_ENUM_PY.read_text()
    anchor = src.index("v1.14.96: pluralize the verified-count suffix")
    block = src[anchor:anchor + 1500]
    assert "if verified_n" in block, (
        "Zero-gate must remain — empty suffix when no verifications "
        "happened, not `0 themes verified`"
    )
