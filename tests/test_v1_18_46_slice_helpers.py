"""v1.18.46 — slice helper utility + refactor of recently-widened tests.

The fixed-window slicing pattern (`src[idx:idx + N]`) used across
~944 source-pin sites recurs as a maintenance tax: every comment
addition between the anchor and the expected substring can push
the substring past the window's end, requiring a "widen the
window" fix.

v1.18.46 adds `tests/_slice_helpers.py` with anchor-based
slicing utilities (`slice_to_next` + `slice_between`) and
refactors 5 tests that have been widened multiple times this
session as proof-of-concept.

## Refactored

  - test_v1_14_27 (12000 → 24000 → 28000 across v1.18.36-38)
  - test_v1_17_6 (2000 → 3000 in v1.18.44)
  - test_v1_18_23 (500 → 1200 in v1.18.28)
  - test_v1_18_28 (1200 → 2200 in v1.18.29)
  - test_v1_18_36 (24000 → 28000 in v1.18.38)
  - test_v1_18_38 (2500 → 5500 → 8000 in same tag)

Future tests should prefer the helper; existing fixed-window
slices that haven't needed widening are left alone (no value
in refactoring stable code).
"""

from __future__ import annotations

import pytest

from _slice_helpers import (
    slice_to_next, slice_between,
    PY_NEXT_ROUTE, PY_NEXT_DEF, CSS_NEXT_RULE,
)


# ── slice_to_next ────────────────────────────────────────────


def test_slice_to_next_basic():
    src = "alpha BEGIN middle END omega"
    out = slice_to_next(src, "BEGIN", "END")
    assert out == "BEGIN middle "


def test_slice_to_next_picks_first_end_anchor():
    """When multiple end_anchors are given, slice ends at
    whichever appears EARLIEST in src after start."""
    src = "alpha BEGIN middle FIRST and SECOND omega"
    out = slice_to_next(src, "BEGIN", "SECOND", "FIRST")
    # FIRST appears first → slice ends there.
    assert out == "BEGIN middle "


def test_slice_to_next_falls_back_to_eos_when_no_end():
    """If no end_anchor appears after start, slice goes to
    end of src."""
    src = "alpha BEGIN middle omega"
    out = slice_to_next(src, "BEGIN", "MISSING")
    assert out == "BEGIN middle omega"


def test_slice_to_next_raises_when_start_missing():
    src = "alpha middle omega"
    with pytest.raises(ValueError):
        slice_to_next(src, "MISSING_START", "END")


def test_slice_to_next_handles_start_at_end_of_src():
    """Edge case: start anchor is at very end of src — slice
    returns just the anchor itself."""
    src = "alpha BEGIN"
    out = slice_to_next(src, "BEGIN", "END")
    assert out == "BEGIN"


# ── slice_between ────────────────────────────────────────────


def test_slice_between_basic():
    src = "alpha BEGIN middle END omega"
    out = slice_between(src, "BEGIN", "END")
    assert out == "BEGIN middle "


def test_slice_between_raises_when_end_missing():
    """Unlike slice_to_next, slice_between requires both
    anchors and raises if end is missing."""
    src = "alpha BEGIN middle omega"
    with pytest.raises(ValueError):
        slice_between(src, "BEGIN", "MISSING_END")


# ── Common anchor sets ───────────────────────────────────────


def test_py_next_route_includes_common_decorators():
    """PY_NEXT_ROUTE must cover the FastAPI decorators motif
    uses across api.py."""
    assert "@app.post(" in PY_NEXT_ROUTE
    assert "@app.get(" in PY_NEXT_ROUTE
    assert "@app.delete(" in PY_NEXT_ROUTE


def test_py_next_def_covers_indent_and_module_level():
    """PY_NEXT_DEF must match both module-level and 4-space-
    indented def declarations."""
    assert "\ndef " in PY_NEXT_DEF
    assert "\n    def " in PY_NEXT_DEF


def test_css_next_rule_covers_selectors_and_at_rules():
    """CSS_NEXT_RULE must match class, id, and at-rule starts."""
    assert "\n." in CSS_NEXT_RULE
    assert "\n#" in CSS_NEXT_RULE
    assert "\n@media" in CSS_NEXT_RULE


# ── Refactored tests still pass ──────────────────────────────


def test_refactored_tests_use_the_helper():
    """Pin that the 5 refactored tests reference _slice_helpers
    so a future revert that drops the import would be caught."""
    from pathlib import Path
    REPO = Path(__file__).resolve().parent.parent
    refactored = [
        "tests/test_v1_14_27_let_plex_serve_unplace_flow.py",
        "tests/test_v1_17_6_bulk_bar_constant_height.py",
        "tests/test_v1_18_28_repush_preserves_kind.py",
        "tests/test_v1_18_36_lps_and_switch_real_fixes.py",
        "tests/test_v1_18_38_lps_restore_for_sidecar.py",
    ]
    for rel in refactored:
        src = (REPO / rel).read_text()
        # Absolute import is what pytest's sys.path manipulation
        # supports without making tests/ a package.
        assert "from _slice_helpers import" in src, (
            f"v1.18.46: {rel} must import from _slice_helpers "
            f"(refactor was rolled into v1.18.46)"
        )
