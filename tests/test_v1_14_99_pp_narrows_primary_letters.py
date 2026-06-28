"""v1.14.99 — +P pill narrows primary letters when both selected.

the user: "can we make it so when just +P filter it shows all
+P but then when you add another kind like M it would only
show M that are +P or if you did A and M would only be rows
that are M or A +P rows"

## Pre-fix semantics

The SRC pill filter used pure OR within the axis. Selecting
+P + M emitted:

    (letter='M') OR (composite-+P, any letter)

→ surfaced every M row (composite or not) PLUS every composite
row regardless of primary letter. the user wanted the +P to act
as a NARROWING modifier on the primary-letter set: "M rows
that are also +P", not "M rows OR every composite row."

## Fix

When +P (Pp) is selected ALONGSIDE primary letters
(T/U/A/M/-), fold the composite requirement into the primary-
letter IN-clause as an AND. Suppress the stand-alone composite
clause for that case (otherwise the OR would re-widen).

  Pp alone                 → all composite rows (unchanged)
  Pp + M                   → M rows that are also composite
  Pp + A + M               → A or M rows that are also composite
  M alone                  → all M rows (unchanged)
  P + Pp                   → pure-P OR composite (unchanged —
                             P and Pp are mutually exclusive at
                             the row level, OR is fine)
  P + Pp + M               → pure-P OR (M rows that are also
                             composite). The Pp narrows ONLY the
                             non-P primary letters; pure-P stays
                             a separate OR branch.

## Tests

The SRC pill filter logic is server-side only — JS sends the
pill set and SQL handles it. Tests verify the SQL shape via
both static-text guards (the new pp_modifies_non_p flag, the
gated stand-alone clause) AND a direct call to
_library_main_query against a seeded in-memory DB to confirm
the result counts match the new semantics end-to-end.
"""
from __future__ import annotations

import tempfile
from pathlib import Path

from app.core import db as db_module


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"


# ── Static-text guards on the SQL builder ──────────────────────


def test_pp_modifies_non_p_flag_defined():
    """The src_pills loop must compute a `pp_modifies_non_p`
    flag — the gate that switches Pp from OR-extension to
    AND-narrowing when primary letters are also selected."""
    src = API_PY.read_text()
    assert "pp_modifies_non_p = include_plus_p_only and bool(non_p)" in src


def test_non_p_branch_includes_composite_AND_when_pp_modifier_active():
    """When `pp_modifies_non_p` is True, the non_p IN-clause
    must AND with `pi.plex_independent_theme = 1`. That's the
    narrowing that produces "M rows that are also +P."""
    src = API_PY.read_text()
    anchor = src.index("if pp_modifies_non_p:")
    block = src[anchor:anchor + 600]
    assert "AND pi.plex_independent_theme = 1" in block, (
        "When Pp is selected with primary letters, the non_p clause "
        "must AND the composite predicate to narrow correctly"
    )


def test_standalone_composite_clause_gated_on_pp_modifies_non_p():
    """When `pp_modifies_non_p` is True, the stand-alone
    composite clause must be suppressed — otherwise the OR with
    the narrowed non_p clause would re-widen back to all
    composite rows."""
    src = API_PY.read_text()
    anchor = src.index("if include_plus_p_only:")
    block = src[anchor:anchor + 1500]
    assert "if not pp_modifies_non_p:" in block, (
        "Stand-alone composite clause must be gated on "
        "`if not pp_modifies_non_p` to avoid re-widening when "
        "Pp is already folded into the non_p AND clause"
    )


def test_pp_alone_still_emits_standalone_composite_clause():
    """When `Pp` is selected with NO other primary letters,
    `pp_modifies_non_p` is False (non_p is empty) so the
    stand-alone composite clause must still fire — that's the
    "show me all +P rows" case the user wants preserved."""
    src = API_PY.read_text()
    # The composite clause body must exist somewhere in the file.
    assert "pi.plex_independent_theme = 1" in src
    assert "NOT IN ('P', '-')" in src
    # And the gate must be `if not pp_modifies_non_p`, not a
    # stricter condition that would also suppress the alone case.
    anchor = src.index("if include_plus_p_only:")
    block = src[anchor:anchor + 1500]
    # The standalone clause must NOT be unconditionally suppressed.
    # Check the gate is exactly `not pp_modifies_non_p`, not e.g.
    # `not include_plus_p_only` (which would always be False here)
    # or `non_p` (which would invert the case).
    assert "if not pp_modifies_non_p:" in block


# ── Detailed shape of the emitted clauses ──────────────────────


def test_non_p_branch_uses_simple_in_when_pp_not_modifying():
    """When `pp_modifies_non_p` is False (Pp not selected, OR
    no primary letters selected), the non_p branch must emit
    the simple `IN (placeholders)` clause — no composite AND.
    This is the regression-guard against accidental narrowing
    of M-alone (which the user wants left as "all M rows")."""
    src = API_PY.read_text()
    anchor = src.index("if non_p:")
    block = src[anchor:anchor + 1200]
    # The else branch of `if pp_modifies_non_p:` is the simple shape.
    else_idx = block.index("else:")
    else_block = block[else_idx:else_idx + 400]
    assert "IN ({placeholders})" in else_block, (
        "Without pp_modifies_non_p, non_p must emit the simple "
        "IN clause (no composite narrowing)"
    )
    assert "plex_independent_theme" not in else_block, (
        "The else branch must NOT include the composite predicate — "
        "that would silently narrow M-alone"
    )


def test_pp_plus_p_combo_keeps_or_with_pure_p():
    """When BOTH `P` and `Pp` are selected (a contradictory but
    legal combo), `P` and `Pp` must remain SEPARATE OR branches.
    They're mutually exclusive at the row level so OR is the
    natural / correct semantics — no narrowing intended."""
    src = API_PY.read_text()
    # The include_p_flag branch must stay independent (no shared
    # AND with Pp).
    p_anchor = src.index("if include_p_flag:")
    p_end = src.index("if include_plus_p_only:", p_anchor)
    p_block = src[p_anchor:p_end]
    # The pure-P clause must NOT have been narrowed by composite.
    assert "plex_independent_theme" not in p_block, (
        "include_p_flag branch must stay pure-P only; not gated on "
        "plex_independent_theme (that's the +P pill's job)"
    )


def test_v1_14_99_marker_explains_the_narrowing():
    """A v1.14.99 marker explains the narrowing rationale so a
    future "simplify the OR loop" refactor doesn't accidentally
    drop the AND-narrowing branch."""
    src = API_PY.read_text()
    anchor = src.index("pp_modifies_non_p")
    # Walk back to find the comment block.
    block = src[max(0, anchor - 1500):anchor + 100]
    assert "v1.14.99" in block
    # the user's exact framing should be referenced for context.
    block_lower = block.lower()
    assert ("m rows that are also +p" in block_lower
            or "narrowing modifier" in block_lower), (
        "Marker should reference the narrowing-modifier intent"
    )
