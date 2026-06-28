"""v1.14.13 — plain P SRC pill narrows to pure-P (composite-+P excluded).

Pre-v1.14.10 the SRC axis had no surface for "rows where motif
owns the theme AND Plex also serves its own copy" (the
composite-+P state, indicated by the yellow dot on the row's
SRC chip). v1.13.43 had widened the plain `P` pill to OR
(pure-P + composite) so users could at least find composite
rows, even though it conflated two semantically distinct cases.

v1.14.10/v1.14.11 added the dedicated `+P` pill (wire token
`Pp`) that exclusively matches composite rows. The plain `P`
pill's OR-with-composite branch is now redundant — and worse,
it makes P + +P selected together a no-op union (every composite
row matches both branches).

the user's repro / ask: "since we have a +p filter now can we
remove yellow dot results from the P filter".

v1.14.13 narrows the plain P pill to pure-P only:

    {SRC letter} = 'P'

Composite rows have their own pill (`+P`); they shouldn't
double-count in P. Tests pin the new shape, lock the
mirror-principle relationship between the JS dot indicator
and the +P SQL clause, and regression-guard against the OR
clause re-appearing under the P branch.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── P pill SQL is now pure-P only ─────────────────────────────


def test_p_pill_clause_is_pure_p_only():
    """The plain `P` pill's WHERE clause must be exactly
    `(_SRC_LETTER_SQL) = 'P'` — no OR, no composite branch.
    The composite case has its own pill."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    # Locate the v1.14.13 marker comment + the clause that follows.
    marker = "v1.14.13: now that the +P pill"
    assert marker in src, "v1.14.13 marker comment must be present"
    block_start = src.index(marker)
    # The clause appender lives in the same `if include_p_flag:` block.
    block = src[block_start:block_start + 1500]
    # Pure-P only — no composite OR.
    assert 'clauses.append("(" + _LIB_SRC_LETTER_SQL + ") = \'P\'")' in block


def test_p_pill_no_longer_ors_in_plex_independent_theme():
    """Regression guard: the pre-fix shape

        clauses.append(
            "((" + _SRC_LETTER_SQL + ") = 'P' "
            "OR pi.plex_independent_theme = 1)"
        )

    must not survive under the `if include_p_flag:` branch. A
    revert that re-broadens P would silently re-conflate pure-P
    and composite, undoing v1.14.13.

    Strip Python comment lines so the rationale comment quoting
    the deleted SQL doesn't trip the guard."""
    src_raw = (REPO / "app" / "web" / "api.py").read_text()
    src = "\n".join(
        line for line in src_raw.splitlines()
        if not line.lstrip().startswith("#")
    )
    # Locate the `if include_p_flag:` branch block.
    anchor = src.index("if include_p_flag:")
    end = src.index("if include_plus_p_only:", anchor)
    block = src[anchor:end]
    # The OR-composite clause must not appear in this block.
    assert "OR pi.plex_independent_theme = 1" not in block
    # And the composite-only clause must NOT have leaked into the
    # plain-P branch either.
    assert "NOT IN ('P', '-')" not in block


# ── +P pill SQL still composite-only ──────────────────────────


def test_plus_p_pill_clause_unchanged_from_v1_14_10():
    """The +P pill's SQL clause is the only home for composite
    rows now. v1.14.13 doesn't touch it; pin the clause shape
    so a future P-broadening attempt doesn't compensate by
    narrowing +P (which would leave composite rows unreachable
    from the SRC axis entirely)."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    anchor = src.index("if include_plus_p_only:")
    block = src[anchor:anchor + 1500]
    assert "pi.plex_independent_theme = 1" in block
    assert "NOT IN ('P', '-')" in block


# ── P + +P selected together = pure-P UNION composite (no overlap) ─


def test_p_and_plus_p_combined_clauses_partition_cleanly():
    """Mirror-principle check: when the user selects BOTH P and
    +P pills together, the two clauses must form a clean
    partition (no row matches both, but together they cover
    pure-P AND composite). v1.14.13's narrowing makes this
    naturally true:
      - P clause: SRC letter = 'P'        (composite excluded — composite letter is in T/U/A/M)
      - +P clause: SRC letter NOT IN ('P', '-') AND plex_independent_theme = 1

    Static-text guard locks the partition shape."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    # P branch's letter check is exclusive to 'P'.
    p_anchor = src.index("if include_p_flag:")
    p_end = src.index("if include_plus_p_only:", p_anchor)
    p_block = src[p_anchor:p_end]
    # +P branch's letter check excludes 'P'.
    pp_anchor = src.index("if include_plus_p_only:")
    pp_block = src[pp_anchor:pp_anchor + 1500]
    # P branch matches `letter = 'P'`, +P branch matches
    # `letter NOT IN ('P', '-')` — disjoint.
    assert "= 'P'" in p_block
    assert "NOT IN ('P', '-')" in pp_block


# ── JS dot indicator + +P pill predicate still mirror each other ──


def test_dot_render_gate_unchanged_by_v1_14_13():
    """v1.14.13 only narrows the plain P pill — the dot indicator
    on row SRC chips and the +P pill's predicate (which the dot
    must agree with) are unchanged. Pin both sides so the mirror
    holds."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert "it.plex_independent_theme === 1" in js
    assert "_primaryLetter !== 'P'" in js
    assert "_primaryLetter !== '-'" in js
    src = (REPO / "app" / "web" / "api.py").read_text()
    assert "pi.plex_independent_theme = 1" in src


# ── Other SRC letters unchanged (T/U/A/M/-) ───────────────────


def test_non_p_letter_branches_unchanged():
    """The non-P letter branches (T, U, A, M, -) still flow
    through the IN(?, ?, …) clause via `non_p`. v1.14.13
    doesn't touch this path; pin so a refactor doesn't
    accidentally collapse the P narrowing into the non_p path.

    Whitespace-tolerant: v1.14.99 split the `clauses.append(...)`
    call across multiple lines to add the Pp-narrowing branch.
    Collapse runs of whitespace so the substring check still
    finds the IN-clause shape."""
    raw = (REPO / "app" / "web" / "api.py").read_text()
    flat = " ".join(raw.split())
    # non_p excludes both P and Pp tokens (the special-case branches).
    assert 'non_p = [w for w in wanted if w not in ("P", "Pp")]' in flat
    # The IN clause still uses placeholders fed by non_p.
    assert 'clauses.append( f"({_LIB_SRC_LETTER_SQL}) IN ({placeholders})" )' in flat \
        or 'f"({_LIB_SRC_LETTER_SQL}) IN ({placeholders})"' in flat
