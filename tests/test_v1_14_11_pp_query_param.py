"""v1.14.11 — Pp token must round-trip through the query-param parser.

the user's repro: clicked the v1.14.10 +P SRC pill → button visually
went active (green border, yellow dot), URL shows ?src_pills=Pp,
but library results stayed at the unfiltered 10,306 matches.

Root cause: a textbook v1.13.85-style mirror-principle leak. The
v1.14.10 fix added 'Pp' to the *inner* SQL gate's valid-tokens
set (api.py:957) and emitted the right WHERE clause, but the
*outer* query-param parser at api.py:6289 had its own
hardcoded valid-letters set and silently stripped 'Pp' before
the SQL gate ever saw it. Net effect: src_set arrived empty,
the slim no-pills code path took over, and the SRC filter
no-op'd with no error surface.

v1.14.11 adds 'Pp' to the _pset valid set so the token survives
parsing.

Lesson: every place that lists valid src tokens must include
'Pp' or the new pill silently no-ops.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


def test_pset_valid_set_includes_pp():
    """The query-param parser at api.py:6289 must whitelist 'Pp'
    alongside the existing letter tokens — otherwise it strips
    the token before the SQL gate gets a chance to act on it."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    assert (
        'src_set = _pset(src_pills, {"T", "U", "A", "M", "P", "Pp", "-"})'
        in src
    )


def test_pset_pre_fix_letter_only_set_is_gone():
    """Regression guard: the pre-fix line

        src_set = _pset(src_pills, {"T", "U", "A", "M", "P", "-"})

    must not survive. A revert that re-narrows the validation
    set would silently re-break the +P pill — same shape as
    the user's original repro."""
    src_raw = (REPO / "app" / "web" / "api.py").read_text()
    # Strip Python comment lines so the rationale comment quoting
    # the deleted set doesn't trip the guard.
    src = "\n".join(
        line for line in src_raw.splitlines()
        if not line.lstrip().startswith("#")
    )
    assert (
        'src_set = _pset(src_pills, {"T", "U", "A", "M", "P", "-"})'
        not in src
    )


def test_pp_token_passes_through_pset_function():
    """Behavioral check: invoke _pset directly with 'Pp' in the
    input + the v1.14.11 valid-tokens set, verify the token
    survives. This locks the parsing semantic separately from
    the static-text guards above."""
    # _pset is a closure inside api.py — replicate its body here
    # since it's not exported. Same shape as the function at
    # api.py:6285.
    def pset(s, valid):
        if not s:
            return set()
        return {p for p in (x.strip() for x in s.split(",")) if p in valid}

    valid = {"T", "U", "A", "M", "P", "Pp", "-"}
    assert pset("Pp", valid) == {"Pp"}
    assert pset("T,Pp,A", valid) == {"T", "Pp", "A"}
    # And with the pre-fix set, 'Pp' is silently dropped (this
    # is the bug we're fixing).
    valid_pre_fix = {"T", "U", "A", "M", "P", "-"}
    assert pset("Pp", valid_pre_fix) == set()


def test_inner_sql_gate_still_lists_pp():
    """The v1.14.10 inner SQL gate's valid_tokens set must keep
    'Pp' too — otherwise even after the parser passes it through,
    the SQL gate would silently drop it. Mirror-principle:
    every layer that filters on valid src tokens needs 'Pp'."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    assert 'valid_tokens = valid_letters | {"Pp"}' in src
