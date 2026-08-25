"""v0.51.261 — ratchet against NEW fixed-width source-slice guards.

A guard shaped `body = src[anchor:anchor + 4000]` is a treadmill. It does not
fail when the thing it protects breaks; it fails when unrelated code GROWS past
the window. Four went red in a single session on 2026-08-11:

  * test_v1_14_54 (+5000, already bumped once from 3500) — a comment pushed
    `log.exception` out of the window
  * test_v1_15_35 — pinned the literal `range(3)`
  * test_v0_51_148 — pinned an exact URL literal
  * test_v1_20_0 — the real damage: its +800 window overshot the `except` it
    named into the NEXT function and matched that function's copy of the line,
    so it would have passed with the handler deleted outright

That last one is the reason this matters beyond tidiness. The everyday cost is
worse though: a gate that manufactures a red every few tags trains you to read
gate failures as noise — and that is the same signal a real regression uses.

**Scope, stated honestly.** This ratchets GROWTH. It does not detect which of
the 1513 existing windows are phantoms. An audit on 2026-08-11 tried to, and
the detector produced 72 false positives, then 4, and all 4 turned out to be a
brace-matcher tripping over an `extras = {}` default parameter — so no
overshoot check ships here. Zero real phantoms were found beyond v1.20.0's.

**The structural alternatives** (what to write instead):
  * python  — `src[i:src.index("\\ndef ", i + 1)]`     (next top-level def)
  * app.js  — `js[i:js.index("\\n  function ", i + 1)]` (next sibling function)
  * SQL     — slice to `.fetchall()` / the closing paren
  * JS bind — slice to the next `')?.addEventListener`
Anchor to a construct the language guarantees; never to a byte count.
"""
from __future__ import annotations

import ast
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
TESTS = REPO / "tests"

# Windows below this are ordinary short slices (`line[i:i + 2]`), not guards.
_MIN_WIDTH = 100

# The number of fixed-width guards at v0.51.261. This may only ever go DOWN.
# Equality, not `<=`, is deliberate: `<=` lets the count sit stale forever, so
# the ratchet never actually tightens. When you convert one to a structural
# anchor the gate goes red and tells you the new number — that IS the ratchet.
# v0.51.264: 1512 → 1504 (the eight `elif url_changed:` windows across three
# files, all bounded now by the next top-level def — they had been re-widened
# six times between v1.21.10 and v1.22.45 and broke again on a 40-line insert).
# v0.51.269: 1504 → 1503 (api_probe_tdb's 12000-char window, already widened once
# at v1.14.54, rotted again — the asserted literal landed 18 chars from the edge).
# v0.51.274: 1503 → 1502 (test_v1_20_46's 220-char focus-ring window, rotted by
# this tag's own comment + selector insert — anchored to the closing brace).
# v0.51.278: 1502 → 1501 (test_v1_19_89's info-card window, widened twice
# before and squeezed to 2% headroom by the revisions section — the .222
# headroom ratchet caught it BEFORE it broke this time; anchored now).
_BUDGET = 1501


def _fixed_windows(path: Path) -> list[tuple[int, int]]:
    """(lineno, width) for each `x[a:a + <int>]` with a matching lower bound.

    AST, not regex. The audit that motivated this tag used a regex and was
    wrong twice; a Subscript/Slice walk cannot mistake a default parameter or
    a template literal for a slice."""
    try:
        tree = ast.parse(path.read_text())
    except SyntaxError:
        return []
    out: list[tuple[int, int]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Subscript):
            continue
        sl = node.slice
        if not isinstance(sl, ast.Slice):
            continue
        lo, up = sl.lower, sl.upper
        if not (isinstance(lo, ast.Name)
                and isinstance(up, ast.BinOp) and isinstance(up.op, ast.Add)
                and isinstance(up.left, ast.Name) and up.left.id == lo.id
                and isinstance(up.right, ast.Constant)
                and isinstance(up.right.value, int)):
            continue
        if up.right.value >= _MIN_WIDTH:
            out.append((node.lineno, up.right.value))
    return out


def _census() -> dict[str, list[tuple[int, int]]]:
    return {p.name: w for p in sorted(TESTS.glob("test_*.py"))
            if (w := _fixed_windows(p))}


def test_no_new_fixed_width_window_guards():
    census = _census()
    total = sum(len(v) for v in census.values())
    if total == _BUDGET:
        return
    widest = sorted(
        ((w, f, ln) for f, hits in census.items() for ln, w in hits),
        reverse=True)[:10]
    listing = "\n".join(f"    {w:>6} chars  {f}:{ln}" for w, f, ln in widest)
    if total > _BUDGET:
        raise AssertionError(
            f"v0.51.261: {total - _BUDGET} NEW fixed-width window guard(s) "
            f"({total} vs budget {_BUDGET}).\n"
            f"A `src[a:a + N]` guard fails when unrelated code grows, not when "
            f"the thing it protects breaks. Anchor structurally instead — see "
            f"this module's docstring for the per-language forms.\n"
            f"Widest windows currently in the suite:\n{listing}"
        )
    raise AssertionError(
        f"v0.51.261: good — {_BUDGET - total} fixed-width guard(s) were "
        f"converted. Lower _BUDGET in this file to {total} to bank it.\n"
        f"(The budget only ratchets DOWN; an assertion that tolerated the "
        f"slack would never tighten.)"
    )


def test_detector_is_not_vacuous():
    """A ratchet that matches nothing passes forever. Prove the detector both
    fires on the real shape and ignores the shapes it must not claim."""
    import tempfile
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "sample.py"
        p.write_text(
            "def f(src, i, n, extras={}):\n"
            "    a = src[i:i + 4000]\n"        # the guard shape -> caught
            "    b = src[i:i + 2]\n"           # short slice -> ignored
            "    c = src[i:i + n]\n"           # variable width -> ignored
            "    d = src[i:j + 4000]\n"        # different name -> ignored
            "    e = src[i:src.index('x', i)]\n"  # structural -> ignored
            "    return a, b, c, d, e\n"
        )
        found = _fixed_windows(p)
    assert found == [(2, 4000)], (
        f"detector must catch exactly the fixed-width guard, got {found}"
    )


def test_budget_matches_a_real_census():
    """Guard the guard: if the census ever returns nothing (a walk bug, a moved
    tests/ dir), the ratchet above would silently compare 0 to 0 forever."""
    census = _census()
    assert len(census) > 100, (
        f"census collapsed to {len(census)} files — the detector is broken, "
        f"not the suite"
    )
    assert _BUDGET > 0


def test_v0_51_261_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
