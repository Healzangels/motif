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
import functools
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
# v0.51.285: 1501 → 1497 (the carousel quartet: test_v0_51_82's two 1400-char
# tile-render windows and test_v1_24_58's 3300-char loadRecentlyAdded window —
# this tag's own comments pushed their pins past the edges — plus
# test_v0_51_87's 4200-char autoscroll window, overrun by the rAF rewrite.
# All anchored to the render loop's tail call / the next sibling function).
# v0.51.288: 1497 → 1496 (test_v1_23_75's 600-char reaper-attach window,
# already widened once at v0.51.151, rotted again when the dispatch became
# dispatch_coalesced — anchored to the record_fire statement after the call).
# v0.51.294: 1496 → 1495 (test_v1_22_74's commit-gate window, already widened
# TWICE (3200→4000→7000), rotted a third time when the snapshot validator
# commit landed before the gate — anchored to the compaction line).
# v0.51.295: 1495 → 1493 (test_v1_22_32's 900-char and test_v1_23_64's
# 700-char still_p windows — both rotted by the phantom-P qualifier, both
# anchored to .fetchone(); the SAME query had THREE independent windows).
# v0.51.298: 1493 → 1492 (test_v1_18_5's 200-char finally window, rotted by
# the FK txn-close guard — sliced to the function body's end).
# v0.51.306: 1492 → 1490 (test_v1_14_82 + test_v1_14_85's 2000-char deep-link
# gate windows, rotted by the consume-once strip — both anchored to the
# gate's catch line).
# v0.51.308: 1490 → 1489 (test_v1_17_20's 4000-char openInfoDialog window,
# rotted by the 404 empty-state — anchored to the success path's first read).
_BUDGET = 1489

# v0.51.309 (audit r2): the BACKWARD shape `x[a - N:a + M]` was invisible to
# the detector (its lower bound is a BinOp, not a bare Name) — and a new one
# shipped straight through the gate in v0.51.308, which is exactly the hole
# a ratchet cannot afford. Same rules: equality, only ever DOWN.
# v0.51.311 (review): 79 → 214 — not growth: the detector learned the two
# shapes it was blind to (`max(0, a - N)` and Call bases — 135 windows, the
# measured 214 − 79), so the census sees the whole population. Still equality,
# still only DOWN.
_BACKWARD_BUDGET = 214


def _backward_base(node):
    """Unwrap `a - N`, `max(0, a - N)` / `max(a - N, 0)`, `x.index(m) - N` → (base_dump, N)."""
    if (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
            and node.func.id == "max" and len(node.args) == 2):
        # v0.51.312 (audit): either arg order — `max(i - N, 0)` was unseen.
        consts = [i for i, arg in enumerate(node.args)
                  if isinstance(arg, ast.Constant) and arg.value == 0]
        if consts:
            node = node.args[1 - consts[0]]
    if (isinstance(node, ast.BinOp) and isinstance(node.op, ast.Sub)
            and isinstance(node.right, ast.Constant)
            and isinstance(node.right.value, int)):
        return ast.dump(node.left), node.right.value
    return None


@functools.lru_cache(maxsize=None)
def _windows(path: Path) -> "tuple[tuple, tuple]":
    """ONE parse + ONE walk per file per process; caches only the two small
    (lineno, width) tuples — v0.51.312 (audit): the .311 tree cache held every
    parsed AST (~190 MB) for the rest of the suite."""
    try:
        tree = ast.parse(path.read_text())
    except SyntaxError:
        return (), ()
    fwd: list[tuple[int, int]] = []
    back: list[tuple[int, int]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Subscript) or not isinstance(node.slice, ast.Slice):
            continue
        lo, up = node.slice.lower, node.slice.upper
        # forward: x[a:a + N] with a bare Name lower bound
        if (isinstance(lo, ast.Name)
                and isinstance(up, ast.BinOp) and isinstance(up.op, ast.Add)
                and isinstance(up.left, ast.Name) and up.left.id == lo.id
                and isinstance(up.right, ast.Constant)
                and isinstance(up.right.value, int)):
            if up.right.value >= _MIN_WIDTH:
                fwd.append((node.lineno, up.right.value))
            continue
        # backward: x[a - N:...] incl. max()-wrapped and Call bases
        base = _backward_base(lo)
        if base is None:
            continue
        base_dump, width = base
        if (isinstance(up, ast.BinOp) and isinstance(up.op, ast.Add)
                and ast.dump(up.left) == base_dump
                and isinstance(up.right, ast.Constant)
                and isinstance(up.right.value, int)):
            width += up.right.value
        if width >= _MIN_WIDTH:
            back.append((node.lineno, width))
    return tuple(fwd), tuple(back)


def _fixed_windows(path: Path) -> list[tuple[int, int]]:
    """(lineno, width) for each `x[a:a + <int>]` guard (AST, not regex)."""
    return list(_windows(path)[0])


def _backward_windows(path: Path) -> list[tuple[int, int]]:
    """(lineno, width) for each `x[a - N:...]` guard the forward walk cannot see."""
    return list(_windows(path)[1])


def _census() -> dict[str, list[tuple[int, int]]]:
    return {p.name: w for p in sorted(TESTS.glob("test_*.py"))
            if (w := _fixed_windows(p))}


def _backward_census() -> dict[str, list[tuple[int, int]]]:
    return {p.name: w for p in sorted(TESTS.glob("test_*.py"))
            if (w := _backward_windows(p))}


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


def test_no_new_backward_window_guards():
    """v0.51.309: the `x[a - N:a + M]` shape — invisible to the forward
    detector — gets its own equality ratchet after one shipped through the
    gate unseen in v0.51.308."""
    census = _backward_census()
    total = sum(len(v) for v in census.values())
    if total == _BACKWARD_BUDGET:
        return
    widest = sorted(
        ((w, f, ln) for f, hits in census.items() for ln, w in hits),
        reverse=True)[:10]
    listing = "\n".join(f"    {w:>6} chars  {f}:{ln}" for w, f, ln in widest)
    if total > _BACKWARD_BUDGET:
        raise AssertionError(
            f"v0.51.309: {total - _BACKWARD_BUDGET} NEW backward window "
            f"guard(s) ({total} vs budget {_BACKWARD_BUDGET}). Anchor "
            f"structurally instead.\nWidest:\n{listing}")
    raise AssertionError(
        f"v0.51.309: good — {_BACKWARD_BUDGET - total} backward guard(s) "
        f"converted. Lower _BACKWARD_BUDGET to {total} to bank it.")


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
        back = _backward_windows(p)
    assert found == [(2, 4000)], (
        f"detector must catch exactly the fixed-width guard, got {found}"
    )
    assert back == [], f"no backward shapes in the sample, got {back}"


def test_backward_detector_is_not_vacuous():
    """Same proof for the v0.51.309 backward-shape detector."""
    import tempfile
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "sample.py"
        p.write_text(
            "def f(src, i, j, n):\n"
            "    a = src[i - 500:i + 1500]\n"   # backward+forward -> 2000
            "    b = src[i - 300:i]\n"          # backward only -> 300
            "    c = src[i - 2:i + 2]\n"        # short -> ignored
            "    d = src[i - n:i]\n"            # variable -> ignored
            "    e = src[i - 500:j + 100]\n"    # mixed names -> 500 only
            "    g = src[max(0, i - 400):i + 100]\n"   # max-wrapped -> 500
            "    h = src[src.index('x') - 300:src.index('x') + 50]\n"  # Call base -> 350
            "    return a, b, c, d, e, g, h\n"
        )
        back = _backward_windows(p)
    assert back == [(2, 2000), (3, 300), (6, 500), (7, 500), (8, 350)], (
        f"backward detector mis-census: {back}")


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
