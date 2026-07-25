"""v0.51.222 — a ratchet that stops fixed-window source-slice rot from recurring.

motif's source-pin tests slice a source file to a fixed char window and assert substrings
land inside it:

    i = APP_JS.index("function foo(")
    block = APP_JS[i:i + 800]
    assert "expected" in block

The recurring failure (documented in _slice_helpers.py, and hit ~8× in the v0.51.212-221
run): a later tag adds a comment or line inside that scope, the expected substring is
pushed past the window edge, and the test fails on a change that preserved the invariant
exactly. The structural fix is anchor-based slicing (slice_to_next / slice_between); the
recurring one is to just widen the window, which is a treadmill.

This test measures the HEADROOM of every measurable fixed-window slice — how far the last
asserted substring sits from the window's end — and enforces two rules:

  1. HARD FLOOR: no site may sit at zero headroom (the asserted content touching the very
     edge). Those break on the next edit anywhere in scope; they must be migrated to
     slice_to_next or widened.
  2. RATCHET: the number of tight (<20% headroom) sites may not EXCEED the recorded
     baseline. A new tight slice, or an existing one drifting tighter, trips it — so the
     population can only hold or shrink, never grow.

The measurement is a heuristic (it recognises the `X = SRC.index("lit")` + `SRC[X:X+N]`
+ following `assert "..." in X` shape). Sites it can't parse are simply not counted — the
ratchet is a floor on rot, not a proof of its absence.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent

# Source files the pin-tests slice, by the alias names those tests use.
_SOURCES = {
    "APP_JS": "app/web/static/app.js",
    "APP_CSS": "app/web/static/app.css",
    "OPS": "app/web/static/ops.css",
    "OPS_JS": "app/web/static/ops.js",
    "DASH": "app/web/templates/dashboard.html",
    "DASH_JS": "app/web/static/app.js",
    "LIBRARY": "app/web/templates/library.html",
    "LIB": "app/web/templates/library.html",
    "API": "app/web/api.py",
    "API_PY": "app/web/api.py",
    "SYNC_PY": "app/core/sync.py",
    "PLEX_ENUM_PY": "app/core/plex_enum.py",
    "ENUM_SRC": "app/core/plex_enum.py",
    "RECOVERY_PY": "app/core/recovery_v55.py",
    "NOTIFY_PY": "app/core/notify.py",
    "SCHED_PY": "app/core/scheduler.py",
    "SCANNER_PY": "app/core/orphan_scan.py",
}

# The current tight-site population, recorded so the ratchet can only shrink. When a
# migration or widening reduces the count, LOWER this number in the same commit — that is
# the ratchet tightening. It must never be raised.
_TIGHT_BASELINE = 96


def _measure():
    """Yield (test_file, alias, window_N, last_needed_offset, headroom_ratio) for every
    measurable fixed-window slice site across tests/."""
    src_cache = {}
    for alias, rel in _SOURCES.items():
        p = REPO / rel
        if p.exists():
            src_cache[alias] = p.read_text()
    for tf in sorted((REPO / "tests").glob("test_*.py")):
        txt = tf.read_text()
        for m in re.finditer(r'(\w+)\s*=\s*([A-Z_]+)\.index\(\s*([\'"])(.+?)\3', txt):
            var, alias = m.group(1), m.group(2)
            lit = m.group(4)
            if alias not in src_cache:
                continue
            seg = txt[m.end():m.end() + 400]
            s = re.search(
                rf'{re.escape(var)}\s*(?:-\s*\d+)?\s*:\s*{re.escape(var)}\s*\+\s*(\d+)\s*\]',
                seg)
            if not s:
                continue
            n = int(s.group(1))
            src = src_cache[alias]
            pos = src.find(lit)
            if pos < 0:
                continue
            window = src[pos:pos + n]
            after = txt[m.end():m.end() + 1500]
            offsets = [window.find(nd) + len(nd)
                       for _, nd in re.findall(
                           r'assert\s+([\'"])(.+?)\1\s+(?:not\s+)?in\b', after)
                       if window.find(nd) >= 0]
            max_off = max(offsets) if offsets else 0
            if max_off == 0:
                continue
            yield (tf.name, alias, n, max_off, (n - max_off) / n)


def test_no_slice_sits_at_zero_headroom():
    """The hard floor. A window whose asserted content reaches its final characters breaks
    on the next edit inside that scope. Migrate it to _slice_helpers.slice_to_next (bounds
    by a structural end-marker, grows with the code) or widen it with real headroom."""
    at_edge = [f"{f}: {alias} window={n} content-ends-at={off} ({round(hr*100)}% headroom)"
               for f, alias, n, off, hr in _measure() if hr < 0.02]
    assert not at_edge, (
        "these fixed-window slices are at/near zero headroom and will fail on the next "
        "in-scope edit — migrate to slice_to_next or widen:\n  " + "\n  ".join(at_edge))


def test_tight_slice_population_does_not_grow():
    """The ratchet. New tight (<20% headroom) slices, or existing ones drifting tighter,
    push the count over baseline. When a cleanup lowers the count, lower _TIGHT_BASELINE in
    the same commit — it may shrink, never grow."""
    tight = [r for r in _measure() if r[4] < 0.20]
    assert len(tight) <= _TIGHT_BASELINE, (
        f"{len(tight)} tight slice sites, baseline is {_TIGHT_BASELINE} — a new fixed-window "
        "slice was added tight, or an existing one drifted tighter. Use slice_to_next for "
        "new source pins; if you legitimately reduced the count, LOWER _TIGHT_BASELINE.")
    # keep the baseline honest — if it drifts far below the real count, tighten it
    assert len(tight) >= _TIGHT_BASELINE - 15, (
        f"only {len(tight)} tight sites vs baseline {_TIGHT_BASELINE} — the population "
        f"shrank; lower _TIGHT_BASELINE to {len(tight)} to lock in the gain.")


# ── the other half of the rot: slice_to_next fall-throughs ────────────────────
#
# v0.51.224 (ultra-review #5): the ratchet above measures FIXED-window slices. The
# STRUCTURAL fix it steers toward — _slice_helpers.slice_to_next — has its own silent
# failure mode: if none of its end-anchors match after the start, it "falls back to
# end-of-src" (its docstring) and returns the whole rest of the file. That is a slice with
# no bound at all — the vacuous window the whole sweep exists to prevent — but _measure()
# can't see it (it recognises only `SRC[X:X+N]`). v0.51.222's own dashCountUp migration
# shipped exactly this: 2-space end-anchors on a 4-space-nested function, silently EOF.
#
# This resolves every static slice_to_next call against its real source and fails if any
# falls through to EOF. Only calls whose source is a known _SOURCES alias and whose
# anchors are string literals are checked — a call over a local var (`rc = ....read_text()`)
# or a computed anchor is skipped, same "can't parse it, don't count it" contract as above.

def _string_const(node) -> str | None:
    return node.value if isinstance(node, ast.Constant) and isinstance(node.value, str) else None


def _iter_slice_to_next_calls():
    """Yield (test_file, lineno, alias, start_anchor, end_anchors) for every statically
    resolvable slice_to_next(...) call across tests/ — alias in _SOURCES, all anchors str
    literals. Unresolvable calls (local-var source, non-literal anchor) are skipped."""
    for tf in sorted((REPO / "tests").glob("test_*.py")):
        try:
            tree = ast.parse(tf.read_text())
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if not (isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Name)
                    and node.func.id == "slice_to_next"):
                continue
            if not node.args:
                continue
            src = node.args[0]
            if not (isinstance(src, ast.Name) and src.id in _SOURCES):
                continue                                   # local-var source — can't resolve
            start = _string_const(node.args[1]) if len(node.args) > 1 else None
            ends = [_string_const(a) for a in node.args[2:]]
            if start is None or any(e is None for e in ends):
                continue                                   # computed anchor — can't resolve
            yield tf.name, node.lineno, src.id, start, ends


def test_no_slice_to_next_call_falls_through_to_end_of_file():
    """A slice_to_next whose end-anchors never match returns the whole rest of the file —
    an unbounded window. Resolve each call and fail loudly on any that does, so the
    slice_helpers footgun can't ship a silently-EOF slice (its asserts then pass only by
    the luck of file-unique substrings, guarding nothing)."""
    src_cache = {}
    for alias, rel in _SOURCES.items():
        p = REPO / rel
        if p.exists():
            src_cache[alias] = p.read_text()
    fell_through = []
    for fname, lineno, alias, start, ends in _iter_slice_to_next_calls():
        src = src_cache.get(alias)
        if src is None or start not in src:
            continue                                       # runtime would ValueError anyway
        search_from = src.index(start) + len(start) + 1
        if not any(src.find(e, search_from) != -1 for e in ends):
            fell_through.append(f"{fname}:{lineno} slice_to_next({alias}, {start!r}) — "
                                f"none of {ends} match after the start; slice runs to EOF")
    assert not fell_through, (
        "these slice_to_next calls fall through to end-of-file (unbounded window) — their "
        "end-anchors don't match the current source; re-anchor them:\n  "
        + "\n  ".join(fell_through))


# ── the wrong-occurrence anchor: a start anchor that resolves into a COMMENT ───
#
# v0.51.227 (ultra-review): a source-pin's start anchor is resolved with str.index() = the
# FIRST occurrence. If the same literal text also appears earlier inside a COMMENT (very
# likely, since these anchors quote code that comments describe), the slice bounds the WRONG
# region and the assertion silently guards nothing. v0.51.225 shipped exactly this: the
# anchor "elif section_id:" matched a `# … the `elif section_id:` branch …` comment in
# api.py before the real branch. This walks every static slice_between/slice_to_next call
# and fails if its start anchor's first occurrence lands on a comment-only line.

_COMMENT_PREFIX = {".py": "#", ".js": "//", ".css": "/*", ".html": "<!--"}


def _comment_prefix_for(rel: str) -> str | None:
    for ext, pfx in _COMMENT_PREFIX.items():
        if rel.endswith(ext):
            return pfx
    return None


def test_no_slice_anchor_resolves_into_a_comment():
    """A start anchor whose first source occurrence is a comment resolves THERE, so the
    slice bounds the wrong region — a silent phantom guard. Fail on any such anchor; the fix
    is a code-unique or line-start (`\\n`+indent) anchor the comment's mid-line copy can't
    match. Only literal-anchor calls over a known _SOURCES alias are checked (same
    can't-parse-it-skip-it contract as the ratchet above)."""
    src_cache = {alias: ((REPO / rel).read_text(), rel)
                 for alias, rel in _SOURCES.items() if (REPO / rel).exists()}
    bad = []
    for tf in sorted((REPO / "tests").glob("test_*.py")):
        try:
            tree = ast.parse(tf.read_text())
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                    and node.func.id in ("slice_between", "slice_to_next")):
                continue
            if len(node.args) < 2:
                continue
            src = node.args[0]
            start = _string_const(node.args[1]) if len(node.args) > 1 else None
            if not (isinstance(src, ast.Name) and src.id in src_cache) or start is None:
                continue
            text, rel = src_cache[src.id]
            pfx = _comment_prefix_for(rel)
            if pfx is None or start not in text:
                continue
            anchor_line = text.splitlines()[text[:text.index(start)].count("\n")]
            if anchor_line.lstrip().startswith(pfx):
                bad.append(f"{tf.name}:{node.lineno} {node.func.id}({src.id}, {start!r}) "
                           f"→ first matches a COMMENT: {anchor_line.strip()!r}")
    assert not bad, (
        "these source-pin start anchors resolve into a comment, not the code they mean to "
        "bound (silent phantom guard) — use a code-unique or line-start anchor:\n  "
        + "\n  ".join(bad))
