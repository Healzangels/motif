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
