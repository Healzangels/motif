"""v1.18.46 — slice helpers for source-pin tests.

motif's test suite has ~944 source-pin sites that use fixed
char-window slicing:

    fn_anchor = src.index("def something(")
    body = src[fn_anchor:fn_anchor + 2000]
    assert "expected substring" in body

The recurring problem: when a later tag adds a comment block
inside the slice scope, the expected substring gets pushed past
the window's end and the test fails. The fix is always the
same — widen the window. Over the v1.18.x cycle this hit
several tests:

  - v1.17.6 bulk-bar: widened 2000 → 3000 (v1.18.44)
  - v1.18.0 phase2: assertion updated (v1.18.36)
  - v1.14.27 unplace flow: widened 12000 → 24000 → 28000
    (v1.18.36 → v1.18.38)
  - v1.18.23 PUSH TO PLEX split: widened 500 → 1200 (v1.18.28)
  - v1.18.28 RE-PUSH: widened 1200 → 2200 (v1.18.29)
  - v1.18.36 audit slice: widened 24000 → 28000 (v1.18.38)

Each widening is a one-line fix but the same bug class keeps
recurring. The structural fix is anchor-based slicing: instead
of "N chars after the start anchor," slice up to the FIRST
occurrence of one of a few likely structural end-markers.
Survives comment additions naturally — as long as the end-
marker is still further down the file, the slice grows with
it.

## Use

    from _slice_helpers import slice_to_next

    body = slice_to_next(
        src,
        "async def api_unplace_item(",
        "@app.post(", "@app.get(", "@app.delete(",
    )
    assert "expected substring" in body

Falls through to end-of-string if no end-marker is found.

## Why not refactor all 944?

The fixed-window pattern is fine when the window comfortably
exceeds any plausible additions (e.g., 5000 chars for a
20-line function body). The tests that get re-widened are
the ones where the window was sized tight to the original
code with no headroom. Those are the high-leverage refactor
targets; the bulk is fine as-is.
"""

from __future__ import annotations


def slice_to_next(src: str, start_anchor: str, *end_anchors: str,
                   start_offset: int = 1) -> str:
    """Slice `src` from `start_anchor` up to the first occurrence
    of any `end_anchor` after the start. Falls back to end-of-
    `src` if none of the end_anchors appear.

    Args:
      src: full source text to slice
      start_anchor: literal substring marking the start. Must
        appear in src exactly once at the position you want,
        or you'll get the first occurrence.
      *end_anchors: one or more literal substrings to use as
        end markers. Slice ends at the FIRST occurrence of ANY
        of these (whichever comes first in src after start).
      start_offset: skip this many chars past start_anchor
        before searching for end_anchors. Default 1 — prevents
        a self-match if start_anchor and end_anchor happen to
        share a prefix.

    Returns: substring from start_anchor (inclusive) to the
    earliest end_anchor (exclusive), or end-of-src if no
    end_anchor matches.

    Raises: ValueError if start_anchor isn't in src.
    """
    start_idx = src.index(start_anchor)
    search_from = start_idx + len(start_anchor) + start_offset
    if search_from > len(src):
        search_from = len(src)
    best_end = len(src)
    for anchor in end_anchors:
        idx = src.find(anchor, search_from)
        if idx != -1 and idx < best_end:
            best_end = idx
    return src[start_idx:best_end]


def slice_between(src: str, start_anchor: str, end_anchor: str,
                   start_offset: int = 1) -> str:
    """Slice `src` from `start_anchor` up to the first occurrence
    of `end_anchor`. Like `slice_to_next` with exactly one
    end_anchor. Raises ValueError if either anchor is missing.

    Use when you have a single structural end marker (e.g., the
    closing `}` of a CSS rule, or a specific other section
    header) and want to fail loudly if it's gone.
    """
    start_idx = src.index(start_anchor)
    search_from = start_idx + len(start_anchor) + start_offset
    end_idx = src.index(end_anchor, search_from)
    return src[start_idx:end_idx]


# Common end-anchor sets for the languages motif tests slice.
#
# Use as: slice_to_next(src, "def foo(", *PY_ROUTE_OR_NEXT_DEF)
# where PY_ROUTE_OR_NEXT_DEF is the right end-anchor set for the
# Python-route source pattern.

PY_NEXT_ROUTE = (
    "@app.post(",
    "@app.get(",
    "@app.put(",
    "@app.delete(",
    "@app.patch(",
)
"""End-anchor set for Python route handler bodies in api.py —
the next FastAPI route decorator marks the natural end."""

PY_NEXT_DEF = (
    "\n    def ",
    "\n    async def ",
    "\ndef ",
    "\nasync def ",
)
"""End-anchor set for general Python function bodies — the next
def or async def declaration at the start of a line."""

CSS_NEXT_RULE = (
    "\n.",      # next class selector at line start
    "\n#",      # next id selector at line start
    "\n@media", # next at-rule
    "\n@keyframes",
    "\n/*",     # large comment block (often delimits sections)
)
"""End-anchor set for CSS rule blocks — the next selector or
at-rule at the start of a line."""
