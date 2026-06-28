"""v1.16.9 — SRC chip render must respect plex_theme_verified_ok
(closes phantom-P render mismatch).

the user on v1.16.8 (filter combinations not matching results):

> this filter combination is not working as we shouldn't be
> seeing these P rows. Adding P shows even stranger results. and
> +P only shows these results

the user selected T + `-` (and various status filters) on /movies
expecting only T-letter and `-`-letter rows. Result included
rows visually rendered as orange P chips — those rows
shouldn't be in T+`-` filter results.

## Root cause

JS had two different P-detection gates that disagreed:

  `computeSrcLetter` (app.js:6510-6541) gates P on:
      plex_has_theme && verifiedOk
  where verifiedOk = (plex_theme_verified_ok IN (null, undefined, 1))

  `renderLibraryRow` srcCell render (app.js:6620, pre-fix):
      else if (it.plex_has_theme) → render P chip

The render branch DROPPED the verifiedOk check. For phantom-P
rows (Plex still claims a theme but HEAD-verification returned
404 → verified_ok=0):

  - computeSrcLetter returns '-'
  - Server _SRC_LETTER_SQL (api.py:646) returns '-'
    (matches computeSrcLetter — checks COALESCE(verified_ok, 1)=1)
  - srcCell renders as ORANGE P CHIP (mismatch!)

That's the CLAUDE.md "Phantom P after PURGE" bug class — Plex's
metadata cache returns 200 to /library/metadata/{rk}/theme for
several seconds after motif unlinks the file. The server
correctly classifies via verified_ok; the chip render didn't.

the user's filter T+`-` SQL: `letter IN ('T', '-')`. Phantom-P
rows have letter='-' (server-side) so they correctly matched
the filter. But their CHIPS rendered as P — so the user saw
orange-P chips in a T+`-` filter result and read it as a
filter bug.

## Fix — `app/web/static/app.js`

Add the verified_ok gate to the P render branch so it mirrors
`computeSrcLetter` exactly. Phantom-P rows now render as '—'
(no chip) — matching their actual server-side letter.

## Tests
"""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"
API_PY = REPO / "app" / "web" / "api.py"


def _read_js() -> str:
    return APP_JS.read_text()


def _render_src_branch() -> str:
    """Slice the render block around line ~6600 where srcCell
    branches by row state."""
    js = _read_js()
    # Anchor on the unique 'Manual sidecar (click ADOPT to manage)'
    # title string, then walk back to the start of the if/else
    # chain (`let srcCell;`).
    end_anchor = js.index("Manual sidecar (click ADOPT to manage)")
    start = js.rindex("let srcCell;", 0, end_anchor)
    # Walk forward to the next `}` after `srcCell = '<span class="muted"...`
    end = js.index("'<span class=\"muted\" title=\"no theme\">", start)
    # Get to the closing brace of the if/else chain.
    end_brace = js.index("}", end)
    return js[start:end_brace + 1]


# ── the bug: render now gates on verified_ok ─────────────────────

def test_p_render_gates_on_plex_theme_verified_ok():
    """The P chip render must check plex_theme_verified_ok the
    same way computeSrcLetter does. Pre-v1.16.9 the branch fired
    on just `it.plex_has_theme`, which painted phantom-P rows as
    orange-P chips while the server filtered them as letter='-'."""
    block = _render_src_branch()
    # v1.21.8: the plex_cloud→P branch (added this tag) ALSO renders
    # link-badge-cloud, so anchor on the phantom-P gate directly via
    # its unique `it.plex_has_theme` condition.
    gate_start = block.index("else if (it.plex_has_theme")
    gate = block[gate_start:block.index("srcCell", gate_start)]
    assert "plex_theme_verified_ok" in gate, (
        "v1.16.9: the P chip render gate must include "
        "`plex_theme_verified_ok` to match computeSrcLetter's "
        "verifiedOk check. Without it, phantom-P rows (Plex "
        "still claims a theme but HEAD-verification returned "
        "404) render as P chips while the server-side filter "
        "classifies them as '-' → confusing filter results."
    )


def test_render_treats_null_undefined_one_as_verified_ok():
    """The verified_ok check must accept NULL / undefined / 1 as
    'verified ok'. That's the COALESCE(verified_ok, 1) = 1 SQL
    semantic — untested rows trust optimistically."""
    block = _render_src_branch()
    # v1.21.8: anchor on the phantom-P gate (the plex_cloud→P branch
    # also renders link-badge-cloud now).
    gate_start = block.index("else if (it.plex_has_theme")
    gate = block[gate_start:block.index("srcCell", gate_start)]
    # NULL / undefined / 1 — all three states should pass.
    assert "=== null" in gate or "== null" in gate
    assert "=== undefined" in gate or "== undefined" in gate
    assert "=== 1" in gate or "== 1" in gate


# ── computeSrcLetter and srcCell render must agree on P logic ────

def test_render_p_gate_mirrors_compute_src_letter_p_gate():
    """The two P-detection sites must use the same gate. Track
    both at once so a future refactor that touches one must
    also touch the other."""
    js = _read_js()
    # Anchor on computeSrcLetter's P return.
    compute_idx = js.index("if (it.plex_has_theme && verifiedOk) return 'P';")
    # Find the render-branch P gate.
    render_block = _render_src_branch()
    # v1.21.8: anchor on the phantom-P gate (it.plex_has_theme); the
    # plex_cloud→P branch also renders link-badge-cloud now.
    render_gate_start = render_block.index("else if (it.plex_has_theme")
    render_gate = render_block[
        render_gate_start:render_block.index("srcCell", render_gate_start)]
    # Both must reference plex_has_theme AND a verified_ok check.
    assert "plex_has_theme" in render_gate
    assert "plex_theme_verified_ok" in render_gate, (
        "v1.16.9: render gate must check plex_theme_verified_ok "
        "to mirror computeSrcLetter."
    )


# ── server still uses the verified_ok-aware SRC SQL ──────────────

def test_server_src_letter_sql_still_checks_verified_ok():
    """Pin the server-side _SRC_LETTER_SQL still gates P on
    verified_ok. This is the authority that the JS render must
    follow."""
    # v1.21.57: check the rendered constant (byte-identical default).
    from app.web.api import _SRC_LETTER_SQL as block
    # The P branch must include COALESCE(verified_ok, 1) = 1.
    assert "COALESCE(pi.plex_theme_verified_ok, 1) = 1" in block, (
        "v1.16.9: server-side _SRC_LETTER_SQL P branch must "
        "still gate on verified_ok. If this changes, the JS "
        "render gate must change too — that's the v1.16.9 "
        "contract."
    )


# ── regression guard: pure-P rows (verified_ok=1) still render P ─

def test_render_still_emits_p_for_verified_p_rows():
    """The fix narrows the P render branch but must NOT remove
    it. Pure-P rows (Plex serves + HEAD verified OK) still
    render as P chips."""
    block = _render_src_branch()
    # The P-chip rendering must still appear in the branch chain.
    assert "link-badge-cloud" in block
    assert "Plex agent / cloud theme" in block
