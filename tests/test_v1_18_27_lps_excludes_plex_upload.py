"""v1.18.27 — lpsState excludes plex_upload (LINK chip fix).

the user's repro on v1.18.26: tried PUSH TO PLEX (API) on a row
that was previously LINK=PS (Plex Pass cloud theme) + SRC=P.
Post-push state:

  - SRC = T+P (correct: motif sourced from TDB, Plex still has
    its own cloud theme)
  - PL  = cyan 'pushed' (correct: plex_upload placement)
  - LINK = **PS** (WRONG — should be PU)

## Root cause

The `lpsState` derivation:

  lpsState = file_path && !media_folder && plex_independent_theme === 1

For a plex_upload row:
  - file_path is set (motif has canonical)
  - media_folder is '' (plex_upload sentinel) → `!media_folder` is true
  - plex_independent_theme=1 (Plex Pass cloud theme exists alongside)

So lpsState evaluates true. The linkCell render's branch ladder
checks lpsState BEFORE placement_kind === 'plex_upload', so the
PS chip renders even though motif's API push is the active
theme.

LPS specifically means motif has WITHDRAWN its placement; a
live plex_upload placement is the opposite intent. The fix:
tighten `lpsState` (and the two backend `is_lps` derivations)
to also require `placement_kind !== 'plex_upload'`.

The SQL `link_pills='ps'` predicate already correctly excludes
plex_upload rows because `p.media_folder IS NULL` is FALSE for
the empty-string sentinel. Only Python's truthy-string check
needed the explicit guard.

## Files

  - app/web/static/app.js: lpsState gains the plex_upload
    exclusion + reordered to compute isPlexUpload first.
  - app/web/api.py: both _row_matches_pl + _row_matches_attn
    is_lps derivations gain the same exclusion.

## Mirror principle

This is the THIRD place the plex_upload vs lpsState gap had to
be patched in the JS/Python parity chain:

  v1.18.16 — awaitingApproval excludes plex_upload
  v1.18.22 — plex_enum indep_observation guards plex_upload
  v1.18.27 — lpsState (THIS) excludes plex_upload

Each one targets a different downstream consumer of "Plex has
something motif doesn't directly own." A future audit pass
should grep for `plex_independent_theme === 1` usages to
verify no fourth consumer needs the same exclusion.
"""

from __future__ import annotations

import sys
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

APP_JS = REPO / "app" / "web" / "static" / "app.js"
API_PY = REPO / "app" / "web" / "api.py"


# ── JS lpsState ──────────────────────────────────────────────


def test_js_lps_state_excludes_plex_upload():
    """`lpsState` const must include `&& !isPlexUpload` (or
    equivalent) so plex_upload rows don't trigger the LINK=PS
    chip."""
    js = APP_JS.read_text()
    idx = js.index("const lpsState =")
    block = js[idx:idx + 600]
    assert "!isPlexUpload" in block, (
        "v1.18.27: lpsState must exclude plex_upload to keep "
        "the PS link chip from firing on API-pushed rows"
    )
    # The discriminator order matters: isPlexUpload must be
    # defined BEFORE lpsState (used inside it).
    plex_upload_idx = js.index(
        "const isPlexUpload = it.placement_kind === 'plex_upload'"
    )
    lps_idx = js.index("const lpsState =")
    assert plex_upload_idx < lps_idx, (
        "v1.18.27: isPlexUpload must be declared before "
        "lpsState (lpsState references it)"
    )


def test_js_lps_state_keeps_legacy_predicate_intact():
    """Regression guard: the v1.14.39 discriminator (file_path
    + !media_folder + plex_independent_theme === 1) must still
    be present. v1.18.27 only ADDS the !isPlexUpload term."""
    js = APP_JS.read_text()
    idx = js.index("const lpsState =")
    block = js[idx:idx + 600]
    assert "!!it.file_path" in block
    assert "!it.media_folder" in block
    assert "it.plex_independent_theme === 1" in block


# ── Backend _row_matches_pl is_lps ────────────────────────────


def test_row_matches_pl_is_lps_excludes_plex_upload():
    """`_row_matches_pl` is_lps derivation must include the
    placement_kind != 'plex_upload' guard."""
    src = API_PY.read_text()
    fn_idx = src.index("def _row_matches_pl(it, pills):")
    body = src[fn_idx:fn_idx + 2000]
    assert "is_lps = (" in body
    # Find the is_lps block and verify the new term.
    lps_idx = body.index("is_lps = (")
    lps_block = body[lps_idx:lps_idx + 600]
    assert (
        'it.get("placement_kind") != "plex_upload"' in lps_block
    ), (
        "v1.18.27: _row_matches_pl is_lps must exclude "
        "plex_upload"
    )


# ── Backend _row_matches_attn is_lps (attn_pills='await') ─────


def test_row_matches_attn_await_is_lps_excludes_plex_upload():
    """The attn_pills='await' matcher's local is_lps must also
    include the plex_upload exclusion. Pre-fix only the
    is_plex_upload check guarded the await branch, but is_lps
    itself was looser — for the broken branch (which doesn't
    have the is_plex_upload extra check) this would have
    let plex_upload rows slip into broken classification."""
    src = API_PY.read_text()
    # Scope to _row_matches_attn function body (multiple
    # `elif p == "await":` lines exist across SQL branches).
    fn_idx = src.index("def _row_matches_attn(it):")
    body = src[fn_idx:fn_idx + 3000]
    assert 'elif p == "await":' in body
    elif_idx = body.index('elif p == "await":')
    block = body[elif_idx:elif_idx + 1500]
    # The is_lps inside this branch needs the same fix.
    assert "is_lps = (" in block
    # Find the new exclusion.
    assert (
        'it.get("placement_kind") != "plex_upload"' in block
        or 'and not is_plex_upload' in block
    ), (
        "v1.18.27: attn 'await' is_lps must factor in "
        "plex_upload (either inline or via the explicit "
        "is_plex_upload local)"
    )


# ── SQL pl_pills='ps' already excludes plex_upload ────────────


def test_sql_link_pills_ps_already_excludes_via_is_null():
    """Regression guard: SQL link_pills='ps' uses
    `p.media_folder IS NULL` which is FALSE for plex_upload
    rows (their media_folder is the empty-string sentinel, not
    NULL). So the SQL predicate already correctly excludes
    plex_upload — no v1.18.27 change needed there. This pin
    catches a future refactor that switches `IS NULL` to
    something like `not media_folder` (which would reintroduce
    the bug)."""
    src = API_PY.read_text()
    # Find the link_pills='ps' branch.
    elif_idx = src.index('elif p == "ps":')
    # v1.19.63: widened 800→1500 to absorb the new comment block
    # explaining the v1.19.61 PS→BK unification drift fix.
    # v1.19.64: widened further to 3000 to absorb the v1.19.64
    # WIDENED comment block explaining the pure-P-rows fold-in.
    # v1.24.28: 3000→3700 to absorb the new 'rp' (re-push) link_pills
    # branch inserted between 'pu' and 'b' (the first branch carrying
    # the media_folder IS NULL predicate this pin anchors on).
    block = src[elif_idx:elif_idx + 3700]
    assert "COALESCE(p_e.media_folder, p_g.media_folder) IS NULL" in block
