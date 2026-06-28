"""v1.18.75 — isPlexAgentRow excludes plex_upload placements.

the user's repro: U+PU row (SRC=U user URL, LINK=PU motif's plex_upload
placement). Click UPLOAD MP3 → triggered the "Plex is already supplying
a theme" confirmation prompt. Click SET URL → same prompt.

That prompt is for true P-agent rows where Plex has ITS OWN theme that
motif doesn't manage — defer-to-Plex semantic. A plex_upload row is
motif's OWN content uploaded via Plex's HTTP API; there's no Plex theme
to defer to.

## Root cause

`isPlexAgentRow(it)` (app.js:~12225) checked:
  !!it && !it.media_folder && !it.plex_local_theme && !!it.plex_has_theme

That predicate matches BOTH true P-agent rows AND plex_upload rows:
  - True P-agent: no placement record → media_folder undefined (falsy)
  - plex_upload: placements row exists but `media_folder=''` (falsy)

Both pass `!it.media_folder`. The v1.18.0 schema widening added the
plex_upload placement_kind with media_folder='' as the sentinel —
isPlexAgentRow wasn't updated to exclude that kind.

## Fix

Mirror the v1.18.0 `placed` calculation in computeSrcLetter:
  motifPlaced = !!it.media_folder || it.placement_kind === 'plex_upload'

When motifPlaced is true, it's not a P-agent row regardless of
plex_has_theme. plex_has_theme=1 on a plex_upload row just means
"motif's API upload IS what Plex now serves" — that's expected, not
a conflict.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


def _is_plex_agent_row_body() -> str:
    """Return the isPlexAgentRow function body."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    fn_idx = js.index("function isPlexAgentRow(it)")
    # Walk forward to the closing brace at the same indent.
    fn_end = js.index("\n    function ", fn_idx + 1)
    return js[fn_idx:fn_end]


def test_predicate_excludes_plex_upload_placement_kind():
    """The fixed predicate must check that the row's placement_kind
    is NOT 'plex_upload'. That kind is motif's own API upload —
    not Plex's own theme."""
    body = _is_plex_agent_row_body()
    assert "placement_kind === 'plex_upload'" in body, (
        "v1.18.75: predicate must check placement_kind to exclude "
        "rows where motif placed via Plex's HTTP API"
    )


def test_predicate_mirrors_compute_src_letter_placed_logic():
    """The 'motifPlaced' calculation must match the placed logic
    in computeSrcLetter (line ~7367): media_folder OR plex_upload.
    Same axis, same predicate."""
    body = _is_plex_agent_row_body()
    assert "motifPlaced" in body, (
        "v1.18.75: explicit motifPlaced local mirrors the "
        "computeSrcLetter `placed` calculation"
    )
    # The two operand shape.
    import re
    body_flat = re.sub(r"\s+", " ", body)
    assert "!!it.media_folder || it.placement_kind === 'plex_upload'" in body_flat


def test_predicate_returns_false_when_motif_placed():
    """The final return must short-circuit on motifPlaced. A row
    where motif owns the placement is not a P-agent row regardless
    of plex_has_theme."""
    body = _is_plex_agent_row_body()
    # Negation of motifPlaced gates the return.
    assert "!motifPlaced" in body
    assert "plex_has_theme" in body  # other condition still present


def test_predicate_handles_null_input_safely():
    """`it` may be undefined (the v1.10.50 lookup path can fail).
    Pre-fix the predicate's `!!it &&` short-circuit guarded against
    that. Post-fix must preserve the guard."""
    body = _is_plex_agent_row_body()
    assert "if (!it) return false;" in body, (
        "v1.18.75: null-input safety guard preserved"
    )


def test_v1_18_75_marker_explains_v1_18_0_link():
    """The marker comment must reference v1.18.0 — that's the tag
    that added the plex_upload placement kind, so future code-
    archaeologists tracking 'why does this check placement_kind'
    can find the schema-widening rationale."""
    body = _is_plex_agent_row_body()
    assert "v1.18.75" in body
    assert "v1.18.0" in body or "plex_upload" in body, (
        "v1.18.75: marker must reference v1.18.0 (the tag that "
        "added plex_upload placement_kind) so the schema lineage "
        "is traceable"
    )


def test_compute_src_letter_placed_logic_unchanged():
    """The reference `placed` logic in computeSrcLetter must be
    unchanged — v1.18.75 mirrors IT, doesn't change it. Pin the
    canonical shape so a future computeSrcLetter refactor that
    diverges trips a test (forcing the author to update
    isPlexAgentRow too)."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    fn_idx = js.index("function computeSrcLetter(it)")
    fn_end = js.index("\n  function ", fn_idx + 1)
    body = js[fn_idx:fn_end]
    # The canonical `placed` shape.
    import re
    body_flat = re.sub(r"\s+", " ", body)
    assert (
        "const placed = !!it.media_folder || it.placement_kind === 'plex_upload';"
        in body_flat
    ), (
        "v1.18.75: computeSrcLetter's `placed` calculation is the "
        "canonical shape isPlexAgentRow mirrors. If this fails, "
        "audit isPlexAgentRow for the matching update"
    )
