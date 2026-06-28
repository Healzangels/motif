"""v1.19.66 — revert v1.19.64 PS widening + drop PS chip entirely.

the user's reaction to v1.19.64 in production: the PS chip widened
to cover pure-P rows landed amber chips on 5,240 of ~5,500 themed
rows on his library. Wall-to-wall amber, with SRC=P + LINK=PS
side-by-side doubling the "Plex serves" signal pointlessly.
the user: "it feels like a lot is going on, i'm leaning towards
revert and maybe getting rid of PS entirely."

Confirmed via design discussion: PS is **vestigial** post-v1.19.61.

  - Original PS (v1.14.40) meant the LET-PLEX-SERVE outcome:
    motif has canonical + no placement + Plex serves. That state
    is now stamped backup_only by v1.19.61's worker — so the row
    paints BU, never PS.
  - v1.19.64 widened to cover pure-P rows. Visual disaster.
  - Filter use case ("Plex serves + no backup") preserved via
    SRC=P chip + LINK=— chip combination (axis-AND).

## v1.19.66 changes

  1. `app/web/static/app.js` — removed the PS render branch from
     the linkCell cascade + the `isPlexServingLink` predicate
     added in v1.19.64. `lpsState` kept (the `awaitingApproval`
     predicate subtracts it to suppress the !P attention glyph
     on intentional no-placement rows).
  2. `app/web/api.py` — PS SQL filter branch reduced to a no-op
     (`1 = 0`) so existing `link_pills=ps` URL params don't 500;
     they return an empty set. Em-dash SQL filter aligned with
     the JS render: matches any row without placement + without
     backup_only stamp (covers pure-P + truly-empty + lpsState).
  3. `app/web/templates/library.html` — PS filter chip button
     removed from the LINK row. Em-dash tooltip rewritten to
     reference the SRC=P + LINK=— combination workflow.

## What stays the same

  - `link_pills=ps` URL param honored (as no-op) for bookmark /
    deep-link backwards compat.
  - `.link-glyph-ps` CSS rule preserved (dead but harmless;
    removing might mask future PS revival).
  - `lpsState` JS variable preserved (awaitingApproval consumer).
  - All BU/BP/HL/C/M/PU chips unchanged.
"""
from __future__ import annotations

import sys
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

API_PY = (REPO / "app" / "web" / "api.py").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
LIBRARY_HTML = (
    REPO / "app" / "web" / "templates" / "library.html"
).read_text()


# ── JS: PS render branch + widened predicate removed ─────────


def test_link_cell_has_no_ps_render_branch():
    """The linkCell cascade must not contain a PS render branch.
    Pre-v1.19.66 had `else if (isPlexServingLink)` (v1.19.64) or
    `else if (lpsState)` (pre-v1.19.64). v1.19.66 removed it
    entirely — pure-P rows fall through to the em-dash default."""
    # Walk from the cascade start to the placement_kind=hardlink
    # branch (the next gate after where PS used to be).
    chain_anchor = APP_JS.index(
        'let linkCell = \'<span class="link-glyph link-glyph-none">'
    )
    end = APP_JS.index("placement_kind === 'hardlink'", chain_anchor)
    block = APP_JS[chain_anchor:end]
    # Neither the widened nor narrow PS branch may appear as a
    # cascade gate.
    assert "} else if (isPlexServingLink) {" not in block, (
        "v1.19.66: widened PS render branch must be removed"
    )
    assert "} else if (lpsState) {" not in block, (
        "v1.19.66: narrow lpsState PS render branch must be removed"
    )
    # The PS link-glyph render must not appear in the cascade.
    assert "link-glyph-ps" not in block, (
        "v1.19.66: no link-glyph-ps in the active cascade"
    )


def test_is_plex_serving_link_predicate_removed():
    """The v1.19.64 `isPlexServingLink` predicate must be gone.
    `lpsState` stays (awaitingApproval consumer)."""
    assert "const isPlexServingLink =" not in APP_JS, (
        "v1.19.66: isPlexServingLink predicate must be removed"
    )
    assert "const lpsState = " in APP_JS, (
        "v1.19.66: lpsState preserved for awaitingApproval"
    )


def test_v1_19_66_marker_documents_revert():
    """A v1.19.66 marker near where PS used to render must
    explain WHY the chip was removed."""
    assert "v1.19.66: removed the PS render branch" in APP_JS


# ── SQL: PS branch no-op + em-dash aligned to JS render ──────


def _link_pills_branch(name: str) -> str:
    link_anchor = API_PY.index("if link_pills:")
    branch_idx = API_PY.index(f'elif p == "{name}":', link_anchor)
    end_idx = API_PY.index("        if branches:", branch_idx)
    return API_PY[branch_idx:end_idx]


def test_ps_sql_branch_is_noop():
    """The link_pills='ps' SQL branch must be a no-op (1 = 0
    or equivalent) so existing URL params return empty instead
    of 500ing."""
    block = _link_pills_branch("ps")
    sql_start = block.index("branches.append(")
    sql_end = block.index(")", sql_start) + 1
    sql_str = block[sql_start:sql_end]
    assert "1 = 0" in sql_str or "1=0" in sql_str, (
        "v1.19.66: PS SQL must be a no-op (1=0) for URL "
        "backwards compat; got: " + sql_str
    )


def test_ps_sql_marker_explains_v1_19_66_removal():
    """The PS SQL branch marker must explain WHY the chip was
    dropped + the migration path (SRC=P + LINK=— combo)."""
    block = _link_pills_branch("ps")
    assert "v1.19.66" in block
    assert "vestigial" in block.lower() or "removed" in block.lower()


def test_none_sql_filter_matches_post_revert_render():
    """Em-dash SQL filter must match what the JS now renders —
    excludes only the rows that paint specific chips
    (HL/C/PU/M/BU/BP). After the v1.19.66 revert that means:
      - exclude `media_folder NOT NULL` (HL/C/PU/M)
      - exclude `backup_only` stamp (BU/BP)
      - INCLUDE pure-P rows (the v1.19.65 plex_independent_theme
        exclusion is GONE)"""
    block = _link_pills_branch("none")
    sql_start = block.index("branches.append(")
    # v1.21.59: p.* reads are now COALESCE(p_e.x, p_g.x), so the appended
    # SQL has nested parens — match the OUTER append(...) paren, not the
    # first ')' (which now closes COALESCE).
    _open = block.index("(", sql_start)
    _depth = 0
    for _j in range(_open, len(block)):
        if block[_j] == "(":
            _depth += 1
        elif block[_j] == ")":
            _depth -= 1
            if _depth == 0:
                sql_end = _j + 1
                break
    sql_str = block[sql_start:sql_end]
    assert "COALESCE(p_e.media_folder, p_g.media_folder) IS NULL" in sql_str
    assert "backup_only" in sql_str, (
        "v1.19.66: em-dash SQL must exclude backup_only rows so "
        "BU/BP rows don't double-match"
    )
    # The v1.19.65 plex_independent_theme exclusion must be GONE
    # — pure-P rows should match em-dash again.
    assert "plex_independent_theme" not in sql_str, (
        "v1.19.66: em-dash SQL must NOT exclude Plex-serving "
        "rows — pure-P rows render em-dash and should match"
    )


# ── Template: PS chip removed from LINK filter row ────────────


def test_library_html_has_no_ps_filter_chip():
    """The LINK filter row must not contain a PS chip button."""
    # Bound to the LINK pill filter row.
    link_anchor = LIBRARY_HTML.index('aria-label="LINK pill filter"')
    ed_anchor = LIBRARY_HTML.index(
        'aria-label="EDITION pill filter"', link_anchor
    )
    link_row = LIBRARY_HTML[link_anchor:ed_anchor]
    assert 'data-link-pill="ps"' not in link_row, (
        "v1.19.66: PS chip button must be removed from the LINK "
        "filter row"
    )
    # The v1.19.66 explanation comment should sit in its place.
    assert "v1.19.66" in link_row


def test_emdash_chip_tooltip_references_src_p_combo_workflow():
    """The em-dash chip tooltip must guide the user to the
    SRC=P + LINK=— combination for the post-PS-removal workflow."""
    idx = LIBRARY_HTML.index('data-link-pill="none"')
    block = LIBRARY_HTML[max(0, idx - 200):idx + 500]
    assert "SRC=P" in block
    assert "backup" in block.lower()


# ── URL deep-link compat preserved ───────────────────────────


def test_pset_allow_list_still_includes_ps():
    """The api.py _pset allow-list for link_pills must STILL
    include 'ps' — deep links should validate, then hit the
    no-op SQL branch and return empty. Removing 'ps' from the
    set would 400 on `?link_pills=ps` URLs instead of returning
    an empty result."""
    assert '"ps"' in API_PY, (
        "v1.19.66: 'ps' must stay in the _pset allow-list for "
        "URL backwards compat (the SQL is a no-op)"
    )


def test_js_deep_link_parser_still_includes_ps():
    """The JS PILL_DEEP_LINKS values Set for link_pills must
    STILL include 'ps' — same backwards-compat reason."""
    idx = APP_JS.index("param: 'link_pills'")
    block = APP_JS[idx:idx + 2000]
    assert "'ps'" in block


# ── Version pin ──────────────────────────────────────────────


def test_v1_19_66_version_pin():
    """v1.19.66 bumped. Relaxed to v1.19.x prefix after v1.19.67
    continued the line."""
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
