"""v1.18.24 — `placed` mirror-drift fix for plex_upload rows.

the user on v1.18.23 deploy:

  > tried a swap to API from the place menu on an existing U row.
  > It looks like it did properly but my SRC is now gone. Also in
  > the collections section see the proper PU link row but still
  > seeing the srs as P when it would be U.

## Root cause: class-P mirror drift

`renderLibraryRow` has THREE sites that compute a local `placed`
flag from row state. v1.18.0 added the plex_upload recognition
to `computeSrcLetter` (line 7212), but TWO other sites with the
same pattern were missed:

  * Line 7294 — `renderLibraryRow`'s inline SRC chip block. Used
    directly by the chip-rendering code at lines 7302+. Without
    the plex_upload OR-clause, the chip falls through every
    `placed && ...` branch to the `P` fallback (collections) or
    the `–` default (movies post-SWITCH PLACEMENT). the user's
    101 Dalmatians + M*A*S*H bugs trace back here.

  * Line 9034 — bulk-bar bucket counter loop. Plex_upload rows
    selected for bulk actions were counted as not-placed,
    misclassifying THEMED counts.

The SAME row data drove the LINK chip correctly (rendering PU)
because the LINK branch reads `it.placement_kind` directly
without going through the local `placed` variable. The visible
contradiction (LINK=PU but SRC=P / SRC=– on the same row) is the
class-P drift signature.

## v1.18.24 fix

Two-line change: both sites OR-in `it.placement_kind ===
'plex_upload'`, matching the canonical pattern in
computeSrcLetter + the v1.18.16 awaitingApproval derivation.
"""

from __future__ import annotations

import sys
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

APP_JS = REPO / "app" / "web" / "static" / "app.js"


# ── All three `placed` sites must agree ───────────────────────


def test_all_placed_sites_recognize_plex_upload():
    """Every `const placed = …` site in app.js must reference
    `plex_upload` within its multi-line expression. Class-P
    mirror drift guard: catches future copy-paste sites that
    drop the plex_upload recognition.

    The expression may span multiple lines (parenthesized or
    OR-chained); we check that within the 200-char window
    following each site, `plex_upload` appears."""
    js = APP_JS.read_text()
    cursor = 0
    sites = []
    while True:
        idx = js.find("const placed = !!it.media_folder", cursor)
        if idx < 0:
            break
        sites.append(idx)
        cursor = idx + 1
    assert len(sites) >= 3, (
        f"expected at least 3 `const placed = !!it.media_folder` "
        f"sites, got {len(sites)}"
    )
    for site_idx in sites:
        block = js[site_idx:site_idx + 200]
        assert "plex_upload" in block, (
            f"v1.18.24: `const placed = ` site at offset "
            f"{site_idx} missing plex_upload recognition in its "
            f"200-char window: {block!r}"
        )


def test_compute_src_letter_placed_includes_plex_upload():
    """Canonical site: computeSrcLetter's placed const. Should
    have had the v1.18.0 plex_upload OR from the start."""
    js = APP_JS.read_text()
    fn_idx = js.index("function computeSrcLetter(")
    body = js[fn_idx:fn_idx + 2000]
    assert "const placed = !!it.media_folder" in body
    assert "it.placement_kind === 'plex_upload'" in body


def test_render_library_row_inline_src_placed_fixed():
    """The renderLibraryRow inline-SRC `placed` const at
    line ~7294 must now OR-in plex_upload."""
    js = APP_JS.read_text()
    # Anchor on the v1.18.24 marker comment.
    assert "v1.18.24: class-P (mirror drift) fix" in js
    idx = js.index("v1.18.24: class-P (mirror drift) fix")
    block = js[idx:idx + 2000]
    assert "const placed = !!it.media_folder" in block
    assert "it.placement_kind === 'plex_upload'" in block


def test_bulk_bar_bucket_counter_placed_fixed():
    """The bulk-bar bucket counter loop (line ~9034) must also
    OR-in plex_upload so selected API-pushed rows count as
    placed for the THEMED/SIDECAR bucket math."""
    js = APP_JS.read_text()
    # Anchor on the comment marker we added.
    assert (
        "v1.18.24: count plex_upload placements as `placed`"
        in js
    )
    idx = js.index("v1.18.24: count plex_upload")
    block = js[idx:idx + 600]
    assert "it.placement_kind === 'plex_upload'" in block


def test_no_bare_placed_definition_survives():
    """Regression guard: the bare `const placed = !!it.media_folder;`
    shape (without the OR clause) must not exist anywhere. Pre-
    fix v1.18.24 had 2 of these (lines 7294 + 9034); both must
    be fully replaced."""
    js = APP_JS.read_text()
    # The bare form must not appear with a semicolon terminator.
    # Multi-line form with the OR is fine; only the single-line
    # bare form is the bug.
    bare = "const placed = !!it.media_folder;"
    assert bare not in js, (
        f"v1.18.24: bare `const placed = !!it.media_folder;` "
        f"shape must not survive — all sites must include "
        f"the plex_upload OR-clause"
    )
