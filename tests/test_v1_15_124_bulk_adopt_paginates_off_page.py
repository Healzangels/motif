"""v1.15.124 — bulk ADOPT paginates to cover off-page selections.

the user's report on v1.15.116:

> Not sure why it only is scoping to the 42 on the first page and
> not the rest, I have M SRC and No TDB scoped to be able to adopt
> themes not in themerrdb but are manually present.

## The bug

The `library-adopt-selected-btn` click handler filtered the bulk-
adopt candidate list from `libraryState.items` — the CURRENTLY-
RENDERED page's row data. After SELECT ALL FILTERED loaded 231
selected keys (across pages) with the SRC=M + NO TDB filter, the
handler only matched the ~42 keys that happened to be on the
visible page.

Pre-fix dialog:

  189 selected row(s) aren't on the visible page; ADOPT will run
  on the 42 sidecar-only rows on this page.

That's the symptom. The user EXPECTED bulk ADOPT to cover all
231 filtered + selected rows.

## The fix

Walk every page of the current filter via /api/library?page=N
(same pagination loop EXPORT CSV uses at app.js:9628). For each
returned row:

  1. Skip rows not in `libraryState.selected` (SELECT ALL
     FILTERED added all of them but the user may have unchecked
     some manually)
  2. Skip rows that aren't sidecar-only state (defensive — the
     SRC=M filter should already scope to these, but per-row
     check guards against filter edge cases)
  3. Collect the rest as ADOPT candidates

Then confirm + run adopt per-rk as before. The confirm dialog
now reads the actual recoverable count, not just the visible-
page subset.

## Behavior changes

  - Pre-fix: bulk ADOPT on 231 selected → 42 rows adopted (page
    contents only), 189 silently dropped
  - Post-fix: bulk ADOPT on 231 selected → 231 rows adopted (or
    however many actually pass the sidecar-only gate)

Initial scan/load adds a "// SCANNING…" label so the button's
state is honest while the pagination runs. Cap at 200 pages (the
existing safety bound — never expected to trip).

## Tests
"""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"


def _adopt_handler_body() -> str:
    """Slice the bulk-ADOPT click-handler block. Anchor on the
    `addEventListener` site specifically, not on the earlier
    `document.getElementById('library-adopt-selected-btn')`
    appearance that just gates UI affordance reveal."""
    src = APP_JS.read_text()
    anchor = src.index(
        "document.getElementById('library-adopt-selected-btn')"
        "?.addEventListener('click'"
    )
    return src[anchor:anchor + 4500]


def test_bulk_adopt_paginates_api_library():
    """The handler must walk /api/library pages, not just filter
    libraryState.items. Match the same pagination shape EXPORT
    CSV uses."""
    body = _adopt_handler_body()
    # Must call api('GET', '/api/library' + ...).
    assert "'/api/library?'" in body or '"/api/library?"' in body, (
        "v1.15.124: bulk ADOPT must call /api/library to recover "
        "off-page selected rows."
    )
    # Must increment page in a loop.
    assert "page++" in body or "page += 1" in body, (
        "v1.15.124: bulk ADOPT must paginate (page-by-page loop)."
    )
    # Must use buildLibraryFilterParams so the current filter is
    # threaded server-side.
    assert "buildLibraryFilterParams(200)" in body


def test_bulk_adopt_filters_by_selection_and_sidecar_state():
    """Each page-walked item must be tested against (selected set
    AND sidecar-only state). Pre-fix only the in-page items were
    tested at all."""
    body = _adopt_handler_body()
    # Selection-set check.
    assert "selectedKeys.has(libKey(it))" in body, (
        "v1.15.124: bulk ADOPT must check selection membership "
        "per page-walked item."
    )
    # Sidecar-only state check.
    # v0.50.89: the gate widened to the canonical `placed` predicate
    # (plex_upload counts as placed) — mirror of the adoptOnlyCount bucket.
    # Pre-fix the bare `!it.media_folder` treated a plex_upload row
    # (media_folder='') as unplaced and mis-included it as an adopt candidate.
    assert "it.placement_kind === 'plex_upload'" in body
    assert "!placed && !!it.plex_local_theme" in body, (
        "v1.15.124/v0.50.89: bulk ADOPT must keep the sidecar-only "
        "per-row gate (now via the widened `placed`) so non-sidecar "
        "selections are skipped."
    )


def test_bulk_adopt_safety_bound_on_page_count():
    """The pagination loop must have a safety bound so a
    pathological response can't infinite-loop the browser."""
    body = _adopt_handler_body()
    assert "page > 200" in body, (
        "v1.15.124: pagination loop must cap at 200 pages "
        "(matches EXPORT CSV safety bound)."
    )


def test_bulk_adopt_scanning_label_shown_during_pagination():
    """While the pagination is running, the button label should
    reflect work-in-progress so the user doesn't think the click
    was lost."""
    body = _adopt_handler_body()
    assert "SCANNING" in body, (
        "v1.15.124: button label should show // SCANNING… while "
        "the pagination is running."
    )
