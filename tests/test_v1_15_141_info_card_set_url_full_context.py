"""v1.15.141 — INFO-card → SET URL forwards full row context.

the user on v1.15.140:

> also the user url from the info card should also have the
> same download only option present if it's a P row.

## Root cause — `app/web/static/app.js`

The info dialog's recovery-options dispatcher routed `manual-url`
clicks through `openManualUrlDialog` with every field except
`ratingKey` set to an empty string:

    openManualUrlDialog({
      ratingKey,
      title: '',
      year: '',
      tdbUrl: '',
      srcLetter: '',
    });

`openManualUrlDialog` uses these to:

  - Render the dialog header (`// {TITLE} ({YEAR})`)
  - Stash `dataset.tdbUrl` / `dataset.appliedUrl` for the live
    match-warning that fires when the user types a URL
    matching the row's TDB or applied URL
  - Reveal the `// DOWNLOAD ONLY` checkbox row IFF
    `srcLetter === 'P'` (v1.15.75 — only P rows have a Plex-
    served theme worth preserving alongside a backup file)

With blanks: header showed `// — (—)`, match-warning was always
silent, and the P-only DOWNLOAD ONLY option silently disappeared
when SET URL was opened from INFO instead of from the row's
SOURCE menu. The SOURCE menu's button has the data stashed at
template-render time (`menuItemHtml(..., { rk, ytUrl,
appliedUrl, srcLetter })` at app.js:7108-7113), so that entry
point worked correctly.

## Fix

Replace the hardcoded blanks with values derived from in-scope
state:

  - `title`, `year`, `tdbUrl`, `appliedUrl` from `data` (the
    /api/items API response — `data.theme.{title,year,
    youtube_url}` and `data.override.youtube_url`).
  - `srcLetter` from `computeSrcLetter(rowItem)` where rowItem
    is the matching `libraryState.items` row by `rating_key`.
    The API response shape doesn't match what `computeSrcLetter`
    expects (it needs library-row fields like media_folder /
    placement_provenance / plex_local_theme / plex_has_theme
    / plex_theme_verified_ok / source_kind), so the library
    state is the right source.
  - Falls back to `srcLetter=''` when the library row isn't in
    state (e.g. info card opened from a dashboard deep-link
    where the library hasn't loaded). Same as pre-v1.15.141
    behavior for those edge paths.

## Tests
"""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"


def _read_js() -> str:
    return APP_JS.read_text()


def _info_card_manual_url_block() -> str:
    """Slice out the info-card dispatcher's `act === 'manual-url'`
    branch. There are multiple manual-url match sites in app.js
    (recovery dispatcher, SOURCE menu); we want the one inside
    `section.querySelectorAll('button.recovery-option-btn')` —
    anchor by the surrounding `closeInfoDialog()` call which only
    appears in the info-card path."""
    js = _read_js()
    # Find the recovery-options click handler, then the manual-url
    # branch within it.
    rec_idx = js.index("button.recovery-option-btn")
    branch_idx = js.index("act === 'manual-url'", rec_idx)
    # Walk forward to the closing `}` of the openManualUrlDialog
    # call (matching brace counter).
    open_idx = js.index("openManualUrlDialog({", branch_idx)
    depth = 0
    i = open_idx
    while i < len(js):
        c = js[i]
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                # Include the trailing `);`.
                return js[branch_idx:i + 3]
        i += 1
    raise AssertionError("openManualUrlDialog call not closed")


def test_old_hardcoded_blanks_are_gone():
    """The pre-v1.15.141 blank dataset would leave a P-row INFO →
    SET URL flow with no // DOWNLOAD ONLY option. The new dispatch
    must NOT contain the literal pattern of all-blank fields."""
    block = _info_card_manual_url_block()
    blank_pattern = re.compile(
        r"title:\s*'',\s*year:\s*'',\s*tdbUrl:\s*'',\s*srcLetter:\s*''",
        re.DOTALL,
    )
    assert not blank_pattern.search(block), (
        "v1.15.141: info-card manual-url dispatcher still passes "
        "all blank fields to openManualUrlDialog — the // DOWNLOAD "
        "ONLY option will go missing on P-row SET URL flows opened "
        "from INFO."
    )


def test_src_letter_computed_from_library_row():
    """srcLetter must come from `computeSrcLetter(rowItem)` where
    rowItem is the library row matched by rating_key. The library
    row has the placement/plex_has_theme/source_kind fields that
    computeSrcLetter needs; the API response shape doesn't."""
    block = _info_card_manual_url_block()
    assert "libraryState.items" in block, (
        "v1.15.141: info-card manual-url dispatcher must look up "
        "the library row from libraryState.items so srcLetter can "
        "be computed correctly. Without the lookup we can't reveal "
        "the // DOWNLOAD ONLY option on P rows."
    )
    assert "it.rating_key === ratingKey" in block, (
        "v1.15.141: library row lookup must match on rating_key — "
        "that's what the dispatcher already validated as non-null."
    )
    assert "computeSrcLetter(rowItem)" in block, (
        "v1.15.141: srcLetter passed to openManualUrlDialog must "
        "be the output of computeSrcLetter(rowItem) — that's the "
        "canonical function the rest of the UI uses for SRC."
    )


def test_title_year_tdb_url_pulled_from_api_response():
    """title/year/tdbUrl come from `data` (the /api/items response,
    in-scope from openInfoDialog). The themes table row has all
    three columns — this is the authoritative source vs the
    library row which could be stale by a poll cycle."""
    block = _info_card_manual_url_block()
    assert "data.theme && data.theme.title" in block
    assert "data.theme && data.theme.year" in block
    assert "data.theme && data.theme.youtube_url" in block


def test_applied_url_falls_back_to_tdb_url():
    """If no user override exists (T-row case), appliedUrl should
    fall back to tdbUrl. Otherwise the match-warning's appliedVid
    branch would be silent on T rows where setting the TDB URL as
    a manual override IS the U→T conversion warning case."""
    block = _info_card_manual_url_block()
    # appliedUrl is the override URL OR the TDB URL as fallback.
    assert "data.override && data.override.youtube_url" in block, (
        "v1.15.141: appliedUrl must check data.override.youtube_url "
        "as its primary source (U-row override)."
    )
    # The fallback to tdbUrl must be the second term.
    fallback_pattern = re.compile(
        r"appliedUrl\s*=\s*\(?[^;]*data\.override[^;]*\)?\s*\|\|\s*tdbUrl",
        re.DOTALL,
    )
    assert fallback_pattern.search(block), (
        "v1.15.141: appliedUrl must fall back to tdbUrl when no "
        "override exists — otherwise the U→T-conversion match "
        "warning is silent on T rows."
    )


def test_source_menu_dispatch_unchanged():
    """The SOURCE menu's manual-url dispatch (a separate code path,
    line ~10789) already worked correctly via btn.dataset reads —
    must NOT be broken by the v1.15.141 fix. Regression guard."""
    js = _read_js()
    # Find the SOURCE menu dispatcher (anchor: btn.dataset.ytUrl,
    # which appears in the SOURCE menu path but not the info-card
    # path).
    dispatch_idx = js.index("act === 'manual-url'")
    # The first match is somewhere; find the one immediately
    # before `tdbUrl: btn.dataset.ytUrl`.
    src_menu_idx = js.index("tdbUrl: btn.dataset.ytUrl", dispatch_idx)
    block = js[src_menu_idx - 400:src_menu_idx + 200]
    assert "srcLetter: btn.dataset.srcLetter" in block, (
        "v1.15.141: SOURCE menu's manual-url dispatch must still "
        "read srcLetter from the menu button's dataset — that path "
        "was already correct and shouldn't be regressed."
    )


def test_open_manual_url_dialog_still_gates_download_only_on_p():
    """The fix only works because openManualUrlDialog gates the
    DOWNLOAD ONLY checkbox on `srcLetter === 'P'`. Re-pin that gate
    so a future refactor that drops the check would surface in the
    test (the v1.15.141 fix becomes a no-op if the gate goes away,
    but more importantly the v1.15.75 "DOWNLOAD ONLY only makes
    sense on P" contract would be lost)."""
    js = _read_js()
    fn_idx = js.index("function openManualUrlDialog(")
    body = js[fn_idx:fn_idx + 4000]
    assert "srcLetter === 'P'" in body, (
        "v1.15.141: openManualUrlDialog must gate the DOWNLOAD "
        "ONLY row reveal on `srcLetter === 'P'` — otherwise the "
        "v1.15.75 'DOWNLOAD ONLY only makes sense on P rows' "
        "contract breaks AND this v1.15.141 fix loses its target."
    )
